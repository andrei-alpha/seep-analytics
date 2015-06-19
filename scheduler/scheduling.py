import os
import time
import math
import json
import util
import requests

from socket import gethostname

class Scheduler(object):
  def __init__(self, config, log, dispatcher):
    self.config = config
    self.log = log
    self.dispatcher = dispatcher
    self.report = {}
    self.updates = []
    self.allocations = {}
    self.lastReport = None
    self.working = True

  def sleep(self, seconds):
    while self.working and seconds > 0:
      time.sleep(1)
      seconds -= 1

  def stop(self):
    self.log.info('Stoping scheduler thread...')
    self.working = False

  def resetAllocations(self):
    self.allocations = {}

  def getResourceReport(self):
    try:
      res = requests.get('http://' + gethostname() + ':' + str(self.config.getint('Basic', 'server.port')) + '/command/scheduler_report')
      if res.text == 'pending':
        return
      report = json.loads(res.text)['hosts']
      self.lastReport = report
      self.dispatcher.checkPending(report)
      self.updates.append(report)
    except requests.ConnectionError:
      self.log.warn("Failed to get resource report!")

  def expPDF(self, x, y):
    return math.pow(y * math.e, y * x)

  def estimatePotential(self, percentage):
    y = self.config.getfloat('Scheduler', 'potential.lambda')
    z = self.config.getfloat('Scheduler', 'potential.max')
    maxp = self.expPDF(1, y)
    x = float(percentage) / 100.0
    val = self.expPDF(x, y)
    return max(1.0, 1 + (z - 1) * ((val - 1) / (maxp - 1)))

  def computeCpuScore(self, host):
    host['avg_cpu'] = host['avg_cpu'] + self.dispatcher.getEstimation(host['host'], 'cpu') / len(host['cpu'])
    host['potential'] = self.estimatePotential(host['avg_cpu'])
    host['cpu_score'] = sum(float(x['cpu_percent'] * host['potential']) / len(host['cpu']) for x in host['workers'])
    nonSeepCpu = host['avg_cpu'] - sum(float(x['cpu_percent']) / len(host['cpu']) for x in host['workers'])
    host['cpu_score'] += int(0 if nonSeepCpu < 10 else host['potential'] * nonSeepCpu)

  def computeIoScore(self, host):
    totalIo = max((sum(host['disk_io']) + sum(host['net_io'])) / 2.0, sum(map(lambda w: sum(w['disk_io']) + sum(w['net_io']) / 2.0, host['workers'])))
    if totalIo == 0:
      host['io_percent'] = 0
      host['io_potential'] = 0
      host['io_score'] = 0
      for worker in host['workers']:
        worker['io_percent'] = 0
      return

    host['io_percent'] = max(100, int(100 * sum(host['disk_io']) / self.config.getfloat('Scheduler', 'max.disk.io.host') + 100 * sum(host['net_io']) / self.config.getfloat('Scheduler', 'max.net.io.host')) / 2)
    host['io_percent'] = host['io_percent'] + self.dispatcher.getEstimation(host['host'], 'io')
    host['io_potential'] = self.estimatePotential(host['io_percent'])
    for worker in host['workers']:
        worker['io_percent'] = int((((sum(worker['disk_io']) + sum(worker['net_io'])) / 2.0) / totalIo) * 100.0)
    host['io_score'] = sum(float(x['io_percent'] * host['io_potential']) for x in host['workers'])
    nonSeepIo = host['io_percent'] - sum(float(x['io_percent']) for x in host['workers'])
    host['io_score'] += int(0 if nonSeepIo < 10 else host['potential'] * nonSeepIo)

  def selectCpuIntensiveWorkers(self, hosts, workers):
    hasSelected = False
    resourceReport = '---------------- CPU Resource Report --------------------\n'
    for host in hosts:
      resourceReport += '%s avg_cpu: %d cpu_score: %d op: %s\n' % (host['host'], host['avg_cpu'], host['cpu_score'], str(map(lambda x: {x['data.port']: x['cpu_percent']}, host['workers'])))

      # cpu score represent actual plus extra estimation utilization
      if not len(host['workers']) or host['cpu_score'] < self.config.getfloat('Scheduler', 'migration.from.score'):
        continue
      worker = max(host['workers'], key=lambda x: x['cpu_percent'])
      worker['cpu_score'] = int(worker['cpu_percent'] * host['potential'])
      if worker['cpu_score'] < 50:
        continue

      worker['source_cpu_score'] = host['cpu_score']
      worker['source_cpu_len'] = len(host['cpu'])
      worker['source'] = host['host']
      workers.append(worker)
      hasSelected = True

    if hasSelected:
      self.log.info(resourceReport)

  def selectIoIntensiveWorkers(self, hosts, workers):
    hasSelected = False
    resourceReport = '---------------- IO Resource Report --------------------\n'
    for host in hosts:
      resourceReport += '%s disk_io: %s net_io %s io_percent: %d io_score: %d op: %s\n' % (host['host'], str(host['disk_io']), str(host['net_io']), host['io_percent'], host['io_score'], str(map(lambda x: {x['data.port']: x['io_percent']}, host['workers'])))

      # cpu score represent actual plus extra estimation utilization
      if not len(host['workers']) or host['io_score'] < self.config.getfloat('Scheduler', 'migration.from.score'):
        continue
      worker = max(host['workers'], key=lambda x: x['io_percent'])
      worker['io_score'] = int(worker['io_percent'] * host['io_potential'])
      if worker['io_score'] < 50:
        continue

      worker['source_io_score'] = host['io_score']
      worker['source'] = host['host']
      workers.append(worker)
      hasSelected = True

    if hasSelected:
      self.log.info(resourceReport)

  def isCpuMigrationRequired(self, host, worker):
    # Calculate the effect of moving this job and see if it's worth it
    newPotential = self.estimatePotential(host['avg_cpu'] + worker['cpu_percent'] / len(host['cpu']))
    newWorkers = [x for x in host['workers']]
    newWorkers.append(worker)
    newCpuScoreDest = sum(float(x['cpu_percent'] * newPotential) / len(host['cpu']) for x in newWorkers)
    nonSeepCpu = host['avg_cpu'] - sum(float(x['cpu_percent']) / len(host['cpu']) for x in host['workers'])
    newCpuScoreDest += int(0 if nonSeepCpu < 10 else newPotential * nonSeepCpu)

    self.log.debug('CPU selection', worker['data.port'] + ':' + str(worker['cpu_percent']), worker['cpu_score'], worker['source'], '>', host['host'])
    self.log.info('src.cpu.score:', worker['source_cpu_score'], 'dest.cpu.score:', newCpuScoreDest, 'new.potential:', newPotential, 'non.seep.cpu:', nonSeepCpu, 'op:', [x['data.port'] for x in newWorkers])
    return worker['source_cpu_score'] - newCpuScoreDest > self.config.getfloat('Scheduler', 'min.movment.score.difference')

  def isIoMigrationRequired(self, host, worker):
    # Calculate the effect of moving this job and see if it's worth it
    newPotential = self.estimatePotential(host['io_percent'] + worker['io_percent'])
    newWorkers = [x for x in host['workers']]
    newWorkers.append(worker)
    newIoScoreDest = sum(float(x['io_percent'] * newPotential) for x in newWorkers)
    nonSeepIo = host['io_percent'] - sum(float(x['io_percent']) for x in host['workers'])
    newIoScoreDest += int(0 if nonSeepIo < 10 else newPotential * nonSeepIo)

    self.log.debug('IO selection', worker['data.port'] + ':' + str(worker['io_percent']), worker['io_score'], worker['source'], '>', host['host'])
    self.log.info('src.io.score:', worker['source_io_score'], 'dest.io.score:', newIoScoreDest, 'new.potential:', newPotential, 'op:', [x['data.port'] for x in newWorkers])
    return worker['source_io_score'] - newIoScoreDest > self.config.getfloat('Scheduler', 'min.movment.score.difference')

  def migrationRequest(self, host, worker):
    return {'id': worker['data.port'],
      'master.ip': worker.get('master.ip', None), 
      'cpu': worker['cpu_percent'], 'io': worker['io_percent'],
      'master.scheduler.port': worker.get('master.scheduler.port', None),
      'destination': host['host'], 'source': worker['source']}

  def schedule(self):
    # Assign some resource consumtion scores based on a greedy algorithm
    hosts = []
    for host in self.report.values():
      # This is reserved for scheduler, zookeeper, analytics
      if host['host'] in self.config.get('Basic', 'zookeeper.host'):
        continue
      self.computeCpuScore(host)
      self.computeIoScore(host)
      hosts.append(host)

    cpuWorkersToMove = []
    self.selectCpuIntensiveWorkers(hosts, cpuWorkersToMove)
    ioWorkersToMove = []
    self.selectIoIntensiveWorkers(hosts, ioWorkersToMove)

    hosts = sorted(hosts, key=lambda x: x['cpu_score'])
    cpuWorkersToMove = sorted(cpuWorkersToMove, key=lambda x: x['cpu_score'], reverse=True)
    for host in hosts:
      if host['cpu_score'] > self.config.getfloat('Scheduler', 'migration.to.score'):
        break
      while len(cpuWorkersToMove):
        worker = cpuWorkersToMove.pop()
        if not self.isCpuMigrationRequired(host, worker):
          continue
        request = self.migrationRequest(host, worker)
        self.dispatcher.add(request, 'CPU')
        break

    hosts = sorted(hosts, key=lambda x: x['io_score'])
    ioWorkersToMove = sorted(ioWorkersToMove, key=lambda x: x['io_score'], reverse=True)
    for host in hosts:
      if host['io_score'] > self.config.getfloat('Scheduler', 'migration.to.score'):
        break
      while len(ioWorkersToMove):
        worker = ioWorkersToMove.pop()
        if not self.isIoMigrationRequired(host, worker):
          continue
        request = self.migrationRequest(host, worker)
        self.dispatcher.add(request, 'IO')
        break

  def run(self):
    while self.working:
      startTime = time.time()
      self.getResourceReport()
      self.report = (self.updates[-1] if len(self.updates) else {})
      if self.config.getint('Scheduler', 'runtime.scheduling.enabled'):
        self.schedule()
      self.updates = []
      self.sleep(max(0, self.config.getfloat('Scheduler', 'scheduling.interval') - (time.time() - startTime)))

  def allocate(self):
    if not self.lastReport or len(self.lastReport) == 0:
      return None

    preferredNodes = []
    for host in self.lastReport.values():
      # If we have at least 3 hosts don't allocate on the current node
      if len(self.lastReport) > 3 and os.uname()[1] in host['host']:
        continue
      preferredNodes.append([int(host['avg_cpu'] / 10) + self.allocations.get(host['host'], 0), host['host']])
    preferredNodes.sort()
  
    node = preferredNodes[0][1]
    # Assume we will allocate a container already
    self.allocations[node] = self.allocations.get(node, 0) + 1
    self.log.info('Allocate on node: ', preferredNodes[0])
    return util.externalIp(self.config, node)