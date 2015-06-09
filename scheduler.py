import os
import sys
import json
import time
import math
import socket
import logger
import requests
import threading
import configparser

import Queue

from socket import gethostname
from bottle import Bottle, request

app = Bottle()

class RequestDispatcher(object):
  def __init__(self):
    self.working = True
    self.requests = Queue.Queue()
    self.postponedRequests = Queue.Queue()
    self.pending = {}
    self.estimate = {}
    self.lastSentPerHost = {}
    self.lastReceivedPerId = {}

  def stop(self):
    print 'Stoping request distpatcher thread...'
    self.working = False

  def setEstimation(self, src, dest, cpu, io):
    if not src in self.estimate:
      self.estimate[src] = {}
    if not dest in self.estimate:
      self.estimate[dest] = {}
    self.estimate[src]['cpu'] = self.estimate[src].get('cpu', 0) - cpu
    self.estimate[dest]['cpu'] = self.estimate[dest].get('cpu', 0) + cpu
    self.estimate[src]['io'] = self.estimate[src].get('io', 0) - io
    self.estimate[dest]['io'] = self.estimate[dest].get('io', 0) + io

  def getEstimation(self, host, resource):
    return (self.estimate[host][resource] if host in self.estimate else 0)

  def printRequest(self, request):
    return 'request W' + request['id'] + ' from ' + request['source'] + ' to ' + request['destination']

  def send(self, request):
    if request['master.scheduler.port'] in self.lastSentPerHost:
      timeDelta = int(time.time() - self.lastSentPerHost[request['master.scheduler.port']])
      if timeDelta < config.getint('Scheduler', 'scheduling.appmaster.interval'):
        log.info("Postpone Request to AppMaster", request['master.scheduler.port'], 'last was sent', timeDelta, 'seconds ago.')
        self.postponedRequests.put(request)
        return

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
      s.connect((request['master.ip'], int(request['master.scheduler.port'])))
      s.sendall('migrate,' + request['id'] + ',' + request['destination'] + ('.doc.res.ic.ac.uk' if 'wombat' in request['destination'] else ''))
      self.setEstimation(request['source'], request['destination'], request['cpu'], request['io'])
      log.info('Dispached', self.printRequest(request))
      request['time'] = time.time()
      self.lastSentPerHost[request['master.scheduler.port']] = request['time']
      self.pending[request['id']] = request
    except:
      log.warn('Request', request, 'failed!')
      log.error(sys.exc_info())
    finally:
      s.close()

  def add(self, request, reason):
    if request['id'] in self.pending:
      return
    if request['id'] in self.lastReceivedPerId:
      timeDelta = int(time.time() - self.lastReceivedPerId[request['id']])
      if timeDelta < config.getint('Scheduler', 'scheduling.operator.interval'):
        return
    self.lastReceivedPerId[request['id']] = time.time()
    self.requests.put(request)
    log.info('Received', reason, 'request', self.printRequest(request))

  def checkPending(self, report):
    widToHostMap = {}
    for host in report.values():
      for worker in host['workers']:
        widToHostMap[worker['data.port']] = host['host']

    for wid in self.pending.keys():
      request = self.pending[wid]
      dest = request['destination']
      # If we see the worker running on the destination host, remove the request
      if wid in widToHostMap and widToHostMap[wid] == dest:
        timeDelta = int(time.time() - request['time'])
        self.setEstimation(request['source'], request['destination'], -request['cpu'], -request['io'])
        log.info('Completed', self.printRequest(request), 'after', timeDelta, 'seconds')
        del self.pending[wid]

    for wid in self.pending.keys():
      request = self.pending[wid]
      timeDelta = int(time.time() - request['time'])
      if timeDelta > 30:
        self.setEstimation(request['source'], request['destination'], -request['cpu'], -request['io'])
        log.warn('Failed', self.printRequest(request))
        del self.pending[wid]

  def run(self):
    while self.working:
      while not self.requests.empty():
        request = self.requests.get()
        self.send(request)
      while not self.postponedRequests.empty():
        self.requests.put(self.postponedRequests.get())
      time.sleep(1)

class Scheduler(object):
  def __init__(self):
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
    log.info('Stoping scheduler thread...')
    self.working = False

  def resetAllocations(self):
    self.allocations = {}

  def getResourceReport(self):
    try:
      res = requests.get('http://' + gethostname() + ':' + str(config.getint('Basic', 'server.port')) + '/command/scheduler_report')
      if res.text == 'pending':
        return
      report = json.loads(res.text)['hosts']
      self.lastReport = report
      dispatcher.checkPending(report)
      self.updates.append(report)
    except requests.ConnectionError:
      log.warn("Failed to get resource report!")

  def estimatePotential(self, percentage):
    x = (1 + percentage / 100.0)
    y = math.pow(math.e, config.getfloat('Scheduler', 'potential.lambda') * x) / 10
    return min(max(y, 1), 2)

  def computeCpuScore(self, host):
    host['avg_cpu'] = host['avg_cpu'] + dispatcher.getEstimation(host['host'], 'cpu') / len(host['cpu'])
    host['potential'] = self.estimatePotential(host['avg_cpu'])
    host['cpu_score'] = sum(float(x['cpu_percent'] + host['potential']) / len(host['cpu']) for x in host['workers'])
    nonSeepCpu = host['avg_cpu'] - sum(float(x['cpu_percent']) / len(host['cpu']) for x in host['workers'])
    host['cpu_score'] += int(0 if nonSeepCpu < 10 else host['potential'] * nonSeepCpu)

  def computeIoScore(self, host):
    totalIo = (sum(host['disk_io']) + sum(host['net_io'])) / 2.0
    host['io_percent'] = max(100, int(100 * sum(host['disk_io']) / config.getint('Scheduler', 'max.disk.io.host') + 100 * sum(host['net_io']) / config.getint('Scheduler', 'max.net.io.host')) / 2)
    host['io_percent'] = host['io_percent'] + dispatcher.getEstimation(host['host'], 'io')
    host['io_potential'] = self.estimatePotential(host['io_percent'])
    for worker in host['workers']:
      worker['io_percent'] = int((((sum(worker['disk_io']) + sum(worker['net_io'])) / 2.0) / totalIo) * 100.0)
    host['io_score'] = sum(float(x['io_percent'] + host['io_potential']) for x in host['workers'])
    nonSeepIo = host['io_percent'] - sum(float(x['io_percent']) for x in host['workers'])
    host['io_score'] += int(0 if nonSeepIo < 10 else host['potential'] * nonSeepIo)

  def selectCpuIntensiveWorkers(self, hosts, workers):
    hasSelected = False
    resourceReport = '---------------- CPU Resource Report --------------------\n'
    for host in hosts:
      resourceReport += '%s avg_cpu: %d cpu_score: %d op: %s\n' % (host['host'], host['avg_cpu'], host['cpu_score'], str(map(lambda x: {x['data.port']: x['cpu_percent']}, host['workers'])))

      # cpu score represent actual plus extra estimation utilization
      if not len(host['workers']) or host['cpu_score'] < config.getint('Scheduler', 'migration.from.score'):
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
      log.info(resourceReport)

  def selectIoIntensiveWorkers(self, hosts, workers):
    hasSelected = False
    resourceReport = '---------------- IO Resource Report --------------------\n'
    for host in hosts:
      resourceReport += '%s disk_io: %s net_io %s io_percent: %d io_score: %d op: %s\n' % (host['host'], str(host['disk_io']), str(host['net_io']), host['io_percent'], host['io_score'], str(map(lambda x: {x['data.port']: x['io_percent']}, host['workers'])))

      # cpu score represent actual plus extra estimation utilization
      if not len(host['workers']) or host['io_score'] < config.getint('Scheduler', 'migration.from.score'):
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
      log.info(resourceReport)

  def isCpuMigrationRequired(self, host, worker):
    # Calculate the effect of moving this job and see if it's worth it
    newPotential = self.estimatePotential(host['avg_cpu'] + worker['cpu_percent'] / len(host['cpu']))
    newWorkers = [x for x in host['workers']]
    newWorkers.append(worker)
    newCpuScoreDest = sum(float(x['cpu_percent'] * newPotential) / len(host['cpu']) for x in newWorkers)
    nonSeepCpu = host['avg_cpu'] - sum(float(x['cpu_percent']) / len(host['cpu']) for x in host['workers'])
    newCpuScoreDest += int(0 if nonSeepCpu < 10 else newPotential * nonSeepCpu)

    log.debug('CPU selection', worker['data.port'] + ':' + str(worker['cpu_percent']), worker['cpu_score'], worker['source'], '>', host['host'])
    log.info('src.cpu.score:', worker['source_cpu_score'], 'dest.cpu.score:', newCpuScoreDest, 'new.potential:', newPotential, 'non.seep.cpu:', nonSeepCpu, 'op:', [x['data.port'] for x in newWorkers])
    return worker['source_cpu_score'] - newCpuScoreDest > config.getint('Scheduler', 'min.movment.score.difference')

  def isIoMigrationRequired(self, host, worker):
    # Calculate the effect of moving this job and see if it's worth it
    newPotential = self.estimatePotential(host['io_percent'] + worker['io_percent'])
    newWorkers = [x for x in host['workers']]
    newWorkers.append(worker)
    newIoScoreDest = sum(float(x['io_percent'] * newPotential) for x in newWorkers)
    nonSeepIo = host['io_percent'] - sum(float(x['io_percent']) for x in host['workers'])
    newIoScoreDest += int(0 if nonSeepIo < 10 else newPotential * nonSeepIo)

    log.debug('IO selection', worker['data.port'] + ':' + str(worker['io_percent']), worker['io_score'], worker['source'], '>', host['host'])
    log.info('src.io.score:', worker['source_io_score'], 'dest.io.score:', newIoScoreDest, 'new.potential:', newPotential, 'op:', [x['data.port'] for x in newWorkers])
    return worker['source_io_score'] - newIoScoreDest > config.getint('Scheduler', 'min.movment.score.difference')

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
      if 'wombat07' in host['host']:
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
      if host['cpu_score'] > config.getint('Scheduler', 'migration.to.score'):
        break
      while len(cpuWorkersToMove):
        worker = cpuWorkersToMove.pop()
        if not self.isCpuMigrationRequired(host, worker):
          continue
        request = self.migrationRequest(host, worker)
        dispatcher.add(request, 'CPU')
        break

    hosts = sorted(hosts, key=lambda x: x['io_score'])
    ioWorkersToMove = sorted(ioWorkersToMove, key=lambda x: x['io_score'], reverse=True)
    for host in hosts:
      if host['io_score'] > config.getint('Scheduler', 'migration.to.score'):
        break
      while len(ioWorkersToMove):
        worker = ioWorkersToMove.pop()
        if not self.isIoMigrationRequired(host, worker):
          continue
        request = self.migrationRequest(host, worker)
        dispatcher.add(request, 'IO')
        break

  def run(self):
    while self.working:
      self.getResourceReport()
      self.report = (self.updates[-1] if len(self.updates) else {})
      if config.getint('Scheduler', 'runtime.scheduling.enabled'):
        self.schedule()
      self.updates = []
      self.sleep(config.getint('Scheduler', 'scheduling.interval'))

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
    log.info('Allocate on node: ', preferredNodes[0])
    return node + ('.doc.res.ic.ac.uk' if 'wombat' in node else '')

def computeBusyScore(data):
  return data['avg_cpu']

@app.route('/scheduler/host')
def scheduler_host():
  if config.getint('Scheduler', 'startup.scheduling.type') == 0:
    return ('wombat01.doc.res.ic.ac.uk' if 'wombat' in os.uname()[1] else os.uname()[1])
  elif config.getint('Scheduler', 'startup.scheduling.type') == 1:
    return None
  else:
    return scheduler.allocate()

@app.route('/command/set_config', method='post')
def server_set_config():
  name = request.forms.get('name')
  value = request.forms.get('value')
  log.info('Set config', name, value)
  config.set('Scheduler', name, value)

@app.route('/command/reset/allocations')
def server_reset_allocations():
  log.info("Reset allocations")
  scheduler.resetAllocations()

@app.route('/ping')
def server_ping():
  return 'ok'

def update_configs():
  try:
    res = requests.get('http://' + gethostname() + ':' + str(config.getint('Basic', 'server.port')) + '/command/get_config')
  except requests.ConnectionError:
    log.warn("Failed to get latest configs!")
    return
  data = json.loads(res.text)
  for name, value in data['Scheduler'].iteritems():
    config.set('Scheduler', name, value)
    log.info('Init config from server', name, value)

if __name__ == "__main__":
  scheduler = Scheduler()
  dispatcher = RequestDispatcher()
  log = logger.Logger('Scheduler')
  config = configparser.SafeConfigParser()
  config.read('analytics.properties')
  update_configs()
  t = threading.Thread(target=scheduler.run)
  t.deamon = True
  t.start()
  t = threading.Thread(target=dispatcher.run)
  t.deamon = True
  t.start()

app.run(host=gethostname(), port=config.getint('Basic', 'scheduler.port'), reloader=False, quiet=True)
scheduler.stop()
dispatcher.stop()
