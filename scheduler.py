import os
import sys
import json
import time
import bottle
import socket
import logger
import requests
import threading
import configparser

import Queue

from socket import gethostname
from bottle import Bottle, response, request

app = Bottle()

class RequestDispatcher:
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

  def setEstimation(self, src, dest, value):
    self.estimate[src] = (self.estimate[src] if src in self.estimate else 0) - value
    self.estimate[dest] = (self.estimate[dest] if dest in self.estimate else 0) + value

  def getEstimation(self, host):
    return (self.estimate[host] if host in self.estimate else 0)

  def printRequest(self, request):
    return 'request W' + request['id'] + ' from ' + request['source'] + ' to ' + request['destination']

  def send(self, request):
    if request['master.scheduler.port'] in self.lastSentPerHost:
      timeDelta = int(time.time() - self.lastSentPerHost[request['master.scheduler.port']])
      if timeDelta < config.getint('Scheduler', 'scheduling.appmaster.interval'):
        print config.getint('Scheduler', 'scheduling.appmaster.interval'), self.lastSentPerHost[request['master.scheduler.port']], time.time()
        log.info("Postpone Request to AppMaster", request['master.scheduler.port'], 'last was sent', timeDelta, 'seconds ago.')
        self.postponedRequests.put(request)
        return

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
      s.connect((request['master.ip'], int(request['master.scheduler.port'])))
      s.sendall('migrate,' + request['id'] + ',' + request['destination'] + ('.doc.res.ic.ac.uk' if 'wombat' in request['destination'] else ''))
      self.setEstimation(request['source'], request['destination'], request['value'])
      log.info('Dispached', self.printRequest(request))
      request['time'] = time.time()
      self.lastSentPerHost[request['master.scheduler.port']] = request['time']
      self.pending[request['id']] = request
    except:
      log.warn('Request', request, 'failed!')
      log.error(sys.exc_info())
    finally:
      s.close()
      pass

  def add(self, request):
    if request['id'] in self.pending:
      return
    if request['id'] in self.lastReceivedPerId:
      timeDelta = int(time.time() - self.lastReceivedPerId[request['id']])
      if timeDelta < config.getint('Scheduler', 'scheduling.operator.interval'):
        return
    self.lastReceivedPerId[request['id']] = time.time()
    self.requests.put(request)
    log.info('Received', self.printRequest(request))

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
        self.setEstimation(request['source'], request['destination'], -request['value'])
        log.info('Completed', self.printRequest(request), 'after', timeDelta, 'seconds')
        del self.pending[wid]

    for wid in self.pending.keys():
      request = self.pending[wid]
      timeDelta = int(time.time() - request['time'])
      if timeDelta > 30:
        self.setEstimation(request['source'], request['destination'], -request['value'])
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

class Scheduler:
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

  def reportResources(self, report):
    self.lastReport = report
    dispatcher.checkPending(report)
    self.updates.append(report)

  def estimatePotential(self, avg_cpu):
    return max(0, avg_cpu - 60)

  def schedule(self):
    # Assign some resource consumtion scores based on a greedy algorithm
    hosts = []
    for host in self.report.values():
      # This is reserved for scheduler, zookeeper, analytics
      if 'wombat07' in host['host']:
        continue
      host['avg_cpu'] = host['avg_cpu'] + dispatcher.getEstimation(host['host']) / len(host['cpu'])
      host['potential'] = self.estimatePotential(host['avg_cpu'])
      host['cpu_score'] = sum(float(x['cpu_percent'] + host['potential']) / len(host['cpu']) for x in host['workers'])
      nonSeepCpu = host['avg_cpu'] - sum(float(x['cpu_percent']) / len(host['cpu']) for x in host['workers'])
      host['cpu_score'] += (0 if nonSeepCpu < 10 else host['potential']) + nonSeepCpu
      hosts.append(host)

    workersToMove = []
    for host in hosts:
      log.debug(host['host'], 'avg_cpu:', host['avg_cpu'], 'cpu_score:', host['cpu_score'], map(lambda x: {x['data.port']: x['cpu_percent']}, host['workers']))

      # cpu score represent actual plus extra estimation utilization
      if not len(host['workers']) or host['cpu_score'] < config.getint('Scheduler', 'migration.from.score'):
        continue
      worker = max(host['workers'], key=lambda x: x['cpu_percent'])
      worker['cpu_score'] = worker['cpu_percent'] + host['potential']
      if worker['cpu_score'] < 50:
        continue

      worker['source_cpu_score'] = host['cpu_score']
      worker['source_cpu_len'] = len(host['cpu'])
      worker['source'] = host['host']
      workersToMove.append(worker)
      log.debug('selected', worker['data.port'] + ':' + str(worker['cpu_percent']), worker['cpu_score'])

    hosts = sorted(hosts, key=lambda x: x['cpu_score'])
    workersToMove = sorted(workersToMove, key=lambda x: x['cpu_score'], reverse=True)
      
    for host in hosts:
      if host['cpu_score'] > config.getint('Scheduler', 'migration.to.score'):
        break
      while len(workersToMove):
        worker = workersToMove.pop()
        # Calculate the effect of moving this job and see if it's worth it
        newPotential = self.estimatePotential(host['avg_cpu'] + worker['cpu_percent'] / len(host['cpu']))
        newWorkers = [x for y in zip(host['workers'],[worker]) for x in y]
        newCpuScoreDest = sum(float(x['cpu_percent'] + newPotential) / len(host['cpu']) for x in newWorkers)
        nonSeepCpu = host['avg_cpu'] - sum(float(x['cpu_percent']) / len(host['cpu']) for x in newWorkers)
        newCpuScoreDest += (0 if nonSeepCpu < 10 else newPotential) + nonSeepCpu
        newCpuScoreSrc = worker['source_cpu_score'] - worker['cpu_score'] / worker['source_cpu_len']
        if worker['source_cpu_score'] - newCpuScoreDest < config.getint('Scheduler', 'min.movment.score.difference'):
          continue

        request = {'id': worker['data.port'],
          'master.ip': worker.get('master.ip', None), 'value': worker['cpu_percent'],
          'master.scheduler.port': worker.get('master.scheduler.port', None),
          'destination': host['host'], 'source': worker['source']}
        dispatcher.add(request)
        break

  def run(self):
    while self.working:
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
      preferredNodes.append([int(host['avg_cpu'] / 20), self.allocations.get(host['host'], 0), host['host']])
    preferredNodes.sort()
  
    node = preferredNodes[0][2]
    # Assume we will allocate a container already
    self.allocations[node] =  self.allocations.get(node, 0) + 1
    log.info('Allocate on node: ', preferredNodes[0])
    return node + ('.doc.res.ic.ac.uk' if 'wombat' in node else '')

@app.route('/scheduler/event', method='post')
def scheduler_evet():
  data = json.loads(request.forms.get('data'))
  scheduler.reportResources(data['hosts'])

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
  section = request.forms.get('section')
  name = request.forms.get('name')
  value = request.forms.get('value')
  config.set('Scheduler', name, value)

if __name__ == "__main__":
  scheduler = Scheduler()
  dispatcher = RequestDispatcher()
  log = logger.Logger('Scheduler')
  config = configparser.SafeConfigParser()
  config.read('analytics.properties')
  t = threading.Thread(target=scheduler.run)
  t.deamon = True
  t.start()
  t = threading.Thread(target=dispatcher.run)
  t.deamon = True
  t.start()

app.run(host=gethostname(), port=config.getint('Basic', 'scheduler.port'), reloader=False, quiet=True)
scheduler.stop()
dispatcher.stop()