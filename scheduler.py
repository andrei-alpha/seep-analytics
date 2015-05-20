import os
import sys
import json
import time
import bottle
import socket
import requests
import threading

import Queue

from socket import gethostname
from bottle import Bottle, response, request

app = Bottle()

class RequestDispatcher:
  def __init__(self):
    self.working = True
    self.requests = Queue.Queue()
    self.pending = {}
    self.estimate = {}

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
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
      s.connect((request['master.ip'], int(request['master.scheduler.port'])))
      if 'wombat' in request['destination'] and not '.doc.res.ic.ac.uk' in request['destination']:
        request['destination'] += '.doc.res.ic.ac.uk'
      s.sendall('migrate,' + request['id'] + ',' + request['destination'])
      self.setEstimation(request['source'], request['destination'], request['value'])
      print 'Dispached', self.printRequest(request)
      request['time'] = time.time()
      self.pending[request['id']] = request
    except:
      print 'Request', request, 'failed!'
      print sys.exc_info()
    finally:
      s.close()
      pass

  def add(self, request):
    if request['id'] in self.pending:
      return
    self.requests.put(request)
    print 'Received', self.printRequest(request)

  def checkPending(self, report):
    widToHostMap = {}
    for host in report.values():
      for worker in host['workers']:
        widToHostMap[worker['data.port']] = host['host']

    for wid in self.pending.keys():
      request = self.pending[wid]
      dest = request['destination']
      # If we see the worker running on the destination host, remove the request
      if wid in widToHostMap[dest]:
        timeDelta = int(time.time() - request['time'])
        self.setEstimation(request['source'], request['destination'], -request['value'])
        print 'Completed', self.printRequest(request), 'after', timeDelta, 'seconds'
        del self.pending[wid]

  def run(self):
    while self.working:
      while not self.requests.empty():
        request = self.requests.get()
        self.send(request)
      time.sleep(1)

class Scheduler:
  def __init__(self):
    self.report = {}
    self.updates = []
    self.working = True

  def sleep(self, seconds):
    while self.working and seconds > 0:
      time.sleep(1)
      seconds -= 1

  def stop(self):
    print 'Stoping scheduler thread...'
    self.working = False

  def reportResources(self, report):
    dispatcher.checkPending(report)
    self.updates.append(report)

  def estimatePotential(self, host):
    return max(0, (host['avg_cpu'] + dispatcher.getEstimation(host['host']) / len(host['cpu']) - 60))

  def schedule(self):
    # Assign some resource consumtion scores based on a greedy algorithm
    hosts = []
    for host in self.report.values():
      # This is reserved for scheduler, zookeeper, analytics
      if 'wombat07' in host['host']:
        continue
      host['potential'] = self.estimatePotential(host)
      host['cpu_score'] = sum(float(x['cpu_percent'] + host['potential']) / len(host['cpu']) for x in host['workers'])
      nonSeepCpu = host['avg_cpu'] - sum(float(x['cpu_percent']) / len(host['cpu']) for x in host['workers'])
      host['cpu_score'] += (0 if nonSeepCpu > 5 else host['potential']) + nonSeepCpu
      hosts.append(host)

    workersToMove = []
    for host in hosts:
      print host['host'], 'avg_cpu:', host['avg_cpu'], 'cpu_score:', host['cpu_score'], map(lambda x: {x['data.port']: x['cpu_percent']}, host['workers'])

      # cpu score represent actual plus extra estimation utilization
      if host['cpu_score'] < 100:
        continue
      worker = max(host['workers'], key=lambda x: x['cpu_percent'])
      worker['cpu_score'] = worker['cpu_percent'] + host['potential']
      worker['source'] = host['host']
      workersToMove.append(worker)

      print 'selected', worker['data.port'] + ':' + str(worker['cpu_percent']), worker['cpu_score']

    hosts = sorted(hosts, key=lambda x: x['cpu_score'], reverse=True)
    workersToMove = sorted(workersToMove, key=lambda x: x['cpu_score'])
      
    for host in hosts:
      if host['cpu_score'] > 95 or not len(workersToMove):
        break
      worker = workersToMove.pop()
      request = {'id': worker['data.port'],
        'master.ip': worker['master.ip'], 'value': worker['cpu_percent'],
        'master.scheduler.port': worker['master.scheduler.port'],
        'destination': host['host'], 'source': worker['source']}
      dispatcher.add(request)

  def run(self):
    while self.working:
      self.report = (self.updates[-1] if len(self.updates) else {})
      self.schedule()
      self.updates = []
      self.sleep(2)

@app.route('/scheduler/event', method='post')
def scheduler_evet():
  data = json.loads(request.forms.get('data'))
  scheduler.reportResources(data['hosts'])

def computeBusyScore(data):
  return data['avg_cpu']

@app.route('/scheduler/host')
def scheduler_host():
  '''preferredNodes = []
  cInfo = Globals.clusterInfo['hosts']
  if len(Globals.clusterInfo['hosts']) == 0:
    return '*'

  for host in cInfo:
    # If we have at least 3 hosts don't allocate on the current node
    if len(cInfo) > 3 and os.uname()[1] in host:
      continue
    preferredNodes.append([cInfo[host]['score'], host])
  preferredNodes.sort()
  
  node = preferredNodes[0][1]
  # Assume we will allocate a container already
  cInfo[node]['allocations'] += 1
  cInfo[node]['score'] = computeBusyScore(cInfo[node])
  return node + ('.doc.res.ic.ac.uk' if 'wombat' in node else '')'''
  return 'wombat01.doc.res.ic.ac.uk'

scheduler = Scheduler()
dispatcher = RequestDispatcher()

if __name__ == "__main__":
  t = threading.Thread(target=scheduler.run)
  t.deamon = True
  t.start()
  t = threading.Thread(target=dispatcher.run)
  t.deamon = True
  t.start()

app.run(host=gethostname(), port=7009, reloader=False)
scheduler.stop()
dispatcher.stop()