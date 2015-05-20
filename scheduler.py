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

  def stop(self):
    print 'Stoping request distpatcher thread...'
    self.working = False

  def send(self, request):
    #s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
      #s.connect((request['master.ip'], int(request['master.scheduler.port'])))
      if 'wombat' in request['destination'] and not '.doc.res.ic.ac.uk' in request['destination']:
        request['destination'] += '.doc.res.ic.ac.uk'
      #s.sendall('migrate,' + request['id'] + ',' + request['destination'])
      print 'Dispached request to move W' + request['id'] + ' from ' + request['source'] + ' to ' + request['destination'] 
    except:
      print 'Request', request, 'failed!'
      print sys.exc_info()
    finally:
      #s.close()
      pass

  def add(self, request):
    self.requests.put(request)
    print 'Received request to move W' + request['id'] + ' from ' + request['source'] + ' to ' + request['destination']

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
    self.updates.append(report)

  def estimatePotential(self, cpu):
    return max(0, (cpu - 60))

  def schedule(self):
    # Assign some greedy resource consumtion scores
    hosts = []
    for hostName in self.report:
      # This is reserved for scheduler, zookeeper, analytics
      if 'wombat07' in hostName:
        continue

      host = self.report[hostName]
      host['potential'] = self.estimatePotential(host['avg_cpu'])
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
        'master.ip': worker['master.ip'],
        'master.scheduler.port': worker['master.scheduler.port'],
        'destination': host['host'], 'source': worker['source']}
      dispatcher.add(request)

  def run(self):
    while self.working:
      self.report = (self.updates[-1] if len(self.updates) else [])
      self.schedule()
      self.updates = []
      self.sleep(5)

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