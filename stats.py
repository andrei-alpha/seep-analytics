import time
import json
import requests

import Queue

class StatsComputer(object):
  def __init__(self, host):
    self.working = True
    self.host = host
    self.events = Queue.Queue()
    self.before = {}

  def addEvent(self, name, wid, timeDelta=0):
    self.events.put([name, wid, time.time() + timeDelta])   

  def stop(self):
    log.info('Stoping stats thread...')
    self.working = False

  def report(self, wid, data):
    # Report empty metrics
    if not wid in self.before:
      self.before[wid] = data

    cpuDelta = data['cpu'] - self.before[wid]['cpu']
    memDelta = data['mem'] - self.before[wid]['mem']
    diskDelta = map(lambda x,y: x - y, data['disk'], self.before[wid]['disk'])
    netDelta = map(lambda x,y: x - y, data['net'], self.before[wid]['net'])

    print '--- Report for ', wid, ' -------'
    print cpuDelta, memDelta, diskDelta, netDelta

    requests.get('http://wombat07.doc.res.ic.ac.uk:9999/new/cpuDelta/' + cpuDelta)
    requests.get('http://wombat07.doc.res.ic.ac.uk:9999/new/cpuDelta/' + memDelta)
    requests.get('http://wombat07.doc.res.ic.ac.uk:9999/new/diskDeltaRead/' + diskDelta[0])
    requests.get('http://wombat07.doc.res.ic.ac.uk:9999/new/diskDeltaWrite/' + diskDelta[1])
    requests.get('http://wombat07.doc.res.ic.ac.uk:9999/new/netDeltaSend/' + netDelta[0])
    requests.get('http://wombat07.doc.res.ic.ac.uk:9999/new/netDeltaReceive/' + netDelta[1])

  def query(self, name, wid):
    r1 = requests.get('http://' + self.host + '/backend/dataset')
    r2 = requests.get('http://' + self.host + '/command/resource_report')
    
    d1 = json.loads(r1.text)
    d2 = json.loads(r2.text)

    metrics = None
    resources = None
    for container in d1['containers'].values():
      if 'data.port' in container and container['data.port'] == wid:
        metrics = container['data'][-1]['1-minute rate']
        break
    for host in d2['hosts'].values():
      for worker in host['workers']:
        if 'data.port' in worker and worker['data.port'] == wid:
          resources = worker
          break

    data = {
      'metrics': metrics,
      'cpu': resources.get('cpu_percent', 0),
      'mem': resources.get('memory_percent', 0),
      'disk': resources.get('disk_io', [0,0]),
      'net': resources.get('net_io', [0,0])
    }

    if name == 'before':
      self.before[wid] = data
    else:
      self.report(wid, data)

  def run(self):
    while self.working:
      while not self.events.empty():
        event = self.events.get()

        # Wait for it
        if time.time() < event[2]:
          time.sleep(max(0, event[2] - time.time() - event[2]))
        self.query(event[0], event[1])
      time.sleep(0.5)
