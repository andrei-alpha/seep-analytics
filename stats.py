import time
import json
import requests

import Queue

class StatsComputer(object):
  def __init__(self, log, host):
    self.working = True
    self.log = log
    self.host = host
    self.events = Queue.PriorityQueue()
    self.before = {}

  def addEvent(self, name, wid, timeDelta=0):
    self.events.put([time.time() + timeDelta, name, wid])   

  def stop(self):
    self.log.info('Stoping stats thread...')
    self.working = False

  def report(self, wid, data):
    # Report empty metrics
    if not wid in self.before:
      self.before[wid] = data


    metrics = data['metrics'] - self.before[wid]['metrics']
    cpuDelta = data['cpu'] - self.before[wid]['cpu']
    memDelta = data['mem'] - self.before[wid]['mem']
    diskDelta = map(lambda x,y: x - y, data['disk'], self.before[wid]['disk'])
    netDelta = map(lambda x,y: x - y, data['net'], self.before[wid]['net'])

    print '--- Report for ', wid, ' -------'
    print cpuDelta, memDelta, diskDelta, netDelta, metrics

    requests.get('http://wombat07.doc.res.ic.ac.uk:9999/new/metrics/' + str(metrics))
    requests.get('http://wombat07.doc.res.ic.ac.uk:9999/new/cpuDelta/' + str(cpuDelta))
    requests.get('http://wombat07.doc.res.ic.ac.uk:9999/new/memDelta/' + str(memDelta))
    requests.get('http://wombat07.doc.res.ic.ac.uk:9999/new/diskDeltaRead/' + str(diskDelta[0]))
    requests.get('http://wombat07.doc.res.ic.ac.uk:9999/new/diskDeltaWrite/' + str(diskDelta[1]))
    requests.get('http://wombat07.doc.res.ic.ac.uk:9999/new/netDeltaSend/' + str(netDelta[0]))
    requests.get('http://wombat07.doc.res.ic.ac.uk:9999/new/netDeltaReceive/' + str(netDelta[1]))

  def query(self, name, wid):
    try:
      r1 = requests.get('http://' + self.host + '/backend/dataset')     
      r2 = requests.get('http://' + self.host + '/command/scheduler_report')
    except:
      return

    d1 = json.loads(r1.text)
    d2 = json.loads(r2.text)

    ts = 0
    metrics = 0
    resources = {}
    for container in d1['containers'].values():
      if 'data.port' in container and container['data.port'] == str(wid):
        if len(container['data']) and int(container['data'][-1]['time']) > ts:
          print name, wid, 'PARSE', container['data'][-1]['1-minute rate']
          metrics = float(container['data'][-1]['1-minute rate'])
          ts = int(container['data'][-1]['time'])
    for host in d2['hosts'].values():
      for worker in host['workers']:
        if 'data.port' in worker and worker['data.port'] == str(wid):
          resources = worker
          break

    if metrics == 0:
      print name, wid, 'CANNOT PARSE METRICS'

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
      while self.working and not self.events.empty():
        event = self.events.get()

        # Wait for it
        if time.time() < event[0]:
          self.events.put(event)
          time.sleep(2)
          continue
        self.query(event[1], event[2])
      time.sleep(0.4)
