import sys
import time
import util
import Queue
import socket

class Dispatcher(object):
  def __init__(self, config, log):
    self.config = config
    self.log = log
    self.working = True
    self.requests = Queue.Queue()
    self.postponedRequests = Queue.Queue()
    self.pending = {}
    self.estimate = {}
    self.lastSentPerHost = {}
    self.lastReceivedPerId = {}

  def stop(self):
    self.log.info('Stoping request distpatcher thread...')
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
      if timeDelta < self.config.getfloat('Scheduler', 'scheduling.appmaster.interval'):
        self.log.info("Postpone Request to AppMaster", request['master.scheduler.port'], 'last was sent', timeDelta, 'seconds ago.')
        self.postponedRequests.put(request)
        return

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
      s.connect((request['master.ip'], int(request['master.scheduler.port'])))
      s.sendall('migrate,' + request['id'] + ',' + util.externalIp(self.config, request['destination']))
      self.setEstimation(request['source'], request['destination'], request['cpu'], request['io'])
      self.log.info('Dispached', self.printRequest(request))
      request['time'] = time.time()
      self.lastSentPerHost[request['master.scheduler.port']] = request['time']
      self.pending[request['id']] = request
    except:
      self.log.warn('Request', request, 'failed!')
      self.log.error(sys.exc_info())
    finally:
      s.close()

  def add(self, request, reason):
    if request['id'] in self.pending:
      return
    if request['id'] in self.lastReceivedPerId:
      timeDelta = int(time.time() - self.lastReceivedPerId[request['id']])
      if timeDelta < self.config.getfloat('Scheduler', 'scheduling.operator.interval'):
        return
    self.lastReceivedPerId[request['id']] = time.time()
    self.requests.put(request)
    self.log.info('Received', reason, 'request', self.printRequest(request))

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
        self.log.info('Completed', self.printRequest(request), 'after', timeDelta, 'seconds')
        del self.pending[wid]

    for wid in self.pending.keys():
      request = self.pending[wid]
      timeDelta = int(time.time() - request['time'])
      if timeDelta > 30:
        self.setEstimation(request['source'], request['destination'], -request['cpu'], -request['io'])
        self.log.warn('Failed', self.printRequest(request))
        del self.pending[wid]

  def run(self):
    while self.working:
      while not self.requests.empty():
        request = self.requests.get()
        self.send(request)
      while not self.postponedRequests.empty():
        self.requests.put(self.postponedRequests.get())
      time.sleep(1)