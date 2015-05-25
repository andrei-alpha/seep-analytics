import configparser
import json
import psutil
import logger
import os
import re
import sys
import select
import subprocess
import requests
import time
import threading

from socket import gethostname
from bottle import Bottle, response, request

app = Bottle()
globalWatcherThread = None
globalResourceThread = None
globalProcess = None

# This function will be called from Watcher or Resource class
def send(self, payload):
  success = False
  pause = 1
  while not success and self.working:
    try:
      requests.post(self.host + '/event', data=payload)
      success = True
    except requests.ConnectionError:
      log.debug('Connection problems. Retrying in', pause, ' sec..')
      self.sleep(pause)
      pause = min(60, pause * 2)

class WatcherThread:
  def __init__(self, host, path, prefix):
    self.host = host
    self.path = path
    self.prefix = prefix  

    self.index = {}
    self.skip_patterns = ['DEBUG', 'TRACE']

  def args(self):
    return (self.host, self.path, self.prefix)

  def update(self, path):
    if path not in self.index:
      #print 'created:', path
      payload = {'event': 'created', 'path': path[len(self.prefix):], 'data': ''}
      send(self, payload)
      self.index[path] = 0

    size = os.path.getsize(path)
    if size > self.index[path]:
      with open(path, 'r') as fin:
        fin.seek(self.index[path])
        data = fin.read()

        filteredData = data
        for pattern in self.skip_patterns:
          filteredData = re.sub('[0-9]+:[0-9][^\n=]*' + pattern + '[^\n]*\n', '', filteredData)
        data = filteredData

        #print 'new data:', path, len(data)
        while len(data) and self.working:
          batch = data[:500000]
          data = data[len(batch):]
          payload = {'event': 'new line', 'path': path[len(self.prefix):], 'data': batch}
          send(self, payload)

      self.index[path] = size

  def scan(self):
    for root, dirs, files in os.walk(self.path):
      for file in files:
        self.update(root + '/' + file)

  def run(self):
    self.working = True
    while self.working:
      self.scan()
      self.sleep(config.getint('monitor.logs.scan.interval'))

  def stop(self):
    log.info('Stoping watcher thread...')
    self.working = False

  def sleep(self, seconds):
    cnt = 0
    while self.working and cnt < seconds:
      time.sleep(1)
      cnt += 1

class ResourceThread:
  def __init__(self, host, seep_root):
    self.info = {}
    self.host = host
    self.seep_root = seep_root
    self.procs = set()

  def args(self):
    return (self.host, self.seep_root)

  def getJVMArgs(self, out, pid):
    args = filter(lambda x: re.search('^' + str(pid) + '\s', x), out)
    if len(args):
      return args[0].split(' ')
    return ''

  def scanWorkers(self):
    workers = []
    pids = []
    for proc in psutil.process_iter():
      try:
        if proc.name() == 'java' and not proc in self.procs:
          pids.append(proc.pid)
      except psutil.NoSuchProcess:
        pass

    for pid in pids:
      proc = psutil.Process(pid)
      try:
        pinfo = proc.as_dict(attrs=['cmdline'])
      except psutil.NoSuchProcess:
        pass
      else:
        cmdline = pinfo['cmdline']
        if not cmdline or len(cmdline) == 0:
          continue

        cntWorker = sum([len(re.findall('seep-worker', x)) for x in cmdline])
        # We can be sure it's a worker with high confidence
        if cntWorker > 20:
          self.procs.add(proc)

    procsToRemove = []
    jvmOutput = subprocess.check_output(['jps', '-m']).split('\n')
    for proc in self.procs:
      try:
        pinfo = proc.as_dict(attrs=['pid', 'name', 'cpu_percent', 'memory_percent'])
      except psutil.NoSuchProcess:
        procsToRemove.append(proc)
      else:
        cmdline = self.getJVMArgs(jvmOutput, pinfo['pid'])
        pinfo['name'] = 'Seep-Worker'
        for i in xrange(len(cmdline)):
          if cmdline[i] == '--data.port':
            pinfo['data.port'] = cmdline[i+1]
          elif cmdline[i] == '--master.ip':
            pinfo['master.ip'] = cmdline[i+1]
          elif cmdline[i] == '--master.scheduler.port':
            pinfo['master.scheduler.port'] = cmdline[i+1]
        workers.append(pinfo)

    for proc in procsToRemove:
      self.procs.remove(proc)
    return workers

  def getClusterMetrics(self):
    try:
      res = requests.get('http://' + config['yarn.host'] + ':8088/ws/v1/cluster/metrics')
      data = json.loads(res.text)
      return data['clusterMetrics']
    except requests.ConnectionError:
      log.warn("Failed to get cluster metrics.")
      return None

  def scan(self):
    mem = psutil.phymem_usage()
    self.info['memory'] = [mem.total, mem.percent]
    self.info['cpu'] = psutil.cpu_percent(interval=0, percpu=True)

    # Get io disk counters
    io = psutil.disk_io_counters()
    if not hasattr(self, 'read_bytes') or not hasattr(self, 'write_bytes'):
      self.info['disk_io'] = [0, 0]
    else:
      self.info['disk_io'] = [(io.read_bytes - self.read_bytes) / 5, (io.write_bytes - self.write_bytes) / 5]
    self.write_bytes = io.write_bytes
    self.read_bytes = io.read_bytes

    # Get net io counters
    net = psutil.net_io_counters()
    if not hasattr(self, 'bytes_sent') or not hasattr(self, 'bytes_recv'):
      self.info['net_io'] = [0, 0]
    else:
      self.info['net_io'] = [(net.bytes_sent - self.bytes_sent) / 5, (net.bytes_recv - self.bytes_recv) / 5]
    self.bytes_sent = net.bytes_sent
    self.bytes_recv = net.bytes_recv

    # Get resource information for each seep worker
    self.info['workers'] = self.scanWorkers()

    # Get other malicious data
    try: 
      out = subprocess.check_output(['bash', 'check-logs-size.sh'], cwd=(self.seep_root + '/deploy'))
      self.info['logs'] = [int(x) for x in re.findall('[0-9]+(?=\t)', out)]
    except subprocess.CalledProcessError:
      self.info['logs'] = [0, 0]
    self.info['host'] = os.uname()[1]
    log.info('Resource Report', self.info)

    if os.uname()[1] == config['yarn.host']:
      self.info['cluster_metrics'] = self.getClusterMetrics()

    payload = {'event': 'resource report', 'data': json.dumps(self.info)}
    send(self, payload)

  def run(self):
    self.working = True
    while self.working:
      startTimestamp = time.time()
      self.scan()
      self.sleep(config.getint('monitor.resources.scan.interval'))

  def stop(self):
    log.info('Stoping resource thread...')
    self.working = False

  def sleep(self, seconds):
    cnt = 0
    while self.working and cnt < seconds:
      time.sleep(1)
      cnt += 1

def startWatcherThread(host, path, prefix):
  global globalWatcherThread
  globalWatcherThread = WatcherThread(host, path, prefix)
  t = threading.Thread(target=globalWatcherThread.run)
  t.deamon = True
  t.start()

def startResourceThread(host, seep_root):
  global globalResourceThread
  globalResourceThread = ResourceThread(host, seep_root)
  t = threading.Thread(target=globalResourceThread.run)
  t.deamon = True
  t.start()

@app.route('/command', method='post')
def server_command():
  command = request.forms.get('command').split(' ')
  cwd = request.forms.get('cwd')
  if command == ['reset']:
    global globalWatcherThread, globalResourceThread
    args = globalWatcherThread.args()
    globalWatcherThread.stop()
    startWatcherThread(args[0], args[1], args[2])
    args = globalResourceThread.args()
    globalResourceThread.stop()
    startResourceThread(args[0], args[1])
  else:
    global globalProcess
    log.info('run', command)
    globalProcess = subprocess.Popen(command, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

@app.route('/status')
def server_status():
  global globalProcess
  if not globalProcess:
    return None
  if globalProcess.poll():
    retVal = ''
    while (select.select([globalProcess.stdout],[],[],0)[0]!=[]):
      retVal+=globalProcess.stdout.read(1)
    return (retVal if len(retVal) else 'pending')
  else:
    out = globalProcess.communicate()[0]
    globalProcess = None
    return out

# Main entry point
if __name__ == '__main__':
  #usage = 'We want as arguments: 1) host 2) port 3) absolute path to userlogs directory 4) absolute path to SEEPng directory'
  #if len(sys.argv) != 5 or not os.path.exists(sys.argv[3]) or not os.path.exists(sys.argv[4]):
  #  print usage
  #  exit(0)
  log = logger.Logger('Monitor')
  config = configparser.SafeConfigParser()
  config.read('analytics.properties')
  config = config['Basic']
  if '~' in config['hadoop.userlogs']:
    config['hadoop.userlogs'] = os.path.expanduser(config.get('hadoop.userlogs'))
  if '~' in config['seep.root']:
    config['seep.root'] = os.path.expanduser(config.get('seep.root'))

  host = 'http://' + config.get('server.host') + ':' + config.get('server.port')
  path = os.path.abspath(config.get('hadoop.userlogs'))
  prefix = path[:path.find('/logs')]
  seep_root = os.path.abspath(config.get('seep.root'))

  startWatcherThread(host, path, prefix)
  startResourceThread(host, seep_root)

app.run(host=gethostname(), port=config['monitor.port'], reloader=False)
globalWatcherThread.stop()
globalResourceThread.stop()