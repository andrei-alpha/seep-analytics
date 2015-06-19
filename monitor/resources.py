import re
import os
import time
import json
import util
import psutil
import requests
import subprocess

class Resources(object):
  def __init__(self, config, log, host, seep_root):
    self.config = config
    self.log = log
    self.info = {}
    self.host = host
    self.seep_root = seep_root
    self.procs = set()
    self.procs_io = {}
    self.procs_net_io = {}

  def args(self):
    return (self.host, self.seep_root)

  def getDiskIo(self, proc, div):
    res = [0, 0]
    if hasattr(proc, 'get_io_counters'):
      data = proc.get_io_counters()
      io = [data.read_bytes, data.write_bytes]
      if not proc.pid in self.procs_io:
        self.procs_io[proc.pid] = io
      else:
        res = [(io[0] - self.procs_io[proc.pid][0]) / div, (io[1] - self.procs_io[proc.pid][1]) / div]
        self.procs_io[proc.pid] = io
    return res

  def getNetIo(self, proc, div):
    res = [0, 0]
    if hasattr(proc, 'get_net_io'):
      data = proc.get_net_io()
      net_io = [data.sent_bytes, data.recv_bytes]
      if not proc.pid in self.procs_net_io:
        self.procs_net_io[proc.pid] = net_io
      else:
        net_io = [(net_io[0] - self.procs_net_io[proc.pid][0]) / div, (net_io[1] - self.procs_net_io[proc.pid][1]) / div]
        self.procs_net_io[proc.pid] = net_io 
    return res

  def scanWorkers(self):
    scanInterval = self.config.getint('monitor.resources.scan.interval')
    workers = []
    pids = []
    for proc in psutil.process_iter():
      try:
        if proc.name() == 'java' and not proc in self.procs:
          pids.append(proc.pid)
      except (psutil.NoSuchProcess, psutil.AccessDenied):
        pass

    for pid in pids:
      try:
        proc = psutil.Process(pid)
        pinfo = proc.as_dict(attrs=['cmdline'])
      except (psutil.NoSuchProcess, psutil.AccessDenied):
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
        cmdline = util.getJVMArgs(jvmOutput, pinfo['pid'])
        scanInterval = self.config.getint('monitor.resources.scan.interval')
        pinfo['disk_io'] = self.getDiskIo(proc, scanInterval)
        pinfo['net_io'] = self.getNetIo(proc, scanInterval)
        pinfo['name'] = 'Seep-Worker'
        check = 0
        for i in xrange(len(cmdline)):
          if cmdline[i] == '--data.port':
            pinfo['data.port'] = cmdline[i+1]
            check += 1
          elif cmdline[i] == '--master.ip':
            pinfo['master.ip'] = cmdline[i+1]
            check += 1
          elif cmdline[i] == '--master.scheduler.port':
            pinfo['master.scheduler.port'] = cmdline[i+1]
            check += 1
        # We want to report only seep processes that have all those arguments 
        # which are taken for granted in other places
        if check == 3:
          workers.append(pinfo)
      except (psutil.NoSuchProcess, psutil.AccessDenied):
        procsToRemove.append(proc)

    for proc in procsToRemove:
      self.procs.remove(proc)
    return workers

  def getClusterMetrics(self):
    try:
      res = requests.get('http://' + self.config['yarn.host'] + ':8088/ws/v1/cluster/metrics')
      data = json.loads(res.text)
      return data['clusterMetrics']
    except requests.ConnectionError:
      self.log.warn("Failed to get cluster metrics.")
      return {}

  def scan(self):
    scanInterval = self.config.getint('monitor.resources.scan.interval')
    mem = psutil.phymem_usage()
    self.info['memory'] = [mem.total, mem.percent]
    self.info['cpu'] = psutil.cpu_percent(interval=0, percpu=True)

    # Get io disk counters
    io = psutil.disk_io_counters()
    if not hasattr(self, 'read_bytes') or not hasattr(self, 'write_bytes'):
      self.info['disk_io'] = [0, 0]
    else:
      self.info['disk_io'] = [(io.read_bytes - self.read_bytes) / scanInterval, (io.write_bytes - self.write_bytes) / scanInterval]
    self.write_bytes = io.write_bytes
    self.read_bytes = io.read_bytes

    # Get net io counters
    net = psutil.net_io_counters()
    if not hasattr(self, 'bytes_sent') or not hasattr(self, 'bytes_recv'):
      self.info['net_io'] = [0, 0]
    else:
      self.info['net_io'] = [(net.bytes_sent - self.bytes_sent) / scanInterval, (net.bytes_recv - self.bytes_recv) / scanInterval]
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
    self.log.info('Resource Report', self.info)

    if os.uname()[1] == self.config['yarn.host']:
      self.info['cluster_metrics'] = self.getClusterMetrics()

    payload = {'event': 'resource report', 'data': json.dumps(self.info)}
    util.send(self, payload)

  def run(self):
    self.working = True
    while self.working:
      startTimestamp = time.time()
      self.scan()
      self.sleep(self.config.getint('monitor.resources.scan.interval'))

  def stop(self):
    self.log.info('Stoping resource thread...')
    self.working = False

  def sleep(self, seconds):
    cnt = 0
    while self.working and cnt < seconds:
      time.sleep(1)
      cnt += 1