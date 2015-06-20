import re
import os
import configparser
import json
import logger
import select
import psutil
import socket
import subprocess
import requests
import time
import threading

from monitor import util, resources, metrics
from socket import gethostname
from bottle import Bottle, response, request

app = Bottle()
globalWatcherThread = None
globalResourceThread = None
globalProcess = None

def startWatcherThread(host, path, prefix):
  global globalWatcherThread
  globalWatcherThread = metrics.Metrics(config, log, host, path, prefix)
  t = threading.Thread(target=globalWatcherThread.run)
  t.deamon = True
  t.start()

def startResourceThread(host, seep_root):
  global globalResourceThread
  globalResourceThread = resources.Resources(config, log, host, seep_root)
  t = threading.Thread(target=globalResourceThread.run)
  t.deamon = True
  t.start()

def stopAppMaster(scheduler_port):
  try:
    for command in ['stop', 'exit']:
      s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
      s.connect((gethostname(), int(scheduler_port)))
      s.sendall(command)
      s.close()
  except:
    log.error('Failed to stopgracefully stop master')

def stopAppMasters():
  pids = []
  jvmOutput = subprocess.check_output(['jps', '-m']).split('\n')
  for proc in psutil.process_iter():
    try:
      if proc.name() == 'java':
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
      if sum([len(re.findall('seep-master', x)) for x in cmdline]) < 20:
        continue
      cmdline =  util.getJVMArgs(jvmOutput, pid)
      for i in xrange(len(cmdline)):
        if cmdline[i] == '--master.scheduler.port':
          stopAppMaster(cmdline[i+1])
          break

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
  elif command == ['stop-all']:
    log.info('stop queries gracefully')
    t = threading.Thread(target=stopAppMasters)
    t.deamon = True
    t.start()
  else:
    global globalProcess
    # If the process is still runnning
    if globalProcess and globalProcess.poll() > 0:
      util.killAll(globalProcess.pid)
    log.info('run', command)
    globalProcess = subprocess.Popen(command, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

@app.route('/status')
def server_status():
  global globalProcess
  if not globalProcess:
    return None
  if globalProcess.poll() > 0:
    retVal = ''
    while (select.select([globalProcess.stdout],[],[],0)[0]!=[]):
      retVal += globalProcess.stdout.read(1)
    return (retVal if len(retVal) else 'pending')
  else:
    out = globalProcess.communicate()[0]
    globalProcess = None
    return out

# Main entry point
if __name__ == '__main__':
  # usage = 'We want as arguments: 1) host 2) port 3) absolute path to userlogs directory 4) absolute path to SEEPng directory'
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