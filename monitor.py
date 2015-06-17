import os
import configparser
import json
import logger
import select
import subprocess
import requests
import time
import threading

from monitor import resources, metrics
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