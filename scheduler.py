import os
import sys
import json
import logger
import requests
import threading
import configparser

from scheduler import util, dispatcher, scheduling
from socket import gethostname
from bottle import Bottle, request

app = Bottle()

def computeBusyScore(data):
  return data['avg_cpu']

@app.route('/scheduler/host')
def scheduler_host():
  if config.getint('Scheduler', 'startup.scheduling.type') == 0:
    return util.externalIp(config, 'wombat01' if 'wombat' in os.uname()[1] else os.uname()[1] )
  elif config.getint('Scheduler', 'startup.scheduling.type') == 1:
    return None
  else:
    return scheduler.allocate()

@app.route('/command/set_config', method='post')
def server_set_config():
  name = request.forms.get('name')
  value = request.forms.get('value')
  log.info('Set config', name, value)
  config.set('Scheduler', name, value)

@app.route('/command/reset/allocations')
def server_reset_allocations():
  log.info("Reset allocations")
  scheduler.resetAllocations()

@app.route('/ping')
def server_ping():
  return 'ok'

def update_configs():
  try:
    res = requests.get('http://' + gethostname() + ':' + str(config.getint('Basic', 'server.port')) + '/command/get_config')
  except requests.ConnectionError:
    log.warn("Failed to get latest configs!")
    return
  data = json.loads(res.text)
  for name, value in data['Scheduler'].iteritems():
    config.set('Scheduler', name, value)
    log.info('Init config from server', name, value)

if __name__ == "__main__":
  log = logger.Logger('Scheduler')
  config = configparser.SafeConfigParser()
  config.read('analytics.properties')
  dispatcher = dispatcher.Dispatcher(config, log)
  scheduler = scheduling.Scheduler(config, log, dispatcher)
  update_configs()
  t = threading.Thread(target=scheduler.run)
  t.deamon = True
  t.start()
  t = threading.Thread(target=dispatcher.run)
  t.deamon = True
  t.start()

app.run(host=gethostname(), port=config.getint('Basic', 'scheduler.port'), reloader=False, quiet=True)
scheduler.stop()
dispatcher.stop()
