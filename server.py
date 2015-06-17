import bottle
import configparser
import json
import logger
import os
import sys
import threading

from master import admin, analytics
from socket import gethostname
from bottle import Bottle, response, request, run, template, static_file

bottle.BaseRequest.MEMFILE_MAX = 1024 * 1024

app = Bottle()

files = {}
urls = {}
dataset = analytics.dataset
totalEventsToDate = analytics.totalEventsToDate
dataPortToWorkerMap = analytics.dataPortToWorkerMap
dataPortToContainerIdMap = analytics.dataPortToContainerIdMap

SUBMIT_QUERY = 'submit_query'
UPDATE_SEEP = 'update_seep'
UPDATE_ANALYTICS = 'update_analytics'
KILL_ALL_SEEP = 'kill_all_seep'
RESET_KAFKA = 'reset_kafka'
CLEAR_HADOOP_LOGS = 'clear_hadoop_logs'

try:
    from line_profiler import LineProfiler

    def do_profile(follow=[]):
        def inner(func):
            def profiled_func(*args, **kwargs):
                try:
                    profiler = LineProfiler()
                    profiler.add_function(func)
                    for f in follow:
                        profiler.add_function(f)
                    profiler.enable_by_count()
                    return func(*args, **kwargs)
                finally:
                    profiler.print_stats()
            return profiled_func
        return inner

except ImportError:
    def do_profile(follow=[]):
        "Helpful if you accidentally leave in production!"
        def inner(func):
            def nothing(*args, **kwargs):
                return func(*args, **kwargs)
            return nothing
        return inner


if __name__ == '__main__':
  #usage = ("We want as arguments:\n 1) hosts file (required)\n 2) SEEP root "
  #         "directory (required)\n 3) Analytics directory (default is current "
  #         "directory)\n 4) Base seep-master port (default is 4500)\n 5) Base "
  #         "seep-worker port (default is 6000)")
  config = configparser.SafeConfigParser()
  config.read('analytics.properties')
  log = logger.Logger('Server')

  # TODO: check that the configuration is valid
  #if len(sys.argv) < 3 or not os.path.exists(sys.argv[1]) or not os.path.exists(sys.argv[2]) or (len(sys.argv) >= 4 and not os.path.exists(sys.argv[3])):  
  #  print usage
  #  exit(0)
  admin.hosts_names = map(lambda x: x.strip(), config.get('Basic', 'hosts').split(','))
  admin.hosts = map(lambda x: 'http://' + x + ':' + str(config.getint('Basic', 'monitor.port')), filter(lambda x: len(x) > 4, admin.hosts_names))
  admin.seep_root = os.path.abspath(os.path.expanduser(config.get('Basic', 'seep.root')))
  admin.analytics_root = os.path.abspath(os.path.expanduser(config.get('Basic', 'analytics.root')))
  admin.baseYarnWorkerMasterPort = config.getint('Basic', 'base.yarn.worker.master.port')
  admin.Globals.initBaseYarnWorkerMasterPort = admin.baseYarnWorkerMasterPort
  admin.baseYarnWorkerDataPort = config.getint('Basic', 'base.yarn.worker.data.port')
  admin.Globals.initBaseYarnWorkerDataPort = admin.baseYarnWorkerDataPort
  admin.baseYarnSchedulerPort = config.getint('Basic', 'base.yarn.scheduler.port')
  admin.Globals.initBaseYarnSchedulerPort = admin.baseYarnSchedulerPort
  analytics.LAST_DATAPOINTS = config.getint('Basic', 'last.datapoints')
  admin.Globals.schedulerPort = config.getint('Basic', 'scheduler.port')
  admin.Globals.log = log

def format_links(links):
  html = '<table>'
  for link in links:
    html += '<tr><td><a href="' + link[0] + '">' + link[1] + '</a></td></tr>'
  html += '</table>'
  return html

def getAppId(path):
  idx1 = path.find('application')
  idx2 = path[idx1:].find('/')
  idx3 = path[idx1:][:idx2].find('_')
  return path[idx1:][:idx2][idx3+1:]

def getContainerId(path):
  idx1 = path.find('container')
  idx2 = path[idx1:].find('/')
  idx3 = path[idx1:][:idx2].find('_')
  return path[idx1:][:idx2][idx3+1:]

@app.route('/event', method='post')
def event():
  event = request.forms.get('event')
  path = request.forms.get('path')
  data = request.forms.get('data')

  appId = getAppId(path) if path else None
  containerId = getContainerId(path) if path else None

  if event == 'created':
    parent = ''
    root = ''
    dirs = path.split('/')
    for i in xrange(1, len(dirs)):
      if not root in files:
        files[root] = []
        urls[root] = []
        if len(parent):
          urls[root].append([parent, 'Parent Directory'])
        else:
          urls[root].append(['/','Home'])

      if not dirs[i] in files[root]:
        #print 'add:', root
        files[root].append(dirs[i])
        urls[root].append([root + '/' + dirs[i], dirs[i]])
      parent = root
      root = root + '/' + dirs[i] 

  if event == 'new line':
    if not path in files:
      #print 'add:', path
      if not os.path.exists(os.path.dirname(path[1:])):
        os.makedirs(os.path.dirname(path[1:]))

      files[path] = 'text/plain'
      urls[path] = 'text/plain'
      with open(path[1:], "w") as fout:
        fout.write('')

    with open(path[1:], "a") as fout:
      fout.write(data)
    analytics.update(appId, containerId, data)

  if event == 'resource report':
    data = json.loads(data)
    for worker in data['workers']:
      if 'data.port' in worker and str(worker['data.port']) in dataPortToContainerIdMap and 'type' in dataset['containers'][dataPortToContainerIdMap[worker['data.port']]] :
        worker['type'] = dataset['containers'][dataPortToContainerIdMap[worker['data.port']]]['type']
        dataPortToWorkerMap[ worker['data.port'] ] = worker
    admin.updateResourceReport(data)

@app.route('/backend/dataset')
def getDataset():
  response.content_type = 'application/json'
  return json.dumps(dataset)

@app.route('/')
@app.route('/containers')
@app.route('/resources')
@app.route('/apps')
@app.route('/cluster')
@app.route('/admin')
def index():
  return static_file('index.html', root='')

@app.route('/static/<filepath:path>')
def server_static(filepath):
    return static_file(filepath, root='')

@app.route('/command/status')
def server_command_status():
  return admin.status()

@app.route('/command/<command>', method='post')
def server_command(command):
  data = dict(request.forms)
  admin.reset()
  if command == KILL_ALL_SEEP:
    t = threading.Thread(target=admin.killAllSeepQueries)
  elif command == UPDATE_SEEP:
    t = threading.Thread(target=admin.updateSeep, args=(data['branch'],))
  elif command == UPDATE_ANALYTICS:
    t = threading.Thread(target=admin.updateAnalytics, args=(data['branch'],))
  elif command == SUBMIT_QUERY:
    t = threading.Thread(target=admin.submitQuery, args=(data['queryName'], int(data['deploymentSize'])))
  elif command == RESET_KAFKA:
    t = threading.Thread(target=admin.resetKafka)
  elif command == CLEAR_HADOOP_LOGS:
    t = threading.Thread(target=admin.clearHadoopLogs)
  else:
    return 'Failed'
  t.deamon = True
  t.start()
  return 'ok' # This way we know the command was sent succesfully

@app.route('/options')
def server_options():
  options = admin.getAvailableOptions()
  return json.dumps(options)

@app.route('/command/resource_report')
def server_get_info():
  response.content_type = 'application/json'
  return admin.getClusterInfo(dataset['cluster'].get(analytics.GENERAL), totalEventsToDate)

@app.route('/command/scheduler_report')
def server_scheduler_report():
  response.content_type = 'application/json'
  return admin.getClusterInfo(dataset['cluster'].get(analytics.GENERAL), totalEventsToDate, True)

@app.route('/command/set_config', method='post')
def server_set_config():
  section = request.forms.get('section')
  name = request.forms.get('name')
  value = request.forms.get('value')
  if section == 'Scheduler' and value and name:
    config.set(section, name, str(value))
    t = threading.Thread(target=updateSchedulerConfig, args=(name, value))
    t.deamon = True
    t.start()

def updateSchedulerConfig(name, value):
  schedulerHost = 'http://' + os.uname()[1] + ':' + str(config.getint('Basic', 'scheduler.port'))
  admin.sendRequest(schedulerHost, '/command/set_config', {'name': name, 'value': value}, 'post')

@app.route('/command/get_config')
def server_get_config():
  return json.dumps({'Basic': config._sections['Basic'], 'Scheduler': config._sections['Scheduler']})

# This is a debug command to communicate with seep workers
@app.route('/moke/sudo', method='post')
def server_moke():
  data = dict(request.forms)
  return admin.moke(data, dataPortToWorkerMap)

@app.route("/<url:re:.+>")
def logs(url):
  if url.endswith('/'):
     url = url[:-1]
  url = '/' + url
  if url in urls and isinstance(urls[url], str):
    return static_file(url[1:], root='', mimetype='text/plain')
  elif url in urls and isinstance(urls[url], list):
    return format_links(urls[url])
  return "No logs data found"

@app.route('/ping')
def server_ping():
  return 'ok'

#@app.route('/stop')
#def server_stop():
#  sys.stderr.close()

admin.sendCommand(None, 'reset')
#@do_profile(follow=[event, update, updateStatistics, updateClusterData, updateAppData, updateContainerData, admin.updateResourceReport, admin.getAvailableOptions])
#def main_wrapper():
app.run(host=gethostname(), port=config.getint('Basic', 'server.port'), reloader=True)
#main_wrapper()
