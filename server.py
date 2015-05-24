import bottle
import copy
import configparser
import datetime
import json
import logger
import os
import re
import sys
import time
import threading
import admin

from socket import gethostname
from bottle import Bottle, response, request, run, template, static_file

bottle.BaseRequest.MEMFILE_MAX = 1024 * 1024

app = Bottle()

files = {}
urls = {}
dataset = {}
totalEventsToDate = 0
dataset['containers'] = {}
dataset['apps'] = {}
dataset['cluster'] = {}
dataPortToContainerIdMap = {}
dataPortToWorkerMap = {}

EVENTS_PER_SECOND = 'events per second'
LAST_DATAPOINTS = None
GENERAL = 'Total'
AVERAGE = 'Average'
STATISTICS = 'Statistics'
FAIRNESS = 'Fairness'
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
  admin.baseYarnWorkerDataPort = config.getint('Basic', 'base.yarn.worker.data.port')
  admin.baseYarnSchedulerPort = config.getint('Basic', 'base.yarn.scheduler.port')
  LAST_DATAPOINTS = config.getint('Basic', 'last.datapoints')
  admin.Globals.schedulerPort = config.getint('Basic', 'scheduler.port')
  admin.Globals.log = log

def format_links(links):
  html = '<table>'
  for link in links:
    html += '<tr><td><a href="' + link[0] + '">' + link[1] + '</a></td></tr>'
  html += '</table>'
  return html

def convert(event):
  data = {}

  utc_date = datetime.datetime.strptime(event[0].replace('=', '').strip(), "%m/%d/%y %I:%M:%S %p")
  # TODO: One by off error related to timezones when converting in javascript (temp fix)
  timestamp = (utc_date - datetime.datetime(1970,1,1)).total_seconds() - 3600

  for i in xrange(4,9):
    label = re.search('[-a-z0-9]+[\sa-z0-9]+', event[i]).group().strip()
    value = re.search('[0-9.]+(?=$|\s)', event[i]).group().strip()
    data[label] = float(value)
  data['time'] = int(timestamp / 30)

  return data

def updateContainerData(dataset, json):
  dataset.append(copy.deepcopy(json))
  return dataset[-LAST_DATAPOINTS:]

def updateAppData(dataset, json):
  for event in dataset:
    if event['time'] == json['time']:
      for label in event:
        if label == 'time':
          continue
        event[label] += json[label]
      return dataset
  
  # Don't append old data
  if len(dataset) and int(dataset[-1]['time']) > int(json['time']):
    return dataset
  dataset.append(copy.deepcopy(json))
  return dataset[-LAST_DATAPOINTS:]

def updateClusterData(dataset, json, appId):
  if not dataset:
    dataset[GENERAL] = {}
    dataset[GENERAL]['data'] = []
    dataset[AVERAGE] = {}
    dataset[AVERAGE]['data'] = []
    dataset[FAIRNESS] = {}
    dataset[FAIRNESS]['data'] = []
    dataset[STATISTICS] = {}

  dataset[GENERAL]['metric'] = EVENTS_PER_SECOND
  dataset[AVERAGE]['metric'] = EVENTS_PER_SECOND
  dataset[FAIRNESS]['metric'] = EVENTS_PER_SECOND

  # Update overall statistics
  dataset[STATISTICS] = updateStatistics(dataset[STATISTICS], json, appId)

  for index, event in enumerate(dataset[GENERAL]['data']):
    if event['time'] == json['time']:
      for label in event:
        # Update special labels when updating normal ones
        if label == 'time':
          continue
        event[label] += json[label]
        
        dataset[AVERAGE]['data'][index][label + '-cnt'] += 1
        dataset[AVERAGE]['data'][index][label + '-avg'] = float(event[label]) / dataset[AVERAGE]['data'][index][label + '-cnt']

        dataset[FAIRNESS]['data'][index][label + '-max'] = max(dataset[FAIRNESS]['data'][index][label + '-max'], float(json[label]))
        dataset[FAIRNESS]['data'][index][label + '-min'] = min(dataset[FAIRNESS]['data'][index][label + '-min'], float(json[label]))
        dataset[FAIRNESS]['data'][index][label + '-fairness'] = dataset[FAIRNESS]['data'][index][label + '-max'] / max(0.01, dataset[FAIRNESS]['data'][index][label + '-min'])

      return dataset
  # Don't append old data
  if len(dataset[GENERAL]['data']) and int(dataset[GENERAL]['data'][-1]['time']) > int(json['time']):
    return dataset

  average_json = {}
  fairness_json = {}
  for label in json:
    if label == 'time':
      average_json[label] = int(json[label])
      fairness_json[label] = int(json[label])
      continue
    average_json[label + '-cnt'] = 1
    average_json[label + '-avg'] = float(json[label])

    fairness_json[label + '-max'] = float(json[label])
    fairness_json[label + '-min'] = float(json[label])
    fairness_json[label + '-fairness'] = 1.0

  dataset[GENERAL]['data'].append(copy.deepcopy(json))
  dataset[AVERAGE]['data'].append(average_json)
  dataset[FAIRNESS]['data'].append(fairness_json)

  dataset[GENERAL]['data'] = dataset[GENERAL]['data'][-LAST_DATAPOINTS:]
  dataset[AVERAGE]['data'] = dataset[AVERAGE]['data'][-LAST_DATAPOINTS:]
  dataset[FAIRNESS]['data'] = dataset[FAIRNESS]['data'][-LAST_DATAPOINTS:]
  return dataset

eventsPerTime = {}
eventsPerTime['apps'] = {}

def updateIndividualStatistic(dataset, ts, value, count):
  concurrentApps = len(eventsPerTime['apps'][ts])
  if not concurrentApps in dataset:
    dataset[concurrentApps] = {'count': 0, 'total': 0, 'tss': {}}
  dataset[concurrentApps]['count'] += count
  dataset[concurrentApps]['total'] += value
  if not ts in dataset[concurrentApps]['tss']:
    dataset[concurrentApps]['tss'][ts] = [0, 0]
  dataset[concurrentApps]['tss'][ts][0] += value
  dataset[concurrentApps]['tss'][ts][1] += count
  return dataset

def updateStatistics(dataset, json, appId):
  ts = json['time']
  value = json['1-minute rate']
  # Add appId to this timestamp's list of apps
  if not ts in eventsPerTime['apps']:
    eventsPerTime['apps'][ts] = {}
  eventsPerTime['apps'][ts][appId] = True

  # Update for current event
  dataset = updateIndividualStatistic(dataset, ts, value, 1)
  concurrentApps = len(eventsPerTime['apps'][ts])

  # Remove previous event if the number of concurrent apps increased
  if concurrentApps > 1 and concurrentApps - 1 in dataset and ts in dataset[concurrentApps - 1]['tss']:
    value = dataset[concurrentApps - 1]['tss'][ts][0]
    count = dataset[concurrentApps - 1]['tss'][ts][1]
    del dataset[concurrentApps - 1]['tss'][ts]
    dataset[concurrentApps - 1]['count'] -= count
    dataset[concurrentApps - 1]['total'] -= value
    dataset = updateIndividualStatistic(dataset, ts, value, count)
  return dataset

def getQueryFileName(data):
  match = re.search('(?<=query.file\s=\s).*jar', data)
  if match:
    return re.search('[a-zA-z-_]+\.jar', match.group()).group()
  return None

def update(appId, contId, data):
  if not appId in dataset['apps']:
    dataset['apps'][appId] = {}
    dataset['apps'][appId]['data'] = []
  if not contId in dataset['containers']:
    dataset['containers'][contId] = {}
    dataset['containers'][contId]['data'] = []
    dataset['containers'][contId]['app'] = appId

  queryFile = getQueryFileName(data)
  if queryFile:
    dataset['apps'][appId]['query'] = queryFile

  # Get container type
  ctype = re.search('(?<=Configuring local task:\s)[A-Za-z]+(?=@)', data)
  if ctype:
    dataset['containers'][contId]['type'] = ctype.group()

  # Get container data.port
  data_port = re.search('(?<=data\.port\s=\s)[0-9][0-9][0-9][0-9]+', data)
  if data_port:
    dataset['containers'][contId]['data.port'] = data_port.group()
    dataPortToContainerIdMap[str(data_port.group())] = contId

  # Skip Source containers updates
  if 'type' in  dataset['containers'][contId] and dataset['containers'][contId]['type'] == 'Source':
    return

  strs = data.split('\n')
  matches = filter(lambda x: 'Meters' in strs[x], xrange(len(strs)))
  for match in matches:
    report = strs[match-2:][:10]

    try:
      json = convert(report)
    except:
      log.error('Unexpected error when converting report:', report)
      continue

    #print 'find', appId, contId, json
    #print ''

    # Add to toal number of events. Assume a event is every 30 sec
    global totalEventsToDate
    totalEventsToDate += json['1-minute rate'] * 30

    # Add event to containers dataset
    dataset['containers'][contId]['data'] = updateContainerData(dataset['containers'][contId]['data'], json)
    dataset['containers'][contId]['metric'] = EVENTS_PER_SECOND

    # Add event to apps dataset
    dataset['apps'][appId]['data'] = updateAppData(dataset['apps'][appId]['data'], json)
    dataset['apps'][appId]['metric'] = EVENTS_PER_SECOND

    # Add event to cluster dataset
    dataset['cluster'] = updateClusterData(dataset['cluster'], json, appId)

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
    update(appId, containerId, data)

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
  options['startup-scheduler-type'] = config.get('Scheduler', 'startup.scheduling.type')
  options['runtime-scheduler'] = config.get('Scheduler', 'runtime.scheduling.enabled')
  return json.dumps(options)

@app.route('/command/resource_report')
def server_get_info():
  if not admin.Globals.clusterInfo:
      return 'pending'
  clusterInfo = copy.deepcopy(admin.Globals.clusterInfo)
  clusterInfo['overall']['total_events'] = totalEventsToDate
  if GENERAL in dataset['cluster'] and 'data' in dataset['cluster'][GENERAL] and len(dataset['cluster'][GENERAL]['data']):
    clusterInfo['overall']['current_rate'] = dataset['cluster'][GENERAL]['data'][-1]['1-minute rate']
  response.content_type = 'application/json'
  return clusterInfo

@app.route('/command/set_config', method='post')
def server_set_config():
  section = request.forms.get('section')
  name = request.forms.get('name')
  value = request.forms.get('value')
  if section == 'Scheduler':
    schedulerHost = 'http://' + os.uname()[1] + ':' + str(config.getint('Basic', 'scheduler.port'))
    admin.sendRequest(schedulerHost, '/command/set_config', {'name': name, 'value': value}, 'post')
    config.set(section, name, value)

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

#@app.route('/stop')
#def server_stop():
#  sys.stderr.close()

admin.sendCommand(None, 'reset')
#@do_profile(follow=[event, update, updateStatistics, updateClusterData, updateAppData, updateContainerData, admin.updateResourceReport, admin.getAvailableOptions])
#def main_wrapper():
app.run(host=gethostname(), port=config.getint('Basic', 'server.port'), reloader=True)
#main_wrapper()
