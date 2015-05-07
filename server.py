import bottle
import copy
import datetime
import json
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
dataset['conts'] = {}
dataset['apps'] = {}
dataset['cluster'] = {}

EVENTS_PER_SECOND = 'events per second'
LAST_DATAPOINTS = 40
GENERAL = 'Total'
AVERAGE = 'Average'
FAIRNESS = 'Fairness'
SUBMIT_QUERY = 'submit_query'
UPDATE_SEEP = 'update_seep'
UPDATE_ANALYTICS = 'update_analytics'
KILL_ALL_SEEP = 'kill_all_seep'

if __name__ == '__main__':
  usage = ("We want as arguments:\n 1) hosts file (required)\n 2) SEEP root "
           "directory (required)\n 3) Analytics directory (default is current "
           "directory)\n 4) Base seep-master port (default is 4500)\n 5) Base "
           "seep-worker port (default is 6000)")
  
  if len(sys.argv) < 3 or not os.path.exists(sys.argv[1]) or not os.path.exists(sys.argv[2]) or (len(sys.argv) >= 4 and not os.path.exists(sys.argv[3])):  
    print usage
    exit(0)

  admin.hosts_names = map(lambda x: x.strip(), open(sys.argv[1], 'r').read().split('\n'))
  admin.hosts = map(lambda x: 'http://' + x + ':7008', filter(lambda x: len(x) > 4, admin.hosts_names))
  admin.seep_root = os.path.abspath(sys.argv[2])
  admin.analytics_root = os.path.abspath(sys.argv[3] if len(sys.argv) >= 4 else '.')
  admin.baseYarnMasterPort = (int(sys.argv[4]) if len(sys.argv) >= 5 else admin.baseYarnMasterPort)
  admin.baseYarnWorkerPort = (int(sys.argv[5]) if len(sys.argv) >= 6 else admin.baseYarnWorkerPort)

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
  data['time'] = int(timestamp)

  return data

def updateContainerData(dataset, json):
  dataset.append(copy.deepcopy(json))
  return dataset[-LAST_DATAPOINTS:]

def updateAppData(dataset, json):
  for event in dataset:
    if abs(int(event['time']) - int(json['time'])) < 30:
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

def updateClusterData(dataset, json):
  if not dataset:
    dataset[GENERAL] = {}
    dataset[GENERAL]['data'] = []
    dataset[AVERAGE] = {}
    dataset[AVERAGE]['data'] = []
    dataset[FAIRNESS] = {}
    dataset[FAIRNESS]['data'] = []

  dataset[GENERAL]['metric'] = EVENTS_PER_SECOND
  dataset[AVERAGE]['metric'] = EVENTS_PER_SECOND
  dataset[FAIRNESS]['metric'] = EVENTS_PER_SECOND

  for index, event in enumerate(dataset[GENERAL]['data']):
    if abs(int(event['time']) - int(json['time'])) < 30:
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

def getQueryFileName(data):
  match = re.search('(?<=query.file\s=\s).*jar', data)
  if match:
    return re.search('[a-zA-z-_]+\.jar', match.group()).group()
  return None

def update(appId, contId, data):
  if not appId in dataset['apps']:
    dataset['apps'][appId] = {}
    dataset['apps'][appId]['data'] = []
  if not contId in dataset['conts']:
    dataset['conts'][contId] = {}
    dataset['conts'][contId]['data'] = []
    dataset['conts'][contId]['app'] = appId

  queryFile = getQueryFileName(data)
  if queryFile:
    dataset['apps'][appId]['query'] = queryFile

  # Get container type
  ctype = re.search('(?<=Configuring local task:\s)[A-Za-z]+(?=@)', data)

  # Skip Source containers updates
  if ctype and ctype.group() == 'Source':
    return
  if ctype:
    dataset['conts'][contId]['type'] = ctype.group()

  strs = data.split('\n')
  matches = filter(lambda x: 'Meters' in strs[x], xrange(len(strs)))
  for match in matches:
    report = strs[match-2:][:10]

    try:
      json = convert(report)
    except:
      print 'Unexpected error when converting report:', report
      print sys.exc_info()[0]
      continue

    #print 'find', appId, contId, json
    #print ''

    # Add event to containers dataset
    dataset['conts'][contId]['data'] = updateContainerData(dataset['conts'][contId]['data'], json)
    dataset['conts'][contId]['metric'] = EVENTS_PER_SECOND

    # Add event to apps dataset
    dataset['apps'][appId]['data'] = updateAppData(dataset['apps'][appId]['data'], json)
    dataset['apps'][appId]['metric'] = EVENTS_PER_SECOND

    # Add event to cluster dataset
    dataset['cluster'] = updateClusterData(dataset['cluster'], json)

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
  appId = getAppId(path)
  containerId = getContainerId(path)

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

@app.route('/backend/dataset')
def getDataset():
  response.content_type = 'application/json'
  return json.dumps(dataset)

@app.route('/')
def index():
  return static_file('index.html', root='')

@app.route('/static/<filepath:path>')
def server_static(filepath):
    return static_file(filepath, root='')

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
  else:
    return json.dumps(admin.getAvailableOptions())
  t.deamon = True
  t.start()
  return 'ok'

@app.route('/command/status')
def server_command_status():
  return admin.status()

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

admin.sendCommand(None, 'reset')
app.run(host=gethostname(), port=7007, reloader=True)
