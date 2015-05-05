import sys
import re
import bottle
import copy
import datetime
import json

import time

from bottle import Bottle, response, request, run, template, static_file

EVENTS_PER_SECOND = 'events per second'

app = Bottle()

files = {}
urls = {}
dataset = {}
dataset['conts'] = {}
dataset['apps'] = {}
dataset['cluster'] = {}

LAST_DATAPOINTS = 40

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

def getQueryFileName(data):
  match = re.search('(?<=query.file\s=\s).*jar', data)
  if match:
    return re.search('[a-zA-z-_]+\.jar', match.group()).group()
  return None

def update(appId, contId, data):
  clusterId = 'wombat'
  if not clusterId in dataset['cluster']:
    dataset['cluster'][clusterId] = {}
    dataset['cluster'][clusterId]['data'] = []
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
    dataset['cluster'][clusterId]['data'] = updateClusterData(dataset['cluster'][clusterId]['data'], json)
    dataset['cluster'][clusterId]['metric'] = EVENTS_PER_SECOND

  # Get container type
  ctype = re.search('(?<=Configuring local task:\s)[A-Za-z]+(?=@)', data)
  if ctype:
    dataset['conts'][contId]['type'] = ctype.group()

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

@app.route('/event', method="post")
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
      files[path] = ''
      urls[path] = ''

    files[path] += data
    urls[path] += data
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

@app.route("/<url:re:.+>")
def logs(url):
  if url.endswith('/'):
     url = url[:-1]
  url = '/' + url
  if url in urls and isinstance(urls[url], str):
    response.content_type = "text/plain; charset=UTF8"
    return urls[url]
    #return template('file.html', content=urls[url])
  elif url in urls and isinstance(urls[url], list):
    return format_links(urls[url])
  return "No logs data found"

app.run(host='localhost', port=7007, reloader=True)
