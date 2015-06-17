import re
import copy
import datetime

dataPortToContainerIdMap = {}
dataPortToWorkerMap = {}

dataset = {}
totalEventsToDate = 0
dataset['containers'] = {}
dataset['apps'] = {}
dataset['cluster'] = {}

EVENTS_PER_SECOND = 'events per second'
LAST_DATAPOINTS = None
GENERAL = 'Total'
AVERAGE = 'Average'
STATISTICS = 'Statistics'
FAIRNESS = 'Fairness'

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