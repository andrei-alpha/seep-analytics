import os
import sys
import time
import json
import logger
import time
import requests
import configparser

"""
commands = [{
  'run': {
    'name': 'submit_query',
    'data': {'queryName': 'kafka-cpu-pipeline.jar', 'deploymentSize': 1}
  },
  'metrics': [{
    'type': 'dataset',
    'path': ['cluster', 'Total'],
    'value': '1-minute rate',
    'metric': 'max'
  },{
    'type': 'resource',
    'path': ['overall', 'cpu_usage'],
    'metric': 'avg'
  }],
  'time': 180,
  'finally': {
    'name': 'kill_all_seep',
    'data': {}
  }
}]
"""

def getDataSetMetric(lst, metric, ts, takeLast=False):
  if not 'value' in metric:
    log.warn('Dataset value missing! Dataset have multple metrics.')
    return

  res = requests.get(server_host + '/backend/dataset')
  data = json.loads(res.text)
  for item in metric['path']:
    data = data.get(item)
    if not data:
      log.warn("Failed to parse metric for path", metric[path])
      return

  # Skip the last two datapoints because they might have missing value
  for x in xrange(len(data['data']) - (2 if not takeLast else 0) ):
    item = data['data'][x]
    # Server time is seconds timestamp / 30
    if item['time'] * 30 > ts and metric['value'] in item:
      lst.append(item[metric['value']])
      ts = item['time']

def getResourceMetric(lst, metric, ts):
  res = requests.get(server_host + '/command/resource_report')
  data = json.loads(res.text)
  for item in metric['path']:
    data = data.get(item)
    if not data:
      log.warn("Failed to parse metric for path", metric[path])
      return
  lst.append(data)

def areQueriesRunning():
  try:
    res = requests.get(yarn_host + '/ws/v1/cluster/metrics')
    data = json.loads(res.text)
    appsRunning = data['clusterMetrics']['appsRunning']
    return (appsRunning != 0)
  except:
    return False

def sendCommand(run, count=0):
  try:
    res = requests.post(server_host + '/command/' + run['name'], data=run['data'])
    if not res.text or res.text != 'ok':
      log.warn('Server failed to take command', run)
      return False
  except requests.ConnectionError:
    log.warn('Cannot connect to server!')
    return False

  timestamp = time.time()
  progress = 0
  current = ''
  while progress < 100:
    res = requests.get(server_host + '/command/status')
    data = json.loads(res.text)
    progress = int(data['progress'])
    current = data['current']
    time.sleep(0.5)
  
  # For kill command we might need to try again
  if run['name'] == 'kill_all_seep':
    time.sleep(1.5)
    if areQueriesRunning():
      if count == 5:
        log.warn("Failed to stop all seep queries. Aborting benchmark.")
        sys.exit(0)
      log.warn("Didn't manage for now to stop seep queries. Try again in 2 seconds...")
      time.sleep(2)
      sendCommand(run, count+1)
      return

  log.debug('Command "' + str(current) + '" took', int(time.time() - timestamp), 'seconds')
  return True

def runBenchmark(commands):
  benchmarkResults = {}
  benchmarkStartTimestamp = time.time()
  for command in commands:
    if not sendCommand(command['run']):
      continue

    metrics = command['metrics']
    timestamp = time.time()
    lastDataPoint = time.time()
    results = [[] for x in xrange(len(metrics))]

    log.info(' ------------- Run Benchmark:', command['time'], 'sec -------------------')
    while time.time() - timestamp < command['time']:
      time.sleep(10)
      for x in xrange(len(metrics)):
        metric = metrics[x]
        if metric['type'] == 'dataset':
          getDataSetMetric(results[x], metric, lastDataPoint)
        else:
          getResourceMetric(results[x], metric, lastDataPoint)

    # Take into account also last datapoints for max
    for x in xrange(len(metrics)):
      metric = metrics[x]
      if metric['type'] == 'dataset' and metric['metric'] == 'max':
        getDataSetMetric(results[x], metric, lastDataPoint, True)

    for x in xrange(len(metrics)):
      metric = metrics[x]
      metricName = '.'.join(metric['path'])
      metricName = metricName + '.' + metric['metric']
      if not results[x]:
        log.warn("No data received for", metricName)
        continue

      if metric['metric'] == 'avg':
        val = sum(results[x]) / len(results[x])
      elif metric['metric'] == 'max':
        val = max(results[x])
      elif metric['metric'] == 'min':
        val = min(results[x])
      log.info(metricName + ':', val)

      if not metricName in benchmarkResults:
        benchmarkResults[metricName] = []
      benchmarkResults[metricName].append(val)

    if 'finally' in command:
      sendCommand(command['finally'])

  log.info(" -------------- Benchmark finished ---------------------- ")
  log.debug(" Run time:", int(time.time() - benchmarkStartTimestamp), 'seconds')
  log.info(" Results:", benchmarkResults)

def initConfigs(configs):
  for config in configs:
    log.debug("Set config", config['name'], config['value'])
    requests.post(server_host + '/command/set_config', data=config)

if __name__ == "__main__":
  config = configparser.SafeConfigParser()
  config.read('analytics.properties')
  server_host = 'http://' + config.get('Basic', 'server.host') + ':' + config.get('Basic', 'server.port')
  yarn_host = 'http://' + config.get('Basic', 'yarn.host') + ':8088'
  
  log = logger.Logger('Benchmark')
  usage = "We want as arguments:\n 1) test data file (required)\n 2) number of repetitions (required)\n"
  if len(sys.argv) < 2:
    log.warn(usage)
    sys.exit(0)

  with open(sys.argv[1], 'r') as test_data:
    data = json.loads(test_data.read())
    initConfigs(data['configs'])
    repetitions = (int(sys.argv[2]) if len(sys.argv) == 3 else 1)
    for x in xrange(repetitions):
      runBenchmark(data['commands'])
  
