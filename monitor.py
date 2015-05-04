import os
import sys
import requests
import time

index = {}
skip_patterns = ['Sender', 'RecordBatch', 'KafkaProducer', 'RecordAccumulator']

def send(payload):
  success = False
  pause = 4
  while not success:
    try:
      requests.post(host + '/event', data=payload)
      success = True
    except requests.ConnectionError:
      print 'Connection problems. Retrying in', pause, ' sec..'
      time.sleep(pause)
      pause *= 2

def update(host, prefix, path):
  if path not in index:
    #print 'created:', path
    payload = {'event': 'created', 'path': path[len(prefix):], 'data': ''}
    send(payload)
    index[path] = 0

  size = os.path.getsize(path)
  if size > index[path]:
    with open(path, 'r') as fin:
      fin.read(index[path])
      data = fin.read()

      #print 'new data:', path, len(data)
      while len(data):
        batch = data[:50000]
        data = data[len(batch):]
        payload = {'event': 'new line', 'path': path[len(prefix):], 'data': batch}
        send(payload)

    index[path] = size

def scan(host, prefix, path):
  for root, dirs, files in os.walk(path):
    for file in files:
      update(host, prefix, root + '/' + file)

if __name__ == '__main__':
  usage = 'We want as arguments: 1) host 2) port 3) absolute path to logs directory'
  if len(sys.argv) != 4 or not os.path.exists(sys.argv[3]):
    print usage
    exit(0)    

  host = 'http://' + sys.argv[1] + ':' + sys.argv[2]
  path = sys.argv[3]
  prefix = path[:path.find('/logs')]  

  while True:
    scan(host, prefix, path)
    time.sleep(30)
