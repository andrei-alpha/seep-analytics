import re
import os
import time
import util

class Metrics(object):
  def __init__(self, config, log, host, path, prefix):
    self.config = config
    self.log = log
    self.host = host
    self.path = path
    self.prefix = prefix  

    self.index = {}
    self.skip_patterns = ['DEBUG', 'TRACE']

  def args(self):
    return (self.host, self.path, self.prefix)

  def update(self, path):
    if path not in self.index:
      #print 'created:', path
      payload = {'event': 'created', 'path': path[len(self.prefix):], 'data': ''}
      util.send(self, payload)
      self.index[path] = 0

    size = os.path.getsize(path)
    if size > self.index[path]:
      with open(path, 'r') as fin:
        fin.seek(self.index[path])
        data = fin.read()

        filteredData = data
        for pattern in self.skip_patterns:
          filteredData = re.sub('[0-9]+:[0-9][^\n=]*' + pattern + '[^\n]*\n', '', filteredData)
        data = filteredData

        #print 'new data:', path, len(data)
        while len(data) and self.working:
          batch = data[:500000]
          data = data[len(batch):]
          payload = {'event': 'new line', 'path': path[len(self.prefix):], 'data': batch}
          util.send(self, payload)

      self.index[path] = size

  def scan(self):
    for root, dirs, files in os.walk(self.path):
      for file in files:
        self.update(root + '/' + file)

  def run(self):
    self.working = True
    while self.working:
      self.scan()
      self.sleep(self.config.getint('monitor.logs.scan.interval'))

  def stop(self):
    self.log.info('Stoping watcher thread...')
    self.working = False

  def sleep(self, seconds):
    cnt = 0
    while self.working and cnt < seconds:
      time.sleep(1)
      cnt += 1