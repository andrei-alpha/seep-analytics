import os
import re
import sys
import subprocess
import requests
import time
import threading

from socket import gethostname
from bottle import Bottle, response, request

app = Bottle()
globalMonitor = None
globalProcess = None

class Monitor:
  def __init__(self, argvs):
    usage = 'We want as arguments: 1) host 2) port 3) absolute path to logs directory'
    if len(argvs) != 4 or not os.path.exists(argvs[3]):
      print usage
      return

    self.host = 'http://' + argvs[1] + ':' + argvs[2]
    self.path = argvs[3]
    self.prefix = self.path[:self.path.find('/logs')]  

    self.index = {}
    self.skip_patterns = ['DEBUG', 'TRACE']

  def send(self, payload):
    success = False
    pause = 1
    while not success and self.working:
      try:
        requests.post(self.host + '/event', data=payload)
        success = True
      except requests.ConnectionError:
        print 'Connection problems. Retrying in', pause, ' sec..'
        self.sleep(pause)
        pause *= 2

  def update(self, path):
    if path not in self.index:
      #print 'created:', path
      payload = {'event': 'created', 'path': path[len(self.prefix):], 'data': ''}
      self.send(payload)
      self.index[path] = 0

    size = os.path.getsize(path)
    if size > self.index[path]:
      with open(path, 'r') as fin:
        fin.read(self.index[path])
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
          self.send(payload)

      self.index[path] = size

  def scan(self):
    for root, dirs, files in os.walk(self.path):
      for file in files:
        self.update(root + '/' + file)

  def run(self):
    self.working = True
    #while self.working:
    #  self.scan()
    #  self.sleep(30)

  def stop(self):
    print 'Stoping monitor thread...'
    self.working = False

  def sleep(self, seconds):
    cnt = 0
    while self.working and cnt < seconds:
      time.sleep(1)
      cnt += 1

def startMonitorThread():
  global globalMonitor
  globalMonitor = Monitor(sys.argv)
  t = threading.Thread(target=globalMonitor.run)
  t.deamon = True
  t.start()

# Main entry point
if __name__ == '__main__':
  startMonitorThread()

@app.route('/command', method='post')
def server_command():
  command = request.forms.get('command').split(' ')
  cwd = request.forms.get('cwd')
  if command == ['reset']:
    global globalMonitor
    globalMonitor.stop()
    startMonitorThread()
  else:
    global globalProcess
    print 'run', command
    globalProcess = subprocess.Popen(command, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    #return subprocess.check_output(command, cwd=cwd)

@app.route('/status', method='get')
def server_status():
  global globalProcess
  if not globalProcess:
    return None
  if globalProcess.poll():
    while (select.select([globalProcess.stdout],[],[],0)[0]!=[]):
      retVal+=globalProcess.stdout.read(1)
    return retVal
  else:
    out = globalProcess.communicate()[0]
    globalProcess = None
    return out

app.run(host=gethostname(), port=7008, reloader=False)
globalMonitor.stop()