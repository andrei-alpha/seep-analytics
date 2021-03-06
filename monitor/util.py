import re
import psutil
import requests

# This function will be called from Metrics or Resources class
def send(self, payload):
  success = False
  pause = 1
  while not success and self.working:
    try:
      requests.post(self.host + '/event', data=payload)
      success = True
    except requests.ConnectionError:
      self.log.debug('Connection problems. Retrying in', pause, ' sec..')
      self.sleep(pause)
      pause = min(60, pause * 2)

def getJVMArgs(out, pid):
  args = filter(lambda x: re.search('^' + str(pid) + '\s', x), out)
  if len(args):
    return args[0].split(' ')
  return ''


def killAll(proc_pid):
  try:
    process = psutil.Process(proc_pid)
    for proc in process.get_children(recursive=True):
      proc.kill()
    process.kill()
  except:
    pass