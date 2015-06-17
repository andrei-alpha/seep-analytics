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