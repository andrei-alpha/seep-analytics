import os
import time
import logger
import logging
import requests
import configparser
import subprocess

from kazoo.client import KazooClient
from kazoo.client import KazooState
from kazoo.protocol.states import EventType

class LeaderElection(object):
  def __init__(self):
    # Send leader proposal
    self.leader = False
    self.path = zk.create('/supervisor/leader/offer_', ephemeral=True, sequence=True, makepath=True)
    self.findSmallerNode()
    
    if not self.leader:
      log.info('Waiting to become leader...')
    while not self.leader:
      time.sleep(1)

  def findSmallerNode(self):
    seqNumber = self.path[25:]
    while int(seqNumber) > 0:
      seqNumber = str(int(seqNumber)-1).zfill(len(seqNumber))
      if zk.exists('/supervisor/leader/offer_' + seqNumber):
        zk.exists('/supervisor/leader/offer_' + seqNumber, watch=self.leaderWatch)
        self.path = '/supervisor/leader/offer_' + seqNumber
        return
    self.leader = True

  def executeLeader(self):
    while True:
      time.sleep(5)

  def leaderWatch(self, event):
    if event.type == EventType.DELETED:
      self.findSmallerNode()

def sessionWatch(state):
  if state == KazooState.LOST:
    # Register somewhere that the session was lost
    log.warn('Session Lost.')
  elif state == KazooState.SUSPENDED:
    # Handle being disconnected from Zookeeper
    log.warn('Session Suspended.')
  else:
    # Handle being connected/reconnected to Zookeeper
    log.warn('Session reconnected.')

def pingServer(address, name):
  timeout = 1
  while timeout < 10:
    try:
      r = requests.get('http://' + address, timeout=timeout)
    except requests.ConnectionError:
      log.info('Server', address, 'is down. Restarting server...')
      subprocess.Popen(['screen', '-d', '-m', '-S', name, 'python', name + '.py'])
    except requests.exceptions.ReadTimeout:
      timeout *= 2
      continue
    return

def executeLeader():
  log.info("I am the leader now!")

  server_host = config.get('Basic', 'server.host')
  server_port = config.get('Basic', 'server.port')
  scheduler_port = config.get('Basic', 'scheduler.port')
  while True:
    pingServer(server_host + ':' + server_port, 'server')
    pingServer(server_host + ':' + scheduler_port, 'scheduler')
    time.sleep(5)

if __name__ == "__main__":
  log = logger.Logger('supervisor')
  config = configparser.SafeConfigParser()
  config.read('analytics.properties')
  zk = KazooClient(hosts=config.get('Basic', 'zookeeper.host'))
  zk.start()
  zk.add_listener(sessionWatch)
  # Send leader proposal
  election = LeaderElection()  
  executeLeader()
  