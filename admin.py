import json
import os
import re
import requests
import select
import subprocess
import time

adminCurrentTask = ''
adminProgress = 0

seep_root = '/Users/andrei/Code/SEEPng'
baseYarnWorkerPort = 4500
baseYarnMasterPort = 6000
hosts_names = ['andrei-mbp']
hosts = map(lambda x: 'http://' + x + ':7008', hosts_names)

def sendCommand(host, command):
    if not host:
        for host in hosts:
            try:
                requests.post(host + '/command', data={'command': command})
            except requests.ConnectionError:
                print 'Failed for host:', host
    else:
        try:
            return requests.post(host + '/command', data={'command': command})
        except requests.ConnectionError:
            print 'Failed for host:', host

def getAvailableQueries():
    out = subprocess.check_output(['ls', seep_root + '/deploy']).split('\n')
    out = filter(lambda x: '.jar' in x, out)
    out = map(lambda x: re.sub('/.*/', '', x), out)
    return filter(lambda x: 'seep-master' not in x and 'seep-worker' not in x, out)

def killAllSeepQueries():
    global adminCurrentTask, adminProgress
    adminCurrentTask = 'Stopping all seep queries...'
    adminProgress = 0

    # Count number of running seep queries
    total = 0
    for host in hosts:
        res = sendCommand(host, 'jps')
        total += len(re.findall('(?<=\s)Main(?=\n)', res.text))

    running = total
    count = 3
    while running > 0 and count > 0:
        running = 0
        for host in hosts:
            sendCommand(host, 'bash ' + seep_root + '/deploy/killall.sh')
            res = sendCommand(host, 'jps')
            running += len(re.findall('(?<=\s)Main(?=\n)', res.text))
        count -= 1
        time.sleep(0.5)
        adminProgress = max(adminProgress, int(max(0, (1.0 - float(running) / float(total))) * 100.0))

    adminCurrentTask = 'Stopping all seep queries - Done'
    adminProgress = 100

def unblockingRead(proc, retVal=''): 
  while (select.select([proc.stdout],[],[],0)[0]!=[]):   
    retVal+=proc.stdout.read(1)
  return retVal

def submitQuery(queryName, deploymentSize):
    global adminCurrentTask, adminProgress
    global baseYarnWorkerPort, baseYarnMasterPort
    adminCurrentTask = 'Deploy ' + str(deploymentSize) + ' seep queries...'
    adminProgress = 0

    # Just a hack to measure progress, 5 is the number of prints containing SeepYarnAppSubmissionClient
    totalSteps = deploymentSize * 5
    steps = 0
    for i in xrange(deploymentSize):
        baseYarnWorkerPort += 5
        baseYarnMasterPort += 5
        process = subprocess.Popen(['bash', 'yarn.sh', queryName, str(baseYarnWorkerPort), str(baseYarnMasterPort)], cwd=seep_root + '/deploy',  stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        while steps < 5 * (i + 1):
            if not process.poll():
                out = unblockingRead(process)
                steps += len(re.findall('SeepYarnAppSubmissionClient', out))
            adminProgress = int(float(steps) / float(totalSteps) * 100.0)

    adminCurrentTask = 'Deploy seep queries...'
    adminProgress = 100

def reset():
    adminCurrentTask = ''
    adminProgress = 0

def status():
    return {'current': adminCurrentTask, 'progress': adminProgress}
