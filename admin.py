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
analytics_root = '/Users/andrei/Code/watcher'
baseYarnWorkerPort = 4500
baseYarnMasterPort = 6000
hosts_names = ['andrei-mbp']
hosts = map(lambda x: 'http://' + x + ':7008', hosts_names)

def sendCommand(host, command, cwd='.'):
    if not host:
        for host in hosts:
            try:
                requests.post(host + '/command', data={'command': command, 'cwd': cwd})
            except requests.ConnectionError:
                print 'Failed for host:', host
    else:
        try:
            return requests.post(host + '/command', data={'command': command, 'cwd': cwd})
        except requests.ConnectionError:
            print 'Failed for host:', host

def getAvailableOptions():
    options = {}
    out = subprocess.check_output(['ls', seep_root + '/deploy']).split('\n')
    out = filter(lambda x: '.jar' in x, out)
    out = map(lambda x: re.sub('/.*/', '', x), out)
    out = filter(lambda x: 'seep-master' not in x and 'seep-worker' not in x, out)
    options['available-queries'] = out
    out = subprocess.check_output(['git', 'branch'], cwd=analytics_root).split('\n')
    out = map(lambda x: re.sub('[^a-zA-z-_]', '', x), out)[:-1]
    options['analytics-branches'] = out
    out = subprocess.check_output(['git', 'branch'], cwd=seep_root).split('\n')
    out = map(lambda x: re.sub('[^a-zA-z-_]', '', x), out)[:-1]
    options['seep-branches'] = out
    return options
 
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

def updateAnalytics(branch):
    global adminCurrentTask, adminProgress
    adminProgress = 0

    adminCurrentTask = 'Fetching origin...'
    for host in hosts:
        sendCommand(host, 'git fetch --all', analytics_root)
        adminProgress += (1.0 / (2.0 * len(hosts))) * 100
    
    # TODO find a way to update worker as well
    adminCurrentTask = 'Applying changes...'
    for host in hosts:
        sendCommand(host, 'git reset --hard origin/' + branch, analytics_root)
        adminProgress += (1.0 / (2.0 * len(hosts))) * 100

    adminCurrentTask = 'Applying changes - Done'
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
    return {'current': adminCurrentTask, 'progress': int(adminProgress)}
