import json
import os
import re
import requests
import select
import subprocess
import time

class Globals:
    startTimestamp = None
    adminCurrentTask = ''
    adminProgress = 0
    expectedTime = None
    baseProgress = None
    allocatedPercentage = None
    clusterInfo = None
    timeEstimations = {
        'Stopping all seep queries...': 2,
        'Fetching origin...': 1,
        'Applying changes...': 1,
        'Compiling seep...': 6,
        'Compiling examples...': 7,
        'default': 5
    }

# Need to be given as sys argument
seep_root = None
analytics_root = None
hosts_names = None
hosts = None

baseYarnWorkerPort = 4500
baseYarnMasterPort = 6000

def updateProgress(val, resetVal=False):
    if resetVal:
        Globals.adminProgress = val
    Globals.adminProgress = max(Globals.adminProgress, min(val, 100))

def getProgress():
    if Globals.allocatedPercentage:
        now = time.time()
        res = int(Globals.baseProgress + min(1.0, float(now - Globals.startTimestamp) / Globals.expectedTime) * Globals.allocatedPercentage)
        print 'progress:', Globals.adminProgress, ' simulatedProgress:', res, 'expectedTime:', Globals.expectedTime, 'elapsedTime:', int(now - Globals.startTimestamp)
        return int(Globals.baseProgress + min(1.0, float(now - Globals.startTimestamp) / Globals.expectedTime) * Globals.allocatedPercentage)
    return Globals.adminProgress

def updateTask(task, taskAllocatedPercentage = None):
    now = time.time()
    if Globals.startTimestamp and Globals.adminCurrentTask:
        deltaTime = int(now - Globals.startTimestamp)
        print Globals.adminCurrentTask, 'took', deltaTime, 'seconds'
        # Update time estimations if required
        if Globals.adminCurrentTask in Globals.timeEstimations and deltaTime > 1:
            Globals.timeEstimations[Globals.adminCurrentTask] = deltaTime
    if taskAllocatedPercentage:
        Globals.allocatedPercentage = taskAllocatedPercentage
        Globals.baseProgress = Globals.adminProgress
        Globals.expectedTime = Globals.timeEstimations[task] if task in Globals.timeEstimations else Globals.timeEstimations['default']
        Globals.startTimestamp = now
    else:
        Globals.allocatedPercentage = None

    Globals.adminCurrentTask = task

def sendCommand(host, command, cwd='.'):
    if not host:
        for host in hosts:
            try:
                requests.post(host + '/command', data={'command': command, 'cwd': cwd}).text
            except requests.ConnectionError:
                print 'Failed for host:', host
    else:
        try:
            return requests.post(host + '/command', data={'command': command, 'cwd': cwd}).text
        except requests.ConnectionError:
            print 'Failed for host:', host

def sendRequest(host, path, data, type):
    if type == 'get':
        try:
            return requests.get(host + path)
        except requests.ConnectionError:
            print 'Failed for host:', host
    else:
        try:
            return requests.post(host + path, data=data)
        except requests.ConnectionError:
            print 'Failed for host:', host

def getStatus(host, readAll = False):
    out = requests.get(host + '/status').text
    while readAll:
        res = requests.get(host + '/status').text
        if res == 'pending':
            time.sleep(1)
            continue
        if not res or len(res) == 0:
            break
        out += res
    return out

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
    updateProgress(0, True)
    updateTask('Stopping all seep queries...', 100)

    # Count number of running seep queries
    total = 0
    for host in hosts:
        sendCommand(host, 'jps')
        res = getStatus(host, True)
        total += len(re.findall('(?<=\s)Main(?=\n)', res))

    running = total
    count = 3
    while running > 0 and count > 0:
        running = 0
        for host in hosts:
            sendCommand(host, 'bash ' + seep_root + '/deploy/killall.sh')
            getStatus(host, True)
            sendCommand(host, 'jps')
            res = getStatus(host, True)
            running += len(re.findall('(?<=\s)Main(?=\n)', res))
        count -= 1
        time.sleep(0.5)
        updateProgress((1.0 - float(running) / float(total)) * 100.0)

    updateTask('Stopping all seep queries - Done')
    updateProgress(100)

def updateAnalytics(branch):
    updateProgress(0, True)
    updateTask('Fetching origin...', 50)

    steps = 2
    for host in hosts:
        sendCommand(host, 'git fetch --all', analytics_root)
        getStatus(host, True)
        updateProgress(Globals.adminProgress + (1.0 / (steps * len(hosts))) * 100)
    
    # TODO find a way to update worker as well
    updateTask('Applying changes...', 50)
    for host in hosts:
        sendCommand(host, 'git reset --hard origin/' + branch, analytics_root)
        updateProgress(Globals.adminProgress + (1.0 / (steps * len(hosts))) * 100)

    updateTask('Updating Analytics - Done')
    updateProgress(100)

def updateSeep(branch):
    updateProgress(0, True)

    steps = 9
    updateTask('Fetching origin...', 10)
    for host in hosts:
        sendCommand(host, 'git fetch --all', seep_root)
        getStatus(host, True)
        updateProgress(Globals.adminProgress + (1.0 / (steps * len(hosts))) * 100)
    updateTask('Applying changes...', 10)
    for host in hosts:
        sendCommand(host, 'git reset --hard origin/' + branch, seep_root)
        getStatus(host, True)
        updateProgress(Globals.adminProgress + (1.0 / (steps * len(hosts))) * 100)
    updateTask('Compiling examples...', 40)
    for host in hosts:
        sendCommand(host, 'bash sync.sh', seep_root + '/deploy')
    for host in hosts:
        getStatus(host, True)
        updateProgress(Globals.adminProgress + (4.0 / (steps * len(hosts))) * 100)
    updateTask('Compiling seep...', 30)
    for host in hosts:
        sendCommand(host, './gradlew installApp', seep_root)
    for host in hosts:
        getStatus(host, True)
        updateProgress(Globals.adminProgress + (3.0 / (steps * len(hosts))) * 100)

    updateTask('Updating Seep - Done')
    updateProgress(100)

def unblockingRead(proc, retVal=''):
    if not proc.poll():
        while (select.select([proc.stdout],[],[],0)[0]!=[]):
            retVal+=proc.stdout.read(1)
        return retVal
    else:
        return proc.communicate()[0]

def submitQuery(queryName, deploymentSize):
    global baseYarnWorkerPort, baseYarnMasterPort
    updateTask('Deploy ' + str(deploymentSize) + ' seep queries...', 100)
    updateProgress(0, True)

    # Just a hack to measure progress, 5 is the number of prints containing SeepYarnAppSubmissionClient
    totalSteps = deploymentSize * 5
    steps = 0
    for i in xrange(deploymentSize):
        baseYarnWorkerPort += 5
        baseYarnMasterPort += 5
        process = subprocess.Popen(['bash', 'yarn.sh', queryName, str(baseYarnWorkerPort), str(baseYarnMasterPort)], cwd=seep_root + '/deploy',  stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        while steps < 5 * (i + 1):
            out = unblockingRead(process)
            steps += len(re.findall('SeepYarnAppSubmissionClient', out))
            updateProgress(float(steps) / float(totalSteps) * 100.0)

    updateTask('Deploy seep queries...')
    updateProgress(100)

def clearKafkaLogs():
    updateTask('Deleting Kafka Logs...', 100)
    for host in hosts:
        sendCommand(host, 'bash kafka-log-cleaner.sh', seep_root + '/deploy')
    for host in hosts:
        getStatus(host, True)
        updateProgress(Globals.adminProgress + 1.0 / len(hosts))

    updateTask('Deleting Kafka Logs - Done')
    updateProgress(100)

def clearHadoopLogs():
    updateTask('Deleting Hadoop Logs...', 100)
    # Assume that HADOOP_PREFIX is set up
    hadoop_prefix = os.environ["HADOOP_PREFIX"]
    for host in hosts:
        sendCommand(host, 'bash hadoop-log-cleaner.sh', hadoop_prefix)
    for host in hosts:
        getStatus(host, True)
        updateProgress(Globals.adminProgress + 1.0 / len(hosts))

    updateTask('Deleting Hadoop Logs - Done')
    updateProgress(100)

def clusterInfo():
    cInfo = {}
    for host in hosts:
        sendRequest(host, '/info', {'cwd': seep_root + '/deploy'}, "post")
    for host in hosts:
        res = sendRequest(host, '/info-status', None, "get")
        if not res:
            continue
        info = res.json()
        if not cInfo:
            cInfo = info
        elif info:
            cInfo = {
                'memory': [sum(x) for x in zip(cInfo['memory'], info['memory'])],
                'logs': [sum(x) for x in zip(cInfo['logs'], info['logs'])],
                'cpu': [sum(x) for x in zip(cInfo['cpu'], info['cpu'])]
            }
    if cInfo:
        Globals.clusterInfo = {
            'total_mem': cInfo['memory'][0],
            'total_cpus': len(hosts) * 4,
            'cpu_usage': float(cInfo['cpu'][0]) / len(hosts),
            'mem_usage': float(cInfo['memory'][1]) / len(hosts),
            'kafka_logs': cInfo['logs'][0] * 1024,
            'hadoop_logs': cInfo['logs'][1] * 1024,
        }
    else:
        Globals.clusterInfo = {'status': 'failed'}

def reset():
    updateTask('')
    updateProgress(0, True)

def status():
    return {'current': Globals.adminCurrentTask, 'progress': getProgress()}
