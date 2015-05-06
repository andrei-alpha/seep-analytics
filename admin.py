import os
import re
import subprocess

adminCurrentTask = ''
adminProgress = 0

def killAllSeepQueries():
	adminCurrentTask = 'Run kill script'

	# Count number of running seep queries
	out = subprocess.check_output('bash /home/aba111/multi.sh jps')
	total = len(re.findall('Main', out))
	running = total
	while running > 0:
		subprocess.call('bash /home/aba111/multi.sh "bash /home/aba111/SEEPng/deploy/killall.sh"')
		out = subprocess.check_output('bash /home/aba111/multi.sh jps')
		print out
		running = len(re.findall('Main', out))
		adminProgress = int(float(running) / float(total) * 100.0)
		break

def status():
	return {'current': adminCurrentTask, 'progress': adminProgress}

if __name__ == "__main__":
	killAllSeepQueries()