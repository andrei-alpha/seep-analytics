# Kill the master
ps aux | grep monitor | grep -v grep | awk '{print $2}' | xargs kill -s abrt
echo 'Killed monitor thread'
