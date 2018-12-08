ps -aef | grep mr_worker | awk -F ' ' '{print $2}' | xargs echo kill -9
ps -aef | grep mr_worker | awk -F ' ' '{print $2}' | xargs kill -9
