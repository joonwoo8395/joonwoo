#!/usr/bin/env bash
#python /home/tacs/SMS-master/SMS.py ServerResource,IrisStatus Filter SMSSKSend /home/tacs/SMS-master/SMS.conf >& /dev/null &
#python /home/tacs/SMS-master/SMS.py ServerResource,IrisStatus,EventFlow Filter SMSSKSend /home/tacs/SMS-master/SMS.conf 

python /home/tacs/SMS-master/SMS.py ServerResource,IrisStatus,EventFlow,LinkSystemStatus,TacsLogMonitor Filter SMSSKSend /home/tacs/SMS-master/SMS.conf SMS_Common
