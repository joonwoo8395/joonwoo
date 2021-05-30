#!/usr/bin/env bash
#python /home/tacs/SMS-master/SMS.py ServerResource,IrisStatus Filter SMSSKSend /home/tacs/SMS-master/SMS.conf >& /dev/null &
#python /home/tacs/SMS-master/SMS.py ServerResource,IrisStatus,EventFlow Filter SMSSKSend /home/tacs/SMS-master/SMS.conf 

#python /home/eva/SMS-master/SMS.py ServerResource,IrisStatus,IrisOpenLab Filter SMSSend /home/eva/SMS-master/SMS.conf SMS
python /DATA/SMS-master/SMS.py ServerResource,IrisStatus,IrisOpenLab Filter SendEmail /DATA/SMS-master/SMS.conf SMS_MAIL
