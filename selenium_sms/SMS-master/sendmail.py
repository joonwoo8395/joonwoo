#!/usr/bin/env python

import smtplib
from email.mime.text import MIMEText

def sendMail(me, to, msg):
    smtp = smtplib.SMTP('shield.mobigen.com', 25)
    #smtp.helo()
    #smtp.starttls()
    smtp.login(me, 'suk852456')
    msg = MIMEText(msg)
    msg['Subject'] = 'TEST'
    smtp.sendmail(me, to, msg.as_string())
    smtp.quit()

sendMail('suk555@mobigen.com', 'kim2100@gmail.com', 'sendmail test')
