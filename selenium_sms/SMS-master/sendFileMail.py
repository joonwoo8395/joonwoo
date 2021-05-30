#!/usr/bin/env python
# -*- coding: utf-8 -*- 

import smtplib
import sys
from email.mime.text import MIMEText

reload(sys)
sys.setdefaultencoding('utf-8')

class SendFileMail() :
	def __init__(self, Parser) :
		self.Parser		= Parser
		self.fromMailAddr 	= Parser.get('EMAIL', 'FROM_MAIL')
		self.toMailAddr 	= Parser.get('EMAIL', 'TO_MAIL').split(',')

	def sendMail(self, me, to, msg, NokCount) :
		smtp = smtplib.SMTP('shield.mobigen.com', 25)
		#smtp.helo()
		#smtp.starttls()
		smtp.login(me, 'suk852456')
#		msg = ''
#		with open('mail.txt', 'r') as mailFile :
#			msg = mailFile.read()
	
	#	msg = 'test'
	
		msg = MIMEText(msg)
		msg['Subject'] = '[Road Construction] %s [ NokCount : %s ]' % ('Mail Check' ,str(NokCount))
		smtp.sendmail(me, to, msg.as_string())
		smtp.quit()

	def run(self, msg, NokCount) :
		for oneMail in self.toMailAddr :
			self.sendMail(self.fromMailAddr, oneMail, msg, NokCount)

#sendMail('suk555@mobigen.com', 'suk555@mobigen.com')
