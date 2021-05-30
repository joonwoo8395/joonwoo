# -*- coding: utf-8 -*-
#!/usr/bin python

import sys
import signal
import os
import json
from Collect.IrisStatus import *
from Collect.ServerResource import *
from Collect.IrisOpenLab import *
from Filter.Filter import *
from Noti.SMSSend import *
from Noti.SendEmail import *
from SendFileMail import *
import Mobigen.Common.Log as Log
import ConfigParser

#from Noti.NotiDB import *

class ServerManagementService() :
	def __init__(self) :
		self.usege()
		self.GetParser()
		self.GetConfig()
		self.SetLog()
		self.SetSignal()

	def GetParser(self) :
		try :
			self.PARSER = ConfigParser.ConfigParser()
			self.PARSER.read(sys.argv[4])
		except Exception, ex:
			__LOG__.Exception()
			sys.exit()

	def GetConfig(self) :
		#SSH Info
		self.ServerDict = {}
		
		#LOG
		self.LOG_PATH = self.PARSER.get('Log','LogFilePath')
		self.LOG_SIZE = int(self.PARSER.get('Log','LogFileSize'))
		self.LOG_COUNT = int(self.PARSER.get('Log','LogFlieCount'))

	def SetLog(self) :
		Log_file = os.path.join(self.LOG_PATH, "%s.log" % sys.argv[5])
		Log.Init(Log.CRotatingLog(os.path.expanduser(Log_file), self.LOG_SIZE, self.LOG_COUNT))

	def usege(self) :
		if len(sys.argv) != 6 :
			print 'usage : Collect Filter Noti ConfigFile' % ( sys.argv[0] )
			sys.exit()
	
	def SetSignal(self) :
		self.SHUTDOWN = False
		signal.signal(signal.SIGTERM, self.Shutdown)
		signal.signal(signal.SIGINT, self.Shutdown)
		signal.signal(signal.SIGHUP, self.Shutdown)
		signal.signal(signal.SIGPIPE, self.Shutdown)
	
	def Shutdown(self, sigNum=0, frame=0):
		#__LOG__.Trace('self.SHUTDOWN')
		self.SHUTDOWN = True

	def run(self) :
		__LOG__.Trace('SMS-matser Start !! ')
		try :
			if sys.argv[2] != 'None' :
				FilInst = eval(sys.argv[2])
			ColInstList = []
			NotiInstList = []
			mailMsg	= ''
			NokCount = 0
	
			CollectList = sys.argv[1].split(',')
			
			if self.SHUTDOWN :
				__LOG__.Trace("Shutdown : %s " % self.SHUTDOWN)
				return

			### Collect ###
			dict = {}
			for Collect in CollectList :
				ColInst = eval(Collect)  # eval 함수로 객체를 생성해줌
#				ColInst	= getattr(Collect)
				Colobj = ColInst(self.PARSER)
				tempdict = Colobj.run()
				
				#__LOG__.Trace(tempdict)
				for ip in tempdict.keys() :
					if not dict.has_key(ip) : dict[ip] = tempdict[ip] # dict 초기화
					else :
						for Col in tempdict[ip].keys() :
							if not dict[ip].has_key(Col) : dict[ip][Col] = tempdict[ip][Col]

			NotiInst = eval(sys.argv[3])

			if self.SHUTDOWN :
				__LOG__.Trace("Shutdown : %s " % self.SHUTDOWN)
				return
		
			
			### Filter ###
			if sys.argv[2] != 'None' :
				#for dict in li :
				Filobj = FilInst(self.PARSER, dict)
				dict = Filobj.run()

			if self.SHUTDOWN :
				__LOG__.Trace("Shutdown : %s " % self.SHUTDOWN)
				return
			
			### NOTI ###
			if sys.argv[3] != 'None' :
				#for dict in li :
				Notiobj 		= NotiInst(self.PARSER, dict, CollectList)
				Notiobj.run()
			#__LOG__.Trace(dict)

#			sendEmail = SendFileMail(self.PARSER)
#			sendEmail.run(mailMsg, NokCount)
				

		except :
			__LOG__.Exception()

def Main() :
	obj = ServerManagementService()
	obj.run()

if __name__ == '__main__' :
	Main()	
