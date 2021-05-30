#!/usr/bin/env python
# -*- coding: utf-8 -*-
import datetime
import os
import sys
import signal
import ConfigParser
import glob
import json
import Mobigen.Common.Log as Log; Log.Init()
import shutil
import subprocess
import SelectToIris
import socket

from apscheduler.schedulers.blocking import BlockingScheduler

# Process Shutdown Flag

SHUTDOWN = False

NETSTAT_LOCAL_ADDR_IDX 		= 3
NETSTAT_FOREIGN_ADDR_IDX	= 4
NETSTAT_STATE_IDX			= 5

MQ_PORT	= 5671

# @param signum 
# @param frame -
def handler(signum, frame):
	global SHUTDOWN
	SHUTDOWN = True
	__LOG__.Trace("signal: process shutdown")


# SIGTERM 
signal.signal(signal.SIGTERM, handler)
# SIGINT 
signal.signal(signal.SIGINT, handler)
# SIGHUP 
signal.signal(signal.SIGHUP, handler)
# SIGPIPE 
signal.signal(signal.SIGPIPE, handler)

# JSON 
class MQConnect:
	def __init__(self, cfg, section) :
		self.irisQuery = SelectToIris.SelectToIris(cfg)
		self.scheduleInterval	= cfg.get(section, 'SCHEDULER_INTERVAL')
		self.auditPath			= cfg.get(section, 'AUDIT_FILE_PATH')
#		self.ANIMP_CONNECT_IP	= cfg.get(section, 'ANIMP_CONNECT_IP')
#		self.ANIMP_IP			= cfg.get(section, 'ANIMP_IP')
		self.NETSTAT_CMD		= cfg.get(section, 'NETSTAT_CMD')
		self.scheduleSec		= cfg.get(section, '{}_SCHEDULER_SECOND'.format(self.getHostName()))

		self.AUDIT_DICT_LIST	= list()

		self.UPDATE_CON_LIST	= list()
		self.UPDATE_DISCON_LIST	= list()
		self.FIRST_CONN_LIST	= list()

	def getHostName(self) :
		hostName = ''
		try :
			hostName = socket.gethostname()
			__LOG__.Trace('Server Host : {}'.format(hostName))
		except :
			__LOG__.Exception
		return hostName.upper()

	def stdout(self, msg):
		sys.stdout.write("stdout" + msg + '\n')
		sys.stdout.flush()
		__LOG__.Trace("std OUT: %s" % msg)

	def connectEms(self) :
		try :
			netstatStr 	= subprocess.check_output(self.NETSTAT_CMD % MQ_PORT, shell=True)
			__LOG__.Trace(netstatStr)
			netstatDict = {}

			if netstatStr != None and netstatStr != '' :
				netstatList = netstatStr.split('\n')

				for oneNetstatList in netstatList :
					oneNetstat = oneNetstatList.split(' ')
					netstatResult = [x for x in oneNetstat if x]
					if len(netstatResult) > 5 :
						connectPort = netstatResult[NETSTAT_LOCAL_ADDR_IDX].split(':')[-1]
	
						if MQ_PORT == int(connectPort) :
							netstatDict[netstatResult[NETSTAT_FOREIGN_ADDR_IDX].split(':')[-2]] = netstatResult[NETSTAT_STATE_IDX]

						else :
							__LOG__.Trace('port : %s' % connectPort)

			return netstatDict

		except :
			__LOG__.Exception()

	def mkdirs(self, path) :
		try : os.makedirs(path)
		except OSError as exc: #Python > 2.5
		#if exc.errno == errno.EEXIST and os.path.isdir(path) :
			pass
		#else : raise

	def auditFileTrantmit(self) :
		self.mkdirs(self.auditPath)
		nowDate			= datetime.datetime.now()
		fileNameDate	= nowDate.strftime('%Y%m%d_%H%M_')	
		filePath		= os.path.join( self.auditPath, fileNameDate + '.audit' )

		__LOG__.Trace('Modify EMS Status Cnt : %s' % str(len(self.AUDIT_DICT_LIST)))

		with open(filePath, 'w') as auditFile :
			for idx, oneAuditDict in enumerate(self.AUDIT_DICT_LIST) :
				if idx+1 == len(self.AUDIT_DICT_LIST) :
					auditFile.write( json.dumps(oneAuditDict, ensure_ascii=False) )
				else :
					auditFile.write('%s\n' % json.dumps(oneAuditDict, ensure_ascii=False) )

		del self.AUDIT_DICT_LIST[:]

		__LOG__.Trace('auditFile Create')

	def auditFileCreate(self, oneEms, connectCheck) :
		auditDict		= dict()
		nowDate			= datetime.datetime.now()
		auditDict['evntTypCd'] 	= '401'
		auditDict['evntDate']	= nowDate.strftime('%Y%m%d%H%M%S')
		auditDict['lnkgEqpIp']	= oneEms['emsIp']
		auditDict['etcItem_02']	= connectCheck

		self.AUDIT_DICT_LIST.append(auditDict)

	def updateQuery (self, oneEms, nowDate, connectCheck, dbEmsConnCheck) :
#		if oneEms['emsIp'] == self.ANIMP_CONNECT_IP :
#			oneEms['emsIp'] = self.ANIMP_IP
#			__LOG__.Trace('%s : ReTransfer -> %s' % (self.ANIMP_CONNECT_IP, self.ANIMP_IP) )

		if (oneEms['firstConnDate'] == None or oneEms['firstConnDate'] == '') and connectCheck == 'Y' :
			self.FIRST_CONN_LIST.append(oneEms['emsIp'])

		if connectCheck == 'Y' :
			self.UPDATE_CON_LIST.append(oneEms['emsIp'])

		elif connectCheck == 'N' and dbEmsConnCheck == 'Y' :
			self.UPDATE_DISCON_LIST.append(oneEms['emsIp'])

		elif connectCheck == 'N' and ( dbEmsConnCheck == '' or dbEmsConnCheck == None ) :
			 self.UPDATE_DISCON_LIST.append(oneEms['emsIp'])

		else :
			__LOG__.Trace('Connection Status invalid')

	def emsConnectCheck(self, netstatDict, dbEmsList) :
		nowDate	= datetime.datetime.now().strftime('%Y%m%d%H%M')
		netstatDictKeys = netstatDict.keys()

		__LOG__.Trace(len(dbEmsList))

		for oneEms in dbEmsList :
#			if self.ANIMP_IP == oneEms['emsIp'] :
#				oneEms['emsIp'] = self.ANIMP_CONNECT_IP
#				__LOG__.Trace('%s : Transfer -> %s' % (self.ANIMP_IP, self.ANIMP_CONNECT_IP) )

			__LOG__.Trace('netstat check')
			connectCheck 	= ''
			if not oneEms['emsIp'] in netstatDictKeys :
				connectCheck	= 'N'

				self.updateQuery (oneEms, nowDate, connectCheck, oneEms['connYn'])

				__LOG__.Trace('%s is DisConnect-1' % oneEms['emsIp'])
			else :
				if 'ESTABLISHED' == netstatDict[ oneEms['emsIp'] ] :
					connectCheck	= 'Y'

					self.updateQuery (oneEms, nowDate, connectCheck, oneEms['connYn'])
					__LOG__.Trace('Connect Ems %s' % oneEms['emsIp'])
				
				else :
					connectCheck 	= 'N'
	
					self.updateQuery (oneEms, nowDate, connectCheck, oneEms['connYn'])
					__LOG__.Trace('Ems Connect State : %s ' % netstatDict[ oneEms['emsIp'] ] )
					__LOG__.Trace('%s is DisConnect-2'  % oneEms['emsIp'])
	
			if oneEms['connYn'] != connectCheck and ( oneEms['connYn'] != '' and oneEms['connYn'] != None ) :
				self.auditFileCreate(oneEms, connectCheck)

			else :
				__LOG__.Trace('EmsConnect Check Not Update')

		if self.UPDATE_CON_LIST :
			self.irisQuery.updateConn(self.UPDATE_CON_LIST, nowDate)
		if self.UPDATE_DISCON_LIST :
			self.irisQuery.updateDisConn(self.UPDATE_DISCON_LIST)
		if self.FIRST_CONN_LIST :
			self.irisQuery.updateFirDate(self.FIRST_CONN_LIST, nowDate)

		__LOG__.Trace('UPDATE Conn Count : %s' % str(len(self.UPDATE_CON_LIST)))
		__LOG__.Trace(self.UPDATE_CON_LIST)
		__LOG__.Trace('UPDATE Dis Conn Count : %s' % str(len(self.UPDATE_DISCON_LIST)))
		__LOG__.Trace(self.UPDATE_DISCON_LIST)
		__LOG__.Trace('UPDATE First Conn Count : %s' % str(len(self.FIRST_CONN_LIST)))
		__LOG__.Trace(self.FIRST_CONN_LIST)


		del self.UPDATE_CON_LIST[:]
		del self.UPDATE_DISCON_LIST[:]
		del self.FIRST_CONN_LIST[:]

		__LOG__.Trace(self.AUDIT_DICT_LIST)

		if self.AUDIT_DICT_LIST :
			self.auditFileTrantmit()
		
	def schedulerExe(self) :
		__LOG__.Trace('scedulere execute')

		nowDate = datetime.datetime.now().strftime('%Y%m%d%H%M')
		checkCnt = self.irisQuery.selectCheckTime(nowDate)

		__LOG__.Trace(checkCnt)

		if 1 == int(checkCnt) :
			__LOG__.Trace('Already Check')
			self.irisQuery.deleteCheck(nowDate)

		elif 0 == int(checkCnt) :
			self.irisQuery.insertCheck(nowDate)

		#	self.auditFileCreate({'emsIp':'1.1.1.1'}, 'Y')
			netstatDict = {}
			dbEmsIpList	= []
	 		netstatDict = self.connectEms()

			if not netstatDict :
				__LOG__.Trace('MQ Port Connect None')
				return
			try :
				dbEmsList = self.irisQuery.selectEmsIp()

				self.emsConnectCheck(netstatDict, dbEmsList)

			except :
				__LOG__.Exception()

		else :
			__LOG__.Trace('Check Balancing Error')
		
	def run(self):
		__LOG__.Trace('MQConnect Start!')	
		
		try :
			self.scheduler	= BlockingScheduler()
			self.scheduler.add_job(self.schedulerExe, 'cron', minute='*/%s' % self.scheduleInterval, second = self.scheduleSec)
			self.scheduler.start()

		except :
			__LOG__.Exception()

def main():
	print '111'
	module 	= os.path.basename(sys.argv[0])
	section = sys.argv[1] # MQConnect
	cfgfile = sys.argv[2] 

	cfg = ConfigParser.ConfigParser()
	cfg.read(cfgfile)

	logPath = cfg.get("GENERAL", "LOG_PATH")
	logFile = os.path.join(logPath, "%s_%s.log" % (module, section))

	logCfgPath = cfg.get("GENERAL", "LOG_CONF")

	logCfg = ConfigParser.ConfigParser()
	logCfg.read(logCfgPath)
	Log.Init(Log.CRotatingLog(logFile, logCfg.get("LOG", "MAX_SIZE"), logCfg.get("LOG", "MAX_CNT") ))

	mc = MQConnect(cfg, section)
	print '222'
	mc.run()

	__LOG__.Trace("end main!")

if __name__ == "__main__":
	try:
		main()
	except:
		__LOG__.Exception("MQConnect main error")
