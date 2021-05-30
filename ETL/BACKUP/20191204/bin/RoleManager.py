#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import sys
import signal
import time
import datetime
import ConfigParser
import json
import logging
import Mobigen.Common.Log as Log; Log.Init()
from apscheduler.schedulers.background import BackgroundScheduler
from RestAPI import RestAPI

import API.M6 as M6
SHUTDOWN = False
ENM_WORK_ROLE = 'Amos_Administrator'
roleManager = None

def handler(signum, frame):
	global SHUTDOWN
	SHUTDOWN = True
	roleManager.shutdown()
	__LOG__.Trace('signal: process shutdown')

def getContents(filePath):
	if not os.path.exists(filePath) : return '{}'
	f = open(filePath, 'r')
	contents = f.read()
	f.close()
	return str(contents).encode('utf-8')


# SIGTERM
signal.signal(signal.SIGTERM, handler)
# SIGINT
signal.signal(signal.SIGINT, handler)
# SIGHUP
signal.signal(signal.SIGHUP, handler)
# SIGPIPE
signal.signal(signal.SIGPIPE, handler)

class RoleManager:
	def __init__(self, cfg, section):
		h = logging.StreamHandler()
		h.setFormatter(logging.Formatter('%(levelname)s:%(name)s:%(message)s'))
		log = logging.getLogger('apscheduler.executors.default')
		log.setLevel(logging.INFO)
		log.addHandler(h)

		self.section	= section
		self.cfg 		= cfg
		self.conn 		= None
		self.cursor 	= None
		self.restAPI	= RestAPI()
		self.hostList   = self.cfg.get('ENM_SERVER_MAPPING', 'ENM_API_SERVER_LIST').split(',') 

		self.initRepo()

		self.scheduler = BackgroundScheduler()
		#self.scheduler.add_job(self.initCommandRepo, 'cron', minute='*/{}'.format(INIT_INTERVAL), second='0', id='RoleManager')
		self.scheduler.add_job(self.checkJob, 'cron', minute='*', second='0', id='RoleManager')
		self.scheduler.add_job(self.checkRoleStatus, 'cron', minute='*/5', second='0', id='RoleCheckManager')
		#self.scheduler.add_job(self.checkRoleStatus, 'cron', minute='*', second='0', id='RoleCheckManager')
		self.scheduler.start()
		__LOG__.Trace('start!')

	def initRepo(self):
		repoStr = getContents(os.path.join(self.cfg.get(self.section, 'DUMP_PATH'), 'schedule_dump.json'))
		self.jobRepo = json.loads(repoStr)

	def stdOut(self, msg):
		sys.stdout.write(msg+'\n')
		sys.stdout.flush()
		# print(msg, file=sys.stdout)
		__LOG__.Trace('OUT: %s' % msg)
		
	def stdErr(self, msg):
		sys.stderr.write('%s\n' % (msg))
		sys.stderr.flush()
		__LOG__.Trace('ERR: %s' % msg)

	def shutdown(self):
		try :
			df = open(os.path.join(self.cfg.get(self.section, 'DUMP_PATH'), 'schedule_dump.json'), 'w')
			df.write(json.dumps(self.jobRepo, encoding='utf-8'))
			df.close()
			if self.scheduler :
				self.scheduler.shutdown()
				__LOG__.Trace('scheduler shutdown')
			else :
				__LOG__.Trace('scheduler is None')
		except :
			__LOG__.Exception()

	def disConnect(self,conn,cursor):
		if cursor != None:
			try : cursor.close()
			except : pass
		if conn != None :	
			try : conn.close()
			except : pass

	def initConnect(self):
		self.conn = M6.Connection(self.IRIS, self.IRIS_ID, self.IRIS_PASS, Database='tacs')
		__LOG__.Trace('IRIS Connect!')
		try :
			self.cursor = self.conn.cursor()
			self.cursor.SetFieldSep('|^|')
			self.cursor.SetRecordSep('|^-^|')
		except :
			__LOG__.Exception()
		finally :
			self.conn.commit()

	def run(self, section):
		__LOG__.Trace('RoleManager start!!')
		#self.initConnect()
		while not SHUTDOWN:
			try : 
				strIn = sys.stdin.readline()
				strLine = strIn.strip()
				if strLine == '' : 
					self.stdErr(strIn)
				else :
					if os.path.exists(strLine) : 
						jsonStr 	= getContents(strLine)
				#jsonStr 	= getContents('/home/tacs/DATA/WORKINFO/RAN_EMS/O190429000001_192.168.100.55.json')
						jsonObj		= json.loads(jsonStr)
						__LOG__.Trace(jsonObj)
						repoKey = jsonObj['workId']
						self.jobRepo[repoKey] = jsonObj

			except :
				__LOG__.Exception()

			finally :
				self.stdErr(strIn)
				

		#self.disConnect(self.conn, self.cursor)


	def checkJob(self):
		nDate 	= datetime.datetime.now()
		nStr 	= nDate.strftime('%Y%m%d%H%M00')
		gabStr  = (nDate - datetime.timedelta(minutes=1)).strftime('%Y%m%d%H%M00')
		__LOG__.Trace('nStr : %s' % nStr)

		for key in self.jobRepo.keys() :
			oneJob 		= self.jobRepo[key]
			staDate 	= oneJob['workStaDate']
			endDate 	= oneJob['workEndDate']
			__LOG__.Trace('%s : %s ~ %s, ENABLED:%s' % (oneJob['workId'], staDate, endDate, 'ENABLED' in oneJob))
			
			if 'ENABLED' not in oneJob and (staDate <= nStr and gabStr <= staDate) : self.addRole(oneJob)
			elif 'ENABLED' in oneJob and endDate <= nStr : self.delRole(oneJob)
			else :
				if nStr < staDate or nStr < endDate : __LOG__.Trace('keep : %s' % oneJob['workId'])
				else :
					del self.jobRepo[oneJob['workId']]
					__LOG__.Trace('delete: %s' % oneJob)

	def addRole(self, jsonObj):
		enmApiServer = self.cfg.get('ENM_SERVER_MAPPING', jsonObj['emsIp'])
		__LOG__.Trace('addRole : %s, %s, %s' % (enmApiServer, jsonObj['workId'], jsonObj['oprrId']))
		self.restAPI.changeUserRole(jsonObj['emsIp'],'ADD',jsonObj['oprrId'])
		self.jobRepo[jsonObj['workId']]['ENABLED'] = True
		
	def delRole(self, jsonObj):
		enmApiServer = self.cfg.get('ENM_SERVER_MAPPING', jsonObj['emsIp'])
		__LOG__.Trace('delRole : %s, %s, %s' % (enmApiServer, jsonObj['workId'], jsonObj['oprrId']))
		self.restAPI.changeUserRole(jsonObj['emsIp'],'REMOVE',jsonObj['oprrId'])
		del self.jobRepo[jsonObj['workId']]

	def checkRoleStatus(self) :
		nDate   	= datetime.datetime.now()
		yymmdd 		= nDate.strftime('%Y%m%d')
		hhmm 		= nDate.strftime('%H%M')
		evntDate 	= nDate.strftime('%Y%m%d%H%M%S')
		for oneHost in self.hostList :
			result = self.checkAllUserRole(oneHost)
			if len(result) > 0 :
				f = open('/home/tacs/DATA2/AUDIT_LOG/AUDIT_17/%s_%s_%s.audit' % (yymmdd, hhmm, oneHost), 'a')
				for oneInfo in result :
					oneInfo['evntDate'] = evntDate
					f.write('%s\n' % JSON.dumps(oneInfo, encoding='utf-8'))
				f.close()

	def getAllUser(self, host) :
		uri = '/oss/idm/usermanagement/users'
		result = '[]'
		try :
			code, result = self.restAPI.execute(host, 'GET', uri)
		except :
			__LOG__.Exception()

		userList = json.loads(result)

		return userList
		

	def getUserRole(self, host, userId) :
		if userId is None or userId == '' : return None
		uri = '/oss/idm/usermanagement/users/%s/privileges' % userId
		code, result = self.restAPI.execute(host, 'GET', uri)
		userInfo = json.loads(result)
		
		return userInfo

	def checkAllUserRole(self, host) :
		userRoleList = []
		userList = self.getAllUser(host)
		for oneUser in userList :
			if oneUser['username'] == 'SKT_TACS' : continue
			userRoleInfo = self.getUserRole(host, oneUser['username'])
			for oneRole in userRoleInfo :
				if ENM_WORK_ROLE == oneRole['role'] :
					__LOG__.Trace('Host: %s ,User : %s, Role : %s' % (host, oneUser['username'], oneRole['role']))
					nDate   = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
					userRoleList.append({'workUserId':oneUser['username'], 'beforePriv':oneRole['role'], 'checkDate':nDate})

			time.sleep(1)

		currentOprrIdList = []
		for key in self.jobRepo.keys() :
			oneJob      = self.jobRepo[key]
			if 'ENABLED' in oneJob :
				oprrIdList = oneJob['oprrId'].split(';')
				oprrIdList = [x for x in oprrIdList if x]
				currentOprrIdList = currentOprrIdList + oprrIdList
		
		result = []
		for oneUserInfo in userRoleList :
			if not oneUserInfo['workUserId'] in currentOprrIdList :
				result.append(oneUserInfo)		
				
		return result
			
def main():
	global roleManager
	module = os.path.basename(sys.argv[0])

	section = sys.argv[1] # ROLE_MANAGER 
	cfgfile = sys.argv[2] # /home/tacs/TACS-EF/ETL/conf/RoleManager.conf

	cfg = ConfigParser.ConfigParser()
	cfg.read(cfgfile)

	logPath = cfg.get("GENERAL", "LOG_PATH")
	logFile = os.path.join(logPath, "%s_%s.log" % (module, section))

	logCfgPath = cfg.get("GENERAL", "LOG_CONF")

	logCfg = ConfigParser.ConfigParser()
	logCfg.read(logCfgPath)

	Log.Init(Log.CRotatingLog(logFile, logCfg.get("LOG", "MAX_SIZE"), logCfg.get("LOG", "MAX_CNT") ))

	roleManager = RoleManager(cfg, section)
	roleManager.run(section)

	__LOG__.Trace('end main!')


if __name__ == '__main__':
	try:
		main()
	except:
		__LOG__.Exception('main error')
