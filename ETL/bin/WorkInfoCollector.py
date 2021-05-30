#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys

reload(sys)
sys.setdefaultencoding('UTF-8')

import signal
import time
from datetime import datetime
from datetime import timedelta
import ConfigParser
import glob
import json
#import uuid
#import shutil
#import requests
#from requests.packages.urllib3.exceptions import InsecureRequestWarning
#requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

from apscheduler.schedulers.blocking import BlockingScheduler

#import SftpClient as SFTPClient
import Mobigen.Common.Log as Log; Log.Init()
#import subprocess
import CollectApi as CollectAPI

workInfoCollector = None

def handler(signum, frame):
	__LOG__.Trace('signal : process shutdown')
	try :
		if workInfoCollector :
			workInfoCollector.shutdown()
	except : 
		__LOG__.Exception()

# SIGTERM
signal.signal(signal.SIGTERM, handler)
# SIGINT
signal.signal(signal.SIGINT, handler)
# SIGHUP
signal.signal(signal.SIGHUP, handler)
# SIGPIPE
signal.signal(signal.SIGPIPE, handler)


class WorkInfoCollector :
	def __init__(self, cfg) :
		self.cfg 			= cfg
		self.WORKINFO_REPO 	= {}

		self._initConfig()

		collectCfg = ConfigParser.ConfigParser()
		collectCfg.read(self.CollectApiCfgPath)

		self.collectApi		= CollectAPI.CollectApi(collectCfg)
		self.failOverFlag	= True

	def _initConfig(self) :
		self.scheduleInterval   = self.cfg.get('MODULE_CONF', 'SCHEDULE_INTERVAL_MIN')
		self.CollectApiCfgPath	= self.cfg.get('MODULE_CONF', 'COLLECT_API_CFG_PATH')
		self.migration			= False

	def _stdout(self, msg) :
		sys.stdout.write(msg+'\n')
		sys.stdout.flush()
		__LOG__.Trace('stdout: %s' % msg)


	def _executeMigration(self, searchStartDate, searchEndDate) :
		__LOG__.Trace('migration process start. searchStartDate({}), searchEndDate({})'.format(searchStartDate, searchEndDate))
		try :
			searchStartDateObj 	= datetime.strptime(searchStartDate, '%Y%m%d%H%M%S')
			searchEndDateObj	= datetime.strptime(searchEndDate, '%Y%m%d%H%M%S')
	
			if searchStartDateObj > searchEndDateObj :
				__LOG__.Trace('searchStartDate({}) bigger than searchEndDate({})'.format(searchStartDate, searchEndDate))
				print '[ERROR] searchStartDate({}) bigger than searchEndDate({})'.format(searchStartDate, searchEndDate)
			else :
				# request workInfo
				workIdList = self.collectApi.lookupWorkInfo(searchStartDate, searchEndDate, True)
				# request eqpInfo by workId
				self.collectApi.lookupEqpInfo(workIdList)
		except Exception as ex :
			__LOG__.Trace('workInfo migration failed. {}'.format(ex))

	def _executeScheduler(self) :
		try :
			self.failOverFlag = True
			__LOG__.Trace('scheduler process start')
			# request workInfo
			workIdList = self.collectApi.lookupWorkInfo()
			# request eqpInfo by workId
			self.collectApi.lookupEqpInfo(workIdList)
		except :
			self.failOverFlag = False
			__LOG__.Exception()
		finally :
			if self.failOverFlag :
				self._stdout('Fail_Over')
			

	def shutdown(self) :
		try :
			if self.scheduler :
				#self.scheduler.remove_job('workInfo_scheduler')
				self.scheduler.shutdown()
				__LOG__.Trace('schduler is terminated')
			else :
				_LOG__.Trace('scheduler is None')
		except Exception as ex :
			__LOG__.Trace('shutdown failed {}'.format(ex))

	def run(self, searchStartDate = None, searchEndDate = None, migration = False) :
		self.migration = migration

		if not migration :
			self.scheduler = BlockingScheduler()
			self.scheduler.add_job(self._executeScheduler, 'cron', minute='*/{}'.format(self.scheduleInterval), second='2', id='workInfo_scheduler')
			self.scheduler.start()

		else :
			self._executeMigration(searchStartDate, searchEndDate)
			__LOG__.Trace('migration proccess done')
	
def main() :
	argvLength = len(sys.argv)
	if argvLength < 3 :
		print '''
[ERROR] WorkInfoCollector argv required at least 3
++ Usage 
++++ scheduler : module section cfgfile
++++ migration : module section cfgfile searchStartDate(yyyyMMddHHmm) searchEndDate(yyyyMMddHHmm)
'''
		return

	module 	= os.path.basename(sys.argv[0])
	section = sys.argv[1]
	cfgfile = sys.argv[2]
	
	searchStartDate = None
	searchEndDate	= None	
	migration 		= False

	if argvLength == 5 :
		migration 		= True
		searchStartDate = sys.argv[3]
		searchEndDate	= sys.argv[4]

	cfg = ConfigParser.ConfigParser()
	cfg.read(cfgfile)

	logPath = cfg.get("GENERAL", "LOG_PATH")
	logFile = os.path.join(logPath, "%s_%s.log" % (module, section))

	logCfgPath = cfg.get("GENERAL", "LOG_CONF")

	logCfg = ConfigParser.ConfigParser()
	logCfg.read(logCfgPath)

	Log.Init(Log.CRotatingLog(logFile, logCfg.get("LOG", "MAX_SIZE"), logCfg.get("LOG", "MAX_CNT") ))

	global workInfoCollector

	workInfoCollector = WorkInfoCollector(cfg)
	workInfoCollector.run(searchStartDate, searchEndDate, migration)

	__LOG__.Trace('main is terminated')

if __name__ == '__main__' :
	try :
		main()
	except :
		__LOG__.Exception()
