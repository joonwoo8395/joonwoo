#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys

reload(sys)
sys.setdefaultencoding('UTF-8')

import signal
import time
import datetime
import ConfigParser
import glob
import json
#import uuid
#import shutil
#import requests
#from requests.packages.urllib3.exceptions import InsecureRequestWarning
#requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

#import SftpClient as SFTPClient
import Mobigen.Common.Log as Log; Log.Init()
#import subprocess
import CollectApi as CollectAPI
SHUTDOWN = False

def handler(signum, frame):
	__LOG__.Trace('signal : process shutdown')

# SIGTERM
signal.signal(signal.SIGTERM, handler)
# SIGINT
signal.signal(signal.SIGINT, handler)
# SIGHUP
signal.signal(signal.SIGHUP, handler)
# SIGPIPE
signal.signal(signal.SIGPIPE, handler)

class WorkInfoFailOver :
	def __init__(self, cfg) :
		self.WORKINFO_REPO 	= {}

		self.errFilePath		= cfg.get('FAIL_OVER', 'ERROR_FILE_PATH')
		self.sliceTime			= cfg.getint('FAIL_OVER', 'SLICE_TIME')
		collectApiCfgPath		= cfg.get('MODULE_CONF', 'COLLECT_API_CFG_PATH')
		
		collectCfg	= ConfigParser.ConfigParser()
		collectCfg.read(collectApiCfgPath)

		self.collectApi		= CollectAPI.CollectApi(collectCfg)

	## stdOut
	def stdOut(self, msg) :
		sys.stdout.write(msg+'\n')
		sys.stdout.flush()
		__LOG__.Trace('STD OUT : %s' % msg)

	def stdErr(self, msg) :
		sys.stderr.write(msg+'\n')
		sys.stderr.flush()
		#__LOG__.Trace('STD ERR : %s' % msg)

	# staDate, endDate 를 구하는 함수 (60분 이상일경우 시작시간 ~ 시작시간 + 60분)
	# @param
	# @fileList = [yyyymmddHHMM_yyyymmddHHMM, ....]
	def dateCompare(self, fileList) :
		lastDate	= None
		firstDate	= None

		lastDateStr 	= None
		firstDateStr	= None

		for oneFile in fileList :
			staDate		= None
			endDate		= None

			if not '_' in oneFile :
				continue

			staDate, endDate = oneFile.split('_')			

			try :
				staDateObj = datetime.datetime.strptime(staDate, '%Y%m%d%H%M')
				endDateObj = datetime.datetime.strptime(endDate, '%Y%m%d%H%M')

				if firstDate is None or staDateObj < firstDate :
					firstDate = staDateObj

				if lastDate is None or endDateObj > lastDate :
					lastDate = endDateObj

				if lastDate - firstDate >= datetime.timedelta(minutes = self.sliceTime) :
#					return firstDate, firstDate + datetime.timedelta(minutes = self.sliceTime)
					lastDate = firstDate + datetime.timedelta(minutes = self.sliceTime)
					break
			except :
				continue

		if firstDate and lastDate :
			firstDateStr 	= firstDate.strftime('%Y%m%d%H%M')
			lastDateStr		= lastDate.strftime('%Y%m%d%H%M')

			if firstDate >= lastDate :
				__LOG__.Trace( 'firstDate : {} , lastDate : {} invalid Date'.format(firstDateStr, lastDateStr) )
				return None, None

		return firstDateStr, lastDateStr

	# 파일 형식 체크
	# @param
	# @fileName : yyyymmddHHMM_yyyymmddHHMM
	def fileValidCheck(self, fileName) :
		validCheck		= False
		checkFileList 	= list()
		staDate			= None
		endDate			= None

		if '_' in fileName :
			staDate, endDate = fileName.split('_')

			try :
				datetime.datetime.strptime(staDate, '%Y%m%d%H%M')
				datetime.datetime.strptime(endDate, '%Y%m%d%H%M')
				validCheck = True
			except :
				__LOG__.Trace('%s file invalid' % fileName)

		return validCheck

#	def thresholdCheck(self, firstDate, lastDate) :	
#		firstDateTime 	= datetime.datetime.strptime(firstDate, '%Y%m%d%H%M')
#		lastDateTime	= datetime.datetime.strptime(lastDate, '%Y%m%d%H%M')
#		loopCnt			= 1
#		# Tango API 실행 시간간격을 분단위로 변환
#		checkMin	= int((lastDateTime - firstDateTime).total_seconds() / datetime.timedelta(minutes = 1).total_seconds())

#		loopCnt = int(checkMin/self.sliceTime) + 1

#		if loopCnt > 1 :
#			__LOG__.Trace('%s > %s (%s, %s) %s 반복 수행' %( checkMin, self.sliceTime, firstDate,firstDate, loopCnt ) )
#		return loopCnt

	# Tango-WM Collect
	# @param
	# @firstDate : staTime (yyyymmddHHMM)
	# @lastDate  : endTime (yyyymmddHHMM)
	def infoCollect(self, firstDateStr, lastDateStr) :

		# 김준우 07-27 추가
		migrationWorkCnt 	= 0
		migrationSucWorkCnt = 0
		try :
			__LOG__.Trace('workInfoCollect [%s~%s]' %(firstDateStr, lastDateStr) )
			workIdList =  self.collectApi.lookupWorkInfo(firstDateStr, lastDateStr, True)
			if workIdList != None :
				migrationWorkCnt = len(workIdList)

			self.collectApi.lookupEqpInfo(workIdList)

			migrationSucWorkCnt = self.collectApi.migrationSucWorkCnt

			if migrationWorkCnt != 0 :
				__LOG__.Trace('!!! Exception !!! 누락 작업수집 : {}/{} '.format(migrationSucWorkCnt, migrationWorkCnt))

#			firstTempDate	= firstDate
#			lastTempDate	= lastDate

#			for loop in range(loopCnt) :
#				if loop + 1 == loopCnt :
#				__LOG__.Trace('workInfoCollect [%s~%s]' %(firstDate, lastDate) )
#				workIdList =  self.collectApi.lookupWorkInfo(firstDate, lastDate, True)
#				self.collectApi.lookupEqpInfo(workIdList)
#				else :
#					__LOG__.Trace('workInfoCollect [%s~%s]' %(firstTempDate, lastTempDate)  )
#					lastTempDate = str(datetime.datetime.strptime(firstTempDate, '%Y%m%d%H%M') + datetime.timedelta(minute = self.sliceTime))
#					workIdList =  self.collectApi.lookupWorkInfo(firstTempDate, lastTempDate, True)
#					self.collectApi.lookupEqpInfo(workIdList)
#					firstTempDate = str(datetime.datetime.strptime(lastTempDate, '%Y%m%d%H%M') + datetime.timedelta(minute = 1))

		except :
			if migrationWorkCnt != 0 :
				__LOG__.Trace('!!! Exception !!! 누락 작업수집 : {}/{} '.format(migrationSucWorkCnt, migrationWorkCnt))
			raise

	# Tango-WM Collect Execute File remove
	# @param
	# @fileList  : Execute File List [yyyymmddHHMM_yyyymmddHHMM, .....]
	# @firstDate : Execute File staDate (yyyymmddHHMM)
	# @lastDate  : Execute File endDate (yyyymmddHHMM)
	def fileRemove(self, fileList, firstDateStr, lastDateStr) :
		staDate = None
		endDate = None

		for oneFile in fileList :
			if '_' in oneFile :
				staDate, endDate = oneFile.split('_')

			if staDate and endDate :
				if int(endDate) <= int(lastDateStr) and int(staDate) >= int(firstDateStr) :
					os.remove( os.path.join(self.errFilePath, oneFile) )
					__LOG__.Trace('File Remove : %s' % oneFile)
			else :
				__LOG__.Trace('invalid Remove {}'.format(oneFile))

	# 파일을 읽어서 처리하는 함수
	def fileRead(self) :
		if not os.path.exists(self.errFilePath) :
			__LOG__.Trace( '{} Path not exists'.format(self.errFilePath) )
			return

		fileList 		= os.listdir(self.errFilePath)

		lastDateStr		= None
		firstDateStr	= None
		checkFileList	= list()

		for oneFile in fileList	:
			if self.fileValidCheck(oneFile) :
				checkFileList.append(oneFile)
		
		if not checkFileList :
			__LOG__.Trace('File not Exists :  %s' % self.errFilePath )
			return

		checkFileList.sort()

		firstDateStr, lastDateStr = self.dateCompare(checkFileList)

#		loopCnt	= self.sliceTimeCheck(firstDate, lastDate)

#		self.infoCollect(firstDate, lastDate, loopCnt)
		if firstDateStr and lastDateStr :
			try :
				self.infoCollect(firstDateStr, lastDateStr)
				self.fileRemove(checkFileList, firstDateStr, lastDateStr)
			except :
				__LOG__.Exception()

	def run(self) :
		__LOG__.Trace('WorkInfo FailOver Start !!')

		while not SHUTDOWN :
			strIn	= ''
			strLine	= ''

			try :
				strIn	= sys.stdin.readline()
				strLine	= strIn.strip()

				if strLine != 'Fail_Over' :
					continue

				self.fileRead()

			except :
				__LOG__.Exception()
				continue

			finally :
				self.stdErr(strIn)


	
def main() :
	module 	= os.path.basename(sys.argv[0])
	cfgfile = sys.argv[1]
	
	cfg = ConfigParser.ConfigParser()
	cfg.read(cfgfile)

	logPath = cfg.get("GENERAL", "LOG_PATH")
	logFile = os.path.join(logPath, "%s.log" % module)

	logCfgPath = cfg.get("GENERAL", "LOG_CONF")

	logCfg = ConfigParser.ConfigParser()
	logCfg.read(logCfgPath)

	Log.Init(Log.CRotatingLog(logFile, logCfg.get("LOG", "MAX_SIZE"), logCfg.get("LOG", "MAX_CNT") ))

	workInfoFailOver = WorkInfoFailOver(cfg)
	workInfoFailOver.run()

	__LOG__.Trace('main is terminated')

if __name__ == '__main__' :
	try :
		main()
	except :
		__LOG__.Exception()
