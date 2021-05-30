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
import uuid
import shutil
import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

from apscheduler.schedulers.blocking import BlockingScheduler

import SftpClient as SFTPClient
import Mobigen.Common.Log as Log; Log.Init()
import subprocess

class CollectApi :
	def __init__(self, cfg) :
		self.cfg 			= cfg
		self.WORKINFO_REPO 	= {}

		self.migrationSucWorkCnt = 0

		self._initConfig()

	def _initConfig(self) :
		self.systemName 		= self.cfg.get('MODULE_CONF', 'TACS_SYSTEM_NAME')
		self.workInfoBaseDir    = self.cfg.get('MODULE_CONF', 'TACS_WORKINFO_RAW')

		self.auditLogTempDir	= self.cfg.get('MODULE_CONF', 'TACS_AUDITLOG_TEMP')
		self.auditLogBaseDir	= self.cfg.get('MODULE_CONF', 'TACS_AUDITLOG_PATH')
		self.receivedWorkCode	= self.cfg.get('MODULE_CONF', 'RECEIVED_WORK_CODE')

		self.tangoWmWorkInfoUrl = self.cfg.get('MODULE_CONF', 'TANGO_WM_WORKINFO_URL')
		self.tangoWmEqpInfoUrl	= self.cfg.get('MODULE_CONF', 'TANGO_WM_EQPINFO_URL')
		self.xAuthToken			= self.cfg.get('MODULE_CONF', 'TANGO_WM_X_AUTH_TOKEN')
		self.headers 			= {'x-auth-token' : self.xAuthToken, 'Content-Type' : 'application/json; charset=utf-8'}
		self.timeout			= int(self.cfg.get('MODULE_CONF', 'TANGO_REQUEST_TIMEOUT'))

		self.host               = self.cfg.get('MODULE_CONF', 'TANGO_WM_SFTP_HOST')
		self.port               = int(self.cfg.get('MODULE_CONF', 'TANGO_WM_SFTP_PORT'))
		self.user               = self.cfg.get('MODULE_CONF', 'TANGO_WM_SFTP_USER')
		self.passwd             = self.cfg.get('MODULE_CONF', 'TANGO_WM_SFTP_PASSWD')

#		self.stdoutSleepTime	= int(self.cfg.get('MODULE_CONF', 'STDOUT_SLEEP_TIME'))

		self.errFilePath		= self.cfg.get('MODULE_CONF', 'ERROR_FILE_PATH')

	def _stdout(self, msg) :
		sys.stdout.write('stdout' + msg + '\n')
		sys.stdout.flush()
		__LOG__.Trace('stdout: %s' % msg)

	def writeErrFile(self) :
		self._mkdirs(self.errFilePath)

		fileName		= '{}_{}'.format(self.searchStartDate, self.searchEndDate)
		fullFilePath	= os.path.join(self.errFilePath, fileName)

		if not os.path.exists(fullFilePath) :
			__LOG__.Trace( 'Tango-WM Request Fail File Create : {}'.format(fullFilePath ) )
			with open(fullFilePath, 'w') as dateFile :
				dateFile.write('')

	def lookupWorkInfo(self, fromDate = None, toDate = None, migration = False) :
		self.migration	= migration
		self.searchStartDate = fromDate
		self.searchEndDate	= toDate

		rawDict	= None

		if not migration :
			searchEndDateObj  	= datetime.now()
			searchStartDateObj	= searchEndDateObj - timedelta(minutes=1)

 			self.searchStartDate  	= searchStartDateObj.strftime('%Y%m%d%H%M')
 			self.searchEndDate 		= searchEndDateObj.strftime('%Y%m%d%H%M')
	
		__LOG__.Trace('lookup workInfo from({}) ~ to({})'.format(self.searchStartDate, self.searchEndDate))

 		url = self.tangoWmWorkInfoUrl.format(self.systemName, self.searchStartDate, self.searchEndDate)
 		__LOG__.Trace('request workInfo url: {}'.format(url))

		#try :
		rawDict = self._requestGet(url)
		#except :
			#raise Exception

		return self._loadWorkInfo(rawDict)

	def lookupEqpInfo(self, workIdList) :
		if not workIdList :
			__LOG__.Trace('workIdList is empty')
		else :
			logDictList = list()
			yyyyMMdd	= None
			eventDate	= None
			rawDict 	= None

			for oneWorkId in workIdList :
				url = self.tangoWmEqpInfoUrl.format(self.systemName, oneWorkId)
				__LOG__.Trace('request eqpInfo url: {}'.format(url))

				#try :
				rawDict = self._requestGet(url, True)
				#except :
					#raise Exception

				logDict, yyyyMMdd, eventDate = self._loadEqpInfo(oneWorkId, rawDict, logDictList)
				logDictList.append(logDict)

			self._writeTacsHistoryFile(yyyyMMdd, eventDate, logDictList)

	def _requestGet(self, url, collectEqp = False, verify = False) :
		rawDict 	= None
		response	= None

		try :
			response 	= requests.get(url = url, headers = self.headers, verify = verify, timeout = self.timeout)

			if response != None and response.status_code == 200 :
				rawDict = response.json()
				if collectEqp and self.migration :
					self.migrationSucWorkCnt += 1
			else :
				__LOG__.Trace('!!! Exception !!! requestGet failed. statusCode: {}'.format(response.status_code))
#				if not self.migration :
#					self.writeErrFile()
				raise

		except :
			if not self.migration :
				self.writeErrFile()
			raise

		return rawDict

	def _loadWorkInfo(self, rawDict) :
		if rawDict :
			__LOG__.Trace('workInfo rawData: {}'.format(rawDict))
			workIdList = []

			if type(rawDict['workInfo']) is list :
				for oneWorkInfo in rawDict['workInfo'] :
					workId = oneWorkInfo['workId']
					__LOG__.Trace('workId: {}'.format(workId))
					if workId is None or not workId :
						__LOG__.Trace('invalid workId({})'.format(workId))
						continue

					workIdList.append(workId)
															
					wrapper 			= {}
					wrapper['workInfo'] = oneWorkInfo
					
					workEvntDate = datetime.now().strftime('%Y%m%d%H%M%S')
					wrapper['workInfo']['workEvntDate'] = workEvntDate					

					self.WORKINFO_REPO[workId] = wrapper
				__LOG__.Trace('WORKINFO_REPO: {}'.format(self.WORKINFO_REPO))
			else :
				__LOG__.Trace('Unsupported type: {}'.format(type(rawDict['workInfo'])))
				pass

			return workIdList
		else :
			__LOG__.Trace('workInfo rawData is None')
			return None

	def _loadEqpInfo(self, oneWorkId, rawDict, logDictList) :
		logDict 	= dict()
		yyyyMMdd	= None
		eventDate	= None

		if rawDict :
			__LOG__.Trace('eqpInfo rawData: {}'.format(rawDict))
			if 'eqpInfo' in rawDict and type(rawDict['eqpInfo']) is list :
				scriptFileList = []
				wrapper = self.WORKINFO_REPO[oneWorkId]
				if wrapper : 
					wrapper['eqpInfo'] = rawDict['eqpInfo']
					for oneEqpInfoDict in rawDict['eqpInfo'] :
						if 'scriptInfo' in oneEqpInfoDict :
							scriptInfoList = oneEqpInfoDict['scriptInfo']
								
							if scriptInfoList :
								for oneScriptInfoDict in scriptInfoList :
									filePathname = oneScriptInfoDict['atchdPathFileNm']
									if filePathname :
										remoteFilepath, remoteFilename = os.path.split(filePathname)
										__LOG__.Trace('remoteFilepath({}), remoteFilename({})'.format(remoteFilepath, remoteFilename))
										scriptFileDict = {}
										scriptFileDict['remoteFilepath'] = remoteFilepath
										scriptFileDict['remoteFilename'] = remoteFilename

										scriptFileList.append(scriptFileDict)
									else :
										__LOG__.Trace('workId({})/eqpNm({}) atchdPathFileNm({}) is invalid'.format(oneWorkId, oneEqpInfoDict['eqpNm'], filePathname))
										pass
							else :
								__LOG__.Trace('workId({})/eqpNm({}) scriptInfoList({}) is invalid'.format(oneWorkId, oneEqpInfoDict['eqpNm'], scriptInfoList))
						else :
							__LOG__.Trace('workId({})/eqpNm({}) scriptInfo does not exist in eqpInfo'.format(oneWorkId, oneEqpInfoDict['eqpNm']))
							pass
				else :
					__LOG__.Trace('no registered workId({}) in WORKINFO_REPO'.format(oneWorkId))
					return
	
				__LOG__.Trace('scriptFileList: {}'.format(scriptFileList))
				eventDate 	= wrapper['workInfo']['workEvntDate']
				yyyyMMdd 	= datetime.strptime(eventDate, '%Y%m%d%H%M%S').strftime('%Y%m%d')
				__LOG__.Trace('eventDate({}), yyyyMMdd({})'.format(eventDate, yyyyMMdd))
				self._getScriptFiles(yyyyMMdd, oneWorkId, scriptFileList)

				logDict = self._writeTangoWorkFile(yyyyMMdd, eventDate, oneWorkId, wrapper)

				self._removeCompleteWorkInfo(oneWorkId)
			else :
				__LOG__.Trace('Unsupported type: {}'.format('eqpInfo' in rawDict if type(rawDict['eqpInfo']) else None ))
				pass
		else :
			__LOG__.Trace('workId({}), eqpInfo rawData is None'.format(oneWorkId))
			pass

		return logDict, yyyyMMdd, eventDate

	def _getScriptFiles(self, yyyyMMdd, workId, scriptFileList) :
		if not scriptFileList :
			__LOG__.Trace('scriptFileList({}) is empty'.format(scriptFileList))
			return

		try :
			tacsWorkInfoPath = os.path.join(self.workInfoBaseDir, yyyyMMdd, workId)
			self._mkdirs(tacsWorkInfoPath)

			sftpClient = SFTPClient.SftpClient(self.host, self.port, self.user, self.passwd)
			for oneScriptFileDict in scriptFileList :
				remoteFilepath 	= oneScriptFileDict['remoteFilepath']
				remoteFilename 	= oneScriptFileDict['remoteFilename'] 

				sftpClient.download(remoteFilepath, remoteFilename, tacsWorkInfoPath)
				__LOG__.Trace('scriptFile from({}) -> to({}) download succeed'.format(os.path.join(remoteFilepath, remoteFilename), os.path.join(tacsWorkInfoPath, remoteFilename)))

			sftpClient.close()
		except Exception as ex :
			__LOG__.Trace('scriptFile download proccess failed {}'.format(ex))
			if not self.migration :
				self.writeErrFile()
			else :
				self.migrationSucWorkCnt -= 1

			self._removeCompleteWorkInfo(workId)
			raise ex
	
	def _writeTangoWorkFile(self, yyyyMMdd, eventDate, workId, wrapper) :
		logDict = {}
		try :
			tacsWorkInfoPath = os.path.join(self.workInfoBaseDir, yyyyMMdd, workId)
			self._mkdirs(tacsWorkInfoPath)

			contents = json.dumps(wrapper, ensure_ascii=False)
			__LOG__.Trace('contents: {}'.format(contents))
			createFilePath = os.path.join(tacsWorkInfoPath, '{}_{}_META.json'.format(eventDate, workId))
			self._createFile(createFilePath, contents)
			logDict['tacsLnkgRst'] = 'OK'

			if self.migration :
				__LOG__.Trace( ['mf','30000', 'put', 'dbl', 'stdoutfile://{}'.format(createFilePath)] )	
				subprocess.call(['mf', '30000', 'put,dbl,stdoutfile://{}'.format(createFilePath)])
			else :
#				time.sleep(self.stdoutSleepTime)
				self._stdout('file://{}'.format(createFilePath))
		except Exception as ex :
			__LOG__.Trace('workFile write process failed {}'.format(ex))
			logDict['tacsLnkgRst'] = 'FAIL'
			logDict['tacsLnkgRsn'] = ex.args
			self._removeCompleteWorkInfo(workId)
			raise ex
		finally :
			logDict['evntTypCd'] 	= self.receivedWorkCode
			logDict['evntDate'] 	= eventDate
			logDict['workId']		= workId
			logDict['lnkgEqpIp']	= ''

		return logDict

	def _writeTacsHistoryFile(self, yyyyMMdd, eventDate, logDictList) :
		if logDictList :
			__LOG__.Trace('received workInfo history: {}'.format(logDictList))
			try :
				tacsHistoryTempPath = os.path.join(self.auditLogTempDir, 'AUDIT_{}'.format(self.receivedWorkCode))
				self._mkdirs(tacsHistoryTempPath)
				contentList	= list()

				for oneLogDict in logDictList :
					content  = json.dumps(oneLogDict, ensure_ascii=False)
					contentList.append(content)		

				contents = '\n'.join(contentList)
			
				__LOG__.Trace('contents: {}'.format(contents))

				tacsHistoryFilename = self._getTacsHistoryFilename(yyyyMMdd, eventDate)
				__LOG__.Trace('tacsHistoryFilename: {}'.format(tacsHistoryFilename))
				self._createFile(os.path.join(tacsHistoryTempPath, tacsHistoryFilename), contents) 
				
				tacsHistoryPath = os.path.join(self.auditLogBaseDir, 'AUDIT_{}'.format(self.receivedWorkCode))
				self._mkdirs(tacsHistoryPath)

				shutil.move(os.path.join(tacsHistoryTempPath, tacsHistoryFilename), os.path.join(tacsHistoryPath, tacsHistoryFilename))
				__LOG__.Trace('tacsHistory file move from {} -> to {} succeed'.format(os.path.join(tacsHistoryTempPath, tacsHistoryFilename), os.path.join(tacsHistoryPath, tacsHistoryFilename)))
			except Exception as ex :
				__LOG__.Trace('tacsHistory {} load process failed {}'.format(logDict, ex))
		else :
			__LOG__.Trace('received workInfo history({}) is invalid'.format(logDict))

	def _mkdirs(self, directory) :
		#__LOG__.Trace('{} isExists: {}'.format(directory, os.path.exists(directory)))
		if not os.path.exists(directory) :
			__LOG__.Trace('create directories {}'.format(directory))
			os.makedirs(directory)

	def _createFile(self, filePath, contents) :
		f = None
		try :
			f = open(filePath, 'w')
			f.write(contents)
			__LOG__.Trace('{} file is created'.format(filePath))
		except Exception as ex :
			__LOG__.Trace('{} to file process failed {}'.format(contents, ex))
			raise ex
		finally :
			if f :
				f.close()

	def _getTacsHistoryFilename(self, yyyyMMdd, eventDate) :
		HHmm 				= datetime.strptime(eventDate, '%Y%m%d%H%M%S').strftime('%H%M')
		tacsHistoryFilename = '{}_{}_{}.audit'.format(yyyyMMdd, HHmm, uuid.uuid4())
		return tacsHistoryFilename

	def _removeCompleteWorkInfo(self, workId) :
		if workId in self.WORKINFO_REPO :
			del self.WORKINFO_REPO[workId]
			__LOG__.Trace('workId({}), WORKINFO_REPO: {}'.format(workId, self.WORKINFO_REPO))

