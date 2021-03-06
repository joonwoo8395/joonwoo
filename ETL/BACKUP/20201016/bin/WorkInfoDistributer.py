#!/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import signal
import ConfigParser
import json
import shutil
import hashlib
import uuid
from datetime import datetime

import IRISSelect
import IRISSelectRange
import SftpClient as SFTPClient
import Mobigen.Common.Log as Log; Log.Init()
import AESDecode

SHUTDOWN = False

def handler(signum, frame):
	global SHUTDOWN
	SHUTDOWN = True
	__LOG__.Trace('signal : process shutdown')

# SIGTERM
signal.signal(signal.SIGTERM, handler)
# SIGINT
signal.signal(signal.SIGINT, handler)
# SIGHUP
signal.signal(signal.SIGHUP, handler)
# SIGPIPE
signal.signal(signal.SIGPIPE, handler)

class WorkInfoDistributer :
	def __init__(self, cfg) :
		self.cfg = cfg

		self._initConfig()

	def _initConfig(self) :
		irisCfgPath = self.cfg.get('GENERAL', 'IRIS_CONF')

		irisCfg = ConfigParser.ConfigParser()
		irisCfg.read(irisCfgPath)
		AES				= AESDecode.AESDecode()
		dbUrl			= irisCfg.get("IRISDB","IRIS")
		dbUser			= irisCfg.get("IRISDB","IRIS_ID")
		dbPasswd		= AES.decodeAES(irisCfg.get("IRISDB","IRIS_PASS"))
		self.irisObj	= IRISSelect.IRIS_SQL(dbUrl, dbUser, dbPasswd)

		self.rawWorkInfoBaseDir = self.cfg.get('MODULE_CONF', 'TACS_WORKINFO_RAW')
		self.emsWorkInfoBaseDir = self.cfg.get('MODULE_CONF', 'TACS_WORKINFO_EMS')
		self.enmWorkInfoBaseDir = self.cfg.get('MODULE_CONF', 'ENM_WORKINFO_BASE')

		self.port 	= int(self.cfg.get('MODULE_CONF', 'ENM_SFTP_PORT'))
		self.user 	= self.cfg.get('MODULE_CONF', 'ENM_SFTP_USER')
		self.passwd = self.cfg.get('MODULE_CONF', 'ENM_SFTP_PASSWD')

		self.auditLogTempDir 	= self.cfg.get('MODULE_CONF', 'TACS_AUDITLOG_TEMP')
		self.auditLogBaseDir 	= self.cfg.get('MODULE_CONF', 'TACS_AUDITLOG_PATH')
		self.exportWorkCode		= self.cfg.get('MODULE_CONF', 'EXPORT_WORK_CODE')

		self.roleFilePath		= self.cfg.get('MODULE_CONF', 'ROLE_FILE_PATH')

		self.JSON_POSTFIX 		= '_META.json'
		self.SITEFILE_POSTFIX 	= '.txt'
		self.SHA_POSTFIX 		= '.sha'

	def _stdOut(self, msg):
		sys.stdout.write(msg+'\n')
		sys.stdout.flush()
		__LOG__.Trace("STD OUT: %s" % msg)

	def _stderr(self, value) :
		sys.stderr.write('stderr: {}{}'.format(value, '\n'))
		sys.stderr.flush()

	def _makeWorkFiles(self, paramDict) :
		__LOG__.Trace('paramDict: {}'.format(paramDict))
		logDict = {}
		try :
			workId 		= paramDict['workId']
			workStaDate = paramDict['workStaDate']
			emsIp 		= paramDict['emsIp']

			workInfoDict, eqpInfoList = self._selectWorkInfo(workId, workStaDate, emsIp)
			
			metaDict 	= {}
			eventDate	= workInfoDict['workEvntDate']
			
			del workInfoDict['workEvntDate']

			metaDict['workInfo'] 	= workInfoDict
			metaDict['eqpInfo']		= []

			enbIpList				= []			
			scriptInfoList 			= []

			for oneEqpInfo in eqpInfoList :
				if 'svrIp' in oneEqpInfo.keys() :
#				if oneEqpInfo['svrIp'] :
					if not (oneEqpInfo['svrIp'] in enbIpList) :
						enbIpList.append(oneEqpInfo['svrIp'])

				if 'scriptInfo' in oneEqpInfo.keys() :
#				if oneEqpInfo['scriptInfo'] :
					scriptInfoList.extend(oneEqpInfo['scriptInfo'])				

				oprrIds = oneEqpInfo['oprrId'].split(';')
				__LOG__.Trace('oprrIds: {}'.format(oprrIds))
				if '' in oprrIds :
					oprrIds.remove('')

				idx = 0
				for oprrId in oprrIds :
					eqpInfoByOprrId = {}
					if not oprrId :
						continue
					
					if len(oprrIds) > 1 :
						idx += 1
						eqpInfoByOprrId['unqIdntNo'] = '{}-{}'.format(oneEqpInfo['unqIdntNo'], idx)
					else :
						eqpInfoByOprrId['unqIdntNo'] = oneEqpInfo['unqIdntNo']

					eqpInfoByOprrId['cmdWorkTypCd'] = oneEqpInfo['cmdWorkTypCd']
					eqpInfoByOprrId['tangoEqpId'] = oneEqpInfo['tangoEqpId']
					eqpInfoByOprrId['enbId'] = oneEqpInfo['enbId']
					eqpInfoByOprrId['emsNm'] = oneEqpInfo['emsNm']
					eqpInfoByOprrId['emsIp'] = oneEqpInfo['emsIp']
					eqpInfoByOprrId['eqpId'] = oneEqpInfo['eqpId']
					eqpInfoByOprrId['eqpNm'] = oneEqpInfo['eqpNm']
					eqpInfoByOprrId['svrIp'] = oneEqpInfo['svrIp']
					eqpInfoByOprrId['svrCnntAcntgId'] = oneEqpInfo['svrCnntAcntgId']
					eqpInfoByOprrId['rootAcntgUseYn'] = oneEqpInfo['rootAcntgUseYn']
					eqpInfoByOprrId['aprvrId'] = oneEqpInfo['aprvrId']
					eqpInfoByOprrId['workRegrtId'] = oneEqpInfo['workRegrtId']
					eqpInfoByOprrId['oprrId'] = oprrId
					eqpInfoByOprrId['secureGwOprrId'] = oneEqpInfo['secureGwOprrId']
					eqpInfoByOprrId['addUserAcntgId'] = oneEqpInfo['addUserAcntgId']
					eqpInfoByOprrId['cmdTypCd'] = oneEqpInfo['cmdTypCd']
					eqpInfoByOprrId['workFildCd'] = oneEqpInfo['workFildCd']
					
					if 'cmdInfo' in oneEqpInfo :
						eqpInfoByOprrId['cmdInfo'] = oneEqpInfo['cmdInfo']

					if 'scriptInfo' in oneEqpInfo :
						eqpInfoByOprrId['scriptInfo'] = oneEqpInfo['scriptInfo']

					metaDict['eqpInfo'].append(eqpInfoByOprrId)
			
			metaJson = json.dumps(metaDict, ensure_ascii=False)
			__LOG__.Trace('metaJson: {}'.format(metaJson))

			emsWorkInfoPath = os.path.join(self.emsWorkInfoBaseDir, emsIp, workId)
			self._mkdirs(emsWorkInfoPath)
			self._createFile(os.path.join(emsWorkInfoPath, '{}{}'.format(workId, self.JSON_POSTFIX)), metaJson)
			__LOG__.Trace('enbIpList: {}'.format(enbIpList))
			self._createSitefile(os.path.join(emsWorkInfoPath, '{}_sitefiles{}'.format(workId, self.SITEFILE_POSTFIX)), enbIpList) 
			__LOG__.Trace('scriptInfoList: {}'.format(scriptInfoList))
			self._copyFile(eventDate, emsIp, workId, scriptInfoList)
			self._makeSHA256Files(emsIp, workId)
			self._uploadWorkFiles(emsIp, workId)
			## ????????? 2020-09-16 ?????? ##
			
			self._updateDistibuteYn(workId, emsIp, workStaDate)

			############################
			logDict['tacsLnkgRst'] = 'OK'

			stdOutDict						= dict()

			stdOutDict['idx']				= paramDict['idx']
			stdOutDict['workId']			= paramDict['workId']
			stdOutDict['workStaDate']		= paramDict['workStaDate']
			stdOutDict['workEndDate']		= paramDict['workEndDate']
			stdOutDict['workProgStatCd']	= paramDict['workProgStatCd']
			stdOutDict['emsIp']				= paramDict['emsIp']
			stdOutDict['emsNm']				= paramDict['emsNm']
			stdOutDict['oprrId']			= paramDict['oprrId']

			self._mkdirs(self.roleFilePath)

			roleFileName	= '%s.json' % ('_'.join([stdOutDict['workId'], stdOutDict['emsIp']]))

			with open(os.path.join(self.roleFilePath, roleFileName), 'w') as f :
				f.write(json.dumps(stdOutDict))

			self._stdOut(os.path.join(self.roleFilePath, roleFileName))

		except Exception as ex :
			__LOG__.Trace('{} makeWorkFiles process failed. {}'.format(paramDict, ex))
			logDict['tacsLnkgRst'] = 'FAIL'
			logDict['tacsLnkgRsn'] = ex.args
			raise ex
		finally :
			currentDateObj	= datetime.now()
			yyyyMMdd		= currentDateObj.strftime('%Y%m%d')
			currentDate		= currentDateObj.strftime('%Y%m%d%H%M%S')

			logDict['evntTypCd']	= self.exportWorkCode
			logDict['evntDate']		= currentDate
			logDict['workId']		= workId
			logDict['lnkgEqpIp']    = emsIp
			self._writeTacsHistoryFile(yyyyMMdd, currentDate, logDict)

	#################### ????????? 09-16 ?????? #####################################

	def _updateDistibuteYn(self, workId, emsIp, workStaDate) :
		__LOG__.Trace('{} EMS distribute Update'.format(emsIp))
		try :
			key 		= workId[-1]
			partition	= IRISSelectRange.IRISSelectRange().dailyRange(workStaDate)
			hint = '''
					/*+ LOCATION(KEY = {} AND PARTITION = {}) */
				'''.format(key, partition)
			self.irisObj.updateDistributeYn(hint, workId, emsIp, workStaDate)

		except Exception as ex :
			__LOG__.Trace('distribute YN Update failed [idx : {}, workId : {}, emsIp : {}'.format(idx, workId, emsIp))
			raise ex

	############################################################################	

	def _selectWorkInfo(self, workId, workStaDate, emsIp) :
		try :
			key 		= workId[-1]
			partition	= IRISSelectRange.IRISSelectRange().dailyRange(workStaDate)

			hint = '''
				/*+ LOCATION(KEY = {} AND PARTITION = {}) */
			'''.format(key, partition)
			workInfoDict = self.irisObj.selectWorkInfo(hint, workId, workStaDate)
			if not workInfoDict :
				raise Exception('No such workId({}), workStaDate({}), workInfo'.format(workId, workStaDate))
			__LOG__.Trace('workInfoDict: {}'.format(workInfoDict))
			
			eqpInfoList = self.irisObj.selectEqpInfo(hint, workId, workStaDate, emsIp)
			if not eqpInfoList :
				raise Exception('No such workId({}), workStaDate({}), emsIp({}), eqpInfo'.format(workId, workStaDate, emsIp))
			__LOG__.Trace('eqpInfoList: {}'.format(eqpInfoList))

			return workInfoDict, eqpInfoList
		except Exception as ex :
			__LOG__.Trace('selectWorkInfo process failed. {}'.format(ex))
			raise ex

	def _mkdirs(self, directory) :
		isExists = os.path.exists(directory)
		__LOG__.Trace('{} isExists: {}'.format(directory, isExists))
		if not isExists :
			__LOG__.Trace('create directories {}'.format(directory))
			os.makedirs(directory)
	
	def _createFile(self, filePath, contents) :
		f = None
		try :
			f = open(filePath, 'w')
			f.write(contents)
			__LOG__.Trace('{} file is created.'.format(filePath))
		except Exception as ex :
			__LOG__.Trace('{} to file process failed. {}'.format(contents, ex))
			raise ex
		finally :
			if f : f.close()

	def _createSitefile(self, filePath, enbIpList) :
		f = None
		try :
			f = open(filePath, 'w')
			length = len(enbIpList)
			for idx, oneEnbIp in enumerate(enbIpList) :
				if idx == (length - 1) :
					f.write(oneEnbIp)
				else :
					f.write('{}{}'.format(oneEnbIp, '\n'))
			__LOG__.Trace('{} file is created.'.format(filePath))
		except Exception as ex :
			__LOG__.Trace('{} to sitefile process failed. {}'.format(enbIpList, ex))
			raise ex
		finally :
			if f : f.close()

	def _readFile(self, filePath) :
		f = None
		try :
			f = open(filePath, 'r')
			contents = f.read()
			return contents
		except Exception as ex :
			__LOG__.Trace('{} readFile process failed. {}'.format(filePath, ex))
			raise ex
		finally :
			if f : f.close()

	def _copyFile(self, eventDate, emsIp, workId, scriptInfoList) :
		__LOG__.Trace('eventDate({}), emsIp({}), workId({}), scriptInfoList: {}'.format(eventDate, emsIp, workId, scriptInfoList))
		try :
			scptNmList = []
			for oneScriptInfo in scriptInfoList :
				atchdPathFileNm = oneScriptInfo['atchdPathFileNm'] if oneScriptInfo['atchdPathFileNm'] else None
				if not atchdPathFileNm :
					continue
				
				tangoScptNm = os.path.basename(atchdPathFileNm)
				scptNm		= oneScriptInfo['scptNm'] if oneScriptInfo['scptNm'] else None
				
				scptNmDict 				= {}
				scptNmDict[tangoScptNm] = scptNm if scptNm else tangoScptNm
				scptNmList.append(scptNmDict)

			rawWorkInfoPath = os.path.join(self.rawWorkInfoBaseDir, eventDate, workId)
			#rawWorkInfoPath = os.path.join('/home/tacs/DATA/WORKINFO/M_COMP', eventDate, workId)
			copyFilesDict 	= {}
			for oneFile in os.listdir(rawWorkInfoPath) :
				if oneFile.endswith(self.JSON_POSTFIX) or not os.path.isfile(os.path.join(rawWorkInfoPath, oneFile)) :
					continue

				for oneScptNmDict in scptNmList :
					if oneFile in oneScptNmDict :
						copyFilesDict[oneFile] = oneScptNmDict[oneFile]
						break

			__LOG__.Trace('copyFilesDict: {}'.format(copyFilesDict))
			emsWorkInfoPath = os.path.join(self.emsWorkInfoBaseDir, emsIp, workId)
			for k, v in copyFilesDict.items() :
				if not v :
					v = k
				
				srcPath = os.path.join(rawWorkInfoPath, k)
				desPath = os.path.join(emsWorkInfoPath, v)
				shutil.copy2(srcPath, desPath)
				__LOG__.Trace('copyFiles {} -> {}, succeed'.format(srcPath, desPath))
		except Exception as ex :
			__LOG__.Trace('{} copyFiles process failed. {}'.format(scriptInfoList, ex))
			raise ex

	def _makeSHA256Files(self, emsIp, workId) :
		__LOG__.Trace('emsIp({}), workId({})'.format(emsIp, workId))
		try :
			emsWorkInfoPath = os.path.join(self.emsWorkInfoBaseDir, emsIp, workId)
			for oneFile in os.listdir(emsWorkInfoPath) :
				if oneFile.endswith(self.SHA_POSTFIX) or oneFile.startswith('.') or not os.path.isfile(os.path.join(emsWorkInfoPath, oneFile)) :
					continue
				
				contents = self._readFile(os.path.join(emsWorkInfoPath, oneFile))
				__LOG__.Trace('contents: {}'.format(contents))
				if not contents :
					continue

				hexdigest = hashlib.sha256(contents.encode()).hexdigest()
				__LOG__.Trace('hexdigest: {}'.format(hexdigest))
				self._createFile(os.path.join(emsWorkInfoPath, '{}{}'.format(oneFile, self.SHA_POSTFIX)), hexdigest)
		except Exception as ex :
			__LOG__.Trace('makeSHA256Files process failed. {}'.format(ex))
			raise ex

	def _uploadWorkFiles(self, emsIp, workId) :
		__LOG__.Trace('emsIp({}), workId({})'.format(emsIp, workId))
		sftpClient = None
		try :
			#########################################
####			sftpClient = SFTPClient.SftpClient(emsIp, self.port, self.user, self.passwd)
			sftpClient = SFTPClient.SftpClient(emsIp, self.port, 'root','!hello.root0')
			##########################################
			enmWorkInfoPath = os.path.join(self.enmWorkInfoBaseDir, workId)
			sftpClient.mkdirs(enmWorkInfoPath)

			emsWorkInfoPath = os.path.join(self.emsWorkInfoBaseDir, emsIp, workId)
			for oneFile in os.listdir(emsWorkInfoPath) :
				if oneFile.startswith('.') or not os.path.isfile(os.path.join(emsWorkInfoPath, oneFile)) :
					continue
				
				sftpClient.upload(os.path.join(emsWorkInfoPath, oneFile), os.path.join(enmWorkInfoPath, oneFile))
		except Exception as ex :
			__LOG__.Trace('uploadWorkFiles process failed. {}'.format(ex))
			raise ex
		finally :
			if sftpClient : sftpClient.close()

	def _writeTacsHistoryFile(self, yyyyMMdd, eventDate, logDict) :
		if logDict :
			__LOG__.Trace('received workInfo history: {}'.format(logDict))
			try :
				tacsHistoryTempPath = os.path.join(self.auditLogTempDir, 'AUDIT_{}'.format(self.exportWorkCode))
				self._mkdirs(tacsHistoryTempPath)

				contents = json.dumps(logDict, ensure_ascii=False)
				__LOG__.Trace('contents: {}'.format(contents))

				tacsHistoryFilename = self._getTacsHistoryFilename(yyyyMMdd, eventDate)
				__LOG__.Trace('tacsHistoryFilename: {}'.format(tacsHistoryFilename))
				self._createFile(os.path.join(tacsHistoryTempPath, tacsHistoryFilename), contents)

				tacsHistoryPath = os.path.join(self.auditLogBaseDir, 'AUDIT_{}'.format(self.exportWorkCode))
				self._mkdirs(tacsHistoryPath)

				shutil.move(os.path.join(tacsHistoryTempPath, tacsHistoryFilename), os.path.join(tacsHistoryPath, tacsHistoryFilename))
				__LOG__.Trace('tacsHistory file move from {} -> to {} succeed.'.format(os.path.join(tacsHistoryTempPath, tacsHistoryFilename), os.path.join(tacsHistoryPath, tacsHistoryFilename)))
			except Exception as ex :
				__LOG__.Trace('tacsHistory {} load process failed. {}'.format(logDict, ex))
		else :
			__LOG__.Trace('received workInfo history({}) is invalid.'.format(logDict))

	def _getTacsHistoryFilename(self, yyyyMMdd, eventDate) :
		HHmm                = datetime.strptime(eventDate, '%Y%m%d%H%M%S').strftime('%H%M')
		tacsHistoryFilename = '{}_{}_{}.audit'.format(yyyyMMdd, HHmm, uuid.uuid4())
		return tacsHistoryFilename

	def run(self) :
		while not SHUTDOWN :
			try :
				strIn	= sys.stdin.readline()
				stdLine = strIn.strip()
				if stdLine :
					if not '://' in stdLine :
						if stdLine.strip() :
							stdLine 	= stdLine.replace('\'', '"')
							paramDict	= json.loads(stdLine)

							if not ('queueMsg' in paramDict) :
								self._makeWorkFiles(paramDict)		

							self._stderr(strIn)

						else :
							self._stderr(strIn)

					else :
						self._stderr(strIn)

				else :
					self._stderr(strIn)
			except :
				__LOG__.Exception()
				self._stderr(strIn)

		__LOG__.Trace('run is terminated')

def main() :
	reload(sys)
	sys.setdefaultencoding('UTF-8')
	
	module 	= os.path.basename(sys.argv[0])
	section = sys.argv[1]
	cfgfile	= sys.argv[2]

	cfg = ConfigParser.ConfigParser()
	cfg.read(cfgfile)

	logPath = cfg.get("GENERAL", "LOG_PATH")
	logFile = os.path.join(logPath, "%s_%s.log" % (module, section))

	logCfgPath = cfg.get("GENERAL", "LOG_CONF")

	logCfg = ConfigParser.ConfigParser()
	logCfg.read(logCfgPath)

	Log.Init(Log.CRotatingLog(logFile, logCfg.get("LOG", "MAX_SIZE"), logCfg.get("LOG", "MAX_CNT") ))

	workInfoDistributer = WorkInfoDistributer(cfg)
	workInfoDistributer.run()

	__LOG__.Trace('main is terminated.')

if __name__ == '__main__' :
	try :
		main()
	except :
		__LOG__.Exception() 
