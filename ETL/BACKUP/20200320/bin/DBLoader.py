#!/usr/bin/env python
# -*- coding: utf-8 -*-
import ConfigParser
import datetime
import glob
import signal
import sys
import os
import csv
from collections import OrderedDict
# from AESCipher import AESCipher
import API.M6 as M6

import time
import json
import IRISSelect
import Mobigen.Common.Log as Log; Log.Init()
import shutil
import IRISSelectRange
import AESDecode
## Process Exit Flag
SHUTDOWN = False

## Signal Process Function
#
# @param signum Signal Number
# @param frame	Now Stack Frame
def handler(signum, frame):
	global SHUTDOWN
	SHUTDOWN = True
	__LOG__.Trace("Catch Signal = %s" % signum)

## SIGTERM 
signal.signal(signal.SIGTERM, handler)
## SIGINT 
signal.signal(signal.SIGINT, handler)
## SIGHUP 
signal.signal(signal.SIGHUP , handler)
## SIGPIPE 
signal.signal(signal.SIGPIPE, handler)

class DBLoader():
	def __init__(self, section, cfg):
		self.section = section 
		self.cfg = cfg

		IRIS_CONF	 			= self.cfg.get("GENERAL", "IRIS_CONF")
		self.STD_OUT_PATH		= self.cfg.get("GENERAL", "STD_OUT_PATH")

		AES	= AESDecode.AESDecode()

		irisCfg = ConfigParser.ConfigParser()
		irisCfg.read(IRIS_CONF)

		self.IRIS  			= irisCfg.get("IRISDB","IRIS")
		self.IRIS_ID 		= irisCfg.get("IRISDB","IRIS_ID")
		self.IRIS_PASS 		= AES.decodeAES(irisCfg.get("IRISDB","IRIS_PASS"))
		self.IRIS_DB		= irisCfg.get("IRISDB","IRIS_DB")

		self.IRIS_LOAD_PATH		= self.cfg.get(section, "LOAD_PATH")
		self.IRIS_ERR_LOAD_PATH	= self.cfg.get(section, "ERR_DIR")

		self.DATETIME_MICRO_FORMAT	= self.cfg.get("DATE_FORMAT", "DATETIME_MICRO_FORMAT")
		self.DATETIME_MIN_FORMAT	= self.cfg.get("DATE_FORMAT", "DATETIME_MIN_FORMAT")
		self.DATETIME_FORMAT		= self.cfg.get("DATE_FORMAT", "DATETIME_FORMAT")

		self.IRIS_TAB 		= irisCfg.get("IRISDB","IRIS_TAB")
		self.IRIS_EQP_TAB 	= irisCfg.get("IRISDB","IRIS_CMD_TAB")

		self.repo			= dict()

		self.IRISDB			= IRISSelect.IRIS_SQL(self.IRIS,self.IRIS_ID,self.IRIS_PASS)
		self.partRange		= IRISSelectRange.IRISSelectRange()
		self.ANIMP_CD		= 'BP99999999'

	## StdOut 
	# @param self	
	# @param msg	
	def writeStdOut(self, msg):
		sys.stdout.write(msg+'\n')
		sys.stdout.flush()
		__LOG__.Trace("STD OUT: %s" % msg)

	## Error
	#
	# @param self	
	# @param msg	
	def writeStdErr(self, msg):
		sys.stderr.write('%s\n' % (msg))
		sys.stderr.flush()
		__LOG__.Trace('STD ERR: %s' % (msg))

	def getContents(self, filePath) :
		f = open(filePath, "r")
		contents = f.read()
		f.close()
		return str(contents).encode('utf-8')

	def setContents(self, fileDir, fileName, contents) :
		f = open(os.path.join(fileDir, fileName), "w")
		f.write(str(contents).encode('utf-8'))
		f.close() 


	def parseWorkData( self, workInfoDict, vendor ) :

		try :
		    workInfoDict['workEndDate']  = datetime.datetime.strptime(workInfoDict['workEndDate'],self.DATETIME_MICRO_FORMAT).strftime(self.DATETIME_FORMAT)
		except :
		    workInfoDict['workEndDate']  = datetime.datetime.strptime(workInfoDict['workEndDate'],self.DATETIME_MIN_FORMAT).strftime(self.DATETIME_FORMAT)
		
		try :
		    workInfoDict['lastChgDate']  = datetime.datetime.strptime(workInfoDict['lastChgDate'],self.DATETIME_MIN_FORMAT).strftime(self.DATETIME_FORMAT)
		except :
		    workInfoDict['lastChgDate']  = datetime.datetime.strptime(workInfoDict['lastChgDate'],self.DATETIME_MICRO_FORMAT).strftime(self.DATETIME_FORMAT)
	
		workInfoDict['vendor']       = vendor
		
		return workInfoDict

	def makeStdOutFile( self ) :
		self.repo['exportInfo']      = self.parseExportData()
		tempExportInfoList	= list()

		for oneExportInfo in self.repo['exportInfo'] :

			oneExportInfo['lkngUnitDict']	= self.IRISDB.selectLkngUnit( oneExportInfo['expEmsIp'] )

			__LOG__.Trace( 'lkngUnitDict of %s : %s ' % ( oneExportInfo['expEmsIp'], oneExportInfo['lkngUnitDict'] ) )

			tempExportInfoList.append( oneExportInfo )

		self.repo['exportInfo'] = tempExportInfoList

		stdOutFilePath   = self.STD_OUT_PATH % datetime.datetime.now().strftime(self.DATETIME_FORMAT)

		outFile      = open(stdOutFilePath, 'w')
		outFile.write( json.dumps(self.repo, encoding='utf-8') )

		#outFile.write(json.dumps(stdOutMap, encoding='utf-8'))
		outFile.close()
		self.writeStdOut("file://%s" % stdOutFilePath )

	def mkdirs(self, path) :
		try : os.makedirs(path)
		except OSError as exc: #Python > 2.5
			#if exc.errno == errno.EEXIST and os.path.isdir(path) : 
			pass
			#else : raise

	def writeWorkInfoToDatFile( self, workInfo ) :
		workInfoColumns = self.cfg.get(self.section, 'WORK_INFO_COLUMNS').replace(',', '|^-^|')
		workInfoKeys 	= self.cfg.get(self.section, 'WORK_INFO_KEYS').split(',')

		loadFileDir	 	= self.IRIS_LOAD_PATH % workInfo['workId']

		self.mkdirs(loadFileDir)
		self.setContents(loadFileDir, 'workInfo.ctl', workInfoColumns)

		workInfoArr = []
		for oneKey in workInfoKeys :
			if not oneKey in workInfo.keys() :
				workInfo[oneKey] = None

			if workInfo[oneKey] == None :
				workInfoArr.append('')
			else :
				workInfoArr.append(workInfo[oneKey])

		self.setContents(loadFileDir, 'workInfo.dat', '|^|'.join(workInfoArr))

		ctlFilePath     = os.path.join(loadFileDir, 'workInfo.ctl')
		datFilePath     = os.path.join(loadFileDir, 'workInfo.dat')

		resultLoad	= self.cursor.Load( self.IRIS_TAB, workInfo['workId'][-1], workInfo['workStaDate'], ctlFilePath, datFilePath )
		__LOG__.Trace( 'WORK Load RESULT : %s ' % resultLoad )

		return resultLoad


	def writeEqpInfoToDatFile(self, workInfo, eqpInfoList) :
		eqpInfoColumns 	= self.cfg.get(self.section, 'EQUIP_INFO_COLUMNS').replace(',', '|^-^|')
		eqpInfoKeys		= self.cfg.get(self.section, 'EQUIP_INFO_KEYS').split(',')
		#workId 			= eqpInfoList[0]['workId']

		loadFileDir		= self.IRIS_LOAD_PATH % workInfo['workId']
		
		self.mkdirs(loadFileDir)

		self.setContents(loadFileDir, 'eqpInfo.ctl', eqpInfoColumns)

		eqpInfoResult = list()
		for oneEqpInfo in eqpInfoList :
			eqpInfoArr 	= list()
			for oneEqpKey in eqpInfoKeys :
				if not oneEqpKey in oneEqpInfo :
					oneEqpInfo[oneEqpKey] = None	

				if oneEqpKey == 'scriptInfo' or oneEqpKey == 'cmdInfo' :
					if oneEqpKey in oneEqpInfo :
						if not oneEqpInfo[oneEqpKey] == None :
							eqpInfoArr.append(str(json.dumps(oneEqpInfo[oneEqpKey], encoding='utf-8')))
						else :
							eqpInfoArr.append(None)
					else :
						eqpInfoArr.append('')
				else :
					eqpInfoArr.append(oneEqpInfo[oneEqpKey])
	
			for eqpIdx in xrange(len(eqpInfoArr)) :
				if eqpInfoArr[eqpIdx] == None :
					eqpInfoArr[eqpIdx] = ''
	
			oneEqpInfoData 	= '|^|'.join(eqpInfoArr)

			eqpInfoResult.append(oneEqpInfoData)

		self.setContents(loadFileDir, 'eqpInfo.dat', '|^-^|'.join(eqpInfoResult))

		ctlFilePath     = os.path.join(loadFileDir, 'eqpInfo.ctl')
		datFilePath     = os.path.join(loadFileDir, 'eqpInfo.dat')

		resultLoad		= self.cursor.Load( self.IRIS_EQP_TAB, workInfo['workId'][-1], workInfo['workStaDate'], ctlFilePath, datFilePath )
		__LOG__.Trace( 'EQP Load RESULT : %s ' % resultLoad )

		return resultLoad


	def getLinkedVendor( self, eqpInfoList ) :
		vendor = ''

		for oneEqpInfoDict in eqpInfoList :
			if 'vendor' in oneEqpInfoDict :
				vendor = oneEqpInfoDict['vendor']
			__LOG__.Trace(vendor)
			if not vendor == None and not vendor == ''  : break

		return vendor

	# DB 연결 및 Commit , close
	def irisInitConnection(self):
		try :
			conn = M6.Connection(self.IRIS, self.IRIS_ID, self.IRIS_PASS, Database = self.IRIS_DB)
			self.conn = conn

			cursor = conn.cursor()
			cursor.SetFieldSep('|^|')
			cursor.SetRecordSep('|^-^|')
			self.cursor = cursor

		except :
			__LOG__.Exception()
			time.sleep(60)
			self.irisInitConnection()
	
	def irisDisConnection(self) :
		self.conn.close()

	def getEmsData(self, oneEqpInfoDict) :

		eqpEmsNm 	= ''
		vendor		= ''
		eqpTyp		= ''
		expEmsNm	= ''
		expEmsIp	= ''
		tacsEqpId	= ''
		emsIp		= ''
		## 김준우 추가 2020-02-25 ######
		ompDataList = list()
		ompIdList	= list()
		emsEqpId	= None
		################################

		emsIp		= oneEqpInfoDict['emsIp'].strip()
		expEmsNm, expEmsIp, vendor, eqpTyp, tacsEqpId	= self.IRISDB.selectIpEmsData( emsIp )

		if self.jsonValue(oneEqpInfoDict, 'emsIp') == None or self.jsonValue(oneEqpInfoDict, 'emsIp') == '' :
			__LOG__.Trace(' Tango-raw_ems_ip : %s' % self.jsonValue(oneEqpInfoDict, 'emsIp'))
			__LOG__.Trace( 'emsIp is Empty And Dont Select' )

		elif ( expEmsIp == None or expEmsIp == '' ) or ( expEmsNm == None or expEmsNm == '' ) or ( vendor == None or vendor == '' ) or ( eqpTyp == None or eqpTyp == '' or eqpTyp not in 'EMS' ) or ( tacsEqpId == None or tacsEqpId == '' ) :
			__LOG__.Trace('EXP EMS IP : %s , EXP EMS NM : %s, VENDOR : %s, EQP TYP : %s, tacsEqpId : %s' %( expEmsIp, expEmsNm, vendor, eqpTyp, tacsEqpId) )
#			__LOG__.Trace(expEmsNm)
#			__LOG__.Trace(vendor)
#			__LOG__.Trace(eqpTyp)
#			__LOG__.Trace(tacsEqpId)
			__LOG__.Trace( 'emsIp not in T-ACS')

		else :
			if ( vendor != None and vendor != '' ) and  vendor == self.ANIMP_CD   :
				__LOG__.Trace('Animp Work')

			elif self.jsonValue( oneEqpInfoDict, 'cmdWorkTypCd' ) == '4'  :
				__LOG__.Trace('Access Work')

			elif (self.jsonValue(oneEqpInfoDict, 'svrIp') != None and self.jsonValue(oneEqpInfoDict, 'svrIp') != '') and (tacsEqpId != None and tacsEqpId.strip() != '') :
				__LOG__.Trace('IP Exists')

				svrIpList	= oneEqpInfoDict['svrIp'].split(';')
				svrIpList = [ x.strip() for x in svrIpList if x.strip() ]
				
				eqpEmsNm	= self.IRISDB.selectIpCommonEqpData( svrIpList, tacsEqpId )
				__LOG__.Trace('NE EQP_ID : %s' % eqpEmsNm)
				## 김준우 추가 2020-02-25 ###############################################
				if eqpEmsNm == None or eqpEmsNm == '' :
					ompDataList = self.IRISDB.selectIdOmpData(tacsEqpId)
					__LOG__.Trace('EMS Down OMP LIST : %s' % ompDataList)
			
					for oneOmpData in ompDataList :
						if oneOmpData['tacsOmpEqpTyp'] == 'EMS' :
							ompIdList.append(oneOmpData['tacsOmpEqpId'])
					
					if ompIdList :
						eqpEmsNm    = self.IRISDB.selectIpOtherEqpData( svrIpList, ompIdList )
						__LOG__.Trace('OMP EQP_ID : %s' % eqpEmsNm)

					else :
						__LOG__.Trace('EMS IP : %s is NOT EMS-OMP-NE layer' % emsIp )

			#########################################################################

			else :
				__LOG__.Trace('eqpInfo is invalid | EMS IP : %s , EXP EMS IP : %s, VENDOR : %s, CmdWorkTypCd : %s, NE IP : %s' % (self.jsonValue(oneEqpInfoDict, 'emsIp'), self.jsonValue(oneEqpInfoDict, 'expEmsIp'), self.jsonValue(oneEqpInfoDict, 'vendor'), self.jsonValue(oneEqpInfoDict, 'cmdWorkTypCd'), self.jsonValue(oneEqpInfoDict, 'svrIp') ) )

#		if (eqpEmsNm == None or eqpEmsNm == '') or (vendor == None or vendor == '')  :
#			eqpNm	= oneEqpInfoDict['eqpNm'].encode('utf-8')
#			__LOG__.Trace(' IP is Unable Nm Select! : %s ' % eqpNm )
#			__LOG__.Trace(' Vendor or Vendor EMS NM IS Invalid !! [%s]' % emsIp )

#			if eqpNm == None or eqpNm.strip() == '' :
#				__LOG__.Trace( 'eqpNm is Empty And Dont Select' )
			
#			else :
#				eqpEmsNm	= self.IRISDB.selectNmEqpData( eqpNm )

#		if vendor == None or vendor == '' :
#			emsNm	= oneEqpInfoDict['emsNm'].encode('utf-8').strip()
#			__LOG__.Trace(' EMS IP IS Unable Nm Select! : %s ' % emsNm )

#			if emsNm == None or emsNm.strip() == '' :
#				__LOG__.Trace( 'emsNm is Empty And Not Select' )

#			else :
#				expEmsNm, expEmsIp, vendor, eqpTyp	= self.IRISDB.selectNmEmsData( emsNm )
		__LOG__.Trace(eqpEmsNm)
		oneEqpInfoDict['eqpId']		= eqpEmsNm
		oneEqpInfoDict['vendor']	= vendor
		oneEqpInfoDict['eqpTyp']	= eqpTyp
		oneEqpInfoDict['expEmsIp']	= expEmsIp
		__LOG__.Trace(oneEqpInfoDict['expEmsIp'])
		oneEqpInfoDict['expEmsNm']	= expEmsNm

		return oneEqpInfoDict

	def parseEqpData (self, oneEqpInfoDict) :
		expValidYn	= 'N'
		oneEqpInfoDict['idx']			= self.jsonValue(self.repo['workInfo'], 'idx')
		oneEqpInfoDict['workStaDate']	= self.jsonValue(self.repo['workInfo'], 'workStaDate')
		oneEqpInfoDict['workId']		= self.jsonValue(self.repo['workInfo'], 'workId')
		oneEqpInfoDict['tangoEqpId']	= self.jsonValue( oneEqpInfoDict, 'eqpId' )

		oneEqpInfoDict  = self.getEmsData( oneEqpInfoDict )

		# 김준우 추가 / enbId를 eqpId로 보내주기 위한 로직
		# Access 의 cmdWorkTypCd = 4
		
		if self.jsonValue( oneEqpInfoDict, 'cmdWorkTypCd' ) == '4' and self.jsonValue( oneEqpInfoDict, 'expEmsIp') :
			oneEqpInfoDict['eqpId']		= self.jsonValue( oneEqpInfoDict, 'enbId' )

		# Animp 의 cmdWorkTypCd = 7
		if self.jsonValue( oneEqpInfoDict, 'vendor' ) == self.ANIMP_CD or ( self.jsonValue( oneEqpInfoDict, 'eqpId' ) != '' and self.jsonValue( oneEqpInfoDict, 'eqpId' ) != None ):
			expValidYn = 'Y'

		oneEqpInfoDict['expValidYn'] = expValidYn

		if oneEqpInfoDict['expValidYn'] == 'N' :
			__LOG__.Trace('!!! Exception !!! NOT Export EMS : workId - %s / emsIp - %s' % ( oneEqpInfoDict['workId'], oneEqpInfoDict['emsIp'] ) )
		__LOG__.Trace(oneEqpInfoDict)

		return oneEqpInfoDict

	def checkWorkInfo (self, dbLastDate) :
		try :
			jsonChgDate = datetime.datetime.strptime(self.repo['workInfo']['lastChgDate'], self.DATETIME_MIN_FORMAT).strftime(self.DATETIME_FORMAT)

		except :
			__LOG__.Exception()
			jsonChgDate = datetime.datetime.strptime(self.repo['workInfo']['lastChgDate'], self.DATETIME_MICRO_FORMAT).strftime(self.DATETIME_FORMAT)

		__LOG__.Trace('Input Data workId = %s' % self.repo['workInfo']['workId'])

		if jsonChgDate > dbLastDate  :
			__LOG__.Trace( 'DATA Change' )
			return True

		elif jsonChgDate == dbLastDate :
			__LOG__.Trace('DATA NOT Update')
			return False

		else :
			__LOG__.Trace('Update ERROR !! ')
			return False


	def dateCheck (self) :

		try :
			self.repo['workInfo']['workStaDate']  = datetime.datetime.strptime(self.repo['workInfo']['workStaDate'], self.DATETIME_MICRO_FORMAT).strftime(self.DATETIME_FORMAT)

		except :
			self.repo['workInfo']['workStaDate']  = datetime.datetime.strptime(self.repo['workInfo']['workStaDate'], self.DATETIME_MIN_FORMAT).strftime(self.DATETIME_FORMAT)

		partition	= self.partRange.dailyRange( self.repo['workInfo']['workStaDate'] )
		#partition	= ''.join( [ self.repo['workInfo']['workStaDate'][0:8] ,'000000' ] )

		dbLastDate  = self.IRISDB.selectDate( self.repo['workInfo']['workId'], partition )

		insertFlag	= self.checkWorkInfo(dbLastDate)

		return insertFlag

	def parseExportData( self ) :

		exportList	= list()
		checkIpList	= list()

		for oneEqpInfo in self.repo['eqpInfo'] :
			if self.jsonValue( oneEqpInfo, 'expEmsIp' ) == '' :
				continue

			if not self.jsonValue( oneEqpInfo, 'expEmsIp' ) in checkIpList and self.jsonValue( oneEqpInfo, 'expValidYn' ) == 'Y' :
				checkIpList.append( oneEqpInfo['expEmsIp'] )
				exportList.append( self.makeExportInfo( oneEqpInfo, self.repo['workInfo'] ) )

		return exportList

	def makeExportInfo( self, oneEqpInfo, workInfoDict ) :
		exportDict	= dict()

		exportDict['eqpTyp']		= oneEqpInfo['eqpTyp']
		exportDict['expEmsIp']		= oneEqpInfo['expEmsIp']
		exportDict['expEmsNm']		= oneEqpInfo['expEmsNm']
		exportDict['idx']			= workInfoDict['idx']
		exportDict['workId']		= workInfoDict['workId']
		exportDict['workStaDate']	= workInfoDict['workStaDate']
		exportDict['vendor']		= workInfoDict['vendor']
		exportDict['cmdWorkTypCd']	= oneEqpInfo['cmdWorkTypCd']
		exportDict['oprrId']		= oneEqpInfo['oprrId']

		return exportDict

	def jsonValue(self, dictNm, key) :
		if not key in dictNm.keys() :
			return ''

		else :
			return dictNm[key]

	def trimJsonData(self, jsonData) :
		if isinstance(jsonData, list) :
			for oneDict in jsonData :
				for key in oneDict :
					if isinstance(oneDict[key], list) :
						for oneDict2 in oneDict[key] :
							for key in oneDict2 :
								if not oneDict2[key] is None : 
									oneDict2[key] = oneDict2[key].strip()
					else :
						if not oneDict[key] is None : 
							oneDict[key] = oneDict[key].strip()

		elif isinstance(jsonData, dict) :
			for key in jsonData :
				if not jsonData[key] is None :
					jsonData[key] = jsonData[key].strip()

		else :
			__LOG__.Trace('need to jsonData')

		return jsonData

	def workCollectCheck(self) :
		collectFlag = False
		workId 		= self.repo['workInfo']['workId'].encode('utf-8')
		lastChgDate = self.repo['workInfo']['lastChgDate'].encode('utf-8')

		workingCnt 	= self.IRISDB.selectCollectCheck(workId, lastChgDate)

		if 0 == int(workingCnt) :
			self.IRISDB.insertCollectCheck(workId, lastChgDate)

		elif 0 < int(workingCnt) :
			self.IRISDB.deleteCollectCheck(workId, lastChgDate)
			collectFlag = True

		else :
			collectFlag = False

		return collectFlag

	def jsonParse(self, filePath)	:
		if not os.path.exists(filePath) : 
			__LOG__.Trace('File not Exists')
			return

		__LOG__.Trace('readFile Exists')
		jsonStrObj		= self.getContents(filePath)
		jsonDataDict	= json.loads(jsonStrObj)
		
		self.repo['workInfo']   = self.trimJsonData(jsonDataDict['workInfo'])
		self.repo['eqpInfo']    = self.trimJsonData(jsonDataDict['eqpInfo'])

		__LOG__.Trace(self.repo)

		tempEqpList = list()

		if self.workCollectCheck() :
			return

		insertFlag 	= self.dateCheck()

		if insertFlag :
			loadFlag	= False
			self.repo['workInfo']['idx'] = self.IRISDB.selectIDX()

			expValid 	= 'N'
			# tobe... 20200106
			for oneEqpInfoDict in self.repo['eqpInfo'] :
				oneEqpInfoDict = self.parseEqpData( oneEqpInfoDict )
				tempEqpList.append( oneEqpInfoDict )
				if expValid == 'N' and  self.jsonValue( oneEqpInfoDict, 'expValidYn') == 'Y' :
					expValid = 'Y'
	
			self.repo['eqpInfo'] = tempEqpList

			workVendor = self.getLinkedVendor( self.repo['eqpInfo'] )

			self.repo['workInfo'] 	= self.parseWorkData( self.repo['workInfo'], workVendor )

			loadFlag	= self.loadWorkInfo( filePath )

			if loadFlag :

				self.makeStdOutFile()

			else :
				__LOG__.Trace('DB LOAD Dont Execute')

		else :
			__LOG__.Trace('IRIS DB Data Already Exists, No Load Data')
		

	def loadWorkInfo( self, filePath ) :
		resultWorkLoad	= self.writeWorkInfoToDatFile( self.repo['workInfo'] ) 	
		resultEqpLoad	= self.writeEqpInfoToDatFile( self.repo['workInfo'], self.repo['eqpInfo'] )

		if 'OK' in resultWorkLoad and 'OK' in resultEqpLoad :
		
			self.IRISDB.updateIdx()

			return True
		
		else :
			self.IRISDB.deleteWorkEqpInfo( self.repo['workInfo']['idx'], self.repo['workInfo']['workId'], self.repo['workInfo']['workId'][-1] , self.partRange.dailyRange ( self.repo['workInfo']['workStaDate'] ) )

			jsonFileName	= os.path.basename( filePath )
			errLoadPath = self.IRIS_ERR_LOAD_PATH % (  self.repo['workInfo']['workId'], self.repo['workInfo']['idx'] )
			self.mkdirs(errLoadPath)
			errFilePath	= os.path.join( errLoadPath, jsonFileName )
			__LOG__.Trace('fileCopy FROM : %s , TO : %s ' %( filePath, errFilePath ))
			shutil.copy( filePath, errFilePath )
			__LOG__.Trace("fileCopy Success")

	# DBLoader Start
	def run(self):
		__LOG__.Trace("DBLoader run!")

		while not SHUTDOWN:
			strIn	= ''
			strLine = ''
			try:
				strIn	= sys.stdin.readline()
				strLine = strIn.strip()
				if strLine == '':
					continue
			except:
				if not SHUTDOWN:
					__LOG__.Exception()
					continue			

			prefix, filePath = strLine.split("://")			
	
			try:
				self.irisInitConnection()
				self.jsonParse(filePath)
				self.irisDisConnection()
	
			except:
				__LOG__.Exception()

			finally :
				self.writeStdErr(strIn)

def main():
	module = os.path.basename(sys.argv[0])
	section = sys.argv[1] 		# TACS_DB_LOADER
	cfgFile = sys.argv[2] 	

	cfg = ConfigParser.ConfigParser() 
	cfg.read(cfgFile)

	logPath = cfg.get("GENERAL", "LOG_PATH")
	logFile = os.path.join(logPath, "%s_%s.log" % (module, section))

	logCfgPath = cfg.get("GENERAL", "LOG_CONF")

	logCfg = ConfigParser.ConfigParser()
	logCfg.read(logCfgPath)

	Log.Init(Log.CRotatingLog(logFile, logCfg.get("LOG", "MAX_SIZE"), logCfg.get("LOG", "MAX_CNT") ))

	dl = DBLoader(section, cfg)
	dl.run()


if __name__ == "__main__":
	reload(sys)
	sys.setdefaultencoding('utf-8')
	try:
		main()
	except:
		__LOG__.Exception()
