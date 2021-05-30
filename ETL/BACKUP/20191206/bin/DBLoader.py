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
	
			vendor = oneEqpInfoDict['vendor']
	
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
		eqpType		= ''
		expEmsNm	= ''
		expEmsIp	= ''

		if oneEqpInfoDict['svrIp'] == None or oneEqpInfoDict['svrIp'] == '' :
			__LOG__.Trace('IP Not Exists')

		else :
			__LOG__.Trace('IP Exists')

			svrIpList	= oneEqpInfoDict['svrIp'].split(';')
			svrIpList = [ x.strip() for x in svrIpList if x.strip() ]

			emsIp		= oneEqpInfoDict['emsIp']

			eqpEmsNm	= self.IRISDB.selectIpEqpData( svrIpList )

			if emsIp == None or emsIp == '' :
				__LOG__.Trace( 'emsIp is Empty And Dont Select' )

			else :
				expEmsNm, expEmsIp, vendor, eqpTyp	= self.IRISDB.selectIpEmsData( emsIp )

		if eqpEmsNm == None or eqpEmsNm == ''  :
			eqpNm	= oneEqpInfoDict['eqpNm'].encode('utf-8')
			__LOG__.Trace(' IP is Unable Nm Select! : %s ' % eqpNm )
			
			if eqpNm == None or eqpNm == '' :
				__LOG__.Trace( 'eqpNm is Empty And Dont Select' )
			
			else :
				eqpEmsNm	= self.IRISDB.selectNmEqpData( eqpNm )

		if vendor == None or vendor == '' :
			emsNm	= oneEqpInfoDict['emsNm'].encode('utf-8')
			__LOG__.Trace(' EMS IP IS Unable Nm Select! : %s ' % emsNm )

			if emsNm == None or emsNm == '' :
				__LOG__.Trace( 'emsNm is Empty And Not Select' )

			else :
				expEmsNm, expEmsIp, vendor, eqpTyp	= self.IRISDB.selectNmEmsData( emsNm )

		oneEqpInfoDict['eqpId']		= eqpEmsNm
		oneEqpInfoDict['vendor']	= vendor
		oneEqpInfoDict['eqpTyp']	= eqpTyp
		oneEqpInfoDict['expEmsIp']	= expEmsIp
		oneEqpInfoDict['expEmsNm']	= expEmsNm

		return oneEqpInfoDict

	def parseEqpData (self, oneEqpInfoDict) :
		
		oneEqpInfoDict['idx']			= self.repo['workInfo']['idx']
		oneEqpInfoDict['workStaDate']	= self.repo['workInfo']['workStaDate']
		oneEqpInfoDict['workId']		= self.repo['workInfo']['workId']
		oneEqpInfoDict['tangoEqpId']	= self.jsonValue( oneEqpInfoDict, 'eqpId' )

		oneEqpInfoDict  = self.getEmsData( oneEqpInfoDict )

		# 김준우 추가 / enbId를 eqpId로 보내주기 위한 로직
		if 'cmdWorkTypCd' in oneEqpInfoDict and oneEqpInfoDict['cmdWorkTypCd'] == 4 :
			oneEqpInfoDict['eqpId']		= oneEqpInfoDict['enbId']

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
			if not oneEqpInfo['expEmsIp'] in checkIpList :
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

	def valueTrim(self, dictVal) :
		for oneKey in dictVal :
			dictVal[oneKey] = dictVal[oneKey].strip()

		return dictVal

	def jsonParse(self, filePath)	:
		if not os.path.exists(filePath) : 
			__LOG__.Trace('File not Exists')
			return

		__LOG__.Trace('readFile Exists')
		jsonStrObj		= self.getContents(filePath)
		jsonDataDict	= json.loads(jsonStrObj)
	
		jsonDataDict['workInfo'] = self.valueTrim(jsonDataDict['workInfo'])

		tempEqpInfoList = list()

		for oneEqpInfoList in jsonDataDict['eqpInfo'] :
			oneEqpInfoList = self.valueTrim(oneEqpInfoList)
			tempEqpInfoList.append(oneEqpInfoList)

		jsonDataDict['eqpInfo'] = tempEqpInfoList

		self.repo['workInfo']   = jsonDataDict['workInfo']
		self.repo['eqpInfo']    = jsonDataDict['eqpInfo']

		tempEqpList = list()

		insertFlag 	= self.dateCheck()

		if insertFlag :
			loadFlag	= False
			self.repo['workInfo']['idx'] = self.IRISDB.selectIDX()

			for oneEqpInfoDict in self.repo['eqpInfo'] :
				oneEqpInfoDict = self.parseEqpData( oneEqpInfoDict )
				tempEqpList.append( oneEqpInfoDict )
	
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
			
			self.mkdirs( errLoadPath )

			errFilePath	= os.path.join( errLoadPath, jsonFileName )

			shutil.move( filePath, errFilePath )

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
