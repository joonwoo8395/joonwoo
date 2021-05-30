#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import sys
import signal
import ConfigParser
import json
import re
import Mobigen.Common.Log as Log; Log.Init()
import datetime
import shutil
import API.M6 as M6
import AESDecode

def handler(signum, frame):
	__LOG__.Trace( 'signal: process shutdown %s' % signum )


# SIGTERM
signal.signal(signal.SIGTERM, handler)
# SIGINT
signal.signal(signal.SIGINT, handler)
# SIGHUP
signal.signal(signal.SIGHUP, handler)
# SIGPIPE
signal.signal(signal.SIGPIPE, handler)

SHUTDOWN	= False 

def camelToUnder(text) :
	textString = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', text)
	return re.sub('([a-z0-9])([A-Z])', r'\1_\2', textString).upper()

class LogLoader :
	def __init__(self, cfg, section):
		self.section = section
		self.cfg = cfg
		self.IRIS_PATH		= self.cfg.get(section, 'IRIS_PATH')
		self.LOAD_FILE_PATH	= self.cfg.get(section, 'LOAD_FILE_PATH')

		AES	= AESDecode.AESDecode()

		irisCfg         = ConfigParser.ConfigParser()
		irisCfg.read(self.IRIS_PATH)

		self.IRIS_IP	= irisCfg.get('IRISDB', 'IRIS')
		self.IRIS_ID	= irisCfg.get('IRISDB', 'IRIS_ID')
		self.IRIS_PW	= AES.decodeAES(irisCfg.get('IRISDB', 'IRIS_PASS'))
		self.IRIS_DB	= irisCfg.get('IRISDB', 'IRIS_DB')
		self.TABLE		= irisCfg.get('IRISDB', 'MQ_TAB')

	def mkdirs(self, path) :
		try : os.makedirs(path)
		except OSError as exc: #Python > 2.5
			#if exc.errno == errno.EEXIST and os.path.isdir(path) : 
			pass
			#else : raise


		
	def stdOut(self, msg):
		sys.stdout.write(msg+'\n')
		sys.stdout.flush()
		# print(msg, file=sys.stdout)
		__LOG__.Trace('STD OUT: %s' % msg)
		
	def stdErr(self, msg):
		sys.stderr.write('%s\n' % (msg))
		sys.stderr.flush()
		__LOG__.Trace('STD ERR: %s' % msg)

	def irisInitConnection(self) :
		conn = M6.Connection (self.IRIS_IP, self.IRIS_ID, self.IRIS_PW, Database = self.IRIS_DB)
		self.conn = conn

		try :
			cursor = conn.cursor()
			cursor.SetFieldSep('|^|')
			cursor.SetRecordSep('|^-^|')
			self.cursor	= cursor

		except :
			__LOG__.Exception()
			time.sleept(60)
			self.irisInitConnection()
			if self.cursor :
				self.cursor.Close()
			if self.conn :	
				conn.close()

	def irisDisConnection(self) :
		if self.cursor :
			self.cursor.Close()
		if self.conn :
			self.conn.close()

	def makeCtlFile(self, keyList, workId) :
		ctlFilePath = os.path.join(self.LOAD_FILE_PATH, '%s.ctl' % workId  )

		if not os.path.isdir(self.LOAD_FILE_PATH) :
			self.mkdirs(self.LOAD_FILE_PATH)

		result	= list()

		for oneKey in keyList :
			result.append(camelToUnder(oneKey).encode('utf-8'))

		try :
			ctlFile	= open(ctlFilePath, 'w')
			ctlFile.write('|^-^|'.join(result))

			ctlFile.close()

		except :
			__LOG__.Trace('ctlFileCreate Error')


		return ctlFilePath

	def makeDatFile(self, valueList, workId) :
		datFilePath = os.path.join(self.LOAD_FILE_PATH, '%s.dat' % workId)

		if not os.path.exists(self.LOAD_FILE_PATH) :
			self.mkdirs(self.LOAD_FILE_PATH)

		try :
			datFile = open(datFilePath, 'w')
			datFile.write('|^|'.join(valueList))

			datFile.close()

		except :
			__LOG__.Exception()
			__LOG__.Trace('datFileCreate Error')


		return datFilePath		

	def parse(self, mqMsgData) :
		mqMsgDict	= json.loads(mqMsgData)
		
		keyList		= mqMsgDict.keys()
		workId		= mqMsgDict['workId']

		if not 'queueMsg' in keyList :
			return
		valueList	= list()

		for oneKey in keyList :
			oneValue = mqMsgDict[oneKey]
			if oneValue is None : oneValue = ''
			if isinstance(oneValue, dict) :
				tempDict = dict()
				for key, value in oneValue.items() :
					tempDict[key.encode('utf-8')] = value.encode('utf-8')
				oneValue = str(tempDict).encode('utf-8')
			valueList.append(oneValue)

		__LOG__.Trace(valueList)

		ctlFilePath = self.makeCtlFile(keyList, workId)
		datFilePath	= self.makeDatFile(valueList, workId)

		key = mqMsgDict['workId'][-1]
		partition	= mqMsgDict['workStaDate']

		self.dataLoad( ctlFilePath, datFilePath, key, partition )

	def dataLoad(self, ctlFilePath, datFilePath, key, partition) :
		try :
			__LOG__.Trace(' datFile : %s' %datFilePath)
			__LOG__.Trace(' ctlFile : %s' %ctlFilePath)

			logLoadResult = self.cursor.Load( 'TACS.TACS_MQ_LOG', key, partition, ctlFilePath, datFilePath)

			__LOG__.Trace('load result : %s ' % logLoadResult)
		
		except :
			__LOG__.Exception()

	def run(self):
		__LOG__.Trace('QueueLog start!!!')

		while not SHUTDOWN :
			strIn	= ''
			strLine	= ''
			
			try :
				strIn	= sys.stdin.readline()
				strLine = strIn.strip()
				if strLine == '' :
					continue
				
			except :
				if not SHUTDONW :
					__LOG__.Exception()
					self.stdErr(strIn)
					continue

			try :
				prefix 		= ''
				mqMsgData 	= ''

				if '://' in strLine :
					prefix, mqMsgData	= strLine.split('://')

				else :	continue

				self.irisInitConnection()

				self.parse(mqMsgData)

				self.irisDisConnection()

			except :
				__LOG__.Exception()
				
			finally :
				self.stdErr(strIn)


def main():
	module = os.path.basename(sys.argv[0])

	if len(sys.argv) > 2 : 
		section 	= sys.argv[1] # LOG_LOADER
		cfgfile 	= sys.argv[2] # /home/tacs/TACS-EF/LOG-ETL/conf/LOG-ETL.conf
		cfg 		= ConfigParser.ConfigParser()
		cfg.read(cfgfile)

		logPath = cfg.get("GENERAL", "LOG_PATH")
		logFile = os.path.join(logPath, "%s_%s.log" % (module, section))

		logCfgPath = cfg.get("GENERAL", "LOG_CONF")

		logCfg = ConfigParser.ConfigParser()
		logCfg.read(logCfgPath)

		Log.Init(Log.CRotatingLog(logFile, logCfg.get("LOG", "MAX_SIZE"), logCfg.get("LOG", "MAX_CNT") ))

		logLoader 	= LogLoader(cfg, section)
		logLoader.run()

	else : 
		__LOG__.Trace('Argument Length !!!')


if __name__ == '__main__':
	try:
		main()
	except:
		__LOG__.Exception('main error')
