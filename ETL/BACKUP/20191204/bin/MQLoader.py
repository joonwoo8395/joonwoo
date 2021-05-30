#!/usr/bin/env python
# -*- coding: utf-8 -*-

import datetime
import json
import requests
import sys
import os
import signal
import ConfigParser
import RabbitMQ as MQ
import Mobigen.Common.Log as Log; Log.Init()
from requests.auth import HTTPBasicAuth

SHUTDOWN = False

## Signal
#
# @param signum 
# @param frame Stack Frame
def handler(signum, frame):
	global SHUTDOWN
	SHUTDOWN = True
	__LOG__.Trace('Catch Signal = %s' % signum)

## SIGTERM 
signal.signal(signal.SIGTERM, handler)
## SIGINT 
signal.signal(signal.SIGINT, handler)
## SIGHUP 
signal.signal(signal.SIGHUP , handler)
## SIGPIPE 
signal.signal(signal.SIGPIPE, handler)

class MQLoader:
	def __init__(self, section, cfg):
		self.section 	= section
		self.cfg 		= cfg
		self.MQ_VHOST 	= self.cfg.get(section, 'MQ_VHOST')
		self.use_bson 	= self.cfg.get(section, 'MQ_USE_BSON').upper() == 'Y'
		self.MQ_HOST 			= self.cfg.get(section, 'MQ_HOST')
		self.MQ_SSL_PORT 		= int(self.cfg.get(section, 'MQ_SSL_PORT'))
		self.MQ_USER 			= self.cfg.get(section, 'USER')
		self.MQ_PASS 			= self.cfg.get(section, 'PASS')
		self.MQ_CA_CERTS 		= self.cfg.get(section, 'MQ_CA_CERTS')
		self.MQ_CERTFILE 		= self.cfg.get(section, 'MQ_CERTFILE')
		self.MQ_KEYFILE 		= self.cfg.get(section, 'MQ_KEYFILE')

		self.ELG				= self.cfg.get(section, 'ELG')

		self.STATUS				= self.cfg.get(section, 'STATUS')
		self.NOT_MQ_EMS			= self.cfg.get(section, 'NOT_MQ_EMS')

		self.MQ_LOG_PATH		= self.cfg.get('GENERAL', 'MQ_LOG_PATH')
		self.repo				= dict()

	def writeStdOut(self, msg):
		sys.stdout.write(msg+'\n')
		sys.stdout.flush()
		__LOG__.Trace("STD OUT: %s" % msg)

	def writeStdErr(self, msg):
		sys.stderr.write('%s\n' % (msg))
		sys.stderr.flush()
		__LOG__.Trace('STD ERR: %s' % (msg))

	## MQ Connection
	def mqInitConnection(self):
		self.mq = MQ.DirectQueueClient()
		self.mq.connectSSL(self.MQ_USER, self.MQ_PASS, self.MQ_HOST, self.MQ_SSL_PORT, self.MQ_VHOST, self.MQ_CA_CERTS, self.MQ_CERTFILE, self.MQ_KEYFILE)

	def jsonLoad( self, initFilePath ) :
		jsonDict    = None
		try :

			jsonFile    = open(initFilePath, 'r')
			jsonFileStr = jsonFile.read()
			jsonFile.close()
			self.repo       = json.loads(jsonFileStr)
			
			return self.repo

		except :
			 __LOG__.Exception

	def exportStdOut( self, dataMap, oneExportInfo) :
		dataMap['emsIp']			= oneExportInfo['expEmsIp']
		dataMap['idx']				= oneExportInfo['idx']
		dataMap['workEndDate']		= self.repo['workInfo']['workEndDate']
		dataMap['workProgStatCd']	= self.repo['workInfo']['workProgStatCd']
		dataMap['emsNm']			= oneExportInfo['expEmsNm']
		dataMap['oprrId']			= oneExportInfo['oprrId']

		__LOG__.Trace( 'RAN_EMS exportStdOut!! %s ' % dataMap )

		self.writeStdOut(json.dumps(dataMap))

	def queueDatCtlFileCreate( self, dataMap, lkngUnitDict ) :

		__LOG__.Trace( 'queue File Create' )
		queueDict	= dict()
		
		queueDict['idx']			= dataMap['idx']
		queueDict['expEmsIp']		= dataMap['expEmsIp']
		queueDict['workId']			= dataMap['workId']
		queueDict['workStaDate']	= dataMap['workStaDate']
		queueDict['workStatus']		= dataMap['workTypCd']
		queueDict['queueNm']		= lkngUnitDict['mqNm']
		queueDict['queueMsg']		= dataMap
		queueDict['evntDate']		= datetime.datetime.now().strftime('%Y%m%d%H%M%S')

		dateName					= datetime.datetime.now().strftime('%Y%m%d_%H%M')

		self.writeStdOut('%s%s' %('file://', json.dumps( queueDict, encoding='utf-8') ))

	def exportQueue( self, dataMap, oneExportInfo ) :
		if 'idx' in dataMap :
			del dataMap['idx']
		if 'expEmsIp' in dataMap :
			del dataMap['expEmsIp']
		__LOG__.Trace( 'MQ Export Start!!')
		vendor			= oneExportInfo['vendor'].encode('utf-8')
		expEmsIp		= oneExportInfo['expEmsIp']
		lkngUnitDict	= oneExportInfo['lkngUnitDict']
		idx				= oneExportInfo['idx']

		if lkngUnitDict['unitDistYn']   == 'Y'  :
			__LOG__.Trace('MQ insert Start!!')
			__LOG__.Trace('Queue Name = %s ' % lkngUnitDict['mqNm'] )

			self.mqInitConnection()

			__LOG__.Trace('EMS Ip = %s Export Y !!' % expEmsIp)

			try :
				self.mq.connectChannel()
				self.mq.put( lkngUnitDict['mqNm'], dataMap, use_bson = self.use_bson)
		
				__LOG__.Trace('Queue insert Success')
				try :
					dataMap['idx']		= idx
					dataMap['expEmsIp']	= expEmsIp
					self.queueDatCtlFileCreate( dataMap, lkngUnitDict )
				except :
					__LOG__.Exception()	
				
			except :
				__LOG__.Exception()

			finally :
				self.mq.disConnect()

		else :
			__LOG__.Trace('Queue insert Fail!! EMS Ip = %s , unitDistYn = %s ' % (expEmsIp, lkngUnitDict['unitDistYn'] ) )


	def exportSeparation( self ) :
	
		if not self.repo == None	:
	
			dataMap		= dict()
	
			dataMap['workId']     		= self.repo['workInfo']['workId'].encode('utf-8')
			dataMap['workStaDate']		= self.repo['workInfo']['workStaDate'].encode('utf-8')
			dataMap['workTypCd']		= self.STATUS

			expEmsIpList				= list()
	
			for oneEqpInfo in self.repo['eqpInfo'] :
				cmdWorkTypCd 	= oneEqpInfo['cmdWorkTypCd']
				if oneEqpInfo['eqpTyp'] == self.NOT_MQ_EMS and oneEqpInfo['vendor'] == self.ELG :
					self.exportStdOut(dataMap, oneEqpInfo)

				else :
					continue
	
			for oneExportInfo in self.repo['exportInfo'] :
				cmdWorkTypCd	= oneExportInfo['cmdWorkTypCd']
				__LOG__.Trace( 'cmdWorkTypCd : %s ' % cmdWorkTypCd )

				if oneExportInfo['eqpTyp'] == self.NOT_MQ_EMS and oneExportInfo['vendor'] == self.ELG :
					continue

				else :
					self.exportQueue( dataMap, oneExportInfo )

	## data To MQ
	def run(self):
		__LOG__.Trace('MQLoader start!!')
		while not SHUTDOWN:
			strLine 	= None
			try:
				strIn	= sys.stdin.readline()
				strLine	= strIn.strip()
				if strLine == '' :
					continue
				prefix, initFilePath = strLine.split('://')

				if not os.path.exists(initFilePath):
					__LOG__.Exception('[ERROR] File not found!')
					self.writeStdErr(strIn)
					continue
				try :	
					self.repo = self.jsonLoad( initFilePath )	
	
					self.exportSeparation()

				except :
					__LOG__.Exception()

			except:
				if not SHUTDOWN :
					__LOG__.exception('[ERROR]')
			finally:
				self.writeStdErr(strIn)
	
def main():
	module	= os.path.basename(sys.argv[0])
	section = sys.argv[1] # TACS_MQ_LOADER
	cfgFile = sys.argv[2] 
	cfg = ConfigParser.ConfigParser()
	cfg.read(cfgFile)

	logPath = cfg.get("GENERAL", "LOG_PATH")
	logFile = os.path.join(logPath, "%s_%s.log" % (module, section))
	logCfgPath = cfg.get("GENERAL", "LOG_CONF")

	logCfg = ConfigParser.ConfigParser()
	logCfg.read(logCfgPath)

	Log.Init(Log.CRotatingLog(logFile, logCfg.get("LOG", "MAX_SIZE"), logCfg.get("LOG", "MAX_CNT") ))
	ml = MQLoader(section, cfg)
	ml.run()

if __name__ == '__main__':
	try:
		main()
		
	except:
		__LOG__.Exception('[ERROR] In main')
