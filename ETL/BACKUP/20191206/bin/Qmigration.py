#!/usr/bin/env python
# -*- coding: utf-8 -*-
import datetime
import os
import sys
import signal
import time
import ConfigParser
import glob
import json
import Mobigen.Common.Log as Log; Log.Init()
from watchdog.observers import Observer
import shutil
import RabbitMQ as MQ
from watchdog.events import FileSystemEventHandler
from watchdog.events import FileCreatedEvent


# Process Shutdown Flag

SHUTDOWN = False

# @param signum 
# @param frame -
def handler(signum, frame):
	global SHUTDOWN
	self.observer.stop()
	self.observer.join()
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
class QMigration(FileSystemEventHandler):

	def __init__(self, cfg, section):
		self.cfg = cfg
		self.PREFIX			= self.cfg.get(section, 'OUT_PREFIX')
		self.PATTERN		= self.cfg.get(section, 'PATTERN')
		self.COMP_PATH		= self.cfg.get(section, 'COMP_PATH')
		self.MONITOR_PATH	= self.cfg.get(section, "DIRECTORY")

		self.MQ_VHOST		= self.cfg.get(section, 'MQ_VHOST')
		self.use_bson		= self.cfg.get(section, 'MQ_USE_BSON').upper() == 'Y'

		self.MQ_HOST            = self.cfg.get(section, 'MQ_HOST')
		self.MQ_SSL_PORT        = int(self.cfg.get(section, 'MQ_PORT'))
		self.MQ_USER            = self.cfg.get(section, 'USER')
		self.MQ_PASS            = self.cfg.get(section, 'PASS')
		self.MQ_CA_CERTS        = self.cfg.get(section, 'MQ_CA_CERTS')
		self.MQ_CERTFILE        = self.cfg.get(section, 'MQ_CERTFILE')
		self.MQ_KEYFILE         = self.cfg.get(section, 'MQ_KEYFILE')


	def mkdirs(self, path) :
		try : os.makedirs(path)
		except OSError as exc: 
			pass

	def stdout(self, msg):
		sys.stdout.write("stdout" + msg + '\n')
		sys.stdout.flush()
		__LOG__.Trace("std OUT: %s" % msg)

	def dispatch( self, event ) :

		if isinstance( event, FileCreatedEvent ) :

			try :
				dirPath, fileName	= os.path.split( event.src_path )
				name, ext			= os.path.splitext( fileName )

				__LOG__.Trace( 'Event : %s' % event )
				
				if ext == self.PATTERN :
					self.parse(event.src_path)
			except :
				__LOG__.Exception()

	def mqInitConnection(self) :
		self.mq = MQ.DirectQueueClient()

		self.mq.connectSSL(self.MQ_USER, self.MQ_PASS, self.MQ_HOST, self.MQ_SSL_PORT, self.MQ_VHOST,self.MQ_CA_CERTS, self.MQ_CERTFILE, self.MQ_KEYFILE)

	def parse(self, filePath) :
		if not os.path.exists(filePath) : return

		__LOG__.Trace('File Exists')

		dataStr = ''

		with open(filePath, 'r') as jsonFile :
			dataStr = jsonFile.read()

		dataDict = json.loads(dataStr)
		self.exportQueue(dataDict, filePath)

	def exportQueue(self, dataDict, filePath) :
		__LOG__.Trace('MQ Export Start !! %s' % dataDict )
		self.mqInitConnection()

		try :
			self.mq.connectChannel()
			self.mq.put(dataDict['QUEUE_NM'], json.dumps(dataDict['QUEUE_MSG'], encoding='utf-8'), use_bson = self.use_bson)

			__LOG__.Trace('Queue Insert Success')

		except :
			__LOG__.Exception()

		finally :
			self.mq.disConnect()


	def run(self, section):
		__LOG__.Trace("start QMigration!!")
		# config 
		monitorPath 	= self.MONITOR_PATH

		if not os.path.exists(monitorPath) :
			os.mkdir(monitorPath)

		self.eventHandler(monitorPath)

		while not SHUTDOWN :
			
			time.sleep(0.1)

	def eventHandler(self, monitorPath) :
		__LOG__.Trace('now Check Path : %s' % monitorPath)

		self.observer   = Observer()
		self.observer.schedule(self, monitorPath, recursive=True)
		self.observer.start()

def main():
	module 	= os.path.basename(sys.argv[0])
	section = sys.argv[1] # TACS_FILE_MONITOR 
	cfgfile = sys.argv[2] # /home/tacs/user/KimJW/ETL/conf/FileMonitor.conf
	cfg = ConfigParser.ConfigParser()
	cfg.read(cfgfile)
	
	logPath = cfg.get("GENERAL", "LOG_PATH")
	logFile = os.path.join(logPath, "%s_%s.log" % (module, section))

	logCfgPath = cfg.get("GENERAL", "LOG_CONF")

	logCfg = ConfigParser.ConfigParser()
	logCfg.read(logCfgPath)

	Log.Init(Log.CRotatingLog(logFile, logCfg.get("LOG", "MAX_SIZE"), logCfg.get("LOG", "MAX_CNT") ))

	fm = QMigration(cfg, section)
	fm.run(section)

	__LOG__.Trace("end main!")

if __name__ == "__main__":
	try:
		main()
	except:
		__LOG__.Exception("Queue Migration main error")
