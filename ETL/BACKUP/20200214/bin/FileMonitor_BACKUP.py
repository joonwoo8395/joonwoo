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
from watchdog.events import FileSystemEventHandler
from watchdog.events import FileCreatedEvent
from watchdog.events import FileModifiedEvent

# Process Shutdown Flag

SHUTDOWN = False

# @param signum 
# @param frame -
def handler(signum, frame):
	global SHUTDOWN
	self.observer.stop()
	self.observer.join()
	SHUTDOWN = True
	__LOG__.info("signal: process shutdown")


# SIGTERM 
signal.signal(signal.SIGTERM, handler)
# SIGINT 
signal.signal(signal.SIGINT, handler)
# SIGHUP 
signal.signal(signal.SIGHUP, handler)
# SIGPIPE 
signal.signal(signal.SIGPIPE, handler)

# JSON 
class FileMonitor(FileSystemEventHandler):

	def __init__(self, cfg, section):
		self.cfg = cfg
		self.PREFIX		= self.cfg.get(section, 'OUT_PREFIX')
		self.PATTERN	= self.cfg.get(section, 'PATTERN')
		self.COMP_PATH	= self.cfg.get(section, 'COMP_PATH')

	def stdout(self, msg):
		sys.stdout.write("stdout" + msg + '\n')
		sys.stdout.flush()
		__LOG__.Trace("std OUT: %s" % msg)

	# ETL shut update DATA insert
	def shutdownContract(self, monitorPath, prefix) :
		__LOG__.Trace(monitorPath)
		dirNameList 	= os.listdir(monitorPath)
		__LOG__.Trace(dirNameList)
		
		for oneDirName in dirNameList :
			dateDir 		= os.path.join(monitorPath,oneDirName)
			dateDirFileList	= os.listdir(dateDir)
		
			for oneDateDirFile in dateDirFileList :
				datePath		= os.path.join(dateDir, oneDateDirFile)
				innerDirList	= os.listdir(datePath)
				jsonFile 		= ''
		
				for oneInnerDir in innerDirList :
					fileName, ext = os.path.splitext(oneInnerDir)
					if ext == self.PATTERN :
						jsonFile = oneInnerDir
						break
				__LOG__.Trace(jsonFile)	
			
				if not jsonFile == '' :
					self.stdout("%s%s" %(prefix, os.path.join( os.path.join(os.path.join(monitorPath, oneDirName), oneDateDirFile), oneInnerDir)))
				else :
					__LOG__.Trace("json File not exist")

	def dispatch(self, event) :
		__LOG__.Trace('---------------------------------- %s' % event)
		if isinstance(event, FileModifiedEvent) or isinstance(event,FileCreatedEvent) :
			dirPath, fileName	= os.path.split(event.src_path)
			name, ext			= os.path.splitext(fileName)

			if ext == '.json' :
				__LOG__.Trace('Event : %s' %event)
				self.stdout('%s%s' % (self.PREFIX, event.src_path))

	def run(self, section):
		__LOG__.Trace("start FileMonitor!!")
		# config 
		monitorPath 	= self.cfg.get(section, "DIRECTORY")

		if not os.path.exists(monitorPath) :
			os.mkdir(monitorPath)

		self.shutdownContract(monitorPath, self.PREFIX)	
		self.eventHandler(monitorPath)

		while not SHUTDOWN :
			
			time.sleep(0.1)

	def eventHandler(self, monitorPath) :
		__LOG__.Trace('now Check Path : %s' % monitorPath)

		self.observer   = Observer()
		self.observer.schedule(self, monitorPath, recursive=True)
		self.observer.start()

	def nowDateCheck(self, oldDate, monitorPath) :
		nowDate 	    = datetime.datetime.now().strftime('%Y%m%d')

		if not nowDate == oldDate :
			__LOG__.Trace("Date Modify %s -> %s " % (oldDate, nowDate))
			__LOG__.Trace('3 Event Handler Stop')	
			self.observer.stop()
			self.observer.join()

			return True

		else :

			return False

def main():
	module 	= os.path.basename(sys.argv[0])
	section = sys.argv[1] # TACS_FILE_MONITOR 
	cfgfile = sys.argv[2] # /home/tacs/user/KimJW/ETL/conf/FileMonitor.conf
	cfg = ConfigParser.ConfigParser()
	cfg.read(cfgfile)
	
	if '-d' not in sys.argv :
		log_path = cfg.get("GENERAL", "LOG_PATH")
		log_file = os.path.join(log_path, "%s_%s.log" % (module, section))
		Log.Init(Log.CRotatingLog(log_file, 1000000, 9))

	else:
		LOG.Init()

	fm = FileMonitor(cfg,section)
	fm.run(section)

	__LOG__.Trace("end main!")

if __name__ == "__main__":
	try:
		main()
	except:
		__LOG__.Exception("FileMonitor main error")
