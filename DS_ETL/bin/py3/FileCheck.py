#!/bin/env python3
# -*- coding: utf-8 -*-
try : import ConfigParser
except : import configparser as ConfigParser
import glob
import os
import signal
import sys
import time
import shutil
import datetime
import Mobigen.Common.Log_PY3 as Log;

Log.Init()

SHUTDOWN = False

def handler(signum, frame):
	global SHUTDOWN
	SHUTDOWN = True
	__LOG__.Trace("Catch Signal = %s" % signum)

signal.signal(signal.SIGTERM, handler)
signal.signal(signal.SIGINT, handler)
signal.signal(signal.SIGHUP, handler)
signal.signal(signal.SIGPIPE, handler)

def mkdirs(path) :
	try : os.makedirs(path)
	except OSError as exc: #Python > 2.5
	#if exc.errno == errno.EEXIST and os.path.isdir(path) : 
		pass
	#else : raise
								

class FileCheck :
	def __init__(self, section, cfg) :
		self.cfg 		= cfg
		self.section 	= section
		self.set_config(self.section, cfg)
	
	def set_config(self, section, cfg) :
		if cfg.has_option(section, "COMP_DIR") :
			self.COMP_DIR = cfg.get(section, "COMP_DIR")

		if cfg.has_option(section, "CHECK_INTERVAL") :
			self.CHECK_INTERVAL = cfg.getint(section, "CHECK_INTERVAL")

		if cfg.has_option(section, "FILE_EXT") :
			self.EXT	= cfg.get(section, "FILE_EXT")

	def fileModifiedCheck(self, filePath, lastMTime=0) :
		# 2020-10-13 추가 (김준우) 파일 mtime 체크
		time.sleep(self.CHECK_INTERVAL)

		if not os.path.exists(filePath) :
			raise Exception("File Not Exists : {} | isStdErr".format(filePath))

		if None == filePath or '' == filePath :
			raise Exception("invalid FilePath : {} | isStdErr".format(filePath))

		if os.path.splitext(filePath)[1] != self.EXT :
			raise Exception("invalid File Ext : {} | isStdErr".format(filePath))

		try :
			fileMTime = os.path.getmtime(filePath)
		except :
			__LOG__.Trace("Modify Checking File Not Found {} isStdErr".format(filePath))
			return None

		returnCompPath = None

		if fileMTime > lastMTime :
			__LOG__.Trace("Updating file : {}.....\n Last Mtime : {}, Now Mtime : {}".format(filePath, lastMTime, fileMTime))
			returnCompPath = self.fileModifiedCheck(filePath, fileMTime)

		elif fileMTime == lastMTime :
			__LOG__.Trace("Transform complete file : {}.....\n Last Mtime : {}, Now Mtime : {}".format(filePath, lastMTime, fileMTime))
			if not os.path.exists(self.COMP_DIR) :
				mkdirs(self.COMP_DIR)

			compPath = os.path.join( self.COMP_DIR, os.path.basename(filePath) )

			if os.path.exists(compPath) :
				tmpCompPath = os.path.join( self.COMP_DIR, os.path.basename(filePath)+ '.' + datetime.datetime.fromtimestamp(mtime).strftime('%Y%m%d%H%M%S') )
				try :
					shutil.move( compPath, tmpCompPath )
				except :
					__LOG__.Trace("Dulpication FileName ,File Move Failed")
					return None
			try :
				shutil.move( filePath, compPath )
				__LOG__.Trace( "Move From : {}  To : {}".format(filePath, compPath) )
				returnCompPath = compPath

			except :
				__LOG__.Trace("File Move Failed")

			if None != returnCompPath or "" != returnCompPath :
				sys.stdout.write( 'file://%s\n' % returnCompPath )
				sys.stdout.flush()
				__LOG__.Trace( 'STD OUT : file://%s' % returnCompPath )

		else :
			__LOG__.Trace("invalid file : {}.....\n Time Last Mtime : {}, Now Mtime : {}".format(filePath, lastMTime, fileMTime))
			returnCompPath = ''

		return returnCompPath
	
	def run(self) :
		__LOG__.Trace( "FileCheck START process: ( pid:{} ) >>>>>>>>>>>>>>>>>>>>>>>>>>>>>".format(os.getpid()) )

		while not SHUTDOWN :

			stdIn		= None
			isStdErr	= False

			try :
				stdIn		= sys.stdin.readline()
				__LOG__.Trace("STD IN : {}".format(stdIn))

				stdInTrim 	= stdIn.strip()

				if stdIn.strip() == "" :
					raise Exception( "input err STD IN  : {}".format(stdIn) )
					isStdErr	= True

				prefix		= None
				inFilePath	= None

				try :
					prefix, inFilePath	= stdInTrim.split("://", 1)
				except :
					raise Exception( "input format err" )
					isStdErr	= True	

#				self.processing(inFilePath)
				self.fileModifiedCheck(inFilePath)
				isStdErr	= True

			except Exception as ex :
				__LOG__.Exception()
				if "isStdErr" in ex :
					isStdErr = True

			finally :
				if stdIn != None and isStdErr :
					sys.stderr.write( "{}\n".format(stdIn) )
					sys.stderr.flush()
					__LOG__.Trace("STD ERR : {}".format(stdIn))

module	= os.path.basename(sys.argv[0])

def main() :

	if len(sys.argv) < 4:
		print( "Usage : {} Section ConfigFile".format( module ), sys.stderr )
		print( "Exam  : {} LTE_CDR /home/eva/E2ES/conf/FilePatternMonitor.conf".format( module ), sys.stderr)
		sys.exit()
	
	section = sys.argv[1]
	cfgfile	= sys.argv[2]
	logSect	= sys.argv[3]

	cfg = ConfigParser.ConfigParser()
	cfg.read(cfgfile)

	if '-d' not in sys.argv :
		log_path	= cfg.get("GENERAL", "LOG_PATH")
		log_file	= os.path.join(log_path, "{}_{}.log".format(module, logSect) )
		Log.Init(Log.CRotatingLog(log_file, 10240000, 9))
	

	FileCheck(section, cfg).run()
	
if __name__ == '__main__' :
	try :
		main()
	except :
		__LOG__.Exception('[ERROR In main]')
