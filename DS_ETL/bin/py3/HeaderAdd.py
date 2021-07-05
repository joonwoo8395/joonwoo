#!/bin/env python3
# -*- coding: utf-8 -*-

#- needed import ----------------------------------------------------
#import $PYTHON_LIB$
import os
import sys
import time
import datetime
import signal
import subprocess
import glob
import configparser as ConfigParser
import csv

#import $MOBIGEN_LIB$
import Mobigen.Common.Log_PY3 as Log; Log.Init()

#import $PROJECT_LIB$

#- shutdown  ----------------------------------------------------
SHUTDOWN = False

def makedirs(path) :
	try :
		os.makedirs(path)
		__LOG__.Trace("makeDir : {}".format(path))
	except : pass

def shutdown(signalnum, handler):
	global SHUTDOWN
	SHUTDOWN = True
	sys.stderr.write('Catch Signal: %s \n' % signalnum)
	sys.stderr.flush()

signal.signal(signal.SIGTERM, shutdown) # sigNum 15 : Terminate
signal.signal(signal.SIGINT, shutdown)  # sigNum  2 : Keyboard Interrupt
signal.signal(signal.SIGHUP, shutdown)  # sigNum  1 : Hangup detected
signal.signal(signal.SIGPIPE, shutdown) # sigNum 13 : Broken Pipe

'''
	On Windows, signal() can only be called with
	SIGABRT, SIGFPE,SIGILL, SIGINT, SIGSEGV, or SIGTERM.
	A ValueError will be raised in any other case.
'''

#- def global setting ----------------------------------------------------

def stderr(msg) :

	sys.stderr.write(msg + '\n')
	sys.stderr.flush()
	__LOG__.Trace('Std ERR : %s' % msg)

def stdout(msg) :

	sys.stdout.write(msg + '\n')
	sys.stdout.flush()
	__LOG__.Trace('Std OUT : %s' % msg)

def makedirs(path) :

	try :
		os.makedirs(path)
		__LOG__.Trace( path )
	except : pass

#- Class ----------------------------------------------------

class ClassName:

	def __init__(self, conf, section) :
		#open
		__LOG__.Trace("__init__")
		section = "HEADER"

		try : self.resultPath = conf.get(section, "HEADER_ADD_PATH")
		except :
			self.resultPath = conf.get("GENERAL", "HEADER_ADD_PATH")

	def __del__(self):
		#close
		__LOG__.Trace("__del__")

	def csvHeaderAdd(self, ctlFile, csvFile) :
		__LOG__.Trace( "ctlFile : {} \n csvFile : {}".format(ctlFile, csvFile) )
		if not os.path.exists(ctlFile) :
			__LOG__.Trace("ctl File Not Found")
			return
		distributePath	= csvFile.split("/")[-3]

		resultFileName 	= os.path.basename(csvFile)
		resultPath		= self.resultPath.format(distributePath)

		makedirs( resultPath )

		resultFile 		= os.path.join( resultPath, resultFileName )

		headerList		= list()

		with open(resultFile, "w") as resultCsv :
			with open(csvFile, "r")  as dataCsv :
				with open(ctlFile, "r") as ctlCsv :
					headReader 	= csv.reader(ctlCsv)
					fileWriter 	= csv.writer(resultCsv)
					fileReader 	= csv.reader(dataCsv)
					
					for header in headReader :
						if len(header) == 1 :
							strHeader = header[0]
							headerList.append(strHeader)
					__LOG__.Trace(headerList)
					fileWriter.writerow(headerList)

					for data in fileReader :
						fileWriter.writerow(data)

		return resultFile

	def processing(self, in_file) :

		__LOG__.Trace( "processing : %s" % in_file )
		dirName, csvFileName = os.path.split(in_file)
		__LOG__.Trace( "dirName, csvFileName : {} | {}".format(dirName, csvFileName) )

		ctlDir		= dirName.replace("LineMerge", "Standardization")
		ctlFileName	= os.path.splitext(csvFileName)[0] + '.ctl'

		ctlFile	= os.path.join(ctlDir, ctlFileName)

		resultFile = self.csvHeaderAdd(ctlFile, in_file)

		if resultFile :
			stdout( 'csv://' + resultFile )

	def run(self):

		while not SHUTDOWN :

			std_in = None
			is_std_err = False

			try:

				std_in = sys.stdin.readline().strip()

				if not std_in :
					is_std_err = True
					continue

				__LOG__.Trace('STD  IN : %s' % std_in )

				try :
					prefix, in_file = std_in.split('://', 1)
				except :
					is_std_err = True
					__LOG__.Trace( 'Input format error : %s' % std_in )
					continue

				if prefix != 'csv' :
					is_std_err = True
					__LOG__.Trace('Prefix is not match : %s' % prefix)
					continue	
				
				if not os.path.exists( in_file )  :
					is_std_err = True
					__LOG__.Trace('Not found file : %s' % in_file)
					continue

				stime = time.time()

				self.processing(in_file)

				etime = time.time()

				__LOG__.Trace( 'Duration %s sec' % ( etime - stime ) )

				is_std_err = True

			except:
				if not SHUTDOWN : __LOG__.Exception()

			finally :
				if std_in != None and is_std_err :
					stderr( std_in )


		
#- main function ----------------------------------------------------
def main():

	module = os.path.basename(sys.argv[0])

	if len(sys.argv) < 3:
		sys.stderr.write('Usage 	: %s section conf {option:[[log_arg]-d]}\n' % module )
		sys.stderr.write('Usage 	: %s section conf {option:[[log_arg]-d]}\n' % module )
		#python3 /home/test/Project_name/bin/py3/BaseModule.py SECTION /home/test/Project_name/conf/BaseModule.conf
		#python3 /home/test/Project_name/bin/py3/BaseModule.py SECTION /home/test/Project_name/conf/BaseModule.conf 0 
		#python3 /home/test/Project_name/bin/py3/BaseModule.py SECTION /home/test/Project_name/conf/BaseModule.conf -d
		sys.stderr.flush()
		os._exit(1)

	section	 = sys.argv[2]
	config_file = sys.argv[1]

	conf = ConfigParser.ConfigParser()
	conf.read(config_file)

	if '-d' not in sys.argv :

		etc_argv = sys.argv[3:]
		log_arg = ''

		if len(sys.argv[3:]) > 0 :
			log_arg = '_' + sys.argv[3]

		log_path = conf.get('GENERAL', 'LOG_PATH')

		makedirs( log_path )	

		log_file = os.path.join(log_path, '%s_%s%s.log' % (os.path.splitext(module)[0], section, log_arg ))
		Log.Init(Log.CRotatingLog(log_file, 10240000, 9))		

	else:
		Log.Init()		

	pid = os.getpid()	
	__LOG__.Trace('============= %s START [pid:%s]==================' % ( module, pid ))

	ClassName(conf, section).run()

	__LOG__.Trace('============= %s END [pid:%s]====================' % (module, pid ))


#- if name start ----------------------------------------------
if __name__ == "__main__" :
	main()


