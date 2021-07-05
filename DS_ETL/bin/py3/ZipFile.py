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
import json
import configparser as ConfigParser
import zipfile
import shutil

#import $MOBIGEN_LIB$
import Mobigen.Common.Log_PY3 as Log; Log.Init()
#import Mobigen.Utils.LogClient as c_log
#import $PROJECT_LIB$
from apscheduler.schedulers.background import BackgroundScheduler

#- shutdown  ----------------------------------------------------
SHUTDOWN = False

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

#PARAMETER_RESOLVER_FORMAT = "/home/test/DATA/{CenterCode}/{dataDir}/KCB_{datasetCode}_{extra}.CSV"

#- Class ----------------------------------------------------

class ClassName:

	def __init__(self, conf, section) :
		#open
		__LOG__.Trace("__init__")
		section = 'ZIP'

		centerId    = conf.get('GENERAL', 'CENTER_ID')
		centerName  = conf.get('GENERAL', 'CENTER_NAME')
		try :
			self.REMOVE_FLAG	= conf.get('GENERAL', 'COMP_REMOVE_FLAG')
		except :
			self.REMOVE_FLAG	= False

#		self.logInit(centerName, centerId)
		
	def __del__(self):
		#close
		__LOG__.Trace("__del__")

#	def logInit(self, centerName, centerId) :
#		self.center_name        	= centerName
#		self.center_id          	= centerId
#		self.process_name       	= os.path.basename(sys.argv[0])
#		self.process_type       	= '일반모듈'
#		self.start_time         	= ''
#		self.end_time           	= ''
#		self.std_in             	= ''
#		self.std_out            	= ''
#		self.in_file_size       	= ''
#		self.in_file_row_cnt    	= ''
#		self.out_file_size      	= ''
#		self.out_file_row_cnt   	= ''
#		self.table_info         	= ''
#		self.key_info           	= ''
#		self.partition_info     	= ''
#		self.result_flag        	= ''
#		self.success_cnt        	= ''
#		self.fail_reason        	= ''
#		self.header_cnt             = ''
#		self.comp_row_cnt           = ''
#		self.error_column_length    = ''
#		self.error_check_notnull    = ''
#		self.error_check_type_legth = ''
#		self.error_check_format     = ''
#		self.error_change_cont      = ''
	
#	def logSend(self, std_out) :
#		if '://' in std_out :
#			std_out = std_out.split('://')[1]
#		self.std_out            = std_out

#		if not os.path.exists(std_out) :
#			self.out_file_size		= ''
#			self.out_file_row_cnt	= ''

#		else :
#			self.out_file_size      = str(os.path.getsize(std_out))
#			if std_out.upper().endswith('.CSV') or std_out.upper().endswith('.DAT') :
#				#self.out_file_row_cnt   = subprocess.check_output(["wc","-l", std_out]).split()[0]
#				self.out_file_row_cnt   = subprocess.getstatusoutput('/usr/bin/wc -l %s' % std_out)[-1].split()[0]

#		self.end_time           = datetime.datetime.now().strftime('%Y%m%d') + '000000'
#		sendLogData = '|^|'.join(map(str, [self.center_name, self.center_id, self.process_name, self.process_type, self.start_time, self.end_time, self.std_in, self.std_out, self.in_file_size, self.out_file_size, self.in_file_row_cnt, self.out_file_row_cnt, self.table_info, self.key_info, self.partition_info, self.result_flag, self.success_cnt, self.fail_reason, self.header_cnt, self.comp_row_cnt, self.error_column_length, self.error_check_notnull, self.error_check_type_legth, self.error_check_format, self.error_change_cont]))
#		c_log.irisLogClient().log("SendLog://{}\n".format(sendLogData))
#		__LOG__.Trace('send Log Data : {}'.format(sendLogData))

	def readFile(self, filePath) :
		if filePath == None : return

		dataList = None

		if os.path.exists(filePath) :
			with open(filePath, "r") as refFile :
				dataList = refFile.readlines()

			__LOG__.Trace("csv File Name : {}".format(dataList))

		else :
			__LOG__.Trace("REF File is not Find")

		return dataList

	def compressFile(self, csvList, in_file) :

		if csvList == None :
			return

		dirName	= os.path.dirname(in_file)
		trCode	= os.path.basename(in_file).split("-")[0]
		zipName	= "{}.zip".format(trCode)

		os.chdir(dirName)
		__LOG__.Trace("Now Dir Change {}".format(dirName))

		zipFile = zipfile.ZipFile( zipName, "w")
		__LOG__.Trace("Zip File : {}".format(os.path.join(dirName, zipName)))

		for tempCsvFile in csvList :
			csvFile 		= os.path.basename(tempCsvFile.strip('\n'))
			fileName, ext 	= os.path.splitext(os.path.basename(csvFile))
			if ".csv" == ext.lower() :
				zipFile.write(csvFile, compress_type=zipfile.ZIP_DEFLATED)
				__LOG__.Trace("Compress File : {}".format(tempCsvFile))

			else :
				__LOG__.Trace("Not csv File : {}".format(csvFile))

		zipFile.close()

		makedirs( os.path.join(os.pardir, "Result") )

		resultZipPath	= os.path.abspath(os.path.join( os.pardir, "Result", zipName ))

		shutil.move(os.path.join(dirName, zipName), resultZipPath )

		__LOG__.Trace("Zip File Move {} >>>> {}".format(os.path.join(dirName, zipName), resultZipPath ))

		if self.REMOVE_FLAG :
			for oneCsvFile in csvList :
				if os.path.exists(oneCsvFile) :
					os.remove(oneCsvFile)
					__LOG__.Trace('File Remove : %s' % oneCsvFile)

		stdout( 'zip://{}'.format(resultZipPath) )
#		self.logSend(resultZipPath)

	def processing(self, prefix, in_file) :
		# processing 실행
		__LOG__.Trace( "processing : %s" % in_file )

		# unzip 파일 path에 대한 정보를 가지고 있는 .ref 파일 처리
		csvList = self.readFile(in_file)
		# compress
		self.compressFile(csvList, in_file)

	def run(self):

		while not SHUTDOWN :
#			self.start_time         = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
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

				if prefix != 'ref' :
					is_std_err = True
					__LOG__.Trace('Prefix is not match : %s' % prefix)
					continue	
				
				if not os.path.exists( in_file )  :
					is_std_err = True
					__LOG__.Trace('Not found file : %s' % in_file)
					continue

				stime = time.time()
#				self.std_in             = in_file
#				self.in_file_size       = str(os.path.getsize(in_file))
#				if in_file.upper().endswith('.CSV') or in_file.upper().endswith('.DAT') :
#					#self.in_file_row_cnt	= subprocess.check_output(["wc","-l", in_file]).split()[0]
#					self.in_file_row_cnt   = subprocess.getstatusoutput('/usr/bin/wc -l %s' % in_file)[-1].split()[0]

				self.processing(prefix, in_file)

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
		sys.stderr.write('Usage 	: %s conf {option:[[log_arg]-d]}\n' % module )
		sys.stderr.write('Usage 	: %s conf {option:[[log_arg]-d]}\n' % module )
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


