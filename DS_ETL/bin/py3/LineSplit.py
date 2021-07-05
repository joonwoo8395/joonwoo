#!/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import time
import datetime
import signal
import subprocess
import glob
import Mobigen.Common.Log_PY3 as Log
import math
import shutil
#import Mobigen.Utils.LogClient as c_log

def handler(sigNum, frame):
	sys.stderr.write('Catch Signal Number : %s \n' % sigNum)
	sys.stderr.flush()
	os.kill(os.getpid(), signal.SIGKILL)

# sigNum 15 : Terminate
signal.signal(signal.SIGTERM, handler)
# sigNum  2 : Keyboard Interrupt
signal.signal(signal.SIGINT, handler)
# sigNum  1 : Hangup detected
try:
	signal.signal(signal.SIGHUP, signal.SIG_IGN)
except: pass
# sigNum 13 : Broken Pipe
try:
	signal.signal(signal.SIGPIPE, signal.SIG_IGN)
except: pass


class LineSplit:

	def __init__(self, split_lines) :

		self.split_lines = int(split_lines)
		#self.run_command = '{SPLIT_COMMAND} --suffix-length={SUFFIX_LENGTH} --lines={SPLIT_LINES} --numeric-suffixes {INPUT_FILE} {OUTPUT_FILE}'
		#self.run_command = '{SPLIT_COMMAND} --lines={SPLIT_LINES} --numeric-suffixes {INPUT_FILE} {OUTPUT_FILE}'
		#self.run_command = '{SPLIT_COMMAND} --lines={SPLIT_LINES} --numeric-suffixes --additional-suffix=_{SPLIT_CNT} {INPUT_FILE} {OUTPUT_FILE}'
		self.run_command = '{SPLIT_COMMAND} --lines={SPLIT_LINES} --numeric-suffixes --additional-suffix=_{SPLIT_CNT}_{TOTAL_ROW_CNT} {INPUT_FILE} {OUTPUT_FILE}'
		self.split_command = '/usr/bin/split'

	def __del__(self):
		pass


	def stderr(self, msg) :

		sys.stderr.write(msg + '\n')
		sys.stderr.flush()
		__LOG__.Trace('Std ERR : %s' % msg)

	def stdout(self, msg) :

		sys.stdout.write(msg + '\n')
		sys.stdout.flush()
		__LOG__.Trace('Std OUT : %s' % msg)

	def makedirs(self, path) :

		try :
			os.makedirs(path)
			__LOG__.Trace( path )
		except : pass

	def getSaveFile(self, in_file) :

		dir_name, base_name = os.path.split(in_file)

		#하위 디렉토리로 저장
		save_path = os.path.join(dir_name, 'LineSplit')
		save_name = '%s_' % base_name
		
		save_file = os.path.join( save_path, save_name )

		self.makedirs( save_path )

		return save_file

	def setDatFile(self, prefix, file_pattern) :

		file_list = []
		file_list = glob.glob( file_pattern + '*')
		#file_list.sort(reverse=True)
		file_cnt = len(file_list)

		for file_name in file_list :
			
			#re_name = file_name + '_%s' % file_cnt
			#os.rename( file_name, re_name )

			stdout_str = '://'.join([ prefix, file_name ])
			self.stdout(stdout_str)
#			self.logSend(stdout_str)

		return file_cnt

	def processing(self, prefix, in_file) :

		__LOG__.Trace( ' === processing ===' )
		record_count = subprocess.getstatusoutput('/usr/bin/wc -l %s' % in_file)[-1].split()[0]
		self.in_file_row_cnt = record_count
		self.in_file_size = float(os.path.getsize(in_file))
		file_size = self.in_file_size / 1024 / 1024 #MB
		__LOG__.Watch( record_count )
		__LOG__.Watch( file_size )

		save_file = self.getSaveFile( in_file )

		if file_size < 100.0 and int(record_count) <= self.split_lines : 

			new_name = save_file + '00_01_%s' % record_count
			shutil.copy(in_file, new_name)
			stdout_str = '://'.join([ prefix, new_name ])
			self.stdout(stdout_str)
#			self.logSend(stdout_str)
		
		else:
			split_cnt = math.ceil(float(record_count)/float(self.split_lines))

			if split_cnt > 99 :
				split_lines = math.ceil(float(record_count)/float(99))
				split_cnt = math.ceil(float(record_count)/float(split_lines))

			else:
				split_lines = self.split_lines

			run_cmd = self.run_command.format(
							  SPLIT_COMMAND = self.split_command
							, SPLIT_LINES   = split_lines
							, INPUT_FILE	= in_file
							, OUTPUT_FILE   = save_file
							, SPLIT_CNT 	= split_cnt
							, TOTAL_ROW_CNT = record_count 
							)

			__LOG__.Trace( run_cmd )
			a = subprocess.getstatusoutput(run_cmd)

			#os.rename(in_file, in_file + '.Original')

			dat_count = self.setDatFile( prefix, save_file )

			if dat_count != split_cnt:
				__LOG__.Trace('split_cnt and glob_cnt are not matched!!!!!!!!!!!!!!!!!!')
			else:
				__LOG__.Trace('Split Completed : %s cnt' % dat_count)


	def run(self):

		while True:
#			self.start_time         = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
			std_in = None
			is_std_err = False

			try:
				std_in = sys.stdin.readline().strip()
				
				if not std_in :
					is_std_err = True
					continue

				__LOG__.Trace('STD  IN : %s' % std_in)

				try:
					prefix, in_file = std_in.split( '://', 1 )
				except:
					is_std_err = True
					__LOG__.Trace( 'Input format error : %s' % std_in )
					continue

				if not os.path.isfile( in_file ) :
					is_std_err = True
					__LOG__.Trace( 'Not found input file : %s' % in_file )
					continue

				stime = time.time()
#				self.std_in             = in_file
				self.processing(prefix, in_file)

				__LOG__.Trace("in_file remove : %s" % in_file)
				os.remove(in_file)
				etime = time.time()

				__LOG__.Trace( 'Duration %s sec' % ( etime - stime ) )

				is_std_err = True

			except:
				__LOG__.Exception()

			finally :
				if std_in != None and is_std_err :
					self.stderr( std_in )


		
#- main function ----------------------------------------------------
def main():

	module = os.path.basename(sys.argv[0])

	if len(sys.argv) < 2:
		sys.stderr.write('Usage 	: %s {split_lines} {option:[[log_arg]-d]}\n' % module )
		sys.stderr.write('Example 	: %s 100000 0\n' % ( module, os.path.splitext(module)[0] ) )
		sys.stderr.write('Example 	: %s 100000 -d\n' % ( module, os.path.splitext(module)[0] ) )
		sys.stderr.flush()
		os._exit(1)

	split_lines	 = sys.argv[1]

	if '-d' not in sys.argv :

		etc_argv = sys.argv[2:]
		log_arg = ''

		if len(sys.argv[2:]) > 0 :
			log_arg = '_' + sys.argv[2]

		log_path = os.path.expanduser('~/DS_ETL/log')
		
		try : os.makedirs( log_path )
		except : pass

		log_file = os.path.join(log_path, '%s%s.log' % (os.path.splitext(module)[0], log_arg ))
		Log.Init(Log.CRotatingLog(log_file, 10240000, 9))		

	else:
		Log.Init()		
	
	__LOG__.Trace('============= %s START [pid:%s]==================' % (module, os.getpid()))

	LineSplit(split_lines).run()

	__LOG__.Trace('============= %s END ====================' % module)


#- if name start ----------------------------------------------
if __name__ == "__main__" :
	main()


