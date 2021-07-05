#!/bin/env python
#coding:utf8

import ConfigParser
import time
import sys
import os
import signal

import commands
import glob

import Mobigen.Common.Log as Log; Log.Init()

SHUTDOWN = False 

def handler(sigNum, frame):
	
	global SHUTDOWN
	SHUTDOWN = True
	__LOG__.Trace("Signal Input : %s" % sigNum)

signal.signal(signal.SIGTERM, handler)
signal.signal(signal.SIGINT, handler)
try:
	signal.signal(signal.SIGHUP, signal.SIG_IGN)
except:	pass
try:
	signal.signal(signal.SIGPIPE, signal.SIG_IGN)
except:	pass

class Config :

	def __init__(self, section, config_file, log_arg) :

		self.section = section
		self.config_file = config_file
		self.log_arg = log_arg

		self.config = ConfigParser.ConfigParser()
		self.config.read( self.config_file )

		self.setLoggingInfo()
		self.setSplitInfo()
		self.setSaveInfo()

	def makedirs(self, path) :

		try :
			os.makedirs( path )
			__LOG__.Trace( path )
		except : pass

	def setLoggingInfo(self) :

		self.log_path = os.path.expanduser(self.config.get('GENERAL', 'LOG_PATH'))
		self.makedirs(self.log_path)

		try :
			self.log_size = self.config.getint('GENERAL', 'LOG_SIZE')
		except : 
			self.log_size = 10*1024*1024

		try :
			self.log_count = self.config.getint('GENERAL', 'LOG_COUNT')
		except :
			self.log_count = 10

	def setSplitInfo(self) :

		self.run_command = '{SPLIT_COMMAND} --suffix-length={SUFFIX_LENGTH} --lines={SPLIT_LINES} --numeric-suffixes {INPUT_FILE} {OUTPUT_FILE}'

		try :
			self.split_command = self.config.get(self.section, 'SPLIT_COMMAND')
		except :
			self.split_command = '/usr/bin/split'

		try :
			self.split_lines = self.config.getint(self.section, 'SPLIT_LINES')
		except :
			self.split_lines = 10000

		try :
			self.max_file_count = self.config.getint(self.section, 'MAX_FILE_COUNT')
		except :
			self.max_file_count = None

	def setSaveInfo(self) :

		self.save_path = os.path.expanduser(self.config.get(self.section, 'SAVE_PATH'))
		self.makedirs(self.save_path)

		try :
			self.save_file_list_mode = self.config.getboolean(self.section, 'SAVE_FILE_LIST_MODE')
		except :
			self.save_file_list_mode = False

class LineSpliter :

	def __init__(self, section, config_file, log_arg=0, debug_mode=False) :

		self.class_name = self.__class__.__name__

		self.config = Config( section, config_file, log_arg )

		if not debug_mode : self.setLogging()

	def setLogging(self) :

		log_name = '%s_%s_%s.log' % ( PROC_NAME, self.config.section, self.config.log_arg )
		log_file = os.path.join( self.config.log_path, log_name )

		Log.Init(Log.CRotatingLog( log_file, self.config.log_size, self.config.log_count ))

		__LOG__.Trace( log_file )

	def getSaveFile(self, in_file) :

		base_name = os.path.basename(in_file)
		name, ext = os.path.splitext(base_name)

		save_name = '%s_' % name
		save_file = os.path.join( self.config.save_path, save_name )

		self.config.makedirs( self.config.save_path )

		return save_file

	def setDatFile(self, file_pattern) :

		file_list = glob.glob( file_pattern + '*')
		file_list.sort()

		for index, file_name in enumerate(file_list,1) :
			if self.config.max_file_count :
				if index > self.config.max_file_count :
					__LOG__.Trace( 'Pass, Max file : %s/%s' % ( index, self.config.max_file_count ) )
					continue

			re_name = file_name

			if not file_name.endswith('.dat') :
				re_name = file_name + '.dat'
				os.rename( file_name, re_name )
			
			sys.stdout.write( 'file://%s\n' % re_name )
			sys.stdout.flush()

			__LOG__.Trace( 'STD OUT : %s' % re_name )

		return len(file_list)

	def setDatFileList(self, file_pattern ) :

		file_list = glob.glob( file_pattern + '*' )
		file_list.sort()

		save_file = file_pattern + 'list.dat'
		with open(save_file, 'w') as fd :

			for index, file_name in enumerate(file_list,1) :
				if self.config.max_file_count :
					if index > self.config.max_file_count :
						__LOG__.Trace( 'Pass, Max file : %s/%s' % ( index, self.config.max_file_count ) )
						continue

				re_name = file_name

				if not file_name.endswith('.dat') :
					re_name = file_name + '.dat'
					os.rename( file_name, re_name )
				
				fd.write('%s\n' % re_name)

		sys.stdout.write( 'file://%s\n' % save_file )
		sys.stdout.flush()

		__LOG__.Trace( 'STD OUT : %s' % save_file )

		return len(file_list)
	
	def processing(self, in_file) :

		start_time = time.time()

		record_count = commands.getstatusoutput('/usr/bin/wc -l %s' % in_file)[-1].split()[0]
		__LOG__.Watch( record_count )

		suffix_length = len(str(int(record_count)/self.config.split_lines))

		save_file = self.getSaveFile( in_file )

		# /usr/bin/split --lines=100000 --numeric-suffixes /data01/TANGOA/E2E_LOG/20170224/E2E-CALL_R1_20170224_1842.dat /data02/TANGOA/TEST/E2E-CALL_R1_20170224_1842_
		run_cmd = self.config.run_command.format(
			  SPLIT_COMMAND	= self.config.split_command
			, SPLIT_LINES	= self.config.split_lines
			, INPUT_FILE	= in_file
			, OUTPUT_FILE	= save_file
			, SUFFIX_LENGTH = suffix_length
			)

		__LOG__.Trace( run_cmd )
		commands.getstatusoutput(run_cmd)

		dat_count = 0
		if self.config.save_file_list_mode :
			dat_count = self.setDatFileList( save_file )
		else :
			dat_count = self.setDatFile( save_file )

		end_time = time.time()
		duration = end_time - start_time

		__LOG__.Trace('Split count %s, duration %s sec' % ( dat_count, duration ) )

	def run(self) :
		
		__LOG__.Trace('START')

		while not SHUTDOWN :
			std_in = None
			is_std_err = False

			try :
				std_in = sys.stdin.readline()

				if not std_in.strip() :
					is_std_err = True
					continue
			
				try :
					prefix, in_file = std_in.strip().split('://', 1)
				except :
					is_std_err = True
					__LOG__.Trace( 'Input format error : %s' % std_in )
					continue


				if prefix != 'file' :
					is_std_err = True
					__LOG__.Trace('Prefix is not match : %s' % prefix)
					continue

				
				if not (os.path.exists(in_file) or os.path.exists(in_file+'.dat')) :
					is_std_err = True
					__LOG__.Trace('Not found file : %s' % in_file)
					continue

				self.processing(in_file)
				is_std_err = True

			except :
				if not SHUTDOWN : __LOG__.Exception()
				
			finally :
				if is_std_err :
					sys.stderr.write(std_in)
					sys.stderr.flush()

		__LOG__.Trace('END')

PROC_NAME = os.path.basename(sys.argv[0])
	
def main() :

	if len(sys.argv) < 2 :
		print 'Usage : %s Section ConfigFile [log_arg] [-d]' % PROC_NAME
		return

	section		= sys.argv[1]
	config_file	= sys.argv[2]

	debug_mode = False
	log_arg	= '0'

	if '-d' in sys.argv :
		debug_mode = True

	else :
		log_arg	= sys.argv[3]

	LineSpliter(section, config_file, log_arg, debug_mode).run()


if '__main__' == __name__ :
	main()

