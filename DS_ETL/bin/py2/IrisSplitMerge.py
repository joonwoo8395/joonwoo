#!/bin/env python
#coding:utf8

import ConfigParser
import time
import sys
import os
import signal
import select
import shutil
import socket

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
		self.setMergeInfo()
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
			self.log_size = 1024*1024

		try :
			self.log_count = self.config.getint('GENERAL', 'LOG_COUNT')
		except :
			self.log_count = 10


	def setMergeInfo(self) :

		try :
			self.merge_interval_sec = self.config.getint(self.section, 'MERGE_INTERVAL_SEC')
		except :
			self.merge_interval_sec = 5

		try :
			self.merge_prefix = self.config.get(self.section, 'MERGE_PREFIX')
		except :
			self.merge_prefix = 'MERGE'

		try :
			self.merge_use_key_flag = self.config.getboolean(self.section, 'MERGE_USE_KEY_FLAG')
		except :
			self.merge_use_key_flag = False

		try :
			self.use_clear_filename = self.config.getboolean(self.section, 'USE_CLEAR_FILENAME')
		except:
			self.use_clear_filename = False

		try :
			self.merge_key_func = eval( 'lambda KEY : %s' % self.config.get(self.section, 'MERGE_KEY_FUNC') )
		except :
			self.merge_key_func = None

		try :
			self.signal_mode = self.config.getboolean(self.section, 'SIGNAL_MODE')
		except :
			self.signal_mode = False

		try :
			self.remove_mode = self.config.getboolean(self.section, 'REMOVE_MODE')
		except :
			self.remove_mode = False

		try :
			self.stdout_prefix = self.config.get(self.section, 'STDOUT_PREFIX')
		except :
			self.stdout_prefix = 'file'

		if self.stdout_prefix.lower() == 'hostname' :
			self.stdout_prefix = socket.gethostname()

	def setSaveInfo(self) :

		self.save_path = os.path.expanduser(self.config.get(self.section, 'SAVE_PATH'))
		self.makedirs(self.save_path)

class SplitMerge :

	def __init__(self, section, config_file, log_arg=0, debug_mode=False) :

		self.class_name = self.__class__.__name__

		self.config = Config( section, config_file, log_arg )

		if not debug_mode : self.setLogging()

		self.merge_file_hash = {}
		self.merge_intreval_hash = {}
		self.cnt = 0

		self.key_sep = '-'

	def setLogging(self) :

		log_name = '%s_%s_%s.log' % ( PROC_NAME, self.config.section, self.config.log_arg )
		log_file = os.path.join( self.config.log_path, log_name )

		Log.Init(Log.CRotatingLog( log_file, self.config.log_size, self.config.log_count ))

		__LOG__.Trace( log_file )

	def processing(self, in_file) :

		try :
			table_name, key, partition = os.path.splitext(os.path.basename(in_file))[0].split(self.key_sep)[-3:]
		except : return

		if self.config.merge_use_key_flag :
			if self.config.merge_key_func != None :
				merge_key = self.key_sep.join( [ table_name, self.config.merge_key_func(key), partition ] )
			else :
				merge_key = self.key_sep.join( [ table_name, key, partition ] )
		else :
			merge_key = self.key_sep.join( [ table_name, '0', partition ] )

		if merge_key not in self.merge_file_hash :
			__LOG__.Trace( 'New merge key %s' % merge_key )
			self.config.makedirs( self.config.save_path )

			if self.config.use_clear_filename :
				merge_name = '%s.dat' % ( merge_key )
			else:
				merge_time = time.strftime( '%Y%m%d%H%M%S', time.localtime() )
				merge_name = '%s-%s-%s.dat' % ( self.config.merge_prefix, merge_time, merge_key )
			
			merge_file = os.path.join( self.config.save_path, merge_name )

			self.merge_file_hash[ merge_key ] = merge_file

		merge_file = self.merge_file_hash[ merge_key ]

		if not self.config.signal_mode :
			try :
				with open( in_file, 'rb' ) as read_fd, open( merge_file, 'ab' ) as write_fd :
					#shutil.copyfileobj(read_fd, write_fd, 10*1024*1024 )
					shutil.copyfileobj(read_fd, write_fd)
			except :
				__LOG__.Exception()

		if self.config.remove_mode :
			os.remove( in_file )

		self.merge_intreval_hash[ partition ] = time.time()

	def mergeIntervalInfo(self) :

		check_time = time.time()

		for merge_partition in sorted(self.merge_intreval_hash.copy()) :

			last_update_partition = self.merge_intreval_hash[merge_partition]

			if check_time - last_update_partition < self.config.merge_interval_sec : continue

			count = 0

			for merge_key in sorted(self.merge_file_hash.copy()) :
				table_name, key, partition = merge_key.split(self.key_sep)

				if merge_partition != partition : continue

				merge_file = self.merge_file_hash[merge_key]
				sys.stdout.write( '%s://%s\n' % ( self.config.stdout_prefix, merge_file ) )
				sys.stdout.flush()

				del self.merge_file_hash[merge_key]
				count += 1

			del self.merge_intreval_hash[merge_partition]

			if count > 0 :
				__LOG__.Trace( 'Interval %s partition count %s' % ( merge_partition, count ) )

	def mergeFileInfo(self) :

		if len( self.merge_file_hash ) == 0 : return

		start_time = time.time()

		count = len(self.merge_file_hash)

		for merge_key in sorted(self.merge_file_hash.copy()) :
			merge_file = self.merge_file_hash[merge_key]
			__LOG__.Trace( 'STD OUT : %s' % merge_file )
			sys.stdout.write( '%s://%s\n' % ( self.config.stdout_prefix, merge_file ) )
			sys.stdout.flush()
			del self.merge_file_hash[merge_key]

		end_time = time.time()
		duration = end_time - start_time

		__LOG__.Trace('Count %s Duration %s sec' % ( count, duration ) )
		__LOG__.Trace('stdin Count :%s' % str(self.cnt))
		self.cnt = 0

	def run(self) :
		
		__LOG__.Trace('START')

		while not SHUTDOWN :
			std_in = None
			is_std_err = False

			try :
				select_in = select.select( [sys.stdin], [], [], self.config.merge_interval_sec )[0]
				if select_in :
					std_in = select_in[0].readline()

					if not std_in.strip() :
						is_std_err = True
						continue
				
					try :
						prefix, in_file = std_in.strip().split('://', 1)
					except :
						is_std_err = True
						__LOG__.Trace( 'Input format error : %s' % std_in )
						continue

					if not self.config.signal_mode :
						if not os.path.isfile( in_file ) :
							is_std_err = True
							continue

					self.cnt += 1
					self.processing(in_file)
					is_std_err = True
				else : 
					self.mergeFileInfo()

				self.mergeIntervalInfo()

			except :
				if not SHUTDOWN : __LOG__.Exception()
				
			finally :
				if is_std_err :
					sys.stderr.write(std_in)
					sys.stderr.flush()

		self.mergeFileInfo()

		__LOG__.Trace('END')

PROC_NAME = os.path.basename(sys.argv[0])
	
def main() :

	if len(sys.argv) < 2 :
		print 'Usage : %s Section ConfigFile [log_arg] [-d]' % PROC_NAME
		return

	section		= sys.argv[1]
	config_file	= sys.argv[2]

	debug_mode = False
	try :
		log_arg	= sys.argv[3]
	except :
		log_arg	= os.getpid()

	if '-d' in sys.argv :
		debug_mode = True

	SplitMerge(section, config_file, log_arg, debug_mode).run()


if '__main__' == __name__ :
	main()

