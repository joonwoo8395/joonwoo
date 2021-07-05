#!/bin/env python3
# -*- coding: utf-8 -*-

#- needed import ----------------------------------------------------
#import $PYTHON_LIB$
import os
import sys
import time
import datetime
import signal
import select
import configparser as ConfigParser
import socket

#import $MOBIGEN_LIB$
import Mobigen.Common.Log_PY3 as Log; Log.Init()


#import $PROJECT_LIB$

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
	#__LOG__.Trace('Std ERR : %s' % msg)

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

class WriteDat:

	def __init__(self, conf, prefix, idx_name) :

		section = 'WRITE_LOG'
		
		self.conf = conf
		self.target_prefix = prefix
		
		self.idx_name = idx_name
		self.conf_idx_path = conf.get(section,'IDX_PATH')
		makedirs(self.conf_idx_path)
		self.idx_file_name = os.path.join(self.conf_idx_path, self.idx_name)
		__LOG__.Watch(self.idx_file_name)

		try : self.out_data_sep = conf.get(section,'OUT_DATA_SEP')
		except : self.out_data_sep = '|^|'

		self.conf_table_name = conf.get(section, self.target_prefix)

		self.conf_dat_path = conf.get(section,'DAT_PATH')
		makedirs(self.conf_dat_path)
		self.dat_file_name_form = os.path.join(self.conf_dat_path, '%s-%s' % ( conf.get(section, self.target_prefix), conf.get("GENERAL", socket.gethostname()) ))


		self.conf_ctl_path = conf.get(section,'CTL_PATH')
		ctl_path = os.path.join(self.conf_ctl_path, self.conf_table_name + ".ctl")
		
		with open (ctl_path ,'r') as fd:
			self.header_len = len([ col for col in fd.read().split('\n') if col != '' ])

		try:
			with open(self.idx_file_name) as fd:
				self.prev_dat_file_name = fd.read().strip()
		except:
			#self.prev_dat_file_name = '%s-%s00.dat' % ( self.dat_file_name_form, datetime.datetime.now().strftime("%Y%m%d%H%M") )
			self.prev_dat_file_name = '%s-%s000.dat' % ( self.dat_file_name_form, datetime.datetime.now().strftime("%Y%m%d%H%M")[:11] )

	def run(self):

		while not SHUTDOWN :

			try:
				std_in = None
				is_std_err = False

				#cur_dat_file_name = '%s-%s00.dat' % ( self.dat_file_name_form, datetime.datetime.now().strftime("%Y%m%d%H%M") )
				cur_dat_file_name = '%s-%s000.dat' % ( self.dat_file_name_form, datetime.datetime.now().strftime("%Y%m%d%H%M")[:11] )
				
				if self.prev_dat_file_name != cur_dat_file_name :

					if os.path.exists( self.prev_dat_file_name ):
						if os.path.getsize(self.prev_dat_file_name) > 0:
							stdout( 'file://' + self.prev_dat_file_name )
						else:
							__LOG__.Trace('file size 0 file remove: %s' % self.prev_dat_file_name)
							os.remove(self.prev_dat_file_name)
					else:
						__LOG__.Trace('prev file is not exists : %s' % self.prev_dat_file_name)

					self.prev_dat_file_name = cur_dat_file_name
					
					with open(self.idx_file_name, 'w') as fd: 
						fd.write(self.prev_dat_file_name)	

				inputready, outputready, exceptready = select.select([sys.stdin] , [], [] , 10)

				for std_in in inputready:

					std_in = std_in.readline().strip()				

					if not std_in :
						is_std_err = True
						continue

					try :
						prefix, line = std_in.split('://', 1)
					except :
						is_std_err = True
						continue

					## US, DJ 처리 전까지 임시 2021 06 01 ########################################################
					if ( prefix == 'ER_LOG' ) and ( self.conf.get("GENERAL", socket.gethostname()) in ['US','DJ'] ):
						is_std_err = True
						continue
					##############################################################################################

					if prefix != self.target_prefix :
						is_std_err = True
						continue

					if line == '' or len(line.split(self.out_data_sep)) != self.header_len :
						__LOG__.Trace("!!!! ERROR !!!!, DIFFERENCE BETWEEN CTL_LENGTH AND LINE_LENGTH => %s" % line)
						is_std_err = True
						continue

					__LOG__.Trace('STD  IN : %s' % std_in )
					with open(self.prev_dat_file_name, 'a+') as fd: 
					#with open(cur_dat_file_name, 'a+') as fd: 
						fd.write('%s\n' % line)

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
		sys.stderr.write('Usage 	: %s prefix ConfigFile {option:[[log_arg]-d]}\n' % module )
		sys.stderr.write('Usage 	: %s prefix ConfigFile {option:[[log_arg]-d]}\n' % module )
		sys.stderr.flush()
		os._exit(1)

	prefix 		= sys.argv[1]
	config_file = sys.argv[2]

	conf = ConfigParser.ConfigParser()
	conf.read(config_file)

	log_arg = ''

	if '-d' not in sys.argv :

		etc_argv = sys.argv[3:]

		if len(sys.argv[3:]) > 0 :
			log_arg = '_' + sys.argv[3]

		log_path = conf.get('GENERAL','LOG_PATH')
		makedirs( log_path )	

		log_file = os.path.join(log_path, '%s_%s%s.log' % (os.path.splitext(module)[0], prefix, log_arg) )
		Log.Init(Log.CRotatingLog(log_file, 10240000, 9))		

	else:
		Log.Init()		


	pid = os.getpid()	
	__LOG__.Trace('=======s===== %s START [pid:%s]==================' % ( module, pid ))

	idx_name = '%s_%s%s.idx' % (os.path.splitext(module)[0], prefix, log_arg)
	WriteDat( conf, prefix, idx_name ).run()

	__LOG__.Trace('============= %s END [pid:%s]====================' % (module, pid ))


#- if name start ----------------------------------------------
if __name__ == "__main__" :
	main()
		


