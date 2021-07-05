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
from pathlib import Path
import configparser as ConfigParser
import socket
import json

#import $MOBIGEN_LIB$
import Mobigen.Common.Log_PY3 as Log; Log.Init()
import Mobigen.Database.iris_py3 as iris


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

class WriteDat:

	def __init__(self, module, conf_file_name, conf, center_id, prefix) :

		self.process_name = module
		self.conf_file_name = conf_file_name
		self.center_id = center_id
		self.prefix = prefix
		self.initConf(conf)

		makedirs(self.conf_dat_path)
		makedirs(self.conf_idx_path)
	
	def initConf(self, conf):
		section = 'WRITE_LOG'

		self.conf_mtime 	= os.path.getmtime(self.conf_file_name)
		#open
		__LOG__.Trace("__init__")
		#sep
		try : self.out_data_sep = conf.get(section,'OUT_DATA_SEP')
		except : self.out_data_sep = '|^|'

		self.conf_table_name = conf.get(section, self.prefix)

		self.conf_dat_path = conf.get(section,'DAT_PATH')
		self.conf_ctl_path = conf.get(section,'CTL_PATH')
		self.conf_idx_path = conf.get(section,'IDX_PATH')

		idx_name_patt = os.path.splitext(self.process_name)[0] + '_{}.idx'
		self.conf_idx_name		= idx_name_patt.format(self.conf_table_name)
		self.read_ctl()	
		

	def __del__(self):
		#close
		__LOG__.Trace("__del__")

	def mtimeCheck(self):
		if os.path.getmtime(self.conf_file_name) != self.conf_mtime :

			conf = ConfigParser.ConfigParser()
			conf.read(self.conf_file_name)
		
			self.initConf(conf)
	
	def read_ctl(self):
		ctl_path = os.path.join(self.conf_ctl_path, self.conf_table_name + ".ctl")

		if os.path.exists( ctl_path ) :

			with open (ctl_path ,'r') as f:

				self.header_len = len([ col for col in f.read().split('\n') if col ])

		else : __LOG__.Exception()

	def processing(self, file_name, line, option) :

		with open( file_name, option ) as f: f.write(line + '\n')

	def run(self):

		pre_file = None
	
		idx_file_name = os.path.join(self.conf_idx_path, self.conf_idx_name)	
		
		if os.path.exists(idx_file_name) :
			with open (idx_file_name) as f:
				pre_file = f.read()
				

		while not SHUTDOWN :

			########TEST TIME
#			self.cur_time = datetime.datetime.now().strftime("%Y%m%d%H%M") + '00'
#			self.cur_time = datetime.datetime.now().strftime("%Y%m%d%H") + '0000'
			self.cur_time = datetime.datetime.now().strftime("%Y%m%d%H%M")[:11] + '000'

			self.cur_file = os.path.join(self.conf_dat_path,  '%s-%s-%s.dat' % \
										(self.conf_table_name, self.center_id, self.cur_time ) )

			if pre_file == None : pre_file = self.cur_file
			option = 'w'
			if os.path.exists(self.cur_file) : option = 'a'
				
			if pre_file != None and pre_file != self.cur_file :
				if os.path.exists(pre_file):
					strOut = 'file://%s' % pre_file
					stdout(strOut)

				pre_file = self.cur_file

				with open( idx_file_name, 'w' ) as f : f.write(self.cur_file)

			input, output, err = select.select([sys.stdin] , [], [] , 10)
			for std_in_IO in input:
				std_in = std_in_IO.readline().strip()
				try:

					is_std_err 	= False

					__LOG__.Trace('STD  IN : %s' % std_in )

					try : prefix, line = std_in.split('://',1)
					except : 
						is_std_err = True
						__LOG__.Trace( 'Input format error : %s' % std_in )
						continue

					line = line.strip()
					if line == '' or line == None :
						__LOG__.Trace("Line is empty : %s " % std_in )
						is_std_err = True
						continue
					
					if len(line.split(self.out_data_sep)) != self.header_len :
						__LOG__.Trace("!!!! ERROR !!!!, DIFFERENCE BETWEEN CTL_LENGTH AND LINE_LENGTH => %s" % line)
						continue
					
					self.processing( self.cur_file, line, option )

					is_std_err = True

					self.mtimeCheck()

				except:
					if not SHUTDOWN : __LOG__.Exception()

				finally :
					if std_in != None and is_std_err :
						stderr( std_in )

#- main function ----------------------------------------------------
def main():

	module = os.path.basename(sys.argv[0])

	if len(sys.argv) < 3:
		sys.stderr.write('Usage 	: %s ConfigFile Log_type {option:[[log_arg]-d]}\n' % module )
		sys.stderr.write('Usage 	: %s ConfigFile Log_type {option:[[log_arg]-d]}\n' % module )
		sys.stderr.flush()
		os._exit(1)

	config_file = sys.argv[1]
	prefix 		= sys.argv[2]
	conf = ConfigParser.ConfigParser()
	conf.read(config_file)
	hostname = socket.gethostname()
	center_id = conf.get("GENERAL", hostname )

	if '-d' not in sys.argv :

		etc_argv = sys.argv[3:]
		log_arg = ''

		if len(sys.argv[3:]) > 0 :
			log_arg = '_' + sys.argv[3]

		log_path = conf.get('GENERAL','LOG_PATH')
		makedirs( log_path )	

		log_file = os.path.join(log_path, '%s_%s_%s.log' % (os.path.splitext(module)[0], center_id, prefix) )
		Log.Init(Log.CRotatingLog(log_file, 10240000, 20))		

	else:
		Log.Init()		

	pid = os.getpid()	
	__LOG__.Trace('=======s===== %s START [pid:%s]==================' % ( module, pid ))

	WriteDat(module, config_file, conf, center_id, prefix ).run()

	__LOG__.Trace('============= %s END [pid:%s]====================' % (module, pid ))


#- if name start ----------------------------------------------
if __name__ == "__main__" :
	main()
		


