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
import configparser as ConfigParser
from pathlib import Path
import shutil
import re

#import $MOBIGEN_LIB$
import Mobigen.Common.Log_PY3 as Log; Log.Init()
#import $PROJECT_LIB$
import Mobigen.Utils.LogClient as c_log

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

def makedirs(path) :

	try :
		os.makedirs(path)
		__LOG__.Trace( 'makedirs : %s' % path )
	except : pass

def stdout(msg) :

	sys.stdout.write('%s\n' % msg)
	sys.stdout.flush()
	__LOG__.Trace('Std OUT [ %s ]' % msg)

#- Class ----------------------------------------------------

class ClassName:

	def __init__(self, conf) :
		#open
		__LOG__.Trace("__init__")
		section	= 'MONITOR'
			
		self.watch_path                 	= conf.get(section, 'WATCH_PATH')
		if not os.path.isdir(self.watch_path): raise Exception( 'WATCH_PATH is not directory' ) 
		#makedirs(self.watch_path)
		if self.watch_path == '/' : self.watch_path = self.watch_path[:-1]
	
		try: self.watch_interval			= conf.getint(section, 'WATCH_PATH')
		except:
			try: self.watch_interval		= conf.getint('GENERAL', 'DIR_RECURSIVE')
			except: self.watch_interval		= 5

		try: self.dir_recursive					= conf.getboolean(section, 'DIR_RECURSIVE')
		except:
			try: self.dir_recursive				= conf.getboolean('GENERAL', 'DIR_RECURSIVE')
			except: self.dir_recursive 			= True		
		
		try: self.watch_file_startswith     	= conf.get(section, 'WATCH_FILE_STARTSWITH').split(';')
		except: self.watch_file_startswith 		= []
		__LOG__.Watch(self.watch_file_startswith)

		try: self.watch_file_endswith       	= conf.get(section, 'WATCH_FILE_ENDSWITH')
		except: self.watch_file_endswith 		= ''

		try: self.watch_file_match          	= conf.get(section, 'WATCH_FILE_MATCH')
		except: self.watch_file_match 			= ''

		try: self.watch_file_match_pattern 		= conf.get(section, 'WATCH_FILE_MATCH_PATTERN')
		except: self.watch_file_match_pattern 	= ''

		try: self.comp_flag						= conf.getboolean(section, 'COMP_FLAG')
		except:
			try: self.comp_flag					= conf.getboolean('GENERAL', 'COMP_FLAG')
			except: self.comp_flag				= False

		try: self.comp_interval            		= conf.getint(section, 'COMP_INTERVAL')
		except:
			try: self.comp_interval        		= conf.getint('GENERAL', 'COMP_INTERVAL')
			except: self.comp_interval     		= 5

		try: self.comp_move						= conf.getboolean(section, 'COMP_MOVE')
		except:
			try: self.comp_move					= conf.getboolean('GENERAL', 'COMP_MOVE')
			except: self.comp_move				= True

		try : self.comp_path					= conf.get(section, 'WATCH_COMP_PATH')
		except :
			try : self.comp_path				= conf.get('GENERAL', 'WATCH_COMP_PATH')
			except : self.comp_path				= ''

	def processing(self, file_name) :
		
		if not os.path.isfile(file_name): return

		startswith_flag = False
		for watch_startswith in self.watch_file_startswith:
			if os.path.basename(file_name).startswith( watch_startswith ): startswith_flag = True
			#if self.watch_file_startswith != '' and not os.path.basename(file_name).startswith( self.watch_file_startswith ) : return
		if not startswith_flag: return

		if self.watch_file_endswith != '' and not os.path.basename(file_name).endswith( self.watch_file_endswith ) : return

		if self.watch_file_match != '' and not self.watch_file_match in file_name : return

		if self.watch_file_match_pattern != '' and not re.findall( self.watch_file_match_pattern, file_name ) : return

		if self.comp_move: 
			dirs, names = os.path.split( file_name )
			comp_path = self.comp_path + dirs.split( self.watch_path )[1]
			makedirs( comp_path )
			comp_file_name = os.path.join( comp_path, names )

			if os.path.isfile( comp_file_name ) : 
				head, tail  = os.path.splitext(comp_file_name)
				comp_file_name = head + '_' + datetime.datetime.now().strftime('%Y%m%d%H%M%S') + tail
				
			shutil.move( file_name, comp_file_name )
			__LOG__.Trace( "File Moved From : {}  To : {}".format(file_name, comp_file_name) )
			stdout( 'file://%s' % comp_file_name )
			
			#############################################################################

	def run(self):

		while not SHUTDOWN :

			try:
			
				if self.dir_recursive:
					configfiles = Path(self.watch_path).glob('**/*')
				else:
					configfiles = Path(self.watch_path).glob('*')

				for file_name in configfiles:

					if not os.path.isfile(file_name): continue
					if os.path.splitext(file_name)[-1] == '.tmp' : continue
					
					if self.comp_flag:
						first_mtime = os.path.getmtime(file_name)
						time.sleep(self.comp_interval)
						if first_mtime != os.path.getmtime(file_name): continue

					#self.std_in             = in_file
					#self.in_file_size		= str(os.path.getsize(file_name))

					#if file_name.endswith('.csv') or  file_name.endswith('.dat') :
					#	self.in_file_row_cnt	= subprocess.check_output(["wc","-l", std_out]).split()[0]

					self.processing(file_name)	

				time.sleep(self.watch_interval)

			except FileNotFoundError: 
				__LOG__.Exception()
				continue
			except:
				if not SHUTDOWN : __LOG__.Exception()

		
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

	config_file = sys.argv[1]

	conf = ConfigParser.ConfigParser()
	conf.read(config_file)

	if '-d' not in sys.argv :

		etc_argv = sys.argv[2:]
		log_arg = ''

		if len(sys.argv[2:]) > 0 :
			log_arg = '_' + sys.argv[2]

		log_path = conf.get('GENERAL', 'LOG_PATH')

		makedirs( log_path )	

		log_file = os.path.join(log_path, '%s%s.log' % (os.path.splitext(module)[0], log_arg ))
		Log.Init(Log.CRotatingLog(log_file, 10240000, 9))		

	else:
		Log.Init()		

	pid = os.getpid()	
	__LOG__.Trace('============= %s START [pid:%s]==================' % ( module, pid ))

	ClassName(conf).run()

	__LOG__.Trace('============= %s END [pid:%s]====================' % (module, pid ))


#- if name start ----------------------------------------------
if __name__ == "__main__" :
	main()


