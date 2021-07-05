#!/bin/env python3
# -*- coding:utf8 -*-

import signal
import sys
import time
import configparser
import os
import re
import os
from pathlib import Path

from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler

import Mobigen.Common.Log_PY3 as Log; Log.Init()


SHUTDOWN = False
def handler(sigNum, frame):
	__LOG__.Trace('Catch Signal Number = [%s]' % sigNum)
	global SHUTDOWN
	SHUTDOWN = True

def stdout(msg) :

	sys.stdout.write(msg + '\n')
	sys.stdout.flush()
	__LOG__.Trace('Std OUT : %s' % msg)

def makedirs(path) :
	try :
		os.makedirs( path )
		__LOG__.Trace( 'Make dirs %s' % path )
	except : pass

signal.signal(signal.SIGTERM, handler)
signal.signal(signal.SIGINT, handler)
try:signal.signal(signal.SIGHUP, signal.SIG_IGN)
except:pass
try:signal.signal(signal.SIGPIPE, signal.SIG_IGN)
except:pass


class EventHandler(PatternMatchingEventHandler) :

	def setOption(self, *args, **kwargs):

		self.watch_mode					= args[0]
		self.watch_file_startswith		= args[1]
		self.watch_file_endswith		= args[2]
		self.watch_file_match			= args[3]
		self.watch_file_match_pattern	= args[4]
		
		file_lists = args[5]
		for file_name in file_lists:
			self.processing(file_name)
	
	def processing(self, file_name) :

		if self.watch_file_startswith != '' :
			if not os.path.basename(file_name).startswith( self.watch_file_startswith ) :
				return

		if self.watch_file_endswith != '' :
			if not os.path.basename(file_name).endswith( self.watch_file_endswith ) :
				return

		if self.watch_file_match != '' :
			if not self.watch_file_match in file_name :
				return

		if self.watch_file_match_pattern != '' :
			if not re.findall( self.watch_file_match_pattern, file_name ) :
				return

		stdout( 'file://%s\n' % file_name )

	def on_created(self, event) :

		if event.event_type not in self.watch_mode :
			return

		self.processing( event.src_path )

	def on_modified(self, event) :

		if event.event_type not in self.watch_mode :
			return

		self.processing( event.src_path )

	def on_deleted(self, event) :

		if event.event_type not in self.watch_mode :
			return

		self.processing( event.src_path )

	def on_moved(self, event) :

		if event.event_type not in self.watch_mode :
			return

		self.processing( event.dest_path )

class FileWatch :

	def __init__(self, conf, section) :

		self.watch_path					= conf.get(section, 'WATCH_PATH')
		makedirs(self.watch_path)

		try: self.watch_mode					= self.setWatchMode( conf.get(section, 'WATCH_MODE') )
		except : self.watch_mode( 'MOVED' )

		try: self.watch_file_startswith		= conf.get(section, 'WATCH_FILE_STARTSWITH')
		except: self.watch_file_startswith = ''

		try: self.watch_file_endswith		= conf.get(section, 'WATCH_FILE_ENDSWITH')
		except: self.watch_file_endswith = ''

		try: self.watch_file_match			= conf.get(section, 'WATCH_FILE_MATCH')
		except: self.watch_file_match = ''
		
		try: self.watch_file_match_pattern	= conf.get(section, 'WATCH_FILE_MATCH_PATTERN')
		except: self.watch_file_match_pattern = ''

	def setWatchMode(self, mode) :
		
		mode_list = [ mode_name.strip() for mode_name in mode.lower().split(',') ]
		
		return mode_list

	def initProc(self):
		
		lists = []
		configfiles = Path(self.watch_path).glob('**/*.*')
		for file_name in configfiles:
			lists.append(file_name)
		
		return lists

	def run(self) :

		file_lists = self.initProc()
		
		event_handler = EventHandler()
		event_handler.setOption(
						self.watch_mode,
						self.watch_file_startswith,
						self.watch_file_endswith,
						self.watch_file_match,
						self.watch_file_match_pattern,
						file_lists
						)

		observer = Observer()
		observer.schedule(event_handler, self.watch_path, recursive=True)
		observer.start()

		global SHUTDOWN
		try:
			while not SHUTDOWN :
				time.sleep(1)
		except :
			if not SHUTDOWN : __LOG__.Exception()
			observer.stop()
		
		#observer.join()

def main():

	module = os.path.basename(sys.argv[0])
	
	if len( sys.argv ) < 3 :
		sys.stderr.write('Usage	 : %s section conf {option:[[log_arg]-d]}\n' % module )
		sys.stderr.flush()
		os._exit(1)

	section		= sys.argv[1]
	conf_file	= sys.argv[2]

	conf = configparser.ConfigParser()
	conf.read( conf_file )

	if '-d' in sys.argv :
		Log.Init()
	else :

		etc_argv = sys.argv[3:]
		log_arg = ''

		if len(sys.argv[3:]) > 0 :
			log_arg = '_' + sys.argv[3]

		log_path = conf.get( 'GENERAL', 'LOG_PATH' )

		makedirs( log_path )

		log_file = os.path.join( log_path, '%s_%s%s.log' % (os.path.splitext(module)[0], section, log_arg ) )
		Log.Init(Log.CRotatingLog(log_file, 10240000, 9))

	pid = os.getpid()
	__LOG__.Trace('============= %s START [pid:%s]==================' % ( module, pid ))

	FileWatch(conf, section).run()

	__LOG__.Trace('============= %s END [pid:%s]====================' % (module, pid ))

if __name__ == "__main__":
	main()
