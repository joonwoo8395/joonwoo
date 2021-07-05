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
								

class FilePatternMonitor:
	def __init__(self, module, section, cfgfile):
		self.module = module
		self.section = section
		self.cfgfile = cfgfile
		self.interval = 10
		self.monitor_path = "~/"
		self.file_patt = "*"
		self.ext_str = ""
		self.search_patt = self.file_patt + self.ext_str
		self.sequence = "name"
		self.file_sequence = lambda x: x

		if '-d' not in sys.argv:
			self.set_logger()
		else: Log.Init() 
		self.set_config(self.section)


	def set_config(self, section):
		cfg = ConfigParser.ConfigParser()
		cfg.read(self.cfgfile)

		if cfg.has_option(section, "LS_INTERVAL"):
			self.interval = cfg.getint(section, "LS_INTERVAL")
		elif self.cfg.has_option("GENERAL", "LS_INTERVAL"):
			self.interval = cfg.getint("GENERAL", "LS_INTERVAL")

		if cfg.has_option(section, "DIRECTORY"):
			self.monitor_path = cfg.get(section, "DIRECTORY")
		else:
			__LOG__.Trace("Error: Require monitoring directory option on config")
			sys.exit()

		if cfg.has_option(section, "INDEX_FILE"):
			self.idx_file = cfg.get(section, "INDEX_FILE")
		else:
			__LOG__.Trace("Error: Require index file path on config")
			sys.exit()

		if cfg.has_option(section, "FILE_PATTERN"):
			self.file_patt = cfg.get(section, "FILE_PATTERN")
		else:
			__LOG__.Trace("Error: Require file pattern on config")
			sys.exit()

		if cfg.has_option(section, "EXTEND_STR"):
			self.ext_str = cfg.get(section, "EXTEND_STR")

		if not os.path.isdir(os.path.split(self.idx_file)[0]):
			os.makedirs(os.path.split(self.idx_file)[0])

		if cfg.has_option(section, "FILE_SORT"):
			self.sequence = cfg.get(section, "FILE_SORT")

		#########################################################################
		if cfg.has_option(section, "COMP_DIR") :
			self.COMP_DIR = cfg.get(section, "COMP_DIR")

		if cfg.has_option(section, "CHECK_INTERVAL") :
			self.CHECK_INTERVAL = cfg.getint(section, "CHECK_INTERVAL")
		#########################################################################

		if self.sequence == "basename":
			self.file_sequence = lambda x: os.path.basename(x)
		elif self.sequence == "atime":
			self.file_sequence = lambda x: os.stat(x).st_atime
		elif self.sequence == "ctime":
			self.file_sequence = lambda x: os.stat(x).st_ctime
		elif self.sequence == "mtime":
			self.file_sequence = lambda x: os.stat(x).st_mtime

	def set_logger(self):
		
		cfg = ConfigParser.ConfigParser()
		cfg.read(self.cfgfile)
		log_path = cfg.get("GENERAL", "LOG_PATH")
		log_file = os.path.join(log_path, "%s_%s.log" % (self.module, self.section))
		Log.Init(Log.CRotatingLog(log_file, 10240000, 9))

	def stdout(self, msg):
		sys.stdout.write(msg + "\n")
		sys.stdout.flush()
		__LOG__.Trace("Std OUT : %s" % (msg))

	def load_index(self):
		idx_file = self.idx_file
		if os.path.exists(idx_file):
			with open(idx_file, "r") as rfd:
				curr = rfd.read().strip()
			__LOG__.Trace("Load index : %s" % curr)
			if not os.path.exists(curr):
				__LOG__.Trace("File indicated by index file not exists : %s" % curr)
				curr = None
			if curr == '':
				curr = None
		else:
			__LOG__.Trace("Index file not exists : %s" % idx_file)
			curr = None
		return curr


	def dump_index(self, curr_file):
		# 인덱스파일 쓰기
		with open(self.idx_file, "w") as wfd:
			wfd.write(curr_file + "\n")
		__LOG__.Trace("Dump Index : %s" % curr_file)

	def get_file_list(self):
		# 패턴을 이용해 monitor_path 안에 있는 파일 리스트 얻기
		file_list = glob.glob(os.path.join(self.monitor_path, self.search_patt))
		file_list.sort(key=self.file_sequence)
		return file_list

	def get_new_list(self, curr_file):
		# 
		file_list = self.get_file_list()
#		__LOG__.Trace("file_list : {}".format(file_list) )
		for file_name in file_list:
#			__LOG__.Trace("index_file : {}".format(self.file_sequence(curr_file)))
#			__LOG__.Trace("new File List : {}".format(self.file_sequence(file_name)))
			if self.file_sequence(curr_file) < self.file_sequence(file_name):
#				__LOG__.Trace("return : {}".format(file_list[file_list.index(file_name):]))
				return file_list[file_list.index(file_name):]
		return []

	def fileModifiedCheck(self, filePath, lastMTime=0) :
		time.sleep(self.CHECK_INTERVAL)
		# 2020-10-13 추가 (김준우) 파일 mtime 체크
		fileMTime = os.path.getmtime(filePath)
		returnCompPath = None

		if fileMTime > lastMTime :
			__LOG__.Trace("Updating file..... Last Mtime : {}, Now Mtime : {}".format(lastMTime, fileMTime))
			returnCompPath = self.fileModifiedCheck(filePath, fileMTime)

		elif fileMTime == lastMTime :
			__LOG__.Trace("File Transform complete Last Mtime : {}, Now Mtime : {}".format(lastMTime, fileMTime))
			if not os.path.exists(self.COMP_DIR) :
				mkdirs(self.COMP_DIR)

			compPath = os.path.join( self.COMP_DIR, os.path.basename(filePath) )

			if os.path.exists(compPath) :
				compPath = os.path.join( self.COMP_DIR, os.path.basename(filePath)+ '.' + datetime.datetime.today().strftime('%Y%m%d%H%M%S') )

			shutil.move( filePath, compPath )
			__LOG__.Trace( "Move From : {}  To : {}".format(filePath, compPath) )
			returnCompPath = compPath

		else :
			__LOG__.Trace("invalid Time Last Mtime : {}, Now Mtime : {}".format(lastMTime, fileMTime))
			returnCompPath = ''

		return returnCompPath
	
	def run(self):
#		curr_file 		= self.load_index()
		compFilePath 	= None

		while not SHUTDOWN :
#			try :
#			if curr_file:
#				new_file_list = self.get_new_list(curr_file)
#			else:
#				new_file_list = self.get_file_list()
			new_file_list = self.get_file_list()

			for file_name in new_file_list:
				if len(self.ext_str) == 0:
					out_file = file_name
				else:
					out_file = file_name[:-(len(self.ext_str))]
				###############################################
				compFilePath = self.fileModifiedCheck(out_file)

				__LOG__.Trace( "File Check : {}".format(compFilePath) )

				if os.path.exists(compFilePath) :
					self.stdout("file://%s" % compFilePath)
				###############################################
#				self.stdout("file://%s" % out_file)

#				curr_file = file_name
#				curr_file = compFilePath

#			if curr_file and len(new_file_list) > 0:
#				self.dump_index(curr_file)

			time.sleep(self.interval)

#		if curr_file:
#			self.dump_index(curr_file)

def main():
	module = os.path.basename(sys.argv[0])

	if len(sys.argv) < 3:
		print( "Usage : %s Section ConfigFile" % module, sys.stderr )
		print( "Exam  : %s LTE_CDR /home/eva/E2ES/conf/FilePatternMonitor.conf" % module, sys.stderr)
		return

	section = sys.argv[1]
	cfgfile = sys.argv[2]

	FilePatternMonitor(module, section, cfgfile).run()


if __name__ == "__main__":
	try:
		main()
	except:
		__LOG__.Exception()
