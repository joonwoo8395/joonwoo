#!/usr/bin/env python
# -*- coding: utf-8 -*-

import re
import os
import sys
import glob
import time
import zipfile
import signal
import ConfigParser
import Mobigen.Common.Log as Log; Log.Init()

SHUTDOWN = False

def handler(signum, frame) :

	global SHUTDOWN
	SHUTDOWN = True
	__LOG__.Trace("Catch Signal = %s" % signum)

signal.signal(signal.SIGTERM, handler)
signal.signal(signal.SIGINT , handler)
signal.signal(signal.SIGHUP , handler)
signal.signal(signal.SIGPIPE, handler)


class FileCompressor() :

	def __init__(self, section, cfgfile) :

		self.cfg = ConfigParser.ConfigParser()
		self.cfg.read(cfgfile)

		self.section = section

		log_path = self.cfg.get("GENERAL", "LOG_PATH")
		log_file = os.path.join(log_path, "%s/%s.log" % (os.path.basename(sys.argv[0])[:-3], (section)))
		Log.Init(Log.CRotatingLog(log_file, 1000000, 9))



	def load_index(self, idx_file) :

		if os.path.exists(idx_file) == True :
			rfd = open(idx_file, "r")
			curr = rfd.read().strip()
			rfd.close()
			__LOG__.Trace("Load Index : %s" % curr)
		else :
			__LOG__.Trace("IndexFile Not Exists : %s" % idx_file)
			curr = None

		return curr


	def dump_index(self, idx_file, curr_file) :

		wfd = open(idx_file, "w")
		wfd.write(curr_file + "\n")
		wfd.close()
		#__LOG__.Trace("Dump Index : %s" % curr_file)


	def sort_key(self, file_name) :

		try : return os.stat(file_name).st_mtime
		except : return 0


	def get_file_list(self, path, patt) :

		while not SHUTDOWN :
			file_list = glob.glob(os.path.join(path, patt))
			file_list.sort( key=self.sort_key )
			time.sleep(0.1)
			file_list2 = glob.glob(os.path.join(path, patt))
			file_list2.sort( key=self.sort_key)

			if file_list == file_list2 :
				return file_list



	def get_new_list(self, path, patt, curr_file) :

		file_list = self.get_file_list(path, patt)

		for f in file_list :
			if self.sort_key( curr_file )  <  self.sort_key( f ) :
				return file_list[file_list.index(f):]

		return []

	def FileCompress(self, fileList) :

		try :

			compressDir = self.cfg.get(self.section, "COMPRESS_DIRECTORY")
			zipFileName = '%s/%s_%s.zip.tmp' % (compressDir, self.section, time.strftime('%Y%m%d%H%M%S'))

			fileCompress = zipfile.ZipFile(zipFileName, 'w')

			for f in fileList :
				__LOG__.Trace(f)
				fileCompress.write(f, os.path.basename(f), zipfile.ZIP_DEFLATED)

			fileCompress.close()

			os.rename(zipFileName, zipFileName[:-4])
			__LOG__.Trace('FileCompressed - %s' % fileCompress)

		except :
			__LOG__.Exception()


	def MakeIdxFile(self) :
		monitor_path = self.cfg.get(self.section, "DIRECTORY")
		idx_file = self.cfg.get(self.section, "INDEX")
		file_patt = self.cfg.get(self.section, "PATTERN")

		new_file_list = self.get_file_list(monitor_path, file_patt)

		if len(new_file_list) > 0 :
			last_file = new_file_list[len(new_file_list) - 1]
			if last_file != None and len(new_file_list) > 0 :
				self.dump_index(idx_file, last_file)


	def run(self) :

		monitor_path = self.cfg.get(self.section, "DIRECTORY")
		idx_file = self.cfg.get(self.section, "INDEX")
		file_patt = self.cfg.get(self.section, "PATTERN")
		ls_sec = self.cfg.getint(self.section, "COMPRESS_INTERVAL")

		curr_file = self.load_index(idx_file)

		while not SHUTDOWN :

			if curr_file != None :
				new_file_list = self.get_new_list(monitor_path, file_patt, curr_file)
			else :
				new_file_list = self.get_file_list(monitor_path, file_patt)

			if (len(new_file_list) > 0) :
				self.FileCompress(new_file_list)
				curr_file = new_file_list[len(new_file_list) - 1]

			if curr_file != None and len(new_file_list) > 0 :
				self.dump_index(idx_file, curr_file)

			time.sleep(ls_sec)
			continue

		if curr_file != None :
			self.dump_index(idx_file, curr_file)


def usage() :

	module = os.path.basename(sys.argv[0])

	print >> sys.stderr, "Usage : %s Section ConfigFile" % module
	print >> sys.stderr, "Exam  : %s SECT FileCompressor.conf" % module
	sys.exit()


if __name__ == "__main__" :

	if (len(sys.argv) < 3) : usage()

	fileMon = FileCompressor(sys.argv[1], sys.argv[2])
	fileMon.MakeIdxFile()
	fileMon.run()

	__LOG__.Trace("PROCESS END...\n")
	#sys.stderr.write("LOG://PROCESS END...\n")

