#!/usr/bin/env python
# -*- coding: utf-8 -*-

import re
import os
import sys
import glob
import time
import zipfile
import bz2
import shutil
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


class FileUnCompressor() :

	def __init__(self, section, cfgfile) :

		self.cfg = ConfigParser.ConfigParser()
		self.cfg.read(cfgfile)

		self.section = section

		log_path = self.cfg.get("GENERAL", "LOG_PATH")
		if os.path.exists(log_path) == False : os.mkdir(log_path)
		log_file = os.path.join(log_path, "%s_%s.log" % (os.path.basename(sys.argv[0])[:-3], (section)))
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

		for file in file_list :
			if self.sort_key( curr_file )  <  self.sort_key( file ) :
				return file_list[file_list.index(file):]

		return []


	def FileUnCompressBz2(self, fileList) :
		try :

			unCompressDir = self.cfg.get(self.section, "UNCOMPRESS_DIRECTORY")

			for file in fileList :

				__LOG__.Trace('Decomp Start - %s')

				bz2_file = bz2.BZ2File(file)

				filePath = '%s/unCompressDir/%s' % (unCompressDir, os.path.basename(bz2_file.name)[:-4])

				f = open(filePath, 'w')
				f.write(bz2_file.read())
				f.close()

				os.rename(filePath, os.path.join(unCompressDir, os.path.basename(filePath)))

				__LOG__.Trace('Decomp End - %s' % bz2_file.name)				
				
		except :
			__LOG__.Exception()



	def FileUnCompress(self, fileList) :

		try :

			unCompressDir = self.cfg.get(self.section, "UNCOMPRESS_DIRECTORY")

			for file in fileList :

				fh = open(file,'r')
				z = zipfile.ZipFile(fh)

				for name in z.namelist():
					f = z.extract(name, '%s/unCompressDir' % unCompressDir)
					fileName = '%s/%s' % (unCompressDir, name)
					os.rename(f, fileName)
					fh.close()

					try :
						if False :
							# 처리 성능 시험용 -- 처리할 CDR 을 두배로 올린다... 파일명을 바꿔서 복사..
							# f = 'UMTS.D2141114.S001.F0066947.B01'
							# '%s1%s' % (f[:15], f[16:])
							#  --> UMTS.D2141114.S101.F0066947.B01
							# shutil.copyfile('d:\\aaa.txt', 'd:\\bbb.txt')
							if fileName[:4] == 'UMTS' :
								shutil.copyfile(fileName, '%s1%s' % (fileName[:15], fileName[16:]))
					except :
						__LOG__.Exception()

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
		ls_sec = self.cfg.getint(self.section, "UNCOMPRESS_INTERVAL")
		unCompressDir = self.cfg.get(self.section, "UNCOMPRESS_DIRECTORY")
	
		if not os.path.exists(os.path.join(unCompressDir, 'unCompressDir')) : os.makedirs(os.path.join(unCompressDir, 'unCompressDir'))

		curr_file = self.load_index(idx_file)

		while not SHUTDOWN :

			if curr_file != None :
				new_file_list = self.get_new_list(monitor_path, file_patt, curr_file)
			else :
				new_file_list = self.get_file_list(monitor_path, file_patt)

			if (len(new_file_list) > 0) :
				self.FileUnCompressBz2(new_file_list)
				#self.FileUnCompress(new_file_list)
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
	print >> sys.stderr, "Exam  : %s SECT FileUnCompressor.conf" % module
	sys.exit()


if __name__ == "__main__" :

	if (len(sys.argv) < 3) : usage()

	fileMon = FileUnCompressor(sys.argv[1], sys.argv[2])
	fileMon.MakeIdxFile()
	fileMon.run()

	__LOG__.Trace("PROCESS END...\n")
	#sys.stderr.write("LOG://PROCESS END...\n")



