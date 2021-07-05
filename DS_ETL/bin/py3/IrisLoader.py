#!/bin/env python3
# -*- coding:utf8 -*-

import configparser as ConfigParser
import os
import shutil
import signal
import socket
import sys
import threading
import time

import Mobigen.API.M6_PY3 as M6

#import $MOBIGEN_LIB$
import Mobigen.Common.Log_PY3 as Log; Log.Init()
import Mobigen.Database.iris_py3 as iris

SHUTDOWN = False

def handler(sigNum, frame):
	global SHUTDOWN
	SHUTDOWN = True
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

def call_shutdown():
	__LOG__.Trace('!!! SHUTDOWN !!!')
	os._exit(1)

def makedirs( path ) :
	try :
		os.makedirs( path )
		__LOG__.Trace( path )
	except : pass

class IrisLoader():
	def __init__(self, cfg):
		self.cfg = cfg

		try:
			self.retry_cnt = cfg.getint("LOADER", "RETRY_CNT")
		except:
			self.retry_cnt = 3
		try:
			self.field_sep = cfg.get("LOADER", "FIELD_SEP").encode('utf-8').decode("unicode-escape")
		except:
			self.field_sep = "\x1c"
		try:
			self.record_sep = cfg.get("LOADER", "RECORD_SEP").encode('utf-8').decode("unicode-escape")
		except:
			self.record_sep = "\n"
		try:
			self.remove_flag = cfg.getboolean("LOADER", "REMOVE_FLAG")
		except:
			self.remove_flag = False
		try :
			self.ctl_file_path	= cfg.get("LOADER", "CTL_FILE_PATH")
		except :
			self.ctl_file_path	= ''
		try :
			self.iris_connect_type	= cfg.get("LOADER", socket.gethostname())
		except :
			self.iris_connect_type	= 'ntbdgw2'
		try :
			self.err_path = os.path.expanduser(cfg.get("LOADER", "ERROR_PATH"))
		except :
			self.err_path = ''

#		iris.Client(self.iris_connect_type)

	def file_name_chk(self, file_name):
		try:
			## yyyymmddHHMMSS_GameCode.dat : 20160328071200_sknights.dat
			fn = os.path.basename(file_name).rsplit("-", 3)

			if len(fn) < 3:
				__LOG__.Trace("""File Format Check : split "-" to 3 sections -> FileName-(Table)-(Key)-(Partition.dat)""")
				return False

			if len(os.path.splitext(fn[-1])[0]) != 14 or not os.path.splitext(fn[-1])[0].isdigit():
				__LOG__.Trace("File Format Check : 4 Section is yyyymmddHHMMSS Format -> FileName-Table-Key-(Partition.dat)")
				return False

		except :
			__LOG__.Exception()
			return False

		return True


	def load(self, file_name) :
		table, key, partition = os.path.splitext(os.path.basename(file_name))[0].rsplit("-", 3)[-3:]

		ctl_file	= os.path.join(self.ctl_file_path, table + '.ctl')

		if not os.path.exists(ctl_file) :
			return False, 'CTL File is not exists : %s' % ctl_file, 0

		table = table.lower()

		__LOG__.Trace("Loading Start [ Table : %s  Key : %s  Partition : %s ]" % (table, key, partition))

		try_count = 0

		while True :

			try_count += 1

			if try_count == self.retry_cnt : break

			try:
				irisClient = iris.Client(self.iris_connect_type)

				irisClient.setSep(self.field_sep, self.record_sep)
				ret = irisClient.load(table, key, partition, ctl_file, file_name).strip()

				__LOG__.Trace(ret)

				if "+OK SUCCESS" in ret:
					__LOG__.Trace("Load Succes : %s" % ( file_name ))

					if self.remove_flag :
						try:
							os.remove(file_name)
							__LOG__.Trace('File Remove : %s' % file_name)
						except:
							__LOG__.Exception("can not remove file : %s" % file_name)

					return True, 'SUCCESS', int(ret.split(":")[-1].strip())
				else :
					__LOG__.Trace(ret)

			except :
				__LOG__.Exception()

			finally :
				#iris.Client.Close()
				if irisClient.curs :
					irisClient.curs.Close()
				if irisClient.conn :
					irisClient.conn.close()
				__LOG__.Trace("IRIS M6 Connection Close OK")

			# 1 sec sleep의 경우 시간차가 짧은 것 같음
			__LOG__.Trace( 'Retry count %s and Sleep' % try_count )
			time.sleep(0.1)

		# 재시도 횟수 전체 실패한 경우 Error path로 이동
		err_path	= os.path.join(self.err_path, table.upper())
		makedirs( err_path )

		err_file = os.path.join(err_path, os.path.basename(file_name))

		try :
			shutil.move(file_name, err_file)
			__LOG__.Trace("Error file moved : %s" % err_file)
		except : pass

		return False, 'retry count %s' % self.retry_cnt, 0


	def processing(self, file_name):

		file_size = os.path.getsize(file_name)
		file_size_mb = '%.2f' % (float(file_size) / 1024 / 1024)

		st_time = time.time()

		is_success, fail_cause, load_cnt = self.load(file_name)

		process_time = '%.2f' % (time.time() - st_time)

		if is_success and load_cnt > 0 :
			__LOG__.Trace("Complete [ Time : %s  Size : %5s MB  Lines : %5s ] " % (process_time, file_size_mb, load_cnt))
		else :
			__LOG__.Trace("FAIL	 [ Time : %s  Size : %5s MB  Lines : %5s Fail Reason : %s ] " % (process_time, file_size_mb, load_cnt, fail_cause))

	def run(self):

		__LOG__.Trace( 'START process: ( pid:%d ) >>>>>>>>>>>>>>>>>>>>>>>>>>>>>' % (os.getpid()) )

		while not SHUTDOWN:

			is_std_error = False
			std_in = None
			try:
				std_in = sys.stdin.readline()

				if std_in.strip() == '' : 
					is_std_error = True
					raise Exception("input error, std_in == '' ")
	
				__LOG__.Trace('STD  IN : %s' % std_in)
	
				try :
					prefix, in_file = std_in.strip().split( '://', 1 )
				except :
					is_std_error = True
					raise Exception("Input format error")
	
				if prefix != 'file' :
					is_std_error = True
					raise Exception('Prefix not match %s' % prefix)
	
				if not self.file_name_chk(in_file):
					is_std_error = True
					raise Exception("Invalid File Naming Rule : %s" % in_file)
	
				if not os.path.exists(in_file):
					is_std_error = True
					raise Exception("File Not Exists : %s" % in_file)
	
				file_size = os.path.getsize(in_file)
				if file_size == 0 : 
					
					__LOG__.Trace("0 Byte File : %s" % in_file)
					try:
						os.remove(in_file)
					except: pass
	
					#sys.stdout.write('%s://%s\n' % (socket.gethostname(), in_file ))
					#sys.stdout.flush()
					## 0 byte이면 삭제  - Park Yun Tae
					#os.remove(in_file)
					#is_std_error = True
					#raise Exception("0 Byte File : %s" % in_file)

				else:
					self.processing(in_file)
				
				is_std_error = True
	
			except :
				__LOG__.Exception()
			finally :
				if is_std_error :
					sys.stderr.write(std_in)
					sys.stderr.flush()
					__LOG__.Trace('STD ERR : %s' % std_in.strip())

		__LOG__.Trace('IRIS LOADER END')


PROC_NAME = os.path.basename(sys.argv[0])

def main():

	if len(sys.argv) < 2:
		print ( "Usage   : %s LogName ConfigFile" % PROC_NAME )
		print ( "Example : %s OrgLoader ~/Columbus/conf/IrisLoader.conf" % PROC_NAME )
		sys.exit()

	cfgFile = sys.argv[2]

	conf = ConfigParser.ConfigParser()
	conf.read(cfgFile)

	if '-d' not in sys.argv :
		etc_argv = sys.argv[2:]
		log_arg = ''

		log_arg = sys.argv[1]

		log_path = conf.get('GENERAL', 'LOG_PATH')

		makedirs( log_path )

		log_file = os.path.join(log_path, '%s_%s.log' % (PROC_NAME, log_arg ))
		Log.Init(Log.CRotatingLog(log_file, 10240000, 9))
	
	else :
		Log.Init()

	pid = os.getpid()
	__LOG__.Trace('============= %s START [pid:%s]==================' % ( PROC_NAME, pid ))
	IrisLoader(conf).run()

	__LOG__.Trace('============= %s END [pid:%s]====================' % ( PROC_NAME, pid ))

if __name__ == "__main__":
	main()
