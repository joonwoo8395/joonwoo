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
import glob
import configparser as ConfigParser
import zipfile
import tarfile
import shutil
import socket

#import $MOBIGEN_LIB$
import Mobigen.Common.Log_PY3 as Log; Log.Init()
import Mobigen.Utils.CodeFormat as CodeFormat

#import $PROJECT_LIB$
#import Mobigen.Utils.LogClient as SendLog

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

class ClassName:

	def __init__(self, conf) :

		section = 'UNZIP'
		self.centerId 	= conf.get('GENERAL', 'CENTER_ID')
		self.centerName	= conf.get('GENERAL', 'CENTER_NAME')
		#CENTER_ID 없을 경우 돌면 안됨으로 try;except를 이용한 default설정 삭제함
		#try :
		#	self.mergeYN = conf.getboolean(section, 'CSV_MERGE')
		#except :
		#	self.mergeYN = False
		
		try: 
			self.merge_dataset_list = conf.get(section, 'MERGE_DATASET').split(';')
		except: 
			self.merge_dataset_list = []

		try:
			self.original_delete_list = conf.get('GENERAL', 'ORIGINAL_DELETE').split(';')
		except :
			self.original_delete_list = []
		try :
			self.ex_flag	= conf.getboolean('GENERAL', 'EX_FLAG')
		except :
			self.ex_flag	= False

		self.logInit(self.centerName, self.centerId)

		self.sock = None
		self.host = socket.gethostname()
		self.port = conf.getint('GENERAL', 'LOG_PORT')

	def __del__(self):

		self.SockDisconn()

	def SockConn(self):

		if not self.sock :
			self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			self.sock.connect((self.host, self.port))

	def SockDisconn(self):

		if self.sock :
			self.sock.close()
		self.sock = None

	def logInit(self, centerName, centerId) :
		self.center_name	  		= centerName
		self.center_id		 		= centerId
		self.product_cd			 = ''
		self.product_nm			 = ''
		self.dataset_cd			 = ''
		self.min_period			 = ''
		self.max_period			 = ''
		self.start_time			 = ''
		self.ps_duration			= ''
		self.std_in				 = ''
		self.in_file_size		   = ''
		self.in_file_row_cnt		= ''
		self.result_flag			= ''
		self.success_cnt			= ''
		self.fail_type			  = ''
		self.fail_reason			= ''
		self.header_cnt			 = ''
		self.comp_row_cnt		   = ''
		self.error_column_length	= ''
		self.error_check_notnull	= ''
		self.error_check_type_legth = ''
		self.error_check_format	 = ''
		self.error_change_cont	  = ''
		self.error_data_range	   = ''

	def processing(self, in_file) :

		__LOG__.Trace("{} File unzip Start".format(in_file))

		dirs, names = os.path.split( in_file )
		
		unzip_dir	= os.path.join(dirs, names.split('.')[0])
#		unzip_dir	= os.path.join(dirs, os.path.splitext(names)[0])
#		unzip_dir	= os.path.join(dirs, names.rsplit('.', 3)[0])
#		unzip_ref_file = in_file + '.ref'

		cf_merge_flag = False
		for merge_dataset in self.merge_dataset_list:
			if merge_dataset.lower() in in_file.lower():
				cf = CodeFormat.CodeFormat(self.centerId + '_MERGE')
				cf_merge_flag = True

		if not cf_merge_flag:
			cf = CodeFormat.CodeFormat(self.centerId)

		dataSetCode = None

		# 압축방식에 따라서 list 반환 형태 변경
		zipFileList	 = list()
		errCheckList = list()

		if in_file.upper().endswith('ZIP') :
			zip_file = zipfile.ZipFile( in_file )
			zipFileList 	= zip_file.infolist()
			for oneFile in zipFileList :
				errCheckList.append(oneFile.filename)

		elif in_file.upper().endswith('TAR') :
			zip_file = tarfile.open( in_file, 'r')
			zipFileList 	= zip_file.getnames()
			errCheckList	= zipFileList.copy()

		elif in_file.upper().endswith('TAR.GZ') :
			zip_file = tarfile.open( in_file, 'r:gz')
			zipFileList 	= zip_file.getnames()
			errCheckList	= zipFileList.copy()

		elif in_file.upper().endswith('CSV') :
#			unzip_dir	= os.path.join(dirs, names)
			zipFileList.append(os.path.basename(in_file))
			errCheckList	= zipFileList.copy()

		__LOG__.Trace('zip file : {}'.format(errCheckList))

		errZipFileList = [oneZipFile for oneZipFile in errCheckList if (oneZipFile.upper().endswith('ZIP') or oneZipFile.upper().endswith('TAR') or oneZipFile.upper().endswith('TAR.GZ')) ]

		if len(errZipFileList) > 0 :
			raise TypeError('%s | !!! EXT Error !!!' % errZipFileList )

		excFileName	= None

		## excFileName : unzip Path 를 키로 잡는다.
		if self.ex_flag :
			excFileName = os.path.join(unzip_dir, 'extraFile')
			makedirs(excFileName)

		self.mergeYN = False 
		for merge_dataset in self.merge_dataset_list:
			if merge_dataset.lower() in in_file.lower():

				zipFileDict 	= cf.extract_path_variable(in_file)
				__LOG__.Trace(zipFileDict)
				dataSetCode		= zipFileDict['datasetCode']
				mergeCsvName	= os.path.join(unzip_dir, '%s:Merge_%s.csv' % (zipFileDict['datasetCode'], zipFileDict['datasetCode']) )
				makedirs(unzip_dir)
				mergeCsvFile = open(mergeCsvName, 'ab')
				self.mergeYN = True

#		with open(unzip_ref_file, 'w') as fd:
		
#		zip_file = zipfile.ZipFile( in_file )

#		for name in zip_file.infolist():

		stdOutCsvList 		= []
		stdOutExList 		= []

		for name in zipFileList:
			if in_file.upper().endswith('ZIP') :
				unziped_file 	= zip_file.extract(name, unzip_dir )

			elif in_file.upper().endswith('TAR') or in_file.upper().endswith('TAR.GZ') :
				unziped_file	= os.path.join(unzip_dir, str(name))
				makedirs(unzip_dir)
				zip_file.extract(name, unzip_dir )

			elif in_file.upper().endswith('CSV') :
				unziped_file	= os.path.join(unzip_dir, str(name))
				makedirs(unzip_dir)
				shutil.copy(in_file, unziped_file)
				
			# data file Encoding setting
			#__LOG__.Trace('file -bi %s' % unziped_file)
			check_res = subprocess.check_output('file -bi %s' % unziped_file, shell=True)

			key = None
			value = None

			for attr in str(check_res).strip().split(';'):
				try :
					key, value = attr.strip().split('=')
				except : pass
			if '.'in unziped_file :
				unzipFile	= unziped_file.rsplit('.', 1)[0] + '_utf-8' + unziped_file.rsplit('.', 1)[1]

			### 파일이 아닌 dir 일때, pass
			else :
				continue

			if 'iso-8859-1'.lower() in value.lower() :
				iconv_res = subprocess.check_output('iconv -c -f euc-kr -t utf-8 %s > %s' % (unziped_file, unzipFile ), shell=True)
				os.remove(unziped_file)
				os.rename(unzipFile, unziped_file)
				__LOG__.Trace('euc-kr >> utf-8 || %s' % unziped_file)
			
			__LOG__.Trace('unzip File name : %s' %unziped_file)

			if not self.mergeYN :
				unzipFileDict 	= cf.extract_path_variable(unziped_file)
				__LOG__.Trace(unzipFileDict)

				if 'datasetCode' in unzipFileDict :
					dataSetCode = unzipFileDict['datasetCode']

				unzipFileDir, unzipedFileName = os.path.split(unziped_file)
				newFileName = '%s:%s' %(dataSetCode, unzipedFileName)

				os.rename( unziped_file, os.path.join(unzipFileDir, newFileName) )
				__LOG__.Trace('File rename : {} >> {}'.format(unziped_file, os.path.join(unzipFileDir, newFileName)))
				newUnzipFile = os.path.join(unzipFileDir, newFileName)
				ext = unziped_file.rsplit('.',1)[-1].lower()
				if 'csv' == ext :
					stdOutCsvList.append('%s://%s' % ( ext, newUnzipFile ))
#					stdout('%s://%s' % ( ext, newUnzipFile ))
#					self.logSend(newUnzipFile)
				elif 'xlsx' == ext or 'rdf' == ext or 'xls' == ext :
					__LOG__.Trace('ext : %s 제외' % ext )

				else :
					if self.ex_flag :
						stdOutExList.append(newUnzipFile)
					else :
						__LOG__.Trace('csv File only processing [%s]' % newUnzipFile )

				__LOG__.Trace( "rename File : %s" %newUnzipFile )

			else :
				ext = unziped_file.rsplit('.',1)[-1].lower()
				if ext == 'csv' :
					with open(unziped_file, 'rb') as readFile :
						shutil.copyfileobj(readFile, mergeCsvFile)

					if len(self.original_delete_list) != 0 :
						for obj_file in self.original_delete_list:
							if obj_file.upper() in unziped_file.upper():
								__LOG__.Trace("unzip file for Merge remove after Merged: %s" % unziped_file)
								os.remove(unziped_file)
#				elif 'xlsx' == ext or 'rdf' == ext or 'xls' == ext :
#					__LOG__.Trace('ext : %s 제외' % ext )
#				else :
#					if self.ex_flag :
#						stdOutExList.append(newUnzipFile)
#					else :
#						__LOG__.Trace('csv File only processing [%s]' % newUnzipFile )


		if self.mergeYN :
			ext = mergeCsvName.rsplit('.', 1)[-1].lower()
			mergeCsvFile.close()

			if 'csv' == ext :
				stdout('%s://%s' % (ext, mergeCsvName))
#				self.logSend(mergeCsvName)
		else :
			for stdoutCsv in stdOutCsvList :
				stdout(stdoutCsv)

		extraPrefix = None

		if self.ex_flag :
			if len(stdOutCsvList) == 0 :
				extraPrefix = 'EXC_FALSE'
				__LOG__.Trace('csv File Not Exists : %s' % extraPrefix)
			else :
				extraPrefix = 'EXC_TRUE'
				__LOG__.Trace('csv File Exists : %s' % extraPrefix)

			with open( os.path.join(excFileName, '%s.txt' % os.path.splitext(names)[0]), 'w') as excf :
				for stdoutEx in stdOutExList :
					excf.write(stdoutEx + '\n')
			if len(stdOutExList) != 0 :
				stdout('%s://%s' % ( extraPrefix, os.path.join(excFileName, '%s.txt' % os.path.splitext(names)[0])))

#		os.rename( unzip_ref_file, os.path.join(os.path.dirname(unzip_ref_file), '%s:%s' % (dataSetCode, os.path.basename(unzip_ref_file)) ) )

#		new_ref_file = os.path.join(os.path.dirname(unzip_ref_file), '%s:%s' % (dataSetCode, os.path.basename(unzip_ref_file)) )
#		stdout('ref://%s' % new_ref_file)
#		stdout('ref://%s' % unzip_ref_file)
		__LOG__.Trace('end unzip')

	def logSend(self, in_file, fail_type) :
		self.std_in			= in_file
		self.result_flag	= 'FA'
		self.fail_type		= fail_type
		self.fail_reason	= os.path.basename(in_file)

		sendLogData		= '|^|'.join(map(str, [
									self.center_name
									,self.center_id
									,self.product_cd
									,self.product_nm
									,self.dataset_cd
									,self.min_period
									,self.max_period
									,self.start_time
									,self.ps_duration
									,self.std_in
									,self.in_file_size
									,self.in_file_row_cnt
									,self.result_flag
									,self.success_cnt
									,self.fail_type
									,self.fail_reason
									,self.header_cnt
									,self.comp_row_cnt
									,self.error_column_length
									,self.error_check_notnull
									,self.error_check_type_legth
									,self.error_check_format
									,self.error_change_cont
									,self.error_data_range ] ))

		sockline = "PS_LOG://{}\n".format(sendLogData)
		#__LOG__.Trace('PS_LOG DATA Load [%s]' % str(sendLogData))
		#SendLog.irisLogClient().log("PS_LOG://{}\n".format(sendLogData))
		__LOG__.Trace(sockline)
		self.SockConn()
		self.sock.send(sockline.encode())

	def fileMove(self, in_file, prePath ,nwPath, MOVE_COMP_FLAG) :
		if MOVE_COMP_FLAG :
			return True
		try :
			origin_path = in_file.replace(prePath, nwPath)
			origin_path_dir = os.path.dirname(origin_path)
			makedirs(origin_path_dir)
			shutil.move(in_file, origin_path)
			__LOG__.Trace("shutil.move : %s -> %s" % (in_file, origin_path))
		except :
			__LOG__.Exception()
			return False

		return True

	def run(self):

		while not SHUTDOWN :
			self.start_time		 = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
			std_in = None
			is_std_err = False

			try:

				std_in = sys.stdin.readline().strip()

				if not std_in :
					is_std_err = True
					continue

				__LOG__.Trace('STD  IN : %s' % std_in )

				try :
					prefix, in_file = std_in.split('://', 1)
				except :
					is_std_err = True
					__LOG__.Trace( 'Input format error : %s' % std_in )
					continue

				if prefix != 'file' :
					is_std_err = True
					__LOG__.Trace('Prefix is not match : %s' % prefix)
					continue	
				
				if not os.path.exists( in_file )  :
					is_std_err = True
					__LOG__.Trace('Not found file : %s' % in_file)
					continue
				
#				if not ".zip" in in_file:
#					is_std_err = True
#					__LOG__.Trace('Not a .zip file : %s' % in_file)
#					continue

				stime = time.time()

#				self.std_in			 = in_file
#				self.in_file_size	   = str(os.path.getsize(in_file))
				FLAG 			= False
#				DISK_FLAG 		= False
				MOVE_COMP_FLAG	= False

#				while not DISK_FLAG :
#					## df -h /data 의 파티션 크기 비교
#					#use_per = int(' '.join(subprocess.check_output('df -h | grep %s' % self.data_partition , shell =True).decode('utf-8').split('   ').split()[4].replace('%', '')))
#					disk_used = shutil.disk_usage(self.data_partition)
#					## 사용중인 디스크 / 총 디스크
#					use_per = int(disk_used.used/disk_used.total * 100)
#					#use_per = disk_used.free/1024/1024/1024   ###GB
#					## 2021.01.13 percentage 사용 안하고 남은 용량 GB 로 변경 -> 사용X
#
#					if self.DISK_FLAG_PER > use_per :
#						DISK_FLAG = True
#
#					else :
#						#__LOG__.Trace('Disk free : %s GB | time sleep 60' % str(use_per) )
#						__LOG__.Trace('Disk used : %s 퍼센트 | time sleep 60' % str(use_per) )
#						time.sleep(60)
				try :
					self.processing(in_file)
				except TypeError as e :
					if '!!! EXT Error !!!' in str(e) :	
						MOVE_COMP_FLAG = self.fileMove(in_file, '/DATA/DATA_COMP/' , '/DATA/DATA_ERROR/', MOVE_COMP_FLAG)
						self.logSend(in_file, '압축규격 미매칭')
				except :
					MOVE_COMP_FLAG = self.fileMove(in_file, 'DATA/DATA_COMP/', '/DATA/DATA_ERROR/', MOVE_COMP_FLAG)
					self.logSend(in_file, '압축풀기 에러')

				try :
					if len(self.original_delete_list) != 0 :
						for obj_file in self.original_delete_list:
							if obj_file.upper() in in_file.upper():
								__LOG__.Trace("original file remove : %s" % in_file)
								os.remove(in_file)
					else:
						MOVE_COMP_FLAG = self.fileMove(in_file, '/DATA/DATA_COMP/' , '/DATA/DATA_ORIGIN/', MOVE_COMP_FLAG)
					
				except :
					MOVE_COMP_FLAG = self.fileMove(in_file, '/DATA/DATA_COMP/' , '/DATA/DATA_ERROR/', MOVE_COMP_FLAG)
					__LOG__.Trace("!!! Error ORIGIN MOVE Fail !!!")

				etime = time.time()

				__LOG__.Trace( 'Duration %s sec' % ( etime - stime ) )

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


