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

#import $MOBIGEN_LIB$
import Mobigen.Common.Log_PY3 as Log; Log.Init()
import Mobigen.Database.Postgres_py3 as pg
import Mobigen.Database.iris_py3 as iris
import Mobigen.Utils.LogClient as logClient

#import $PROJECT_LIB$

#- shutdown  ----------------------------------------------------
SHUTDOWN = False

def shutdown(signalnum, handler):
	global SHUTDOWN
	SHUTDOWN = True
	sys.stderr.write('Catch Signal: %s \n' % signalnum)
	sys.stderr.flush()

signal.signal(signal.SIGTERM, shutdown) # sigNum 15 : Terminate
signal.signal(signal.SIGINT, shutdown)	# sigNum  2 : Keyboard Interrupt
signal.signal(signal.SIGHUP, shutdown)	# sigNum  1 : Hangup detected
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

class LoadDat:

	def __init__(self, module, conf, section) :
		#open
		__LOG__.Trace("__init__")

		self.process_name = module

		#sep
		try : self.out_data_sep = conf.get("GENERAL", 'OUT_DATA_SEP')
		except : self.out_data_sep = '^'
		
		try : self.conf_dat_path = conf.get("GENERAL", "SAVE_DAT_DIR")
		except : raise Exception("Require DAT_PATH for writing")

		#Pgsql Connection Name
		try : self.conf_pgsql_conn_name = conf.get("GENERAL","PGSQL_CLASS")
		except : raise Exception("Require POSTGRES_CLASS for Loading")

		#IRIS connection Name
		try : self.conf_iris_conn_name = conf.get("GENERAL","IRIS_CLASS")
		except : raise Exception("Require IRIS_CLASS for Loading")

		try : self.conf_ctl_path = conf.get("GENERAL","IRIS_CTL_PATH")
		except Exception as e : raise Exception("error : conf read error : %s"%e)

		try : self.conf_iris_path = conf.get("GENERAL","IRIS_DAT_PATH")
		except Exception as e : raise Exception("error : conf read error : %s"%e)

		self.cur_time = datetime.datetime.now().strftime("%Y%m%d%H%M%S")

	def __del__(self):
		#close
		__LOG__.Trace("__del__")

	#IRIS DB 연결 후 커서 생성
	def connectIRIS(self):

		try:

			self.iris_cur = iris.Client(self.conf_iris_conn_name)
			self.iris_cur.setSep(self.out_data_sep,"\n")

		except : __LOG__.Exception()

	#데이터 읽고 table name 추출
	def readDat(self, in_file):

		data_list = []

		#table_name = "_".join(in_file.strip().split("_")[0:4])
		table_name = os.path.basename(in_file).rsplit("_",1)[0]

		__LOG__.Trace("Table Name : %s"%table_name)

		with open(in_file, 'r') as f:

			data_list = f.readlines()

			__LOG__.Trace("read file : %s"%in_file)

		return table_name,data_list

	#커녁션 연결
	def connectPgsql(self):

		try : self.cur = pg.Postgres_py3(self.conf_pgsql_conn_name)

		except : __LOG__.Exception()

	
	#for iris
	def splitHeader(self, table_name, filePath):
	
		path_ctl = self.conf_ctl_path + "/" + table_name + ".ctl"

		path_dat = self.conf_iris_path + "/" + table_name + "_" + self.cur_time + ".dat"

		if not os.path.exists(filePath) :
			return None, None

		dataList	= list()

		if not os.path.exists(filePath) :
			return ctlFile, datFile

		with open(filePath, 'r') as f :
			dataList = f.readlines()

		__LOG__.Trace("Read Data for IRIS : %s" % filePath)

		dataList.pop(0)

		with open(path_dat, 'w') as df :
			df.write( ''.join(dataList) )

		__LOG__.Trace("Write dat file for IRIS : %s" % path_dat)

		return path_ctl, path_dat


		# update _ 20201212 Park
	def loadIRIS(self, table_name, dat_file,ctl_file):

#		self.loggingInit("IRIS로딩")

		result = None

		self.load_some_success_flag = False
		self.load_result_flag = False
		self.result_flag = False

		try :

			if os.path.isfile(ctl_file) and os.path.isfile(dat_file) :

				self.partition_info = self.cur_time

				self.key_info = '0'

				result = self.iris_cur.load(table_name, self.key_info ,\
				self.partition_info , ctl_file, dat_file)

			if '+OK' in result : 
				
				if 'SOME SUCCESS' in result :

					self.load_some_succes_flag = True

					__LOG__.Trace('IRIS Load Fail : SOME SUCCESS')

					self.fail_reason = result

				else : 

					__LOG__.Trace('IRIS Load Success')

					self.success_cnt = result.rsplit(":",1)[1].strip()

					self.load_result_flag = True
					self.result_flag = True

			else : 

				__LOG__.Trace('IRIS Load Fail')

				self.fail_reason = result

			__LOG__.Trace(result)

		except : __LOG__.Exception()

		if self.load_some_success_flag or self.load_result_flag:

			try : self.deletePastIRIS(table_name, self.load_some_success_flag )

			except : __LOG__.Exception()

#		self.logging()
		
		#Logging
#	def logging(self):
#
#		#center_name, center_id, process_name, process_type, start_time, end_time, duration, in_file_size,
#		#out_file_size, in_file_row_cnt, out_file_row_cnt, table_info, key_info, partition_info, result_flag,
#		#success_cnt, fail_reason, '', '', '', '', '', '', ''
#
#		#start_time = self.cur_time[:8]+'000000'
#		start_time = self.cur_time
#
#		#end_time = datetime.datetime.now().strftime("%Y%m%d") + '000000'
#		end_time = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
#
#		msg = '|^|'.join(map(str,[self.center_name, self.center_id, self.process_name,\
#		self.process_type, start_time, end_time, self.std_in, self.std_out,\
#		self.in_file_size, self.out_file_size, self.in_file_row_cnt, self.out_file_row_cnt,\
#		self.table_info, self.key_info, self.partition_info, self.result_flag, self.success_cnt, self.fail_reason,\
#		self.header_cnt, self.comp_row_cnt, self.error_column_length, self.error_check_notnull, self.error_check_type_legth,\
#		self.error_check_format, self.error_change_cont]))
#
#		logClient.irisLogClient().log(msg)
#		__LOG__.Trace("Send Log Socket : %s" % msg )
#
#
#	def loggingInit(self, process_type):
#
#		self.center_name			= '중계서버'
#		self.center_id		  		= 'GW'
#		
#		self.process_type 			= process_type
#		self.std_out 				= ''
#		self.out_file_size 			= ''
#		self.out_file_row_cnt		= ''
#
#		self.fail_reason 			= ''
#		self.result_flag			= False
#		self.load_result_flag 		= False
#		self.success_cnt 			= '0'
#		self.partition_info 		= ''
#		self.key_info 				= ''
#
#		self.header_cnt		 		= ''
#		self.comp_row_cnt	   		= ''
#		self.error_column_length	= ''
#		self.error_check_notnull	= ''
#		self.error_check_type_legth = ''
#		self.error_check_format	 	= ''
#		self.error_change_cont	  	= ''




	#list를 Pgsql로 Load
	def loadPostgres(self,table_name,in_file):

#		self.loggingInit("POSTGRES로딩")

		self.load_result_flag = False
		self.result_flag = False

		result = None

		try :

			self.cur.setSep(self.out_data_sep)

			result = self.cur.load(table_name,in_file)

			if 'Success' in result :

				__LOG__.Trace("PGSQL Load Success")

				self.success_cnt = result.rsplit(":",1)[1].strip()

				self.load_result_flag = True

				self.result_flag = True

				self.cur.commit()

			else :
				
				__LOG__.Trace("PGSQL Load Failed")

				self.fail_reason = result

				self.cur.rollback()

			__LOG__.Trace(result)

		except :
			__LOG__.Exception()

		
		#Past Delete
	
		try :

			self.deletePastPgsql(table_name)

		except :

			__LOG__.Exception()

#		self.logging()
		
	#pgsql Past Delete
	def deletePastPgsql(self, table_name):

		selectSql = '''
		SELECT max(create_time) FROM {tableName}
		'''.format(tableName = table_name)

		max_time = self.cur.execute_iter(selectSql)[0][0]

		if not max_time:

			return False

		countSql = '''
		SELECT count(*) FROM {tableName}
		'''.format(tableName = table_name)

		__LOG__.Trace("AS-IS Delete sql in POSTGRES.%s Total Count = %s"%(table_name, self.cur.execute_iter(countSql)[0][0]))

		deleteSql = '''
		DELETE FROM {tableName} WHERE CAST(NULLIF(create_time,'') AS DOUBLE PRECISION)
		< CAST({maxTime} AS DOUBLE PRECISION) or NULLIF(create_time,'') is null
		'''.format(tableName = table_name,maxTime = max_time)

		del_result = self.cur.execute(deleteSql)

		__LOG__.Trace("Delete Past Data : %s"%del_result)

		self.cur.commit()

		__LOG__.Trace("TO-BE Delete sql in  POSTGRES.%s Total Count = %s"%(table_name, self.cur.execute_iter(countSql)[0][0]))

		# iris del
	def deletePastIRIS(self, table_name, some_success_flag ):

		selectSQL = '''
		SELECT max(create_time) FROM {tableName};
		'''.format(tableName = table_name)

		result = self.iris_cur.execute_iter(selectSQL)

		ctime = next(result)[0]

		if not ctime : return False
		
		tmSQL = '''
		SELECT count(*) FROM {tableName} ;
		'''.format(tableName = table_name)

		__LOG__.Trace("AS-IS Delete sql in  IRIS.%s Total Count = %s"%(table_name,next(self.iris_cur.execute_iter(tmSQL))))
		
		del_option = '<>'

		if some_success_flag :  		#True = Some success , False : success
			del_option = '='
		deleteSQL = '''
		DELETE FROM {tableName} WHERE create_time {option} {ctime} ;
		'''.format(tableName = table_name, option = del_option, ctime = ctime)

		self.iris_cur.execute(deleteSQL)

		__LOG__.Trace("TO-DB Delete sql in  IRIS.%s Total Count = %s"%(table_name,next(self.iris_cur.execute_iter(tmSQL))))


	def processing(self, in_file) :

		table_name = os.path.basename(in_file).rsplit("_",1)[0]

		self.table_info = table_name

		ctl_f,dat_f = self.splitHeader(table_name, in_file)

		self.connectIRIS()
		__LOG__.Trace("IRIS Connect")
		self.loadIRIS(table_name,dat_f,ctl_f)
		
		
		#self.connectPgsql()
		#__LOG__.Trace("PGSQL Connect")
		#self.loadPostgres(table_name,in_file)

	def run(self):

		while not SHUTDOWN :
						
			self.cur_time = datetime.datetime.now().strftime("%Y%m%d%H%M%S")

			std_in = None

			is_std_err = False

			try:

				std_in = sys.stdin.readline().strip()
				#file://FILENAME
				if not std_in :
					is_std_err = True
					continue

				__LOG__.Trace('STD	IN : %s' % std_in )

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
				
#				self.std_in = std_in
#				
#				self.in_file_size = os.path.getsize(in_file)
#
#				#self.in_file_row_cnt = int(subprocess.check_output(["wc","-l",in_file]).split()[0])
#				self.in_file_row_cnt   = subprocess.getstatusoutput('/usr/bin/wc -l %s' % in_file)[-1].split()[0]
			
				stime = time.time()

				self.processing(in_file)

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
		sys.stderr.write('Usage		: %s section conf {option:[[log_arg]-d]}\n' % module )
		sys.stderr.write('Usage		: %s section conf {option:[[log_arg]-d]}\n' % module )
		#python3 /home/test/Project_name/bin/py3/BaseModule.py SECTION /home/test/Project_name/conf/BaseModule.conf
		#python3 /home/test/Project_name/bin/py3/BaseModule.py SECTION /home/test/Project_name/conf/BaseModule.conf 0 
		#python3 /home/test/Project_name/bin/py3/BaseModule.py SECTION /home/test/Project_name/conf/BaseModule.conf -d
		sys.stderr.flush()
		os._exit(1)

	section	 = sys.argv[1]
	config_file = sys.argv[2]

	conf = ConfigParser.ConfigParser()
	conf.read(config_file)

	if '-d' not in sys.argv :

		etc_argv = sys.argv[3:]
		log_arg = ''

		if len(sys.argv[3:]) > 0 :
			log_arg = '_' + sys.argv[3]

		log_path = conf.get('GENERAL', 'LOG_PATH')

		makedirs( log_path )	

		log_file = os.path.join(log_path, '%s_%s%s.log' % (os.path.splitext(module)[0], section, log_arg ))
		Log.Init(Log.CRotatingLog(log_file, 10240000, 9))		

	else:
		Log.Init()		

	pid = os.getpid()	
	__LOG__.Trace('============= %s START [pid:%s]==================' % ( module, pid ))

	LoadDat(module, conf, section).run()

	__LOG__.Trace('============= %s END [pid:%s]====================' % (module, pid ))


#- if name start ----------------------------------------------
if __name__ == "__main__" :
	main()


