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
import re

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
signal.signal(signal.SIGINT, shutdown)  # sigNum  2 : Keyboard Interrupt
signal.signal(signal.SIGHUP, shutdown)  # sigNum  1 : Hangup detected
signal.signal(signal.SIGPIPE, shutdown) # sigNum 13 : Broken Pipe

'''
	On Windows, signal() can only be called with
	SIGABRT, SIGFPE,SIGILL, SIGINT, SIGSEGV, or SIGTERM.
	A ValueError will be raised in any other case.
'''

#- def global setting ----------------------------------------------------

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

class DataBaseObservation:

	def __init__(self, module, conf, section) :

		self.process_name = module
		#open
		__LOG__.Trace("__init__")

		#ref conf
		try : self.out_data_sep = conf.get("GENERAL","OUT_DATA_SEP")
		except Exception as e: raise Exception(e)
		#get connection
		try : self.pgsql_connection_name = conf.get("GENERAL","PGSQL_CLASS")
		except : raise Exception("Need POSTGRESQL_CONNECTION in conf for Database Connect")

		try : self.iris_connection_name = conf.get("GENERAL","IRIS_CLASS")
		except : raise Exception("Need IRIS_CONNECTION in conf for Database Connect")

		#get table code
		try : self.table_code_list = conf.get(section,"OBSERVE_TABLE_CODE").split(',')
		except : raise Exception("Need OBSERVE_TABLE_CODE in conf for Observing")

		#get check_time
		try : self.check_time_list = [ int(x) for x in conf.get(section,"CHECK_TIME").split(',') ]
		except : __LOG__.Exception()

	def __del__(self):
		#close
		__LOG__.Trace("__del__")

	#업데이트 타임이 있는지 확인
	def ObserveUpdate(self,table_name):
			#sql
			sql='''
			SELECT 1 from {tableName} where update_time <> ''
			'''.format(tableName = table_name)
			try :
				result = self.cur.execute_iter(sql)

				if result : return True

				return False
			except Exception as e:
				__LOG__.Trace(e)
				return False

	def ObserveUpdateIRIS(self,table_name):

		result = False

		sql = '''
		SELECT 1 FROM {tableName} where update_time <> ''  ;
		'''.format(tableName = table_name)

		try :

			result_list = [line for line in self.iris_cur.execute_iter(sql)]

			if result_list : result = True

		except:
			__LOG__.Exception()

		return result

	def connectIRIS(self):
		self.iris_cur = iris.Client(self.iris_connection_name)
		self.iris_cur.setSep(self.out_data_sep,"\n")
		__LOG__.Trace("IRIS Connect!!")

#	def loggingInit(self, process_type):
#
#		self.center_name 		= '중계서버'
#		self.center_id			= 'GW'
#
#		self.process_type 		= process_type
#
#		self.in_file_size 		= ''
#		self.in_file_row_cnt 	= ''
#
#		self.std_in 			= ''
#
#		self.out_file_size 		= ''
#		self.out_file_row_cnt 	= ''
#
#		self.table_info 		= ''
#		self.key_info 			= ''
#		self.partition_info		= ''
#		self.result_flag		= ''
#		self.success_cnt		= ''
#		self.fail_reason		= ''
#		self.header_cnt			= ''
#		self.comp_row_cnt		= ''
#		self.error_column_length	= ''
#		self.error_check_notnull	= ''
#		self.error_check_type_legth	= ''
#		self.error_check_format		= ''
#		self.error_change_cont		= ''
#		
#
#
#
#	def logging(self):
#
#		#center_name, center_id, process_name, process_type, start_time, end_time, duration, in_file_size,
#		#out_file_size, in_file_row_cnt, out_file_row_cnt, table_info, key_info, partition_info, result_flag,
#		#success_cnt, fail_reason, '', '', '', '', '', '', ''
#
#		#start_time = self.cur_time[:8]+'000000'
#		start_time = self.cur_time
#
#		end_time = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
#		#end_time = datetime.datetime.now().strftime("%Y%m%d") + '000000'
#
#
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

	def processing(self) :
		
		__LOG__.Trace( "processing : %s" % self.process_name  )
		
		#DB connection !!
		self.cur = pg.Postgres_py3(self.pgsql_connection_name)
		__LOG__.Trace("PGSQL Connect!!")

		#IRIS connection !!
		self.connectIRIS()
		#업데이트 체크 여부 리스트

		for idx_code,code in enumerate(self.table_code_list):

			#테이블이름 생성
			__LOG__.Trace("SECTION : {}".format(code))
			table_name = "CENTER_STANDARDIZATION_%s_DEFINITION"%code.strip()

			#셀렉트 결과 (True,False)
			db_result = self.ObserveUpdate(table_name)
			iris_result = self.ObserveUpdateIRIS(table_name)
			__LOG__.Trace("pgsql result : %s, iris result : %s"%(db_result,iris_result))

			std_out_msg = None

			if db_result and iris_result : 
				__LOG__.Trace("Update is detected in both, table_name : %s"%table_name)
				std_out_msg = "BOTH://UPDATED>>%s" % table_name

			elif db_result :
				__LOG__.Trace("Update is detected in postgres, table_name : %s"%table_name)
				std_out_msg = "PGSQL://UPDATED>>%s" % table_name

			elif iris_result :
				__LOG__.Trace("Update is detected in iris, table_name : %s"%table_name)
				std_out_msg = "IRIS://UPDATED>>%s" % table_name	

			else :
				__LOG__.Trace("Update is not detected, table_name : %s"%table_name)
			

			if db_result or iris_result :
				stdout(std_out_msg)

#				self.std_out = std_out_msg

#				self.logging()
	
	def run(self):

		__LOG__.Trace("run")

		__LOG__.Trace("Set check Hour = %s" % self.check_time_list)

		prev_time = None
		log_cnt = 0

		while not SHUTDOWN :

			self.cur_time = datetime.datetime.now().strftime("%Y%m%d%H%M%S")

#			self.loggingInit("일반모듈")		

			#현재 시간 받아오기 
			hour_now = int(datetime.datetime.now().hour)

			if log_cnt >= 60:

				__LOG__.Trace("Observer is Running : %d" % hour_now)

				log_cnt = 0

			#해당 시간에만 Processing
			if prev_time != hour_now:

				if hour_now in self.check_time_list :

					__LOG__.Trace("current hour : %d" % hour_now)

					try:
						stime = time.time()
						self.processing()
						prev_time = hour_now
						etime = time.time()

						__LOG__.Trace( 'Duration %s sec' % ( etime - stime ) )

					except:
						if not SHUTDOWN : __LOG__.Exception()

			log_cnt += 1
			time.sleep(60) #1 min


		
#- main function ----------------------------------------------------
def main():

	module = os.path.basename(sys.argv[0])

	if len(sys.argv) < 3:
		sys.stderr.write('Usage 	: %s section conf {option:[[log_arg]-d]}\n' % module )
		sys.stderr.write('Usage 	: %s section conf {option:[[log_arg]-d]}\n' % module )
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

	DataBaseObservation(module, conf, section).run()

	__LOG__.Trace('============= %s END [pid:%s]====================' % (module, pid ))


#- if name start ----------------------------------------------
if __name__ == "__main__" :
	main()


