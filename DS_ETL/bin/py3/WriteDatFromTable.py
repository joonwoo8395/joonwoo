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
import numpy as np

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

	def __init__(self, module, conf, section) :

		self.process_name = module
		#open
		__LOG__.Trace("__init__")
		#sep
		try : self.out_data_sep = conf.get("GENERAL", 'OUT_DATA_SEP')
		except : self.out_data_sep = '^'
		
		try : self.conf_dat_path = conf.get("GENERAL", "SAVE_DAT_DIR")
		except : raise Exception("Require DAT_DIR for writing")

		try : self.conf_db_class = conf.get("GENERAL", "PGSQL_CLASS")
		except Exception as e : raise Exception("error : conf read error : %s"% e)

		try : self.conf_iris_class = conf.get("GENERAL", "IRIS_CLASS")
		except Exception as e : raise Exception("error : conf read error : %s"% e)

		try : self.conf_code_key = conf.get(section,"CODE_KEY").split(',')
		except Exception as e : raise Exception("error : conf read error : %s"% e )

		try : self.conf_column_key = conf.get(section,"COLUMN_KEY").split(',')
		except Exception as e : raise Exception("error : conf read error : %s"% e )

		try : self.conf_ctl_path = conf.get("GENERAL","IRIS_CTL_PATH")
		except Exception as e : raise Exception("error : conf read error : %s"%e)

		try : self.conf_iris_path = conf.get("GENERAL","IRIS_DAT_PATH")
		except Exception as e : raise Exception("error : conf read error : %s"%e)


		self.result_dict = {}
		self.key_index = []

	def __del__(self):
		#close
		__LOG__.Trace("__del__")
	
	#get header and key index
	def getColumns(self, table_name):

		header_list = []

#		with open (self.conf_header_path+'/'+table_name+'.header','r') as f:
#			header_list = f.read().replace('\n','').split("^")

		# os.path.join(dirname, filename)
		with open (self.conf_ctl_path + '/' + table_name + '.ctl','r') as f:
			header_list = [line for line in f.read().split("\n") if line]
		
		table_idf = table_name.split("_")[2]
		
		#gey key And header
		if table_idf == 'CODE':
			self.key_index =  [ header_list.index(key) for key in self.conf_code_key ]
		elif table_idf == 'COLUMN':
			self.key_index =  [ header_list.index(key) for key in self.conf_column_key ]
		else:
			raise Exception("Table Name is not match")
		
		__LOG__.Trace(self.key_index)
		
		return header_list

	#DB 연결 후 커서 생성
	def connectDB(self):
		try:
			self.cur = pg.Postgres_py3(self.conf_db_class)
		except Exception as e:
			__LOG__.Trace("Cannnot connect DB : %s"% e)

	#IRIS connect and get cursor
	def connectIRIS(self):
		try:
			self.iris_cur = iris.Client(self.conf_iris_class)
			self.iris_cur.setSep(self.out_data_sep,"\n")
		except Exception as e:
			__LOG__.Trace("Cannot connect DB : %s"% e)

	def getKeyDataStr(self, row):
		
		key = None
		data = None

		try :
			key = self.out_data_sep.join( map ( str , [ row[key] for key in self.key_index ] ))
			data = self.out_data_sep.join( map ( str , row ) )
		except :

			__LOG__.Watch(row)
			__LOG__.Exception()

		return key , data
	
	#IRIS Select
	def selectIRIS(self, table_name, column_list):

		self.connectIRIS()

		selectSQL = '''
		SELECT {columns} FROM {tableName} ;
		'''.format(columns = ','.join(column_list),tableName = table_name)
		
		cnt = 0

		for row in self.iris_cur.execute_iter(selectSQL):

			key, data = self.getKeyDataStr( row )
			
			self.result_dict.setdefault(key, []).append( data )	

		__LOG__.Trace("IRIS Select Complete : %s" % table_name)


	#PGSQL Select
	def selectPG(self,table_name, column_list):

		#cursor 생성
		self.connectDB()
		
		#테이블 데이터 쿼리
		sql = '''
		SELECT {columns} FROM {tableName}  
		'''.format(columns = ','.join(column_list),tableName=table_name)
		for row in self.cur.execute_iter(sql=sql):

			key, data = self.getKeyDataStr(row)

			self.result_dict.setdefault(key, []).append( data )

		__LOG__.Trace("Select Complete : %s"% table_name)


	# reset time columns
	def resetTimeColumn(self, row_str):
		
		row = row_str.split(self.out_data_sep)
		
		row[-2] = self.cur_time 									# CREATE_TIME
		row[-1] = ''												# UPDATE_TIME
		
		return self.out_data_sep.join(row)
		
	# combinate dict and write file
	def combinateAndWrite(self, table_name, column_list):

#		self.loggingInit("일반모듈")

		__LOG__.Trace("Combinate And Write Dict Start ")

		file_name = "%s_%s" % ( table_name.upper() , self.cur_time )

		file_path = os.path.join( self.conf_dat_path , file_name )			#real file path

		comb_dict = {}

		with open( file_path + '.tmp' , 'w' ) as f:

			f.write(self.out_data_sep.join(column_list) + "\n")

			row_cnt = 0

			for key in self.result_dict:									#start for

				row = None

				row_list = list(set(self.result_dict[key]))

				if len(row_list) == 1:
					
					row = self.resetTimeColumn( row_list[0] )


				elif len(row_list) == 2:

					u_time_list = []

					for one_row in row_list :

						u_time = one_row.split(self.out_data_sep)[-1]

						if u_time == '' or u_time == None : u_time = 0

						try : u_time = int(u_time)

						except :
							__LOG__.Exception()
						
						u_time_list.append(u_time)

					row_str = None

					if u_time_list[0] > u_time_list[1] : row_str = row_list[0]

					else : row_str = row_list[1]
					
					row = self.resetTimeColumn(row_str)

				else:

					raise Exception("same key is repeated 3 or more")
					__LOG__.Watch(row_list)
				

				row_list_spt = row.split(self.out_data_sep)

				comb_key, comb_data = self.getKeyDataStr(row_list_spt)
				comb_data = row

				comb_dict.setdefault(comb_key, comb_data)

			#end for
			#start for
			for c_key in comb_dict:
				
				w_row = None
				w_row = comb_dict[c_key]

				row_cnt += 1
				f.write(w_row + "\n")												
			#end for

#			self.out_file_row_cnt = row_cnt

		os.rename( file_path + '.tmp' , file_path + '.dat')

#		if os.path.exists(file_path + '.dat'):
#			self.out_file_size = os.path.getsize(file_path +  ".dat" )
#		else :
#			slef.out_file_size = 0

		__LOG__.Trace("Combinate And Write Dict Complete : %s" % ( file_path + '.dat' ))
		
		std_out_msg = "file://%s" % (file_path + '.dat')

		stdout(std_out_msg)

#		self.std_out = std_out_msg
#
#		self.logging()


#	def loggingInit(self, process_type):
#
#		self.center_name 		= '중계서버'
#		self.center_id			= 'GW'
#
#		self.process_type = process_type
#
#		self.in_file_size = ''
#	
#		self.in_file_row_cnt = ''
#
#		self.out_file_size = ''
#
#		self.out_file_row_cnt = ''
#
#		self.table_info		 = ''
#		self.key_info		   = ''
#		self.partition_info	 = ''
#		self.result_flag		= ''
#		self.success_cnt		= ''
#		self.fail_reason		= ''
#		self.header_cnt		 = ''
#		self.comp_row_cnt	   = ''
#		self.error_column_length	= ''
#		self.error_check_notnull	= ''
#		self.error_check_type_legth = ''
#		self.error_check_format	 = ''
#		self.error_change_cont	  = ''
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

	def processing(self, prefix, table_name) :

		__LOG__.Trace( "processing : %s" % table_name )

		header_list = self.getColumns(table_name)

		self.result_dict = {}

		#DB data get
		if prefix == 'IRIS':
			self.selectIRIS( table_name, header_list )

		elif prefix == 'PGSQL':
			self.selectPG( table_name, header_list )
		
		elif prefix == 'BOTH':
			self.selectIRIS( table_name, header_list )
			self.selectPG( table_name, header_list )
		
		self.combinateAndWrite( table_name, header_list )

	def run(self):

		while not SHUTDOWN :

			self.cur_time = datetime.datetime.now().strftime("%Y%m%d%H%M%S")

			std_in = None
			is_std_err = False

			try:

				std_in = sys.stdin.readline().strip()
				#BOTH://UPDATED>>%s
				#PGSQL://UPDATED>>%s
				#IRIS://UPDATED>>%s

				if not std_in :
					is_std_err = True
					continue

				__LOG__.Trace('STD  IN : %s' % std_in )

				try :
					prefix, line = std_in.split('://', 1)
				except :
					is_std_err = True
					__LOG__.Trace( 'Input format error : %s' % std_in )
					continue

				if prefix not in ["BOTH","PGSQL","IRIS"] :
					is_std_err = True
					__LOG__.Trace('Prefix is not match : %s' % prefix)
					continue	
				
				try :
					msg , table_name = line.split('>>')
				except :
					is_std_err = True
					__LOG__.Trace("Data format error : %s " % line)
					continue
			
				if msg != "UPDATED":
					is_std_err = True
					__LOG__.Trace("Message is not match : %s" % msg )
					continue

				stime = time.time()

#				self.std_in = std_in

				self.processing( prefix, table_name )

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
		sys.stderr.write('Usage 	: %s section conf {option:[[log_arg]-d]}\n' % module )
		sys.stderr.write('Usage 	: %s section conf {option:[[log_arg]-d]}\n' % module )
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

	WriteDat(module, conf, section).run()

	__LOG__.Trace('============= %s END [pid:%s]====================' % (module, pid ))


#- if name start ----------------------------------------------
if __name__ == "__main__" :
	main()


