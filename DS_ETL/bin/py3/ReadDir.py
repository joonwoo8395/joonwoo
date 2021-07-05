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
import Mobigen.API.M6_PY3 as M6
import Mobigen.Utils.LogClient as logClient

#import $PROJECT_LIB$

#import pandas,numpy
import pandas as pd
import numpy as np

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

def makedirs(path) :

	try :
		os.makedirs(path)
		__LOG__.Trace( path )
	except : pass

#- Class ----------------------------------------------------
class DirObservation:

	def __init__(self, module, conf, section) :
		#open
		__LOG__.Trace("__init__")
		pd.options.display.float_format = '{:.f}'.format

		#sheet
		try : self.conf_sheet_names = conf.get(section, 'SHEET_NAMES')
		except : self.conf_sheet_names = ''
		
		#sep
		try : self.conf_out_sep = conf.get("GENERAL","OUT_DATA_SEP")
		except : self.conf_out_sep = '^'
		
		#sheet out name
		try : self.conf_sheet_out_names = conf.get(section,"SHEET_OUT_NAMES")
		except : self.conf_sheet_out_names = 'COLUMN,CODE,HDONGCODE,HBDONGCODE'
		
		#save dir
		try : self.conf_save_dir = conf.get("GENERAL","SAVE_DAT_DIR")
		except : self.conf_save_dir = ""
		else : makedirs(self.conf_save_dir) 
		
		#pgsql table sql
		try : self.conf_table_sql_path = conf.get(section,"TABLE_SQL_PATH")
		except : self.conf_table_sql_path = ''
		
		#Pgsql info
		try : self.conf_psql_connection = conf.get("GENERAL","PGSQL_CLASS")
		except : raise Exception("Please check configure PGSQL_CLASS")

		#IRIS info
		try : self.conf_iris_conn = conf.get("GENERAL","IRIS_CLASS")
		except : raise Exception("Please check configure IRIS_CLASS")

		#time init
		#self.cur_time = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
		
		self.table_name_list = ["CENTER_STANDARDIZATION_CODE_DEFINITION","CENTER_STANDARDIZATION_COLUMN_DEFINITION"]
		#CENTERS
		try : self.conf_centers = conf.get(section,"CENTERS")
		except : self.conf_centers = 'ALL'

		#CTL PATH
		try : self.conf_ctl_path = conf.get("GENERAL","IRIS_CTL_PATH")
		except : raise Exception("Cannot read conf")

		#IRIS(DAT) PATH
		try : 
			self.conf_iris_path = conf.get("GENERAL","IRIS_DAT_PATH")
			makedirs(self.conf_iris_path)
		except : raise Exception("Cannot read conf")




	def __del__(self):
		#close
		__LOG__.Trace("__del__")
	
	def getSQL(self, table_name):

		table_sql_file = table_name+".sql"

		sql_file = os.path.join(self.conf_table_sql_path,table_sql_file)

		sql = None

		with open(sql_file,'r') as f:
			sql = f.read()

		__LOG__.Trace(sql)

		return sql

	#테이블 생성
	def createTable(self, table_name):

		if not table_name :	raise Exception("Need Table Name for create")
		
		__LOG__.Trace(table_name)

		try : sql = self.getSQL(table_name+"_pgsql")
		except : return None

		if sql:

			try :

				result = self.cur.curs.execute(sql)

				self.cur.commit()
		
				__LOG__.Trace(result)

			except pg.psycopg2.errors.DuplicateTable :

				self.cur.rollback()
				__LOG__.Trace("%s Table is already exists" % table_name)

	def createFunction(self):

		sql = '''
		CREATE FUNCTION update_false() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN
			IF NEW.CREATE_TIME <> OLD.CREATE_TIME THEN
			RAISE EXCEPTION 'update create_time is not a allowed';
			END IF;
			IF NEW.UPDATE_TIME < OLD.CREATE_TIME THEN
			RAISE EXCEPTION 'update update_time less than create_time is not a allowed';
			END IF;
			RETURN NEW;
		END $$
		'''
		try :

			self.cur.curs.execute(sql)

			self.cur.commit()

			__LOG__.Trace("Create Function Success !!")
		
		except pg.psycopg2.errors.lookup("42723") : #DuplicateFunction Code
			self.cur.rollback()
			__LOG__.Trace("update_false() Function already exists !" )
			

		#Function을 테이블에 Trigger 연결
	def createTrigger(self,table_name):

		sql = '''
		CREATE TRIGGER avoid_update_{table}
		BEFORE UPDATE
		ON {tableName}
		FOR EACH ROW
		EXECUTE PROCEDURE update_false()
		'''.format(table=table_name.split('_')[2],tableName=table_name)
		try :
			self.cur.curs.execute(sql)

			self.cur.commit()

			__LOG__.Trace("Create Trigger Success !!")

		except pg.psycopg2.errors.lookup("42710") : #DuplicateObject
			self.cur.rollback()
			__LOG__.Trace("avoid_update_%s Trigger already exists !" % table_name.split('_')[2])


	def tableInit(self):

		#Connection 연결
		self.cur = pg.Postgres_py3(self.conf_psql_connection)

		#table 이름 정보
		self.table_names = ["CODE","COLUMN"] 
		
		#Function생성( 두 테이블이 공유 )
		self.createFunction()


		#테이블 별로
		for table_name in self.table_name_list:
			self.createTable(table_name)
			self.createTrigger(table_name)

	#Xlsx을 temp로 변환 (Pandas)
	def convertXlsxToTemp(self,file_path) : 

		sheet_name_list = self.conf_sheet_names.split(",")

		sheet_out_name_list = self.conf_sheet_out_names.split(",")
		
		#시트별로 읽어야 함 
		for idx,sheet_name in enumerate(sheet_name_list):
		
			#법정동코드는 header가 2번행에 있음(CONF로 가능)
			if sheet_name != '법정동코드 연계 자료분석용' : sheet_df = pd.read_excel(file_path,sheet_name=sheet_name,dtype=str)

			else : sheet_df = pd.read_excel(file_path,sheet_name=sheet_name,header=1,dtype=str)
			
			save_path = os.path.normcase("%s/CENTER_STANDARDIZATION_%s_DEFINITION"\
				%(self.conf_save_dir,sheet_out_name_list[idx]))
			
			save_file = '%s_%s_TMP.temp'%(save_path, self.cur_time)
			
			header = sheet_df.columns.tolist()
			df_col_size = len(header)

			time_list = [self.cur_time,'']
			header.extend(['CREATE_TIME','UPDATE_TIME'])

			#파일에 쓰기
			with open(save_file,'w') as f:

				f.write( '%s\n'% self.conf_out_sep.join(header) )

				for i in range(0,len(sheet_df)) :
					
					#( \n, \t, nan ) 처리
					row = sheet_df.iloc[i].str.replace('\n',' ').replace('\t',' ').replace(np.nan,'').tolist()
					
					try:

						if sheet_out_name_list[idx] in ["CODE","COLUMN"]:
							

							if self.conf_centers.upper() == 'ALL' :	pass

							else :

								cnter_list = [cnter.strip() for cnter in self.conf_centers.split(",") ]

								cnter_id = row[header.index("CNTER_ID")].strip().upper()

								if cnter_id not in cnter_list : continue	

					except:

						__LOG__.Exception()
					
					#(row가 비어있는게 아닐 떄)
					if not row == [''] * df_col_size :

						row.extend(time_list)

						f.write( '%s\n'% self.conf_out_sep.join(row))
			
			if sheet_out_name_list[idx] == "COLUMN" : 

				new_file_name = '%s_%s.dat'%(save_path,self.cur_time)

				os.rename(save_file, new_file_name)
				__LOG__.Trace("Rename : %s -> %s" % (save_file, new_file_name))
				

				save_file = new_file_name
				self.loading_file_dict[sheet_out_name_list[idx]] = save_file

			else :
				self.temp_file_dict[sheet_out_name_list[idx]] = save_file

			__LOG__.Trace("convertXlsx To Dat : %s"%save_file)

			self.std_out = ''

	#temp 최신 temp읽어서 dat 2개로
	def combitationTemp(self):

		code_list = self.conf_sheet_out_names.split(",")

		code_file_name = self.temp_file_dict["CODE"]
		hdongcode_file_name = None
		hbdongcode_file_name = None

		if 'HDONGCODE' in code_list: hdongcode_file_name = self.temp_file_dict["HDONGCODE"]
		if 'HBDONGCODE' in code_list: hbdongcode_file_name = self.temp_file_dict["HBDONGCODE"]
		
		#CODE
		code_dict= {}
		
		file_name_list = [ code_file for code_file in [code_file_name, hdongcode_file_name,\
		hbdongcode_file_name] if code_file ]

		__LOG__.Trace("Combination Temp files : %s" % ",".join(file_name_list))

		code = pd.read_csv(code_file_name,sep = self.conf_out_sep,header=0,dtype=str,na_values='')

		code=code.fillna('')

		header = code.columns.tolist()
		
		#CM이 아닌 값들 저장
		not_cm_code_list = []

		not_cm_code_list.append(header)

		not_cm_code=code[~code['CNTER_ID'].isin(["CM"]) | ~code['CODE_NAME'].isin(["행정동 코드","시군구 코드","시도 코드"])]
		
		for idx,r in not_cm_code.iterrows():

			not_cm_code_list.append(r.tolist())
		
		code = code[code['CNTER_ID'] == 'CM']
		
		for i in code.index:

			code_dict.setdefault(code['CNTER_ID'][i],{}).setdefault(code['CODE_NAME'][i],{}).setdefault(code['CODE_VALUE'][i],code['KOR_CODE_VALUE_MEANING'][i])
		
		code_dict.setdefault('CM',{})
		code_dict['CM']['행정동 코드']= {}
		code_dict['CM']['시군구 코드'] = {}
		code_dict['CM']['시도 코드'] = {}
		
		code = None

		#법정동 연계코드
		if hbdongcode_file_name :

			hbdongcode = pd.read_csv(hbdongcode_file_name,sep = self.conf_out_sep,header=0,dtype=str,usecols=['행정기관코드','시도','시군구','행정동(행정기관명)'],na_values='')

			hbdongcode = hbdongcode.fillna('')
		
			for row in hbdongcode.index:

				if hbdongcode['행정기관코드'][row][2:]=='00000000':

					code_dict['CM']['시도 코드'].setdefault(hbdongcode['행정기관코드'][row][:2],hbdongcode['시도'][row])
			
				elif hbdongcode['행정기관코드'][row][5:]=='00000':

					code_dict['CM']['시군구 코드'].setdefault(hbdongcode['행정기관코드'][row][:5], ' '.join([hbdongcode['시도'][row],hbdongcode['시군구'][row] ] ) )
			
				else :
	
					code_dict['CM']['행정동 코드'].setdefault(hbdongcode['행정기관코드'][row], ' '.join([hbdongcode['시도'][row],hbdongcode['시군구'][row],hbdongcode['행정동(행정기관명)'][row] ]))
		
			hbdongcode = None
		
		#행정동 코드
		if hdongcode_file_name :

			hdongcode = pd.read_csv(hdongcode_file_name,sep = self.conf_out_sep, header=0, dtype=str , usecols=['행정동코드','시도명','시군구명','읍면동명'],na_values='')

			hdongcode = hdongcode.fillna('')
		
			for row in hdongcode.index:
			
				if hdongcode['행정동코드'][row][2:]=='00000000':

					if hdongcode['행정동코드'][row][:2] not in code_dict['CM']['시도 코드']:

						code_dict['CM']['시도 코드'].setdefault(hdongcode['행정동코드'][row][:2],hdongcode['시도명'][row])

				elif hdongcode['행정동코드'][row][5:]=='00000':

					if hdongcode['행정동코드'][row][:5] not in code_dict['CM']['시군구 코드']:

						code_dict['CM']['시군구 코드'].setdefault(hdongcode['행정동코드'][row][:5],' '.join([hdongcode['시도명'][row],hdongcode['시군구명'][row] ] ) )

				else :

					if hdongcode['행정동코드'][row] not in code_dict['CM']['행정동 코드']:

						code_dict['CM']['행정동 코드'].setdefault(hdongcode['행정동코드'][row],' '.join([hdongcode['시도명'][row],hdongcode['시군구명'][row],hdongcode['읍면동명'][row] ]))
			
			hdongcode = None

		#dict to Dat	
		self.dictToDat( code_dict, code_file_name, not_cm_code_list)

		for file_name in file_name_list:
			
			os.remove(file_name)

			__LOG__.Trace("file delete : %s" % file_name)

	#dict를 Dat파일로 
	def dictToDat(self, code_dict, code_file_name, extend_list):

		data_list = []
		data_list.extend(extend_list)
		data_list.extend( self.getSortList(code_dict,"시도 코드"))
		data_list.extend( self.getSortList(code_dict,"시군구 코드"))
		data_list.extend( self.getSortList(code_dict,"행정동 코드"))
		
		file_name,ext = os.path.splitext(code_file_name)

		save_dat_name_tmp = file_name+"_COMB.temp"
		
		with open (save_dat_name_tmp,'w') as f:
			
			row_cnt = 0
			
			for row in data_list:

				try:

					f.write( '%s\n'% self.conf_out_sep.join(row))

					row_cnt +=1

				except :
					__LOG__.Trace("File Write Error : %s" % row )

			#self.out_file_row_cnt = row_cnt

		save_dat_name =file_name.replace('_TMP','') + ".dat"

		os.rename(save_dat_name_tmp,save_dat_name)

		self.loading_file_dict["CODE"] = save_dat_name

		__LOG__.Trace("Combination result : %s " % save_dat_name )

	#dict를 정렬된 리스트로	
	def getSortList(self, code_dict, p_key):

		key_tmp = code_dict['CM'][p_key].keys()

		sort_key = sorted(key_tmp)
		
		result_list = []
		
		for key in sort_key:
			tmp_list = ["CM","COMMON",p_key,key,code_dict["CM"][p_key].get(key),"-",'한국행정구역분류_2020.10.1-"법정동코드 연계 자료분석용"기준 + KIKcd_H.20181210(말소코드포함) ,  행정동코드 2자리',self.cur_time,'']
			result_list.append(tmp_list)
		
		return result_list
	
	def loadDatToDatabase(self):

		__LOG__.Trace("Loading Target File : %s " % ', '.join(self.loading_file_dict))

		for code in self.loading_file_dict :

			self.loadDatFile(code,self.loading_file_dict[code])

			if self.result_flag:

				try :

					self.deletePastPgsql(code)

				except :
					
					__LOG__.Exception()

#			self.logging()

	def loadDatFile(self,table_code,save_file):

#		self.loggingInit("POSTGRES로딩")

		#self.out_file_size = os.path.getsize(save_file)

		#self.out_file_row_cnt = int(subprocess.check_output(["wc","-l",save_file]).split()[0])

		self.cur.separator = self.conf_out_sep

		table_name = 'CENTER_STANDARDIZATION_%s_DEFINITION'%table_code

		self.table_info = table_name

		result =  self.cur.load(table_name,save_file)

		if 'Success' in result :

			__LOG__.Trace("PGSQL Load Success : %s" % save_file)

			self.success_cnt = result.rsplit(":",1)[1].strip()

			self.result_flag = True

			self.cur.commit()

		else :

			__LOG__.Trace("PGSQL Load Failed")

			self.fail_reason = result

			self.cur.rollback()

		__LOG__.Trace(result)

	# 12월 3일 추가 --- 지난 데이터 삭제 : Pgsql
	def deletePastPgsql(self, table_code):
		
		table_name = 'CENTER_STANDARDIZATION_%s_DEFINITION'%table_code

		file_cur_time = os.path.splitext(self.loading_file_dict[table_code])[0].rsplit("_",1)[1]

		selectSql = '''
		SELECT max(create_time) FROM {tableName}
		'''.format(tableName = table_name)

		max_time = self.cur.execute_iter(selectSql)[0][0]

		if not max_time : return False

		if max_time != file_cur_time:

			__LOG__.Trace("Selected Max time : %s And File Write Time : %s" % (max_time , file_cur_time))

			return False

		countSql = '''
		SELECT count(*) FROM {tableName}
		'''.format(tableName = table_name)

		__LOG__.Trace("AS-IS Delete sql in  POSTGRES.%s Total Count = %s"%(table_name, self.cur.execute_iter(countSql)[0][0]))
		

		deleteSql = '''
		DELETE FROM {tableName} WHERE CAST(NULLIF(create_time,'') AS DOUBLE PRECISION)
		< CAST({maxTime} AS DOUBLE PRECISION) or NULLIF(create_time, '') is null
		'''.format(tableName = table_name,maxTime = max_time)

		del_result = self.cur.execute(deleteSql)

		__LOG__.Trace("Delete Past Data : %s"%del_result)

		__LOG__.Trace("TO-BE Delete sql POSTGRES.in %s Total Count = %s"%(table_name, self.cur.execute_iter(countSql)[0][0]))

		self.cur.commit()

	def splitHeader(self, table_name, filePath) :

		path_ctl = self.conf_ctl_path + "/" + table_name + ".ctl"

		path_dat = self.conf_iris_path + "/" + table_name + "_" + self.cur_time + ".dat" 

		if not os.path.exists(filePath) :
			return None, None

		dataList	= list()

		with open(filePath, 'r') as f :
			dataList = f.readlines()

		header_str = dataList.pop(0)
		
		with open(path_ctl, 'w') as cf :
			#Write ctl
			cf.write(header_str.replace(self.conf_out_sep, '\n'))

		with open(path_dat, 'w') as df :
			df.write(''.join(dataList) )

		return path_ctl, path_dat

	def irisLoadDat(self, tableNm, filePath) :

#		self.loggingInit("IRIS로딩")

		#self.out_file_size 	= os.path.getsize(filePath)
		#self.out_file_row_cnt 	= int(subprocess.check_output(["wc","-l",filePath]).split()[0])
	
		self.load_some_success_flag = False

		ctlFile, datFile = self.splitHeader(tableNm, filePath)

		result = None

		if os.path.isfile(ctlFile) and os.path.isfile(datFile) :

			self.partition_info = self.cur_time

			self.key_info = '0'

			result = self.iris_db.load(tableNm, self.key_info , self.partition_info , ctlFile, datFile)

		if '+OK' in result :

			if 'SOME SUCCESS' in result :

				self.load_some_success_flag = True

				__LOG__.Trace('IRIS Load Fail : SOME SUCCESS')

				self.fail_reason = result

			else :

				__LOG__.Trace('IRIS Load Success : %s' % datFile)

				self.success_cnt = result.rsplit(":",1)[1].strip()

				self.result_flag = True
			
		else :

			__LOG__.Trace('IRIS Load Fail')

			self.fail_reason = result

		__LOG__.Trace(result)

#		self.logging()

	# iris Connection 및 Load
	def irisLoad(self) :

		for code in self.loading_file_dict:
			
			table_nm = "CENTER_STANDARDIZATION_%s_DEFINITION" % code

			self.table_info = table_nm

			__LOG__.Trace("Table Name : %s, File Name : %s"%(table_nm, self.loading_file_dict[code]))

			self.irisLoadDat(table_nm, self.loading_file_dict[code])

			if self.load_some_success_flag or self.result_flag :

				try : self.deletePastIRIS(table_nm, self.load_some_success_flag )

				except : __LOG__.Exception()

	# iris del
	def deletePastIRIS(self, table_name, some_success_flag):

		# GET max Create Time
		selectSQL = '''
		SELECT max(create_time) FROM {tableName};
		'''.format(tableName = table_name)

		result = self.iris_db.execute_iter(selectSQL)

		file_cur_time = os.path.splitext(self.loading_file_dict[table_name.split("_")[2]])[0].rsplit("_",1)[1]

		#get ctime
		ctime = next(result)[0]

		if not ctime : return False

		if file_cur_time != ctime :

			__LOG__.Trace("Selected Max time : %s And File Write Time : %s" % (ctime , file_cur_time))

			return False

		
		tmSQL = '''
		SELECT count(*) FROM {tableName} ;
		 '''.format(tableName = table_name)
		__LOG__.Trace("AS-IS Delete sql in  IRIS.%s Total Count = %s"%(table_name,next(self.iris_db.execute_iter(tmSQL))))

		del_option = '<>'

		if some_success_flag :		  #True = Some success , False : success
			del_option = '='


		#Delete
		deleteSQL = '''
		DELETE FROM {tableName} WHERE create_time {option} {ctime} ;
		'''.format(tableName = table_name, option = del_option, ctime = ctime)

		self.iris_db.execute(deleteSQL)

		__LOG__.Trace("TO-BE Delete sql in IRIS.%s Total Count = %s"%(table_name,next(self.iris_db.execute_iter(tmSQL))))

		#IRIS GET CURSOR AND TABLE CREATE
	def irisTableInit(self):

		self.iris_db = iris.Client(self.conf_iris_conn)

		self.iris_db.setSep(self.conf_out_sep,'\n')

		for table_name in self.table_name_list:

			try : sql = self.getSQL(table_name+"_iris")

			except : __LOG__.Exception()

			try :

				self.iris_db.execute(sql)

				__LOG__.Trace("Table Create : %s"%table_name)
			
			except Exception as e:

				if "already exists" in str(e):
					pass
				else : raise Exception(e)


#	def logging(self):
#
#		#center_name, center_id, process_name, process_type, start_time, end_time, duration, in_file_size,
#		#out_file_size, in_file_row_cnt, out_file_row_cnt, table_info, key_info, partition_info, result_flag,
#		#success_cnt, fail_reason, '', '', '', '', '', '', ''
#
#		#start_time = self.cur_time[:8] + '000000'
#		self.start_time = self.cur_time
#
#		end_time = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
#		#end_time = datetime.datetime.now().strftime("%Y%m%d%H%M%S")[:8] + '000000'
#
#		#a = 
#		#send.
#		#__LOG__.Trace()
#		msg = '|^|'.join(map(str,[self.center_name , self.center_id, self.process_name, self.process_type, self.start_time, end_time, self.std_in, self.std_out,\
#		self.in_file_size, self.out_file_size, self.in_file_row_cnt, self.out_file_row_cnt, self.table_info, \
#		self.key_info , self.partition_info, str(self.result_flag), self.success_cnt, self.fail_reason, self.header_cnt,\
#		self.comp_row_cnt, self.error_column_length, self.error_check_notnull, self.error_check_type_legth, self.error_check_format,\
#		self.error_change_cont ]))
#		
#		logClient.irisLogClient().log(msg)
#		__LOG__.Trace("Send Log Socket : %s" % msg )
#
#
#	def loggingInit(self, process_type):
#
#		self.center_name 			= '중계서버'
#		self.center_id 				= 'GW'
#		self.process_name			= os.path.basename(sys.argv[0])
#		self.process_type 			= process_type
#		self.start_time				= ''
#		self.end_time				= ''
#		self.std_in					= ''
#		self.std_out				= ''
#		self.in_file_size			= ''
#		self.out_file_size			= ''
#		self.in_file_row_cnt		= ''
#		self.out_file_row_cnt 		= ''
#
#		self.table_info				= ''
#		self.key_info				= ''
#		self.partition_info			= ''
#		self.result_flag			= ''
#		self.success_cnt			= ''
#		self.fail_reason			= ''
#
#		self.header_cnt 			= ''
#		self.comp_row_cnt 			= ''
#		self.error_column_length 	= ''
#		self.error_check_notnull	= ''
#		self.error_check_type_legth	= ''
#		self.error_check_format		= ''
#		self.error_change_cont		= ''


	def processing(self,in_file) :
		
		__LOG__.Trace( "processing : %s"%in_file )

		
		
		if in_file:
			self.loading_file_dict = {}
			self.temp_file_dict = {}

			self.convertXlsxToTemp(in_file)
			self.combitationTemp()

			self.tableInit()
			self.loadDatToDatabase()

			self.irisTableInit()
			self.irisLoad()			

	def run(self):

		while not SHUTDOWN:


			std_in = None

			is_std_err = False
	
			try:
				std_in = sys.stdin.readline().strip()
	
				self.cur_time = datetime.datetime.now().strftime('%Y%m%d%H%M%S')

				if not std_in :
					is_std_err =True
					continue
				__LOG__.Trace("STD IN : %s"%std_in)
	
				try :
					prefix, in_file = std_in.split('://',1)
				except :
					is_std_err= True
					__LOG__.Trace("Input Format error : %s"%std_in)
					continue
	
				if prefix != "file":
					is_std_err = True
					__LOG__.Trace("Prefix is not match : %s"%prefix)
					continue
	
				if not os.path.exists(in_file):
					is_std_err = True
					__LOG__.Trace("File is not Exists : %s"%in_file)
					continue 
					
				if os.path.splitext(in_file)[1] != ".xlsx":
					is_std_err = True
					__LOG__.Trace("File is not xlsx : %s"%in_file)

#				self.std_in = std_in
#			
#				self.in_file_size = os.path.getsize(in_file)
#
#				self.in_file_row_cnt = ''

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

	DirObservation(module, conf, section).run()

	__LOG__.Trace('============= %s END [pid:%s]====================' % (module, pid ))


#- if name start ----------------------------------------------
if __name__ == "__main__" :
	main()


