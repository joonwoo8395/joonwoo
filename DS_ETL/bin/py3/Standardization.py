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
import csv
import copy
import json

#import $MOBIGEN_LIB$
import Mobigen.Common.Log_PY3 as Log; Log.Init()
import Mobigen.Utils.FileProgress_py3 as FileProgress
import socket
#from Mobigen.Utils.EFClient import EFClient
#import Mobigen.Utils.LogClient as SendLog
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

class Standardization:

	def __init__(self, conf, debug_mode, log_arg) : 
		
		section = 'STANDARD'
		self.debug_mode = debug_mode
		self.log_arg = log_arg
		
		self.center_id = conf.get('GENERAL', 'CENTER_ID')
		self.center_name = conf.get('GENERAL', 'CENTER_NAME')

		self.ref_path = conf.get(section, 'REF_PATH')
		self.ref_column_name = conf.get(section, 'REF_COLUMN_NAME')
		self.ref_code_name = conf.get(section, 'REF_CODE_NAME')
		#self.ref_hdong_name = conf.get(section, 'REF_HDONG_NAME')
		#self.ref_hbdong_name = conf.get(section, 'REF_HBDONG_NAME')
		self.ref_sep = conf.get(section, 'REF_SEP')
		self.date_column_name_list = conf.get(section, 'DATE_COLUMN_NAME').split(';')
		self.date_column_length_list = conf.get(section, 'DATE_COLUMN_LENGTH').split(';')
		if len(self.date_column_name_list) != len(self.date_column_length_list) : 
			raise Exception('Please check configure [DATE_COLUMN_NAME|DATE_COLUMN_LENGTH]')
		self.json_path	= conf.get(section, 'JSON_PATH')
		self.leave_error_log = conf.getint(section, 'LEAVE_ERROR_LOG')

		try:
			self.csv_read_sep = conf.get(section, 'CSV_READ_SEP')
		except:
			self.csv_read_sep = ','

		self.colum_file_ctime = None
		self.code_file_ctime = None
		#self.hdong_file_ctime = None
		#self.hbdong_file_ctime = None

		self.get_colum_dict()
		self.get_code_dict()

		self.SendLogInit()

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

	def SendLogInit(self) :
		
		self.center_name = self.center_name
		self.center_id = self.center_id
		self.start_time = ''
		self.std_in = ''
		self.in_file_size = ''
		self.in_file_row_cnt = ''
		self.result_flag = ''
		self.total_row_cnt = ''
		self.fail_reason = ''
		self.header_cnt = ''
		self.comp_row_cnt = ''
		self.error_column_length = ''
		self.error_check_notnull = ''
		self.error_check_type_legth = ''
		self.error_check_format = ''
		self.error_change_cont = ''
		self.error_value_range = ''

		self.product_cd = ''
		self.product_nm = ''
		self.dataset_cd = ''
		self.ps_duration = ''

		self.min_period = ''
		self.max_period = ''
		self.fail_type = ''

#	def dump_dict(self, dicts, path):
#
#		try:
#			with open(path, 'w') as fd:
#				json.dump(dicts, fd)
#
#			__LOG__.Trace("dump_dict : %s " % (path))
#
#		except:
#			__LOG__.Exception()	
#
	
	def get_lastest_file(self, glob_name):
		
		list_of_files = glob.glob(glob_name)
		#latest_file = max(list_of_files, key=os.path.getctime)
		latest_file = max(list_of_files)
		#latest_file_ctime = os.path.getctime(latest_file)
		latest_file_ctime = os.path.basename(latest_file).rsplit('_',1)[-1].split('.')[0]
		return [ latest_file, latest_file_ctime ]

	def	get_colum_dict(self):

		file_name, file_ctime = self.get_lastest_file( os.path.join(self.ref_path, '%s*.dat' % self.ref_column_name) )
		if self.colum_file_ctime == None or file_ctime > self.colum_file_ctime:
			__LOG__.Trace("!! NEW === COLUMN REF FILE CHANGED [ %s ]" % file_name)
			self.colum_file_ctime = file_ctime

			self.dataset_dict = {}
			self.table_dict = {}
			self.colum_dict = {}
			self.table_header_dict = {}
			self.new_table_header_dict = {}
			self.column_func_dict = {}
			line_count = 0

			with open(file_name) as fd:
				for line in fd:
					line_list = line.strip().split( self.ref_sep )
					if line_count == 0 : 
						header_list = line_list
						line_count += 1
					else:
						#CNTR_NM^CNTR_ID^PRD_CD^PRD_NM^PREV_TB_NM^DTST_CD^NW_TB_NM^IDX^PREV_CLMN_NM^KOR_CLMN_NM^NW_CLMN_NM^DMN_LRG_CTGRY^DMN_MDDL_CTGRY^
						#KOR_DMN_NM^TYPE_LNTH^DATA_FRMT^NOTNULL_YN^MINM_VL^MAX_VL^XCPTN_VL^CLMN_FUNC^DATA_CONT_FUNC^RMKS^CREATE_TIME^UPDATE_TIME
						if line_list[header_list.index('CNTR_ID')].upper().strip() == self.center_id.upper().strip():

							#try:
							self.dataset_dict.setdefault(line_list[header_list.index('DTST_CD')].upper().strip(), \
														[ line_list[header_list.index('PRD_CD')].upper().strip(), line_list[header_list.index('PRD_NM')].strip()] )
							self.table_dict.setdefault(line_list[header_list.index('DTST_CD')].upper().strip(), \
														[ line_list[header_list.index('PRD_CD')].upper().strip(), line_list[header_list.index('NW_TB_NM')].upper().strip() ] )


							if line_list[header_list.index('IDX')].strip() != '' :	
								self.colum_dict.setdefault(line_list[header_list.index('DTST_CD')].upper().strip(), {}).setdefault( line_list[header_list.index('PREV_CLMN_NM')].upper().strip(), \
																																							[ line_list[header_list.index('TYPE_LNTH')].upper().strip(), \
																																								line_list[header_list.index('NOTNULL_YN')].upper().strip(), \
																																								line_list[header_list.index('DATA_FRMT')].strip(), \
																																								line_list[header_list.index('DATA_CONT_FUNC')].strip(), \
																																								line_list[header_list.index('MINM_VL')].strip(), \
																																								line_list[header_list.index('MAX_VL')].strip(), \
																																								line_list[header_list.index('XCPTN_VL')].strip() \
																																								])
								
								self.table_header_dict.setdefault(line_list[header_list.index('DTST_CD')].upper().strip(), []).append(\
																		[int(line_list[header_list.index('IDX')].strip()), line_list[header_list.index('PREV_CLMN_NM')].upper().strip()])
								self.new_table_header_dict.setdefault(line_list[header_list.index('DTST_CD')].upper().strip(), []).append(\
																		[int(line_list[header_list.index('IDX')].strip()), line_list[header_list.index('NW_CLMN_NM')].upper().strip()])
								self.column_func_dict.setdefault(line_list[header_list.index('DTST_CD')].upper().strip(), []).append(\
																		[int(line_list[header_list.index('IDX')].strip()), line_list[header_list.index('CLMN_FUNC')].strip()])


							#except:
							#	__LOG__.Exception()
							#	__LOG__.Trace(line)

			
			#__LOG__.Watch(self.table_dict)
			#self.dump_dict( self.table_dict, os.path.join( self.json_path, 'sd_table_dict%s.json' % self.log_arg))
			#__LOG__.Watch(self.colum_dict)
			#self.dump_dict( self.colum_dict, os.path.join( self.json_path, 'sd_colum_dict%s.json' % self.log_arg))

			for keys in self.table_header_dict:
				self.table_header_dict[keys] = [ x[1] for x in sorted(self.table_header_dict[keys], key=lambda x:x[0]) ]

			#__LOG__.Watch(self.table_header_dict)
			#self.dump_dict( self.table_header_dict, os.path.join( self.json_path, 'sd_table_header_dict%s.json' % self.log_arg))

			for keys in self.new_table_header_dict:
				self.new_table_header_dict[keys] = [ x[1] for x in sorted(self.new_table_header_dict[keys], key=lambda x:x[0]) ]

			#__LOG__.Watch(self.new_table_header_dict)
			#self.dump_dict( self.new_table_header_dict, os.path.join( self.json_path, 'sd_new_table_header_dict%s.json' % self.log_arg))			

			for keys in self.column_func_dict:
				self.column_func_dict[keys] = [ x[1] for x in sorted(self.column_func_dict[keys], key=lambda x:x[0]) ]

			#__LOG__.Watch(self.column_func_dict)
			#self.dump_dict( self.column_func_dict, os.path.join( self.json_path, 'sd_column_func_dict%s.json' % self.log_arg))

			
	def	get_code_dict(self):

		afile_name, afile_ctime = self.get_lastest_file( os.path.join(self.ref_path, '%s*.dat' % self.ref_code_name) )

		if self.code_file_ctime == None or afile_ctime > self.code_file_ctime:

			self.code_dict = {}
			
			__LOG__.Trace("!!! NEW === CODE REF FILE CHANGED [ %s ]" % afile_name)
			self.code_file_ctime = afile_ctime

			line_count = 0
			with open(afile_name) as fd:
				for line in fd:
					line_list = line.strip().split( self.ref_sep )
					if line_count == 0 :
						header_list = line_list
						line_count += 1
					else:
						#CNTR_ID^CNTR_NM^CD_NM^CD_VL^CD_KOR_MNNG^CD_ENG_MNNG^CD_EXPLN^CREATE_TIME^UPDATE_TIME
						if line_list[header_list.index('CNTR_ID')].upper() == self.center_id.upper() or line_list[header_list.index('CNTR_ID')].upper() == 'CM' :
							self.code_dict.setdefault(line_list[header_list.index('CNTR_ID')].upper(), {}).setdefault(line_list[header_list.index('CD_NM')], {}).setdefault(\
																		line_list[header_list.index('CD_VL')], line_list[header_list.index('CD_KOR_MNNG')] )

			#self.dump_dict( self.code_dict, os.path.join( self.json_path, 'sd_code_dict%s.json' % self.log_arg))

	
	def check_type_legth( self, in_data, ty_len):

		result = False

		try:
			types, length = ty_len.split('|')

			# 글자일때
			if types.lower() == 'text':
				if len(in_data) <= int(length) : result = True
			
			# 정수일때
			elif types.lower() == 'integer':

				if in_data.startswith('-'): 
					in_data = in_data[1:] 
				
				if type(eval(in_data)).__name__ == 'int' and len(in_data) <= int(length) : result = True


			# 소수점 있을때
			elif types.lower() == 'real' :

				if in_data.startswith('-'):
					in_data = in_data[1:]

				tot_len, decimal_place =  length.split(',')
				if type(eval(in_data)).__name__ == 'float' and len(in_data.split('.')[0]) <= int(tot_len) - int(decimal_place) and len(in_data.split('.')[1]) <= int(decimal_place) : result = True

		#else:
		#	raise Exception('TYPES ERROR : types must in [integer/text/real] : %s' % types)
			
		#except SyntaxError: pass
		except : pass
		#except:
		#	__LOG__.Exception()

		#if not result : __LOG__.Watch([in_data, ty_len])

		return result

	def check_notnull( self, in_data, notnull ):

		result = False
		
		if notnull.lower() in ( 'not null', 'notnull', 'y', 'yes' ):
			if len(in_data) > 0 : 
				result = True

		else: result = True

		return result

	def check_format( self, in_data, formats ):
	
		result = False
		
		if formats.lower() == 'yyyymm':
			try:
				datetime.datetime.strptime(in_data,'%Y%m')
				result = True
			except: pass
			#__LOG__.Exception()

		elif formats.lower() == 'yyyymmdd':
			try:
				datetime.datetime.strptime(in_data,'%Y%m%d')
				result = True
			except: pass
			#__LOG__.Exception()

		elif formats.lower() == 'yyyymmddhhmm':
			try:
				datetime.datetime.strptime(in_data,'%Y%m%d%H%M')
				result = True
			except: pass
			#__LOG__.Exception()

		elif formats.lower() == 'yyyymmddhhmmss':
			try:
				datetime.datetime.strptime(in_data,'%Y%m%d%H%M%S')
				result = True
			except: pass
			#__LOG__.Exception()
		
		elif formats.lower() == 'yyyymmddhhmmss.ffffff':
			try:
				datetime.datetime.strptime(in_data,'%Y%m%d%H%M%S.%f')
				result = True
			except: pass
			#__LOG__.Exception()

		elif formats.lower() == 'hh':
			try:
				datetime.datetime.strptime(in_data,'%H')
				result = True
			except: pass
			#__LOG__.Exception()

		elif formats.lower() == 'hhmm':
			try:
				datetime.datetime.strptime(in_data,'%H%M')
				result = True
			except: pass
			#__LOG__.Exception()

		elif formats.lower().startswith('in_'):
			key_list = formats.lower().split('in_')[1].split(',')
			if in_data.lower() not in key_list : result = False
			else: result = True

		elif formats.lower().startswith('code_'):
			key = formats.split('code_')[1]
			
			try:
				if in_data in self.code_dict['CM'][key].keys(): result = True

			except:
				try:
					if 'NOT CHECK' in self.code_dict[self.center_id][key].keys() : result = True
					elif in_data in self.code_dict[self.center_id][key].keys() : result = True

				except: pass
					#__LOG__.Exception()
		
		elif formats.lower().startswith('phone_number'):

			try:
				in_data_list = in_data.split(';')
				false_count = 0

				for one_number in in_data_list :
					phone_number_count = 0
					for letter in one_number:
						phone_number_count += 1
						if phone_number_count == 1 and letter == '+': continue
						elif type(eval(letter)).__name__ == 'int' : continue
						else: false_count += 1

				if false_count == 0 : result = True
			except: pass
		
		#if not result : __LOG__.Watch([in_data, formats])

		return result

	def change_data_cont( self, in_data, change_rule):

		def split_use(in_data, delimiter, use_index):
			if in_data != '' :
				return in_data.strip().split(delimiter)[use_index]
			else: return in_data

		#표준화 CM으로 치환
		def change_to_cmcode(in_data, cm_name):
			if in_data != '' :
				#self.code_dict <'dict> = {'KC': { '성별구분코드': {'1': '남성', '2': '여성'} , CM': {'성별구분코드': {'M': '남성', 'F': '여성'}}}
				in_meaning = self.code_dict[self.center_id][cm_name][in_data]
				return next(( k for k, v in self.code_dict['CM'][cm_name].items() if v == in_meaning), None)
			else: return in_data

		def codemean_to_codekey(in_data, code_name):
			if in_data != '' :
				if in_data in self.code_dict[self.center_id][code_name] : return in_data
				else: return next((k for k, v in self.code_dict[self.center_id][code_name].items() if v == in_data), None)
			else: return in_data

		#행정동코드 자리수 맞추기 = 뒤에서 부터 0 채우기(KC)
		#09시간 -> 0900시간으로 변경(KS)
		def op_zfill(in_data, length):
			if in_data == '99999999': return in_data + '9'*( length - len(in_data))
			else: return in_data + '0'*( length - len(in_data)) 

		def zfill(in_data, length):
			return in_data.zfill(length)

		def strstrip(in_data):
			return in_data.strip()

		def division(in_data, length):
			if in_data != '' :
				return float(in_data)/length
			else: return in_data

		def image_to_alphabet(in_data):
			if in_data == '○' : return 'N'
			elif in_data == '●' : return 'Y'
			elif in_data == '-' : return 'X'
			elif in_data == '.' : return 'E'
			else: return in_data

		def letter_change(in_data, obj, obj_true):
			#if obj == '' : obj = 'None'
			if in_data.upper() == obj.upper(): return obj_true
			else: return in_data

		#리눅스timestamp to date
		def timestamp_to_dt(in_data):
			if in_data != '' :
				return datetime.datetime.fromtimestamp(int(in_data)).strftime('%Y%m%d%H%M%S')
				#a = datetime.datetime.fromtimestamp(int(in_data)).strftime('%Y%m%d%H%M%S')
				#if not a.startswith('202006'): __LOG__.Trace("!!!ERROR!!!!!: %s %s" % (in_data, a))
				#return a
			else: return in_data

		def tran_to_decimal(in_data):
			
			if in_data != '' :
				try:
					if type(eval(in_data)).__name__ != 'float': return float(in_data)
					else: return in_data
				except: return None
			else: return in_data

		def float_round(in_data):
			if in_data != '' :
				if type(eval(in_data)).__name__ == 'float': return str(round(eval(in_data)))
				else : return in_data
			else: return in_data

		def time_to_time(in_format, out_format, in_data):
			if in_data != '' :
				return datetime.datetime.strptime(in_data, in_format).strftime(out_format)
			else: return in_data
		
		def phone_number(in_data):

			if in_data != '' :
				in_data = in_data.strip().replace('-','').replace(' ','')
					
				if in_data[-2] == '~' and ( int(in_data[-3]) < int(in_data[-1]) ) and int(in_data[-1]) <= 9 :
				
					new_number_list = []
					pre_num = in_data[:-3]
					for x in range( int(in_data[-3]), int(in_data[-1])+1 ) :
						new_number_list.append( pre_num + str(x) )
						
					in_data = ';'.join(new_number_list)

				elif in_data[-3] == '(' and in_data[-1] == ')' :
					pre_num = in_data[:-4]
					new_number_list = [ pre_num+in_data[-4], pre_num+in_data[-2]]
					in_data = ';'.join(new_number_list)

				in_data = in_data.replace('(','').replace(')','')
				return in_data
			else: return in_data

		try:
			return eval(change_rule)
		except:
			#__LOG__.Exception()
			#os._exit(1)
			return None

	def exception_check( self, in_data, rules):

		if in_data in [x.strip() for x in rules.split(';')]: return True
		else: return False

	def data_value_range_check( self, in_data, min_val, max_val):

		result = False
		min_len = len(in_data)
		max_len = len(in_data)
		inFlag = False
		
		if min_val.startswith('date_') or max_val.startswith('date_'):
			inFlag = True
			
			import Mobigen.Utils.CodeFormat as CodeFormat
			cf = CodeFormat.CodeFormat(self.center_id + '_DATE')
			unzipFileDict   = cf.extract_path_variable(self.fail_reason)

			def get_keyvalue(value):
				new_val = []
				for key in value.split('_')[1:]:
					new_val.append(unzipFileDict[key.upper()])
				return len(''.join(new_val)), ''.join(new_val)

			if min_val != '' : 
				min_len, min_val = get_keyvalue(min_val)
			if max_val != '' : 
				max_len, max_val = get_keyvalue(max_val)		

		if min_val != '' and max_val != '' :
			if float(min_val) <= float(in_data[:min_len]) and float(max_val) >= float(in_data[:max_len]): result = True
		elif min_val != '' :
			if float(min_val) <= float(in_data[:min_len]) : result = True
		elif max_val != '' :
			if float(max_val) >= float(in_data[:max_len]) : result = True

		return result, min_val, max_val, inFlag

	def processing(self, in_file) :

		__LOG__.Trace( ' === processing ===' )
		__LOG__.Watch( in_file )
		
		### input 과 output file 명, etc 에 대하여 
		out_data_dict = {}
		#/home/ds_center/DATA/DATA_COMP/KCB_T003_DEMO_RES3_202008.CSV/LineSplit/T003_DEMO_RES3:KCB_T003_DEMO_RES3_202008.CSV_19_20_1945746
		dataset_code, rest_file_name = os.path.basename(in_file).split(':')
		self.dataset_cd = dataset_code = dataset_code.upper()

		self.fail_reason = rest_file_name.rsplit('_',3)[0]

		try:
			save_base_name = '-'.join( self.table_dict[dataset_code] )
		except KeyError:
			#__LOG__.Exception()
			__LOG__.Trace("FA!!! 데이터셋 미등록: [%s] %s" % (dataset_code, in_file))

			self.result_flag 			= 'FA'
			self.fail_type				= '데이터셋 미등록'

			os.remove(in_file)
			__LOG__.Trace('데이터셋 미등록 파일 삭제 : %s' % in_file)
			return

		#rest_file_name.rsplit('_',3)[-3] = split file index
		#rest_file_name.rsplit('_',3)[-2] = split file 총 갯수
		#rest_file_name.rsplit('_',3)[-1] = 원본 총 row수

		### input 컬럼 명세
		try:
			header_list = self.table_header_dict[dataset_code]	
			__LOG__.Watch( [dataset_code , len(header_list), header_list ])
		except KeyError:
			__LOG__.Trace("FA!!! 표준화정보 미등록: [%s] %s" % (dataset_code, in_file))

			self.result_flag			= 'FA'
			self.fail_type			  	= '표준화정보 미등록'

			os.remove(in_file)
			__LOG__.Trace('표준화정보 미등록 파일 삭제 : %s' % in_file)
			return

		file_num = rest_file_name.rsplit('_',3)[-3]
		split_cnt = rest_file_name.rsplit('_',3)[-2]
		row_cnt = rest_file_name.rsplit('_',3)[-1]

		#- csv 파일 이 LineSplit 을 탔니 안탔니에 따라 달라지는 디렉토리 depth 를 output 통일로 맞추기
		if not file_num.isdigit() or not split_cnt.isdigit() or not row_cnt.isdigit():
			#/home/ds_center/DATA/DATA_COMP/KCB_T003_DEMO_RES3_202008.CSV/T003_DEMO_RES3:KCB_T003_DEMO_RES3_202008.CSV
			save_path = os.path.join(os.path.dirname(in_file), 'Standardization')
			rest_name = '.csv'
			split_flag = False
			__LOG__.Trace('!!!!!!!!!!!!!!!! 여기 지나가면 안되는데 왜 지나가지? 확인 필요!!!!!!!!!!!!!!!!!!!!!!!!!!')
			### LinSplit 변경되서 이제 사용 안하는 부분이지만 일단 냅두는 것으로

		else:
			#/home/ds_center/DATA/DATA_COMP/KCB_T003_DEMO_RES3_202008.CSV/LineSplit/T003_DEMO_RES3:KCB_T003_DEMO_RES3_202008.CSV_19_20_1945746
			save_path = os.path.join(os.path.abspath(os.path.join(os.path.dirname(in_file), os.pardir)), 'Standardization')	
			rest_name = '.csv_%s_%s_%s' % (file_num, split_cnt, row_cnt)
			split_flag = True

		makedirs(save_path)

		### output 컬럼 명세, 삭제 컬럼 대상 확인

		new_header_list = copy.deepcopy( self.new_table_header_dict[dataset_code] )
		#__LOG__.Watch( [ dataset_code, len(new_header_list), new_header_list ])

		column_remove_idx = []
		#column_rename_list = []
		for i, func in enumerate(self.column_func_dict[dataset_code]):
			if func == 'column_remove()':
				column_remove_idx.append(i)
			#elif func.startswith('column_rename'):
				#column_rename_list.append([i, func])

		__LOG__.Watch(column_remove_idx)
		#__LOG__.Watch(column_rename_list)

		#def column_rename(new_name): 
		#	return new_name

		#for idx, func in column_rename_list : 
		#	new_header_list[idx] = eval(func)
	
		remove_idx_cnto = 0
		for i in column_remove_idx: 
			del new_header_list[i - remove_idx_cnto]
			remove_idx_cnto += 1

		__LOG__.Watch( [ dataset_code, len(new_header_list), new_header_list ])

	

		### csv 파일 열어서 데이터 확인 시작
		progress = FileProgress.Progress( in_file=in_file )
		with open( in_file, newline='', encoding='utf-8') as csvread:
			#for row in csv.reader(csvread, delimiter=' ', quotechar='|')

			total_row_cnt = 0
			error_column_length = 0
			error_check_type_legth = 0
			error_check_notnull = 0
			error_check_format = 0
			error_change_cont = 0
			error_value_range = 0
			comp_row_cnt = 0
			header_cnt = 0

			### 파일의 로우별로 읽어서
			for index, row in enumerate(csv.reader(csvread, delimiter=self.csv_read_sep),1):

				total_row_cnt += 1

				###헤더확인  => file 이 쪼개져서 들어오기 때문에 여기서 하면 안되고 split 하는데서 해야함 2021.01.14 (예:포항을 생각해볼것)
				#if index == 1 and int(file_num) == 0:

				#	in_header = [x.upper() for x in row]
				#	if in_header == header_list :
				#		header_cnt += 1
				#		continue
				#	else:
				#		__LOG__.Trace("FA!!! 등록헤더 미매칭: 등록헤더%s, 들어온헤더%s " % (header_list, in_header))

				#		self.result_flag			= 'FA'
				#		self.fail_type			  = '등록헤더 미매칭'

				#		return

				## 모든 데이터 strip() 추가 2021.02.10
				row = [x.strip() for x in row]

				## 컬럼 갯수 비교
				if len(header_list) != len(row):
					out_data_dict.setdefault('error_column_length', []).append('|^|'.join(row))
					error_column_length += 1
					#if error_column_length < self.leave_error_log : 
						#__LOG__.Watch(['!!!ERROR : COLUMN CNT!!!', index, len(header_list), len(row)])
					self.erSendLog(index, '컬럼갯수', '등록갯수[%s개] 들어온갯수[%s개] 데이터[%s]' % (len(header_list), len(row), row))
					continue

				#헤더가 들어가있을 경우 무시
				row_compare = [x.upper() for x in row]
				if header_list == row_compare : 
					header_cnt += 1
					continue
				
				#if self.debug_mode: __LOG__.Watch(row)

				### 로우의 데이터별 확인
				row_comp_flag = True
				
				break_flag = False
				for i, in_data in enumerate(row):

					## 왜 strip() 이 안되지? 하나더 추가 2021.02.10
					in_data = in_data.strip()
					rules = self.colum_dict[dataset_code][header_list[i]]

					if rules[3] != '' :
						#DATA_CONT_FUNC 변경
						#if self.debug_mode: __LOG__.Watch([ in_data, row[i] ])

						for rule in [ x.strip() for x in rules[3].split('->') ]:

							#if in_data != '':

							c_value = str(self.change_data_cont( in_data, rule))

							if c_value == 'None': 
								out_data_dict.setdefault('error_change_cont', []).append('|^|'.join(row))
								error_change_cont += 1
								row_comp_flag = False
								#if error_change_cont < self.leave_error_log : 
									#__LOG__.Watch(['!!!ERROR : DATA_CONT_FUNC!!!', header_list[i], i, in_data, rule ])
								self.erSendLog(index, '데이터변형', '변형룰[%s] 컬럼명[%s] 데이터[%s]' % (rule, header_list[i], in_data) )
								break_flag = True
								break

							else:
								in_data = row[i] = c_value

						if break_flag: break

						#if self.debug_mode: __LOG__.Watch([in_data, row[i], c_value])
					
					# 예외데이터 기능 추가 rules[6] XCPTN_VL(;구분자), 예외일 경우 아래의 rule 무시
					if rules[6] != '' and in_data != '' and self.exception_check( in_data, rules[6]): continue

					#NOTNULL 여부에 체크
					if not self.check_notnull( in_data, rules[1] ):
						out_data_dict.setdefault('error_check_notnull', []).append('|^|'.join(row))
						error_check_notnull += 1
						row_comp_flag = False
						#if error_check_notnull < self.leave_error_log : 
							#__LOG__.Watch(['!!!ERROR : NOT_NULL!!!', header_list[i], i, in_data, rules[1]])
						self.erSendLog(index, '빈값불가여부', 'NOTNULL여부[%s] 컬럼명[%s] 데이터[%s]' % (rules[1], header_list[i], in_data) )
						break

					#타입과 글자수 체크
					if not self.check_type_legth( in_data, rules[0] ) and in_data != '': 
						out_data_dict.setdefault('error_check_type_legth', []).append('|^|'.join(row))
						error_check_type_legth += 1
						row_comp_flag = False
						#if error_check_type_legth < self.leave_error_log : 
							#__LOG__.Watch(['!!!ERROR : TYPE AND LENGTH!!!', header_list[i], i, in_data, rules[0]])
						self.erSendLog(index, '데이터타입', '타입|길이[%s] 컬럼명[%s] 데이터[%s]' % (rules[0], header_list[i], in_data) )
						break						


					if rules[2] != '' and in_data != '':
						#FORMAT 체크
						if not self.check_format( in_data, rules[2] ):
							out_data_dict.setdefault('error_check_format', []).append('|^|'.join(row))
							error_check_format += 1
							row_comp_flag = False
							#if error_check_format < self.leave_error_log : 
								#__LOG__.Watch(['!!!ERROR : FORMAT!!!', header_list[i], i, in_data, rules[2]])
							self.erSendLog(index, '데이터형식', '형식[%s] 컬럼명[%s] 데이터[%s]' % (rules[2], header_list[i], in_data) )
							break

					# 데이터범위 기능추가 rules[4] MINM_VL rules[5] MAX_VL 
					if (rules[4] != '' or rules[5] != '') and in_data != '':
						#__LOG__.Trace('첫번째 데이터범위 IF')
						result, min_val, max_val, inFlag = self.data_value_range_check( in_data, rules[4], rules[5])
						#if not self.data_value_range_check( in_data, rules[4], rules[5]):
						if not result:
							#__LOG__.Trace('두번째 데이터범위 IF')
							out_data_dict.setdefault('error_value_range', []).append('|^|'.join(row))
							error_value_range += 1
							row_comp_flag = False
							#if error_check_format < self.leave_error_log :

							if inFlag :
								self.erSendLog(index, '데이터값범위', '범위[%s이상-%s이하] 컬럼명[%s] 데이터[%s]' % (min_val, max_val, header_list[i], in_data) , 'IN_EXC')
								
							else :
								self.erSendLog(index, '데이터값범위', '범위[%s이상-%s이하] 컬럼명[%s] 데이터[%s]' % (min_val, max_val, header_list[i], in_data) )
							break
						
				
				#row의 모든 데이터가 OK 통과하면 
				if row_comp_flag:
					
					date_key = None
					for x,y in zip(self.date_column_name_list, self.date_column_length_list):
						if x.upper() in header_list : 
							#date_key = row[header_list.index(self.date_column_name)][:self.date_column_length]
							date_key = row[header_list.index(x.upper())][:int(y)]

					if date_key == None:
						for x,y in zip(self.date_column_name_list, self.date_column_length_list):
							if x.upper() == 'SDTMAKEDATE':
								date_key = datetime.datetime.now().strftime('%Y%m%d%H%M%S')[:int(y)]

					if date_key == None:
						__LOG__.Trace('!!!!!!!!!!!!!!!! 여기 지나가면 안되는데 왜 지나가지? 확인 필요!!!!!!!!!!!!!!!!!!!!!!!!!!')

					else:
						out_data_dict.setdefault(date_key, []).append('|^|'.join(row))
						comp_row_cnt += 1

				progress.getStatus(index)
		
				####### test 용 #######
				#if total_row_cnt == 5: 
				#	break
				#	__LOG__.Watch(out_data_dict)
				#	__LOG__.Watch([total_row_cnt, error_check_type_legth, error_check_notnull, error_check_format, error_change_cont, comp_row_cnt])	
				#	os._exit(1)

		__LOG__.Trace('< total_row_cnt = %s, header_cnt = %s >:[comp_row_cnt = %s, error_column_length = %s, error_check_notnull = %s, error_check_type_legth = %s, error_check_format = %s, error_change_cont = %s, error_value_range = %s]' % \
							(total_row_cnt, header_cnt, comp_row_cnt, error_column_length, error_check_notnull, error_check_type_legth, error_check_format, error_change_cont, error_value_range))

		self.total_row_cnt = total_row_cnt
		self.header_cnt = header_cnt
		self.comp_row_cnt = comp_row_cnt
		self.error_column_length = error_column_length
		self.error_check_notnull = error_check_notnull
		self.error_check_type_legth = error_check_type_legth
		self.error_check_format = error_check_format
		self.error_change_cont = error_change_cont
		self.error_value_range = error_value_range

		if total_row_cnt - header_cnt == 0 : 
			__LOG__.Trace("FA!!! 빈 데이터: [%s] %s" % (dataset_code, in_file))
			self.result_flag			= 'FA'
			self.fail_type			  	= '빈 데이터'

			os.remove(in_file)
			__LOG__.Trace('빈 데이터 파일 삭제 : %s' % in_file)
			return

		#표준화 결과 분류
		error_total = self.error_column_length + self.error_check_notnull+ self.error_check_type_legth + self.error_check_format + self.error_change_cont + self.error_value_range
		if self.comp_row_cnt != 0 and error_total == 0:
			self.result_flag = 'SC'
		elif self.comp_row_cnt == 0 and error_total != 0:
			self.result_flag = 'ERR'
		else:
			self.result_flag = 'PSC'

		period_list = [int(i) for i in out_data_dict.keys() if not i.startswith('error_')]

		if len(period_list) == 0 :
			self.min_period = ''
			self.max_period = ''
		else:
			self.min_period = min(period_list)
			self.max_period = max(period_list)

		##csv file 쓰기 (삭제할 컬럼은 쓸때 정리)
		if split_flag : rest_name += '_%s' % header_cnt
		for keys in out_data_dict:

			save_file_name = os.path.join(save_path, '%s-%s%s' % (save_base_name, keys, rest_name))
			ctl_file_name = os.path.join(save_path, '%s-%s.ctl' % (save_base_name, keys))

			with open(save_file_name, mode='w') as csvwrite:

				#error
				if keys.startswith('error'):
					prefix = 'error'
					for line in out_data_dict[keys]:
						csv_writer = csv.writer(csvwrite, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
						line_list = line.split('|^|')
						csv_writer.writerow(line_list)
					
					with open(ctl_file_name, 'w') as erf:
						erf_writer = csv.writer(erf, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
						erf_writer.writerow(header_list)


				#표준화 완료
				else:
					prefix = 'csv'
					for line in out_data_dict[keys]:
						csv_writer = csv.writer(csvwrite, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
						line_list = line.split('|^|')

						remove_idx_cntt = 0
						for i in column_remove_idx: 
							del line_list[i - remove_idx_cntt]
							remove_idx_cntt += 1

						csv_writer.writerow(line_list)

					with open(ctl_file_name, 'w') as cpf:
						cpf_writer = csv.writer(cpf, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
						cpf_writer.writerow(new_header_list)


			stdout( '%s://%s' % ('ctl', ctl_file_name) )
			stdout( '%s://%s' % (prefix, save_file_name) )


	def psSendLog(self):
		
		try: self.product_cd = self.dataset_dict[self.dataset_cd][0]
		except: self.product_cd = ''
		try: self.product_nm = self.dataset_dict[self.dataset_cd][1]
		except: self.product_nm = ''

		sendLogData = '|^|'.join(map(str, [   self.center_name 
											, self.center_id 
											, self.product_cd
											, self.product_nm
											, self.dataset_cd
											, self.min_period
											, self.max_period
											, self.start_time
											, self.ps_duration
											, self.std_in 
											, self.in_file_size 
											, self.in_file_row_cnt 
											, self.result_flag 
											, self.total_row_cnt
											, self.fail_type
											, self.fail_reason 
											, self.header_cnt 
											, self.comp_row_cnt 
											, self.error_column_length 
											, self.error_check_notnull 
											, self.error_check_type_legth 
											, self.error_check_format 
											, self.error_change_cont
											, self.error_value_range 	]))

		sockline = "PS_LOG://%s\n" % sendLogData
		__LOG__.Trace(sockline)

		self.SockConn()
		self.sock.send(sockline.encode())

	def erSendLog(self, error_index, error_type, error_reason, preFix='ER_LOG'):

		sendLogData = '|^|'.join(map(str, [   self.center_name
											, self.center_id
											, self.dataset_dict[self.dataset_cd][0] #self.product_cd
											, self.dataset_dict[self.dataset_cd][1] #self.product_nm
											, self.dataset_cd
											, self.start_time
											, self.std_in
											, error_index
											, error_type
											, error_reason   	]))
	
		sockline = "%s://%s\n" % (preFix, sendLogData)
		#__LOG__.Trace(sockline)	

		self.SockConn()
		self.sock.send(sockline.encode())

	def run(self):

		while not SHUTDOWN :

			std_in = None
			is_std_err = False

			try:

				self.SendLogInit()
				#csv:///home/ds_center/DATA/DATA_COMP/KCB_T003_DEMO_RES3_202008.CSV/LineSplit/T003_DEMO_RES3:KCB_T003_DEMO_RES3_202008.CSV_19_20_1945746
				std_in = sys.stdin.readline().strip()
				self.std_in = std_in

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

				if prefix != 'csv' :
					is_std_err = True
					__LOG__.Trace('Prefix is not match : %s' % prefix)
					continue	
				
				if not os.path.exists( in_file )  :
					is_std_err = True
					__LOG__.Trace('Not found file : %s' % in_file)
					continue

				stime = time.time()
				self.start_time = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
				self.in_file_size = os.path.getsize(in_file)
				self.in_file_row_cnt = subprocess.getstatusoutput('/usr/bin/wc -l %s' % in_file)[-1].split()[0]

				self.get_colum_dict()
				self.get_code_dict()
				self.processing(in_file)

				etime = time.time()

				self.ps_duration = etime - stime	
				__LOG__.Trace( 'Duration %s sec' % self.ps_duration )

				self.psSendLog()
				is_std_err = True

			except:
				if not SHUTDOWN : __LOG__.Exception()

			finally :
				if std_in != None and is_std_err :
					stderr( std_in )
				
				time.sleep(1)


		
#- main function ----------------------------------------------------
def main():

	module = os.path.basename(sys.argv[0])

	if len(sys.argv) < 2:
		sys.stderr.write('Usage 	: %s conf {option:[[log_arg]-d]}\n' % module )
		sys.stderr.write('Usage 	: %s conf {option:[[log_arg]-d]}\n' % module )
		#python3 /home/test/Project_name/bin/py3/BaseModule.py /home/test/Project_name/conf/BaseModule.conf
		#python3 /home/test/Project_name/bin/py3/BaseModule.py /home/test/Project_name/conf/BaseModule.conf 0 
		#python3 /home/test/Project_name/bin/py3/BaseModule.py /home/test/Project_name/conf/BaseModule.conf -d
		sys.stderr.flush()
		os._exit(1)

	config_file = sys.argv[1]

	conf = ConfigParser.ConfigParser()
	conf.read(config_file)

	debug_mode = False

	log_arg = ''
	if '-d' not in sys.argv :

		etc_argv = sys.argv[2:]

		if len(sys.argv[2:]) > 0 :
			log_arg = '_' + sys.argv[2]

		log_path = conf.get('GENERAL', 'LOG_PATH')

		makedirs( log_path )	

		log_file = os.path.join(log_path, '%s%s.log' % (os.path.splitext(module)[0], log_arg ))
		Log.Init(Log.CRotatingLog(log_file, 10240000, 9))		

	else:
		Log.Init()		
		debug_mode = True

	pid = os.getpid()	
	__LOG__.Trace('============= %s START [pid:%s]==================' % ( module, pid ))

	Standardization(conf, debug_mode, log_arg).run()

	__LOG__.Trace('============= %s END [pid:%s]====================' % (module, pid ))


#- if name start ----------------------------------------------
if __name__ == "__main__" :
	main()


