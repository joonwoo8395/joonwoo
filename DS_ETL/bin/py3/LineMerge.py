#!/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import time
import datetime
import signal
import subprocess
import glob
import shutil
import Mobigen.Common.Log_PY3 as Log
#import Mobigen.Utils.LogClient as c_log
import json
import copy
import Mobigen.Database.iris_py3 as iris
import Mobigen.Utils.LogClient as SendLog

TR_SQL	= '''
		SELECT 
			PRD_CD, DTST_CD
		FROM 
			TNQKR3753.CNTR_STDZN_CLMN_DFNTN_TB 
		WHERE
			DTST_CD = '%s'
		GROUP BY PRD_CD, DTST_CD;
		'''


def handler(sigNum, frame):
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


class LineMerge:

	def __init__(self, remove_option, idx_file) :

		self.remove_option = remove_option
		self.idx_dict = idx_file + '_dict.idx'
		self.idx_head = idx_file + '_head.idx'
		self.idx_exc_dict	= idx_file + '_exc_dict.idx'
		self.load_dict()

		centerId		= sys.argv[2]
		self.iris_type	= sys.argv[3]

		self.logInit(centerId)

	def __del__(self):
		self.dump_dict()

	def logInit(self, centerId) :
		self.center_name			= ''
		self.center_id				= centerId
		self.product_cd				= ''
		self.product_nm				= ''
		self.dataset_cd				= ''
		self.min_period				= ''
		self.max_period				= ''
		self.start_time				= ''
		self.ps_duration			= ''
		self.std_in					= ''
		self.in_file_size			= ''
		self.in_file_row_cnt		= ''
		self.result_flag			= ''
		self.success_cnt			= ''
		self.fail_type				= ''
		self.fail_reason			= ''
		self.header_cnt				= ''
		self.comp_row_cnt			= ''
		self.error_column_length	= ''
		self.error_check_notnull	= ''
		self.error_check_type_legth	= ''
		self.error_check_format		= ''
		self.error_change_cont		= ''
		self.error_data_range     	= ''

#	def logSend(self, std_out) :
#		if '://' in std_out :
#			std_out = std_out.split('://')[1]

#		self.std_out            = std_out

#		if not os.path.exists(std_out) :
#			self.out_file_size		= ''
#			self.out_file_row_cnt	= ''
#		else :
#			self.out_file_size      = str(os.path.getsize(std_out))
#			if std_out.upper().endswith('.CSV') or std_out.upper().endswith('.DAT') :
#				#self.out_file_row_cnt   = subprocess.check_output(["wc","-l", std_out]).split()[0]
#				self.out_file_row_cnt   = subprocess.getstatusoutput('/usr/bin/wc -l %s' % std_out)[-1].split()[0]

#		self.end_time           = datetime.datetime.now().strftime('%Y%m%d%H%M%S')

#		sendLogData = '|^|'.join(map(str, [self.center_name, self.center_id, self.process_name, self.process_type, self.start_time, self.end_time, self.std_in, self.std_out, self.in_file_size, self.out_file_size, self.in_file_row_cnt, self.out_file_row_cnt, self.table_info, self.key_info, self.partition_info, self.result_flag, self.success_cnt, self.fail_reason, self.header_cnt, self.comp_row_cnt, self.error_column_length, self.error_check_notnull, self.error_check_type_legth, self.error_check_format, self.error_change_cont]))
#		c_log.irisLogClient().log("SendLog://{}\n".format(sendLogData))
#		__LOG__.Trace('send Log Data : {}'.format(sendLogData))

	def load_dict(self):

		try:
			self.dicts 				= {}
			self.headerinfo_dicts 	= {}
			self.exc_dicts			= {}

			try:
				with open(self.idx_dict, 'r') as f , open(self.idx_head, 'r') as h , open(self.idx_exc_dict, 'r') as sf:
					self.dicts = json.load(f)
					self.headerinfo_dicts = json.load(h)
					self.exc_dicts = json.load(sf)

				__LOG__.Trace("load_dict : [%s, %s, %s]" % (self.idx_dict, self.idx_head, self.exc_dicts))

			except FileNotFoundError:
				__LOG__.Trace("FileNotFoundError : pass")

			except:
				__LOG__.Exception()
	
			patt_keys = [*self.dicts.keys()]

			for path_pattern in patt_keys:
				
				type_keys = [*self.dicts[path_pattern].keys()]

				for type_key in type_keys:

					file_name_list = copy.deepcopy( self.dicts[path_pattern][type_key] )

					for file_name, r_cnt in file_name_list:

						if not os.path.isfile(file_name):
							self.dicts[path_pattern][type_key].remove([file_name, r_cnt])

					if len(self.dicts[path_pattern][type_key]) == 0:
						del self.dicts[path_pattern][type_key]

				if len(self.dicts[path_pattern]) == 0:
					del self.dicts[path_pattern]
					del self.headerinfo_dicts[path_pattern]

			__LOG__.Trace("self.dicts : %s" % self.dicts)
			__LOG__.Trace("self.headerinfo_dicts : %s" % self.headerinfo_dicts)
			self.dump_dict()

		except: __LOG__.Exception()

	def dump_dict(self):

		try:
			with open(self.idx_dict, 'w') as f , open(self.idx_head, 'w') as h, open(self.idx_exc_dict, 'w') as sf : 
				json.dump(self.dicts, f)
				json.dump(self.headerinfo_dicts, h)
				json.dump(self.exc_dicts, sf)
				
			__LOG__.Trace("dump_dict : [%s, %s, %s]" % (self.idx_dict, self.idx_head, self.idx_exc_dict))

		except:
			__LOG__.Exception()

	def stderr(self, msg) :

		sys.stderr.write(msg + '\n')
		sys.stderr.flush()
		__LOG__.Trace('Std ERR : %s' % msg)

	def stdout(self, msg) :

		sys.stdout.write(msg + '\n')
		sys.stdout.flush()
		__LOG__.Trace('Std OUT : %s' % msg)

	def makedirs(self, path) :

		try :
			os.makedirs(path)
			__LOG__.Trace( path )
		except : pass
	
	def addHeader(self, in_file, tmp_file):

	#	ctl:///home/ds_center/DATA/DATA_COMP/KCB_T004_INCOME1_202008.CSV/Standardization/TR111900020001-KC_ADSTRD_STDR_INCOME_INFO-202008.ctl
	#	csv:///home/ds_center/DATA/DATA_COMP/KCB_T004_INCOME1_202008.CSV/Standardization/TR111900020001-KC_ADSTRD_STDR_INCOME_INFO-202008.csv_03_4_39272_0
	#	ctl:///home/ds_center/DATA/DATA_COMP/KCB_T004_INCOME1_202008.CSV/Standardization/TR111900020001-KC_ADSTRD_STDR_INCOME_INFO-error_check_type_legth.ctl
	#	error:///home/ds_center/DATA/DATA_COMP/KCB_T004_INCOME1_202008.CSV/Standardization/TR111900020001-KC_ADSTRD_STDR_INCOME_INFO-error_check_type_legth.csv_03_4_39272_0
		in_dir_name, in_base_name = os.path.split(in_file)
		out_dir_name, out_base_name = os.path.split(tmp_file)

		#csv:///home/ds_center/DATA/DATA_COMP/KCB_T004_INCOME1_202008.CSV/LineMerge/TR111900020001-KC_ADSTRD_STDR_INCOME_INFO-error_check_type_legth.csv
		#csv:///home/ds_center/DATA/DATA_COMP/KCB_T004_INCOME1_202008.CSV/LineMerge/TR111900020001-KC_ADSTRD_STDR_INCOME_INFO-202008.csv

		ctl_file = os.path.join(in_dir_name, out_base_name.rsplit('.',2)[0]+ '.ctl')
		result_file = tmp_file.rsplit('.',1)[0]

		with open(ctl_file, 'rb') as read_ctl, open(tmp_file, 'rb') as read_data, open(result_file, 'ab') as write_fd:
		    shutil.copyfileobj(read_ctl,write_fd)
		    shutil.copyfileobj(read_data,write_fd)

		os.remove(tmp_file)

		return result_file

	def excFileRead(self, in_file, ext_path_pattern) :
		excFileList	= list()
		__LOG__.Trace('EXC_FLAG path_pattern : %s' % ext_path_pattern)

		tempFileList	= list()
		with open(in_file, 'r') as excf :
			tempFileList = excf.readlines()

		for oneExcFile in tempFileList :
			excFileList.append(os.path.join(os.path.dirname(oneExcFile), 'LineMerge' ,os.path.basename(oneExcFile)))

		self.exc_dicts[ext_path_pattern] = excFileList

	def processing(self, prefix, in_file) :

		__LOG__.Trace( ' === processing ===' )
		ext_path_pattern = os.path.dirname(in_file).rsplit('extraFile', 1)[0]

		if 'EXC_TRUE' == prefix :
			self.excFileRead(in_file, ext_path_pattern)

		elif 'EXC_FALSE' == prefix :
			self.excFileRead(in_file, ext_path_pattern)
			irisConn = iris.Client(self.iris_type)
#			irisConn = iris.Client('IRIS_PRD')

			trCode		= None
			dataSetCode = in_file.rsplit(':', 1)[0]
			resultList	= list(irisConn.execute_iter(TR_SQL % dataSetCode))

			if len(resultList) == 0 :
				self.start_time		= datetime.datetime.now().strftime('%Y%m%d%H%M%S')
				self.std_in			= in_file
				self.result_flag	= 'FA'
				self.fail_type		= '데이터셋미등록'
				self.fail_reason	= os.path.splitext(os.path.basename(self.exc_dicts[ext_path_pattern][0]))[0] + '.tar'

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

				__LOG__.Trace('PS_LOG DATA Load [%s]'  % str(sendLogData))
				SendLog.irisLogClient().log("PS_LOG://{}\n".format(sendLogData))
				
			else :
				for dataList in resultList :
					trCode = dataList[0]

				ref_file	= '%s-Exc.ref' % tr_code

				with open(ref_file, 'w') as ab:
					for exc_result_file in self.exc_dicts[ext_path_pattern] :
						ab.write('%s\n' % exc_result_file)

				self.stdout('://'.join(['ref', ref_file]))

			del self.exc_dicts[ext_path_pattern]

		elif prefix in ('csv', 'error') :
	
	#		if not in_file.rsplit('_',4)[-1].isdigit() or not in_file.rsplit('_',4)[-2].isdigit() or not in_file.rsplit('_',4)[-3].isdigit() or not in_file.rsplit('_',4)[-4].isdigit():
	#
	#			stdout_dir = os.path.join(os.path.abspath(os.path.join(os.path.dirname(in_file), os.pardir)), 'LineMerge')
	#			self.makedirs(stdout_dir)
	#
	#			stdout_file = os.path.join(stdout_dir, os.path.basename(in_file)+ '.tmp')
	#			__LOG__.Trace('Not Split stdout_file : %s' % stdout_file)
	#
	#			shutil.copy(in_file, stdout_file)
	#			__LOG__.Trace('File Copy %s >>> %s' %(in_file, stdout_file))
	#
	#			result_file = self.addHeader(in_file,stdout_file)	
	#
	#			ref_file = result_file.rsplit('-',1)[0] + '.ref'
	#			with open(ref_file, 'w') as ab:
	#				ab.write('%s\n' % result_file)
	#
	#			self.stdout('://'.join([prefix, ref_file]))
	#
	#		else:
			file_pattern, splitnum, splitTotal, totalRowCnt, headerCnt = in_file.rsplit('_',4)
			path_pattern, type_key = file_pattern.rsplit('-',1)
			record_count = int(subprocess.getstatusoutput('/usr/bin/wc -l %s' % in_file)[-1].split()[0])
	
			self.dicts.setdefault(path_pattern, {}).setdefault(type_key, []).append([in_file, record_count])
			self.headerinfo_dicts.setdefault(path_pattern, {}).setdefault(splitnum, int(headerCnt))
	
			total_header_cnt = sum(self.headerinfo_dicts[path_pattern].values())
	
			input_row_cnt = 0
			for k,v in self.dicts[path_pattern].items():
				for f, c in v:
					input_row_cnt += c
	
			#__LOG__.Watch([totalRowCnt, input_row_cnt, total_header_cnt])
	
		
			if int(totalRowCnt) == input_row_cnt + total_header_cnt :
	
				dir_name, base_name = os.path.split(path_pattern)
				
				save_dir = os.path.join(os.path.abspath(os.path.join(dir_name, os.pardir)), 'LineMerge')
				self.makedirs(save_dir)
	
				result_file_list = []
	
				for type_key, v in self.dicts[path_pattern].items():
	
					done_file = os.path.join(save_dir, '-'.join([base_name, type_key]))
					tmp_file = done_file + '.tmp'
	
				#	tmp_file = os.path.join(save_dir, '-'.join([base_name, type_key])) + '.tmp'
	
					if os.path.isfile( done_file ) : os.remove(done_file)
					if os.path.isfile( tmp_file ) : os.remove(tmp_file)
	
					o_count = 0
					o_size = 0.0
	
					for input_file, c in v:
					
						#__LOG__.Watch(input_file)
						record_count = int(subprocess.getstatusoutput('/usr/bin/wc -l %s' % input_file)[-1].split()[0])
						in_file_size = float(os.path.getsize(input_file)) / 1024 / 1024 #MB
						__LOG__.Trace('Merge list : %s, count : %s, size : %s MB' % (input_file, record_count, in_file_size))
	
						o_count += record_count
						o_size += in_file_size
	
						with open(input_file, 'rb') as read_fd, open(tmp_file, 'ab') as write_fd:
							shutil.copyfileobj(read_fd,write_fd)
	
					#__LOG__.Watch(tmp_file)
					r_count = int(subprocess.getstatusoutput('/usr/bin/wc -l %s' % tmp_file)[-1].split()[0])
					r_size = float(os.path.getsize(tmp_file)) / 1024 / 1024
	
					__LOG__.Trace('Total count : %s, Total size : %s' % (o_count, o_size))
					__LOG__.Trace('Merged : %s, count : %s, size : %s MB' % (tmp_file, r_count, r_size))
	
					result_file = self.addHeader(in_file, tmp_file)
					result_file_list.append(result_file)
					#self.stdout('://'.join([prefix, result_file]))
					#os.rename(tmp_file, done_file)
	
				ref_file = result_file.rsplit('-',1)[0] + '.ref'

				with open(ref_file, 'w') as ab:
					for result_file in result_file_list:
						ab.write('%s\n' % result_file)
					if ext_path_pattern == path_pattern.rsplit('Standardization', 1)[0] :
						for exc_result_file in self.exc_dicts[path_pattern.rsplit('Standardization', 1)[0]] :
							shutil.move(exc_result_file, ext_path_pattern + os.path.basename(exc_result_file) )
							ab.write('%s\n' % ext_path_pattern + os.path.basename(exc_result_file))

				self.stdout('://'.join(['ref', ref_file]))
	#			self.logSend(ref_file)
			
				if self.remove_option:
					for k,v in self.dicts[path_pattern].items():
						for input_file, c in v:
							os.remove(input_file)

				del self.dicts[path_pattern]
				del self.headerinfo_dicts[path_pattern]

				if path_pattern.rsplit('Standardization', 1)[0] in self.exc_dicts :
					del self.exc_dicts[ext_path_pattern]

		
		else:
			__LOG__.Trace('len_type : [%s] splitTotal : [%s] file_pattern : [%s] ' % (len(self.dicts[path_pattern]), int(splitTotal), path_pattern))
			__LOG__.Trace('totalRowCnt [%s] input_row_cnt [%s] total_header_cnt [%s] file_pattern : [%s] ' % (totalRowCnt, input_row_cnt, total_header_cnt, path_pattern))
			__LOG__.Trace('waiting for next file ...')



	def run(self):

		while True:
#			self.start_time         = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
			std_in = None
			is_std_err = False

			try:
				std_in = sys.stdin.readline().strip()

				if not std_in :
					is_std_err = True
					continue

				__LOG__.Trace('STD  IN : %s' % std_in)

				try:
					prefix, in_file = std_in.split( '://', 1 )
				except:
					is_std_err = True
					__LOG__.Trace( 'Input format error : %s' % std_in )
					continue

				if prefix not in ('csv', 'error', 'EXC_TRUE', 'EXC_FALSE'): 
					is_std_err = True
					__LOG__.Trace( 'prefix not alowed : %s' % prefix )
					continue

				if not os.path.isfile( in_file ) :
					is_std_err = True
					__LOG__.Trace( 'Not found input file : %s' % in_file )
					continue

				if prefix in ('csv', 'error') and (not in_file.rsplit('_',4)[-1].isdigit() or not in_file.rsplit('_',4)[-2].isdigit() or not in_file.rsplit('_',4)[-3].isdigit() or not in_file.rsplit('_',4)[-4].isdigit()) :
					is_std_err = True
					__LOG__.Trace( 'in_file cannot proceed : %s' % in_file )
					continue

#				self.std_in             = in_file
#				self.in_file_size       = str(os.path.getsize(in_file))

#				if in_file.upper().endswith('.CSV') or in_file.upper().endswith('.DAT') :
					#self.in_file_row_cnt	= subprocess.check_output(["wc","-l", in_file]).split()[0]
#					self.in_file_row_cnt   = subprocess.getstatusoutput('/usr/bin/wc -l %s' % in_file)[-1].split()[0]

				self.processing(prefix, in_file)

				self.dump_dict()
				is_std_err = True

			except:
				__LOG__.Exception()

			finally :
				if std_in != None and is_std_err :
					self.stderr( std_in )

			#self.dicts 에서 시간이 지나도 갯수가 맷칭이 안되는 애들은 그냥 묶어서 내보내기 (예: error)


		
#- main function ----------------------------------------------------
def main():

	module = os.path.basename(sys.argv[0])

	if len(sys.argv) < 4:
		sys.stderr.write('Usage 	: %s {split remove option} {center_id} {iris type} {option:[[log_arg]-d]} \n' % module )
		sys.stderr.write('Example 	: %s test\n' % ( module, os.path.splitext(module)[0] ) )
		sys.stderr.write('Example 	: %s -d\n' % ( module, os.path.splitext(module)[0] ) )
		sys.stderr.flush()
		os._exit(1)

	if sys.argv[1].upper() == 'TRUE': remove_option = True
	else: remove_option = False
	
	log_arg = ''

	if '-d' not in sys.argv :

		#etc_argv = sys.argv[2:]

		if len(sys.argv[4:]) > 0 :
			log_arg = '_' + sys.argv[4]

		log_path = os.path.expanduser('~/DS_ETL/log')
		
		try : os.makedirs( log_path )
		except : pass

		log_file = os.path.join(log_path, '%s%s.log' % (os.path.splitext(module)[0], log_arg ))
		Log.Init(Log.CRotatingLog(log_file, 10240000, 9))		

	else:
		Log.Init()		
	
	__LOG__.Trace('============= %s START [pid:%s]==================' % (module, os.getpid()))

	idx_path = os.path.expanduser('~/DS_ETL/idx')
	try: os.makedirs( idx_path )
	except : pass
	idx_file = os.path.join(idx_path, '%s%s' % (os.path.splitext(module)[0], log_arg ))

	LineMerge(remove_option, idx_file).run()

	__LOG__.Trace('============= %s END ====================' % module)


#- if name start ----------------------------------------------
if __name__ == "__main__" :
	main()


