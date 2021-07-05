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

import json
import calendar
import mimetypes
import xmltodict
import zipfile

#import $MOBIGEN_LIB$
import Mobigen.Common.Log_PY3 as Log; Log.Init()
#import Mobigen.Utils.LogClient as c_log

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
class overlab(object) :
	def __init__(self, name) :
		self.name = name
	def __repr__(self) :
		return "'"+self.name+"'"

#------------------------------------------------------------
class RDFParser :

	def __init__(self, conf, section, optionEn) :
		#open
		__LOG__.Trace("__init__")

		self.encodeFlag = optionEn
		self.conf		= conf
		self.section 	= 'RDF'

		try : self.centerUserId = self.conf.get('GENERAL', 'CENTER_USER_ID')
		except : __LOG__.Trace("conf CDNTER_USER_ID 확인요망")

		try : self.encodeLan = self.conf.get(self.section, 'ENCODE_LANG')
		except : self.encodeLan = None

		try : self.dateFormat = self.conf.get(self.section, 'DATE_FORMAT')
		except : self.dateFormat = '%Y%m%d'

		try : self.rdfTempPath = self.conf.get(self.section, 'RDF_TEMPLE_PATH')
		except : __LOG__.Trace("conf RDF_TEMPLE_PATH 확인요망")

		try : self.sftpPutPath	= self.conf.get("SFTP_UPLOAD", 'ZIP_UPLOAD_PATH')
		except : self.sftpPutPath = '/DATA/DATA/{}/{}/{}/{}.zip'

		try : self.kcbRdfPath	= self.conf.get(self.section, "RDF_PATH")
		except : __LOG__.Trace("conf RDF_PATH 확인요망")

		centerId    = conf.get('GENERAL', 'CENTER_ID')
		centerName  = conf.get('GENERAL', 'CENTER_NAME')

#		self.logInit(centerName, centerId)

	def __del__(self):
		#close
		__LOG__.Trace("__del__")

#	def logInit(self, centerName, centerId) :
#		self.center_name        	= centerName
#		self.center_id          	= centerId
#		self.process_name       	= os.path.basename(sys.argv[0])
#		self.process_type       	= '일반모듈'
#		self.start_time         	= ''
#		self.end_time           	= ''
#		self.std_in             	= ''
#		self.std_out            	= ''
#		self.in_file_size       	= ''
#		self.in_file_row_cnt    	= ''
#		self.out_file_size      	= ''
#		self.out_file_row_cnt   	= ''
#		self.table_info         	= ''
#		self.key_info           	= ''
#		self.partition_info     	= ''
#		self.result_flag        	= ''
#		self.success_cnt        	= ''
#		self.fail_reason        	= ''
#		self.header_cnt             = ''
#		self.comp_row_cnt           = ''
#		self.error_column_length    = ''
#		self.error_check_notnull    = ''
#		self.error_check_type_legth = ''
#		self.error_check_format     = ''
#		self.error_change_cont      = ''

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

	def rdfWrite(self, dirdf, in_file) :
		__LOG__.Trace('xml Dict : \n %s' % dirdf)
		fileName 	= os.path.basename(in_file).split('.')[0]
		fileDir		= os.path.dirname(in_file)

		xml = xmltodict.unparse(dirdf, pretty=True)

		with open(os.path.join(fileDir, fileName + '.rdf') , 'wb') as testRdf :
			testRdf.write(xml.encode('utf-8'))
			testRdf.flush()

	def zipFileListParser(self, in_file) :
		__LOG__.Trace('zipFile Parser START!!')

		trCode		= ''
		startDate	= ''
		endDate		= ''

		fileDate	= None
		dateList	= list()

		trCode		= os.path.basename(in_file).rsplit('.', 1)[0].rsplit('-',1)[0].rsplit('_', 1)[0]
		innerFileList = zipfile.ZipFile(in_file).namelist()

		__LOG__.Trace('file List of {} : {}'.format(in_file, innerFileList))

		for innerFile in innerFileList :
			if 'csv' != innerFile.rsplit('.', 1)[-1].lower() :
				__LOG__.Trace('innser File is not csv {}'.format(innerFile))
				continue
				
			fileDate	= innerFile.split('.')[0].rsplit('-', 2)[-1]

			try :
				datetime.datetime.strptime(fileDate, '%Y%m')
			except :
				try :
					datetime.datetime.strptime(fileDate, '%Y%m%d')
				except : continue
					
#		dateList.append(innerFile.rsplit('-', 2)[-1])
			dateList.append(fileDate)

		dateList.sort()

		if dateList :
			if len(dateList[0]) == 6 and len(dateList[-1]) == 6 :
				startDate	= datetime.datetime.strptime(dateList[0] + '01', '%Y%m%d').strftime('%Y-%m-%d')
				endDate		= datetime.datetime.strptime(dateList[-1] + str(calendar.monthrange(int(dateList[-1][0:4]), int(dateList[-1][4:6]))[-1]) , '%Y%m%d').strftime('%Y-%m-%d')
			elif len(dateList[0]) == 8 and len(dateList[-1]) == 8 :
				startDate	= datetime.datetime.strptime(dateList[0], '%Y%m%d').strftime('%Y-%m-%d')
				endDate		= datetime.datetime.strptime(dateList[-1], '%Y%m%d').strftime('%Y-%m-%d')
		else :
			startDate	= "error"
			endDate		= "error"

		return trCode, startDate, endDate

	def rdfParsing(self, dirdf, in_file) :
		__LOG__.Trace('xml Dict : \n %s' % dirdf)

		self.rdfConf = ConfigParser.ConfigParser()
		self.rdfConf.read(self.kcbRdfPath)

		dataSetCode, staDate, endDate = self.zipFileListParser(in_file)

#		dateYYYYMM	= os.path.basename(in_file).rsplit('.', 1)[0].rsplit('-', 2)[-1][0:6]				# YYYYMM
#		dataSetCode	= os.path.basename(in_file).rsplit('.', 1)[0]

#		staDate		= datetime.datetime.strptime(dateYYYYMM + '01', '%Y%m%d').strftime('%Y-%m-%d')															# YYYY-MM-01
#		endDate		= datetime.datetime.strptime(calendar.monthrange(dateYYYYMM[0:4], dateYYYYMM[4:6]), '%Y%m%d').strftime('%Y-%m-%d') 						# YYYY-MM-{end DD}

		for key in dirdf['rdf:RDF'].keys() :
			if dirdf['rdf:RDF'][key] == 'http://www.w3.org/ns/dcat#' : dcatns = key.split(':')[1]
			if dirdf['rdf:RDF'][key] == 'http://purl.org/dc/terms/' : dctns = key.split(':')[1]

		dataDict	= json.loads(self.rdfConf.get(self.section, dataSetCode))

		nowDate 	= datetime.datetime.now().strftime(self.dateFormat)

#		dirdf['rdf:RDF']['%s:Catalog' % dcatns]['%s:dataset' % dcatns]['%s:Dataset' % dcatns]['%s:identifier' % dctns] = 'data'
#		dirdf['rdf:RDF']['%s:Catalog' % dcatns]['%s:dataset' % dcatns]['%s:Dataset' % dcatns]['%s:distribution' % dcatns]['%s:Distribution' % dcatns]['%s:issued' % dctns ] = nowDate
#		dirdf['rdf:RDF']['%s:Catalog' % dcatns]['%s:dataset' % dcatns]['%s:Dataset' % dcatns]['%s:distribution' % dcatns]['%s:Distribution' % dcatns]['%s:modified' % dctns ] =nowDate
		
#		dirdf['rdf:RDF']['%s:Catalog' % dcatns]['%s:dataset' % dcatns]['%s:Dataset' % dcatns]['%s:temporal' % dctns]['%s:PeriodOfTime' % dctns]['%s:startDate' % dctns ] = None
#		dirdf['rdf:RDF']['%s:Catalog' % dcatns]['%s:dataset' % dcatns]['%s:Dataset' % dcatns]['%s:temporal' % dctns]['%s:PeriodOfTime' % dctns]['%s:endtDate' % dctns ] = None

#		__LOG__.Trace('Code : %s \n Issued Time : %s \n modified Time : %s \n startDate : %s \n endDate : %s' % (dirdf['rdf:RDF']['%s:Catalog' % dcatns]['%s:dataset' % dcatns]['%s:Dataset' % dcatns]['%s:identifier' % dctns],dirdf['rdf:RDF']['%s:Catalog' % dcatns]['%s:dataset' % dcatns]['%s:Dataset' % dcatns]['%s:distribution' % dcatns]['%s:Distribution' % dcatns]['%s:issued' % dctns ], dirdf['rdf:RDF']['%s:Catalog' % dcatns]['%s:dataset' % dcatns]['%s:Dataset' % dcatns]['%s:distribution' % dcatns]['%s:Distribution' % dcatns]['%s:modified' % dctns ], dirdf['rdf:RDF']['%s:Catalog' % dcatns]['%s:dataset' % dcatns]['%s:Dataset' % dcatns]['%s:temporal' % dctns]['%s:PeriodOfTime' % dctns]['%s:startDate' % dcatns ], dirdf['rdf:RDF']['%s:Catalog' % dcatns]['%s:dataset' % dcatns]['%s:Dataset' % dcatns]['%s:temporal' % dctns]['%s:PeriodOfTime' % dctns]['%s:endDate' % dcatns ] ))
		############### RDF 포맷팅이 정해지면 확인 ##########################

		# <dcat:Catalog>
		dirdf['rdf:RDF']['dcat:Catalog']['dct:identifier']									= dataSetCode
		dirdf['rdf:RDF']['dcat:Catalog']['dct:title']										= dataDict['title']
		dirdf['rdf:RDF']['dcat:Catalog']['dct:description']									= dataDict['Catalog_description']
		dirdf['rdf:RDF']['dcat:Catalog']['dct:language']['@rdf:resource']					= dataDict['Catalog_language']
		dirdf['rdf:RDF']['dcat:Catalog']['foaf:homepage']									= dataDict['Catalog_homepage']

		# <dcat:publisher>
		dirdf['rdf:RDF']['dcat:Catalog']['dct:publisher']['foaf:Organization']['foaf:name']								= dataDict['publisher_name']
		dirdf['rdf:RDF']['dcat:Catalog']['dct:publisher']['foaf:Organization']['foaf:homepage']['@rdf:resource']			= ''
		dirdf['rdf:RDF']['dcat:Catalog']['dct:publisher']['foaf:Organization']['foaf:homepage']['#text']				= dataDict['publisher_homepage']
		dirdf['rdf:RDF']['dcat:Catalog']['dct:publisher']['foaf:Organization']['foaf:mbox']								= dataDict['publisher_mbox']
		dirdf['rdf:RDF']['dcat:Catalog']['dct:publisher']['foaf:Organization']['foaf:phone']							= dataDict['publisher_phone']

		# <dct:issued> 
		dirdf['rdf:RDF']['dcat:Catalog']['dct:issued']		= ''
		# <dct:modified>
		dirdf['rdf:RDF']['dcat:Catalog']['dct:modified']	= ''

		# <dcat:Dataset>
		dirdf['rdf:RDF']['dcat:Catalog']['dcat:dataset']['dcat:Dataset']['dcat:spatialResolutionInMeters'] 										 = ''
		dirdf['rdf:RDF']['dcat:Catalog']['dcat:dataset']['dcat:Dataset']['dct:language']['@rdf:resource'] 										 = dataDict['language']
		dirdf['rdf:RDF']['dcat:Catalog']['dcat:dataset']['dcat:Dataset']['dct:identifier']														 = dataSetCode
		dirdf['rdf:RDF']['dcat:Catalog']['dcat:dataset']['dcat:Dataset']['dct:accessRights']													 = dataDict['accessRights']
		dirdf['rdf:RDF']['dcat:Catalog']['dcat:dataset']['dcat:Dataset']['dcat:theme']															 = dataDict['theme']
		dirdf['rdf:RDF']['dcat:Catalog']['dcat:dataset']['dcat:Dataset']['dct:title']															 = dataDict['title']
		dirdf['rdf:RDF']['dcat:Catalog']['dcat:dataset']['dcat:Dataset']['dct:description']														 = dataDict['description']

		# <dcat:Dataset> <dcat:contactPoint><'vcard:Individual'>
		dirdf['rdf:RDF']['dcat:Catalog']['dcat:dataset']['dcat:Dataset']['dcat:contactPoint']['vcard:Individual']['vcard:fn']								= dataDict['indi_fn']
		dirdf['rdf:RDF']['dcat:Catalog']['dcat:dataset']['dcat:Dataset']['dcat:contactPoint']['vcard:Individual']['vcard:hasEmail']['@rdf:resource']		= ''
		dirdf['rdf:RDF']['dcat:Catalog']['dcat:dataset']['dcat:Dataset']['dcat:contactPoint']['vcard:Individual']['vcard:hasEmail']['#text']				= dataDict['indi_hasEmail']
		dirdf['rdf:RDF']['dcat:Catalog']['dcat:dataset']['dcat:Dataset']['dcat:contactPoint']['vcard:Individual']['vcard:hasTelephone']['@rdf:resource']	= ''
		dirdf['rdf:RDF']['dcat:Catalog']['dcat:dataset']['dcat:Dataset']['dcat:contactPoint']['vcard:Individual']['vcard:hasTelephone']['#text']			= dataDict['indi_hasTelephone']

		# <dcat:Dataset><dct:creator><foaf:Organization>
		dirdf['rdf:RDF']['dcat:Catalog']['dcat:dataset']['dcat:Dataset']['dct:creator']['foaf:Organization']['foaf:name']										= dataDict['creator_name']
		dirdf['rdf:RDF']['dcat:Catalog']['dcat:dataset']['dcat:Dataset']['dct:creator']['foaf:Organization']['foaf:homepage']['@rdf:resource']					= ''
		dirdf['rdf:RDF']['dcat:Catalog']['dcat:dataset']['dcat:Dataset']['dct:creator']['foaf:Organization']['foaf:homepage']['#text']							= dataDict['creator_homepage']
		dirdf['rdf:RDF']['dcat:Catalog']['dcat:dataset']['dcat:Dataset']['dct:creator']['foaf:Organization']['foaf:mbox']										= dataDict['creator_mbox']
		dirdf['rdf:RDF']['dcat:Catalog']['dcat:dataset']['dcat:Dataset']['dct:creator']['foaf:Organization']['foaf:phone']										= dataDict['creator_phone']


		dirdf['rdf:RDF']['dcat:Catalog']['dcat:dataset']['dcat:Dataset']['dcat:landingPage']																	= ''
		dirdf['rdf:RDF']['dcat:Catalog']['dcat:dataset']['dcat:Dataset']['dct:issued']																			= ''
		dirdf['rdf:RDF']['dcat:Catalog']['dcat:dataset']['dcat:Dataset']['dct:modified']																		= ''

		# <dcat:Dataset><dct:publisher><foaf:Organization>
		dirdf['rdf:RDF']['dcat:Catalog']['dcat:dataset']['dcat:Dataset']['dct:publisher']['foaf:Organization']['foaf:name']										= dataDict['publisher_name']
		dirdf['rdf:RDF']['dcat:Catalog']['dcat:dataset']['dcat:Dataset']['dct:publisher']['foaf:Organization']['foaf:homepage']									= dataDict['publisher_homepage']
		dirdf['rdf:RDF']['dcat:Catalog']['dcat:dataset']['dcat:Dataset']['dct:publisher']['foaf:Organization']['foaf:mbox']										= dataDict['publisher_mbox']
		dirdf['rdf:RDF']['dcat:Catalog']['dcat:dataset']['dcat:Dataset']['dct:publisher']['foaf:Organization']['foaf:phone']									= dataDict['publisher_phone']

		# <dcat:Dataset><dcat:keyword>
		for idx, keyword in enumerate(dataDict['keyword_list'].split(',')) :
			dirdf['rdf:RDF']['dcat:Catalog']['dcat:dataset']['dcat:Dataset']['dcat:keyword'][idx]		= keyword

		# <dcat:Dataset><dct:license>
		dirdf['rdf:RDF']['dcat:Catalog']['dcat:dataset']['dcat:Dataset']['dct:license']					= dataDict['license']

		# <dcat:Dataset><dct:type>
		dirdf['rdf:RDF']['dcat:Catalog']['dcat:dataset']['dcat:Dataset']['dct:type']					= dataDict['type']

		# <dcat:Dataset><dcat:temporalResolution>
		dirdf['rdf:RDF']['dcat:Catalog']['dcat:dataset']['dcat:Dataset']['dcat:temporalResolution']		= dataDict['temporalResolution']
		dirdf['rdf:RDF']['dcat:Catalog']['dcat:dataset']['dcat:Dataset']['dct:accrualPeriodicity']		= dataDict['accrualPeriodicity']

		# <dcat:Dataset><dct:temporal><dcat:PeriodOfTime>
		dirdf['rdf:RDF']['dcat:Catalog']['dcat:dataset']['dcat:Dataset']['dct:temporal']['dct:PeriodOfTime']['dct:startDate']			= staDate
		dirdf['rdf:RDF']['dcat:Catalog']['dcat:dataset']['dcat:Dataset']['dct:temporal']['dct:PeriodOfTime']['dct:endDate']				= endDate

		# <dcat:Dataset><dcat:distribution><dcat:Distribution>
		dirdf['rdf:RDF']['dcat:Catalog']['dcat:dataset']['dcat:Dataset']['dcat:distribution']['dcat:Distribution']['dct:format']								= dataDict['format']
		dirdf['rdf:RDF']['dcat:Catalog']['dcat:dataset']['dcat:Dataset']['dcat:distribution']['dcat:Distribution']['dcat:byteSize']['@rdf:datatype']				= ''
		dirdf['rdf:RDF']['dcat:Catalog']['dcat:dataset']['dcat:Dataset']['dcat:distribution']['dcat:Distribution']['dct:title']									= dataDict['title']
		dirdf['rdf:RDF']['dcat:Catalog']['dcat:dataset']['dcat:Dataset']['dcat:distribution']['dcat:Distribution']['dct:description']							= dataDict['description']
		dirdf['rdf:RDF']['dcat:Catalog']['dcat:dataset']['dcat:Dataset']['dcat:distribution']['dcat:Distribution']['dct:issued']								= ''
		dirdf['rdf:RDF']['dcat:Catalog']['dcat:dataset']['dcat:Dataset']['dcat:distribution']['dcat:Distribution']['dct:modified']								= ''
		dirdf['rdf:RDF']['dcat:Catalog']['dcat:dataset']['dcat:Dataset']['dcat:distribution']['dcat:Distribution']['dcat:temporalResolution']					= dataDict['temporalResolution']
		dirdf['rdf:RDF']['dcat:Catalog']['dcat:dataset']['dcat:Dataset']['dcat:distribution']['dcat:Distribution']['dcat:accessURL']							= ""
		dirdf['rdf:RDF']['dcat:Catalog']['dcat:dataset']['dcat:Dataset']['dcat:distribution']['dcat:Distribution']['dcat:compressFormat']						= dataDict['compressFormat']
		dirdf['rdf:RDF']['dcat:Catalog']['dcat:dataset']['dcat:Dataset']['dcat:distribution']['dcat:Distribution']['dct:license']								= dataDict['license']
		dirdf['rdf:RDF']['dcat:Catalog']['dcat:dataset']['dcat:Dataset']['dcat:distribution']['dcat:Distribution']['dcat:accessService']						= dataDict['accessService']
		dirdf['rdf:RDF']['dcat:Catalog']['dcat:dataset']['dcat:Dataset']['dcat:distribution']['dcat:Distribution']['dct:downloadURL']							= self.sftpPutPath.format(self.centerUserId, dataSetCode, nowDate, dataSetCode)
		dirdf['rdf:RDF']['dcat:Catalog']['dcat:dataset']['dcat:Dataset']['dcat:distribution']['dcat:Distribution']['dcat:mediaType']							= mimetypes.guess_type(in_file)[1]
		# dact -> dcat 20210504
		dirdf['rdf:RDF']['dcat:Catalog']['dcat:dataset']['dcat:Dataset']['dcat:distribution']['dcat:Distribution']['dcat:accessRights']							= dataDict['accessRights']
		dirdf['rdf:RDF']['dcat:Catalog']['dcat:dataset']['dcat:Dataset']['dcat:distribution']['dcat:Distribution']['dcat:spatialResolutionInMeters']				= dataDict['spatialResolutionInMeters']
		dirdf['rdf:RDF']['dcat:Catalog']['dcat:dataset']['dcat:Dataset']['dcat:distribution']['dcat:Distribution']['dct:rights']								= dataDict['rights']

		###################################################################

		self.rdfWrite(dirdf, in_file)

	def processing(self, in_file) :

		__LOG__.Trace( "processing : %s" % in_file )

		dirdf = None

		if self.encodeFlag :
			__LOG__.Trace('encoding xml : %s' % self.encodeLan)
			with open(self.rdfTempPath, 'r', encoding=self.encodeLan) as rdfInput :
				dirdf = xmltodict.parse(str(rdfInput.read()))
		else :
			with open(self.rdfTempPath, 'r') as rdfInput :
				dirdf = xmltodict.parse(str(rdfInput.read()))

		self.rdfParsing(dirdf, in_file)

		uploadTime	= datetime.datetime.now().strftime("%Y%%m%d%H%M%S")

		# SFTP 중복을 막기 위한 sleep
		time.sleep(1)
		stdout( '{}://{}'.format('RESULT', in_file) )
#		self.logSend(in_file)

	def run(self):

		while not SHUTDOWN :
#			self.start_time         = datetime.datetime.now().strftime('%Y%m%d') + '000000'
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

				if prefix != 'zip' :
					is_std_err = True
					__LOG__.Trace('Prefix is not match : %s' % prefix)
					continue	
				
				if not os.path.exists( in_file )  :
					is_std_err = True
					__LOG__.Trace('Not found file : %s' % in_file)
					continue

				stime = time.time()

#				self.std_in             = in_file
#				self.in_file_size       = str(os.path.getsize(in_file))
#				if in_file.upper().endswith('.CSV') or in_file.upper().endswith('.DAT') :
#					#self.in_file_row_cnt	= subprocess.check_output(["wc","-l", in_file]).split()[0]
#					self.in_file_row_cnt   = subprocess.getstatusoutput('/usr/bin/wc -l %s' % in_file)[-1].split()[0]

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

	module 		= os.path.basename(sys.argv[0])
	optionEn 	= False

	if len(sys.argv) < 3:
		sys.stderr.write('Usage 	: %s conf {option:[[log_arg]-d]}\n' % module )
		sys.stderr.write('Usage 	: %s conf {option:[[log_arg]-d]}\n' % module )
		#python3 /home/test/Project_name/bin/py3/BaseModule.py SECTION /home/test/Project_name/conf/BaseModule.conf
		#python3 /home/test/Project_name/bin/py3/BaseModule.py SECTION /home/test/Project_name/conf/BaseModule.conf 0 
		#python3 /home/test/Project_name/bin/py3/BaseModule.py SECTION /home/test/Project_name/conf/BaseModule.conf -d
		sys.stderr.flush()
		os._exit(1)

	if len(sys.argv) > 3 and '-e' in sys.argv :
		optionEn = True

	section	 = sys.argv[2]
	config_file = sys.argv[1]

	conf = ConfigParser.ConfigParser()
	conf.read(config_file)

	if '-d' not in sys.argv :

		etc_argv = sys.argv[3:]
		log_arg = ''

		if len(sys.argv[3:]) > 0 :
			log_arg = '_' + sys.argv[3]

		log_path = conf.get('GENERAL', 'LOG_PATH')
#		log_path = '/home/test/user/KimJW/log'

		makedirs( log_path )

		log_file = os.path.join(log_path, '%s_%s%s.log' % (os.path.splitext(module)[0], section, log_arg ))
		Log.Init(Log.CRotatingLog(log_file, 10240000, 9))		

	else:
		Log.Init()		

	pid = os.getpid()	
	__LOG__.Trace('============= %s START [pid:%s]==================' % ( module, pid ))

	RDFParser(conf, section, optionEn).run()

	__LOG__.Trace('============= %s END [pid:%s]====================' % (module, pid ))


#- if name start ----------------------------------------------
if __name__ == "__main__" :
	main()


