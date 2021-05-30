#!/usr/bin/env python
# -*- coding: utf-8 -*-
import datetime
import os
import sys
import signal
import time
import ConfigParser
import glob
import json
import Mobigen.Common.Log as Log; Log.Init()

import API.M6 as M6

def handler(signum, frame):
	__LOG__.info('signal: process shutdown')


# SIGTERM
signal.signal(signal.SIGTERM, handler)
# SIGINT
signal.signal(signal.SIGINT, handler)
# SIGHUP
signal.signal(signal.SIGHUP, handler)
# SIGPIPE
signal.signal(signal.SIGPIPE, handler)


class IrisMirgration :
	def __init__(self, cfg, section):
		self.section = section
		self.cfg = cfg
		self.IRIS = self.cfg.get(self.section , 'IRIS')
		self.IRIS_ID = self.cfg.get(self.section , 'IRIS_ID')
		self.IRIS_PASS = self.cfg.get(self.section , 'IRIS_PASS')

		self.conn = None
		self.cursor = None
		__LOG__.Trace('start!')

	def stdout(self, msg):
		sys.stdout.write(msg+'\n')
		sys.stdout.flush()
		# print(msg, file=sys.stdout)
		__LOG__.Trace('OUT: %s' % msg)
		
	def disConnect(self,conn,cursor) :
		if cursor != None:
			try : cursor.close()
			except : pass
		if conn != None :	
			try : conn.close()
			except : pass

	def initConnect(self) :
		self.conn = M6.Connection(self.IRIS, self.IRIS_ID, self.IRIS_PASS, Database='tacs')
		__LOG__.Trace('IRIS Connect!')
		try :
			self.cursor = self.conn.cursor()
			self.cursor.SetFieldSep('|§|')
			self.cursor.SetRecordSep('|§-§|')
			#self.cursor.SetFieldSep('|^|')
			#self.cursor.SetRecordSep('|^-^|')
		except :
			__LOG__.Exception()
		finally :
			self.conn.commit()

	def getMigrationTableList(self) :
		tableList = self.cfg.get(self.section , 'MIGRATION_TABLES')
		
		return tableList.split(',')

	def getSelectDataList(self, table) :
		cursor			= self.cursor
		table 			= table.strip()

#		queryLimit		= 'SELECT * FROM %s limit 0;'

		sql				= """TABLE LIST %s""" 

		cursor.Execute2(sql % table)

		result 		= list()
		tableData	= dict()

		for raw in cursor :	
			tableData['scope'] 	= raw[2].encode('utf-8')
			tableData['key']	= raw[5].encode('utf-8')
			tableData['part']	= raw[6].encode('utf-8')

#		cursor.Execute2(queryLimit % table )

#		columnList = cursor.Metadata()['ColumnName']

#		query           = 'SELECT %s FROM %s GROUP BY IDX;'

#		query           = """
#				/*+ LOCATION ( PARTITION >= '20190929000000' AND PARTITION < '20190930000000' ) */ 
#				SELECT %s FROM %s where LNKG_DMN_DIV_CD != '111' ;"""

		query			= """
							SELECT %s FROM %s
						"""

		columnList = self.cfg.get( self.section, table )

		__LOG__.Trace(columnList)

		cursor.Execute2(query % (columnList, table) )

		for raw in cursor :
			r = '|§|'.join(raw).encode('utf-8')
			result.append(r)

		return columnList, result, tableData

	def makeCTLFile(self, table, columnList) : 
		#for idx in range(0, len(columnList)) :
		#	columnList[idx] = str(columnList[idx]).encode('utf-8')

		if not os.path.exists(table) : os.mkdir(table)
		
		ctlFile = open(os.path.join(table, 'column.ctl'), 'w')
		ctlFile.write(columnList.strip().replace(',',  '|§-§|'))
#		ctlFile.write('|^-^|'.join(columnList))
		ctlFile.close()

	def makeDatFile(self, table, dataList, tableData) :
		if len(dataList) == 0 :
			__LOG__.Trace('DataList Size zero')
		else :
			if not os.path.exists(table) : os.mkdir(table)

			if tableData['scope'].upper() == 'LOCAL' :		
				cntData		= open(os.path.join(table, 'cntData.txt'),'w')
				cntData.write(str(len(dataList)))
				cntData.close()

				for oneRawData in dataList :
					oneRawList	= oneRawData.split('|§|')
					key 		= oneRawList[0]
					evntDate 	= oneRawList[1]

					partitionDate	= datetime.datetime.strptime(evntDate, '%Y%m%d%H%M%S')
					partition		= datetime.datetime.strftime(partitionDate, '%Y%m%d%H0000')
					
					#dataDict 	= dict()
					datFilePath = os.path.join(table, '_'.join(['data.dat', key, partition]))
					
					if os.path.exists(datFilePath) :
						oneRawData = ''.join(['|§-§|', oneRawData])

					datFile 	= open(os.path.join(table, '_'.join(['data.dat', key, partition])), 'a+')
					
					#dataDict["key"]	= tableData["key"]
					#dataDict["part"] 	= tableData["part"]
					#dataDict["data"]	= dataList[listNum]

					datFile.write(str(oneRawData))
					datFile.close()

			elif tableData['scope'].upper() == 'GLOBAL' :
				datFile     = open(os.path.join(table, 'data.dat'), 'w')
				datFile.write('|§-§|'.join(dataList))
				datFile.close()


	def run(self):
		__LOG__.Trace('IrisMigration start!!')
		try : 
			self.initConnect()
			for table in self.getMigrationTableList() :
				columnList, dataList, tableData = self.getSelectDataList(table)
				print columnList
				self.makeCTLFile(table, columnList)
				self.makeDatFile(table, dataList, tableData)
		except :
			__LOG__.Exception()

		self.disConnect(self.conn, self.cursor)

	def loadData(self, table) :
		__LOG__.Trace('Load to Table : %s' % table)

		try :
			self.initConnect()	

			fileList = os.listdir(table)
			fileList.sort()

			datFileList = list()

			for oneDatFile in fileList :
				if oneDatFile.find('data.dat') is not -1 :
					datFileList.append(oneDatFile)

			__LOG__.Trace('datFile : %s | datFileCnt : %s' % (datFileList, len(datFileList) ) )
			if len(datFileList) == 1 :

				print 'Global!!'
				try :
					resultGlobal = self.cursor.LoadGlobal(table, os.path.join(table, 'column.ctl') , os.path.join(table, datFileList[0] ) )
					print resultGlobal

				except :
					__LOG__.Exception()
			else :
				for oneDatFile in datFileList :

					f = open (os.path.join(table, oneDatFile ), 'r' )

					name, key, partition  =  oneDatFile.split('_')

					fkp = f. read()
					# key 값을 찾아서 인덱스 입력 

					# partition 값을 찾아서 인덱스 입력

					result = self.cursor.Load(table, key, partition, os.path.join(table, 'column.ctl'), os.path.join(table, oneDatFile) )

					print ( 'LOAD RESULT : %s' % result)

				
		except :
			__LOG__.Exception()

		self.disConnect(self.conn, self.cursor)

def main():
	module = os.path.basename(sys.argv[0])
	
	section = 'IRIS_MIGRATION' 
	cfgfile = '/home/tacs/user/KimJW/Migration/migration.conf'

	cfg = ConfigParser.ConfigParser()
	cfg.read(cfgfile)

	log_file = '%s_%s.log' % (module, section)
	Log.Init(Log.CRotatingLog(log_file, 1000000, 9))

	im = IrisMirgration(cfg, section)
	loadTable = None

	if len(sys.argv) > 1 : 
		loadTable = sys.argv[1]
		im.loadData(loadTable)
	else :
		im.run()

	__LOG__.Trace('end main!')


if __name__ == '__main__':
	try:
		main()
	except:
		__LOG__.Exception('main error')
