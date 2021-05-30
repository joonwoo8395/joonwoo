#!/usr/bin/env python
#-*- coding:utf-8 -*-
import os
import sys
import json
import Mobigen.Common.Log as Log;
import ConfigParser
import API.M6 as M6

class IRIS_SQL :
	def __init__(self, IRIS,IRIS_ID,IRIS_PASS) :
		self.IRIS = IRIS
		self.IRIS_ID = IRIS_ID
		self.IRIS_PASS = IRIS_PASS

	def updateIdx( self ) :
		conn = None
		try :
			conn = M6.Connection(self.IRIS, self.IRIS_ID, self.IRIS_PASS, Database ='tacs')
			cursor = conn.cursor()

			sql = """ UPDATE TACS.TACS_WORK_INFO_IDX 
					SET IDX = IDX + 1 """

			cursor.Execute2(sql)

		except :
			__LOG__.Exception()

		finally :
			if conn :	conn.close()


	def selectIDX ( self ) :
		conn	= None
		result	= ''
		try :
			conn = M6.Connection(self.IRIS, self.IRIS_ID, self.IRIS_PASS, Database ='tacs')
			cursor = conn.cursor()
			
			sql = """ SELECT IDX
					FROM TACS.TACS_WORK_INFO_IDX
				"""

			cursor.Execute2(sql)

			for oneRaw in cursor :
				result = oneRaw[0]


		except :
			__LOG__.Exception()

		finally :
			if conn : conn.close()

		return result

	def selectWorkInfoCnt (self, workId) :
		__LOG__.Trace('workId = %s' % workId )
		conn	= None
		result	= 0
		try :
			conn = M6.Connection(self.IRIS, self.IRIS_ID, self.IRIS_PASS, Database ='tacs')
			cursor = conn.cursor()

			sql = """
					SELECT COUNT(*) 
					FROM TACS.TACS_WORK_INFO
					WHERE WORK_ID = '%s'
					"""  % workId
			
			cursor.Execute2(sql)

			for oneRaw in cursor :
				result = int(oneRaw.encode('utf-8'))

		except :
			__LOG__.Exception()

		finally :
			if conn : conn.close()

		return result

	def selectIpEmsData ( self, emsIp ) :
		__LOG__.Trace( 'svrIp = %s' % emsIp )
		resultDict = {'vendor' : '', 'eqpTyp' : ''}
		conn	= None
		try :
			conn 	= M6.Connection(self.IRIS, self.IRIS_ID, self.IRIS_PASS, Database = 'tacs' )
			cursor 	= conn.cursor()

			sql = """
					SELECT SPLY_BP_ID, EQP_TYP
					FROM TACS.TNG_IM_EQP_BAS
					WHERE REP_IP_ADDR = '%s'
					""" % emsIp

			resultMsg = cursor.Execute2(sql)

			__LOG__.Trace(resultMsg)

			for oneRaw in cursor :
				resultDict['vendor'] = oneRaw[0].encode('utf-8')
				resultDict['eqpTyp'] = oneRaw[1].encode('utf-8')
		except :
			__LOG__.Exception()

		finally :
			if conn : conn.close()

		return resultDict['vendor'], resultDict['eqpTyp']

	def selectIpEqpData ( self, svrIpList ) :
		__LOG__.Trace('svrIp = %s' % svrIpList)
		conn	= None
		result	= ''
		try :
			conn 	= M6.Connection(self.IRIS, self.IRIS_ID, self.IRIS_PASS, Database = 'tacs' )
			cursor 	= conn.cursor()

			ipList = '\'%s\'' % '\',\''.join(svrIpList)
			__LOG__.Trace(ipList)
				
			sql = """
					SELECT EQP_EMS_NM 
					FROM TACS.TNG_IM_EQP_BAS
					WHERE REP_IP_ADDR IN (%s)
					""" % ipList

			resultMsg = cursor.Execute2(sql)

			__LOG__.Trace(resultMsg)

			if 'OK' in  resultMsg :

				for oneRaw in cursor :
					result		= oneRaw[0].encode('utf-8')

			else :
				__LOG__.Trace('Query Fail!! ')

		except :
			__LOG__.Exception()

		finally	:
			if conn : conn.close()

		return result

	def selectNmEmsData (self, emsNm) :
		__LOG__.Trace('emsNm = %s' % emsNm)
		conn 		= None
		resultDict = {'vendor' : '', 'eqpTyp' : ''}
		try :
			conn = M6.Connection(self.IRIS, self.IRIS_ID, self.IRIS_PASS, Database='tacs')
			cursor = conn.cursor()	

			#ipList = '\'%s\'' % '\',\''.join(svrIpList)

			sql = """SELECT SPLY_BP_ID, EQP_TYP
				FROM TACS.TNG_IM_EQP_BAS 
				WHERE EQP_NM = '%s' """ % emsNm

			resultMsg	= cursor.Execute2(sql)

			for oneRaw in cursor :
				resultDict = oneRaw[0].encode('utf-8')
				resultDict = oneRaw[1].encode('utf-8')
	
		except :
			__LOG__.Exception()

		finally :
			if conn : conn.close()

		return resultDict['vendor'], resultDict['eqpTyp']

	def deleteWorkEqpInfo ( self, workId, idx, key, partition ) :
		__LOG__.Trace('key : %s | partition : %s | workId : %s | idx : %s' % ( key, partition ,workId, idx ) )

		conn = None
		try :
			conn = M6.Connection(self.IRIS, self.IRIS_ID, self.IRIS_PASS,Database='tacs')
			cursor = conn.cursor()

			workSql = """/*+ LOCATION( key = '%s' AND PATITION = '%s' ) */
					DELETE 
					FROM TACS.TACS_WORK_INFO
					WHERE IDX = '%s' and WORK_ID = '%s' 
					""" %( key, partition, idx, workId )

			resultWorkMsg	= cursor.Execute2(workSql)

			eqpSql = """/*+ LOCATION( key = '%s' AND PATITION = '%s' ) */
					DELETE 
					FROM TACS.TACS_WORK_INFO
					WHERE IDX = '%s' and WORK_ID = '%s'
				""" %( key, partition, idx, workId )

			resultEqpMsg	= cursor.Execute2(eqpSql)

		except :
			__LOG__.Exception()

		finally :
			if conn : conn.close()

	
	def selectNmEqpData (self, eqpNm) :
		__LOG__.Trace('eqpNm = %s' %eqpNm)
		conn = None
		result = ''
		try :
			conn = M6.Connection(self.IRIS, self.IRIS_ID, self.IRIS_PASS,Database='tacs')
			cursor = conn.cursor()

			sql = """SELECT EQP_EMS_NM
					FROM TACS.TNG_IM_EQP_BAS
	      		    WHERE EQP_NM ='%s'""" % str(eqpNm)

			cursor.Execute2(sql)

	 		for oneRaw in cursor :
				result     = oneRaw[0].encode('utf-8')

		except :
			__LOG__.Exception()

		finally:
			if conn : conn.close()

		return result

	def selectDate (self, workId ,workStaDate) :
		workIdKey	= workId[-1]
		__LOG__.Trace('workIdKey = %s / workId = %s / workStaDate = %s' %( workIdKey, workId, workStaDate ) )
		result = ''
		conn = None
		try :
			conn = M6.Connection(self.IRIS, self.IRIS_ID, self.IRIS_PASS, Database='tacs')
			cursor = conn.cursor()

			sql1 = """/*+ LOCATION( key = '%s' and partition = '%s' ) */
					SELECT MAX(IDX+0)
					FROM TACS.TACS_WORK_INFO
					WHERE WORK_ID = '%s'
					""" % ( workIdKey, workStaDate, workId )

			cursor.Execute2(sql1)

			IDX = ''

			for oneRaw in cursor :
				IDX = oneRaw[0]

			sql2 = """/*+ LOCATION( key = '%s' and partition = '%s' ) */
					SELECT LAST_CHG_DATE
					FROM TACS.TACS_WORK_INFO 
					WHERE IDX = '%s' """ % ( workIdKey, workStaDate , IDX )

			cursor.Execute2(sql2)

			lastChgDate = ''

			for oneRaw in cursor :
				result	= oneRaw[0].encode('utf-8')

			__LOG__.Trace(lastChgDate)

			cursor.Close()
			conn.close()

		except :

			__LOG__.Exception()

		finally : 
			if conn : conn.close()

		return result

	def selectLkngUnit (self, emsIp) :
		__LOG__.Trace('emsIp : %s' % emsIp)
		conn = None
		resultDict = {'mqNm' : '', 'unitDistYn' : ''}
		try :
			conn = M6.Connection(self.IRIS, self.IRIS_ID, self.IRIS_PASS, Database='tacs')
			cursor = conn.cursor()
			sql = "SELECT WORK_MQ_NM, UNIT_DIST_YN FROM TACS.TACS_TB_LNKG_UNIT WHERE EMS_IP = '%s'" % str(emsIp)
			cursor.Execute2(sql)

			for oneRaw in cursor :
				resultDict['mqNm']			= oneRaw[0].encode('utf-8')
				resultDict['unitDistYn']	= oneRaw[1].encode('utf-8')

		except :
			__LOG__.Exception()

		finally:
			if conn : conn.close()

		return resultDict

