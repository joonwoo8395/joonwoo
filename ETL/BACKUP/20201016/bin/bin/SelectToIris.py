#!/usr/bin/env python
#-*- coding:utf-8 -*-
import Mobigen.Common.Log as Log;
import API.M6 as M6

class SelectToIris :
	def __init__ (self, cfg) :
		
		self.IRIS_IP 		= cfg.get('IRIS', 'IRIS_IP')
		self.IRIS_ID 		= cfg.get('IRIS', 'IRIS_ID')
		self.IRIS_PW		= cfg.get('IRIS', 'IRIS_PW')
		self.IRIS_DB 		= cfg.get('IRIS', 'IRIS_DB')

	def selectCheckTime(self, nowDate) :
		checkCnt = ''
		try :
			conn = M6.Connection(self.IRIS_IP, self.IRIS_ID, self.IRIS_PW, Database= self.IRIS_DB)
			cursor = conn.cursor()

			sql = """
				SELECT 
					COUNT(*)
				FROM 
					TACS.TACS_EMS_CONNECTION_CHECK
				WHERE 
					EVNT_DATE = '%s'
				""" % nowDate

			cursor.Execute2(sql)

			for result in cursor :
				checkCnt = result[0].encode('utf-8')
		except :
			__LOG__.Exception()
		
		finally :
			cursor.Close()
			conn.close()

		return checkCnt

	def deleteCheck (self, nowDate) :
		try :
			conn = M6.Connection(self.IRIS_IP, self.IRIS_ID, self.IRIS_PW, Database= self.IRIS_DB)
			cursor = conn.cursor()

			sql = """
				DELETE FROM
					TACS.TACS_EMS_CONNECTION_CHECK
				WHERE
					EVNT_DATE = '%s'
				""" % nowDate
			cursor.Execute2(sql)
		except :
			__LOG__.Exception()
		finally :
			cursor.Close()
			conn.close()

	def insertCheck (self, nowDate) :
		try :
			conn = M6.Connection(self.IRIS_IP, self.IRIS_ID, self.IRIS_PW, Database= self.IRIS_DB)
			cursor = conn.cursor()

			sql = """
				INSERT INTO TACS.TACS_EMS_CONNECTION_CHECK(EVNT_DATE)
				VALUES ('%s')
				""" % nowDate
			cursor.Execute2(sql)
		except :
			__LOG__.Exception()

		finally :
			cursor.Close()
			conn.close()

		

	def selectEmsIp (self) :
		try :
			conn = M6.Connection(self.IRIS_IP, self.IRIS_ID, self.IRIS_PW, Database= self.IRIS_DB)
			cursor = conn.cursor()

			sql = """
				SELECT WORK_MQ_NM, REAL_MQ_NM, EMS_IP, UNIT_DIST_YN, REG_DATE, REG_USER_ID, FIRST_CONN_DATE, LAST_CONN_DATE, CONN_YN
				FROM TACS_TB_LNKG_UNIT
				"""
	
			cursor.Execute2(sql)
			resultEmsDataList = []

			for resultData in cursor :				
				resultEmsDataDict = {"workMqNm" : "", "realMqNm" : "", "emsIp" : "", "unitDistYn" : "", "regDate" : "", "regUserId" : "", "firstConnDate" : "", "lastConnDate" : "", "connYn" : ""}
				resultEmsDataDict["workMqNm"]		= resultData[0].encode('utf-8')
				resultEmsDataDict["realMqNm"]		= resultData[1].encode('utf-8')
				resultEmsDataDict["emsIp"]			= resultData[2].encode('utf-8')
				resultEmsDataDict["unitDistYn"]		= resultData[3].encode('utf-8')
				resultEmsDataDict["regDate"]		= resultData[4].encode('utf-8')
				resultEmsDataDict["regUserId"]		= resultData[5].encode('utf-8')
				resultEmsDataDict["firstConnDate"]	= resultData[6].encode('utf-8')
				resultEmsDataDict["lastConnDate"]	= resultData[7].encode('utf-8')
				resultEmsDataDict["connYn"]			= resultData[8].encode('utf-8')

				resultEmsDataList.append(resultEmsDataDict)

			cursor.Close()
			conn.close()
			return resultEmsDataList
		except :
			__LOG__.Exception()

	def updateConn(self, connList, nowDate) :
		conn = M6.Connection(self.IRIS_IP, self.IRIS_ID, self.IRIS_PW, Database= self.IRIS_DB)
		cursor = conn.cursor()

		ipList = '\'%s\'' % '\',\''.join(connList)

		sql 		= """ 
						UPDATE TACS_TB_LNKG_UNIT 
						SET CONN_YN = 'Y', LAST_CONN_DATE = '%s'
						WHERE EMS_IP IN (%s)
						""" % (nowDate, ipList)
		result = cursor.Execute2(sql)

		__LOG__.Trace('Connect Update : %s ' % result)

		if cursor :
			cursor.Close()
		if conn :
			conn.close()

	def updateDisConn(self, disConnList) :
		conn = M6.Connection(self.IRIS_IP, self.IRIS_ID, self.IRIS_PW, Database= self.IRIS_DB)
		cursor = conn.cursor()

		ipList = '\'%s\'' % '\',\''.join(disConnList)

		sql 		= """ 
						UPDATE TACS_TB_LNKG_UNIT 
						SET CONN_YN = 'N'
						WHERE EMS_IP IN (%s)
						""" % ipList
		result = cursor.Execute2(sql)

		__LOG__.Trace('DisConnect Update : %s ' % result)

		if cursor :
			cursor.Close()
		if conn :
			conn.close()

	def updateFirDate(self, connList, nowDate) :
		result = ''
		try :
			conn = M6.Connection(self.IRIS_IP, self.IRIS_ID, self.IRIS_PW, Database= self.IRIS_DB)
			cursor = conn.cursor()

			ipList = '\'%s\'' % '\',\''.join(connList)
	
			sql 		= """ 
						UPDATE TACS_TB_LNKG_UNIT 
						SET CONN_YN = 'Y', FIRST_CONN_DATE = '%s' ,LAST_CONN_DATE = '%s'
						WHERE EMS_IP IN (%s)
						""" % (nowDate, nowDate, ipList)
			result = cursor.Execute2(sql)

			__LOG__.Trace('FirstDate Update : %s' % result)

			if cursor :
				cursor.Close()
			if conn :
				conn.close()
		except :
			__LOG__.Exception()

		return result

	def updateEmsConnectCheck (self, emsIp, nowDate, firstConnDate, connectCheck) :	
		__LOG__.Trace( 'emsIp : %s , nowDate : %s, firstConnDate : %s' % (emsIp, nowDate, firstConnDate) )
		result = ''	
		try :
			conn = M6.Connection(self.IRIS_IP, self.IRIS_ID, self.IRIS_PW, Database= self.IRIS_DB)
			cursor = conn.cursor()
			sql 		= 'UPDATE TACS_TB_LNKG_UNIT '
			updateData	= list()
			whereData	= list()
			updateData.append(" SET CONN_YN = '%s'" % connectCheck)
			whereData.append(" WHERE EMS_IP = '%s'" % str(emsIp))

			if ( firstConnDate == None or firstConnDate == '' ) and 'Y' == connectCheck :
				updateData.append(" FIRST_CONN_DATE = '%s'" % str(nowDate))
			
			if 'Y' == connectCheck :
				updateData.append(" LAST_CONN_DATE = '%s'"  % str(nowDate))

			updateQuery	= ' '.join([sql, ','.join(updateData), ','.join(whereData)])

			result = cursor.Execute2(updateQuery)

			cursor.Close()
			conn.close()
		except :
			__LOG__.Exception()

		return result

	def updateEmsDisConnect (self, emsIp, connectCheck) :
		__LOG__.Trace('emsIp : %s' %emsIp)
		result = ''
		try :
			conn = M6.Connection(self.IRIS_IP, self.IRIS_ID, self.IRIS_PW, Database= self.IRIS_DB)
			cursor = conn.cursor()

			sql = """
				UPDATE TACS_TB_LNKG_UNIT
				SET CONN_YN = '%s'
				WHERE EMS_IP = '%s' """ %( connectCheck, str(emsIp), connectCheck )
	
			result =  cursor.Execute2(sql)

			cursor.Close()
			conn.close()
		except :
			__LOG__.Exception()

		return result



