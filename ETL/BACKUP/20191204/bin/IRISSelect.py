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
		workId = workdId.strip()
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
		emsIp = emsIp.strip()
		__LOG__.Trace( 'emsIp = %s' % emsIp )
		resultDict = {'expEmsNm' : '' ,'expEmsIp' : '', 'vendor' : '', 'eqpTyp' : ''}
		conn	= None
		try :
			conn 	= M6.Connection(self.IRIS, self.IRIS_ID, self.IRIS_PASS, Database = 'tacs' )
			cursor 	= conn.cursor()

			sql = """
					SELECT 
						EQP_NM, REP_IP_ADDR, SPLY_BP_ID, EQP_TYP
					FROM 
						TACS.TNG_IM_EQP_BAS
					WHERE 
						(REP_IP_ADDR = '%s'
						OR REP_IP_ADDR_1 = '%s'
						OR REP_IP_ADDR_2 = '%s'
						OR REP_IP_ADDR_3 = '%s'
						OR REP_IP_ADDR_4 = '%s'
						OR REP_IP_ADDR_5 = '%s')
					LIMIT 1
					""" % (emsIp, emsIp, emsIp, emsIp, emsIp, emsIp)

			resultMsg = cursor.Execute2(sql)

			__LOG__.Trace(resultMsg)

			for oneRaw in cursor :
				resultDict['expEmsNm']	= oneRaw[0].encode('utf-8')
				resultDict['expEmsIp']	= oneRaw[1].encode('utf-8')
				resultDict['vendor'] 	= oneRaw[2].encode('utf-8')
				resultDict['eqpTyp'] 	= oneRaw[3].encode('utf-8')
		except :
			__LOG__.Exception()

		finally :
			if conn : conn.close()

		return resultDict['expEmsNm'], resultDict['expEmsIp'], resultDict['vendor'], resultDict['eqpTyp']

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
					SELECT 
						EQP_EMS_NM 
					FROM 
						TACS.TNG_IM_EQP_BAS
					WHERE 
						REP_IP_ADDR IN (%s)
						OR REP_IP_ADDR_1 IN (%s)
						OR REP_IP_ADDR_2 IN (%s)
						OR REP_IP_ADDR_3 IN (%s)
						OR REP_IP_ADDR_4 IN (%s)
						OR REP_IP_ADDR_5 IN (%s)
					LIMIT 1
						""" % (ipList, ipList, ipList, ipList, ipList, ipList)

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
		emsNm = emsNm.strip()
		__LOG__.Trace('emsNm = %s' % emsNm)
		resultDict = {'expEmsNm' : '' ,'expEmsIp' : '', 'vendor' : '', 'eqpTyp' : ''}
		conn 		= None
	
		try :
			conn = M6.Connection(self.IRIS, self.IRIS_ID, self.IRIS_PASS, Database='tacs')
			cursor = conn.cursor()	

			#ipList = '\'%s\'' % '\',\''.join(svrIpList)

			sql = """
				SELECT 
					EQP_NM, REP_IP_ADDR, SPLY_BP_ID, EQP_TYP
				FROM 
					TACS.TNG_IM_EQP_BAS 
				WHERE 
					EQP_EMS_NM = '%s' """ % emsNm

			resultMsg	= cursor.Execute2(sql)

			for oneRaw in cursor :
				resultDict['expEmsNm']	= oneRaw[0].encode('utf-8')
				resultDict['expEmsIp'] 	= oneRaw[1].encode('utf-8')
				resultDict['vendor']	= oneRaw[2].encode('utf-8')
				resultDict['eqpTyp'] 	= oneRaw[3].encode('utf-8')

		except :
			__LOG__.Exception()

		finally :
			if conn : conn.close()

		return resultDict['expEmsNm'], resultDict['expEmsIp'], resultDict['vendor'], resultDict['eqpTyp']

	def deleteWorkEqpInfo ( self, workId, idx, key, partition ) :
		__LOG__.Trace('key : %s | partition : %s | workId : %s | idx : %s' % ( key, partition ,workId, idx ) )

		conn = None
		try :
			conn = M6.Connection(self.IRIS, self.IRIS_ID, self.IRIS_PASS,Database='tacs')
			cursor = conn.cursor()

			workSql = """/*+ LOCATION( key = '%s' AND PARTITION = '%s' ) */
					DELETE 
					FROM TACS.TACS_WORK_INFO
					WHERE IDX = '%s' and WORK_ID = '%s' 
					""" %( key, partition, idx, workId )

			resultWorkMsg	= cursor.Execute2(workSql)

			eqpSql = """/*+ LOCATION( key = '%s' AND PARTITION = '%s' ) */
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
		eqpNm = eqpNm.strip()
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

			__LOG__.Trace(result)

			cursor.Close()
			conn.close()

		except :

			__LOG__.Exception()

		finally : 
			if conn : conn.close()

		return result

	def selectLkngUnit (self, emsIp) :
		emsIp = emsIp.strip()
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

	def selectWorkInfo(self, hint, workId, workStaDate) :
		__LOG__.Trace('hint({}), workId({}), workStaDate({})'.format(hint, workId, workStaDate))
		conn 		= None
		resultDict 	= {}
		try :
			conn 	= M6.Connection(self.IRIS, self.IRIS_ID, self.IRIS_PASS, Database='tacs')
			cursor 	= conn.cursor()
			sql 	=  '''
				{}
				SELECT 
					MAX(IDX + 0) 
				FROM TACS.TACS_WORK_INFO 
				WHERE 
					WORK_ID = \'{}\'
				AND
					WORK_STA_DATE = \'{}\'
			'''.format(hint, workId, workStaDate)
			__LOG__.Trace('query: {}'.format(sql))
			cursor.Execute2(sql)	
			
			idx = None
			for oneRow in cursor :
				idx = oneRow[0]

			if not idx :
				raise Exception('Unavailable IDX({})'.format(idx))

			sql = '''
				{}
				SELECT
					WORK_ID
					, DMN_DIV_CD
					, TO_CHAR(WORK_EVNT_DATE, \'YYYYMMDD\')
					, WORK_TYP_CD
					, WORK_NM
					, TO_CHAR(WORK_STA_DATE, \'YYYY-MM-DD HH24:MI\')
					, TO_CHAR(WORK_END_DATE, \'YYYY-MM-DD HH24:MI\')
					, CMD_TYP_CD
					, CMD_WORK_TYP_CD
					, RSK_CMD_INCL_YN
					, API_CALN_SVR_DIV_CD
					, CMD_CTRL_TYP_CD
					, WORK_PROG_STAT_CD
					, TO_CHAR(LAST_CHG_DATE, \'YYYY-MM-DD HH24:MI\')
					, VENDOR
				FROM 
					TACS_WORK_INFO
				WHERE
					IDX = \'{}\'
				AND
					WORK_ID = \'{}\'
				AND
					WORK_STA_DATE = \'{}\'
				LIMIT 1
			'''.format(hint, idx, workId, workStaDate)
			__LOG__.Trace('query: {}'.format(sql))
			cursor.Execute2(sql)
			
			for oneRow in cursor :
				resultDict['workId'] 			= oneRow[0]
				resultDict['dmnDivCd'] 			= oneRow[1]
				resultDict['workEvntDate'] 		= oneRow[2]
				resultDict['workTypCd']			= oneRow[3]
				resultDict['workNm']			= oneRow[4]
				resultDict['workStaDate']		= oneRow[5]
				resultDict['workEndDate']		= oneRow[6]
				#resultDict['cmdTypCd']			= oneRow[7]
				resultDict['cmdWorkTypCd']		= oneRow[8]
				resultDict['rskCmdInclYn']		= oneRow[9]
				resultDict['apiCalnSvrDivCd'] 	= oneRow[10]
				resultDict['cmdCtrlTypCd']		= oneRow[11]
				resultDict['workProgStatCd']	= oneRow[12]
				resultDict['lastChgDate']		= oneRow[13]
				#resultDict['vendor']			= oneRow[14]
		except :
			__LOG__.Exception()
		finally :
			if conn : conn.close()

		return resultDict

	def selectEqpInfo(self, hint, workId, workStaDate, emsIp) :
		__LOG__.Trace('hint({}), workId({}), workStaDate({}), emsIp({})'.format(hint, workId, workStaDate, emsIp))
		conn 		= None
		resultList 	= []
		try :
			conn 	= M6.Connection(self.IRIS, self.IRIS_ID, self.IRIS_PASS, Database='tacs')
			cursor 	= conn.cursor()
			sql 	=  '''
				{}
				SELECT 
					MAX(IDX + 0) 
				FROM TACS_EQP_INFO 
				WHERE 
					WORK_ID = \'{}\'
				AND
					WORK_STA_DATE = \'{}\'
			'''.format(hint, workId, workStaDate)
			__LOG__.Trace('query: {}'.format(sql))
			cursor.Execute2(sql)	
			
			idx = None
			for oneRow in cursor :
				idx = oneRow[0]

			if not idx :
				raise Exception('Unavailable IDX({})'.format(idx))

			sql = '''
				{}
				SELECT
					UNQ_IDNT_NO
					, CMD_WORK_TYP_CD
					, TANGO_EQP_ID
					, ENB_ID
					, EMS_NM
					, EMS_IP
					, EQP_ID
					, EQP_NM
					, SVR_IP
					, SVR_CNNT_ACNTG_ID
					, ROOT_ACNTG_USE_YN
					, APRVR_ID
					, WORK_REGRT_ID
					, OPRR_ID
					, SECURE_GW_OPRR_ID
					, ADD_USER_ACNTG_ID
					, CMD_TYP_CD
					, WORK_FILD_CD
					, CMD_INFO
					, SCRIPT_INFO
					, VENDOR
				FROM 
					TACS_EQP_INFO
				WHERE
					IDX = \'{}\'
				AND
					WORK_ID = \'{}\'
				AND
					WORK_STA_DATE = \'{}\'
				AND
					EMS_IP = \'{}\'
			'''.format(hint, idx, workId, workStaDate, emsIp)
			__LOG__.Trace('query: {}'.format(sql))
			cursor.Execute2(sql)
			
			for oneRow in cursor :
				resultDict = {}
				resultDict['unqIdntNo']			= oneRow[0]
				resultDict['cmdWorkTypCd']		= oneRow[1]
				resultDict['tangoEqpId'] 		= oneRow[2]
				resultDict['enbId']				= oneRow[3]
				resultDict['emsNm']				= oneRow[4]
				resultDict['emsIp']				= oneRow[5]
				resultDict['eqpId']				= oneRow[6]
				resultDict['eqpNm']				= oneRow[7]
				resultDict['svrIp']				= oneRow[8]
				resultDict['svrCnntAcntgId']	= oneRow[9]
				resultDict['rootAcntgUseYn'] 	= oneRow[10]
				resultDict['aprvrId']			= oneRow[11]
				resultDict['workRegrtId']		= oneRow[12]
				resultDict['oprrId']			= oneRow[13]
				resultDict['secureGwOprrId']	= oneRow[14]
				resultDict['addUserAcntgId']	= oneRow[15]
				resultDict['cmdTypCd']			= oneRow[16]
				resultDict['workFildCd']		= oneRow[17]
				
				if oneRow[18] :
					resultDict['cmdInfo'] = json.loads(oneRow[18])
				
				if oneRow[19] :
					resultDict['scriptInfo'] = json.loads(oneRow[19])
				
				#resultDict['vendor'] = oneRow[20]
				resultList.append(resultDict)
		except :
			__LOG__.Exception()
		finally :
			if conn : conn.close()

		return resultList

