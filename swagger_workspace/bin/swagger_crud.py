#!~/usr/local/Python-3.6.8/bin/python3
# -*- coding:utf8 -*-

import os
import sys
import werkzeug
import json
import logging
from logging.handlers import TimedRotatingFileHandler

werkzeug.cached_property = werkzeug.utils.cached_property

from flask import make_response
from flask import Flask, request
from flask_restplus import Api, Resource, fields, Namespace, cors
from flask_restplus._http import HTTPStatus
from flask_cors import CORS
from flask_restplus import reqparse
from flask import current_app
from flask import render_template

#sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
import Mobigen.Database.iris_py3 as irisConn
import psycopg2
from psycopg2.extensions import new_type, DECIMAL
import datetime

app = Flask(__name__)
api = Api(app, version='1.0', title='IRIS API',
		  description='IRIS API',
		  )

CORS(app, resources={r'/*': {'origins': '*'}})

get_req_parser = reqparse.RequestParser(bundle_errors=True)
get_req_parser.add_argument('param', type=str, required=True)

json_parser = api.parser()
json_parser.add_argument('json', type=str, required=True, location='json',
						 help='JSON BODY argument')
arg_parser = api.parser()
arg_parser.add_argument('json', type=str, required=True,
						help='URL JSON argument')

rows_list = []

fields = []
updateKey 					= ['UPDATEKEY', 'UPDATEKEY2', 'UPDATEKEY3', 'UPDATEKEY4']
tableUpdateKeyDict			= {
								'CM_NB_SERVICE_GROUP' : {'UPDATEKEY' : 'NW_CODE'},
								'CM_NB_APP_EQUIP_INFO' : {'UPDATEKEY' : 'IPV4_LIST'}, 
								'CM_NB_COMMON_CODE' : {'UPDATEKEY' : 'CM_CODE'},
								'CM_NB_APP_INFO' : {'UPDATEKEY' : 'APP_SERVICE', 'UPDATEKEY2' : 'APP_SERVICE_CODE'},
								'CM_NB_APP_FUNC_INFO' : {'UPDATEKEY' : 'APP_FUNC_CODE'},
								'CM_NB_GROUP' : {'UPDATEKEY' : 'GROUP_ID'},
								'CM_NB_GROUP_TARGET' : {'UPDATEKEY' : 'GROUP_ID'},
								'CM_NB_APPLICATION_INFO' : {'UPDATEKEY' : 'APP_ID'},
								'CM_NB_SERVICE_INFO' : {'UPDATEKEY' : 'SERVICE_CODE'},
								'CM_NB_SERVER_GROUP' : {'UPDATEKEY' : 'NW_CODE'},
								'CM_NB_SERVICE_CHARGE' : {'UPDATEKEY' : 'SERVICE_CHARGE_ID'},
								'CM_NB_USER_ALARM_THRESHOLD' : {'UPDATEKEY' : 'ALARM_CLASS'},
								'CM_NB_DEVICE' : {'UPDATEKEY' : 'DEVICE_ID'},
								'FM_ALARM' : {'UPDATEKEY' : 'ALARM_TYPE_CODE', 'UPDATEKEY2' : 'ALARM_CLASS', 'UPDATEKEY3' : 'STAT_TYPE'},
								'FM_ALARM_THRESHOLD' : {'UPDATEKEY' : 'ALARM_TYPE_CODE', 'UPDATEKEY2' : 'ALARM_CLASS'},
								'PM_STAT_STATUS' : {'UPDATEKEY' : 'STAT_TYPE'},
								'FM_IOT_ALARM_EFFECT' : {'UPDATEKEY' : 'ALARM_CODE'},
								'FM_IOT_ALARM_THRESHOLD' : {'UPDATEKEY' : 'STAT_TYPE', 'UPDATEKEY2' : 'APP_SERVICE_CODE', 'UPDATEKEY3' : 'ALARM_CODE'},
								'TB_AD_HOLIDAY' : {'UPDATEKEY' : 'H_DATE' },
								'TB_ANOMALY_SCORE' : {'UPDATEKEY' : 'AD_CODE_TYPE'},
								'TB_ANOMALY_THRESHOLD' : {'UPDATEKEY' : '1'},
								'CM_NB_SMSALARMRULE' : {'UPDATEKEY' : 'RULE_ID'},
								'TB_ANOMALY_OCCUR_LOG' : {'UPDATEKEY' : 'COLLECT_DATE', 'UPDATEKEY2' : 'APP_SERVICE_CODE', 'UPDATEKEY3' : 'AD_ALARM_CODE', 'UPDATEKEY4' : 'OBS_NAME'}
							}

def connectIirs() :
	irisClient	= None
	try :
		irisClient = irisConn.Client('IRIS_DEV')
#		conn 	= M6.Connection('%s:%s' % (app.config['IRIS_IP'], app.config['IRIS_PORT']), app.config['IRIS_USER'], app.config['IRIS_PASSWD'], Direct=False, Database=app.config['IRIS_DATABASE'] )
		#conn 	= M6.Connection('%s:%s' % ('192.168.102.124:5000'), app.config['IRIS_USER'], app.config['IRIS_PASSWD'], Direct=True, Database=app.config['IRIS_DATABASE'] )
		#conn 	= M6.Connection(app.config['IRIS_IP'], app.config['IRIS_USER'], app.config['IRIS_PASSWD'], Direct=False, Database = app.config['IRIS_DATABASE'] )

		app.logger.info('IRIS Connection IP!!')
		return irisClient
	except Exception as e :
		print(e)
		app.logger.error(e)
		return irisClient

def connectPostgre() :
	conn	= psycopg2.connect(host = app.config['PGSQL_IP'], dbname = app.config['PGSQL_DATABASE'], user = app.config['PGSQL_USER'], password = app.config['PGSQL_PASSWD'])
	cursor	= conn.cursor()	
	app.logger.info('Postgresql Connection IP : {} / DB : {}/ USER : {}'.format(app.config['PGSQL_IP'], app.config['PGSQL_DATABASE'], app.config['PGSQL_USER']))
	return conn, cursor

def closeIris(conn, cursor) :
	if cursor != None :
		cursor.Close()

	if conn != None :
		conn.close()

	app.logger.info('IRIS Close')

def	closePostgre(conn, cursor) :
	if cursor != None :
		cursor.close()

	if conn != None :
		conn.close()

	app.logger.info('Postgresql Close')

class global_variable:
	values = {}

	def __init__(self, *args, **kwargs):
		super(global_variable, self).__init__(*args, **kwargs)

	@classmethod
	def get(cls, _val_name):
		return cls.values[_val_name]

	@classmethod
	def set(cls, _val_name, _val):
		cls.values[_val_name] = _val

@api.route('/delete/<string:table_name>')
class delete(Resource) :
	@api.response(200, "Success")
	def options(self, table_name) :
		return {}

	@api.expect(get_req_parser)
	@api.response(200, "Success")
#	def get(self, table_name) :
	def post(self, table_name) :
		deleteParam = parse_req_data(request)
#		deleteParam = json.loads(request.args["param"])

#		conn, cursor = connectIirs()
		conn, cursor = connectPostgre()

		columns	= list()
		values	= list()

		keyList		= list()

		deleteKeyStr = None

		for key, value in deleteParam.items() :
			if None == value or '' == value :				
				return{"success" : {"code" : 0, "messages": "삭제하실 데이터를 선택해주세요."}}

			keyList.append("%s = '%s'"%(key.upper(), value) )

#			columns.append(str(key))
#			values.append(str(value))


		if len(keyList) >= 2 :
			deleteKeyStr = ' and '.join(keyList)
		else :
			deleteKeyStr = keyList[0]

#		colStr	= ','.join(columns)
#		valStr	= "','".join(values)

		sql = ''' delete from %s where %s;
			''' % (table_name , deleteKeyStr)

		app.logger.info('DELETE SQL : {}'.format(sql))

		try :
#			result = cursor.Execute2(sql)
			result = cursor.execute(sql)
			conn.commit()
#			if result.startswith('+OK') :
#				return{"success" : {"code" : 0}}
#			if table_name == 'CM_NB_APP_EQUIP_INFO' :
#				return{"success" : {"code" : 0}}
			return{"success" : {"code" : 0, "messages": "Delete Success\n{}".format(deleteKeyStr)}}

		except Exception as e :
			return {"Exception" : str(e)}
		finally :
#			closeIris(conn, cursor)
			closePostgre(conn, cursor)

		return {}

@api.route('/delete_iris/<string:table_name>')
class deleteIris(Resource) :
	@api.response(200, "Success")
	def options(self, table_name) :
		return {}

	@api.expect(get_req_parser)
	@api.response(200, "Success")
#	def get(self, table_name) :
	def post(self, table_name) :
		deleteParam = parse_req_data(request)
#		deleteParam = json.loads(request.args["param"])

		irisClient = connectIirs()

		columns	= list()
		values	= list()

		keyList		= list()

		deleteKeyStr = None

		for key, value in deleteParam.items() :
			keyList.append("%s = '%s'"%(key.upper(), value) )

		if len(keyList) >= 2 :
			deleteKeyStr = ' and '.join(keyList)
		else :
			deleteKeyStr = keyList[0]

		sql = ''' delete from %s where %s;
			''' % (table_name , deleteKeyStr)

		app.logger.info(" IRIS DELETE SQL : {}".format(sql) )

		try :
			result = irisClient.execute(sql)
			if result.startswith('+OK') :
				return{"success" : {"code" : 0, "messages": "Delete Success\n{}".format(deleteKeyStr)}}

		except Exception as e :
			return {"Exception" : str(e)}
		#finally :
			#closeIris(conn, cursor)

		return {}
@api.route('/update/<string:table_name>')
class update(Resource) :
	@api.response(200, "Success")
	def options(self, table_name) :
		return {}

	@api.expect(get_req_parser)
	@api.response(200, "Success")
#	def get(self, table_name) :
	def post(self, table_name) :
		updateParam = parse_req_data(request)
#		updateParam = json.loads(request.args["param"])

#		conn, cursor = connectIirs()
		conn, cursor = connectPostgre()

		columns	= list()
		values	= list()

		updateList 	= list()
		keyList		= list()

		updateSetStr = None
		updateKeyStr = None
		updateList	 = list()

		for key, value in updateParam.items() :
			if key.upper().strip() in updateKey :
				keyList.append("%s = '%s'" %(tableUpdateKeyDict[table_name.upper()][key.upper()], value) )
			elif key.upper().strip() == 'UPDATE_TIME' or key.upper().strip() == 'UPDATE_DATE' :
				now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
				updateList.append("%s = '%s'" %(key.upper(), now))
			else :
				updateList.append("%s = '%s'" %(key.upper(), value) )

#			columns.append(str(key))
#			values.append(str(value))


		updateSetStr = ','.join(updateList)

		if len(keyList) >= 2 :
			updateKeyStr = ' and '.join(keyList)
		else :
			updateKeyStr = keyList[0]

#		colStr	= ','.join(columns)
#		valStr	= "','".join(values)

		

		sql = ''' update %s set %s where %s;
			''' % (table_name , updateSetStr, updateKeyStr)

		app.logger.info('update sql : {}'.format(sql))
#		print (sql)

		try :
#			result = cursor.Execute2(sql)
			result = cursor.execute(sql)
			conn.commit()
#			if result.startswith('+OK') :
#				return{"success" : {"code" : 0}}
#			if table_name == 'CM_NB_APP_EQUIP_INFO' :
#				return{"success" : {"code" : 0}}
			return{"success" : {"code" : 0, "messages" : "Update Success"}}

		except Exception as e :
			app.logger.error(e)
			return {"error" : {"code" : -1, "messages" : "데이터를 확인하세요."}}
		finally :
#			closeIris(conn, cursor)
			closePostgre(conn, cursor)

		return {}


@api.route('/insert/<string:table_name>')
class insert(Resource) :
	@api.response(200, "Success")
	def options(self, table_name) :
		return {}

	@api.expect(get_req_parser)
	@api.response(200, "Success")
#	def get(self, table_name) :
	def post(self, table_name) :
		insertParam = parse_req_data(request)
#		insertParam = json.loads(request.args["param"])

#		conn, cursor = connectIirs('192.168.100.180:5050', 'kepco_iot', 'kepco12#$', 'KEPCO_IOT')
		conn, cursor = connectPostgre()

		columns	= list()
		values	= list()

		now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

		for key, value in insertParam.items() :
			if key == 'UPDATE_TIME' or key == 'INSERT_TIME' or key == 'CREATE_TIME' or key == 'UPDATE_DATE' or key == 'INSERT_DATE' :
				columns.append(str(key))
				values.append(str(now))
			elif key.upper() == 'RULE_ID' :
				try :
					ruleSql = """
								SELECT 
									MAX(RULE_ID)
								FROM
									{}
							""".format(table_name)
					app.logger.info(ruleSql)
					cursor.execute(ruleSql)
					rows = cursor.fetchall()
					for row in rows :
						columns.append(str(key))
						values.append(str(int(row[0]) + 1 ))

				except Exception as e :
					app.logger.error(e)
			else :
				columns.append(str(key))
				values.append(str(value))
		
		colStr	= ','.join(columns)
		valStr	= "','".join(values)

		
		sql = ''' insert into %s (%s) values('%s');
			''' % (table_name, colStr, valStr)
		app.logger.info("insert sql : {}".format(sql) )
		try :
#			result = cursor.Execute2(sql)
			
			result = cursor.execute(sql)
#			if result.startswith('+OK') :
#				return{"success" : {"code" : 0}, "messages" : "장비등록 완료"}
#			if table_name == 'CM_NB_APP_EQUIP_INFO' :
#				return{"success" : {"code" : 0}}
			conn.commit()
			return{"success" : {"code" : 0, "messages" : "추가 성공\n{}".format('\n'.join(values))}}

		except Exception as e :
			app.logger.error(e)
			return {"error" : {"code" : -1, "messages" : "데이터를 확인하세요."}}
		finally :
			closePostgre(conn, cursor)

		return {}

@api.route('/insert_iris/<string:table_name>')
class insertIris(Resource) :
	@api.response(200, "Success")
	def options(self, table_name) :
		return {}

	@api.expect(get_req_parser)
	@api.response(200, "Success")
#	def get(self, table_name) :
	def post(self, table_name) :
		insertParam = parse_req_data(request)
#		insertParam = json.loads(request.args["param"])
		app.logger.info(insertParam)
		irisClient = connectIirs()
#		conn, cursor = connectPostgre()

		columns	= list()
		values	= list()

		now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
		ruleId	= 0
		for key, value in insertParam.items() :
			if key == 'UPDATE_TIME' or key == 'INSERT_TIME' or key == 'CREATE_TIME' or key == 'UPDATE_DATE' or key == 'INSERT_DATE' :
				columns.append(str(key))
				values.append(str(now))
			elif key.upper() == 'RULE_ID' :
				try :
					ruleSql = """
								SELECT 
									MAX(RULE_ID + 0)
								FROM
									{}
							""".format(table_name)
					irisClient.execute_iter(ruleSql)
					for row in cursor :
						columns.append(str(key))
						values.append(str(int(row[0]) + 1 ))

				except Exception as e :
					app.logger.error(e)
					
			else :
				columns.append(str(key))
				values.append(str(value))
		
		colStr	= ','.join(columns)
		valStr	= "','".join(values)

		
		sql = ''' insert into %s (%s) values('%s');
			''' % (table_name, colStr, valStr)
		app.logger.info( 'iris insert sql : {}'.format(sql) )
		try :
			result = irisClient.execute(sql)
			
			if result.startswith('+OK') :
				return{"success" : {"code" : 0, "messages" : "추가 성공\n{}".format('\n'.join(values))}}

		except Exception as e :
			(e)
			return {"error" : {"code" : -1, "messages" : "데이터를 확인하세요."}}
		#finally :
			#closeIris(conn, cursor)

		return {}

@api.route('/update_iris/<string:table_name>')
class updateIris(Resource) :
	@api.response(200, "Success")
	def options(self, table_name) :
		return {}

	@api.expect(get_req_parser)
	@api.response(200, "Success")
#	def get(self, table_name) :
	def post(self, table_name) :
		updateParam = parse_req_data(request)
#		updateParam = json.loads(request.args["param"])

		irisClient = connectIirs()

		columns	= list()
		values	= list()

		updateList 	= list()
		keyList		= list()

		updateSetStr = None
		updateKeyStr = None
		updateList	 = list()

		for key, value in updateParam.items() :
			if key.upper().strip() in updateKey :
				keyList.append("%s = '%s'" %(tableUpdateKeyDict[table_name.upper()][key.upper()], value) )
			elif key.upper().strip() == 'UPDATE_TIME' or key.upper().strip() == 'UPDATE_DATE' :
				now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
				updateList.append("%s = '%s'" %(key.upper(), now))
			else :
				updateList.append("%s = '%s'" %(key.upper(), value) )

		updateSetStr = ','.join(updateList)

		if len(keyList) >= 2 :
			updateKeyStr = ' and '.join(keyList)
		else :
			updateKeyStr = keyList[0]

		sql = ''' update %s set %s where %s;
			''' % (table_name , updateSetStr, updateKeyStr)

		if 'TB_ANOMALY_THRESHOLD' == table_name.upper() :
			sql = ''' update %s set %s;
				''' % (table_name , updateSetStr)

		app.logger.info("update iris sql : {}".format(sql) )

		try :
			result = irisClient.execute(sql)
			if result.startswith('+OK') :
				return{"success" : {"code" : 0, "messages" : "Update Success"}}

		except Exception as e :
			app.logger.error(e)
			return {"error" : {"code" : -1, "messages" : "데이터를 확인하세요."}}
		#finally :
			#closeIris(conn, cursor)
#			closePostgre(conn, cursor)

		return {}


@api.route('/select_service_err/')
class select(Resource):
	@api.response(200, "Success")
	def options(self):
		return {}

	@api.response(200, "Success")
	def get(self):
		DEC2FLOAT	 = new_type(DECIMAL.values, 'DEC2FLOAT', lambda value, curs : float(value) if value is not None else None)
		psycopg2.extensions.register_type(DEC2FLOAT)
		conn, cursor 		= connectPostgre()
		irisClient		 	= connectIirs()

		alarmValue	= {'C' : 1, 'M' : 2, 'I' : 3, 'W' : 4 , 'N' : 5}

		errCnt		= 0
		commonCnt	= 0
		totalCnt	= 0

		delta = datetime.timedelta(minutes = 10)
		bHoursTime	= datetime.datetime.now() - delta
		pgsqlTime	= bHoursTime.strftime('%Y-%m-%d %H:%M:%S')
		strTime		= bHoursTime.strftime('%Y%m%d%H%M%S')

		sqlStr = """
					SELECT 							
						grp.app_func_code,
						CASE WHEN STATUS_CHECK.event_time <= '{}' THEN 'N'
						ELSE coalesce(null, STATUS_CHECK.alarm_level, 'N')
						END,
						CASE WHEN STATUS_CHECK."COLLECT_DATE" <= '{}' THEN 10
						ELSE coalesce(null, STATUS_CHECK."AD_LEVEL", 10)
						END
					FROM
						cm_nb_app_func_info grp
					LEFT OUTER JOIN
						( SELECT
							A.app_service_code, A.app_service, B.app_func_code AS func, A.ipv4_list, C.alarm_level, D."AD_LEVEL", C.event_time, D."COLLECT_DATE"
						FROM 
							cm_nb_app_info A
						LEFT OUTER JOIN 
							cm_nb_app_rel_func B 
						ON 
							A.app_service_code = B.app_service_code
						LEFT OUTER JOIN 
							fm_alarm C
						ON 
							A.app_service_code = C.app_service_code AND c.event_time >= '{}'
						LEFT OUTER JOIN
							tb_anomaly_occur_stat D
						ON
							A.app_service_code = D."APP_SERVICE_CODE" AND D."COLLECT_DATE" >= '{}'
						WHERE
							C.alarm_level IS NOT NULL or D."AD_LEVEL" IS NOT NULL) STATUS_CHECK
					ON 
						grp.app_func_code = STATUS_CHECK.func
					ORDER BY grp.app_func_code
				""".format(pgsqlTime, strTime, pgsqlTime, strTime)

		app.logger.info(sqlStr)

		deleteChart = """
					DELETE
					FROM
						KEPCO_IOT.DASHBOARD_CHART
					WHERE
						CHART_KEY1 = 'dashboard' AND (CHART_KEY2 = 'service_chart' or CHART_KEY2 = 'service_cnt');
					"""

		insertServiceCnt = """
					INSERT INTO
						KEPCO_IOT.DASHBOARD_CHART (CHART_KEY1, CHART_KEY2, VALUE1)
					VALUES
						('dashboard', 'service_cnt', '{}');
					"""

		insertService = """
					INSERT INTO
						KEPCO_IOT.DASHBOARD_CHART (CHART_KEY1, CHART_KEY2, VALUE1)
					VALUES
						('dashboard', 'service_chart', '{}');
					"""

		try :
			irisClient.execute(deleteChart)
			cursor.execute(sqlStr)
			rows = cursor.fetchall()
			resultDict = dict()
			for row in rows :
				selectList 	= list(row)
				imgName		= 'SERVICE_{}_PNG'.format(selectList[0])
				imgUrl		= None

				try :
					imgUrl 	= app.config[imgName]
				except Exception as ex:
					app.logger.info('Error : {} is invalid \n {}'.format(imgName, ex))
					continue

				if not selectList[0] in resultDict :
					resultDict[selectList[0]] = ['N', 10]
					resultDict[selectList[0]].append('{}:{}/{}/{}'.format(app.config['SERVER_IP'], app.config['SERVER_PORT'], app.config['IMG_COMMON_URI'], app.config[imgName]))

				if 3 == len(selectList) :
					if selectList[1] == 'C' :
						selectList.append('{}:{}/{}/{}'.format(app.config['SERVER_IP'], app.config['SERVER_PORT'], app.config['IMG_CRITICAL_URI'], app.config[imgName]))
						resultDict[selectList[0]] = [selectList[1], selectList[2], selectList[3]]

					elif selectList[1] == 'M' :
						selectList.append('{}:{}/{}/{}'.format(app.config['SERVER_IP'], app.config['SERVER_PORT'], app.config['IMG_MAJOR_URI'], app.config[imgName]))
						if alarmValue[resultDict[selectList[0]][0]] > alarmValue[selectList[1]] :
							resultDict[selectList[0]] = [selectList[1], selectList[2], selectList[3]]

					elif selectList[1] == 'I' :
						selectList.append('{}:{}/{}/{}'.format(app.config['SERVER_IP'], app.config['SERVER_PORT'], app.config['IMG_MINOR_URI'], app.config[imgName]))
						if alarmValue[resultDict[selectList[0]][0]] > alarmValue[selectList[1]] :
							resultDict[selectList[0]] = [selectList[1], selectList[2], selectList[3]]

					elif selectList[1] == 'N' and int(selectList[2]) < 7 :
						selectList.append('{}:{}/{}/{}'.format(app.config['SERVER_IP'], app.config['SERVER_PORT'], app.config['IMG_AD_URI'], app.config[imgName]))
						if alarmValue[resultDict[selectList[0]][0]] > 4 :
							resultDict[selectList[0]] = [selectList[1], selectList[2], selectList[3]]

					else :
						selectList.append('{}:{}/{}/{}'.format(app.config['SERVER_IP'], app.config['SERVER_PORT'], app.config['IMG_COMMON_URI'], app.config[imgName]))
						resultDict[selectList[0]] = [selectList[1], selectList[2], selectList[3]]
						

				else :
					app.logger.info('{} len is invalid'.format(selectList))

#				if not row[0] in resultDict :
#					resultDict[row[0]] = [row[1], row[2]]
#				elif row[1] != 'N' or row[2] != 10 :	
#					resultDict[row[0]] = [row[1], row[2]]
#				else :
#					continue
			for key, valueList in resultDict.items() :
				app.logger.info(valueList)
				if (valueList[0] != 'N' and valueList[0] != 'W') :
					irisClient.execute(insertService.format('에러'))
					app.logger.info(insertService.format('에러'))
				else :
					irisClient.execute(insertService.format('정상'))
					app.logger.info(insertService.format('정상'))

			irisClient.execute( insertServiceCnt.format(str(len(resultDict))) )
			app.logger.info( insertServiceCnt.format(str(len(resultDict))) )
					
			return {"data" : resultDict ,"hide_messages": True}

		except Exception as ex:
			# __LOG__.Trace("Except: %s" % ex)
			app.logger.error(ex)
			return {"data" : "", "hide_messages": True}
		finally:
			closePostgre(conn, cursor)
			#closeIris(irisCon, irisCur)
		return {}

@api.route('/select_server_err/')
class select(Resource):
	@api.response(200, "Success")
	def options(self):
		return {}

	@api.response(200, "Success")
	def get(self):
		DEC2FLOAT	 = new_type(DECIMAL.values, 'DEC2FLOAT', lambda value, curs : float(value) if value is not None else None)
		psycopg2.extensions.register_type(DEC2FLOAT)

		conn, cursor 		= connectPostgre()
		irisClient 			= connectIirs()
		alarmValue  = {'C' : 1, 'M' : 2, 'I' : 3, 'W' : 4 , 'N' : 5}

#		mg_server_str	= '\'%s\'' % '\',\''.join(app.config['MG_SERVER_LIST'].split(','))
#		mg_server_list	= app.config['MG_SERVER_LIST'].split(',')
		db_server_list	= app.config['DB_SERVER_LIST'].split(',')

		delta = datetime.timedelta(minutes = 10)
		bHoursTime	= datetime.datetime.now() - delta
		pgsqlTime	= bHoursTime.strftime('%Y-%m-%d %H:%M:%S')
		strTime		= bHoursTime.strftime('%Y%m%d%H%M%S')

		sqlStr = """
				SELECT 
					mg_eqp.ipv4_list,
					CASE WHEN alarm_eqp.event_time <= '{}' THEN 'N'
					ELSE coalesce(null, alarm_eqp.alarm_level, 'N')
					END , 
					CASE WHEN alarm_eqp."COLLECT_DATE" <= '{}' THEN 10
					ELSE coalesce(null, alarm_eqp."AD_LEVEL", 10)
					END

				FROM
					cm_nb_app_equip_info mg_eqp
				LEFT OUTER JOIN
					(SELECT
						arm.threshold_level, eqp.ipv4_list, app.app_service_code, arm.alarm_level, tb."AD_LEVEL", arm.event_time, tb."COLLECT_DATE"
					FROM 
					    cm_nb_app_equip_info eqp
					LEFT OUTER JOIN
					    cm_nb_app_info app
					ON 
					    app.ipv4_list = eqp.ipv4_list
					LEFT OUTER JOIN 
					    fm_alarm arm
					ON 
					    app.app_service_code = arm.app_service_code and arm.event_time >= '{}'
					LEFT OUTER JOIN
						tb_anomaly_occur_stat tb
					ON
						app.app_service_code = tb."APP_SERVICE_CODE" and tb."COLLECT_DATE" >= '{}'
					WHERE
						(arm.alarm_level IS NOT NULL or tb."AD_LEVEL" IS NOT NULL)
					) alarm_eqp
				ON 
					alarm_eqp.ipv4_list = mg_eqp.ipv4_list
				""".format(pgsqlTime, strTime, pgsqlTime, strTime)

#				WHERE 
#					mg_eqp.ipv4_list IN ({})
		app.logger.info(sqlStr)

		deleteChart = """
					DELETE
					FROM
						KEPCO_IOT.DASHBOARD_CHART
					WHERE
						CHART_KEY1 = 'dashboard' AND (CHART_KEY2 = 'server_chart' or CHART_KEY2 = 'server_cnt');
					"""

		insertServerCnt = """
					INSERT INTO
						KEPCO_IOT.DASHBOARD_CHART (CHART_KEY1, CHART_KEY2, VALUE1)
					VALUES
						('dashboard', 'server_cnt', '{}');
					"""

		insertServer = """
					INSERT INTO
						KEPCO_IOT.DASHBOARD_CHART (CHART_KEY1, CHART_KEY2, VALUE1)
					VALUES
						('dashboard', 'server_chart', '{}');
					"""

		try:
			irisClient.execute(deleteChart)
			cursor.execute(sqlStr)
			rows = cursor.fetchall()
			resultDict = dict()

			for row in rows :
				selectList = list(row)

				
				if not selectList[0] in resultDict :
					resultDict[selectList[0]] = ['N', 10]
					if selectList[0] in db_server_list :
						resultDict[selectList[0]].append('{}:{}/{}/{}'.format(app.config['SERVER_IP'], app.config['SERVER_PORT'], app.config['IMG_CRITICAL_URI'], app.config['DB_PNG']))
					else :
						resultDict[selectList[0]].append('{}:{}/{}/{}'.format(app.config['SERVER_IP'], app.config['SERVER_PORT'], app.config['IMG_CRITICAL_URI'], app.config['SERVER_PNG']))


				if 3 == len(selectList) :
					if selectList[1] == 'C' :
						if selectList[0] in db_server_list :
							selectList.append('{}:{}/{}/{}'.format(app.config['SERVER_IP'], app.config['SERVER_PORT'], app.config['IMG_CRITICAL_URI'], app.config['DB_PNG']))
						else :
							selectList.append('{}:{}/{}/{}'.format(app.config['SERVER_IP'], app.config['SERVER_PORT'], app.config['IMG_CRITICAL_URI'], app.config['SERVER_PNG']))
						resultDict[selectList[0]] = [selectList[1], selectList[2], selectList[3]]

					elif selectList[1] == 'M' :
						if selectList[0] in db_server_list :
							selectList.append('{}:{}/{}/{}'.format(app.config['SERVER_IP'], app.config['SERVER_PORT'], app.config['IMG_MAJOR_URI'], app.config['DB_PNG']))
						else :
							selectList.append('{}:{}/{}/{}'.format(app.config['SERVER_IP'], app.config['SERVER_PORT'], app.config['IMG_MAJOR_URI'], app.config['SERVER_PNG']))

						if alarmValue[resultDict[selectList[0]][0]] > alarmValue[selectList[1]] :
							resultDict[selectList[0]] = [selectList[1], selectList[2], selectList[3]]


					elif selectList[1] == 'I' :
						if selectList[0] in db_server_list :
							selectList.append('{}:{}/{}/{}'.format(app.config['SERVER_IP'], app.config['SERVER_PORT'], app.config['IMG_MINOR_URI'], app.config['DB_PNG']))
						else :
							selectList.append('{}:{}/{}/{}'.format(app.config['SERVER_IP'], app.config['SERVER_PORT'], app.config['IMG_MINOR_URI'], app.config['SERVER_PNG']))

						if alarmValue[resultDict[selectList[0]][0]] > alarmValue[selectList[1]] :
							resultDict[selectList[0]] = [selectList[1], selectList[2], selectList[3]]

					elif selectList[2] <= 7 :
						if selectList[0] in db_server_list :
							selectList.append('{}:{}/{}/{}'.format(app.config['SERVER_IP'], app.config['SERVER_PORT'], app.config['IMG_AD_URI'], app.config['DB_PNG']))
						else :
							selectList.append('{}:{}/{}/{}'.format(app.config['SERVER_IP'], app.config['SERVER_PORT'], app.config['IMG_AD_URI'], app.config['SERVER_PNG']))
	
						if alarmValue[resultDict[selectList[0]][0]] > 4 :
							resultDict[selectList[0]] = [selectList[1], selectList[2], selectList[3]]


					else :
						if selectList[0] in db_server_list :
							selectList.append('{}:{}/{}/{}'.format(app.config['SERVER_IP'], app.config['SERVER_PORT'], app.config['IMG_COMMON_URI'], app.config['DB_PNG']))
						else :
							selectList.append('{}:{}/{}/{}'.format(app.config['SERVER_IP'], app.config['SERVER_PORT'], app.config['IMG_COMMON_URI'], app.config['SERVER_PNG']))
						resultDict[selectList[0]] = [selectList[1], selectList[2], selectList[3]]

				else :
					app.logger.info('{} len is invalid'.format(selectList))
					continue

			for key, valueList in resultDict.items() :
				if (valueList[0] != 'N' and valueList[0] != 'W') :
					irisClient.execute(insertServer.format('에러'))
					app.logger.info(insertServer.format('에러'))
				else :
					irisClient.execute(insertServer.format('정상'))
					app.logger.info(insertServer.format('정상'))

			irisClient.execute( insertServerCnt.format(str(len(resultDict))) )
			app.logger.info( insertServerCnt.format(str(len(resultDict))) )

			return {"data" : resultDict ,"hide_messages": True}

		except Exception as ex:
			# __LOG__.Trace("Except: %s" % ex)
			app.logger.error(ex)
			return {"data" : "", "hide_messages": True}
		finally:
			closePostgre(conn, cursor)
			#closeIris(irisCon, irisCur)
		return {}

@api.route('/select_ad/')
class select(Resource):
	@api.response(200, "Success")
	def options(self):
		return {}

	@api.response(200, "Success")
	def get(self):
		irisClient = connectIirs()

		resultDict = dict()

		sql = """
				SELECT 
					MI_VALUE, MJ_VALUE, CR_VALUE 
				FROM 
					TB_ANOMALY_THRESHOLD;
			"""	

		try :
			app.logger.info(sql)
			result = irisClient.execute_iter(sql)

			for row in cursor :
				resultDict['MINOR'] = str(row[0])
				resultDict['MAJOR'] = str(row[1])
				resultDict['CRITICAL'] = str(row[2])

			if result.startswith('+OK') :
				return {"data" : resultDict , "hide_messages" : True}

		except Exception as e :
			app.logger.error(e)
			return {"error" : {"code" : -1, "messages" : "데이터를 확인하세요."}}
		#finally :
			#closeIris(conn, cursor)

@api.route('/select_ad_score/')
class select(Resource):
	@api.response(200, "Success")
	def options(self):
		return {}

	@api.response(200, "Success")
	def get(self):
		irisClient = connectIirs()

		resultDict = dict()

		sql = """
				SELECT 
					AD_CODE_TYPE, VALUE
				FROM 
					TB_ANOMALY_SCORE
				ORDER BY AD_CODE_TYPE;
			"""	

		try :
			app.logger.info(sql)
			result = irisClient.execute_iter(sql)

			for row in cursor :
				resultDict[str(row[0])] = str(row[1])

			if result.startswith('+OK') :
				return {"data" : resultDict , "hide_messages" : True}

		except Exception as e :
			app.logger.error(e)
			return {"error" : {"code" : -1, "messages" : "데이터를 확인하세요."}}
		#finally :
			#closeIris(conn, cursor)

@api.route('/datedays/<string:dateTime>/<string:deltaDays>')
class select(Resource) :
	@api.response(200, "Success")
	def options(self, dateTime, deltaDays) :
		return{}

	@api.response(200, "Success")
	def get(self, dateTime, deltaDays) :
		resultList = list()
		try :
			delta = datetime.timedelta(days = int(deltaDays))
			app.logger.info(dateTime)
			inputDate = datetime.datetime.strptime(str(dateTime), '%Y%m%d')
			app.logger.info(inputDate)

			resultDate	= inputDate - delta
			resultStr	= datetime.datetime.strftime(resultDate, '%Y%m%d')
			resultList.append(resultStr)

			app.logger.info(resultStr)

			app.logger.info('{} date -1 days = {}'.format(dateTime, resultStr))
			return {"data" : resultList, "hide_messages" : True}
		except Exception as e :
			app.logger.error(e)
			return {"error" : {"code" : -1 , "messages" : "날짜 데이터가 유효하지 않습니다."}}

@api.route('/datetime/<string:dateTime>')
class select(Resource) :
	@api.response(200, "Success")
	def options(self, dateTime) :
		return{}

	@api.response(200, "Success")
	def get(self, dateTime) :
		resultList = list()
		try :
			delta = datetime.timedelta(hours = 1)
			app.logger.info(dateTime)
			inputDate = datetime.datetime.strptime(str(dateTime), '%Y%m%d%H%M')
			app.logger.info(inputDate)

			resultDate	= inputDate - delta
			resultStr	= datetime.datetime.strftime(resultDate, '%Y%m%d%H%M')
			resultList.append(resultStr)

			app.logger.info(resultStr)

			app.logger.info('{} date -1 hours = {}'.format(dateTime, resultStr))
			return {"data" : resultList, "hide_messages" : True}
		except Exception as e :
			app.logger.error(e)
			return {"error" : {"code" : -1 , "messages" : "날짜 데이터가 유효하지 않습니다."}}

@api.route('/datetimeDate/<string:hoursDelta>')
class select(Resource) :
	@api.response(200, "Success")
	def options(self, hoursDelta) :
		return{}

	@api.response(200, "Success")
	def get(self, hoursDelta) :
		resultList = list()
		try :
			delta = datetime.timedelta(hours = int(hoursDelta))

			inputDate = datetime.datetime.now()
			app.logger.info(inputDate)

			resultDate	= inputDate - delta
			resultStr	= datetime.datetime.strftime(resultDate, '%Y-%m-%d %H:%M:%S')
			resultList.append(resultStr)

			app.logger.info(resultList)

			app.logger.info('{} date -{} hours = {}'.format(inputDate, hoursDelta, resultStr))
			return {"data" : resultList, "hide_messages" : True}
		except Exception as e :
			app.logger.error(e)
			return {"error" : {"code" : -1 , "messages" : "날짜 데이터가 유효하지 않습니다."}}

@api.route('/datetimeHours/<string:dateTime>')
class select(Resource) :
	@api.response(200, "Success")
	def options(self, dateTime) :
		return{}

	@api.response(200, "Success")
	def get(self, dateTime) :
		resultList = list()
		try :
			delta = datetime.timedelta(hours = 1)
			app.logger.info(dateTime)

			dateTimeStr = dateTime[0:10]
			resultTimeStr = dateTime[0:12]
		
			resultList.append(dateTimeStr)
			resultList.append(resultTimeStr)

			inputDate = datetime.datetime.strptime(str(dateTimeStr), '%Y%m%d%H')
			app.logger.info(inputDate)

			resultDate	= inputDate - delta
			resultStr	= datetime.datetime.strftime(resultDate, '%Y%m%d%H')
			resultList.append(resultStr)

			app.logger.info(resultList)

			app.logger.info('{} date -1 hours = {}'.format(dateTimeStr, resultStr))
			return {"data" : resultList, "hide_messages" : True}

		except ValueError as ve :
			app.logger.error(ve)
			return {"data" : resultList, "hide_messages" : True}

		except Exception as e :
			app.logger.error(e)
			return {"error" : {"code" : -1 , "messages" : "날짜 데이터가 유효하지 않습니다."}}
		

#@app.route('/image_db/<string:status>')
#def db_image_get(status) :
#	app.logger.info('DB Server Status : {}'.format(status))
#	app.logger.info('image/{}/2D_DB.png'.format(status))
#	return render_template('/img_static.html', image_file='image/{}/{}'.format(status, app.config['DB_PNG']))

#@app.route('/image_server/<string:status>')
#def server_image_get(status) :
#	app.logger.info('Server Status : {}'.format(status))
#	app.logger.info('image/{}/2D_SERVER.png'.format(status))
#	return render_template('/img_static.html', image_file='image/{}/{}'.format(status, app.config['SERVER_PNG']))
#	return render_template('/img_static.html')

@api.route('/select_grid/')
class select(Resource):
	@api.response(200, "Success")
	def options(self):
		return {}

	@api.doc(parser=json_parser)
	@api.expect(get_req_parser)
	@api.response(200, "Success")
	def get(self):		
		selectParam = json.loads(request.args["param"])

		global_variable.set('rows_list', [])
		global_variable.set('fields', [])
		conn 	= None
		cursor 	= None
		sql 	= None
		nwCdCol = None
#		columnDict = json.loads(app.config['COLUMNS_KEY'])

		try :
			columnDict = json.loads(app.config['COLUMNS_KEY'])
			irisClient = connectIirs()
			app.logger.info(app.config['{}_TABLE'.format(selectParam['protocol'])])
			tableName = '{}_TABLE'.format(selectParam['protocol'])

			delta 		= datetime.timedelta(hours = 1)
			fromDateStr	= selectParam['event_time']

			inputDate 	= datetime.datetime.strptime(fromDateStr, '%Y%m%d%H%M%S')

			app.logger.info(inputDate)

			toDate		= inputDate + delta
			toDateStr	= datetime.datetime.strftime(toDate, '%Y%m%d%H%M%S')

			if 'N000000003' == selectParam['nw_code'] :
				nwCdCol = 'SERVERNWCODE'
			else :	
				nwCdCol = 'CLIENTNWCODE'

			sql = """
				/*+ LOCATION ( PARTITION >= '{}0000' AND PARTITION <= '{}0000' ) */ 
				SELECT 
					{}
				from
					{}
				WHERE
					DEVICEID = '{}' and {} = '{}' and PROTOCOL = '{}' and CREATETIME like '{}%' ;
				""".format(fromDateStr[0:10], toDateStr[0:10], app.config[selectParam['protocol']], app.config['{}_TABLE'.format(selectParam['protocol'])], selectParam['device_id'], nwCdCol, selectParam['nw_code'], selectParam['protocol'], fromDateStr[0:12] )
#format(event_time, event_time, app.config[protocol], app.config['{}_TABLE'.format(protocol)], device_id, nw_code, protocol)
#       sql = str('select * from TRANSPORTSTAT mlimit 10;')

			app.logger.info(sql)
		except Exception as e :
			app.logger.error(e)

		try:
			rslt = irisClient.execute_iter(sql)
			app.logger.info(rslt)
			if rslt.startswith('+OK') :
				meta_data = cursor.Metadata()

				for row in cursor :
					global_variable.get('rows_list').append(row)

				for cname in meta_data["ColumnName"]:
					global_variable.get('fields').append({"name": columnDict[cname], "type": "TEXT", "grouped": False})
#					global_variable.get('fields').append({"name": cname, "type": "TEXT", "grouped": False})
				return {"data": {"fields": global_variable.get('fields'), "results": global_variable.get('rows_list')}}

		except Exception as ex:
			# __LOG__.Trace("Except: %s" % ex)
			app.logger.error(ex)
			return {"Except" : str(ex) }
		#finally:
			#closeIris(conn, cursor)
		return {}

def parse_req_data(request):
	if not hasattr(request, 'method'):
		return None
	if request.method.upper() != 'GET':
		if request.data:
			return json.loads(request.data)
	if 'json' in request.args:
		return json.loads(request.args['json'])
	if request.args:
		return request.args  # note: type is ImmutableMultiDict
	return {}


@app.after_request
def after_request(response):
	response.headers.add('Access-Control-Request-Method', '*')
	return response

def _init_logger(logPath) :
	logFormatStr = '[%(asctime)s] | p%(process)s | %(pathname)s:%(lineno)d | %(levelname)s - %(message)s'
	logging.basicConfig(format = logFormatStr, filename = logPath, level=logging.INFO)
	formatter = logging.Formatter(logFormatStr,'%m-%d %H:%M:%S')
	fileHandler = TimedRotatingFileHandler(logPath, when='midnight', interval=1, backupCount=10)
	fileHandler.setLevel(logging.DEBUG)
	fileHandler.setFormatter(formatter)
	app.logger.addHandler(fileHandler)
	app.logger.info('Logging is set up.')

if __name__ == '__main__' :
	app.config.from_pyfile(sys.argv[1], silent=True)
	_init_logger(app.config['LOG_PATH'])

	app.run(host= app.config['SERVER_IP'], port = int(app.config['SERVER_PORT']), debug=True)
	app.logger.info('API Start')
