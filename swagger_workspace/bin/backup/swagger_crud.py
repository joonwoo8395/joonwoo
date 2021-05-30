#!/bin/env python3
# -*- coding:utf8 -*-

import os
import sys
import werkzeug
import json

werkzeug.cached_property = werkzeug.utils.cached_property

from flask import Flask, request
from flask_restplus import Api, Resource, fields, Namespace, cors
from flask_restplus._http import HTTPStatus
from flask_cors import CORS
from flask_restplus import reqparse

sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
import API_py3.M6 as M6
import API_py3.Log as Log
import psycopg2
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
updateKey 					= ['UPDATEKEY', 'UPDATEKEY2', 'UPDATEKEY3']
tableUpdateKeyDict			= {
								'CM_NB_SERVICE_GROUP' : {'UPDATEKEY' : 'NW_CODE'},
								'CM_NB_APP_EQUIP_INFO' : {'UPDATEKEY' : 'IPV4_LIST'}, 
								'CM_NB_COMMON_CODE' : {'UPDATEKEY' : 'CM_CODE'},
								'CM_NB_APP_INFO' : {'UPDATEKEY' : 'APP_SUB_SERVICE', 'UPDATEKEY2' : 'APP_SUB_SERVICE_CODE'},
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
								'FM_IOT_ALARM_THRESHOLD' : {'UPDATEKEY' : 'STAT_TYPE', 'UPDATEKEY2' : 'APP_SERVICE_CODE', 'UPDATEKEY3' : 'ALARM_CODE'}
							}

def connectIirs(url, userId, passwd, db) :
	conn 	= M6.Connection(url, userId, passwd, Direct=False, Database=db )
	cursor	= conn.Cursor()

	return conn, cursor

def connectPostgre(url, userId, passwd, db) :
	conn	= psycopg2.connect(host = url, dbname = db, user = userId, password = passwd)
	cursor	= conn.cursor()
	
	return conn, cursor

def closeIris(conn, cursor) :
	if cursor != None :
		cursor.Close()

	if conn != None :
		conn.close()

def	closePostgre(conn, cursor) :
	if cursor != None :
		cursor.close()

	if conn != None :
		conn.close()

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
	def get(self, table_name) :
		deleteParam = json.loads(request.args["param"])

#		conn, cursor = connectIirs('192.168.100.180:5050', 'kepco_iot', 'kepco12#$', 'KEPCO_IOT')
		conn, cursor = connectPostgre('192.168.102.107', 'iot', 'iot.123', 'iotdb')

		columns	= list()
		values	= list()

		keyList		= list()

		deleteKeyStr = None

		for key, value in deleteParam.items() :
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
	def get(self, table_name) :
		deleteParam = json.loads(request.args["param"])

		conn, cursor = connectIirs('192.168.100.180:5050', 'kepco_iot', 'kepco12#$', 'KEPCO_IOT')

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

		try :
			result = cursor.Execute2(sql)
			if result.startswith('+OK') :
				return{"success" : {"code" : 0, "messages": "Delete Success\n{}".format(deleteKeyStr)}}

		except Exception as e :
			return {"Exception" : str(e)}
		finally :
			closeIris(conn, cursor)

		return {}
@api.route('/update/<string:table_name>')
class update(Resource) :
	@api.response(200, "Success")
	def options(self, table_name) :
		return {}

	@api.expect(get_req_parser)
	@api.response(200, "Success")
	def get(self, table_name) :
		updateParam = json.loads(request.args["param"])

#		conn, cursor = connectIirs('192.168.100.180:5050', 'kepco_iot', 'kepco12#$', 'KEPCO_IOT')
		conn, cursor = connectPostgre('192.168.102.107', 'iot', 'iot.123', 'iotdb')

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

		print (sql)

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
			print(e)
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
	def get(self, table_name) :
		insertParam = json.loads(request.args["param"])

#		conn, cursor = connectIirs('192.168.100.180:5050', 'kepco_iot', 'kepco12#$', 'KEPCO_IOT')
		conn, cursor = connectPostgre('192.168.102.107', 'iot', 'iot.123', 'iotdb')

		columns	= list()
		values	= list()

		now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

		for key, value in insertParam.items() :
			if key == 'UPDATE_TIME' or key == 'INSERT_TIME' or key == 'CREATE_TIME' or key == 'UPDATE_DATE' or key == 'INSERT_DATE' :
				columns.append(str(key))
				values.append(str(now))
			else :
				columns.append(str(key))
				values.append(str(value))
		
		colStr	= ','.join(columns)
		valStr	= "','".join(values)

		
		sql = ''' insert into %s (%s) values('%s');
			''' % (table_name, colStr, valStr)
		print (sql)
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
			print (e)
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
	def get(self, table_name) :
		insertParam = json.loads(request.args["param"])

		conn, cursor = connectIirs('192.168.100.180:5050', 'kepco_iot', 'kepco12#$', 'KEPCO_IOT')
#		conn, cursor = connectPostgre('192.168.102.107', 'iot', 'iot.123', 'iotdb')

		columns	= list()
		values	= list()

		now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

		for key, value in insertParam.items() :
			if key == 'UPDATE_TIME' or key == 'INSERT_TIME' or key == 'CREATE_TIME' or key == 'UPDATE_DATE' or key == 'INSERT_DATE' :
				columns.append(str(key))
				values.append(str(now))
			else :
				columns.append(str(key))
				values.append(str(value))
		
		colStr	= ','.join(columns)
		valStr	= "','".join(values)

		
		sql = ''' insert into %s (%s) values('%s');
			''' % (table_name, colStr, valStr)
		try :
			result = cursor.Execute2(sql)
			
			if result.startswith('+OK') :
				return{"success" : {"code" : 0, "messages" : "추가 성공\n{}".format('\n'.join(values))}}

		except Exception as e :
			print (e)
			return {"error" : {"code" : -1, "messages" : "데이터를 확인하세요."}}
		finally :
			closeIris(conn, cursor)

		return {}

@api.route('/update_iris/<string:table_name>')
class updateIris(Resource) :
	@api.response(200, "Success")
	def options(self, table_name) :
		return {}

	@api.expect(get_req_parser)
	@api.response(200, "Success")
	def get(self, table_name) :
		updateParam = json.loads(request.args["param"])

		conn, cursor = connectIirs('192.168.100.180:5050', 'kepco_iot', 'kepco12#$', 'KEPCO_IOT')

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

		try :
			result = cursor.Execute2(sql)
			if result.startswith('+OK') :
				return{"success" : {"code" : 0, "messages" : "Update Success"}}

		except Exception as e :
			print(e)
			return {"error" : {"code" : -1, "messages" : "데이터를 확인하세요."}}
		finally :
			closeIris(conn, cursor)
#			closePostgre(conn, cursor)

		return {}


@api.route('/server_list_insert/<string:table_name>')
class serverListInsert(Resource) :
	@api.response(200, "Sucess")
	def options(self, table_name) :
		return {"success" : {"code" : 200}}

	def get(self, table_name) :
		selectKey =	request.args["param"]

		conn, cursor = connectIirs('192.168.100.180:5050', 'kepco_iot', 'kepco12#$', 'KEPCO_IOT')

		columns	= list()
		values	= list()

#		for key, value in insertParam.items() :
#			columns.append(str(key))
#			values.append(str(value))
		
#		colStr	= ','.join(columns)
#		valStr	= "','".join(values)

		selectSQL = ''' 
						SELECT 
							SERVER, SERVICE, IP, PORT, PROT, SERVER_CD
						FROM
							KEPCO_IOT.SERVER_LIST
						WHERE SERVER_CD = '%s' limit 1 ;
					''' % selectKey
		insertSQL = ''' 
						INSERT INTO %s (SERVER, SERVICE, IP, PORT, PROT, SERVER_CD)
						VALUES('%s');
					'''
		
		try :
			selectRes = cursor.Execute2(selectSQL)

			if selectRes.startswith('+OK') :
				for row in cursor :
					for data in row :
						values.append(data)

			valStr	= "','".join(values)

			insertRes = cursor.Execute2(insertSQL % (table_name, valStr))

			if insertRes.startswith('+OK') :
				return{"success" : {"code" : 0}, "hide_messages": True}

		except Exception as e :
			return {"error" : {"code" : -1, "messages" : "데이터를 확인하세요."}}
		finally :
			closeIris(conn, cursor)

		return {}

@api.route('/server_list_delete/<string:table_name>')
class serverListInsert(Resource) :
	@api.response(200, "Sucess")
	def options(self, table_name) :
		return {"success" : {"code" : 200}}

	def get(self, table_name) :
		selectKey =	request.args["param"]

		conn, cursor = connectIirs('192.168.100.180:5050', 'kepco_iot', 'kepco12#$', 'KEPCO_IOT')

		deleteSQL	= '''
						DELETE FROM
							%s
						WHERE SERVER_CD = '%s';
					'''

		try :
			deleteRes = cursor.Execute2(deleteSQL %(table_name, selectKey))

			if deleteRes.startswith('+OK') :
				return {"success":{"code":0}, "hide_messages": True}

		except Exception as e :
			return {"error" : {"code" : -1, "messages" : "데이터를 확인하세요."}}
		finally :
			closeIris(conn, cursor)

		return {}


@api.route('/select/<string:table_name>')
class select(Resource):
	@api.response(200, "Success")
	def options(self, table_name):
		return {}

	@api.doc(parser=json_parser)
	@api.expect(get_req_parser)
	@api.response(200, "Success")
	def get(self, table_name):
		#return {"data":""} 		
		global_variable.set('rows_list', [])
		global_variable.set('fields', [])

		param = request.args["param"]
		conn, cursor = connectIirs('192.168.100.180:5050', 'kepco_iot', 'kepco12#$', 'KEPCO_IOT')

		sql = str('select * from %s limit 10;' % table_name)
#		sql = str('select * from TRANSPORTSTAT mlimit 10;')

		try:
			rslt = cursor.Execute2(sql)
			if rslt.startswith('+OK') :
				meta_data = cursor.Metadata()

				for row in cursor :
					global_variable.get('rows_list').append(row)

				for cname in meta_data["ColumnName"]:
					global_variable.get('fields').append({"name": cname, "type": "TEXT", "grouped": False})
				return {"data": {"fields": global_variable.get('fields'), "results": global_variable.get('rows_list')}}

		except Exception as ex:
			# __LOG__.Trace("Except: %s" % ex)
			return {"Except" : str(ex) }
		finally:
			closeIris(conn, cursor)
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


if __name__ == '__main__':
	app.run(host='192.168.102.253', port=5050, debug=True)
