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
cm_nb_app_equip_info_pk		= ['IPV4_LIST', 'PORT_LIST']
updateKey 					= ['UPDATEKEY', 'UPDATEKEY2', 'UPDATEKEY3']
cm_nb_app_equip_info_key	= {'UPDATEKEY' : 'IPV4_LIST', 'UPDATEKEY2' : 'PORT_LIST'}

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
		insertParam = json.loads(request.args["param"])

#		conn, cursor = connectIirs('192.168.100.180:5050', 'kepco_iot', 'kepco12#$', 'KEPCO_IOT')
		conn, cursor = connectPostgre('192.168.102.107', 'iot', 'iot.123', 'iotdb')

		columns	= list()
		values	= list()

		keyList		= list()

		deleteKeyStr = None

		for key, value in insertParam.items() :
			if key.upper() in cm_nb_app_equip_info_pk :
				keyList.append("%s = '%s'"%(key.upper(), value) )

#			columns.append(str(key))
#			values.append(str(value))


		if len(keyList) >= 2 :
			deleteKeyStr = ' and '.join(keyList)

#		colStr	= ','.join(columns)
#		valStr	= "','".join(values)

		sql = ''' delete from %s where %s;
			''' % (table_name , deleteKeyStr)

		print (sql)

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

@api.route('/update/<string:table_name>')
class update(Resource) :
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

		updateList 	= list()
		keyList		= list()

		updateSetStr = None
		updateKeyStr = None
		updateList	 = list()
		keyIdx		 = 0

		for key, value in insertParam.items() :
			if key.upper() in updateKey :
				keyList.append("%s = '%s'"%(cm_nb_app_equip_info_key[key.upper()], value) )
				keyIdx = keyIdx + 1
			else :
				updateList.append("%s = '%s'" %(key.upper(), value) )

#			columns.append(str(key))
#			values.append(str(value))


		updateSetStr = ','.join(updateList)
		if len(keyList) >= 2 :
			updateKeyStr = ' and '.join(keyList)

#		colStr	= ','.join(columns)
#		valStr	= "','".join(values)

		sql = ''' update %s set %s where %s;
			''' % (table_name , updateSetStr, updateKeyStr)

		print(sql)
		try :
#			result = cursor.Execute2(sql)
			result = cursor.execute(sql)
			print(result)
			conn.commit()
#			if result.startswith('+OK') :
#				return{"success" : {"code" : 0}}
#			if table_name == 'CM_NB_APP_EQUIP_INFO' :
#				return{"success" : {"code" : 0}}
			return{"success" : {"code" : 0, "messages" : "Update Success"}}

		except Exception as e :
			print(e)
			return {"error" : {"code" : -1, "messages" : "???????????? ???????????????."}}
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

		for key, value in insertParam.items() :
			columns.append(str(key))
			values.append(str(value))
		
		colStr	= ','.join(columns)
		valStr	= "','".join(values)
		
		sql = ''' insert into %s (%s) values('%s');
			''' % (table_name, colStr, valStr)
		try :
#			result = cursor.Execute2(sql)
			
			result = cursor.execute(sql)
#			if result.startswith('+OK') :
#				return{"success" : {"code" : 0}, "messages" : "???????????? ??????"}
#			if table_name == 'CM_NB_APP_EQUIP_INFO' :
#				return{"success" : {"code" : 0}}
			conn.commit()
			return{"success" : {"code" : 0, "messages" : "?????? ??????\n{}".format('\n'.join(values))}}

		except Exception as e :
			return {"error" : {"code" : -1, "messages" : "???????????? ???????????????."}}
		finally :
			closePostgre(conn, cursor)

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
			return {"error" : {"code" : -1, "messages" : "???????????? ???????????????."}}
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
			return {"error" : {"code" : -1, "messages" : "???????????? ???????????????."}}
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
