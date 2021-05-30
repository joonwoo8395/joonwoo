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
from functools import wraps

sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
import API_py3.M6 as M6
import API_py3.Log as Log

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


def options(f):
    @wraps(f)
    def options(*args, **kwargs):
        return f(*args, **kwargs)
    return options


@api.route('/select/<string:table_name>')
class select(Resource):
    @options
    @api.doc(parser=json_parser)
    @api.expect(get_req_parser)
    @api.response(200, "Success")
    def get(self, table_name):
        global_variable.set('rows_list', [])
        global_variable.set('fields', [])

        param = request.args["param"]

        conn = M6.Connection('b-iris.mobigen.com', 'kepco_iot', 'kepco12#$',
                             Direct=False, Database='KEPCO_IOT')
        c = conn.Cursor()

        sql = str('select * from %s mlimit 10;' % table_name)

        try:
            rslt = c.Execute2(sql)
            if rslt.startswith('+OK'):
                meta_data = c.Metadata()

                for row in c:
                    global_variable.get('rows_list').append(row)

                for cname in meta_data["ColumnName"]:
                    global_variable.get('fields').append({"name": cname, "type": "TEXT", "grouped": False})
                return {"data": {"fields": global_variable.get('fields'), "results": global_variable.get('rows_list')}}

        except Exception as ex:
            # __LOG__.Trace("Except: %s" % ex)
            print("Except: %s" % ex)
        finally:
            c.Close()
            conn.close()
        return {}


@api.route('/post/')
class post(Resource):
    @options
    @api.response(200, "Success")
    def post(self):
        args = parse_req_data(request)
        return {"success": {"code": 0, "messages": args["json"]}, "data": {"method": "post"}}


@api.route('/put/')
class put(Resource):
    @options
    @api.response(200, "Success")
    def put(self):
        args = parse_req_data(request)
        return {"error": {"code": -1, "messages": args["json"]}, "data": {"method": "put"}}

@api.route('/delete/')
class delete(Resource):
    @options
    @api.response(200, "Success")
    def delete(self):
        args = parse_req_data(request)
        return {"success": {"code": 0, "messages": args["json"]}, "data": {"method": "post"}}

@api.route('/patch/')
class patch(Resource):
    @options
    @api.response(200, "Success")
    def patch(self):
        args = parse_req_data(request)
        return {"success": {"code": 0, "messages": args["json"]}, "data": {"method": "patch"}}


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
    app.run(host='192.168.102.253', port=5051, debug=True)
