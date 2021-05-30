#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import sys
import signal
import Mobigen.Common.Log as Log; Log.Init()
import API.M6 as M6
import requests
import json

ENM_WORK_ROLE = 'Amos_Administrator'

ENM_USER_ID = 'administrator';
ENM_USER_PW = 'TestPassw0rd';


class RestAPI:
	def __init__(self):
		pass

	def changeUserRole(self, emsIp, action, oprrId) :
		reqArr, apiMethod, apiUrl = self.makeELGReq(action, oprrId)
		__LOG__.Trace('---------------------------------------------')
		__LOG__.Trace('emsIp : %s' % emsIp)
		__LOG__.Trace('reqArr : %s' % reqArr)
		__LOG__.Trace('apiMethod : %s' % apiMethod)
		__LOG__.Trace('apiUrl : %s' % apiUrl)
		return True
		#code, result = self.execute(emsIp, apiMethod, apiUrl, reqArr)

		#__LOG__.Trace('code : %s' % code)
		#__LOG__.Trace('result : %s' % result)

		#if code == '200' : 
		#	return True
		#else : return False 

	def makeELGReq(self, action, oprrId) :
		reqArr = []	
		# Value is ADD or REMOVE
		oprrIdList	= oprrId.split(';')
		apiMethod 	= 'PUT' 
		apiUrl 		= '/oss/idm/usermanagement/modifyprivileges'
	
		# Value is ADD or REMOVE
		for one in oprrIdList :
			reqArr.append({ 'action' : action, 'user' : one, 'targetGroup' : 'ALL', 'role' : ENM_WORK_ROLE } )
			#reqArr.append({ 'action' : action, 'user' : one, 'targetGroup' : 'ALL', 'role' : 'Amos_Operator' } )

		return reqArr, apiMethod, apiUrl
   
	def getELGSession(self, host) :
		session = requests.Session() 

		res = session.post('https://%s/login' % host, data={'IDToken1' : ENM_USER_ID, 'IDToken2' : ENM_USER_PW}, verify=False, allow_redirects=False)
		redirect_cookie = res.headers['Set-Cookie']
		redirect_url = res.headers['Location']
		headers = {"Cookie": redirect_cookie}
		session.get(redirect_url, headers=headers)

		return session
	
	def execute(self, host, method, uri, param=None) :
		session = self.getELGSession(host)
		method = method.upper()
		res_code = None
		res_body = None
		try :
			requests.packages.urllib3.disable_warnings()
			r = None
			if param is None :
				r = session.request(method, 'https://%s%s' % (host, uri), verify = False) 
			else :
				r = session.request(method, 'https://%s%s' % (host, uri), verify = False, json = param) 
			res_code = r.status_code
			res_body = r.text
		except Exception as e :
			__LOG__.Trace(e)
		finally :
			self.close(session)
		return res_code, res_body

	def close(self, session) :
		try : 
			session.close()
		except : pass

	def getAllUsers(self, host, userName=None) :
		uri = '/oss/idm/usermanagement/users'
		if not userName == None : uri = '%s/%s' % (uri, userName)
		code, result = self.execute(host, 'GET', uri)
		__LOG__.Trace('code : %s' % code)
		__LOG__.Trace('result : %s' % result)
		userRepo = json.loads(result)
		

def main():
	try :
		ra = RestAPI()
		#ra.getAllUsers('bdenm.sktbundang.net:443', 'SKT12345')
		#ra.changeUserRole('bdenm.sktbundang.net:443','ADD','SKT12345')
		#ra.changeUserRole('bdenm.sktbundang.net:443','REMOVE','SKT12345')
		#ra.changeUserRole('192.168.100.55:28443','REMOVE','SKT1234;SKT4321')
	except :
		__LOG__.Exception()
	__LOG__.Trace('end main!')

if __name__ == '__main__':
	pass
	#try:
	#	main()
	#except Exception as e:
	#	__LOG__.Exception('main error')
