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

ENM_BUSAN1_DOMAIN = 'bsenm.sk.com'
ENM_BUSAN2_DOMAIN = 'bs2enm.sk.com'
ENM_DAEGU1_DOMAIN = 'dgenm.sk.com'
ENM_DAEGU2_DOMAIN = 'dg2enm.sk.com'

ENM_BUSAN1_PKI = '/home/tacs/cert/enm/ENM_PKI_Root_CA_BUSAN1.pem'
ENM_BUSAN2_PKI = '/home/tacs/cert/enm/ENM_PKI_Root_CA_BUSAN2.pem'

DOMAIN_PKI_REPO = {ENM_BUSAN1_DOMAIN : ENM_BUSAN1_PKI, ENM_BUSAN2_DOMAIN : ENM_BUSAN2_PKI, ENM_DAEGU1_DOMAIN : False, ENM_DAEGU2_DOMAIN : False}
USER_INFO_REPO = {ENM_BUSAN1_DOMAIN : {'PARAM1':'SKT_TACS', 'PARAM2':'Skttacs7!'}, ENM_BUSAN2_DOMAIN:{'PARAM1':'SKT_TACS', 'PARAM2':'Skttacs7!'}, ENM_DAEGU1_DOMAIN : {'PARAM1':'SKT_TACS', 'PARAM2':'Skttacs7!'}, ENM_DAEGU2_DOMAIN : {'PARAM1':'SKT_TACS', 'PARAM2':'Skttacs7!'}}

class RestAPI:
	def __init__(self):
		pass

	def changeUserRole(self, emsIp, action, oprrId, timeout) :
		reqArr, apiMethod, apiUrl = self.makeELGReq(action, oprrId)
		__LOG__.Trace('---------------------------------------------')
		__LOG__.Trace('emsIp : %s' % emsIp)
		__LOG__.Trace('reqArr : %s' % reqArr)
		__LOG__.Trace('apiMethod : %s' % apiMethod)
		__LOG__.Trace('apiUrl : %s' % apiUrl)
		#return True
		code, result = self.execute(emsIp, apiMethod, apiUrl, reqArr, timeout)

		__LOG__.Trace('code : %s' % code)
		__LOG__.Trace('result : %s' % result)

		if code == '200' : 
			return True
		else : return False 

	def makeELGReq(self, action, oprrId) :
		reqArr = []	
		# Value is ADD or REMOVE
		oprrIdList	= oprrId.split(';')
		apiMethod 	= 'PUT' 
		apiUrl 		= '/oss/idm/usermanagement/modifyprivileges'
	
		# Value is ADD or REMOVE
		for one in oprrIdList :
			reqArr.append({ 'action' : action, 'user' : one, 'targetGroup' : 'ALL', 'role' : ENM_WORK_ROLE } )

		return reqArr, apiMethod, apiUrl
   
	def getELGSession(self, host) :
		session = requests.Session() 

		res = session.post('https://%s/login' % host, data={'IDToken1' : USER_INFO_REPO[host]['PARAM1'], 'IDToken2' : USER_INFO_REPO[host]['PARAM2']}, verify=DOMAIN_PKI_REPO[host], allow_redirects=False)
		redirect_cookie = res.headers['Set-Cookie']
		redirect_url = res.headers['Location']
		headers = {"Cookie": redirect_cookie}
		session.get(redirect_url, headers=headers)

		return session
	
	def execute(self, host, method, uri, param=None, session=None, timeout=None) :
		if session == None :
			session = self.getELGSession(host)
		method = method.upper()
		res_code = None
		res_body = None
		try :
			requests.packages.urllib3.disable_warnings()
			r = None
			if param is None :
				r = session.request(method, 'https://%s%s' % (host, uri), verify = DOMAIN_PKI_REPO[host], timeout=timeout) 
			else :
				r = session.request(method, 'https://%s%s' % (host, uri), verify = DOMAIN_PKI_REPO[host], json = param, timeout=timeout) 
			res_code = r.status_code
			res_body = r.text

		except Exception as e :
			__LOG__.Trace(e)
		finally :
			__LOG__.Trace('Request_result = %s' % str(r.status_code))
			__LOG__.Trace('Request_result = %s' % str(r.text)) 

			self.close(session)
		return res_code, res_body

	def close(self, session) :
		try : 
			session.close()
		except : pass

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
