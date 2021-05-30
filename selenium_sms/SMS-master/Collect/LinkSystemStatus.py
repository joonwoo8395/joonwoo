#! /bin/env python
#coding:utf8

from threading import Thread
from Queue import Queue
from socket import error
from re import compile
from ConfigParser import *
#from os import *
import pika
import ssl
import subprocess
import time
import paramiko
import sys
import signal
import Mobigen.Common.Log as Log
import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
import ConfigParser
import datetime
from requests.exceptions import ConnectionError


SHUTDOWN = False
def shutdown(sigNum, frame):
	global SHUTDOWN
	SHUTDOWN = True
	sys.stderr.write("Catch Signal :%s" % sigNum)
	sys.stderr.flush()

signal.signal(signal.SIGTERM, shutdown) # sigNum 15 : Terminate
signal.signal(signal.SIGINT, shutdown)  # sigNum  2 : Interrupt
signal.signal(signal.SIGHUP, shutdown)  # sigNum  1 : HangUp
signal.signal(signal.SIGPIPE, shutdown) # sigNum 13 : Broken Pipe


class ServerWatch(object):
	def __init__(self, ip, username, password, port, configParser) :
		self.ip 			= ip
		self.uname 			= username
		self.pw 			= password
		self.port 			= int(port)
		self.client 		= paramiko.SSHClient()
		self.OKFlag 		= "OK"
		self.configParser 	= configParser

	def SSHClinetConnection(self, ip, name, passwd) :
		client = self.client
#		client.load_system_host_keys()
		client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
		try:
			client.connect(ip, username=name, password=passwd, port=self.port, timeout=10)
		except:
			__LOG__.Exception()

		return client

	def commandHOSTNAME(self):
		hlist=[]
		stdin, stdout, stderr = self.client.exec_command('cat /proc/sys/kernel/hostname')
		for line in stdout:
			line = line.strip()
			hlist.append(line)
		retdic={'VALUE':hlist}
		return retdic

	def tangoSSOStatus(self, tangoSSOUrl) :
		result	= []
		response = ''
		try :
			response = str(requests.get(tangoSSOUrl, timeout = 60).status_code)
		except ConnectionError :
			response 	= 'Connection Error'
		except :
			reponse 	= 'Error Check Please'
		
		result.append({'STATUS':'OK', 'VALUE':{'URL': tangoSSOUrl, 'STATUS': response}} )
		return result

	def tangoWmLogStatus(self, tangoWmLogPath, findStr) :
		result 	= []

		nowDate = datetime.datetime.now()
		status 	= 'OK'

		for dateMinute in range(0, 15) :
			logDateCheck 	= ( nowDate - datetime.timedelta(minutes = dateMinute) ).strftime('\[%Y/%m/%d %H:%M')
			cmd = 'grep \'%s\' %s | grep \'%s\'' % (findStr, tangoWmLogPath, logDateCheck)
			stdin, stdout, stderr = self.client.exec_command(cmd)

			resultList = stdout.readlines()

			if len(resultList) == 0 :
				continue

			else :
				status = 'ERROR'
				break

		result.append({'STATUS':'OK', 'VALUE':{'TANGO_WM_PATH': tangoWmLogPath, 'STATUS': status}})

		return result

	def realTimeStatus(self, realTimeUrl, verify = False):
		result = []
		responseDict = dict()

		if 'https' in realTimeUrl :
			headers 	= {"Content-type":"application/json;charset=utf-8"}
			paramData	= {"WORK_ID":"12345"}
			try :
				responseDict[realTimeUrl]	= str(requests.post(url = realTimeUrl, headers = headers, json = paramData, timeout = 60, verify = verify).status_code)
			except ConnectionError :
				responseDict[realTimeUrl]	= 'Connection Error'
			except : 
				responseDict[realTimeUrl]	= 'Error Check Please'
		for oneUrl  in responseDict.keys():
			status 	= responseDict[oneUrl]

			result.append({'STATUS':'OK','VALUE':{'URL': realTimeUrl, 'STATUS': status}})

		return result

	def run(self):
		infodic=dict()
		
		try:
			self.SSHClinetConnection(self.ip, self.uname, self.pw)
			infodic['HOSTNAME'] 		= self.commandHOSTNAME()
			infodic['STATUS']			= self.OKFlag #이건 Connection Status 아닌가?
			infodic['LINK_SYSTEM_STATUS'] 	= {}

			realTimeUrl			= self.configParser.get('LINK_SYSTEM_STATUS', 'REAL_TIME_URL')
			tangoWmLogPath		= self.configParser.get('LINK_SYSTEM_STATUS', 'TANGO_WM_LOG_PATH')
			findStr				= self.configParser.get('LINK_SYSTEM_STATUS', 'TANGO_WM_FIND_STR')
			tangoSSOUrl			= self.configParser.get('LINK_SYSTEM_STATUS', 'TANGO_SSO_URL')

			infodic['LINK_SYSTEM_STATUS'][realTimeUrl] 		= self.realTimeStatus(realTimeUrl)
			infodic['LINK_SYSTEM_STATUS'][tangoWmLogPath] 	= self.tangoWmLogStatus(tangoWmLogPath, findStr)
			infodic['LINK_SYSTEM_STATUS'][tangoSSOUrl]		= self.tangoSSOStatus(tangoSSOUrl)

			self.client.close()
			__LOG__.Trace(infodic)	

			return infodic

		except :
			self.OKFlag = "NOK"
			infodic['STATUS']=self.OKFlag
			shell = "cat /etc/hosts | awk '{if(/"+self.ip+"/){print $2}}'"
			p = subprocess.Popen(shell,shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
			hostname = p.stdout.readline()
			hostname = hostname.strip()
			infodic['HOSTNAME']={'VALUE': [hostname]}
			self.client.close()
			__LOG__.Exception()
			return infodic

class JobProcess(object):
	def __init__(self, svrobjlist):
		self.data_q = Queue([])
		self.THREADPOOL = 10
		self.total = dict()
		self.putdata(svrobjlist)
		
	def job_process(self,th_id):
		while not SHUTDOWN:
			try:
				ip,obj = self.data_q.get_nowait()
				__LOG__.Trace('thread get : %s ' % th_id)
			except:
				__LOG__.Trace("thread %s is done" % th_id)
				break
			self.total[ip] = obj.run() # obj = ServerWatch
			time.sleep(0.1)

	def putdata(self, svrobjlist):
		for ip,svrobj in svrobjlist:
			self.data_q.put((ip,svrobj))

	def makeThreadlist(self):
		th_list = list()
		for i in range(self.THREADPOOL):
			th_obj = Thread(target=self.job_process, args=[i])
			th_list.append(th_obj)
		return th_list

	def run(self):
		th_list = self.makeThreadlist()
		for th_obj in th_list:
			th_obj.start()
		for th_obj in th_list:
			th_obj.join()
		__LOG__.Trace("[Collect]SERVER RESOURCE END_______________________")
		return self.total

class LinkSystemStatus(object):
	def __init__(self, getconfigparser) :
		self.config 	= getconfigparser
#		self.url_list	= getconfigparser.get('TOMCAT_STATUS', 'URL_LIST')
	
	def getConfParser(self):
		conflist 	= list()
		conf_dict 	= dict()

		type_list	= ['SSH_PORT', 'USER_TACS','TACS_PASSWD']

		for rec_ip in self.config.get('TOMCAT_STATUS','SERVER_LIST').split(','):
			conf_dict['IP'] 	= rec_ip
			for type in type_list :
				try :
					conf_dict[type]	= self.config.get(rec_ip, type)
				except :
					conf_dict[type] = self.config.get('RESOURCES', type)

			conflist.append( (conf_dict['IP'], conf_dict['SSH_PORT'], conf_dict['USER_TACS'], conf_dict['TACS_PASSWD'] ,self.config) )
#		__LOG__.Trace(conflist)

		return conflist
	
	def run(self):	
		svrlist =[]
		__LOG__.Trace("[Collect]SERVER RESOURCE START_____________________")
		infolist = self.getConfParser()

		for tup in infolist:
			svr_obj = ServerWatch(tup[0], tup[2], tup[3], tup[1], tup[4])   # ip, username, passwd, port,  getconfigparser
			svrlist.append( (tup[0], svr_obj) )

		jp_obj = JobProcess(svrlist)
		return jp_obj.run()
