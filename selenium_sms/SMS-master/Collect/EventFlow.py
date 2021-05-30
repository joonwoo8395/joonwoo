#! /bin/env python
#coding:utf8

from threading import Thread
from Queue import Queue
from socket import error
from re import compile
from ConfigParser import *
#from os import *
import subprocess
import time
import paramiko
import sys
import signal
import Mobigen.Common.Log as Log

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


class ServerWatch(object) :
	def __init__(self, ip, username, password, port, path, password2, qCnt21, qCnt30, qCnt32, qCnt33):
		self.ip =ip
		self.uname = username
		self.pw = password
		self.pw2 = password2
		self.port = int(port)
		self.path = path
		self.client = paramiko.SSHClient()
		self.OKFlag = "OK"
		self.Q_CNT = {'20001' : qCnt21, '30000' : qCnt30, '32000' : qCnt32, '33000' : qCnt33}

	def SSHClinetConnection(self):
		client = self.client
#		client.load_system_host_keys()
		client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
		try:
			client.connect(self.ip, username=self.uname, password=self.pw, port=self.port, timeout=10)
		except:
			client.connect(self.ip, username=self.uname, password=self.pw2, port=self.port, timeout=10)
	
	def commandHOSTNAME(self):
		hlist=[]
		stdin, stdout, stderr = self.client.exec_command('cat /proc/sys/kernel/hostname')
		for line in stdout:
			line = line.strip()
			hlist.append(line)
		retdic={'VALUE':hlist}
		return retdic

	def commandQUEUE_COUNT2(self, port):
		__LOG__.Trace(port)
		result 		= []
		excNodeDict	= dict()
		cmd = '%s %s' % (self.path, port)
		__LOG__.Trace(cmd)
		stdin, stdout, stderr = self.client.exec_command(cmd)
		rs = stdout.readlines()
		__LOG__.Trace('\n' + '\n'.join(rs))
		for line in rs:
			if line.startswith('<begin>') or line.startswith('<end>') or line.startswith('-----------') or ('ACTIVATE TIME' in line and 'Q SIZE' in line):
				continue
			abn = line.split('|')[0].strip()
			ps_name = line.split('|')[1].strip()
			status = line.split('|')[3].strip()
			que_cnt = line.split('|')[6].strip()

			if int(que_cnt) > int(self.Q_CNT[port]) and ('OK' in abn) and ('ACT' in status) :
		
				excNodeDict[ps_name] = que_cnt

			result.append({'STATUS':'OK','VALUE':{'NODE':ps_name, 'ABN':abn, 'Q_STATUS': 'OK', 'STATUS': status , 'Q_CNT' : que_cnt}})

		if excNodeDict :
			result = self.commandQUEUE_COUNT(port, result, excNodeDict)

		return result

	def commandQUEUE_COUNT(self, port, result, nodeDict):
		__LOG__.Trace('Exceed Queue Count')
		tempNum = 0
		firFlag = False
		secFlag	= False
		while tempNum < 2 :
			time.sleep(3)
			tempNum += 1

			cmd = '%s %s' % (self.path, port)
			stdin, stdout, stderr = self.client.exec_command(cmd)
			rs = stdout.readlines()
			for line in rs :	
				if line.startswith('<begin>') or line.startswith('<end>') or line.startswith('-----------') or ('ACTIVATE TIME' in line and 'Q SIZE' in line) :
					continue
				__LOG__.Trace(line)
				resultList = line.split('|')

				if resultList[1].strip() not in nodeDict.keys() :
					continue

				ps_name	= resultList[1].strip()
				que_cnt = resultList[6].strip()

				if int(que_cnt) > int(nodeDict[ps_name]) and tempNum == 1 :
					nodeDict[ps_name] = que_cnt
					firFlag = True
					for oneReDict in result :
						if oneReDict['VALUE']['NODE'] == ps_name :
							oneReDict['VALUE']['Q_CNT'] += (', ' + que_cnt)

				if firFlag and tempNum == 2 :
					for oneReDict in result :
						if oneReDict['VALUE']['NODE'] == ps_name and int(que_cnt) > int(nodeDict[ps_name]) :
							oneReDict['VALUE']['Q_STATUS'] = 'NOK'	
							oneReDict['VALUE']['Q_CNT'] += (', ' + que_cnt)
							
				if not firFlag :
					break

		return result
		
	def run(self):
		__LOG__.Trace('Start EventFlow')	
		infodic=dict()
		try:
			self.SSHClinetConnection()
			infodic['HOSTNAME']=self.commandHOSTNAME()
			infodic['STATUS']=self.OKFlag #이건 Connection Status 아닌가?
			infodic['EF'] = {}
			stdin, stdout, stderr = self.client.exec_command('ps -ef | grep SIOEF | grep -v grep') # 구동중인  Event Flow 구하기
			rs = stdout.readlines()
			PORT_IDX = -2
			for ef in rs:
				port = ef.split()[PORT_IDX]
				infodic['EF'][port] = self.commandQUEUE_COUNT2(port)

				#infodic[port] = self.commandQUEUE_COUNT2(port)
				#infodic['QUEUE_COUNT']=self.commandQUEUE_COUNT2(port)
			self.client.close()

			return infodic

		#password나 useraname이 잘못되었을 경우
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
			self.total[ip] = obj.run()
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

class EventFlow(object):
	def __init__(self, getconfigparser):
		self.config = getconfigparser
	
	def getConfParser(self):
		conflist = list()
		conf_dict = dict()
		type_list = ['SSH_PORT','USER','PASSWD','PASSWD2']
		path = self.config.get('EVENTFLOW','PATH')		
		qCnt21	= self.config.get('EVENTFLOW','20001_Q_CNT')
		qCnt30	= self.config.get('EVENTFLOW','30000_Q_CNT')
		qCnt32	= self.config.get('EVENTFLOW','32000_Q_CNT')
		qCnt33	= self.config.get('EVENTFLOW','33000_Q_CNT')

		for rsc_ip in self.config.get('EVENTFLOW','SERVER_LIST').split(','):
			conf_dict['IP'] =rsc_ip
			for type in type_list:
				try:
					conf_dict[type] = self.config.get('EVENTFLOW',type)
				except:
					conf_dict[type] = self.config.get('RESOURCES',type)
					
			conflist.append((conf_dict['IP'], conf_dict['SSH_PORT'], conf_dict['USER'], conf_dict['PASSWD'], path, conf_dict['PASSWD2'], qCnt21, qCnt30, qCnt32, qCnt33))
		return conflist
	
	def run(self):	
		svrlist =[]
		__LOG__.Trace("[Collect]SERVER RESOURCE START_____________________")
		infolist = self.getConfParser()

		for tup in infolist:
			svr_obj = ServerWatch(tup[0],tup[2],tup[3],tup[1],tup[4],tup[5],tup[6],tup[7],tup[8],tup[9]) # ip, username, password, port, path, password2
			svrlist.append((tup[0],svr_obj))
		jp_obj = JobProcess(svrlist)
		return jp_obj.run()

