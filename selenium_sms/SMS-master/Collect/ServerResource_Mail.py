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


class ServerWatch(object):
	def __init__(self, ip, username, password, password2, port=22):
		self.ip =ip
		self.uname = username
		self.pw = password
		self.pw2 = password2
		self.port = int(port)
		self.client = paramiko.SSHClient()
		self.OKFlag = "OK"

	def SSHClinetConnection(self):
		client = self.client
		client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
		try:
			__LOG__.Trace( 'SSH IP : %s , USER : %s , PORT : %s' %(self.ip, self.uname, str(self.port)) )
			client.connect(self.ip, username=self.uname, password=self.pw, port=self.port, timeout=10)
		except:
			__LOG__.Trace( 'SSH2 IP : %s , USER : %s , PORT : %s' %(self.ip, self.uname, str(self.port)) )
			client.connect(self.ip, username=self.uname, password=self.pw2, port=self.port, timeout=10)

	def commandDISK(self):
		stdin, stdout, stderr = self.client.exec_command('df -m')
		dlist 		= []
		diskMailMsg = ''

		for line in stdout:
			line = line.strip()
			diskMailMsg += line + '\n'
			dlist.append(line.split())
		
		#get total list
		rl =[]
		for idx in range(1, len(dlist)):
			if len(dlist[idx])==1:
				rl.append(dlist[idx]+dlist[idx+1])
			elif len(dlist[idx])==6:
				rl.append(dlist[idx])
		del(dlist)

		#get total, use, free, usage value
		result = []
		for i in range(len(rl)):
			total = int(rl[i][2])+int(rl[i][3])
			tmp =[]
			for j in [5,1,2,3,4]:
				if j==1:
					tmp.append(str(total)) #total = use+free
				elif j==4:
					tmp.append(str(int(rl[i][2])*100/total)) #usage percent = use / total
				else:
					tmp.append(rl[i][j])
			result.append({'STATUS':'OK','VALUE':tmp, 'MailMsg' : diskMailMsg})
			del(tmp)
		del(rl)
		return result

	def commandSWAP(self):
		stdin, stdout, stderr = self.client.exec_command('free -m')
		slist 		= []
		swapMailMsg = ''

		for line in stdout:
			line = line.strip()
			swapMailMsg = += line + '\n'
			slist.append(line.split())
		#print slist
		result =[]

		used = int(slist[len(slist)-1][2]) # free -m 의 used 컬럼 값
		free = int(slist[len(slist)-1][3]) # free -m 의 free 컬럼 값
		total = used + free
		result.append(str(total))
		result.append(str(used))
		result.append(str(free))
		result.append(str(used*100/total))
		retdic={'STATUS':'OK','VALUE':result, 'MailMsg' : swapMailMsg}
		return retdic

	def commandLOAD_AVG(self): # 부하평균
		result=[]
		loadAvgMailMsg = ''
		stdin, stdout, stderr = self.client.exec_command('uptime')
		patt = compile(r"[0-9]?\.[0-9]{2}")

		for line in stdout:
			loadAvgMailMsg += line + '\n'
			loadavg = patt.findall(line)
		result.append(loadavg[0])
		result.append(loadavg[1])
		result.append(loadavg[2])
		retdic={'STATUS':'OK','VALUE':result, 'MailMsg' : loadAvgMailMsg} # load average 3개의 값
		return retdic
	
	def commandMEMORY(self):
		stdin, stdout, stderr = self.client.exec_command('free -m')
		flist =[]
		memoryMailMsg = ''

		for line in stdout:
			line = line.strip()
			memoryMailMsg += line + '\n'
			flist.append(line.split())

		#total = used + free + buffers + cached
		total = int(flist[1][2])+int(flist[1][3])+int(flist[1][5])+int(flist[1][6])
		#real free memory = free + buffers + cached
		free_memory = int(flist[1][3])+int(flist[1][5])+int(flist[1][6])
		#real use memory = total - (free + buffers + cached)
		use_memory = total - free_memory
		#real usage percent = (use_memory+free_memory)/use_memory
		usage_percent = use_memory*100 / total

		result =[]
		result.append(str(total))
		result.append(str(use_memory))
		result.append(str(free_memory))
		result.append(str(usage_percent)[:2])
		retdic={'STATUS':'OK','VALUE':result}
		return retdic

	def commandHOSTNAME(self):
		hlist=[]
		stdin, stdout, stderr = self.client.exec_command('hostname')
		for line in stdout:
			line = line.strip()
			hlist.append(line)
		retdic={'VALUE':hlist}
		return retdic

	def run(self):
		infodic=dict()
		try:
			self.SSHClinetConnection()
			infodic['HOSTNAME']=self.commandHOSTNAME()
			infodic['STATUS']=self.OKFlag
			infodic['LOAD_AVG']=self.commandLOAD_AVG()
			infodic['DISK']=self.commandDISK()
			infodic['MEMORY']=self.commandMEMORY()
			infodic['SWAP']=self.commandSWAP()
			self.client.close()
			__LOG__.Trace(infodic)
			return infodic

		except :
			self.OKFlag = "NOK"
			infodic['STATUS']=self.OKFlag
			shell = "hostname"
			p = subprocess.Popen(shell,shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
			hostname = p.stdout.readline()
			hostname = hostname.strip()
			infodic['HOSTNAME']={'VALUE': [hostname]}
			self.client.close()
			__LOG__.Trace(infodic)
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

class ServerResource(object):
	def __init__(self, getconfigparser):
		self.config = getconfigparser
	
	def getConfParser(self):
		conflist = list()
		conf_dict = dict()
		type_list = ['SSH_PORT','USER','PASSWD','PASSWD2']

		for rsc_ip in self.config.get('RESOURCES','SERVER_LIST').split(','):
			conf_dict['IP'] =rsc_ip
			for type in type_list:
				try:
					conf_dict[type] = self.config.get(rsc_ip,type)
				except:
					conf_dict[type] = self.config.get('RESOURCES',type)
					
			conflist.append((conf_dict['IP'], conf_dict['SSH_PORT'], conf_dict['USER'], conf_dict['PASSWD'], conf_dict['PASSWD2']))
#		__LOG__.Trace(conflist)
		return conflist
	
	def run(self):	
		svrlist =[]
		__LOG__.Trace("[Collect]SERVER RESOURCE START_____________________")
		infolist = self.getConfParser()
		for tup in infolist:
			__LOG__.Trace('IP : %s, UserName : %s , Password : %s, Passworkd2 : %s, port : %s' %( tup[0],tup[2],tup[3],tup[4],tup[1]) )
			svr_obj = ServerWatch(tup[0],tup[2],tup[3],tup[4],tup[1]) # ip, username, password, password2, port=22
			svrlist.append((tup[0],svr_obj))

		jp_obj = JobProcess(svrlist)
		return jp_obj.run()
