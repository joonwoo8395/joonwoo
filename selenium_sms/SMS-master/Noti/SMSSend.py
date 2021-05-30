# -*- coding: utf-8 -*-
#!/usr/bin python

import datetime
import sys
import os
import re

import Mobigen.Common.Log as Log
from socket import *
import time
#import ConfigParser
import struct

IDX_IP = 0
IDX_PORT = 1

IDX_VALUE = 0
IDX_DESC = 1

IDX_IRIS_NODEID = 0
IDX_IRIS_SYS_STATUS = 1
IDX_IRIS_ADM_STATUS = 2
IDX_IRIS_UPDATE_TIME = 3
IDX_IRIS_CPU = 4
IDX_IRIS_LOADAVG = 5
IDX_IRIS_MEMP = 6
IDX_IRIS_MEMF = 7
IDX_IRIS_DISK = 8

IDX_MEMORY_TOTAL = 0
IDX_MEMORY_USED = 1
IDX_MEMORY_AVAILABE = 2
IDX_MEMORY_USE_PER = 3

IDX_DISK_MOUNT = 0
IDX_DISK_1MBLOCKS = 1
IDX_DISK_USED = 2
IDX_DISK_AVAILABLE = 3
IDX_DISK_USE_PER = 4

IDX_LOG_DATE = 0
IDX_LOG_VALUE = 1
IDX_FILE_VALUE =1 

class SMSSend() :
	#제목 / 통신사 정보 Dict / 전화번호 정보 Dict / 값 Dict
	def __init__(self, _Parser, _ValueDict,collect) :
		self.ValueDict = _ValueDict
		self.PARSER = _Parser
		self.Collect = collect
		
		self.GetConfig()

	def GetConfig(self) :
		try :
			self.Title 			= self.PARSER.get('SMS', 'TITLE')
			self.fromNumber 	= self.PARSER.get('SMS', 'FROM_Number')
			self.toNumberList	= self.PARSER.get('SMS', 'TO_NUMBER').split(',')
			self.alramFilePath	= self.PARSER.get('SMS', 'ALRAM_OPEN_PATH')
		
		except :
			__LOG__.Exception()

	def alreadyAlramMsg(self, Server, Type, status, DiskMount=None) :
		if not os.path.exists(self.alramFilePath) :
			try :
				os.makedirs(self.alramFilePath)
			except :
				__LOG__.Exception()

		errFilePath		= None
		
		if DiskMount :
			if '/' in DiskMount :
				DiskMount = DiskMount.replace('/', '-')
			errFilePath = os.path.join(self.alramFilePath, '%s_%s_%s' % (Server, Type, DiskMount) )
		else :
			errFilePath = os.path.join(self.alramFilePath, '%s_%s' % (Server, Type) )

		if status == 'NOK' :
			__LOG__.Trace( '이상 발생 [%s] - [%s]' %(Server, Type) )

			if not os.path.exists( errFilePath ) :
				with open (errFilePath, 'w' ) as filePath :
					filePath.write('')
				__LOG__.Trace('Error 처음 발생 %s' % errFilePath)
				return True, 'ERR-Open'

			else :
				__LOG__.Trace('Error 해결중.. %s' % errFilePath )
				return False, None
				

		else :
			__LOG__.Trace( '이상 없음  [%s] - [%s]' %(Server, Type) )
			
			if not os.path.exists( errFilePath ) :
				return False, None
			else :
				__LOG__.Trace('Error 해결 %s' % errFilePath )
				os.remove(errFilePath)
				return True, 'ERR-Close'

	def run(self) :
		try :
			#Make Msg List
			MsgList = []
			for Server in self.ValueDict.keys() :
				HostName = self.ValueDict[Server]['HOSTNAME']['VALUE'][0]

				for Type in self.ValueDict[Server].keys() :
					__LOG__.Trace(Type)

					# UTF-8 로 바꿀 필요성
					if type(HostName)==unicode : 
						HostName = HostName.encode('cp949')

					msgFlag	 	= False
					msgStatus	= None

					######## OPEN, CLOSE 개념추가 2020-04-02  ################
					if Type == 'STATUS' :
						msgFlag, msgStatus = self.alreadyAlramMsg(Server ,Type, self.ValueDict[Server][Type])

					elif Type == 'IRIS' or Type == 'MEMORY' or Type == 'SWAP' or Type == 'LOAD_AVG' or Type == 'IRIS_OPENLAB' or Type == 'WEB-LOGIN' :
						msgFlag, msgStatus = self.alreadyAlramMsg(Server ,Type, self.ValueDict[Server][Type]['STATUS'])

					elif Type == 'DISK' :
						msgFlag = True

					if not msgFlag :
						continue
					##########################################################

					#Connected Fail
					if Type == 'STATUS' :
						#and self.ValueDict[Server][Type] == 'NOK') : 
						Msg = '[%s-%s] | %s | (%s) | Connected Fail' % (self.Title, msgStatus, Server,HostName) # SSH 자체 Connection Error
						Msg = Msg.decode('utf-8')
						MsgList.append(Msg)
						continue
					elif Type == 'IRIS' :
						__LOG__.Trace(self.ValueDict[Server][Type])
#						if self.ValueDict[Server][Type]['STATUS'] == 'NOK' : 
						Desc = self.ValueDict[Server][Type]['VALUE'][IDX_IRIS_SYS_STATUS] + '/' + self.ValueDict[Server][Type]['VALUE'][IDX_IRIS_UPDATE_TIME]
						Msg = '[%s-%s]  | %s (%s) | %s | %s' % (self.Title, msgStatus, Server, HostName, Type, Desc) # IRIS Error
						
						Msg = Msg.decode('utf-8')
						MsgList.append(Msg)
						continue
						#	Msg = Msg.decode('utf-8')

					elif Type == 'MEMORY' or Type == 'SWAP' :
						#__LOG__.Trace(self.ValueDict[Server][Type]['VALUE'][IDX_MEMORY_USE_PER])
#						if self.ValueDict[Server][Type]['STATUS'] == 'NOK' : 
						Msg = '[%s-%s] | %s | (%s) | %s(%%)' % (Type, msgStatus, Server, HostName, self.ValueDict[Server][Type]['VALUE'][IDX_MEMORY_USE_PER])

						Msg = Msg.decode('utf-8')
						MsgList.append(Msg)
						continue
																						# 사용 메모리 * 100 / total 메모리 [total 메모리 = used + free 
					elif Type == 'LOAD_AVG' :
						#if self.ValueDict[Server][Type]['STATUS'] == 'NOK' : 
						Msg = '[%s-%s] | %s | (%s) | %s' % (self.Title, msgStatus, Server, HostName, Type, '/'.join(self.ValueDict[Server][Type]['VALUE']))
																					# 1분 시스템 평균 부하율 / 5분 .. / 15분 ..
						Msg = Msg.decode('utf-8')
						MsgList.append(Msg)
						continue
					elif Type == 'DISK' :
						for Disk in self.ValueDict[Server][Type] :
							#__LOG__.Trace(Disk)
							msgFlag, msgStatus = self.alreadyAlramMsg(Server, Type, Disk['STATUS'], Disk['VALUE'][IDX_DISK_MOUNT]) 
							if not msgFlag :
								continue
						
#							if Disk['STATUS'] == 'NOK' : 
################################# 임시 추가 #######################
#								if (HostName == 'Koti-Chain-01' or HostName == 'Koti-Chain-02') and ('/snap/core/8592' == Disk['VALUE'][IDX_DISK_MOUNT] or '/snap/core/8689' ==  Disk['VALUE'][IDX_DISK_MOUNT] ) :
#									continue
##################################################################
#								else :
							Msg = '[%s-%s] | %s | (%s) | %s | %s | %s(%%)' % (self.Title, msgStatus, Server, HostName, Type, Disk['VALUE'][IDX_DISK_MOUNT], Disk['VALUE'][IDX_DISK_USE_PER])
																									# mount 위치, 디스크 사용 퍼센트	
							Msg = Msg.decode('utf-8')
							MsgList.append(Msg)

					elif Type == 'IRIS_OPENLAB' :
#						if self.ValueDict[Server][Type]['STATUS'] == 'NOK' :
						__LOG__.Trace(self.ValueDict[Server][Type]['VALUE'])
						Msg = '[%s-%s] | %s (%s) | %s' % (self.Title, msgStatus, Server, HostName, self.ValueDict[Server][Type]['VALUE'])
						Msg = Msg.decode('utf-8')
						MsgList.append(Msg)
						continue
					elif Type == 'WEB-LOGIN' :
#						if self.ValueDict[Server][Type]['STATUS'] == 'NOK' :
						Msg = '[%s-%s] | %s (%s) | %s' % (self.Title, msgStatus, Server, HostName, self.ValueDict[Server][Type]['STATUS'])
						__LOG__.Trace(self.ValueDict[Server][Type]['VALUE'])
						Msg = Msg.decode('utf-8')
						MsgList.append(Msg)
						continue
					else : pass
			#Msg 전송 = 한 Connection 당 한 Number 메시지 전송
			if MsgList :
				for Msg in MsgList :
					__LOG__.Trace('\n'+Msg)
					for toNumber in self.toNumberList : # 전화번호
						__LOG__.Trace('TO Number[%s]' % toNumber)
						__LOG__.Trace("/home/tacs/user/KimJW/selenium_sms/SMS-master/smsTransfer.p %s %s '%s'" %(self.fromNumber, toNumber, Msg) )
						os.system("/home/tacs/user/KimJW/selenium_sms/SMS-master/smsTransfer.p %s %s '%s'" %(self.fromNumber, toNumber, Msg) ) 
						time.sleep(2) #빨리 보내면 안감 , 그래서 Sleep 줌
			else :
				__LOG__.Trace('추가적인 이상 없음')
		except :
			__LOG__.Exception()
