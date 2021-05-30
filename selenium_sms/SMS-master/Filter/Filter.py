# -*- coding: utf-8 -*-
#!/usr/bin python

import datetime
import sys
import os
import Mobigen.Common.Log as Log
import time

IDX_MAX = 0
IDX_MIN = 1

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

class Filter() :
	#임계치 Dict, 필터 Dict
	def __init__(self, _Parser, _ValueDict) :
		self.PARSER = _Parser
		self.ValueDict = _ValueDict
		self.GetConfig()

	def GetConfig(self) :

		"""
		임계치 값을 config에서 불러와서 dict에 넣는다.
		"""
		try :
			self.Thresholddict = {}
			
			#default Threshold 
			self.Thresholddict['DEFAULT'] = {'LOAD_AVG':self.PARSER.get('RESOURCES', 'LOAD_AVG') ,
					 'MEMORY':self.PARSER.get('RESOURCES', 'MEMORY') ,
					 'DISK':self.PARSER.get('RESOURCES', 'DISK') ,
					 'SWAP':self.PARSER.get('RESOURCES', 'SWAP'), 
					 'IRIS_LOAD_AVG' : self.PARSER.get('IRIS','IRIS_LOAD_AVG'),
					 'IRIS_MEM_P' : self.PARSER.get('IRIS','IRIS_MEM_P'),
					 'IRIS_MEM_F' : self.PARSER.get('IRIS','IRIS_MEM_F'),
					 'IRIS_DISK' : self.PARSER.get('IRIS','IRIS_DISK'),
					 'IRIS_OPENLAB' : self.PARSER.get('IRIS', 'OPENLAB_STATUS'),
					 'WEB-LOGIN' : self.PARSER.get('IRIS', 'WEB-LOGIN')}
					
			ServerList = self.PARSER.get('RESOURCES','SERVER_LIST').split(',')
			__LOG__.Trace(ServerList)
			THRESHOLD_TYPE_LIST = ['LOAD_AVG','MEMORY','DISK','SWAP','LOG_SECOND','FILE_SECOND']

			# DEFAULT 값이 아닌 서버 별 Type 임계치 값을 다르게 주고 싶을 때 사용.
			# 현재는 사용 안함
			for ServerIP in ServerList :
				value_dict = {}

				for Type in THRESHOLD_TYPE_LIST :
					try :
						tmp_str = self.PARSER.get(ServerIP,Type)
						value_dict[Type] = tmp_str
					except :
						pass

				if len(value_dict) > 0 : self.Thresholddict[ServerIP] = value_dict
				
		except :
			__LOG__.Exception()

	def GetTresholdValue(self, _Server, _Type) :
		"""사용자가 설정한 임계치를 사용하고 싶을 때 사용함"""
		try :
			if self.Thresholddict.has_key(_Server) :
				if self.Thresholddict[_Server].has_key(_Type) : return self.Thresholddict[_Server][_Type]
			
			# Server 별로 사용하고 있지 않기 때문에 DEFAULT 키 값에 있는 값을 전달
			return self.Thresholddict['DEFAULT'][_Type]
			
			__LOG__.Trace(self.Thresholddict)
		except :
			__LOG__.Exception()

		
	def run(self) :
		try :
			for Server in self.ValueDict.keys() :
				#__LOG__.Trace(Server)
				#__LOG__.Trace(self.ValueDict[Server])
				
				#상태가 NOK-접속불가면 확인할 필요 없음
				if self.ValueDict[Server]['STATUS'] == 'NOK' :
					continue

				for Type in self.ValueDict[Server].keys() :
					__LOG__.Trace('Type ----> %s' % Type)
					if Type == 'IRIS' : #{'STATUS':'OK' , 'VALUE':[SYS_STATUS, UPDATE_TIME]}
						Value = self.ValueDict[Server][Type]['VALUE']

						#만약 아이리스 SYS_STATUS가 BUSY나 WARN이 되어있으면 오류
						if Value[IDX_IRIS_SYS_STATUS] != 'VALID' : 
							__LOG__.Trace(Value[IDX_IRIS_SYS_STATUS])
							self.ValueDict[Server][Type]['STATUS'] = 'NOK'
						#현재시간보다 5분이 느리다면 오류

						if datetime.datetime.strptime(Value[IDX_IRIS_UPDATE_TIME], '%Y%m%d%H%M%S') < datetime.datetime.now() - datetime.timedelta(minutes=5) : 
							self.ValueDict[Server][Type]['STATUS'] = 'NOK'
					
					elif Type == 'LOAD_AVG' : #{'STATUS':'OK' , 'VALUE':[1분,5분,15분]}
						for Per in self.ValueDict[Server][Type]['VALUE'] :
							ThreshValue = self.GetTresholdValue(Server, Type)
							if float(Per) > float(ThreshValue) : 
							# 1분 ,5분, 15분 평균 부하중 하나라도 임계치보다 높으면 NOK
								self.ValueDict[Server][Type]['STATUS'] = 'NOK'
								break
					elif Type == 'MEMORY' or Type == 'SWAP': #{'STATUS':'OK' , 'VALUE':[Total, Used, Available, Use%]}
						ThreshValue = self.GetTresholdValue(Server, Type)
						if int(self.ValueDict[Server][Type]['VALUE'][IDX_MEMORY_USE_PER]) > int(ThreshValue) :
						# 메모리, 스왑의 사용률이 둘중 하나라도 임계치보다 높으면 NOK
							self.ValueDict[Server][Type]['STATUS'] = 'NOK'
					
					elif Type == 'DISK' : #[{'STATUS':'OK', VALUE[Mount, 1M-blocks, Used, Available, Use%]}]
						ThreshValue = self.GetTresholdValue(Server, Type)
						for Disk in self.ValueDict[Server][Type] :
							for oneDiskPath in Disk['VALUE'] :
								if '/snap/core/' in oneDiskPath :
									Disk['VALUE'][IDX_DISK_USE_PER] = u'0'
							# 디스크 사용량이 하나라도 임계치보다 높으면 NOK
#							__LOG__.Trace( Disk['VALUE'][IDX_DISK_USE_PER])
							if int(Disk['VALUE'][IDX_DISK_USE_PER]) > int(ThreshValue) :
								Disk['STATUS'] = 'NOK'
					elif Type == 'IRIS_OPENLAB' : # {'STATUS' : 'OK', 'VALUES' : [Status_code]}
						ThreshValue = self.GetTresholdValue(Server, Type)
						__LOG__.Trace(self.ValueDict[Server][Type])
						if self.ValueDict[Server][Type]['VALUE'] != 200 :
						# IRIS_OPENLAP Rest API 호출 상태코드가 200 이 아니라면
							self.ValueDict[Server][Type]['STATUS'] = 'NOK'

					elif Type == 'WEB-LOGIN' : # {'STATUS' : 'OK', 'VALUES' : [Status_code]}
						ThreshValue = self.GetTresholdValue(Server, Type)
						__LOG__.Trace(self.ValueDict[Server][Type])
						if self.ValueDict[Server][Type]['VALUE'] != 'OK' :
							self.ValueDict[Server][Type]['STATUS'] = 'NOK'
					else :
						__LOG__.Trace(self.ValueDict[Server][Type])
						pass

		except :
			__LOG__.Exception()

		return self.ValueDict

def Main() :
	obj = SMSFilter()
	value_dict = obj.run()
	for ServerID in value_dict.keys() :
		for Key in value_dict[ServerID].keys() :
			print '%s %s = %s' % (ServerID, Key, value_dict[ServerID][Key])

if __name__ == '__main__' :
	Main()	
