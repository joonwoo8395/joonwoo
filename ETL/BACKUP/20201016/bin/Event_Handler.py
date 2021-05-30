#!/usr/bin/env python
# -*- coding: utf-8 -*-
import datetime
import os
import sys
import signal
import time
import ConfigParser
import glob
import json
import Mobigen.Common.Log as Log; Log.Init()
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

resultList = list()
resultSrc = ''
class Handler(FileSystemEventHandler) :
	#def on_any_event(event) : # 모든 이벤트 발생시
	#	__LOG__.Trace(event)
	
	def on_modified(self, event) :
		__LOG__.Trace(event)

		strEvent = str(event)
		strFile = strEvent.split("'")[1]
		global resultList
		global resultSrc
		resultSrc = ''

		if os.path.isfile(strFile) :
			tempList = strFile.split("/")[0:-1]
			tempSrc = ''
			for i in tempList :
				if tempList[-1] == i :
					tempSrc = tempSrc + i
				else :
					tempSrc = tempSrc + i + "/"
			resultSrc = tempSrc
			resultList.append(strFile)
