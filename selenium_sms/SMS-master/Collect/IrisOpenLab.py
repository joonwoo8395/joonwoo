# -*- coding: utf-8 -*-
#!/usr/bin python

import paramiko
import datetime
import sys
import signal
#import ConfigParser
import os
import Mobigen.Common.Log as Log
import API.M6 as M6
import time
import re
import requests
from selenium import webdriver
#from bs4 import BeautifulSoup
import requests
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.alert import Alert
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
import time

#import subprocess

IDX_NODEID = 0
IDX_SYS_STATUS = 1
IDX_ADM_STATUS = 2
IDX_UPDATE_TIME = 3
IDX_IP = 4
IDX_CPU = 5
IDX_LOADAVG = 6
IDX_MEMP = 7
IDX_MEMF = 8
IDX_DISK = 9

class IrisOpenLab() :
	def __init__(self, _Parser) :
		self.PARSER = _Parser
		self.GetConfig()

	def GetConfig(self) :
		try :
			self.OPENLAB_DOMAIN	= self.PARSER.get('WEB', 'IRIS_OPENLAB_DOMAIN')
			self.REQUEST_URL	= self.PARSER.get('WEB', 'IRIS_OPENLAB_URL')
			self.SUCCESS_URL	= self.PARSER.get('WEB', 'SUCCESS_URL')
		except : 
			__LOG__.Exception()

	def selenium(self) :
		result	= 'OK'
		retdict = dict()

		chrome_options = webdriver.ChromeOptions()
		chrome_options.add_argument('--headless')
		chrome_options.add_argument('--no-sandbox')
		chrome_options.add_argument('--disable-dev-shm-usage')
		chrome_options.add_argument('--user-data-dir=/home/tacs/user/selenium_sms/SMS-master/')
		chrome_options.add_experimental_option("prefs",
		{
			"download.default_directory": r"/home/tacs/user/selenium_sms/SMS-master/",
			"download.prompt_for_download": False,
			"download.directory_upgrade": True,
			"safebrowsing.enabled": True
		})

		driver = webdriver.Chrome(executable_path='/home/tacs/user/KimJW/selenium_sms/SMS-master/chromedriver', chrome_options=chrome_options)

		driver.get('https://www.bigdata-transportation.kr/login')
		time.sleep(3)
		#driver.implicitly_wait(3)

		driver.find_element_by_name('loginEmail').send_keys('id')
		driver.find_element_by_name('password').send_keys('passwd')
		driver.find_element_by_id('btnLogin').click()
		driver.implicitly_wait(10)
		#wait = WebDriverWait( driver, 5 )
		
#		driver.close()

		try :
			__LOG__.Trace("current_url --> %s " % (driver.current_url))
			if driver.current_url == self.SUCCESS_URL :
				result = 'OK'
				__LOG__.Trace('login success')
			else :
				result = 'NOK'
				__LOG__.Trace('Login Fail')
		except TimeoutException as e :
			__LOG__.Exception()

		retdict = {'STATUS' : 'OK', 'VALUE' : result}

		return retdict


	def irisOpenLabCheck(self) :
		requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)
		result	= ''
		retdic = dict()
		try :
			res = requests.get(self.REQUEST_URL, verify = False)
			result = res.status_code
			retdic = {'STATUS' : 'OK' , 'VALUE' : result}
		except :
			__LOG__.Exception()

		return retdic


	def run(self) :
		__LOG__.Trace("[Collect]IRIS OPENLAB START_______________________")		
		ResultDict = {}
		try :
			openLabResult	= self.irisOpenLabCheck()
			seleresult		= self.selenium()
			ResultDict[self.OPENLAB_DOMAIN] = {'HOSTNAME' : {'VALUE' : ['OPENLAB'] }, 'STATUS' : 'OK' ,'IRIS_OPENLAB' : openLabResult, 'WEB-LOGIN' : seleresult}
		except :
			__LOG__.Exception()
	
		__LOG__.Trace("[Collect]IRIS OPENLAB END_______________________")

		return ResultDict

def Main() :
	obj = IrisOpenLab(sys.argv[1], sys.argv[2], sys.argv[3])
	dict = obj.run()

if __name__ == '__main__' :
	Main()	
