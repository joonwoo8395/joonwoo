#!/bin/env python3
# -*- coding: utf-8 -*-

#- needed import ----------------------------------------------------
#import $PYTHON_LIB$
import os
import sys
import time
import datetime
import signal
import subprocess
import glob
import configparser as ConfigParser
import paramiko
import socket
import shutil

#import $MOBIGEN_LIB$
import Mobigen.Common.Log_PY3 as Log; Log.Init()
import Mobigen.Utils.LogClient as c_log

#import $PROJECT_LIB$

#- shutdown  ----------------------------------------------------
SHUTDOWN = False

def shutdown(signalnum, handler):
	global SHUTDOWN
	SHUTDOWN = True
	sys.stderr.write('Catch Signal: %s \n' % signalnum)
	sys.stderr.flush()

signal.signal(signal.SIGTERM, shutdown) # sigNum 15 : Terminate
signal.signal(signal.SIGINT, shutdown)  # sigNum  2 : Keyboard Interrupt
signal.signal(signal.SIGHUP, shutdown)  # sigNum  1 : Hangup detected
signal.signal(signal.SIGPIPE, shutdown) # sigNum 13 : Broken Pipe

'''
	On Windows, signal() can only be called with
	SIGABRT, SIGFPE,SIGILL, SIGINT, SIGSEGV, or SIGTERM.
	A ValueError will be raised in any other case.
'''

#- def global setting ----------------------------------------------------

def stderr(msg) :

	sys.stderr.write(msg + '\n')
	sys.stderr.flush()
	__LOG__.Trace('Std ERR : %s' % msg)

def stdout(msg) :

	sys.stdout.write(msg + '\n')
	sys.stdout.flush()
	__LOG__.Trace('Std OUT : %s' % msg)

def makedirs(path) :

	try :
		os.makedirs(path)
		__LOG__.Trace( path )
	except : pass

#- Class ----------------------------------------------------

class SFTPUpload:

	def __init__(self, conf, section) :
		#open
		__LOG__.Trace("__init__")
		section = 'SFTP_UPLOAD'
		
		server_host_name = socket.gethostname()

		try : self.HOST_IP	= conf.get(section, "HOST_IP")
		except :
			self.HOST_IP = conf.get("GENERAL", "HOST_IP")

		if 'ntbdgw2' == server_host_name :
			try :
				self.HOST_IP = conf.get(section, "GW_HOST_IP")
			except :	
				self.HOST_IP = conf.get("GENERAL", "GW_HOST_IP")

		elif 'tbdetl1' == server_host_name :
			try :
				self.HOST_IP = conf.get(section, "EX_HOST_IP")
			except :
				self.HOST_IP = conf.get("GENERAL", "EX_HOST_IP")

		try : self.PORT	= conf.getint(section, "%s_PORT" % server_host_name )
		except :
			self.PORT	= conf.getint("GENERAL", "%s_PORT" % server_host_name )
	
		try : self.USER	= conf.get(section, "%s_USER" % server_host_name)
		except :
			self.USER = conf.get("GENERAL", "%s_USER" % server_host_name)

		try : self.PASSWD	= conf.get(section, "%s_PASSWD" % server_host_name)
		except :
			self.PASSWD	= conf.get("GENERAL", "%s_PASSWD" % server_host_name)

		try : self.RETRY_CNT	= conf.getint(section, "RETRY_CNT")
		except :
			self.RETRY_CNT	= conf.getint("GENERAL", "RETRY_CNT")

		try : self.RETRY_SLEEP = conf.getint(section, "RETRY_SLEEP")
		except : 
			self.RETRY_SLEEP = conf.getint("GENERAL", "RETRY_SLEEP")

		try : self.UPLOAD_PATH	= conf.get(section, "UPLOAD_PATH")
		except :
			self.UPLOAD_PATH	= conf.get("GENERAL", "UPLOAD_PATH")

		try : self.EXT	= conf.get(section, "EXT")
		except :
			self.EXT = conf.get("GENERAL" , "EXT")

		try : self.CENTER_ID	= conf.get(section, socket.gethostname())
		except : self.CENTER_ID	= conf.get("GENERAL", socket.gethostname())

	def __del__(self):
		#close
		__LOG__.Trace("__del__")

	def closeSFTP(self, sftp, transport) :
		if transport :
			transport.close()
		if sftp :
			sftp.close()

		__LOG__.Trace("Close SFTP Connect")


	def connectSFTP(self) :
		__LOG__.Trace("SFTP Connect")
		sftp		= None
		transport	= None

		attempts	= 0

		while attempts < self.RETRY_CNT :
			try :
				transport	= paramiko.Transport( (self.HOST_IP, self.PORT) )
				transport.connect(username=self.USER,
								password=self.PASSWD)

				sftp = paramiko.SFTPClient.from_transport(transport)
				__LOG__.Trace("SFTP Connect Success!")
				break
			except :
				attempts += 1
				__LOG__.Exception()
				time.sleep(self.RETRY_SLEEP)
		
		if attempts == self.RETRY_CNT :
			os._exit(1)

		return sftp, transport
	
	def sftpMakeDir(self, sftp, remotePath) :
		remoteLastPath	= os.path.basename(remotePath)
		newLastPath		= ''

		__LOG__.Trace('last Path : %s' %remoteLastPath)
		__LOG__.Trace('full Path : %s' %remotePath)
#		if '_' in remoteLastPath and 'TR' in remoteLastPath :
#			newLastPath	= remoteLastPath.rsplit('_', 1)[0]
#			remotePath 	= os.path.join(os.path.dirname(remotePath), newLastPath)

		__LOG__.Trace('full Path2 : %s' %remotePath)
		try :
			sftp.stat(remotePath)
			pass
		except :
			try :
				sftp.mkdir(remotePath)
			except :
				self.sftpMakeDir(sftp, os.path.abspath(os.path.join( remotePath, os.pardir ) ) )
				sftp.mkdir(remotePath)

	def fileUpload(self, sftp, transport, in_file, in_path, nowTime) :
		uploadFlag = False
		if sftp == None or transport == None :
			sftp, transport = self.connectSFTP()

#		filename 		= os.path.basename(in_file)
		tempName, ext 	= os.path.splitext(os.path.basename(in_file))
#		fileDir			= os.path.dirname(in_file)

		try : 
			uploadPath 		= os.path.join(in_path, tempName)

			tempUploadPath	= uploadPath + ".tmp"
			fileUploadPath	= uploadPath + ext

			self.sftpMakeDir(sftp, os.path.dirname(tempUploadPath))
			__LOG__.Trace("UPLOAD File Path : {} -> {}".format(in_file, tempUploadPath) )

			sftp.put(in_file, tempUploadPath)

			try :
				sftp.stat(fileUploadPath)
				__LOG__.Trace("Upload File already Exists")
				new_uploadPath	= os.path.join( os.path.dirname(uploadPath), os.path.basename(uploadPath).rsplit(".", 1)[0] + "_{}".format(nowTime) + ext )
				__LOG__.Trace("New File : {}".format(new_uploadPath))
				sftp.rename( tempUploadPath, new_uploadPath )
				__LOG__.Trace("UPLOAD Success!! {}".format(new_uploadPath))
			except IOError :
				sftp.rename( tempUploadPath, fileUploadPath )
				__LOG__.Trace("UPLOAD Success!! {}".format(fileUploadPath))
			uploadFlag = True
		except :
			__LOG__.Trace("UPLOAD Fail!!")
			raise

		return uploadFlag
		
	def processing(self, in_file) :
		__LOG__.Trace( "processing : %s" % in_file )

		fileName, fileExt = os.path.splitext(in_file)
		nowTime	= datetime.datetime.now().strftime("%Y%m%d%H%M%S")

		sftpUploadPath	= self.UPLOAD_PATH.format(self.CENTER_ID)

		sftp, transport	= self.connectSFTP()
		uploadFlag = self.fileUpload(sftp, transport, in_file, sftpUploadPath, nowTime)

		self.closeSFTP(sftp, transport)
		if uploadFlag : 
			if os.path.exists(in_file) and os.path.getsize(in_file) == 0 :  
				os.remove(in_file) # 파일 삭제
				__LOG__.Trace("0 Byte File Remove!! , Remove File : %s" % in_file)

		__LOG__.Trace('Upload Result : {}'.format(uploadFlag) )

	def run(self) :

		while not SHUTDOWN :

			try:

				std_in = sys.stdin.readline().strip()

				if not std_in :
					is_std_err = True
					continue

				__LOG__.Trace('STD  IN : %s' % std_in )

				try :
					prefix, in_file = std_in.split('://', 1)
				except :
					is_std_err = True
					__LOG__.Trace( 'Input format error : %s' % std_in )
					continue

				if prefix != 'file' :
					is_std_err = True
					__LOG__.Trace('Prefix is not match : %s' % prefix)
					continue	
				
				if not os.path.exists( in_file )  :
					is_std_err = True
					__LOG__.Trace('Not found file : %s' % in_file)
					continue

				if os.path.splitext( in_file )[1] != self.EXT :
					is_std_err = True
					__LOG__.Trace('Invalid File EXT : %s' % in_file )
					continue

				stime = time.time()

				self.processing(in_file)

				etime = time.time()

				__LOG__.Trace( 'Duration %s sec' % ( etime - stime ) )

				is_std_err = True

			except:
				if not SHUTDOWN : __LOG__.Exception()

			finally :
				if std_in != None and is_std_err :
					stderr( std_in )


		
#- main function ----------------------------------------------------
def main():

	module = os.path.basename(sys.argv[0])

	if len(sys.argv) < 3:
		sys.stderr.write('Usage 	: %s conf {option:[[log_arg]-d]}\n' % module )
		sys.stderr.write('Usage 	: %s conf {option:[[log_arg]-d]}\n' % module )
		#python3 /home/test/Project_name/bin/py3/BaseModule.py  /home/test/Project_name/conf/BaseModule.conf KC
		#python3 /home/test/Project_name/bin/py3/BaseModule.py  /home/test/Project_name/conf/BaseModule.conf 0 
		#python3 /home/test/Project_name/bin/py3/BaseModule.py  /home/test/Project_name/conf/BaseModule.conf -d
		sys.stderr.flush()
		os._exit(1)

	section	 = sys.argv[2]
	config_file = sys.argv[1]

	conf = ConfigParser.ConfigParser()
	conf.read(config_file)

	if '-d' not in sys.argv :

		etc_argv = sys.argv[3:]
		log_arg = ''

		if len(sys.argv[3:]) > 0 :
			log_arg = '_' + sys.argv[3]

		log_path = conf.get('GENERAL', 'LOG_PATH')

		makedirs( log_path )	

		log_file = os.path.join(log_path, '%s_%s%s.log' % (os.path.splitext(module)[0], section, log_arg ))
		Log.Init(Log.CRotatingLog(log_file, 10240000, 9))		

	else:
		Log.Init()		

	pid = os.getpid()	
	__LOG__.Trace('============= %s START [pid:%s]==================' % ( module, pid ))

	SFTPUpload(conf, section).run()

	__LOG__.Trace('============= %s END [pid:%s]====================' % (module, pid ))


#- if name start ----------------------------------------------
if __name__ == "__main__" :
	main()


