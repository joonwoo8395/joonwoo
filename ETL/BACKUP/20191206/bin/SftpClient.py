#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import paramiko

import Mobigen.Common.Log as Log; Log.Init()

class SftpClient :
	def __init__(self, host, port, user, passwd) :
		self.host 		= host
		self.port 		= port
		self.user 		= user
		self.passwd 	= passwd

		self._connect()

	def _connect(self) :
		try :
			sftpHosts = self.host.split(';')
			__LOG__.Trace('SFTP host: {}'.format(sftpHosts))

			for oneHost in sftpHosts :
				try :
					self.transport = paramiko.Transport((oneHost, int(self.port)))
					self.transport.connect(username = self.user, password = self.passwd)
					__LOG__.Trace('SFTP Connected HOST({})'.format(oneHost))
				except :
					__LOG__.Trace('SFTP Connection faild. HOST({})'.format(oneHost))
		
			self.sftp = paramiko.SFTPClient.from_transport(self.transport)
		except :
			__LOG__.Trace('SFTP Connection error. HOST({})/PORT({})'.format(self.host, self.port))
			raise

	def download(self, remoteFilepath, remoteFilename, localFilepath) :
		try :
			self.sftp.get(os.path.join(remoteFilepath, remoteFilename), os.path.join(localFilepath, remoteFilename))
			__LOG__.Trace('{} -> {} file download succeed'.format(os.path.join(remoteFilepath, remoteFilename), os.path.join(localFilepath, remoteFilename)))
		except :
			__LOG__.Trace('SFTP file download failed.')
			raise

	def mkdirs(self, remoteDir) :
		if not remoteDir :
			return

		if remoteDir == '/' :
			self.sftp.chdir(remoteDir)
			return

		try :
			self.sftp.chdir(remoteDir)
		except IOError :
			dirname, basename = os.path.split(remoteDir.rstrip('/'))
			self.mkdirs(dirname)
			self.sftp.mkdir(basename)
			self.sftp.chdir(basename)
			return

	def upload(self, sourceFilepath, destinationFilepath) :
		try :
			self.sftp.put(sourceFilepath, destinationFilepath)
			__LOG__.Trace('{} -> {} file upload succeed'.format(sourceFilepath, destinationFilepath))
		except :
			__LOG__.Trace('SFTP file upload failed.')
			raise

	def close(self) :
		try :
			if self.sftp :
				self.sftp.close()
				__LOG__.Trace('sftp closed')

			if self.transport :
				self.transport.close()
				__LOG__.Trace('transport closed')
		except :
			__LOG__.Trace('SFTP Connection close failed.')
			raise

