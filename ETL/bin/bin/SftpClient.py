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
#					self.transport = paramiko.Transport((oneHost, int(self.port)))
#					self.transport.connect(username = self.user, password = self.passwd)

#					self.transport.connect(username = '11', password = self.passwd)

					self.ssh = paramiko.SSHClient()
					self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
					self.ssh.connect(oneHost, port = int(self.port), username = self.user, password = self.passwd, timeout = 10)

					__LOG__.Trace('SSH Connected HOST({})'.format(oneHost))
					break
				except :
					__LOG__.Trace('SFTP Connection faild. HOST({})'.format(oneHost))

#			self.sftp = paramiko.SFTPClient.from_transport(self.transport)
			self.sftp	= self.ssh.open_sftp()
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

#			if self.transport :
#				self.transport.close()
			if self.ssh :
				self.ssh.close()
				__LOG__.Trace('ssh closed')
		except :
			__LOG__.Trace('SFTP Connection close failed.')
			raise

