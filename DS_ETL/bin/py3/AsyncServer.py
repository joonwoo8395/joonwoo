#!/bin/env python3
# -*- coding:utf8 -*-
import asyncore
import socket
import signal
import os
import sys
import Mobigen.Common.Log_PY3 as Log; Log.Init()

def handler(sigNum, frame):
	sys.stderr.write('Catch Signal Number : %s \n' % sigNum)
	sys.stderr.flush()
	os.kill(os.getpid(), signal.SIGKILL)

# sigNum 15 : Terminate
signal.signal(signal.SIGTERM, handler)
# sigNum  2 : Keyboard Interrupt
signal.signal(signal.SIGINT, handler)
# sigNum  1 : Hangup detected
try:
	signal.signal(signal.SIGHUP, signal.SIG_IGN)
except: pass
# sigNum 13 : Broken Pipe
try:
	signal.signal(signal.SIGPIPE, signal.SIG_IGN)
except: pass

def call_shutdown():
	__LOG__.Trace('!!! SHUTDOWN !!!')
	os._exit(1)

# header_size | file_name, file_size | content_data
# length : 4  | length : 0~9999	 | length : 1~4 GB

class FileHandler(asyncore.dispatcher_with_send):

	def __init__(self, sock):

		asyncore.dispatcher_with_send.__init__(self, sock=sock)
		self.recv_size = 8192
		self.file_name = ''
		self.file_size = 0
		self.write_size = 0
		self.chk_size = 0
		self.new_file_flag = True
		self.file_obj = None
		self.save_path = ''
		self.save_file = ''
		self.temp_file = ''

	def handle_read(self):

		try:
			data = self.recv(self.recv_size)
			if data:
				if self.new_file_flag:
					length = int(data[:4])
					header = str(data[4:length+4],'utf-8')
					self.section = header.split('|')[0].strip()
					self.file_name = header.split('|')[1].strip()
					self.file_size = int(header.split('|')[2].strip())
					self.save_path = os.path.dirname(self.file_name)
					self.save_file = os.path.join(self.file_name)
					self.temp_file = self.save_file + '.tmp' 

					if not os.path.exists(self.save_path):
						os.makedirs(self.save_path)
						__LOG__.Trace("makedirs: %s"%self.save_path)

					self.file_obj = open(self.temp_file, 'wb')
					__LOG__.Trace('file open: %s'%self.temp_file)

					self.file_obj.write(data[length+4:])
					self.write_size = len(data[length+4:])
					self.new_file_flag = False
					__LOG__.Trace('recv header: size[%s], msg[%04d%s]'%(len(data), length, header) ) 
				else:
					self.file_obj.write(data)
					self.write_size = self.write_size + len(data)
					if self.chk_size <= self.write_size:
						__LOG__.Trace('recv file: size[%s], total[%s/%s]'%(len(data), self.write_size, self.file_size) )
						self.chk_size = self.chk_size + (self.file_size/10)
		except:
			__LOG__.Exception()
			self.handle_close()

	def handle_close(self):

		try:
			__LOG__.Trace('connection close: %s'%self)

			if self.file_obj: 
				self.file_obj.close()
				__LOG__.Trace('file close: %s'%self.temp_file)

			if os.path.exists(self.temp_file):
				os.rename(self.temp_file, self.save_file)
				__LOG__.Trace("rename : %s"%self.save_file)
		
			__LOG__.Trace('STDOUT = %s://%s' % (self.section, self.save_file ))
			self.close()
		except:
			__LOG__.Exception()

class FileServer(asyncore.dispatcher):

	def __init__(self, host, port):
		asyncore.dispatcher.__init__(self)
		self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
		self.set_reuse_addr()
		self.bind((host, port))
		self.listen(5)
		__LOG__.Trace('binding to {}'.format(self.socket.getsockname()) )

	def handle_accepted(self, sock, addr):
		__LOG__.Trace('Incoming connection from %s' % repr(addr))
		handler = FileHandler(sock)

PROC_NAME = os.path.basename(sys.argv[0])

def main():

	if len(sys.argv) < 3:
		print ( "Usage   : %s IP PORT" % PROC_NAME )
		print ( "Example : %s 0.0.0.0 19288" % PROC_NAME )
		sys.exit()

	ip = sys.argv[1]
	port = int(sys.argv[2])
	#path = sys.argv[3]

	try :
		log_suffix = sys.argv[3]
	except :
		log_suffix = os.getpid()

	if '-d' not in sys.argv :
		log_path = os.path.expanduser('~/BPETL/log/Async')
		try : os.makedirs(log_path)
		except : pass
		log_name = '%s_%s.log' % ( os.path.basename(sys.argv[0]), log_suffix )
		log_file = os.path.join( log_path, log_name )
		Log.Init(Log.CRotatingLog(log_file, 10240000, 9))

	__LOG__.Trace('START')

	try:
		server = FileServer(ip, port)	
		asyncore.loop()
	except:
		__LOG__.Exception()

	__LOG__.Trace('END')

if __name__ == '__main__':
	main()
