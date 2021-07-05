#!/bin/env python3
# -*- coding:utf8 -*-
import socket
import os
import sys
import asyncore
import signal
import time
#import Log; Log.Init()
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

class FileClient(asyncore.dispatcher):
	
	def __init__(self, host, port, prefix, in_file):
		asyncore.dispatcher.__init__(self)
		self.host = host
		self.port = port
		self.in_file = in_file
		self.buffer_size = 8192 
		#self.buffer_size = 2097152 
		self.section = prefix
		self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
		self.connect((self.host, self.port))
		__LOG__.Trace('init')

	def handle_connect(self):
		__LOG__.Trace('handle_connect')

	def send_header(self, in_file):
		filesize = os.path.getsize(in_file)
		header = '%s|%s|%s'%(self.section, in_file, filesize)
		length = len(header)
		msg = '%04d%s'%(length, header)
		sent = self.send(bytes(msg,'utf-8'))
		__LOG__.Trace('send header: size[%s], msg[%s]'%(sent, msg) )
	
	def handle_close(self):
		try:
			self.close()
		except Exception as ex:
			__LOG__.Trace(ex)
		__LOG__.Trace('handle_close')
	
	def handle_read(self):
		__LOG__.Trace('handle_read: %s'%self.recv(8192))

	def writable(self):
		__LOG__.Trace('writable: %s'%len(self.in_file))
		return (len(self.in_file) > 0 )

	def handle_write(self):
		in_file = self.in_file

		stime = time.time()
		self.send_header(in_file)

		file_size = os.path.getsize(in_file)
		chk_size = 0 
		send_size = 0
		with open(in_file, 'rb') as fo:
			while True:
				try:
					byte = fo.read(self.buffer_size)
					if len(byte) > 0:
						sent = self.send(byte)
						send_size = send_size + sent
						if chk_size <= send_size:
							chk_size = chk_size + (file_size/10)
							__LOG__.Trace('send file: size[%s], total[%s/%s]'%(sent, send_size, file_size))
						time.sleep(0.005)
					else:
						break
				except Exception as ex:
					__LOG__.Trace(ex)
					break
		etime = time.time() - stime
		__LOG__.Trace('time(s): %s, mb/s: %s'%(etime, (file_size/1024/1024)/etime))
		self.in_file = ''
		self.handle_close()

class FileSender:
	
	def __init__(self, host, port):
		self.host = host
		self.port = port

	def processing(self, prefix, in_file):

		try:
			client = FileClient(self.host, self.port, prefix, in_file)
			asyncore.loop(count=1)
		except:
			__LOG__.Exception()


	def run(self):

		__LOG__.Trace( 'START process: ( pid:%d ) >>>>>>>>>>>>>>>>>>>>>>>>>>>>>' % (os.getpid()) )

		std_in = None
		is_std_error = False

		while True:

			try:
				std_in = sys.stdin.readline().strip()
				__LOG__.Trace('STD  IN : %s' % std_in)

				if std_in.strip() == '' :
					is_std_error = True
					raise Exception("input error, std_in == '' ")

				try :
					prefix, in_file = std_in.strip().split( '://', 1 )
				except :
					is_std_error = True
					raise Exception("Input format error")

				#if prefix != 'file' :
				#	is_std_error = True
				#	raise Exception('Prefix not match %s' % prefix)

				if not os.path.exists(in_file):
					is_std_error = True
					raise Exception("File Not Exists : %s" % in_file)

				#if os.path.getsize(in_file) == 0 :
				#	is_std_error = True
				#	raise Exception("0 Byte File : %s" % in_file)

				stime = time.time()
				self.processing(prefix, in_file)
				etime = time.time()
				__LOG__.Trace( 'Duration %s sec' % ( etime - stime ) )
				is_std_err = True

			except:
				__LOG__.Exception()

			finally :
				if std_in != None and is_std_err :
					sys.stderr.write( '%s\n' % std_in )
					sys.stderr.flush()
					__LOG__.Trace('STD ERR : %s' % std_in)

		__LOG__.Trace('END')
		
PROC_NAME = os.path.basename(sys.argv[0])

def main():

	if len(sys.argv) < 2:
		print ( "Usage   : %s IP PORT" % PROC_NAME )
		print ( "Example : %s 0.0.0.0 19288" % PROC_NAME )
		sys.exit()

	ip = sys.argv[1]
	port = int(sys.argv[2])

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
		FileSender(ip, port).run()
	except Exception as ex:
		__LOG__.Error(ex)
	
	__LOG__.Trace('END')

if __name__ == '__main__':
	main()	
