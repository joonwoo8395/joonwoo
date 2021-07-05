#!/usr/bin/python

import sys
import socket
import os
import select
import signal

import Mobigen.Common.Log as Log; Log.Init()

SHUTDOWN = False
def handler(sigNum, frame):
	__LOG__.Trace('Catch Signal Number = [%s]' % sigNum)
	global SHUTDOWN
	SHUTDOWN = True

signal.signal(signal.SIGTERM, handler)
signal.signal(signal.SIGINT, handler)
try:
	signal.signal(signal.SIGHUP, signal.SIG_IGN)
except:	pass
try:
	signal.signal(signal.SIGPIPE, signal.SIG_IGN)
except:	pass

class EFClient() :
	
	def __init__(self, host, port, **opt) :

		self.host = host
		self.port = port

		self.sock = None

		self.connect()

	def connect( self ) :

		if not self.sock :
			self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			self.sock.connect((self.host, self.port))

	def __del__(self) :

		self.close()

	def close(self) :
	
		try : 
			self.sock.send("BYE\n")
		except : pass

		try: self.sock.close()
		except: pass

		self.sock = None

	def stdin(self, line) :

		while True :
			try :
				self.sock.send(line)

				sys.stderr.write(line)
				sys.stderr.flush()

				__LOG__.Trace( line.strip() )
				break;
			except : 
				self.close()
				self.connect()


if __name__ == "__main__" :
	if len(sys.argv) < 3 :
		print "usage : %s ip port" % (sys.argv[0])
		sys.exit()

	ip = sys.argv[1]
	port = int(sys.argv[2])

	try :
		log_suffix = sys.argv[3]
	except :
		log_suffix = os.getpid()

	if '-d' not in sys.argv :
		log_path = os.path.expanduser('~/log')
		try : os.makedirs(log_path)
		except : pass
		log_name = '%s_%s.log' % ( os.path.basename(sys.argv[0]), log_suffix )
		log_file = os.path.join( log_path, log_name )
		Log.Init(Log.CRotatingLog(log_file, 10240000, 9))

	__LOG__.Trace('START')

	cli = None

	while not SHUTDOWN :
		if not cli :
			cli = EFClient( ip, port )
			cli.connect()

		try :
			std_in = select.select( [sys.stdin], [], [], 5 )[0]
			if std_in :
				cli.stdin( std_in[0].readline() )
			else :
				cli.stdin( 'Heartbeat 5 sec\n' )
		except : 
			if not SHUTDOWN : __LOG__.Exception()
			cli.close()
			cli = None

	__LOG__.Trace('END')
