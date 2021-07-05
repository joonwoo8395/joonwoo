#!/usr/bin/python3

import sys
import select
import socket
import os
import signal

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


class EFServer() :
	
	def __init__(self, host, port, **opt) :
		self.host = host
		self.port = port

		self.sock_list = []
		self.client_hash = {}

		self.listen_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.listen_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		self.listen_sock.bind((self.host, self.port))
		self.listen_sock.listen(100)

		self.sock_list.append(self.listen_sock)


	def __del__(self) :
		for sock in self.sock_list :
			self.close(sock)


	def close(self, sock) :

		try :
			addr = self.client_hash[sock][1]
			__LOG__.Trace( addr )
		except : pass

		try :
			self.sock_list.remove(sock)
		except : pass

		try : sock.close()
		except : pass

		try : self.client_hash[sock][0].close()
		except : pass

		try : del(self.client_hash[sock])
		except : pass


	def stdout(self, itr=None) :

		while (True) :
			try:
				input, output, exception = select.select(self.sock_list, [], [], 1)

				if (len(input) == 0) : continue
	
				for sock in input :
					if (sock == self.listen_sock) :
						client, addr = self.listen_sock.accept()
						self.sock_list.append(client)
						self.client_hash[client] = [client.makefile(), addr]
						__LOG__.Watch( addr )
					else :
						try :
							line, addr = self.client_hash[sock][0].readline(), self.client_hash[sock][1]

							__LOG__.Watch([line, addr])

							if (line.strip() == "") : raise Exception
							
							#if (line[:3].upper() == "BYE") : raise Exception
							
							#elif (line[:3].upper() == "KIL") : raise Exception
		
							else :
								__LOG__.Trace( '%s : %s' % ( addr, line.strip() ) )
								if not line.startswith( 'Heartbeat' ) :
									yield line
						except :
							self.close(sock)

	
			except KeyboardInterrupt : break
			except:
				__LOG__.Exception()

		for sock in self.sock_list :
			self.close(sock)



if __name__ == "__main__" :
	if len(sys.argv) < 3 :
		print ("usage : %s ip port" % (sys.argv[0]))
		sys.exit()
	
	#ip = sys.argv[1]
	ip = socket.gethostname()
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

	sockQ = EFServer( ip, port )
	for line in sockQ.stdout():
		#stdLine	= '{}\n'.format(line)
		stdLine	= '{}'.format(line)
		sys.stdout.write(stdLine)
		sys.stdout.flush()
		__LOG__.Trace(stdLine)

	__LOG__.Trace('END')
