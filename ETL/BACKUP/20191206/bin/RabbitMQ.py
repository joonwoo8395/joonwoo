import Mobigen.Common.Log as Log; Log.Init()
import pika
import ssl
import bson
import time

class DirectQueueClient:
		def __init__( self ):
				self.connection = None
				self.channel	= None

		def disConnect( self ):
				if self.channel != None:
					self.channel.close()

				if self.connection != None:
					self.connection.close()
		
		def connect( self, mqUser, mqPass, mqHost, mqPort, mqVhost ):
			userInfo = pika.PlainCredentials( mqUser, mqPass )
			hostInfo = pika.ConnectionParameters( host = mqHost, port = mqPort, virtual_host = mqVhost, credentials = userInfo )
			self.connection = pika.BlockingConnection( hostInfo )
			#self.channel = self.connection.channel()
			

		def connectSSL( self, mqUser, mqPass, mqHost, mqPort, mqVhost, mqCaCerts, mqCertFile, mqKeyFile ):
			try :
				userInfo = pika.PlainCredentials( mqUser, mqPass )
				s_options	 = ({"ca_certs"		: mqCaCerts,
								"certfile"		: mqCertFile,
								"keyfile"		: mqKeyFile,
								"cert_reqs"		: ssl.CERT_REQUIRED,
								"server_side"	: False})

				hostInfo	= pika.ConnectionParameters( host=mqHost, port=mqPort, virtual_host=mqVhost, credentials=userInfo, ssl=True, ssl_options = s_options)

				self.connection = pika.BlockingConnection(hostInfo)

				#self.channel 	= self.connection.channel()
			except Exception as e :
				__LOG__.Trace('ERROR : %s ' %e )
				time.sleep(60)
				self.connectSSL(mqUser, mqPass, mqHost, mqPort, mqVhost, mqCaCerts, mqCertFile, mqKeyFile)

		def connectChannel (self) :
			try :
				self.channel = self.connection.channel()
				
				#__LOG__.Trace( 'Channel success' )
		
			except Exception as e :
				__LOG__.Trace('Channel Connection Error : %s' %e )
				time.sleep(60)
				self.connectChannel()

		def is_open( self ):
			if self.channel.is_open:
				
				if self.connection.is_open:
						return True
				
				return False

		def disconnect( self ):
#			try:
#				if self.channel != None:
#					__LOG__.Trace("channel dont  close")
#					self.channel.close()
		
#			except:
#				pass
#			__LOG__.Trace( 'Channel close' )
			
			try:
				if self.connection != None:
					#__LOG__.Trace("connection dont close")
					self.connection.close()
				
			except:
				pass
			__LOG__.Trace( 'Connect close' )
			self.connection = None

		def disconnectChannel( self ) :
			try :
				if self.channel != None :
					#__LOG__.Trace('channel dont close')
					self.channel.close()

			except :
				pass
			__LOG__.Trace( 'Channel Close')
			self.channel 	= None

		def exchange_declare(self, exchangeName) :
			try:	
				self.channel.exchange_declare(exchange = exchangeName, exchange_type= 'direct', durable = True)
			except Exception as e :
				__LOG__.Trace('ERROR : %s ' %e )

			#except :
			#	__LOG__.Exception()

		def queue_declare( self, queueName ):
			try:	
				#autu-delete = True
				self.channel.queue_declare( queue = queueName, durable = True)
			except:
				__LOG__.Exception()

		def queue_delete( self, queueName ):
			self.channel.queue_delete( queue= queueName )

		def put(self, queueName, message, use_bson=True ):
			if not queueName == None and not queueName == '' :

				self.queue_declare(queueName)

				try :
					__LOG__.Trace(queueName)
					result = self.channel.basic_publish(
						exchange 		= ''
						, routing_key 	= queueName
						, body = str(message)
						, properties = pika.BasicProperties(
									delivery_mode = 2
						)
						, mandatory=True
					)
					__LOG__.Trace(result)
				except : __LOG__.Exception()

		def get( self, queueName, use_bson=True ):
			method_frame, header_frame, message = self.channel.basic_get( queue=queue_name )
			# message = message.decode('UTF-8')
			__LOG__.Trace("RECEIVE MESSGAGE: %s, %s" % (type(message), message))
			#if method_frame.name == 'Basic.GetOk':
			if method_frame:
				try:
					self.channel.basic_ack( method_frame.delivery_tag )
					if use_bson:
						# message = bson.decode_all(message)
						message = bson.BSON(message).decode()
						# message = message.decode()
				except:
						__LOG__.Exception("[ERROR] In Channel.basic_ack")

				else:
						method_frame = None

				return ( method_frame, header_frame, message )

#def main():

#		import hashlib
#		import json
#		import random

#if __name__ == '__main__':
#		main()
