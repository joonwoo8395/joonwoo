#!/usr/bin/env python
#-*- coding:utf-8 -*-
import os
import sys
import Mobigen.Common.Log as Log;
from Crypto.Cipher import AES
import base64

def pad(data, PADDING) :
	SIZE 	= 32
	padStr = ''
	padStr = data + (SIZE - len(data) % SIZE) * PADDING
	return padStr

class AESDecode() :
	def __init__(self) :
		secret	= '140b41b22a29be4061bda66b6747e143'
		self.cipher	= AES.new(secret)
		self.PADDING = '|'

	def encodeAES(self, data) :
		encodeStr = ''
		encodeStr = base64.b64encode(self.cipher.encrypt( pad(data, self.PADDING) ))
		return encodeStr

	def decodeAES(self, data) :
		decodeStr = ''
		decodeStr = self.cipher.decrypt(base64.b64decode(data)).rstrip(self.PADDING)
		return decodeStr

if __name__ == "__main__":
	try :
		AES = AESDecode()
		print AES.encodeAES('tacs12#$')
	except Exception  as e :
		print e
