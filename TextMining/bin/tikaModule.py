#!/home/mobigen/anaconda3/bin/python
# -*- coding: utf-8 -*-

import configparser
from tika import parser
import os
import sys
import olefile
import chardet
import zlib
import base64

class TikaModule() :
	def __init__(self, initFile) :
		cfg		= configparser.ConfigParser()
		cfg.read('/home/mobigen/user/KimJW/TextMining/conf/module.conf')

		self.path			= initFile
		self.ext			= os.path.splitext(initFile)[1]
		self.fileName		= os.path.basename(self.path).split('.')[0]
		self.FILE_PATH		= cfg.get('TIKA', 'FILE_PATH')
		self.OLE_EXT_LIST	= cfg.get('TIKA','OLE_EXT_LIST').split(',')

	def fileToText(self) :
		fileInfoDict 	= parser.from_file(self.path)
		writeStr 	= fileInfoDict['content']

		self.fileWrite(writeStr)

	def fileWrite(self, writeStr) :
		createFilePath = os.path.join(self.FILE_PATH, self.fileName)
		fileOut = open('%s.txt' % createFilePath, 'w', encoding='utf-8')
		fileOut.write(writeStr)
		#print (writeStr, file = fileOut)
		fileOut.close()	

	def run(self) :
		print ('Tika Start')
		print ( '확장자명 : %s' % self.ext )

		if self.ext in self.OLE_EXT_LIST :
			print (self.ext)
			ole 		= olefile.OleFileIO(self.path)

			hwpTree = ole.listdir()
			contents = ole.openstream('PrvText').read()
			self.fileWrite(contents.decode('utf-16'))
#			for oneInfo in hwpTree :
#				if 'BodyText' in oneInfo :
#					for oneStr in oneInfo :
#						if oneStr != 'BodyText' :
							#contents = ole.openstream(('BodyText/%s' % oneStr)).read()
							#print (type(ole.openstream('PrvText').read()) )
							#contents = ole.openstream(('BodyText/%s' % oneStr)).decode('utf-16le').readlines()
#							unzipCont = zlib.decompress(contents, -15)
#							print( unzipCont )
						#	print( base64.decodestring(unzipCont) )
								
#							self.fileWrite(base64.decodestring(unzipCont).decode('utf-16'))
			#print (ole.listdir( streams = False, storages = True )[0] )

			#datas		= ole.get_metadata()

			#datas		= ole.openstream('BodyText')
			#print (ole.dump())
#			oleStr	 	= datas.read()
#			print(dir(oleStr))

#			self.fileWrite(oleStr)

		else :
			self.fileToText()

def main() :
	module 	= os.path.basename(sys.argv[0])
#	section	= sys.argv[1]
#	cfgFile	= sys.argv[2]

	inputFile	= sys.argv[1]	

#	cfg		= ConfigParser.ConfigParser()
#	cfg.read(cfgFile)

#	logPath	= cfg.get('GENERAL', 'LOG_PATH')
#	logFile	= os.path.join( logPath, '%s_%s.log' % (module, section) )
	
#	if '-d' in sys.argv :
#		Log.Init()

#	else :
#		Log.Init( Log.CRotatingLog(logFile, 100000, 9) )

	tk		= TikaModule(inputFile)
	tk.run()

if __name__ == '__main__' :
	try:
		main()
	except ValueError :
		print(ValueError)

