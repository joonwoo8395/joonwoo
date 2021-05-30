#!/home/mobigen/anaconda3/bin/python
# -*- coding: utf-8 -*-

import configparser
from konlpy.tag import Kkma
from konlpy.utils import pprint
import os
import sys
import json

class KonLpyModule() :

	def __init__(self, inputFile) :
		cfg     = configparser.ConfigParser()
		cfg.read('/home/mobigen/user/KimJW/TextMining/conf/module.conf')

		self.path       	= inputFile 
		self.FILE_PATH		= cfg.get('KONLPY', 'FILE_PATH')
		self.KKMA_TAG_DICT	= json.loads(cfg.get('KONLPY', 'KKMA_TAG'))

	def run(self) :
		print ('KonLpy Start')
		fileName	= os.path.basename(self.path)
		compFilePath	= os.path.join(self.FILE_PATH, fileName)
	
		fileText = ''
		fileTextList = ''

		kkma	= Kkma()

		with open(self.path, 'r', encoding='utf-8') as readFile :
			fileTextList =	readFile.readlines()

		
		with open(compFilePath, 'w', encoding='utf-8') as comFile :
			result = None

			for fileText in fileTextList :
				if not(fileText == None or fileText.strip() == '') :
					try :
						result 	= kkma.pos(fileText)
					except Exception as e :
						print (e)
					for resultTuple in result :
						if resultTuple[1] in self.KKMA_TAG_DICT  :
							comFile.write('%s : %s [%s]' % (resultTuple[0], resultTuple[1], self.KKMA_TAG_DICT[resultTuple[1]])  + '\n')
						else :
							comFile.write('%s : %s [%s]' % (resultTuple[0], resultTuple[1], 'UnKonwn')  + '\n')
				else :
					continue

#		inputStr = input()
#		inputStr = sys.stdin.readline()
		


def main() :
	module 	= os.path.basename(sys.argv[0])
#	section	= sys.argv[1]
#	cfgFile	= sys.argv[2]

	inputFile       = sys.argv[1]
#	cfg		= ConfigParser.ConfigParser()
#	cfg.read(cfgFile)

#	logPath	= cfg.get('GENERAL', 'LOG_PATH')
#	logFile	= os.path.join( logPath, '%s_%s.log' % (module, section) )
	
#	if '-d' in sys.argv :
#		Log.Init()

#	else :
#		Log.Init( Log.CRotatingLog(logFile, 100000, 9) )

#	km		= KonlpyModule(section, cfg)
	km		= KonLpyModule(inputFile)
	km.run()

if __name__ == '__main__' :
	try:
		main()
	except ValueError :
		print(ValueError)

