#!/home/mobigen/anaconda3/bin/python
# -*- coding: utf-8 -*-

import configparser
from pykospacing import spacing
import os
import sys

class KoSpacingModule() :
	def __init__(self) :
		cfg     = configparser.ConfigParser()
		cfg.read('/home/mobigen/user/KimJW/TextMining/conf/module.conf')

		self.FILE_PATH = cfg.get('KOSPACING', 'FILE_PATH')

	def spacing(self, inputText, writeFile) :
		if len(inputText) < 198 :
			writeFile.write( spacing( inputText ) + '\n')

		else :
			textTemp 	= inputText[:197]
			reText		= inputText[198:]
			writeFile.write( spacing( textTemp ) + '\n')
			self.spacing(reText, writeFile)

	def run(self, inputFile) :
		print ('KoSpacingModule Start')

#		filePath 		= '/home/mobigen/user/KimJW/bin/textFile/recipe.txt'
		filePath		= inputFile
		fileName		= os.path.basename(inputFile)

		compFilePath	= os.path.join(self.FILE_PATH, fileName)
	
		fileTextList 	= list()

		with open(filePath, 'r', encoding='utf-8') as readFile :
			fileTextList =	readFile.readlines()

		with open(compFilePath, 'w', encoding='utf-8') as writeFile :
			for fileText in fileTextList :
				self.spacing(fileText, writeFile)

		#ShutDown = False
		#while not ShutDown :
			#inputStr = input()
			#print(spacing(inputStr))


def main() :
	module 		= os.path.basename(sys.argv[0])
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

#	km		= KonlpyModule(section, cfg)
	ks		= KoSpacingModule()
	ks.run(inputFile)

if __name__ == '__main__' :
	try:
		main()
	except ValueError :
		print(ValueError)

