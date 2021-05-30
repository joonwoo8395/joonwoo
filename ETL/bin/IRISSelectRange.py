#!/usr/bin/env python
#-*- coding:utf-8 -*-
import os
import sys
import json
import Mobigen.Common.Log as Log;
import ConfigParser
import API.M6 as M6


class IRISSelectRange() :

	def dailyRange( self, partitionDate ) :
		resultDate = ''.join( [ partitionDate[0:8], '000000' ] )
		return resultDate

	def minRange( self, partitionDate ) :
		resultDate = ''.join( [ partitionDate[0:10], '0000' ] )
		return resultDate

	def secRange( self, partitionDate ) :
		resultDate = ''.join( [ partitionDate[0:12], '00' ] )
		return resultDate
