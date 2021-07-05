# -*- coding=utf-8 -*-
'''
Created on 2019. 12. 19.

@author: scyun
'''

import re
import os


class PathParameterResolver(object):
	'''
	#- 생성시 전달된 패턴을 기반으로 주어진 문자열에 대해 변수를 추출하는 기능을 담당한다.
	#- 변수 선언은 {} 로 감싸서 지정한다.

	Usage:
	#- Instance 생성 :
	pr = PathParameterResolver("abc.api.com/learn/sections/{sectionId}/assignments/{assignmentId}-{yyyymmdd}.csv")

	#- 각 변수영역에 대한 값 추출, 응답은 dictionary 로 전달된다.
	__values_dic__ = pr.extract_path_variable("abc.api.com/learn/sections/sec1213/assignments/uuid-1299-20191212.csv")

	#- __values_dic__ 의 결과
	{'sectionId': 'sec1213', 'assignmentId': 'uuid-1299', 'yyyymmdd': '20191212'}

	'''

	def __init__(self, path_template):
		self.path_template = path_template
		self.parameter_pattern_str = "(\\{[a-zA-Z0-9]+\\})"

		## 사용자가 설정한 Pattern 문자열 추출하여 배열로 저장해둠.
		self.path_var_keys = re.compile(self.parameter_pattern_str).findall(path_template)

		## 입력된 패턴을 정규식으로 변환
		for v in self.path_var_keys :
			self.path_template = self.path_template.replace(v , "([^\/:\*\?\"<>\|]+)")

		self.path_regexp = re.compile(self.path_template)


	def extract_path_variable(self, v):
		'''
	#-입력된 문자열을 정의된 패턴에 맞는지 검사하고 패턴에 맞을경우 각 패턴에 대한 값을 return 한다.
		'''

		result = {}
		x = self.path_regexp.findall(v)
		if len(x) <= 0:
			return {} ## empty dictionary return

		_extract_values = x[0];

		for v in enumerate(_extract_values) :
			key_idx = v[0]
			key_name = self.path_var_keys[key_idx]
			key_name = key_name[1:len(key_name) - 1]
			result[key_name] = v[1]

		return result

if __name__ == '__main__' :
	pr = PathParameterResolver("/DATA/COLLECT/{centerCode}/{dataDir}/{yyyyMMdd}/{range}/{directory}/{datasetCode}\.csv_{idx}")

	#- 각 변수영역에 대한 값 추출, 응답은 dictionary 로 전달된다.
	__values_dic__1 = pr.extract_path_variable("/DATA/COLLECT/DJ/SPLIT/20200113/20191101-20191130/089.ALL.atms-rse2section-match/atms-rse2section-match.csv_00000")
	#__values_dic__1 = pr.extract_path_variable("/DATA/COLLECT/DJ/SPLIT2/atmsrsecollectinfo.csv_00009")
	print (__values_dic__1)
	ds_code_1 = __values_dic__1["datasetCode"].split(".")
	print (ds_code_1)
	ds_code_1 = ds_code_1[0]
	print(ds_code_1)

