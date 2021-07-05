#!/bin/env python3
# -*- coding: utf-8 -*-

#import $PYTHON_LIB$
import os
import sys
import glob
import time, datetime
import signal


#import $MOBIGEN_LIB$
import Mobigen.Common.Log_PY3 as Log; Log.Init()

#import $PROJECT_LIB$

SHUTDOWN = False

def shutdown(signalnum, handler):
	    global SHUTDOWN
	    SHUTDOWN = True
	    sys.stderr.write('Catch Signal: %s \n' % signalnum)
	    sys.stderr.flush()

signal.signal(signal.SIGTERM, shutdown) # sigNum 15 : Terminate
signal.signal(signal.SIGINT, shutdown)  # sigNum  2 : Keyboard Interrupt
signal.signal(signal.SIGHUP, shutdown)  # sigNum  1 : Hangup detected
signal.signal(signal.SIGPIPE, shutdown) # sigNum 13 : Broken Pipe

def makedirs(path) :

    try :
        os.makedirs(path)
        __LOG__.Trace( path )
    except : pass
#- File Remover class
class FileRemover:
	run_function = None
	def __init__(self, strDir):
		__LOG__.Trace("__init__")
		self.strDir = os.path.normcase(strDir)
		self.mode = None
		__LOG__.Trace("=== Target Directory Setting : %s" % self.strDir)

	#- Remove Function
	def time_remove(self, pattern, std_day):

		cur_time = datetime.datetime.now()

		file_list = sorted(glob.glob('%s/%s' % (self.strDir, pattern)))
		for file_name in file_list:
			if not os.path.isfile(file_name):
				continue
			file_mtime = datetime.datetime.fromtimestamp(os.path.getmtime(file_name))
			if (cur_time - file_mtime).total_seconds() / (24 * 60 * 60) >= int(std_day) :
				try:
					os.remove(file_name)
					__LOG__.Trace('=== %s is removed ===' % file_name)
				except OSError: 
					__LOG__.Trace('=== ERROR !!! %s can not remove ===' % file_name)

	def count_remove(self, pattern, count):

		files = list(filter(os.path.isfile, glob.glob('%s/%s' %(self.strDir, pattern))))

		if len(files) > int(count):

			files.sort(key=os.path.getmtime)
			target_file = files[:len(files) - int(count)]

			for file_name in target_file:
				try:
					os.remove(file_name)
					__LOG__.Trace('=== %s is removed ===' % file_name)
				except OSError: 
					__LOG__.Trace('=== ERROR !!! %s can not remove ===' % file_name)

	def disk_check(self, target_disk):
		dir_total = 0
		for target_dir in self.strDir.split(','):
			dir_total += self.get_dir_size(path=os.path.expanduser(target_dir))
		ds = os.statvfs(target_disk)
		total = ds.f_blocks * ds.f_frsize
		#used = (ds.f_blocks - ds.f_bfree) * ds.f_frsize
		
		return (dir_total / total) * 100

	#- 주의 ! 디스크 컷라인을 낮게 잡으면 모든 하위 파일 삭제
	def disk_remove(self, pattern, disk_cutline, target_disk, act_cutline):
		__LOG__.Trace('directory / disk total = %f ' % self.disk_check(target_disk))
		if self.disk_check(target_disk) > float(act_cutline):
			#disk full
			total_files = {}

			for strDir in self.strDir.split(','):
				for file_name in list(filter(os.path.isfile,glob.glob(os.path.join(strDir,'**', pattern),recursive=True))):
					total_files[os.path.getmtime(file_name)] = file_name
			sort_keys = sorted(total_files.keys(),reverse = True)	

			while_cnt = 0

			while self.disk_check(target_disk) >= float(disk_cutline):
				try:
					target_key = sort_keys.pop()
					#os.remove(total_files[target_key])
					__LOG__.Trace('=== File Remove : "%s" ' % total_files[target_key])

					if while_cnt >= 10 :
						__LOG__.Trace('!!! WARNING !!! Many File deleted, Please Check this Option !!!')
						break

					while_cnt += 1
				except OSError:
					__LOG__.Trace('=== ERROR !!! "%s" can not remove ===' % total_files[target_key])

				except IndexError:
					__LOG__.Trace('This Directory has no files like "%s"' % pattern)
					break

	def get_dir_size(self, path):
		total = 0
		path = os.path.expanduser(path)
		with os.scandir(path) as it:
			for entry in it:
				if entry.is_file():
					total += entry.stat().st_size
				elif entry.is_dir():
					total += self.get_dir_size(entry.path)
		return total

	def recu_time_remove(self, pattern, std_day):
		cur_time = datetime.datetime.now()

		for file_name in list(filter(os.path.isfile,glob.glob(os.path.join(self.strDir,'**', pattern),recursive=True))):

			file_mtime = datetime.datetime.fromtimestamp(os.path.getmtime(file_name))

			if (cur_time - file_mtime).total_seconds() / (24 * 60 * 60) >= int(std_day) :
				try:
					os.remove(file_name)
					__LOG__.Trace('=== "%s" is removed ===' % file_name)
				except OSError: 
					__LOG__.Trace('=== ERROR !!! "%s" can not remove ===' % file_name)
		

				
	#- Option Setting
	def setOption(self, opt_arg):
		
		#opt_arg = [opt, opt-data, dir, pattern, (target_disk),(action_value)]
		#- mtime 정렬 후 상위 몇 개 이하 삭제
		opt = opt_arg[0]
		if opt == '-c' or opt == '--count':
			self.mode = 'count'
			self.option_1 = opt_arg[3]
			self.option_2 = opt_arg[1]
			self.run_function = self.count_remove

			__LOG__.Trace("setting : Count Option Setting")

		#- 특정 시간 이상 지난 파일 삭제
		elif opt == '-t' or opt == '--time':
			self.mode = 'time'
			self.option_1 = opt_arg[3]
			self.option_2 = opt_arg[1]
			self.run_function = self.time_remove

			__LOG__.Trace("setting : Time Option Setting")

		#- 디스크 사용량에 맞춰 mtime이 낮은 순서대로 삭제
		elif opt == '-d' or opt == '--disk':
			self.mode = 'disk'
			self.option_1 = opt_arg[3] # patt
			self.option_2 = opt_arg[1] # opt value
			self.option_3 = opt_arg[4] # target disk
			self.option_4 = opt_arg[5] # action value
			self.run_function = self.disk_remove

			__LOG__.Trace("setting : Disk Option Setting")

		#- 하위 디렉토리까지 t 옵션 적용
		elif opt == '-rt' or opt == '--recu-time' :
			self.mode = 'recu-time'
			self.option_1 = opt_arg[3]
			self.option_2 = opt_arg[1]
			self.run_function = self.recu_time_remove

			__LOG__.Trace("setting : Recursive Time Option Setting")

		else :

			__LOG__.Trace("setting : False")
			return False

		return True

	def run(self):
		try:
			if self.mode == 'disk':
				self.run_function(self.option_1, self.option_2, self.option_3, self.option_4)
			else :
				self.run_function(self.option_1, self.option_2)

		except:
			__LOG__.Exception()

#- Directory Remover class
class DirRemover:
	run_function = None
	def __init__(self, strDir):
		#__LOG__.Trace("__init__")
		self.strDir = os.path.normcase(strDir)
		__LOG__.Trace("=== Target Directory Setting : %s" % self.strDir)

	#- Remove Function
	def time_remove(self, std_day):
		remove_target = []
		#- check from leaf node
		cur_time = datetime.datetime.now()
		for parent_path, child_dir, child_files in os.walk(self.strDir, topdown = False):
			#- strDir Preserve
			if self.strDir == parent_path:
				continue

			if not child_files and not child_dir:
				dir_mtime = datetime.datetime.fromtimestamp(os.path.getmtime(parent_path))

				if (cur_time - dir_mtime).total_seconds() / (60 * 60 * 24) >= float(std_day):
					remove_target.append(parent_path)

			elif not child_files:
				dir_mtime = datetime.datetime.fromtimestamp(os.path.getmtime(parent_path))

				if (cur_time - dir_mtime).total_seconds() / (60 * 60 * 24) >= float(std_day):
					#하위 디렉토리가 삭제 대상인 경우
					if set(map(lambda x : os.path.join(parent_path,x), child_dir)) - set(remove_target) == set():
						remove_target.append(parent_path)
				
				

		for rm_file_name in remove_target:
			try:
				os.rmdir(rm_file_name)
				__LOG__.Trace('=== "%s" Directory is removed ===' % rm_file_name)
			except OSError: 
				__LOG__.Trace('=== ERROR !!! "%s" can not remove ===' % rm_file_name)

	#- Option Setting
	def setOption(self, opt_arg):
		#- 특정 시간 이상 지난 파일 삭제
		opt = opt_arg[0]
		if opt == '-t' or opt == '--time' :

			self.option_1 = opt_arg[1]
			self.run_function = self.time_remove

			__LOG__.Trace("setting : Time Option Setting")

		else :
			__LOG__.Trace("setting : False")
			return False

		return True

	def run(self):
		try:
			self.run_function(self.option_1)

		except:
			__LOG__.Exception()

#- Option Check
def optionCheck(arg):
	remover_class = None
	
	target_dir = arg[4]
	for t_dir in target_dir.split(','):
		if not (os.path.exists(t_dir) and os.path.isdir(t_dir)):
			sys.stderr.write('Target Directory is not valid , "%s\n' % t_dir)
			return None

	try :
		target_type = arg[1]

	except IndexError:
		sys.stderr.write('Required option is not filled\n')
		usage()

	#-type Check
	if target_type == '-F' or target_type == '--file' :

		remover_class = FileRemover(target_dir)

	elif target_type == '-D' or target_type == '--directory':
		remover_class = DirRemover(target_dir)

	else :
		usage()

	if not remover_class.setOption(arg[2:]):
		return None

	return remover_class

#- Usage
def usage():
	sys.stderr.write('Usage		: %s type option option-value directory ("file-pattern") ("target-disk") ("action-value") ...\n'% sys.argv[0] )
	sys.stderr.write('Usage		: type ? directory : -D or --directory , file : -F or --file\n')
	sys.stderr.write('Usage		: option ? count : -c or --count , time : -t or --time, disk : -d or --disk \n')
	sys.stderr.flush()
	sys.exit()
	

#- main
def main():
	module = os.path.basename(sys.argv[0])
	#- pattern "*" is changed FDRemover.py, replace this
	sys.argv[1:] = list(map(lambda x : '*' if x == module or x == module.split('.')[0] else x, sys.argv[1:]))

	#- Usage
	if len(sys.argv) < 5:
		usage()

	#- Log path
	log_path = os.path.expanduser('~/DS_ETL/log')
	makedirs(log_path)

	log_file = os.path.join(log_path, '%s%s%s_%s.log' % (os.path.splitext(module)[0], sys.argv[1], sys.argv[2], sys.argv[3]) )
	Log.Init(Log.CRotatingLog(log_file, 10240000, 9))

	pid = os.getpid()
	__LOG__.Trace('============= %s START [pid:%s]==================' % ( module, pid ))
	
	remover_class = optionCheck(sys.argv)
	if not remover_class:
		sys.stderr.write("Option Setting Fail... Please retry\n")
		usage()
		
	remover_class.run()

	__LOG__.Trace('============= %s END [pid:%s]====================' % ( module, pid ))


if __name__=='__main__':
	main()
