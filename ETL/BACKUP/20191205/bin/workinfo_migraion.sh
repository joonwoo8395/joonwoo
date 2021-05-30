#!/usr/bin/env bash

if [ $# -ne 2 ]
	then 
	echo '[ERROR] WorkInfoCollector migraion arguments at least 2'
	printf 'Usage: sh %s {searchStartDate} {searchEndDate}' $0
	echo
	printf 'ex) sh %s 201901010000 20190101001' $0
	echo
	exit 1
fi

searchStartDate=$1
searchEndDate=$2

if [ ${#searchStartDate} -ne 12 ]
	then
	echo '[ERROR] searchStartDate format is yyyyMMddHHmm(12 digits)'
	exit 1
elif [ ${#searchEndDate} -ne 12 ]
	then
	echo '[ERROR] searchEndDate format is yyyyMMddHHmm(12 digits)'
	exit 1
fi
	
python /home/tacs/TACS-EF/ETL_2.0/bin/WorkInfoCollector.py TANGO_WORKINFO_MIGRATION /home/tacs/TACS-EF/ETL_2.0/conf/WorkInfoCollector.conf $1 $2

