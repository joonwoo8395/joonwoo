#!/bin/env bash

DEPLOY_WAR_PATH="/home/tacs/DEPLOY_WAR"
TACS_BASE_PATH="/home/tacs/TACS/apache-tomcat-8.5.32"
TACS_API_BASE_PATH="/home/tacs/TACS-API/tacs-api-tomcat"
TACS_BASE_WAR_PATH="$TACS_BASE_PATH/webapps"
TACS_API_BASE_WAR_PATH="$TACS_API_BASE_PATH/war"
TACS_BASE_WAR="TACS.war"
TACS_API_BASE_WAR="tacsapi.war"

function get_process_pid(){
	local process_name=$1
	local pid=`ps -ef|grep "$process_name"|grep -v grep|grep -v vi|awk '{print $2}'`
	echo "${pid}"	
}

function shutdown_tomcat(){
	local process_base_path=$1
	echo `sh $process_base_path/bin/shutdown.sh`
}

function backup_war() {
	local base_path=$1
	local war_path=$2
	local war_name=$3
	local backup_folder="$base_path/BACKUP"

	if [ ! -d $backup_folder ]; then
		echo `mkdir $backup_folder`
	fi

	if [ ! -d "$backup_folder/$(date +%Y%m%d)" ]; then
		echo `mkdir $backup_folder"/$(date +%Y%m%d)"`
	fi

	if [ -e $war_path"/"$war_name ]; then
		echo `mv -f $war_path"/"$war_name $backup_folder"/$(date +%Y%m%d)/"$war_name` 
	fi
	echo `mv -f $base_path"/webapps/"* $backup_folder"/$(date +%Y%m%d)/"` 
}

#TACS Deploy
echo "Deploy Type : TACS || TACS-API "
echo -e "Deploy Type : \c"
read DEPLOY_TYPE

echo "------------------------------START DEPLOY [ $DEPLOY_TYPE ]------------------------------"
if [ $DEPLOY_TYPE == "TACS" ]; then
	TOMCAT_PID=$(get_process_pid $TACS_BASE_PATH)
	if [ "$TOMCAT_PID" != "" ]; then 
		shutdown_tomcat $TACS_BASE_PATH
		echo "Shutdown To $DEPLOY_TYPE Tomcat !!!"
		sleep 30 
		TOMCAT_PID=$(get_process_pid $TACS_BASE_PATH)	
		echo $TOMCAT_PID
		if [ "$TOMCAT_PID" != "" ]; then
			echo `kill -9 $TOMCAT_PID`
		fi

		backup_war $TACS_BASE_PATH $TACS_BASE_WAR_PATH $TACS_BASE_WAR

		echo `mv $DEPLOY_WAR_PATH"/"$TACS_BASE_WAR $TACS_BASE_WAR_PATH"/"$TACS_BASE_WAR`

		echo `sh $TACS_BASE_PATH/bin/startup.sh`
	else 
		echo "The process has already ended."
	fi	

elif [ $DEPLOY_TYPE == "TACS-API" ]; then
	TOMCAT_PID=$(get_process_pid $TACS_API_BASE_PATH)
	echo "$TOMCAT_PID"
	if [ "$TOMCAT_PID" != "" ]; then
   		shutdown_tomcat $TACS_API_BASE_PATH 
    	echo "Shutdown To $DEPLOY_TYPE Tomcat !!!"
    	sleep 30
    	TOMCAT_PID=$(get_process_pid $TACS_API_BASE_PATH)
    	echo $TOMCAT_PID
    	if [ "$TOMCAT_PID" != "" ]; then
        	echo `kill -9 $TOMCAT_PID`
   		fi

		backup_war $TACS_API_BASE_PATH $TACS_API_BASE_WAR_PATH $TACS_API_BASE_WAR

		echo `mv $DEPLOY_WAR_PATH"/"$TACS_API_BASE_WAR $TACS_API_BASE_WAR_PATH"/"$TACS_API_BASE_WAR`

		echo `sh $TACS_API_BASE_PATH/bin/startup.sh`
	else
	    echo "The process has already ended."
	fi
fi

echo "------------------------------END DEPLOY [ $DEPLOY_TYPE ]------------------------------"
