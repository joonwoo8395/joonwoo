#!/bin/sh

# OS
OS=`uname`
PS_LOG_DIR="/home/tacs/SMS-master/logs/PS/"
TOP_LOG_DIR="/home/tacs/SMS-master/logs/TOP/"
SWAP_LOG_DIR="/home/tacs/SMS-master/logs/SWAP/"
TOP="top -d2 -s1 -n 20"
DISPLAY="tail -28"
if [ ${OS} = 'Linux' ]; then
    TOP="top -b -n 1"
    SWAP="/home/tacs/SMS-master/smem --sort=swap"
    DISPLAY="cat"
fi

cd /home/tacs/SMS-master 
. /home/tacs/.bash_profile 

umask 0 2> /dev/null
mkdir -p ${PS_LOG_DIR} 2> /dev/null
echo ============================================ >> ${PS_LOG_DIR}`date '+%Y%m%d'` 2> /dev/null
echo `date '+%H:%M:%S'` >> ${PS_LOG_DIR}`date '+%Y%m%d'` 2> /dev/null
ps -ef > /tmp/ps.tmp 2> /dev/null
cat /tmp/ps.tmp >> ${PS_LOG_DIR}`date '+%Y%m%d'` 2> /dev/null

mkdir -p ${TOP_LOG_DIR} 2> /dev/null
echo ============================================ >> ${TOP_LOG_DIR}`date '+%Y%m%d'` 2> /dev/null
echo `date '+%H:%M:%S'` >> ${TOP_LOG_DIR}`date '+%Y%m%d'` 2> /dev/null
${TOP} > /tmp/top.tmp 2> /dev/null
${DISPLAY} /tmp/top.tmp >> ${TOP_LOG_DIR}`date '+%Y%m%d'` 2> /dev/null


mkdir -p ${SWAP_LOG_DIR} 2> /dev/null
echo ============================================ >> ${SWAP_LOG_DIR}`date '+%Y%m%d'` 2> /dev/null
echo `date '+%H:%M:%S'` >> ${SWAP_LOG_DIR}`date '+%Y%m%d'` 2> /dev/null
${SWAP} > /tmp/swap.tmp 2> /dev/null
${DISPLAY} /tmp/swap.tmp >> ${SWAP_LOG_DIR}`date '+%Y%m%d'` 2> /dev/null

