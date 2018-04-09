#!/bin/bash
set -e # exit on error

#------------------------------------------------------------------------------------
# What: underlying onions data source preparation
#
# Features: 拉取线上准备好的onions库内按日备份的部分表，回滚到xserver
#
# Built-in tools: p7zip mongorestore scp
# Internal script: null
#
# author:       diggzhang
# contact:	    diggzhang@gmail.com/xingze@guanghe.tv
# since:        Mon Apr  9 13:12:27 CST 2018
#
# Update: date - description
#
#------------------------------------------------------------------------------------

#------------------------------------------------------------------------------------
# SCRIPT CONFIGURATION
#------------------------------------------------------------------------------------

SCRIPT_NAME=$(basename "$0")
VERSION=0.1

# Global variables
YEAR=$(date -d -0day '+%Y')
MONTH=$(date -d -0day '+%m')
DAY=$(date -d -0day '+%d')
LOGFILE=/tmp/airflow_scheduling.log
WORK_DIR=/home/master/yangcongDatabase/v4collections/temp/

#------------------------------------------------------------------------------------
# UTILITY FUNCTIONS
#------------------------------------------------------------------------------------

# print a log a message
log ()
{
    echo "[${SCRIPT_NAME}]: $1" >> "$LOGFILE"
    echo "[${SCRIPT_NAME}]: $1"
}

# Define script functions here
pull_compress_package() {
  date
  echo "从线上拷贝每日优先dump的业务库"
  cd "$WORK_DIR" || exit 1
  scp_pack_from_vpcdoor() {
    if [ -f "$YEAR$MONTH$DAY.7z" ]; then
      rm ./"$YEAR$MONTH$DAY.7z"
    fi
    scp -o ConnectTimeout=1000 -P 233 backup@vpcdoor:/Backup2/onions_collctions_data_use_first/"$YEAR$MONTH$DAY".7z ./
  }
  if ! scp_pack_from_vpcdoor;then
    echo "拷贝每日优先dump的业务库失败"
    rm ./"$YEAR$MONTH$DAY".7z
    date
    exit
  fi
  echo "拷贝每日优先dump的业务库成功"
  date
}

restore_dbs() {
  echo "解压业务库备份包"
  if [ ! -d "$YEAR$MONTH$DA" ]; then
    7za x "$YEAR$MONTH$DAY".7z
  else
    rm -rf ./"$YEAR$MONTH$DAY"
    7za x "$YEAR$MONTH$DAY".7z
  fi
  date
  echo "回滚业务库"
  restore_processing() {
    mongorestore --drop --db onions ./"$YEAR$MONTH$DAY"/onions/
    mongorestore --drop --db sundries ./"$YEAR$MONTH$DAY"/sundries/
  }
  restore_processing
  echo "清理临时文件"
  rm -rf ./"$YEAR$MONTH$DAY" "$YEAR$MONTH$DAY".7z
  date
}

main()
{
    log "version $VERSION"
    log "(1/2) pull_compress_package"
    pull_compress_package >> "$LOGFILE"
    log "(2/2) restore_dbs"
    restore_dbs >> "$LOGFILE"
}

main "$@"
