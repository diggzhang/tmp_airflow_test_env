#!/bin/bash
set -e # exit on error
# export PATH="/usr/local/bin:/usr/bin:/usr/local/sbin:/usr/sbin:/home/master/.local/bin:/home/master/bin"

#------------------------------------------------------------------------------------
# What: underlying orders data source preparation
#
# Features: 准备orders表业务库底层数据,构建orderEvents表
#
# Built-in tools: p7zip mongo python2.7 mongorestore
# Internal script: ./service/orders_processing_bundle_style.py ./service/collectionStatus.py
#
# author:       diggzhang
# contact:	    diggzhang@gmail.com/xingze@guanghe.tv
# since:        Mon Apr  9 14:51:51 CST 2018
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
LOGFILE=/tmp/airflow_scheduling.log
WORK_DIR=/home/master/yangcongDatabase/v4collections
BACKDOOR_BACKUP_FILEPATH=/Backup2/online_orders_daily_backup
DBHOSTNAME="localhost"
YEAR=$(date -d -1day '+%Y')
MONTH=$(date -d -1day '+%m')
DAY=$(date -d -1day '+%d')
DB_V4_EVENTS="eventsV4"

#------------------------------------------------------------------------------------
# UTILITY FUNCTIONS
#------------------------------------------------------------------------------------

drop_restore_orders_mongo_collection() {
  local arr=("$@")
  for collection_name in "${arr[@]}";
  do
    mongorestore --drop --host $DBHOSTNAME --db $DB_V4_EVENTS --collection "$collection_name" ./OrderProcessing/"$collection_name".bson
  done
}

# print a log a message
log ()
{
    echo "[${SCRIPT_NAME}]: $1" >> "$LOGFILE"
    echo "[${SCRIPT_NAME}]: $1"
}

pull_compress_orders_package() {
  log "开始拉取备份包 $(date)"
  cd $WORK_DIR
  scpOrdersPackFromVpcDoor() {
    scp -P 233 backup@vpcdoor:$BACKDOOR_BACKUP_FILEPATH/"orderStuff""$YEAR$MONTH$DAY".7z ./
  }
  scpOrdersPackFromVpcDoor
  if [ $? -ne 0 ];then
    log "拉取orders备份包失败"
    date
    exit
  fi
  $(which python) ./service/collectionStatus.py events 1
  log "拉取备份包结束 $(date)"
}

restore_orders_package() {
  cd $WORK_DIR
  7za x -aoa "orderStuff""$YEAR$MONTH$DAY".7z
  log "解压备份包完成 $(date)"

  log "重命名昨天的ordersEvents表"
  echo "db.orderEvents.renameCollection('orderevents_$YEAR$MONTH$DAY')" > removeBackendOrderEvents.js
  mongo --host $DBHOSTNAME $DB_V4_EVENTS ./removeBackendOrderEvents.js

  log "开始回滚 OrderProcessing 表"

  declare -a orders_remaining_collctions_name_arr=(
      "orders"
      "goods"
      "textbooks"
      "abtests"
      "coupons"
      "couponcodes"
      "redeemcodes"
      "refundrequests"
      "examorders"
      "packageprices"
  )
  drop_restore_orders_mongo_collection "${orders_remaining_collctions_name_arr[@]}"

  rm -rf ./*.js ./OrderProcessing
  log "完成回滚 OrderProcessing 其余表 $(date)"

}

orders_events_processing() {
  log "执行 orders->ordersEvents的清洗逻辑 $(date)"
  cd ./service && $(which python) ./orders_processing_bundle_style.py
  log "完成 orders->ordersEvents的清洗逻辑 $(date)"
  log "建立orderEvents索引"
  echo "db.orderEvents.ensureIndex({'eventKey':1, 'orderTime':1}, {'background': true})" > indexOrderEvents.js
  mongo --host $DBHOSTNAME $DB_V4_EVENTS ./indexOrderEvents.js
  $(which python) ./service/collectionStatus.py orderEvents 1
}

main()
{
    log "version $VERSION"
    log "(1/1) pull_compress_orders_package"
    pull_compress_orders_package
    log "(2/3) restore_orders_package"
    restore_orders_package
    log "(3/3) orders_events_processing"
    orders_events_processing
}

main "$@"
