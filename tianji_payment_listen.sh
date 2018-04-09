#!/bin/bash
set -e # exit on error

#------------------------------------------------------------------------------------
# What: tianji payment listen scripts
#
# Features: 天机平台相关脚本
#
# Built-in tools:
# Internal script: payment_listen.py ./service/collectionStatus.py
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
WORK_DIR=/home/master/yangcongDatabase/v4collections/temp/
MIX_SCRIPT_DIR=/home/master/superset/user_and_device_data/
SHINY_APP_DIR=/home/master/ShinyApps/tianji/

#------------------------------------------------------------------------------------
# UTILITY FUNCTIONS
#------------------------------------------------------------------------------------

# print a log a message
log ()
{
    echo "[${SCRIPT_NAME}]: $1" >> "$LOGFILE"
    echo "[${SCRIPT_NAME}]: $1"
}

tianji_scripts() {
  log "启动 payment_listen 为天机平台准备核心数据"
  cd "$SHINY_APP_DIR" || exit 1
  $(which python3.6) ./payment_listen.py
  cd $WORK_DIR || exit 1
  $(which python) ./service/collectionStatus.py taojie 1
  python ./smack_my_log_to_guanghe.py
}

tianji_mix_data () {
  cd "$MIX_SCRIPT_DIR" || exit 1
  $(which bash) ./mix.sh
}

main()
{
    log "version $VERSION"
    log "(1/2) tianji_scripts"
    log "(2/2) tianji_mix_data"
}

main "$@"
