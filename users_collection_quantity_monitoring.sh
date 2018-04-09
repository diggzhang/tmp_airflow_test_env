#!/bin/bash
set -e # exit on error

#------------------------------------------------------------------------------------
# What: users collection quantity monitoring
#
# Features: 监控users表的量级，如果有问题，及时预警
#
# Built-in tools: python3
# Internal script: ./users_coll_warning.py
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

#------------------------------------------------------------------------------------
# UTILITY FUNCTIONS
#------------------------------------------------------------------------------------

# print a log a message
log ()
{
    echo "[${SCRIPT_NAME}]: $1" >> "$LOGFILE"
    echo "[${SCRIPT_NAME}]: $1"
}

monitoring_users_collection() {
  log date
  $(which python3) users_coll_warning.py >> $LOGFILE
  log date
}

main()
{
    log "version $VERSION"
    log "(1/1) monitoring_users_collection"
    monitoring_users_collection
}

main "$@"
