#!/bin/bash
set -e # exit on error

#------------------------------------------------------------------------------------
# What: A quick-start BASH template for airflow shell command
#
# Features: description function of your script
#
# author:       Id
# contact:	    Email
# since:        Initial date
#
# Update: date - description
#------------------------------------------------------------------------------------

#------------------------------------------------------------------------------------
# SCRIPT CONFIGURATION
#------------------------------------------------------------------------------------

SCRIPT_NAME=$(basename "$0")

# If debug information should be shown
VERBOSE=

VERSION=0.1

# Add global variables here
OP1=
OP2=

#------------------------------------------------------------------------------------
# UTILITY FUNCTIONS
#------------------------------------------------------------------------------------

# print a log a message
log ()
{
    echo "[${SCRIPT_NAME}]: $1" > /dev/stderr
}

# Define your own script functions here

# Print a usage message
usage()
{
cat << USAGE
usage: $0 [-v] [-h] -a op1 -b op2

Short description

REQUIRED OPTIONS:
    -a op1      Option 1
    -b op2      Option 2
OTHER OPTIONS:
    -v         Show debuging messages
    -h         Show this help message
    -V         Show version
USAGE
}

# Get the script options
get_options()
{
    while getopts "a:b:hvV:" OPTION
    do
        if [ "$OPTION" == "-" ]; then
            OPTION=$OPTARG
        fi
        case $OPTION in
            a)  OP1=${OPTARG};;
            b)  OP2=${OPTARG};;
            h)  usage && exit 0;;
       'help')  usage && exit 0;;
            V)  echo $VERSION && exit 0;;
    'version')  echo $VERSION && exit 0;;
            v)  VERBOSE=1;;
            \?) echo "Invalid option" && usage && exit 1;;
        esac
    done
}

main()
{
    get_options "$@"
    # Put the rest of your main script here
}

main "$@"
