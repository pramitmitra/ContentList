#!/bin/ksh -eu

# Title:        HDFS File Check
# File Name:    dw_infra.hdfs_file_check.ksh
# Description:  Checks if a file exists on HDFS. Exits successfully if file is
#               found.  Loop will continue to check for file every 60 seconds.
#               JOB_ENV must be set before calling into this script.
# Developer:    Michael Weng
# Created on:
# Location:     $DW_MASTER_BIN
# Logic:
#
#
# Called by:    UC4/Unix
#
# Date          Ver#   Modified By(Name)   Change and Reason for Change
#-----------    -----  ------------------  ----------------------------------------
# 2015-10-04    1.0    Michael Weng        Initial version
# 2015-10-17    1.1    Michael Weng        Create a different commit due to rollout failed.
# 2016-10-12    1.2    Michael Weng        Add hadoop authentication
###################################################################################

typeset -fu usage

function usage {
  print "Usage: $SCRIPTNAME [-E <JOB_ENV>] [-i <INTERVAL>] -[defsz] <path>

REQUIRED:
  -d             return success if <path> is a directory.
  -e             return success if <path> exists. And this is the default.
  -f             return success if <path> is a file.
  -s             return success if file <path> is greater than zero bytes in size.
  -z             return success if file <path> is zero bytes in size.

OPTIONAL:
  -E <JOB_ENV>   [hd1|hd2|hd3|...] specify the JOB_ENV. Overwrite export environment.
  -i <INTERVAL>  sleep interval in minutes. Default is 1.
  "
}

export SCRIPTNAME=${0##*/}
export OPTIONS=""
export HDFS_FILE=""
export INTERVAL=$((1 * 60))

# Process options
while getopts "E:i#d:e:f:s:z:" opt
do
  case $opt in
    E ) export JOB_ENV=$OPTARG
        ;;
    i ) INTERVAL=$(($OPTARG * 60))
        ;;
    d ) OPTIONS="-d"
        HDFS_FILE=$OPTARG
        ;;
    e ) OPTIONS="-e"
        HDFS_FILE=$OPTARG
        ;;
    f ) OPTIONS="-f"
        HDFS_FILE=$OPTARG
        ;;
    s ) OPTIONS="-s"
        HDFS_FILE=$OPTARG
        ;;
    z ) OPTIONS="-z"
        HDFS_FILE=$OPTARG
        ;;
  esac
done

# JOB_ENV is required
if [ -z "$JOB_ENV" ]; then
  print "FATAL ERROR: JOB_ENV required"
  usage
  exit 1
fi

# Setup env based on JOB_ENV
. /dw/etl/mstr_cfg/etlenv.setup

# Login into hadoop
. $DW_MASTER_CFG/hadoop.login

# Print header
export DWI_CALLED=$0
export DWI_CALLED_ARGS=${@:-""}
export DWI_WHOAMI=$(whoami)
export DWI_START_DATETIME=$(date '+%Y%m%d-%H%M%S')

set +u
print_header
set -u

# Check parameters
if [ -z "$OPTIONS" ]; then
  print "FATAL ERROR: Missing options"
  usage
  exit 2
fi

if [ -z "$HDFS_FILE" ]; then
  print "FATAL ERROR: No HDFS file provided"
  usage
  exit 3
fi

# Checking for HDFS file
print "$HADOOP_HOME: Checking for HDFS file $HDFS_FILE"

while true
do
  $HADOOP_HOME/bin/hadoop fs -test $OPTIONS $HDFS_FILE
  if [[ $? == 0 ]]
  then
    print "$HADOOP_HOME: HDFS file check returns success $(date '+%Y%m%d-%H%M%S')"
    break
  else
    print "$HADOOP_HOME: HDFS file check failed, waiting $INTERVAL seconds $(date '+%Y%m%d-%H%M%S')"
    sleep $INTERVAL
  fi
done

tcode=0
exit
