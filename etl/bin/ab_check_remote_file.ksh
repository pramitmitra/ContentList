#!/bin/ksh -eu

#------------------------------------------------------------------------------------------------
# Filename:     ab_check_remote_file.ksh
# Description:  Calls check_remote_file.ksh and handles the return code for processing within
#               Ab Initio.  
#
# Developer:    Craig Werre
# Created on:   10/05/2005
# Location:     $DW_EXE/
#
# Execution:    $DW_EXE/ab_check_remote_file.ksh <REM_HOST> <REM_FILE> <NUM_TRIES>
#
# Parameters:   REM_HOST = <remote host>
#               REM_FILE = <remote file>
#               NUM_TRIES = <number of tries> (number of tries when file does not exist - retries every 5 minutes)
#
# Revision History:
#
# Name             Date            Description
# ---------------  --------------  ---------------------------------------------------
# Craig Werre      10/05/2005      Initial Creation
# Ryan Wong        10/04/2013      Redhat changes
#
#------------------------------------------------------------------------------------------------
if [ $# != 4 ]
then
   print "Usage:  $0 <REM_USER> <REM_HOST> <REM_FILE> <NUM_TRIES>"
   exit 4
fi

REM_USER=$1
REM_HOST=$2
REM_FILE=$3
NUM_TRIES=$4

SCRIPT_NAME=${0##*/}
REM_FILE_NAME=${REM_FILE##*/}

LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.check_remote_file.$REM_FILE_NAME.$CURR_DATETIME.log

set +e
$DW_EXE/check_remote_file.ksh $REM_USER $REM_HOST $REM_FILE $NUM_TRIES > $LOG_FILE 2>&1
rcode=$?
set -e

if [[ $rcode = 0 || $rcode = 1 ]]
then
	print $rcode
else
	print "${0##*/}:  ERROR, see log file $LOG_FILE" >&2
	exit 4
fi

exit
