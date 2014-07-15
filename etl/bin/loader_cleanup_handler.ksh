#!/bin/ksh -eu

#------------------------------------------------------------------------------------------------
# Filename:     loader_cleanup_handler.ksh
# Description:  Wrapper for the script loader_cleanup.ksh
# Developer:    Wenhong Cao
# Created on:   03/12/2007
# Location:     $DW_EXE/
#
# Execution:    $DW_EXE/shell_handler.ksh <ETL_ID> <JOB_ENV>
#
# Parameters:   ETL_ID = <SUBJECT_AREA.TABLE_ID>
#               JOB_ENV = <extract|primary|secondary>
#
# Revision History:
#
# Name             Date            Description
# ---------------  --------------  ---------------------------------------------------
# Wenhong Cao      03/12/2007      Initial Creation
# Ryan Wong        10/04/2013      Redhat changes
#------------------------------------------------------------------------------------------------
if [ $# -lt 2 ]
then
	print "Usage:  $0 

	Parameters:
	<ETL_ID> <JOB_ENV>

	ETL_ID = <SUBJECT_AREA.TABLE_ID>
	JOB_ENV = <extract|primary|secondary>

	Example: \$DW_EXE/loader_cleanup_handler.ksh dw_soj.dw_soj_session_log extract
"

   exit 4
fi

export ETL_ID=$1
export JOB_ENV=$2
SHELL_EXE=$DW_EXE/loader_cleanup.ksh

SCRIPT_NAME=${0##*/}
SHELL_EXE_NAME=${SHELL_EXE##*/}
export SUBJECT_AREA=${ETL_ID%%.*}
export TABLE_ID=${ETL_ID##*.}
export CURR_DATETIME=$(date '+%Y%m%d-%H%M%S')

if [ $JOB_ENV = extract ]
then
	export JOB_TYPE=extract
	export JOB_TYPE_ID=ex
else
	export JOB_TYPE=load
	export JOB_TYPE_ID=ld
fi

. /dw/etl/mstr_cfg/etlenv.setup

export DW_SA_LOG=$DW_LOG/$JOB_ENV/$SUBJECT_AREA
export DW_SA_TMP=$DW_TMP/$JOB_ENV/$SUBJECT_AREA
export DW_SA_DAT=$DW_DAT/$JOB_ENV/$SUBJECT_AREA
export DW_SA_IN=$DW_IN/$JOB_ENV/$SUBJECT_AREA
export DW_SA_ARC=$DW_ARC/$JOB_ENV/$SUBJECT_AREA

export JOB_ENV_UPPER=$(print $JOB_ENV | tr [:lower:] [:upper:])

PARENT_ERROR_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.${SHELL_EXE_NAME%.ksh}.$CURR_DATETIME.err

# Source the error message handling logic.  On failure, trap will send the contents of the PARENT_ERROR_FILE to the
# subject area designated email addresses.
. $DW_LIB/message_handler

print "
####################################################################################################################
#
# Beginning loader cleanup handler ETL_ID: $ETL_ID, JOB_ID: $JOB_ENV:  `date`
#
####################################################################################################################
"
print "Processing shell script : $SHELL_EXE  `date`"
LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.${SHELL_EXE_NAME%.ksh}.$CURR_DATETIME.log

set +e
$SHELL_EXE $JOB_ENV $JOB_TYPE_ID > $LOG_FILE 2>&1
rcode=$?
set -e

if [ $rcode != 0 ]
then
	print "${0##*/}:  ERROR running $SHELL_EXE_NAME, see log file $LOG_FILE" >&2
	exit $rcode
fi

print "
####################################################################################################################
#
# loader_cleanup handler for ETL_ID: $ETL_ID, JOB_ID: $JOB_ENV complete   `date`
#
####################################################################################################################
"

tcode=0
exit
