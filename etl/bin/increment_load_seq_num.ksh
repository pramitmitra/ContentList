#!/bin/ksh -eu
# ---------------------------------------------------------------------------------------
# Title:      Load batch seq number increment
# Filename:   increment_load_seq_num.ksh 
# Description: This script increments the load batch seq number for the SA by 1
#              
#
# Developer:   Veera 
# Created on:  11/05/2006
# Location:    $DW_EXE/
# Logic:  
#
# Called BY    Appworx
#
# Input
#   Parameters          : none
#   Prev. Set Variables :
#   Tables, Views       : N/A
#
# Output/Return Code    :
#   0 - success
#   otherwise error
#
# Last Error Number:
#
# Date        Modified By(Name)       Change and Reason for Change
# ----------  ----------------------  ---------------------------------------
# ????-??-??  ???                     Initial Creation
# 2013-10-04  Ryan Wong               Redhat changes
##########################################################################################################

SCRIPT_NAME=${0##*/}

if [ $# != 1 ]
then
        print "Usage:  $SCRIPT_NAME <ETL ID>"
        exit 4
fi

. /dw/etl/mstr_cfg/etlenv.setup

export ETL_ID=$1
export SUBJECT_AREA=${ETL_ID%%.*}
export TABLE_ID=${ETL_ID##*.}

set +e
grep "^LOAD_JOB_ENV\>" $DW_CFG/$ETL_ID.cfg | read PARAM LOAD_JOB_ENV COMMENT
rcode=$?
set -e

if [ $rcode != 0 ]
then
	print "${0##*/}:  ERROR, failure determining value for LOAD_JOB_ENV parameter from $DW_CFG/$ETL_ID.cfg" >&2
        exit 4
fi

if [ $LOAD_JOB_ENV = all ]
then
	cat $DW_DAT/primary/$SUBJECT_AREA/$TABLE_ID.load.batch_seq_num.dat | read LOAD_BATCH_SEQ_NUM
	((NEW_BATCH_SEQ_NUM=LOAD_BATCH_SEQ_NUM+1))
	print $NEW_BATCH_SEQ_NUM > $DW_DAT/primary/$SUBJECT_AREA/$TABLE_ID.load.batch_seq_num.dat

        cat $DW_DAT/secondary/$SUBJECT_AREA/$TABLE_ID.load.batch_seq_num.dat | read LOAD_BATCH_SEQ_NUM
        ((NEW_BATCH_SEQ_NUM=LOAD_BATCH_SEQ_NUM+1))
        print $NEW_BATCH_SEQ_NUM > $DW_DAT/secondary/$SUBJECT_AREA/$TABLE_ID.load.batch_seq_num.dat	

elif [[ $LOAD_JOB_ENV = primary || $LOAD_JOB_ENV = secondary ]]
then
	cat $DW_DAT/$LOAD_JOB_ENV/$SUBJECT_AREA/$TABLE_ID.load.batch_seq_num.dat | read LOAD_BATCH_SEQ_NUM
        ((NEW_BATCH_SEQ_NUM=LOAD_BATCH_SEQ_NUM+1))
        print $NEW_BATCH_SEQ_NUM > $DW_DAT/$LOAD_JOB_ENV/$SUBJECT_AREA/$TABLE_ID.load.batch_seq_num.dat
else 
	print "${0##*/}:  ERROR, invalid value for parameter LOAD_JOB_ENV ($LOAD_JOB_ENV)" >&2
        exit 4
fi
print "batch seq number incremented for $LOAD_JOB_ENV environment"

exit

