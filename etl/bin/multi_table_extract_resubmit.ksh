#!/bin/ksh -eu
#------------------------------------------------------------------------------------------------
# Title:        CATY extract job resubmit
# File Name:    caty_extract_resubmit.ksh
# Description:  This job is for ease of restart of a multi-table extract run for a particular ETL_ID and host
#               The overall job must still be processing.  The host may, or may not, be presently extracting
#               data, but it can not yet be complete.
# Developer:    Brian Wenner
# Created on:   2006-09-11
# Location:     $DW_EXE  
# Logic:       
#
#
# Called by:    Appworx/Unix
#
# Date         Ver#   Modified By(Name)            Change and Reason for Change
#---------    -----  ---------------------------  ------------------------------
# 2006-09-11   1.0    Brian Wenner                  Added Heading Info
# 2013-10-04   1.1    Ryan Wong                     Redhat changes
#
#------------------------------------------------------------------------------------------------

if [ $# != 3 ]
then
   print "Usage:  $0 <ETL_ID> <FILE_ID> <STANDBY YN>"
   exit 4
fi

ETL_ID=$1
RFILE_ID=$2
HOST_STBY=$3
JOB_ENV=extract
SUBJECT_AREA=${ETL_ID%%.*}
TABLE_ID=${ETL_ID##*.}
JOB_TYPE_ID=ex

. /export/home/abinitio/cfg/abinitio.setup

DW_SA_TMP=$DW_TMP/$JOB_ENV/$SUBJECT_AREA
DW_SA_LOG=$DW_LOG/$JOB_ENV/$SUBJECT_AREA
CATY_LIS_FILE=$DW_CFG/dw_caty.sources.lis
TABLE_LIS_FILE=$DW_CFG/$ETL_ID.sources.lis
LIS_FILE=$TABLE_LIS_FILE
IS_CATY_STBY=0

# validate that this ETL job is indeed a CATY PROCESS, and get standard IS_CATY_STBY value
# This is needed since the existing run for a host would be via the standard DBC file.
set +e
grep "^IS_CATY\>" $DW_CFG/$ETL_ID.cfg | read PARAM IS_CATY COMMENT
rcode=$?
set -e

if [ $rcode != 0 ]
then
   print "${0##*/}:  ERROR, failure determining value for IS_CATY parameter from $DW_CFG/$ETL_ID.cfg" >&2
   exit 4
fi

if [ $IS_CATY -eq 1 ]
then
   LIS_FILE=$CATY_LIS_FILE
   set +e
   grep "^IS_CATY_STBY\>" $DW_CFG/$ETL_ID.cfg | read PARAM IS_CATY_STBY COMMENT
   rcode=$?
   set -e

   if [ $rcode != 0 ]
   then
      print "${0##*/}:  ERROR, failure determining value for IS_CATY_STBY parameter from $DW_CFG/$ETL_ID.cfg" >&2
      exit 4
   fi
fi

#make sure the caty number exists in the CATY_LIS_FILE
FILE_ID_FOUND=0
while read FILE_ID DBC_FILE STBY_DBC_FILE
do
	 # this line checks for if the is number or not
	 if [ $FILE_ID -eq $FILE_ID 2> /dev/null ]	
	 then
       COMP_OPER="-eq"
	 else 
       COMP_OPER="=="
	 fi
   if [ $FILE_ID $COMP_OPER $RFILE_ID ]
   then
        FILE_ID_FOUND=1
        if [ $IS_CATY_STBY -eq 0 ]
        then
             eval DBC_FILE=$DBC_FILE
        else
             eval DBC_FILE=$STBY_DBC_FILE
        fi

        break
   fi   
done < $LIS_FILE

if [ $FILE_ID_FOUND -eq 0 ]
then
   print "${0##*/}:  ERROR, Restart FILE_ID: $RFILE_ID for ETL_ID $ETL_ID could not be found" >&2
   exit 4
fi

if [[ "$HOST_STBY" = "y" || "$HOST_STBY" = "Y" || "$HOST_STBY" = "1" ]]
then
   HOST_STBY=1
else
   HOST_STBY=0
fi

print "$HOST_STBY" > $DW_SA_TMP/$ETL_ID.ex.$DBC_FILE.restart
print "$DW_SA_TMP/$ETL_ID.ex.$DBC_FILE.restart is created with $HOST_STBY as its entry.restart file $RFILE_ID $HOST_STBY" > $DW_SA_LOG/$TABLE_ID.multi_table_extract_resubmit.$(date '+%Y%m%d-%H%M%S').log
exit

