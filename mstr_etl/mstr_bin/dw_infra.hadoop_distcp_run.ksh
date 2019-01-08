#!/bin/ksh -eu
# Title:        Hadoop Distcp Run
# File Name:    dw_infra.hadoop_distcp_run.ksh
# Description:  Handle submiting a distcp job for hadoop
# Developer:    Ryan Wong
# Created on:   2018-11-29
# Location:     $DW_MASTER_BIN
# Logic:
#
#
# Called by:    UC4/Unix
#
# Date           Ver#   Modified By(Name)            Change and Reason for Change
#-----------    -----  ---------------------------  ---------------------------------------------------------
#
# 2018-11-29      1.0   Ryan Wong                    Initial
###################################################################################################################

. $DW_MASTER_LIB/dw_etl_common_functions.lib

COMP_FILE=$DW_SA_TMP/$TABLE_ID.$JOB_TYPE.complete

if [[ ! -f $COMP_FILE ]]
then
	# COMP_FILE does not exist.  1st run for this processing period.
	FIRST_RUN=Y
	> $COMP_FILE
else
	FIRST_RUN=N
fi

#-------------------------------------------------------------------------------------
# Source the error message handling logic.  On failure, trap will send the contents
# of the PARENT_ERROR_FILE to the subject area designated email addresses.
#-------------------------------------------------------------------------------------
. $DW_LIB/message_handler

# Print standard environment variables
set +u
print_standard_env
set -u

print "
####################################################################################################################
#
# Beginning Hadoop Distcp for ETL_ID: $ETL_ID, Unit of Work: $UOW_TO `date`
#
####################################################################################################################
"


###################################################################################################################
# Logic to call distcp submit
###################################################################################################################
PROCESS=hadoop_distcp_submit
RCODE=`grepCompFile $PROCESS $COMP_FILE`

if [ $RCODE = 1 ]
then

   LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.${PROCESS}${UOW_APPEND}.$CURR_DATETIME.log

   set +e
   $DW_MASTER_BIN/dw_infra.hadoop_distcp_submit.ksh > $LOG_FILE 2>&1
   rcode=$?
   set -e

   if [ $rcode != 0 ]
   then
      print "${0##*/}:  ERROR, Running dw_infra.hadoop_distcp_submit.ksh, see log file $LOG_FILE" >&2
      exit 4
   fi

   print "$PROCESS" >> $COMP_FILE
	
elif [ $RCODE = 0 ]
then
   print "$PROCESS process already complete"
else
   print "${0##*/}:  ERROR, Unable to grep for $PROCESS in $COMP_FILE"
   exit $RCODE
fi

#############################################################################################################
#
#                                Finalize Processing
#
#  This section creates the watch_file.  It is a non-repeatable process to avoid issues with restartability
#
#############################################################################################################

PROCESS=finalize_processing
RCODE=`grepCompFile $PROCESS $COMP_FILE`

if [ $RCODE = 1 ]
then
   WATCH_FILE=$ETL_ID.$JOB_TYPE.done
   LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.touchWatchFile${UOW_APPEND}.$CURR_DATETIME.log
   print "Running $DW_MASTER_EXE/touchWatchFile.ksh $ETL_ID $JOB_TYPE $JOB_ENV $WATCH_FILE $UOW_PARAM_LIST"

   set _e
   $DW_MASTER_EXE/touchWatchFile.ksh $ETL_ID $JOB_TYPE $JOB_ENV $WATCH_FILE $UOW_PARAM_LIST > $LOG_FILE 2>&1
   rcode=$?
   set -e

   if [ $rcode -ne 0 ]
   then
      print "${0##*/}:  ERROR, see log file $LOG_FILE" >&2
      exit 4
   fi

   WATCH_FILE=$ETL_ID.$JOB_TYPE.$UOW_TO.done
   print "Running $DW_MASTER_EXE/touchWatchFile.ksh $ETL_ID $JOB_TYPE $JOB_ENV $WATCH_FILE $UOW_PARAM_LIST"

   set _e
   $DW_MASTER_EXE/touchWatchFile.ksh $ETL_ID $JOB_TYPE $JOB_ENV $WATCH_FILE $UOW_PARAM_LIST >> $LOG_FILE 2>&1
   rcode=$?
   set -e

   if [ $rcode -ne 0 ]
   then
      print "${0##*/}:  ERROR, see log file $LOG_FILE" >&2
      exit 4
   fi

   print "$PROCESS" >> $COMP_FILE

elif [ $RCODE = 0 ]
then
   print "$PROCESS process already complete"
else
   exit $RCODE
fi

print "Removing the complete file  `date`"

rm -f $COMP_FILE

print "${0##*/}:  INFO, 
####################################################################################################################
#
# Hadoop Distcp for ETL_ID: $ETL_ID, Unit of Work: $UOW_TO complete `date`
#
####################################################################################################################"

tcode=0
exit
