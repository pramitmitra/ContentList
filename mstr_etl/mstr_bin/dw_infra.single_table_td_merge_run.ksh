#!/bin/ksh -eu
# Title:        Single Table TD Merge Run
# File Name:    dw_infra.single_table_td_merge_run.ksh
# Description:  Handle submiting a single table merge job for Teradata.
# Developer:    Kevin Oaks 
# Created on:   2017-08-21
# Location:     $DW_MASTER_BIN
# Logic:
#
#
# Called by:    UC4/Unix
#
# Date          Ver#   Modified By(Name)            Change and Reason for Change
#-----------    -----  ---------------------------  ------------------------------
# 2017-08-21     .7    Kevin Oaks                   Copied and altered from single_table_merge_load_run.ksh 
# 2017-08-22     .8    Ryan Wong                    Adding target_transform (merge) component
# 2017-08-23     .9    Kevin Oaks                   Pre-dev testing version
# 2018-08-29     .95   Kevin Oaks                   Added INPUT_FILE_LIST existence/empty check
####################################################################################################

. $DW_MASTER_LIB/dw_etl_common_functions.lib

COMP_FILE=$DW_SA_TMP/$TABLE_ID.$JOB_TYPE.complete
export UNIT_OF_WORK_FILE=$DW_SA_DAT/$TABLE_ID.$JOB_TYPE.uow.dat

if [[ ! -f $COMP_FILE ]]
then
	# COMP_FILE does not exist.  1st run for this processing period.
	FIRST_RUN=Y
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
# Beginning single table td merge for ETL_ID: $ETL_ID, Unit of Work: $UOW_TO `date`
#
####################################################################################################################
"

if [ $FIRST_RUN = Y ]
then
  # Need to run the clean up process since this is the first run for the current processing period.
  # We will need to add an explicit path for merge in dw_infra.loader_cleanup.ksh
  # We will need to differentiate td and sp merge cleanup so they don't step on each other
  print "Running dw_infra.loader_cleanup.ksh for JOB_ENV: $JOB_ENV, JOB_TYPE_ID: $JOB_TYPE_ID  `date`"
  LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.dw_infra.loader_cleanup${UOW_APPEND}.$CURR_DATETIME.log

  set +e
  $DW_MASTER_BIN/dw_infra.loader_cleanup.ksh $JOB_ENV $JOB_TYPE_ID > $LOG_FILE 2>&1
  rcode=$?
  set -e

  if [ $rcode != 0 ]
  then
    print "${0##*/}:  ERROR, see log file $LOG_FILE" >&2
    exit 4
  fi

  > $COMP_FILE

else
  print "dw_infra.loader_cleanup.ksh process already complete"
fi

###################################################################################################################
#       Create Input File List
#       No COMP_FILE entry - build from current dir contents on restart
###################################################################################################################

export INPUT_FILE_LIST=$DW_SA_TMP/$TABLE_ID.merge.ld.lis

if [[ -f $INPUT_FILE_LIST ]]
then
  rm $INPUT_FILE_LIST
fi

for fn in $IN_DIR/*
do
  if [[ -f $fn ]]
  then

    tfn=${fn%%.dat}
    tfn=${tfn##*.}
   
    if [[ $tfn != 'record_count' ]]
    then
      print $fn >> $INPUT_FILE_LIST
    fi
  fi
done

if [[ ! -s $INPUT_FILE_LIST ]]
then
   print "${0##*/}:  ERROR, Generated $INPUT_FILE_LIST does not exist or is empty. Check for existence of data files in $IN_DIR" >&2
   exit 4
fi

###################################################################################################################
#	Data File Loading process
###################################################################################################################

PROCESS=single_table_merge_load
RCODE=`grepCompFile $PROCESS $COMP_FILE`

if [ $RCODE = 1 ]
then
		
  print "Processing single table merge load for TABLE_ID: $TABLE_ID  `date`"
		
  LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.single_table_merge_load${UOW_APPEND}.$CURR_DATETIME.log
  UTILITY_TABLE_LOGFILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.utility_load${UOW_APPEND}.$CURR_DATETIME.log

  set +e
  $DW_MASTER_EXE/dw_infra.single_table_td_merge_load.ksh -ETL_ID $ETL_ID -JOB_ENV $JOB_ENV -INPUT_DML_FILENAME $INPUT_DML -INPUT_FILE_LIST $INPUT_FILE_LIST $UOW_PARAM_LIST_AB > $LOG_FILE 2>&1
  rcode=$?
  set -e
		
  if [ $rcode != 0 ]
  then
    print "${0##*/}:  ERROR running $DW_MASTER_EXE/dw_infra.single_table_td_merge_load.ksh. See log file $LOG_FILE" >&2
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

######################################################################################################
# Single Table Load Run has a code snippet here for checking for rejected records.
# Will need to be modified for Single Table TD Merge Run if it is needed, but should go
# here prior to merge 
######################################################################################################


######################################################################################################
# stage to final table merge
######################################################################################################
PROCESS=target_transform
RCODE=`grepCompFile $PROCESS $COMP_FILE`

if [ $RCODE = 1 ]
then
  print "Processing target transform SQL Runner for TABLE_ID: $TABLE_ID  `date`"

  # determine which database we are using through the DBC file
  export JOB_ENV_UPPER
  CFG_DBC_PARAM=$(JOB_ENV_UPPER=$(print $JOB_ENV | tr [:lower:] [:upper:]); eval print ${JOB_ENV_UPPER}_DBC)
  DEFAULT_DB_NAME=$(JOB_ENV_UPPER=$(print $JOB_ENV | tr [:lower:] [:upper:]); eval print teradata_\$DW_${JOB_ENV_UPPER}_DB)

  set +e
  DB_NAME=$(grep "^$CFG_DBC_PARAM\>" $DW_CFG/${ETL_ID}.cfg | read PARAM VALUE PARAM_COMMENT; eval print ${VALUE:-$DEFAULT_DB_NAME})
  rcode=$?
  set -e
  if [ $rcode != 0 ]
  then
    DB_NAME=$DEFAULT_DB_NAME
  fi

  set +e
  DB_TYPE=$(grep "^dbms\>" $DW_DBC/${DB_NAME}.dbc | tr [:lower:] [:upper:] | read PARAM VALUE PARAM_COMMENT; print ${VALUE:-0})
  rcode=$?
  set -e

  if [ $rcode != 0 ]
  then
    print "${0##*/}:  ERROR, Failure determining dbms value from $DW_DBC/${DB_NAME}.dbc" >&2
    exit 4
  fi

  LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.target_transform.${SQL_FILE_BASENAME}${UOW_APPEND}.$CURR_DATETIME.log

  if [[ $DB_TYPE == "ORACLE" || $DB_TYPE == "MSSQL" || $DB_TYPE == "MYSQL" ]]
  then
    print "${0##*/}:  ERROR, DB_TYPE: $DB_TYPE not supported at this time"
    exit 8
  else
    set +e
    $DW_MASTER_BIN/dw_infra.runTDSQL.ksh $ETL_ID $JOB_ENV $SQL_FILE $UOW_PARAM_LIST > $LOG_FILE 2>&1
    rcode=$?
    set -e
  fi

  if [ $rcode != 0 ]
  then
    print "${0##*/}:  ERROR, see log file $LOG_FILE" >&2
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


######################################################################################################
#
#                                Increment Unit of Work 
#
#  This section updates the Unit of Work once all processing is complete.
#  Note: This was made non-repeatable for restartability purposes with Batch Seq Num
#        but that may not apply to Unit of Work. If that proves to be the case, we can
#        remove the check in the compfile and just write it fresh every time.
######################################################################################################

PROCESS=Update_UOW
RCODE=`grepCompFile $PROCESS $COMP_FILE`

if [ $RCODE = 1 ]
then

   print "Updating the $DW_SA_DAT Unit of Work file `date`"
   print $UOW_TO > $UNIT_OF_WORK_FILE
   print "$PROCESS" >> $COMP_FILE

elif [ $RCODE = 0 ]
then
   print "$PROCESS already complete"
else
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

   WATCH_FILE=$ETL_ID.$JOB_TYPE.$UOW_TO.done
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
# Single Table TD Merge for ETL_ID: $ETL_ID, Unit of Work: $UOW_TO complete `date`
#
####################################################################################################################"

tcode=0
exit
