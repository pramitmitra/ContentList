#!/bin/ksh -eu
###################################################################################################################
###################################################################################################################
#
# Title:        Single Table Merge Run Spark
# File Name:    dw_infra.single_table_sp_merge_run.ksh
# Description:  Handler for Load-Merge Target Tables from staged data
# Developer:    Ryan Wong
# Created on:   2017-08-21
# Location:     $DW_MASTER_BIN
#
# Revision History
#
#  Date         Ver#   Modified By(Name)            Change and Reason for Change
# ----------    -----  ---------------------------  ----------------------------------------------------------
# 2017-08-21     0.1   Ryan Wong                    Initial
# 2017-09-12     0.2   Michael Weng                 Check and load data onto HDFS
###################################################################################################################

. $DW_MASTER_LIB/dw_etl_common_functions.lib

COMP_FILE=$DW_SA_TMP/$TABLE_ID.$BASENAME.complete
export UNIT_OF_WORK_FILE=$DW_SA_DAT/$TABLE_ID.$JOB_TYPE.uow.dat
export SPARK_CONF_SUFF=stm

if [ ! -f $COMP_FILE ]
then
     # COMP_FILE does not exist.  1st run for this processing period.
  FIRST_RUN=Y
else
  FIRST_RUN=N
fi

# Source the error message handling logic.  On failure, trap will send the contents of the PARENT_ERROR_FILE to the
# subject area designated email addresses.

. $DW_MASTER_LIB/message_handler

# Print standard environment variables
set +u
print_standard_env
set -u

print "
##########################################################################################################
#
# Beginning Single Table Merge SPARK Execution for ETL_ID: $ETL_ID JOB_ENV: $JOB_ENV  `date`
#
##########################################################################################################
"

if [ $FIRST_RUN = Y ]
then
     # Need to run the clean up process since this is the first run for the current processing period.

        print "Running dw_infra.loader_cleanup.ksh for JOB_ENV: $JOB_ENV, JOB_TYPE_ID: $JOB_TYPE_ID  `date`"
        LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.dw_infra.loader_cleanup.${UOW_APPEND}.$CURR_DATETIME.log

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


############################################################
# START OF SPARK MERGE
############################################################
### Check load environment is Spark
PROCESS=single_table_merge_spark
RCODE=`grepCompFile $PROCESS $COMP_FILE`

if [ $RCODE = 1 ]
then
  LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.single_table_merge.$ETL_ID.merge.sp${UOW_APPEND}.$CURR_DATETIME.log

  ### Check and load data onto HDFS if missing
  assignTagValue HD_MERGE_WORKING_PATH HD_MERGE_WORKING_PATH $ETL_CFG_FILE W ""

  if [[ -n ${HD_MERGE_WORKING_PATH:-""} ]]
  then
    STM_SA=${SUBJECT_AREA#*_}
    STM_HDFS_PATH=${HD_MERGE_WORKING_PATH}/${STM_SA}/${STM_MERGE_TABLE_ID}

    . $DW_MASTER_CFG/hadoop.login

    print "HADOOP_HOME2=$HADOOP_HOME2"
    set +e
    $HADOOP_HOME2/bin/hadoop fs -test -d $STM_HDFS_PATH
    rcode=$?
    set -e

    if [ $rcode = 0 ]
    then
      print "HDFS file already exists: $STORAGE_ENV:$STM_HDFS_PATH"

    else
      print "HDFS file is missing: $STORAGE_ENV:$STM_HDFS_PATH, loading from Tempo ..."

      if ! [[ -d $IN_DIR ]]
      then
        print "${0##*/}: INFRA_ERROR - File/path not found on Tempo: $IN_DIR"
        exit 4
      fi

      ### Cleanup previous failure copy HDFS directory
      ### Create HDFS directory as $STM_MERGE_TABLE_ID.incomplete
      set +e
      $HADOOP_HOME2/bin/hadoop fs -rm -r -skipTrash ${STM_HDFS_PATH}.incomplete
      $HADOOP_HOME2/bin/hadoop fs -mkdir -p ${STM_HDFS_PATH}.incomplete
      rcode=$?
      set -e

      if [ $rcode = 0 ]
      then
        ### Generate file list based on file name pattern
        export DATA_LIS_FILE=$DW_SA_TMP/$TABLE_ID.$JOB_TYPE_ID.stm_hdfs_check$UOW_APPEND
        > $DATA_LIS_FILE

        DATA_FILE_PATTERN="$IN_DIR/$STM_MERGE_TABLE_ID*.dat*"
        print "DATA_FILE_PATTERN is $DATA_FILE_PATTERN"

        for data_file_entry in `ls $DATA_FILE_PATTERN |grep -v ".record_count."`
        do
          print "$data_file_entry" >> $DATA_LIS_FILE
        done

        ### Copy files from Tempo to HDFS
        while read SOURCE_FILE_TMP 
        do
          set +e
          $HADOOP_HOME2/bin/hadoop fs -copyFromLocal $SOURCE_FILE_TMP ${STM_HDFS_PATH}.incomplete
          rcode=$?
          set -e

          if [ $rcode = 0 ]
          then
            print "Load completion of FILE: ${SOURCE_FILE_TMP}"
          else
            print "${0##*/}: INFRA_ERROR - Failure processing FILE: $SOURCE_FILE_TMP, $STORAGE_ENV:${STM_HDFS_PATH}.incomplete"
            exit 4
          fi
        done < $DATA_LIS_FILE

        ### Rename $STM_MERGE_TABLE_ID.incomplete to $STM_MERGE_TABLE_ID
        set +e
        $HADOOP_HOME2/bin/hadoop fs -mv ${STM_HDFS_PATH}.incomplete ${STM_HDFS_PATH}
        rcode=$?
        set -e

        if [ $rcode = 0 ]
        then
          print "Successfully load data from Tempo to HDFS"
          print "Tempo - $IN_DIR"
          print "HDFS  - $STORAGE_ENV:$STM_HDFS_PATH"
        else
          print "${0##*/}: INFRA_ERROR - Failed to rename HDFS directory"
          exit 4
        fi

      else
        print "${0##*/}: INFRA_ERROR - Failed to create directory on $STORAGE_ENV"
        exit 4
      fi
    fi

  else
    print "HD_MERGE_WORKING_PATH is not defined. HDFS file checking is skipped."
  fi

  set +e
  $DW_MASTER_BIN/dw_infra.runSparkSubmit.ksh $ETL_ID $JOB_ENV 0 > $LOG_FILE 2>&1
  rcode=$?
  set -e


  ############################################################
  # Adding code for log enhancements https://jirap.corp.ebay.com/browse/ADPO-138
  ############################################################
  SPARK_APP_ID=$(grep -m 1 "tracking URL" $LOG_FILE | awk -F"/" '{print $((NF-1))}')
  SPARK_APP_ID=${SPARK_APP_ID:-"NA"}

print "
##########################################################################################################
#
# Single Table Merge for ETL_ID: $ETL_ID:  Hadoop/Spark Log File Information  `date`
#
##########################################################################################################

SQL Summary Logs: ${SPARK_DEFAULT_FS:-"NA"}/user/${HD_USERNAME}/
Hadoop History Logs: ${HADOOP_HISTORY_LOG:-"NA"}/${SPARK_APP_ID}
Debug Wiki Location : ${ADPO_DEBUG_WIKI_LOG-"NA"}
"

  if [ $rcode != 0 ]
  then
    print "${0##*/}:  ERROR, see log file $LOG_FILE" >&2
    exit 4
  fi

  print "$PROCESS" >> $COMP_FILE

elif [ $RCODE = 0 ]
then
  print "$PROCESS already complete"
else
  exit $RCODE
fi

############################################################
# END OF SPARK MERGE
############################################################


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
#                                Touch Watchfile
#
#  This section creates the watch_file.  It is a non-repeatable process to avoid issues with restartability
#
#############################################################################################################

PROCESS=touch_watchfile
RCODE=`grepCompFile $PROCESS $COMP_FILE`

if [ $RCODE -eq 1 ]
 then
   WATCH_FILE=${ETL_ID}.${SPARK_CONF_SUFF}.done
   LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.$PROCESS${UOW_APPEND}.$CURR_DATETIME.log

   print "Running $DW_MASTER_EXE/touchWatchFile.ksh $ETL_ID $JOB_TYPE $JOB_ENV $WATCH_FILE $UOW_PARAM_LIST"
   set +e
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
##########################################################################################################
#
# Single Table Merge SPARK for ETL_ID: $ETL_ID JOB_ENV: $JOB_ENV complete   `date`
#
##########################################################################################################"

tcode=0
exit
