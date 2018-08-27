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
# 2017-10-26     0.3   Michael Weng                 Add parallel copy feature from etl to hdfs
# 2017-11-29     0.3   Ryan Wong                    Add touch file with standard naming ETL_ID.JOB_TYPE.done.UOW_TO
# 2017-12-28     0.4   Michael Weng                 Add optional partition to HD_MERGE_WORKING_PATH
# 2017-12-28     0.5   Michael Weng                 Additional check if HD_MERGE_WORKING_PATH is empty
# 2018-02-15     0.6   Michael Weng                 Optional overwrite when loading from etl to hdfs
# 2018-06-06     0.7   Michael Weng                 Sync git-repo to the version on ETL PROD
# 2018-05-29     0.8   Michael Weng                 Enable optional empty folder check on HDFS
# 2018-06-12     0.9   Michael Weng                 Update dw_infra.multi_etl_to_hdfs_copy.ksh command line
# 2018-06-22     1.0   Michael Weng                 Support SA variable overwrite
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
    assignTagValue STM_SA HD_MERGE_WORKING_SA $ETL_CFG_FILE W "${SUBJECT_AREA#*_}"
    STM_HDFS_PATH=${HD_MERGE_WORKING_PATH}/${STM_SA}/${STM_MERGE_TABLE_ID}

    # STT output directory could be partitioned
    assignTagValue STT_OUTPUT_PARTITION_COL STT_OUTPUT_PARTITION_COL $ETL_CFG_FILE W ""
    assignTagValue HD_MERGE_WORKING_OVERWRITE HD_MERGE_WORKING_OVERWRITE $ETL_CFG_FILE W ""
    assignTagValue STM_ENABLE_EMPTY_CHECK STM_ENABLE_EMPTY_CHECK $ETL_CFG_FILE W 0

    if [[ -n ${STT_OUTPUT_PARTITION_COL:-""} ]]
    then
      STM_HDFS_PATH=${STM_HDFS_PATH}/$(eval print ${STT_OUTPUT_PARTITION_COL})
    fi

    . $DW_MASTER_CFG/hadoop.login

    print "HADOOP_HOME2=$HADOOP_HOME2"
    set +e
    $HADOOP_HOME2/bin/hadoop fs -test -d $STM_HDFS_PATH
    rcode=$?
    set -e

    LOAD_FROM_TEMPO=1
    if [ $rcode = 0 ]
    then
      LOAD_FROM_TEMPO=0
      if [ $STM_ENABLE_EMPTY_CHECK != 0 ]
      then
        STM_HDFS_SIZE=$($HADOOP_HOME2/bin/hadoop fs -du -s $STM_HDFS_PATH | awk '{print $1;}')
        if [ $STM_HDFS_SIZE = 0 ]
        then
          LOAD_FROM_TEMPO=1
        fi
      fi
    fi

    HD_OVERWRITE=0
    if [[ -n ${HD_MERGE_WORKING_OVERWRITE:-""} ]]
    then
      for HD_ENV in $(echo ${HD_MERGE_WORKING_OVERWRITE} | sed "s/,/ /g")
      do
        if [[ $HD_ENV = $JOB_ENV ]]
        then
          HD_OVERWRITE=1
          break
        fi
      done
    fi

    if [[ $LOAD_FROM_TEMPO = 0 && $HD_OVERWRITE = 0 ]]
    then
      print "HDFS file already exists and no overwrite specified: $STORAGE_ENV:$STM_HDFS_PATH"

    else
      print "HDFS file is either missing or overwrite is specified: $STORAGE_ENV:$STM_HDFS_PATH, loading from Tempo ..."

      if ! [[ -d $IN_DIR ]]
      then
        print "${0##*/}: INFRA_ERROR - File/path not found on Tempo: $IN_DIR"
        exit 4
      fi

      print "Cleaning up target HDFS folder before loading: $STORAGE_ENV:$STM_HDFS_PATH"
      set +e
      $HADOOP_HOME2/bin/hadoop fs -rm -r -skipTrash $STM_HDFS_PATH > /dev/null 2>&1
      set -e

      ### Load data into a temporary hdfs directory. Upon success, rename it.
      UOW_TO_FLAG=1
      LOAD_LOG_FILE=$DW_SA_LOG/$STM_MERGE_TABLE_ID.$JOB_TYPE_ID.multi_etl_to_hdfs.$ETL_ID.load.sp${UOW_APPEND}.$CURR_DATETIME.log
      set +e
      $DW_MASTER_BIN/dw_infra.multi_etl_to_hdfs_copy.ksh $ETL_ID $STORAGE_ENV $IN_DIR $STM_MERGE_TABLE_ID ${STM_HDFS_PATH}_incomplete $STM_MERGE_TABLE_ID $UOW_TO_FLAG > $LOAD_LOG_FILE 2>&1
      rcode=$?
      set -e

      if [ $rcode = 0 ]
      then
        print "Successfully loaded data from Tempo:"
        print "    Source ($IN_DIR)"
        print "    Destination ($STORAGE_ENV:${STM_HDFS_PATH}_incomplete)"
        print "    Log file: $LOAD_LOG_FILE"

        ### Rename $STM_MERGE_TABLE_ID_incomplete to $STM_MERGE_TABLE_ID
        . $DW_MASTER_CFG/hadoop.login
        set +e
        $HADOOP_HOME2/bin/hadoop fs -mv ${STM_HDFS_PATH}_incomplete ${STM_HDFS_PATH}
        rcode=$?
        set -e

        if [ $rcode = 0 ]
        then
          print "Successfully rename ${STM_HDFS_PATH}_incomplete to ${STM_HDFS_PATH} on $STORAGE_ENV"
        else
          print "${0##*/}: INFRA_ERROR - Failed to rename HDFS directory"
          exit 4
        fi
      else
        print "${0##*/}: INFRA_ERROR - Failed to load data from Tempo, see log file: $LOAD_LOG_FILE"
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
   WATCH_FILE=$ETL_ID.$JOB_TYPE.done
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

   WATCH_FILE=${ETL_ID}.${SPARK_CONF_SUFF}.done

   print "Running $DW_MASTER_EXE/touchWatchFile.ksh $ETL_ID $JOB_TYPE $JOB_ENV $WATCH_FILE $UOW_PARAM_LIST"
   set +e
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
##########################################################################################################
#
# Single Table Merge SPARK for ETL_ID: $ETL_ID JOB_ENV: $JOB_ENV complete   `date`
#
##########################################################################################################"

tcode=0
exit
