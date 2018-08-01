#!/bin/ksh -eu
###################################################################################################################
###################################################################################################################
#
# Title:        Target Table Load Run Spark
# File Name:    target_table_load_run_spark.ksh
# Description:  Handler for Loading Target Tables from staged data
# Developer:    Pramit Mitra
# Created on:   Legacy code re-purposed on 2017-03-01
# Location:     $DW_MASTER_BIN
#
# Usage Notes: UOW_TO and UOW_FROM work in tandem. If one is passed in, then the other must be also.
# Revision History
#
#  Date         Ver#   Modified By(Name)            Change and Reason for Change
# ----------    -----  ---------------------------  ----------------------------------------------------------
# 2017-03-01     2.0   Pramit Mitra                 Extended the code for Spark Submit added JOB_ENV=sp1
# 2017-04-04     2.1   Pramit Mitra                 Deriving SQLFILE & CFG File from ETL_ID 
# 2017-06-02     2.1   Pramit Mitra                 Watch File name according to BaseScript Name(STT / TTM)
# 2017-06-06     2.2   Michael Weng                 Extract STT working tables back to ETL
# 2017-06-15     2.1   Pramit Mitra                 Adding log file enhancements adpo-138
# 2017-06-22     2.3   Pramit Mitra                 HDFS File copy back to ETL only for STT adpo-207
# 2017-07-20     2.4   Michael Weng                 Differentiate STE and STT
# 2017-08-24     2.5   Pramit Mitra                 Removing .<cluster>_env.sh as part of new design for STM 
# 2017-09-13     2.6   Michael Weng                 Add second touch file for hd#
# 2017-09-27     2.7   Pramit Mitra                 DINT-993: COUNT Logic modification, Complete file scope extension
# 2017-10-10     2.8   Michael Weng                 Add support for sp* on hdfs copy back to ETL
# 2017-10-13     2.9   Michael Weng                 Create second touch file on STORAGE_ENV
# 2017-10-24     3.0   Ryan Wong                    Add parallel copy feature for hdfs copy back to ETL
# 2018-02-15     3.1   Michael Weng                 Cleanup local storage before copying back from hdfs
# 2018-02-26     3.2   Michael Weng                 Optional local storage purge based on UOW
# 2018-05-14     3.4   Michael Weng                 Enable STT to pull staging data from ETL
# 2018-07-02     3.6   Michael Weng                 Enable STT template without transformation
###################################################################################################################

. $DW_MASTER_LIB/dw_etl_common_functions.lib

COMP_FILE=$DW_SA_TMP/$TABLE_ID.$BASENAME.${SQL_FILE_BASENAME}${UC4_JOB_NAME_APPEND}.complete

if [ ! -f $COMP_FILE ]
then
     # COMP_FILE does not exist.  1st run for this processing period.
  FIRST_RUN=Y
else
  FIRST_RUN=N
fi

## Setting Touch File name based on Spark Handler type
if [[ ${BASENAME} == target_table_merge_handler ]]
   then
   export TFILE_SUFF=ttm
   elif [[ ${BASENAME} == single_table_transform_handler ]]
   then
   export TFILE_SUFF=stt
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
# Beginning SparkSQL Execution for ETL_ID: $ETL_ID   `date`
#
##########################################################################################################
"
if [ $FIRST_RUN = Y ]
then
     # Need to run the clean up process since this is the first run for the current processing period.

        print "Running dw_infra.loader_cleanup.ksh for JOB_ENV: $JOB_ENV, JOB_TYPE_ID: $JOB_TYPE_ID  `date`"
        LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.dw_infra.loader_cleanup.${SQL_FILE_BASENAME}${UOW_APPEND}.$CURR_DATETIME.log

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


######################################################################################################
#
#                                Pre target table load processing
#
#  Jobs that need processes executed or variables set before the target table load process runs
#  are handled here.
#
#  To run a pre target table load process, set the TRGT_TBL_LD_PRE_PROC_LIS tag in the
#  $DW_CFG/$ETL_ID.cfg file equal to the pre process list containing the processes to be run.
#
#  This handler will then loop through the specified file and serially evaluate each entry in
#  the file.
#
#  This allows for great flexibility since now either pre processes can be run or list files
#  containing variables and/or functions to export into the environment called that the
#  subsequently executing SQL can leverage.
#
#  To accomodate this flexibility it's understood that some processes would need to re-run even in
#  a restart scenario, while others may not need to run. The entries in in the pre process list
#  must follow the convention of COMP_REC_FLAG COMMAND, where COMP_REC_FLAG determines whether or
#  not the process is stored in the complete flag. A value of 1 indicates yes, therefore if the
#  process completes and there is a subsequent failure, the process will not run again on restart.
#  A value of 0 indicates that this process must run each time the handler runs for that ETL_ID.
#  Always use this in the case of exporting variables, as they will not persist from run to run.
#
######################################################################################################

set +e
grep "^TRGT_TBL_LD_PRE_PROC_LIS\>" $DW_CFG/$ETL_ID.cfg | read PARAM TRGT_TBL_LD_PRE_PROC_LIS PARAM_COMMENT
rcode=$?
TRGT_TBL_LD_PRE_PROC_LIS=$(eval print $TRGT_TBL_LD_PRE_PROC_LIS)
set -e

if [ $rcode != 0 ]
then
     print "No pre-process list exists for this job"
else
  while read COMP_REC_FLAG COMMAND
  do

         set +e
         grep -s "$COMMAND" $COMP_FILE >/dev/null
         rcode=$?
         set -e

        if [ $rcode = 1 ]
        then

           print "Running Pre Process $COMMAND `date`"
           LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.preproccess${UOW_APPEND}.$CURR_DATETIME.log

           set +e
           eval $COMMAND >> $LOG_FILE 2>&1
           rcode=$?
           set -e

          if [ $rcode != 0 ]
          then
          print "${0##*/}:  ERROR, see log file $LOG_FILE" >&2
          exit 4
          fi

         if [ $COMP_REC_FLAG = 1 ]
         then
         print "$COMMAND" >> $COMP_FILE
         fi

        elif [ $rcode = 0 ]
         then
         print "$COMMAND already complete"
         continue
         else
         print "${0##*/}: ERROR, $COMP_FILE does not exist."
         exit $rcode
        fi

      done < $TRGT_TBL_LD_PRE_PROC_LIS
fi

######################################################################################################
#
#                                STT without Transformation
#
#   Allow user to launch STT without real transformation. If STT_DISABLE_TRANSFORMATION is set, create
#   UOW working table folder on ETL and link data files to the staging table. Subsequent processings
#   will be skipped except for the post processing.
#
#   STT_DISABLE_TRANSFORMATION   # Any non-zero to disable the transformation. Default is 0.
#   STT_WORKING_TABLES           # Only one working table be specifed. A 1:1 mapping to staging table.
#
######################################################################################################
assignTagValue STT_DISABLE_TRANSFORMATION STT_DISABLE_TRANSFORMATION $ETL_CFG_FILE W 0

if [[ ${BASENAME} == single_table_transform_handler ]] && [[ $STT_DISABLE_TRANSFORMATION != 0 ]]
then
  PROCESS=stt_transformation_check
  RCODE=`grepCompFile $PROCESS $COMP_FILE`

  if [ $RCODE -eq 1 ]
  then
    assignTagValue IN_DIR IN_DIR $ETL_CFG_FILE W $DW_IN
    assignTagValue STT_WORKING_TABLES STT_WORKING_TABLES $ETL_CFG_FILE W ""

    if [[ -n ${STT_WORKING_TABLES:-""} ]]
    then
      print "STT transformation is disabled. Create working table on ETL and link data files"

      if [[ X"$UOW_TO" = X ]]
      then
        print "${0##*/}:  FATAL ERROR, UOW_TO is not defined."
        exit 4
      fi

      ETL_STAGING_DIR=$IN_DIR/extract/${SUBJECT_AREA}/${TABLE_ID}/$UOW_TO_DATE/$UOW_TO_HH/$UOW_TO_MI/$UOW_TO_SS
      ETL_WORKING_DIR=$IN_DIR/extract/${SUBJECT_AREA}/${STT_WORKING_TABLES}/$UOW_TO_DATE/$UOW_TO_HH/$UOW_TO_MI/$UOW_TO_SS

      if ! [[ -d $ETL_STAGING_DIR ]]
      then
        print "${0##*/}: INFRA_ERROR - File/path not found on Tempo ($ETL_STAGING_DIR)"
        exit 4
      fi

      mkdirifnotexist $ETL_WORKING_DIR
      for data_file in `ls $ETL_STAGING_DIR/*.*`
      do
        /bin/ln $data_file $ETL_WORKING_DIR/$(basename $data_file)
      done

      # Populate <ETL_ID>.stt.done file on all JOB_ENVs being setup
      LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.touchWatchFile${UOW_APPEND}.$PROCESS.$CURR_DATETIME.log
      for job_env in $JOB_ENVS
      do
        print "Create done file ${ETL_ID}.${TFILE_SUFF}.done on $job_env"
        $DW_MASTER_EXE/touchWatchFile.ksh $ETL_ID $JOB_TYPE $job_env ${ETL_ID}.${TFILE_SUFF}.done $UOW_PARAM_LIST >> $LOG_FILE 2>&1
      done

    else
      print "${0##*/}:  FATAL ERROR, STT transformation is disabled but STT_WORKING_TABLES is empty."
      exit 4
    fi

  elif [ $RCODE -eq 0 ]
  then
    print "$PROCESS already complete"
  else
    exit $RCODE
  fi
fi

######################################################################################################
#
#                                STT pull processing
#
#   STT_ENABLE_PULL_MODE         # Enable loading data from ETL if staging tables not existed
#   STT_STAGING_PATH             # STT Staging Base Path on HDFS. Default is $gdw_workingPath
#   STT_STAGE_MAPPING            # Comma separated tables list in the form of
#                                  <dw_sa1>.<table1>:<sa1>.<stg_table1>,<dw_sa2>.<table2>:<sa2>.<stg_table2>
#
######################################################################################################
if [[ ${BASENAME} == single_table_transform_handler ]] && [[ $STT_DISABLE_TRANSFORMATION = 0 ]]
then
  PROCESS=stt_copy_from_etl
  RCODE=`grepCompFile $PROCESS $COMP_FILE`

  if [ $RCODE -eq 1 ]
  then
    assignTagValue IN_DIR IN_DIR $ETL_CFG_FILE W $DW_IN
    assignTagValue STT_ENABLE_PULL_MODE STT_ENABLE_PULL_MODE $ETL_CFG_FILE W 0
    assignTagValue STT_STAGING_PATH STT_STAGING_PATH $ETL_CFG_FILE W $gdw_workingPath
    assignTagValue STT_STAGE_MAPPING STT_STAGE_MAPPING $ETL_CFG_FILE W ""

    if [[ $STT_ENABLE_PULL_MODE != 0 ]] && [[ -n ${STT_STAGE_MAPPING:-""} ]]
    then
      if [[ X"$UOW_TO" = X ]]
      then
        print "${0##*/}:  FATAL ERROR, UOW_TO is not defined."
        exit 4
      fi

      STT_WAIT_FOR_DATA_LIS=$DW_SA_TMP/$TABLE_ID.$BASENAME.stt_copy_from_etl.$UOW_TO.$(date '+%Y%m%d-%H%M%S')
      > $STT_WAIT_FOR_DATA_LIS

      for FIELD in $(echo $STT_STAGE_MAPPING | sed "s/,/ /g")
      do
        ETL_SA_DOT_TABLE=${FIELD%%:*}
        STT_SA_DOT_TABLE=${FIELD##*:}
        ETL_SA=${ETL_SA_DOT_TABLE%%.*}
        ETL_TABLE=${ETL_SA_DOT_TABLE##*.}
        STT_SA=${STT_SA_DOT_TABLE%%.*}
        STT_TABLE=${STT_SA_DOT_TABLE##*.}

        ETL_DIR=${IN_DIR}/extract/${ETL_SA}/${ETL_TABLE}/$UOW_TO_DATE/$UOW_TO_HH/$UOW_TO_MI/$UOW_TO_SS
        STT_DIR=${STT_STAGING_PATH}/${STT_SA}/${STT_TABLE}/$UOW_TO_DATE/$UOW_TO_HH/$UOW_TO_MI/$UOW_TO_SS
        STT_SA_DIR=${STT_STAGING_PATH}/${STT_SA}
        STT_DIR_LOCK=${STT_STAGING_PATH}/${STT_SA}/${STT_TABLE}.${PROCESS}.${UOW_TO}.lock

        . $DW_MASTER_CFG/hadoop.login

        set +e
        $HADOOP_HOME2/bin/hadoop fs -mkdir -p $STT_SA_DIR
        $HADOOP_HOME2/bin/hadoop fs -test -d $STT_DIR
        rcode=$?
        set -e

        LOAD_FROM_TEMPO=1
        if [ $rcode = 0 ]
        then
          LOAD_FROM_TEMPO=0
        fi

        if [[ $LOAD_FROM_TEMPO = 0 ]]
        then
          print "HDFS file already exists ($STORAGE_ENV:$STT_DIR)"
          continue
        fi

        print "HDFS file is missing ($STORAGE_ENV:$STT_DIR)"
        if ! [[ -d $ETL_DIR ]]
        then
          print "${0##*/}: INFRA_ERROR - File/path not found on Tempo ($ETL_DIR)"
          exit 4
        fi

        set +e
        $HADOOP_HOME2/bin/hadoop fs -mkdir $STT_DIR_LOCK
        rcode=$?
        set -e

        if [ $rcode != 0 ]
        then
          print "Another job is loading data onto ($STORAGE_ENV:$STT_DIR)"
          print $STT_DIR >> $STT_WAIT_FOR_DATA_LIS
          continue
        fi

        print "Loading data from Tempo onto ($STORAGE_ENV:$STT_DIR)"

        ### Load data into a temporary hdfs directory. Upon success, rename it.
        UOW_TO_FLAG=1
        LOAD_LOG_FILE=$DW_SA_LOG/$STT_SA_DOT_TABLE.$JOB_TYPE_ID.multi_etl_to_hdfs.$ETL_ID.load.sp${UOW_APPEND}.$CURR_DATETIME.log
        set +e
        $DW_MASTER_BIN/dw_infra.multi_etl_to_hdfs_copy.ksh $ETL_ID $STORAGE_ENV $ETL_DIR $ETL_TABLE ${STT_DIR}_incomplete $STT_TABLE $UOW_TO_FLAG > $LOAD_LOG_FILE 2>&1
        rcode=$?
        set -e

        if [ $rcode != 0 ]
        then
          set +e
          $HADOOP_HOME2/bin/hadoop fs -rm -r -skipTrash ${STT_DIR}_incomplete > /dev/null 2>&1
          $HADOOP_HOME2/bin/hadoop fs -rmdir $STT_DIR_LOCK > /dev/null 2>&1
          set -e

          print "${0##*/}: INFRA_ERROR - Failed to load data from Tempo, see log file: $LOAD_LOG_FILE"
          exit 4
        fi

        print "Successfully loaded data from Tempo:"
        print "    Source ($ETL_DIR)"
        print "    Destination ($STORAGE_ENV:${STT_DIR}_incomplete)"
        print "    Log file: $LOAD_LOG_FILE"

        set +e
        $HADOOP_HOME2/bin/hadoop fs -mv ${STT_DIR}_incomplete ${STT_DIR}
        rcode=$?
        set -e

        if [ $rcode = 0 ]
        then
          print "Successfully rename ${STT_DIR}_incomplete to ${STT_DIR} on $STORAGE_ENV"
          set +e
          $HADOOP_HOME2/bin/hadoop fs -rmdir $STT_DIR_LOCK > /dev/null 2>&1
          set -e
        else
          set +e
          $HADOOP_HOME2/bin/hadoop fs -rm -r -skipTrash ${STT_DIR}_incomplete > /dev/null 2>&1
          $HADOOP_HOME2/bin/hadoop fs -rmdir $STT_DIR_LOCK > /dev/null 2>&1
          set -e

          print "${0##*/}: INFRA_ERROR - Failed to rename HDFS directory"
          exit 4
        fi

      done

      if [ -s $STT_WAIT_FOR_DATA_LIS ]
      then
        while read STT_WAIT_FOR_DATA
        do
          while true
          do
            print "Check every 60sec waiting for data to be ready: $STT_WAIT_FOR_DATA"

            . $DW_MASTER_CFG/hadoop.login
            set +e
            $HADOOP_HOME2/bin/hadoop fs -test -d $STT_WAIT_FOR_DATA
            rcode=$?
            set -e

            if [ $rcode = 0 ]; then break; fi

            sleep 60
          done
        done < $STT_WAIT_FOR_DATA_LIS
      fi

      print $PROCESS >> $COMP_FILE

    else
      print "STT_ENABLE_PULL_MODE is not set, or STT_STAGE_MAPPING is empty."
    fi

  elif [ $RCODE -eq 0 ]
  then
    print "$PROCESS already complete"
  else
    exit $RCODE
  fi
fi

######################################################################################################
#
#                                Launching Spark Job
#
######################################################################################################
if [[ $STT_DISABLE_TRANSFORMATION = 0 ]]
then
set +e
grep -s "target_table_load" $COMP_FILE >/dev/null
RCODE=$?
set -e

## Consolidate SP* and HD* JOB_ENV as per new design - pmitra - 08/24/2017
   
   HADOOP_JAR=$SQL_FILE
   HADOOP_JAR_BASENAME=${HADOOP_JAR##*/}
   export HADOOP_JAR_BASENAME=${HADOOP_JAR_BASENAME%.*}
   set +u
   if [[ -n $MAIN_CLASS ]]
   then
     CLASS_APPEND=.$MAIN_CLASS
   else
     CLASS_APPEND=""
   fi
   set -u
   LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.target_table_load.${HADOOP_JAR_BASENAME}${CLASS_APPEND}${UOW_APPEND}.$CURR_DATETIME.log

   if [ $RCODE = 1 ]
   then
     set +e
     PARAM_LIST=${PARAM_LIST:-""}
     $DW_MASTER_BIN/dw_infra.runSparkJob.ksh $ETL_ID $JOB_ENV $BASENAME > $LOG_FILE 2>&1
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
# Target table load for ETL_ID: $ETL_ID:  Hadoop/Spark Log File Information  `date`
#
##########################################################################################################

SQL Summary Logs: ${SPARK_DEFAULT_FS:-"NA"}/user/${HD_USERNAME}/
Hadoop History Logs: ${HADOOP_HISTORY_LOG:-"NA"}/${SPARK_APP_ID}
Debug Wiki Location : ${ADPO_DEBUG_WIKI_LOG:-"NA"}
"
     if [ $rcode != 0 ]
     then
     print "${0##*/}:  ERROR, see log file $LOG_FILE" >&2
     exit 4
     fi

    print "target_table_load_spark" >> $COMP_FILE

    elif [ $RCODE = 0 ]
    then
    print "target_table_load process already complete"
    else
    exit $RCODE
 fi
fi

######################################################################################################
#
#                                Post target table load processing
#
#  Jobs that need processes executed or variables set after the target table load process runs
#  are handled here.
#
#  To run a post target table load process, set the TRGT_TBL_LD_POST_PROC_LIS tag in the
#  $DW_CFG/$ETL_ID.cfg file equal to the post process list containing the processes to be run.
#
#  This handler will then loop through the specified file and serially evaluate each entry in
#  the file.
#
#  To accomodate this flexibility it's understood that some processes would need to re-run even in
#  a restart scenario, while others may not need to run. The entries in in the post process list
#  must follow the convention of COMP_REC_FLAG COMMAND, where COMP_REC_FLAG determines whether or
#  not the process is stored in the complete flag. A value of 1 indicates yes, therefore if the
#  process completes and there is a subsequent failure, the process will not run again on restart.
#  A value of 0 indicates that this process must run each time the handler runs for that ETL_ID.
#  Always use this in the case of exporting variables, as they will not persist from run to run.
#
######################################################################################################

set +e
grep "^TRGT_TBL_LD_POST_PROC_LIS\>" $DW_CFG/$ETL_ID.cfg | read PARAM TRGT_TBL_LD_POST_PROC_LIS PARAM_COMMENT
rcode=$?
TRGT_TBL_LD_POST_PROC_LIS=$(eval print $TRGT_TBL_LD_POST_PROC_LIS)
set -e

if [ $rcode != 0 ]
then
     print "No post-process list exists for this job"
else
  while read COMP_REC_FLAG COMMAND
  do

     set +e
     grep -s "$COMMAND" $COMP_FILE >/dev/null
     rcode=$?
     set -e

     if [ $rcode = 1 ]
     then

       print "Running Post Process $COMMAND `date`"
       LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.postproccess${UOW_APPEND}.$CURR_DATETIME.log

       set +e
       eval $COMMAND >> $LOG_FILE 2>&1
       rcode=$?
       set -e

       if [ $rcode != 0 ]
          then
          print "${0##*/}:  ERROR, see log file $LOG_FILE" >&2
          exit 4
       fi

       if [ $COMP_REC_FLAG = 1 ]
         then
         print "$COMMAND" >> $COMP_FILE
       fi

       elif [ $rcode = 0 ]
     then
         print "$COMMAND already complete"
         continue
     else
         print "${0##*/}: ERROR, $COMP_FILE does not exist."
         exit $rcode
     fi

     done < $TRGT_TBL_LD_POST_PROC_LIS
fi

######################################################################################################
#
#                                Touch Watch Files
#
######################################################################################################
 PROCESS=touch_watchfile
 RCODE=`grepCompFile $PROCESS $COMP_FILE`

 if [ $RCODE -eq 1 ]
 then
    LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.$PROCESS.${SQL_FILE_BASENAME}${UOW_APPEND}.$CURR_DATETIME.log
    TFILE_NAME=${SQL_FILE_BASENAME}.done

    print "Touching Watchfile $TFILE_NAME$UOW_APPEND"

   set +e
   ##$DW_MASTER_EXE/touchWatchFile.ksh $ETL_ID $JOB_TYPE $JOB_ENV $TFILE_NAME $UOW_PARAM_LIST > $LOG_FILE 2>&1
   $DW_MASTER_EXE/touchWatchFile.ksh $ETL_ID $JOB_TYPE $JOB_ENV ${ETL_ID}.${TFILE_SUFF}.done $UOW_PARAM_LIST > $LOG_FILE 2>&1
   rcode=$?
   $DW_MASTER_EXE/touchWatchFile.ksh $ETL_ID $JOB_TYPE $(eval print \$${JOB_ENV_UPPER}_STORAGE) ${ETL_ID}.${TFILE_SUFF}.done $UOW_PARAM_LIST >> $LOG_FILE 2>&1
   rcode2=$?
   set -e

   if [[ $rcode -ne 0 || $rcode2 -ne 0 ]]
   then
        print "${0##*/}:  ERROR, see log file $LOG_FILE" >&2
        exit 4
   fi

  print $PROCESS >> $COMP_FILE

 elif [ $RCODE -eq 0 ]
 then
 print "$PROCESS already complete"
 else
  exit $RCODE
fi



######################################################################################################
#
#                                ADPO: copy result to ETL
#
#  If the parameter STT_WORKING_SOURCE in $DW_CFG/$ETL_ID.cfg is set to [hd1|hd2|hd3|...], data files 
#  generated from STT job will be copied back to ETL in $DW_IN following the standard extract data 
#  location.
#
#    STT_WORKING_SOURCE   # used to determine whether and which hadoop cluster to source from
#    STT_WORKING_PATH     # the working table HDFS path
#    STT_WORKING_TABLES   # the working tables
#
######################################################################################################
if [[ $STT_DISABLE_TRANSFORMATION = 0 ]]
then
PROCESS=hdfs_etl_copy
RCODE=`grepCompFile $PROCESS $COMP_FILE`

if [ $RCODE -eq 1 ]
then
    if [[ ${BASENAME} == single_table_transform_handler ]]
    then
      assignTagValue STT_WORKING_SOURCE STT_WORKING_SOURCE $ETL_CFG_FILE W ""

      set +eu
      if [[ -n ${STT_WORKING_SOURCE:-""} ]] && [[ $STT_WORKING_SOURCE == @(hd*|sp*) ]]
      then
          print "${0##*/}:  STT_WORKING_SOURCE is:  $STT_WORKING_SOURCE"
          export HDFS_CLUSTER=$(JOB_ENV_UPPER=$(print $STT_WORKING_SOURCE | tr [:lower:] [:upper:]); eval print \$DW_${JOB_ENV_UPPER}_DB)
          if ! [[ -f $DW_MASTER_CFG/.${HDFS_CLUSTER}_env.sh ]]
          then
            print "${0##*/}:  FATAL ERROR:  Environment file not found:   $DW_MASTER_CFG/.${HDFS_CLUSTER}_env.sh" >&2
            exit 4
          fi

          assignTagValue IN_DIR IN_DIR $ETL_CFG_FILE W $DW_IN
          assignTagValue STT_WORKING_PATH STT_WORKING_PATH $ETL_CFG_FILE
          assignTagValue STT_WORKING_TABLES STT_WORKING_TABLES $ETL_CFG_FILE
          assignTagValue STT_LOCAL_OVERWRITE STT_LOCAL_OVERWRITE $ETL_CFG_FILE W 1
          assignTagValue STT_LOCAL_RETENTION STT_LOCAL_RETENTION $ETL_CFG_FILE W 0

          STT_SA=${SUBJECT_AREA#*_}

          for TABLE in $(echo $STT_WORKING_TABLES | sed "s/,/ /g")
          do
            export STT_TABLE=$TABLE
            export ETL_DIR=${IN_DIR}/extract/${SUBJECT_AREA}
            export SOURCE_PATH=${STT_WORKING_PATH}/${STT_SA}/${TABLE}

            if [[ X"$UOW_TO" != X ]]
            then
              TABLE_DIR=$ETL_DIR/$TABLE
              ETL_DIR=$ETL_DIR/$TABLE/$UOW_TO_DATE/$UOW_TO_HH/$UOW_TO_MI/$UOW_TO_SS

              if [[ $STT_LOCAL_RETENTION -gt 0 ]]
              then
                print "Cleanup local storage based on retention days specified: $STT_LOCAL_RETENTION on table: $TABLE"
                DEL_DATE=$($DW_EXE/add_days $UOW_TO_DATE -${STT_LOCAL_RETENTION})
                for FOLDER in $TABLE_DIR/{8}-([0-9])
                do
                  FOLDER_NUMBER=$(basename $FOLDER)
                  if [[ -d $FOLDER ]] && [[ $FOLDER_NUMBER -lt $DEL_DATE ]]
                  then
                    print "Cleaning up STT UOW LOCAL WORKING TABLE: $FOLDER"
                    rm -rf $FOLDER
                  fi
                done
              fi
            fi

            if [ $STT_LOCAL_OVERWRITE != 0 ]
            then
              print "Cleaning up local storage before copy back from hdfs on $servername: $ETL_DIR"
              rm -rf $ETL_DIR
            fi

            LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.dw_infra.parallel_hdfs_to_etl_copy${UOW_APPEND}.$STT_TABLE.$CURR_DATETIME.log
            print "Copy from HDFS is started. Source: ${SOURCE_PATH}, Destination: ${ETL_DIR}"
            print "Log file: $LOG_FILE"

            set +e
            $DW_MASTER_BIN/dw_infra.parallel_hdfs_to_etl_copy.ksh > $LOG_FILE 2>&1
            rcode=$?
            set -e

            if [ $rcode != 0 ]
            then
              print "${0##*/}:  FATAL ERROR, running dw_infra.parallel_hdfs_to_etl_copy.ksh.  See log file $LOG_FILE" >&2
              exit 4
            fi

            print "
###############################################################################
# Copy from HDFS for ETL_ID: $ETL_ID - `date`
#   HDFS  - $HDFS_CLUSTER:$SOURCE_PATH
#   LOCAL - $ETL_DIR
###############################################################################"

          done # Loop through each STT_WORKING_TABLES

          # Creating Done file after HDFS file copy 
          LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.touchWatchFile${UOW_APPEND}.stt_hdfs_copy_success.$CURR_DATETIME.log
          $DW_MASTER_EXE/touchWatchFile.ksh $ETL_ID $JOB_TYPE $STT_WORKING_SOURCE ${ETL_ID}.stt_HDFS_Copy_Success.done $UOW_PARAM_LIST > $LOG_FILE 2>&1
      else
          print "Warning : ADPO: HDFS file copy not done as STT_WORKING_SOURCE value is not hd " 
      fi
  else
    print "Warning : ADPO: HDFS file copy to ETL host can't be performed for ${BASENAME}" 
  fi   

print $PROCESS >> $COMP_FILE

 elif [ $RCODE -eq 0 ]
    then
    print "$PROCESS already complete"
  else
   exit $RCODE
fi
fi

print "Removing the complete file  `date`"
rm -f $COMP_FILE

print "
##########################################################################################################
#
# Target table load for ETL_ID: $ETL_ID complete   `date`
#
##########################################################################################################"

tcode=0
exit
