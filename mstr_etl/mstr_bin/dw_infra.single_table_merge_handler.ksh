#!/bin/ksh -eu
# Title:        Single Table Merge Handler
# File Name:    single_table_merge_handler.ksh
# Description:  Handler submitting a single table merge job.
# Developer:    Kevin Oaks 
# Created on:
# Location:     $DW_EXE
# Logic:
#
#
# Called by:    UC4/Unix
#
# Date           Ver#   Modified By(Name)            Change and Reason for Change
#-----------    -----  ---------------------------  ------------------------------
#
# 2017-08-17       .1   Kevin Oaks                   Initial shell
# 2017-08-18       .7   Kevin Oaks                   Completely tested sans run script call
# 2017-08-21       .8   Ryan Wong                    Fixing syntax errors
# 2017-08-22       .81  Kevin Oaks                   Added export for UOW_FROM/TO
# 2017-08-22       .82  Ryan Wong                    Add support for JOB_ENV=hd
# 2017-08-23       .85  Kevin Oaks                   prepended name with dw_infra.
# 2017-08-23       .89  Kevin Oaks                   Removed 'if' conditions around existence of
#                                                    UOW variables since initial job call already
#                                                    tests for those
# 2017-09-12       .9   Michael Weng                 Check and load data onto HDFS
# 2017-11-23       1.0  Pramit Mitra                 Hive Snapshot merge partition cleanup(DINT-1054) 
# 2017-12-01       1.1  Pramit Mitra                 Renaming DATE_RET_DAYS to STM_DATE_RET_DAYS
####################################################################################################

typeset -fu usage

function usage {
   print "Usage:  $0 <ETL_ID> <JOB_ENV> -f <UOW_FROM> -t <UOW_TO> [[ -p <PARAM_NAME1=PARAM_VALUE1> ] [ -p <PARAM_NAME2=PARAM_VALUE2> ] ... ]
   -- where JOB_ENV == (sp*|hd*|td*)"
}

. /dw/etl/mstr_cfg/etlenv.setup

export DWI_CALLED=$0
export DWI_CALLED_ARGS=${@:-""}
export DWI_WHOAMI=$(whoami)
export DWI_START_DATETIME=$(date '+%Y%m%d-%H%M%S')

set +u
print_header
set -u

if [[ $# -lt 6 || $2 != @(sp*|hd*|td*) || $3 != "-f" ||  $5 != "-t" ]]
then
   usage
   exit 4
fi


export ETL_ID=$1
export JOB_ENV=$2
shift 2

export SCRIPTNAME=${0##*/}
export BASENAME=${SCRIPTNAME%.*}

export INPUT_DML=$ETL_ID.merge.read.dml

export JOB_TYPE=merge
export JOB_TYPE_ID=mrg

. /dw/etl/mstr_cfg/etlenv.setup

export UOW_FROM=""
export UOW_TO=""
export UOW_FROM_FLAG=0
export UOW_TO_FLAG=0

print "Processing Options"
while getopts "f:t:p:" opt
do
   case $opt in
      f ) if [ $UOW_FROM_FLAG -ne 0 ]
          then
            print "FATAL ERROR: -f flag specified more than once" >&2
            exit 8
          fi
          print "Setting UOW_FROM_FLAG == 1"
          UOW_FROM_FLAG=1
          print "Setting UOW_FROM == $OPTARG"
          UOW_FROM=$OPTARG;;
      t ) if [ $UOW_TO_FLAG -ne 0 ]
          then
            print "FATAL ERROR: -t flag specified more than once" >&2
            exit 8
          fi
          print "Setting UOW_TO_FLAG == 1"  
          UOW_TO_FLAG=1
          print "Setting UOW_TO == $OPTARG"
          UOW_TO=$OPTARG;;
      p ) if [[ ${OPTARG%=*} = $OPTARG ]]
          then
             print "${0##*/}: ERROR, parameter definition $OPTARG is not of form <PARAM_NAME=PARAM_VALUE>"
             usage
             exit 4
          fi
          print "Exporting $OPTARG"
          export $OPTARG;;
      \? ) usage 
           exit 1 ;;
   esac
done
shift $(($OPTIND - 1))

# Setup common definitions
. $DW_MASTER_CFG/dw_etl_common_defs.cfg

# read in UC4 runtime parameters, they will be used in teradata query band if it is an extract job from teradata
export UC4_JOB_NAME=${UC4_JOB_NAME:-"NA"}
export UC4_PRNT_CNTR_NAME=${UC4_PRNT_CNTR_NAME:-"NA"}
export UC4_TOP_LVL_CNTR_NAME=${UC4_TOP_LVL_CNTR_NAME:-"NA"};
export UC4_JOB_RUN_ID=${UC4_JOB_RUN_ID:-"NA"}
export UC4_JOB_BATCH_MODE=${UC4_JOB_BATCH_MODE:-"NA"}
export UC4_JOB_PRIORITY=${UC4_JOB_PRIORITY:-"NA"}
export QB_STR_UC4="UC4_JOB_NAME=${UC4_JOB_NAME};UC4_PRNT_CNTR_NAME=${UC4_PRNT_CNTR_NAME};UC4_TOP_LVL_CNTR_NAME=${UC4_TOP_LVL_CNTR_NAME};UC4_JOB_RUN_ID=${UC4_JOB_RUN_ID};UC4_JOB_BATCH_MODE=${UC4_JOB_BATCH_MODE};UC4_JOB_PRIORITY=${UC4_JOB_PRIORITY}"

export QUERY_BAND_STRING="SA=$SUBJECT_AREA;TBID=$TABLE_ID;$QB_STR_UC4; UPDATE"

# Get IN_DIR from etl cfg
assignTagValue IN_DIR IN_DIR $ETL_CFG_FILE W $DW_IN
export IN_DIR=$IN_DIR/$JOB_ENV/$SUBJECT_AREA

# Modify DW_SA_LOG to a date dir based on CURR_DATETIME
if [[ ${DW_SA_LOG##*/} != $CURR_DATE ]]
then
  export DW_SA_LOG=$DW_SA_LOG/$TABLE_ID/$CURR_DATE
  if [[ ! -d $DW_SA_LOG ]]
  then
     set +e
     mkdir -pm 0775 $DW_SA_LOG
     set -e
  fi
fi

# Calculate UOW values - Should this should turn into an externally called function?
export UOW_APPEND=""
export UOW_PARAM_LIST=""
export UOW_PARAM_LIST_AB=""
UOW_APPEND=.$UOW_TO
UOW_PARAM_LIST="-f $UOW_FROM -t $UOW_TO"
UOW_PARAM_LIST_AB="-UOW_FROM $UOW_FROM -UOW_TO $UOW_TO"
is_valid_ts $UOW_FROM
is_valid_ts $UOW_TO
. $DW_MASTER_CFG/dw_etl_common_defs_uow.cfg
assignTagValue LOAD_UOW_FROM_DATE_RFMT_CODE LOAD_UOW_FROM_DATE_RFMT_CODE $ETL_CFG_FILE W 0
assignTagValue LOAD_UOW_TO_DATE_RFMT_CODE LOAD_UOW_TO_DATE_RFMT_CODE $ETL_CFG_FILE W 0
export UOW_FROM_DATE_RFMT=$($DW_MASTER_EXE/dw_infra.reformat_date.ksh $UOW_FROM_DATE $LOAD_UOW_FROM_DATE_RFMT_CODE)
export UOW_TO_DATE_RFMT=$($DW_MASTER_EXE/dw_infra.reformat_date.ksh $UOW_TO_DATE $LOAD_UOW_TO_DATE_RFMT_CODE)
export UOW_DATE=$UOW_TO_DATE

assignTagValue STM_MERGE_TABLE_ID STM_MERGE_TABLE_ID $ETL_CFG_FILE W $TABLE_ID
export STM_MERGE_TABLE_ID
export IN_DIR=$IN_DIR/$STM_MERGE_TABLE_ID/$UOW_TO_DATE/$UOW_TO_HH/$UOW_TO_MI/$UOW_TO_SS

# export REC_CNT_IN_DIR=$REC_CNT_IN_DIR/$TABLE_ID/$UOW_TO_DATE/$UOW_TO_HH/$UOW_TO_MI/$UOW_TO_SS
export UOW_REC_CNT_IN_DIR=$IN_DIR

# Add run script for both load and merge
export JOB_ENV_PREFIX=$(print $JOB_ENV | cut -c1-2)
print "JOB_ENV_PREFIX is $JOB_ENV_PREFIX"
if [[ $JOB_ENV_PREFIX != @(td||sp||hd) ]]
then
  print "${0##*/}: FATAL ERROR: $JOB_ENV not supported"
  usage
  exit 5
fi

RUN_SCRIPTNAME=dw_infra.single_table_${JOB_ENV_PREFIX}_merge_run.ksh
export SQL_FILE=$ETL_ID.merge.$JOB_ENV_PREFIX.sql
if [[ $JOB_ENV_PREFIX == hd ]]
then
  RUN_SCRIPTNAME=dw_infra.single_table_sp_merge_run.ksh
  export SQL_FILE=$ETL_ID.merge.sp.sql
fi

SQL_FILE_BASENAME=${SQL_FILE##*/}
export SQL_FILE_BASENAME=${SQL_FILE_BASENAME%.*}

export PARENT_ERROR_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.dw_infra.single_table_${JOB_ENV_PREFIX}_merge_run${UOW_APPEND}.$CURR_DATETIME.err
export PARENT_LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.dw_infra.single_table_${JOB_ENV_PREFIX}_merge_run${UOW_APPEND}.$CURR_DATETIME.log

# Comp File Handler
HANDLER_COMP_FILE=$DW_SA_TMP/$TABLE_ID.$JOB_TYPE.handler.complete
if [[ ! -f $HANDLER_COMP_FILE ]]
then
   touch $HANDLER_COMP_FILE
fi

######################################################################################################
#
#                                Infra Run
#
#  This secion runs the main job.
#  It is now in a non-repeatable section to avoid issues of restartability.
#
######################################################################################################

PROCESS=Infra_Run
RCODE=`grepCompFile $PROCESS $HANDLER_COMP_FILE`

RUN_RCODE=0
if [[ $RCODE -eq 1 ]]
then
   print "Redirecting output to $PARENT_LOG_FILE"
   set +e
   (time $DW_MASTER_BIN/$RUN_SCRIPTNAME) > $PARENT_LOG_FILE 2>&1
   RUN_RCODE=$?
   print "Value of RUN_RCODE for $PROCESS = $RUN_RCODE"
   export DWI_END_DATETIME=$(date '+%Y%m%d-%H%M%S')
   print "$DWI_INFRA_IND DWI_END_DATETIME=$DWI_END_DATETIME" >> $PARENT_LOG_FILE
   set -e

   ## Added explicit failure logic, in case of non zero INFRA_RUN return
   if [[ $RUN_RCODE -ne 0 ]]
   then
      print "$PROCESS does not complete. Please check Spark RM Log to debug"
      exit 1;
    fi   

   if [[ $RUN_RCODE -eq 0 ]]
   then
      if [[ -f $DW_SA_TMP/$TABLE_ID.$JOB_TYPE.complete ]]
      then
      print "$PROCESS does not complete. Please try to reset the job."
      RUN_RCODE=8
      ######Additing exit statement to fail STM if Infra Run fails######
      exit 4;
    else
      print "$PROCESS" >> $HANDLER_COMP_FILE
      print "$PROCESS successfully complete"
    fi
   fi

elif [[ $RCODE -eq 0 ]]
then
   print "$PROCESS already complete"
else
   exit $RCODE
fi


######################################################################################################
#
#                                Hive Snapshot merge partition cleanup
#
#  This secion runs Hive Snapshot merge partition cleanup.
#  The process will look for following two values from ETL_ID.cfg file. It is exhibit following properties 
#  ( 1 ) if 1st variable "STM_DATE_RET_DAYS" is not available then default to 0. It means all HIVE partitions will be deleted
#  except latest UOW_TO date partion. IF "INF" value is specified as STM_DATE_RET_DAYS then Purge process will be ignored.
#  ( 2 ) if 2nd variable "MERGE_TABLE" is not available then Purge process will be ignored.
######################################################################################################

PROCESS=Hive_Snapshot_Merge_Partition
RCODE=`grepCompFile $PROCESS $HANDLER_COMP_FILE`

RUN_RCODE=0
if [[ $RCODE -eq 1 ]]
then
    print "Redirecting Hive_Snapshot_Merge_Partition output to $PARENT_LOG_FILE"
    assignTagValue STM_DATE_RET_DAYS STM_DATE_RET_DAYS $ETL_CFG_FILE W 0
    assignTagValue MERGE_TABLE MERGE_TABLE $ETL_CFG_FILE W NOT_ASSIGNED
    if [[ $STM_DATE_RET_DAYS == 'INF' ]]
        then
        print "Use has defined STM_DATE_RET_DAYS as INF, so ignoring the purge process"
        print "$PROCESS" >> $HANDLER_COMP_FILE
        print "$PROCESS IGNORED!!!"
    elif [[ $MERGE_TABLE == 'NOT_ASSIGNED' ]]
        then
        print "MERGE_TABLE Value is Not set, so skipping Hive_Snapshot_Merge_Partition process"
        print "$PROCESS" >> $HANDLER_COMP_FILE
    else
        print "Process Eligible for Hive_Snapshot_Merge_Partition"
        print "Value of STM_DATE_RET_DAYS == $STM_DATE_RET_DAYS"
        print "Value of MERGE_TABLE == $MERGE_TABLE"
    $DW_MASTER_BIN/dw_infra.hive_snapshot_merge_partition_cleanup.ksh $ETL_ID $JOB_ENV $UOW_TO $MERGE_TABLE $STM_DATE_RET_DAYS > ${PARENT_LOG_FILE}_hive_snapshot_merge_partition.log 2>&1
        RUN_RCODE=$?
        print "Value of RUN_RCODE=$RUN_RCODE"
            if [[ $RUN_RCODE -eq 0 ]]
                then
                print "$PROCESS" >> $HANDLER_COMP_FILE
                print "$PROCESS successfully complete"
            else
                print "$PROCESS does not complete. Please try to reset the job."
            fi
    fi
  elif [[ $RCODE -eq 0 ]]
      then
      print "$PROCESS already complete"
   else
   exit $RCODE
 fi
######################################################################################################
#
#                                Infra Log Copy
#
#  This secion copies the Infra_Run log for processing
######################################################################################################

print "Start $DW_MASTER_BIN/dw_infra.handler_log_copy.ksh"
set +e
################
#echo "This is commented for testing"
#$DW_MASTER_BIN/dw_infra.handler_log_copy.ksh
LOGCOPY_RCODE=$?

if [[ $LOGCOPY_RCODE -ne 0 ]]
then
   print "Failure for log copy, sending email to dw_infra SAE"
   email_subject="$servername: INFO: Infra Handler Log Copy Failed"
   email_body="The associated Parent Log File was not copied: $PARENT_LOG_FILE"
   grep "^dw_infra\>" $DW_CFG/subject_area_email_list.dat | read PARAM EMAIL_ERR_GROUP
   print $email_body | mailx -s "$email_subject" $EMAIL_ERR_GROUP
fi
set -e
print "End $DW_MASTER_BIN/dw_infra.handler_log_copy.ksh"


######################################################################################################
# Only Fail Handler if Infra_Run fails
######################################################################################################
if [[ $RUN_RCODE -ne 0 ]]
then
   print "FATAL ERROR:  See log file $PARENT_LOG_FILE"
   exit $RUN_RCODE
fi

print "Removing the handler complete file  `date`"
rm -f $HANDLER_COMP_FILE

print "Load Complete"

exit 0
