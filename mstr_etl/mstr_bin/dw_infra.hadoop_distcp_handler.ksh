#!/bin/ksh -eu
# Title:        Hadoop Distcp Handler
# File Name:    dw_infra.hadoop_distcp_handler.ksh
# Description:  Handler submitting a hadoop distcp job
# Developer:    Ryan Wong
# Created on:
# Location:     $DW_MASTER_BIN
# Logic:
#
#
# Called by:    UC4/Unix
#
# Date           Ver#   Modified By(Name)            Change and Reason for Change
#-----------    -----  ---------------------------  ---------------------------------------------------------
#
# 2018-11-29      0.1   Ryan Wong                    Initial
###################################################################################################################

typeset -fu usage

function usage {
   print "Usage:  $0 <ETL_ID> <JOB_ENV> -s <SOURCE_URI> -d <TARGET_URI> [OPTION]
   -- where JOB_ENV == (sp*|hd*)
   -- where SOURCE_URI|TARGET_ URI == hdx|spx:/path/to/file/or/dir
   OPTION...
     -f <UOW_FROM>, format is 'YYYYMMDD24hrmmss', must be paired with -t
     -t <UOW_TO>, format is 'YYYYMMDD24hrmmss', must be paired with -f
     -e <JOB_ENV>, Set the environment which the distcp shall launch from. Default, use target env
     -r, Set to remove target data prior to data copy.  Default, target is not removed
     -q <QUEUE>, Specify queue, default uses HD_QUEUE
     -m <NUM>, Number of mappers, default 100
     -p key=value, Format for variable export (must specify -p for each key=value pair)
   "
}

. /dw/etl/mstr_cfg/etlenv.setup

export DWI_CALLED=$0
export DWI_CALLED_ARGS=${@:-""}
export DWI_WHOAMI=$(whoami)
export DWI_START_DATETIME=$(date '+%Y%m%d-%H%M%S')

set +u
print_header
set -u

if [[ $# -lt 2 || $2 != @(sp*|hd*) ]]
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

export JOB_TYPE=hdcopy
export JOB_TYPE_ID=hdc

. /dw/etl/mstr_cfg/etlenv.setup

export UOW_FROM=""
export UOW_TO=""
export UOW_FROM_FLAG=0
export UOW_TO_FLAG=0
export TARGET_DELETE=0
export SOURCE_URI=""
export TARGET_URI=""
SUBMIT_HD_ENV=$JOB_ENV

print "Processing Options"
while getopts "f:t:s:d:e:q:m:p:r" opt
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
      s ) print "Setting SOURCE_URI == $OPTARG"
          SOURCE_URI=$OPTARG;;
      d ) print "Setting TARGET_URI == $OPTARG"
          TARGET_URI=$OPTARG;;
      e ) print "Setting SUBMIT_HD_ENV == $OPTARG"
          export SUBMIT_HD_ENV=$OPTARG;;
      q ) print "Setting HD_QUEUE_OPTION == $OPTARG"
          export HD_QUEUE_OPTION=$OPTARG;;
      m ) print "Setting MAPPER_CNT_OPTION == $OPTARG"
          export MAPPER_CNT_OPTION=$OPTARG;;
      r ) print "Setting TARGET_DELETE == 1"
          TARGET_DELETE=1;;
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

# Validity check for SOURCE_URI and TARGET_URI
if [[ 'X'$SOURCE_URI == 'X' || 'X'$TARGET_URI == 'X' ]]
then
  print "${0##*/}: ERROR, SOURCE_URI and TARGET_URI are required"
  usage
  exit 5
fi

if ! print ${SOURCE_URI} | egrep '[hd|sp]{1,1}[0-9]+:' >/dev/null 2>&1
then
    print "${0##*/}: ERROR, Invalid URI Format"
    usage
    exit 4
fi

if ! print ${TARGET_URI} | egrep '[hd|sp]{1,1}[0-9]+:' >/dev/null 2>&1
then
    print "${0##*/}: ERROR, Invalid URI Format"
    usage
    exit 4
fi

export SOURCE_HD_ENV=$(print ${SOURCE_URI%%:*})
export SOURCE_HD_PATH=${SOURCE_URI##*:}
export TARGET_HD_ENV=$(print ${TARGET_URI%%:*})
export TARGET_HD_PATH=${TARGET_URI##*:}

print "SOURCE_HD_ENV == $SOURCE_HD_ENV"
print "SOURCE_HD_PATH == $SOURCE_HD_PATH"
print "TARGET_HD_ENV == $TARGET_HD_ENV"
print "TARGET_HD_PATH == $TARGET_HD_PATH"

# Validity check SUBMIT_HD_ENV
if [[ $SUBMIT_HD_ENV != @(hd*|sp*) ]]
then
    print "${0##*/}: ERROR, Invalid value for SUBMIT_HD_ENV = $SUBMIT_HD_ENV"
    usage
    exit 4
fi


# Calculate UOW values - Should this should turn into an externally called function?
export UOW_APPEND=""
export UOW_PARAM_LIST=""
export UOW_PARAM_LIST_AB=""
if [[ $UOW_FROM_FLAG -eq 1 && $UOW_TO_FLAG -eq 1 ]]
then
   UOW_APPEND=.$UOW_TO
   UOW_PARAM_LIST="-f $UOW_FROM -t $UOW_TO"
   UOW_PARAM_LIST_AB="-UOW_FROM $UOW_FROM -UOW_TO $UOW_TO"
   is_valid_ts $UOW_FROM
   is_valid_ts $UOW_TO
   . $DW_MASTER_CFG/dw_etl_common_defs_uow.cfg
   assignTagValue UOW_FROM_DATE_RFMT_CODE UOW_FROM_DATE_RFMT_CODE $ETL_CFG_FILE W 0
   assignTagValue UOW_TO_DATE_RFMT_CODE UOW_TO_DATE_RFMT_CODE $ETL_CFG_FILE W 0
   export UOW_FROM_DATE_RFMT=$($DW_MASTER_EXE/dw_infra.reformat_date.ksh $UOW_FROM_DATE $UOW_FROM_DATE_RFMT_CODE)
   export UOW_TO_DATE_RFMT=$($DW_MASTER_EXE/dw_infra.reformat_date.ksh $UOW_TO_DATE $UOW_TO_DATE_RFMT_CODE)
   export UOW_DATE=$UOW_TO_DATE
elif [[ ($UOW_FROM_FLAG -eq 1 && $UOW_TO_FLAG -eq 0) || ($UOW_TO_FLAG -eq 1 && $UOW_FROM_FLAG -eq 0) ]]
then
   print "Missing required UOW (FROM or TO) parameter."
   usage
   exit 1
fi

export PARENT_ERROR_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.${BASENAME}${UOW_APPEND}.$CURR_DATETIME.err
export PARENT_LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.dw_infra.hadoop_distcp_run${UOW_APPEND}.$CURR_DATETIME.log

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
   (time $DW_MASTER_BIN/dw_infra.hadoop_distcp_run.ksh) > $PARENT_LOG_FILE 2>&1
   RUN_RCODE=$?
   print "Value of RUN_RCODE for $PROCESS = $RUN_RCODE"
   export DWI_END_DATETIME=$(date '+%Y%m%d-%H%M%S')
   print "$DWI_INFRA_IND DWI_END_DATETIME=$DWI_END_DATETIME" >> $PARENT_LOG_FILE
   set -e

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
#                                Infra Log Copy
#
#  This secion copies the Infra_Run log for processing
######################################################################################################

print "Start $DW_MASTER_BIN/dw_infra.handler_log_copy.ksh"
set +e
$DW_MASTER_BIN/dw_infra.handler_log_copy.ksh
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

print "Hadoop Distcp Complete"

exit 0
