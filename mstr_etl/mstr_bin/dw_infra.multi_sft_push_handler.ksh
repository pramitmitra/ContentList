#!/bin/ksh -eu
####################################################################################################
# Title:        Multi SFT Push Handler
# File Name:    dw_infra.multi_sft_push_handler.ksh
# Description:  Handle multiple sft push
# Developer:    ???
# Created on:
# Location:     $DW_MASTER_EXE
# Logic:
#
#
# Called by:    UC4/Unix
#
# Date         Ver#   Modified By(Name)            Change and Reason for Change
#---------    -----  ---------------------------  ------------------------------
# ????-??-??   1.0    ???                          Initital
# 2012-11-26   2.0    Ryan Wong                    Add UOW_[FROM/TO]_TIME, SFT_UOW_[FROM/TO]_DATE_RFMT_CODE
# 2013-03-25   2.1    Jacky Shen                   Add a check to Infra_Run complete file to fail job if the complete file still exists
# 2013-04-25   2.2    Ryan Wong                    Update OUT_DIR for UOW directory path to include UOW HH/MM/SS
# 2013-07-16   2.3    Ryan Wong                    Update UOW variable definition to use $DW_MASTER_EXE/dw_etl_common_defs_uow.cfg
# 2013-10-04   2.4    Ryan Wong                    Redhat changes
####################################################################################################

typeset -fu usage

function usage {
   print "Usage:  $0 <ETL_ID> <JOB_ENV> [ -f <UOW_FROM> -t <UOW_TO> ]"
}

if [[ $# -lt 2 ]]
then
        usage
        exit 4
fi

export ETL_ID=$1
export JOB_ENV=$2             # dual-active database environment (primary or secondary)
export JOB_TYPE=sft_push
export JOB_TYPE_ID=sft_push

export SUBJECT_AREA=${ETL_ID%%.*}
export TABLE_ID=${ETL_ID##*.}

. /dw/etl/mstr_cfg/etlenv.setup
. /dw/etl/mstr_cfg/dw_etl_common_defs.cfg
. $DW_MASTER_LIB/dw_etl_common_abinitio_functions.lib


export DWI_CALLED=$0
export DWI_CALLED_ARGS=${@:-""}
export DWI_WHOAMI=$(whoami)
export DWI_START_DATETIME=$(date '+%Y%m%d-%H%M%S')

set +u
print_header
set -u

shift 2

# Check for optional UOW
export UOW_FROM=""
export UOW_TO=""
export UOW_FROM_FLAG=0
export UOW_TO_FLAG=0

# getopts loop for processing optional args including UOW
print "Processing Options"
while getopts "f:t:p:" opt
do
   case $opt in
      f ) if [[ $UOW_FROM_FLAG -ne 0 ]]
          then
             print "Fatal Error: -f flag specified more than once" >&2
             usage
             exit 4
          fi
          print "Setting UOW_FROM_FLAG == 1"
          UOW_FROM_FLAG=1
          print "Setting UOW_FROM == $OPTARG"
          UOW_FROM=${OPTARG};;
      t ) if [[ $UOW_TO_FLAG -ne 0 ]]
          then
             print "FATAL ERROR: -t flag specified more than once" >&2
             usage
             exit 4
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
           exit 1;;
   esac
done
shift $((OPTIND - 1))

# Get IN_DIR from etl cfg
assignTagValue OUT_DIR OUT_DIR $ETL_CFG_FILE
export OUT_DIR=$OUT_DIR/$JOB_ENV/$SUBJECT_AREA
export DW_SA_OUT=$OUT_DIR

# Check if IN_DIR point to a MFS folder
if [[ ${OUT_DIR} != ${OUT_DIR%mfs*} ]]
then
   export REC_CNT_IN_DIR=$DW_SA_OUT
else
   export REC_CNT_IN_DIR=$OUT_DIR
fi

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

# Calculate UOW values
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
   assignTagValue SFT_UOW_FROM_DATE_RFMT_CODE SFT_UOW_FROM_DATE_RFMT_CODE $ETL_CFG_FILE W 0
   assignTagValue SFT_UOW_TO_DATE_RFMT_CODE SFT_UOW_TO_DATE_RFMT_CODE $ETL_CFG_FILE W 0
   export UOW_FROM_DATE_RFMT=$($DW_MASTER_EXE/dw_infra.reformat_date.ksh $UOW_FROM_DATE $SFT_UOW_FROM_DATE_RFMT_CODE)
   export UOW_TO_DATE_RFMT=$($DW_MASTER_EXE/dw_infra.reformat_date.ksh $UOW_TO_DATE $SFT_UOW_TO_DATE_RFMT_CODE)
   export UOW_DATE=$UOW_TO_DATE
   export OUT_DIR=$OUT_DIR/$TABLE_ID/$UOW_DATE/$UOW_TO_HH/$UOW_TO_MI/$UOW_TO_SS
   export DW_SA_OUT=$OUT_DIR
   # Check if IN_DIR point to a MFS folder
   if [[ ${OUT_DIR} != ${OUT_DIR%mfs*} ]]
   then
     m_mkdirifnotexist $OUT_DIR
   else
     mkdirifnotexist $OUT_DIR
   fi
   export REC_CNT_IN_DIR=$REC_CNT_IN_DIR/$TABLE_ID/$UOW_DATE
   mkdirifnotexist $REC_CNT_IN_DIR
elif [[ ($UOW_FROM_FLAG -eq 1 && $UOW_TO_FLAG -eq 0) || ($UOW_TO_FLAG -eq 1 && $UOW_FROM_FLAG -eq 0) ]]
then
   print "Missing required UOW (FROM or TO) parameter."
   usage
   exit 1
fi

export CURR_DATETIME=$(date '+%Y%m%d-%H%M%S')

export PARENT_ERROR_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.multi_sft_push_handler${UOW_APPEND}.$CURR_DATETIME.err
export PARENT_LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.multi_sft_push_run${UOW_APPEND}.$CURR_DATETIME.log

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
   (time $DW_MASTER_BIN/dw_infra.multi_sft_push_run.ksh) > $PARENT_LOG_FILE 2>&1
   RUN_RCODE=$?
   export DWI_END_DATETIME=$(date '+%Y%m%d-%H%M%S')
   print "$DWI_INFRA_IND DWI_END_DATETIME=$DWI_END_DATETIME" >> $PARENT_LOG_FILE
   set -e

   if [[ $RUN_RCODE -eq 0 ]]
   then
    if [[ -f $DW_SA_TMP/$TABLE_ID.$JOB_TYPE.complete ]]
    then
      print "$PROCESS does not complete. Please try to reset the job."
      RUN_RCODE=8
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

print "SFT Push Complete"

exit 0
