#!/bin/ksh -eu
# Title:        Single Table Extract Handler
# File Name:    single_table_extract_handler.ksh
# Description:  Handle submiting a single table extract job.  Will also spawn multi-table processing.
# Developer:    Craig Werre
# Created on:   
# Location:     $DW_EXE  
# Logic:       
#
#
# Called by:    UC4/Unix
#
# Date         Ver#   Modified By(Name)            Change and Reason for Change
#---------    -----  ---------------------------  ------------------------------
# 2006-09-11   1.0    Brian Wenner                  added Host level restart ability for multi-table Jobs
# 2007-05-31   1.1    Orlando Jin                   Pass PPID into resubmit hanlder as well to avoid unexpected process kill
# 2007-10-23   1.2    Cesar Valenzuela              Converted caty to distributed table lis 
# 2010-04-01   1.3    Brian Wenner                  added HA and DR processing,
#                                                   used modularized functions and defs
# 2011-07-12   1.4    Jacky Shen                    add new EXTRACT_PROCESS_TYPE SFT for SocParc file transfer
# 2011-08-24   1.5    Jacky Shen                    add logic to determine if SFT in .cfg after extracting
# 2011-09-21   1.5    Ryan Wong                     add uow
# 2011-09-26   1.6    Kevin Oaks                    Altered touchfile logic to call
#                                                   $DW_MASTER_EXE/touchWatchFile.ksh
# 2011-10-05   1.7    Ryan Wong                     Fix bsn definition on restart
# 2011-10-12   1.8    Ryan Wong                     Split main code to single_table_extract_run.ksh
#                                                   Allow use of time and a redirect for log
# 2011-12-15   1.9    Ryan Wong                     Modify DW_SA_LOG to a date dir based on CURR_DATE
#                                                   Define IN_DIR, add UOW date based dir
#                                                   Define REC_CNT_IN_DIR, add UOW date based dir
# 2012-02-13   1.10   Ryan Wong                     Fixing logic for DW_SA_LOG, check if CURR_DATE is appended
#                                                   Adding values for UOW_FROM/TO_DATE and UOW_FROM/TO_DATE_RFMT
# 2012-05-08   1.11   Ryan Wong                     Modify UOW IN_DIR, add time based dirs UOW_TO_HH/UOW_TO_MI/UOW_TO_SS
#
# 2012-10-22   1.12   Jacky Shen                    Add an additional call to etlenv.setup after assignment for ETL_ID
# 2012-10-31   1.13   Jacky Shen                    Add QUERY_BAND_STRING for teradata extract job
# 2012-11-26   1.14   Ryan Wong                     Add UOW_[FROM/TO]_TIME
# 2013-03-25   1.15   Jacky Shen                    Add a check to Infra_Run complete file to fail job if the complete file still exists
# 2013-07-16   1.16   Ryan Wong                     Update UOW variable definition to use $DW_MASTER_EXE/dw_etl_common_defs_uow.cfg
# 2013-10-04   1.17   Ryan Wong                     Redhat changes
# 2016-09-16   1.18   Ryan Wong                     Adding Queryband name-value-pairs UC4_JOB_BATCH_MODE and UC4_JOB_PRIORITY
#############################################################################################################

typeset -fu usage

function usage {
   print "Usage:  $0 <ETL_ID> [-f <UOW_FROM> -t <UOW_TO>]
NOTE: UOW_FROM and UOW_TO are optional but must be used in tandem if either is present."
}

. /dw/etl/mstr_cfg/etlenv.setup

export DWI_CALLED=$0
export DWI_CALLED_ARGS=${@:-""}
export DWI_WHOAMI=$(whoami)
export DWI_START_DATETIME=$(date '+%Y%m%d-%H%M%S')

set +u
print_header
set -u

if [ $# -lt 1 ]
then
        usage
	exit 4
fi

export SCRIPTNAME=${0##*/}
export BASENAME=${SCRIPTNAME%.*}

export ETL_ID=$1
shift 1

export JOB_ENV=extract
export JOB_TYPE=extract
export JOB_TYPE_ID=ex

. /dw/etl/mstr_cfg/etlenv.setup

# Check for optional UOW
export UOW_FROM=""
export UOW_TO=""
export UOW_FROM_FLAG=0
export UOW_TO_FLAG=0

print "Processing Options"
while getopts "f:t:" opt
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
      \? ) usage
           exit 1;;
   esac
done
shift $(($OPTIND - 1))

# Setup common definitions
. $DW_MASTER_CFG/dw_etl_common_defs.cfg
. $DW_MASTER_LIB/dw_etl_common_abinitio_functions.lib

# read in UC4 runtime parameters, they will be used in teradata query band if it is an extract job from teradata
export UC4_JOB_NAME=${UC4_JOB_NAME:-"NA"}
export UC4_PRNT_CNTR_NAME=${UC4_PRNT_CNTR_NAME:-"NA"}
export UC4_TOP_LVL_CNTR_NAME=${UC4_TOP_LVL_CNTR_NAME:-"NA"}
export UC4_JOB_RUN_ID=${UC4_JOB_RUN_ID:-"NA"}
export UC4_JOB_BATCH_MODE=${UC4_JOB_BATCH_MODE:-"NA"}
export UC4_JOB_PRIORITY=${UC4_JOB_PRIORITY:-"NA"}
export QB_STR_UC4="UC4_JOB_NAME=${UC4_JOB_NAME};UC4_PRNT_CNTR_NAME=${UC4_PRNT_CNTR_NAME};UC4_TOP_LVL_CNTR_NAME=${UC4_TOP_LVL_CNTR_NAME};UC4_JOB_RUN_ID=${UC4_JOB_RUN_ID};UC4_JOB_BATCH_MODE=${UC4_JOB_BATCH_MODE};UC4_JOB_PRIORITY=${UC4_JOB_PRIORITY}"

export QUERY_BAND_STRING="SA=$SUBJECT_AREA;TBID=$TABLE_ID;$QB_STR_UC4; UPDATE"

# Get IN_DIR from etl cfg
assignTagValue IN_DIR IN_DIR $ETL_CFG_FILE W $DW_IN
export IN_DIR=$IN_DIR/$JOB_ENV/$SUBJECT_AREA

# Check if IN_DIR point to a MFS folder
if [[ ${IN_DIR} != ${IN_DIR%mfs*} ]]
then
   export REC_CNT_IN_DIR=$DW_SA_IN
else
   export REC_CNT_IN_DIR=$IN_DIR
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

mkdirifnotexist $DW_SA_LOG

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
   assignTagValue UOW_FROM_DATE_RFMT_CODE UOW_FROM_DATE_RFMT_CODE $ETL_CFG_FILE W 0
   assignTagValue UOW_TO_DATE_RFMT_CODE UOW_TO_DATE_RFMT_CODE $ETL_CFG_FILE W 0
   export UOW_FROM_DATE_RFMT=$($DW_MASTER_EXE/dw_infra.reformat_date.ksh $UOW_FROM_DATE $UOW_FROM_DATE_RFMT_CODE)
   export UOW_TO_DATE_RFMT=$($DW_MASTER_EXE/dw_infra.reformat_date.ksh $UOW_TO_DATE $UOW_TO_DATE_RFMT_CODE)
   export UOW_DATE=$UOW_TO_DATE
   export IN_DIR=$IN_DIR/$TABLE_ID/$UOW_TO_DATE/$UOW_TO_HH/$UOW_TO_MI/$UOW_TO_SS
   # Check if IN_DIR point to a MFS folder
   if [[ ${IN_DIR} != ${IN_DIR%mfs*} ]]
   then
     m_mkdirifnotexist $IN_DIR
   else
     mkdirifnotexist $IN_DIR
   fi
   export REC_CNT_IN_DIR=$REC_CNT_IN_DIR/$TABLE_ID/$UOW_TO_DATE/$UOW_TO_HH/$UOW_TO_MI/$UOW_TO_SS
   mkdirifnotexist $REC_CNT_IN_DIR
elif [[ ($UOW_FROM_FLAG -eq 1 && $UOW_TO_FLAG -eq 0) || ($UOW_TO_FLAG -eq 1 && $UOW_FROM_FLAG -eq 0) ]]
then
   print "Missing required UOW (FROM or TO) parameter."
   usage
   exit 1
fi

export PARENT_ERROR_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.${BASENAME}${UOW_APPEND}.$CURR_DATETIME.err
export PARENT_LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.single_table_extract_run${UOW_APPEND}.$CURR_DATETIME.log

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
   (time $DW_MASTER_BIN/single_table_extract_run.ksh) > $PARENT_LOG_FILE 2>&1
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

print "Extract Complete"

exit 0
