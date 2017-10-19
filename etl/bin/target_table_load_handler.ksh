#!/bin/ksh -eu
###################################################################################################################
###################################################################################################################
#
# Title:        Target Table Load Handler 
# File Name:    target_table_load_handler.ksh 
# Description:  Handler for Loading Target Tables from staged data
# Developer:    Kevin Oaks 
# Created on:   Legacy code re-purposed on 2011-09-01 
# Location:     $DW_BIN
#
# Usage Notes: UOW_TO and UOW_FROM work in tandem. If one is passed in, then the other must be also.
# Revision History
#
#  Date         Ver#   Modified By(Name)            Change and Reason for Change
# ----------    -----  ---------------------------  ----------------------------------------------------------
# 2011-09-02     1.0   Kevin Oaks                   Added UOW option and getops functionality while leaving
#                                                   backwards compatibility intact.
# 2011-09-21     1.1   Kevin Oaks                   Modified to use UOW_FROM/UOW_TO
# 2011-10-12     1.2   Ryan Wong                    Split main code to target_table_load_run.ksh
#                                                   Allow use of time and a redirect for log
# 2011-12-15     1.3   Ryan Wong                    Modify DW_SA_LOG to a date dir based on CURR_DATE
# 2012-02-13     1.4   Ryan Wong                    Fixing logic for DW_SA_LOG, check if CURR_DATE is appended
#                                                   Adding values for UOW_FROM/TO_DATE and UOW_FROM/TO_DATE_RFMT
#
# 2012-10-22     1.5   Jacky Shen                   Add an additional call to etlenv.setup after assignment for ETL_ID
# 2012-11-26     1.6   Ryan Wong                    Add UOW_[FROM/TO]_TIME, TRGT_UOW_[FROM/TO]_DATE_RFMT_CODE
# 2013-03-25     1.7   Jacky Shen                   Add a check to Infra_Run complete file to fail job if the complete file still exists
# 2013-07-16     1.8   Ryan Wong                    Update UOW variable definition to use $DW_MASTER_EXE/dw_etl_common_defs_uow.cfg
# 2013-07-30     1.9   Jacky Shen                   Add hadoop jar job support
# 2013-10-04     1.10  Ryan Wong                    Redhat changes
# 2017-10-18     1.11  Michael Weng                 Add support for sp*
###################################################################################################################

typeset -fu usage

function usage {
   print "Usage:  $0 <ETL_ID> <JOB_ENV> <SQL_FILE|JAR_FILE> [ -f <UOW_FROM> -t <UOW_TO> ] [ -m <main_class> [ -p <PARAM_NAME1=PARAM_VALUE1> -p <PARAM_NAME1=PARAM_VALUE1> ... ] OR [<PARAM_NAME1=PARAM_VALUE1> <PARAM_NAME2=PARAM_VALUE2> ...]]
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

if [ $# -lt 3 ]
then
   usage
   exit 4
fi

export SCRIPTNAME=${0##*/}
export BASENAME=${SCRIPTNAME%.*}

export ETL_ID=$1
export JOB_ENV=$2
export SQL_FILE=$3

# At some point we should do an explicit check for all shifted options for more accurate
# and graceful failures and messages.
shift 3

if [[ $JOB_ENV == @(hd*|sp*) ]]
then
export JOB_TYPE=hadoop_tr
export JOB_TYPE_ID=mr
else
export JOB_TYPE=bteq
export JOB_TYPE_ID=bt
fi

SQL_FILE_BASENAME=${SQL_FILE##*/}
export SQL_FILE_BASENAME=${SQL_FILE_BASENAME%.*}

. /dw/etl/mstr_cfg/etlenv.setup

# Check for optional UOW
export UOW_FROM=""
export UOW_TO=""
export UOW_FROM_FLAG=0
export UOW_TO_FLAG=0

# getopts loop for processing optional args including UOW
print "Processing Options"
while getopts "f:t:p:m:" opt
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
      m ) 
          print "Setting MAIN_CLASS to $OPTARG"
          MAIN_CLASS=$OPTARG
          export MAIN_CLASS
          ;;
      \? ) usage
           exit 1;;
   esac
done
shift $((OPTIND - 1))

# retain old style optional args processing for backwards compatibility. Should deprecate at some point.
PARAM_LIST=""
if [ $# -ge 1 ]
then
   if [[ $JOB_ENV == @(hd1|hd2) ]]
   then
     export PARAM_LIST=$*
   else
   for param in $*
   do
      if [ ${param%=*} = $param ]
      then
         print "${0##*/}: ERROR, parameter definition $param is not of form <PARAM_NAME=PARAM_VALUE>"
         usage
         exit 4
      else
         print "Exporting $param"
         export $param
      fi
   done
  fi
fi

# Setup common definitions
. $DW_MASTER_CFG/dw_etl_common_defs.cfg

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
   assignTagValue TRGT_UOW_FROM_DATE_RFMT_CODE TRGT_UOW_FROM_DATE_RFMT_CODE $ETL_CFG_FILE W 0
   assignTagValue TRGT_UOW_TO_DATE_RFMT_CODE TRGT_UOW_TO_DATE_RFMT_CODE $ETL_CFG_FILE W 0
   export UOW_FROM_DATE_RFMT=$($DW_MASTER_EXE/dw_infra.reformat_date.ksh $UOW_FROM_DATE $TRGT_UOW_FROM_DATE_RFMT_CODE)
   export UOW_TO_DATE_RFMT=$($DW_MASTER_EXE/dw_infra.reformat_date.ksh $UOW_TO_DATE $TRGT_UOW_TO_DATE_RFMT_CODE)
   export UOW_DATE=$UOW_TO_DATE
elif [[ ($UOW_FROM_FLAG -eq 1 && $UOW_TO_FLAG -eq 0) || ($UOW_TO_FLAG -eq 1 && $UOW_FROM_FLAG -eq 0) ]]
then
   print "Missing required UOW (FROM or TO) parameter."
   usage
   exit 1
fi

export PARENT_ERROR_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.$BASENAME.$SQL_FILE_BASENAME${UOW_APPEND}.$CURR_DATETIME.err
export PARENT_LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.target_table_load_run.$SQL_FILE_BASENAME${UOW_APPEND}.$CURR_DATETIME.log

# Comp File Handler
export UC4_JOB_NAME=${UC4_JOB_NAME:-""}
export UC4_JOB_NAME_APPEND=""
if [[ -n $UC4_JOB_NAME ]]
then
  export UC4_JOB_NAME_APPEND=".$UC4_JOB_NAME"
fi
HANDLER_COMP_FILE=$DW_SA_TMP/$TABLE_ID.$BASENAME.${SQL_FILE_BASENAME}${UC4_JOB_NAME_APPEND}.handler.complete

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
   (time $DW_MASTER_BIN/target_table_load_run.ksh) > $PARENT_LOG_FILE 2>&1
   RUN_RCODE=$?
   export DWI_END_DATETIME=$(date '+%Y%m%d-%H%M%S')
   print "$DWI_INFRA_IND DWI_END_DATETIME=$DWI_END_DATETIME" >> $PARENT_LOG_FILE
   set -e

   if [[ $RUN_RCODE -eq 0 ]]
   then
    if [[ -f $DW_SA_TMP/$TABLE_ID.$BASENAME.${SQL_FILE_BASENAME}${UC4_JOB_NAME_APPEND}.complete ]]
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

print "Target Complete"

exit 0
