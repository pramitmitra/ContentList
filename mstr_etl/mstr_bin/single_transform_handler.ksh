#!/bin/ksh -eu

#------------------------------------------------------------------------------------------------
# Filename:     single_transform_handler.ksh
# Description:  Basic wrapper for customized transform scripts. Provides logging and error handling,
#                      cleanup and process control.
# Developer:    Jacky Shen
# Created on:   20/05/2011
# Location:     $DW_MASTER_BIN/
#
# Execution:    $DW_MASTER_BIN/single_transform_handler.ksh -i <ETL_ID>  -f <UOW_FROM> -t <UOW_TO> -s <SCRIPT_NAME> -p "<Param1> <Param2> <Param3> ... <ParamX>"
#
# Parameters:   ETL_ID = <SUBJECT_AREA.TABLE_ID>
#                       SHELL_EXE = <shell executable>
#                       Param[1-X] = <parameters for shell executable>
#
# Revision History:
#
# Name             Date            Description
# ---------------  --------------  ---------------------------------------------------
# Jacky Shen  20/05/2011      Initial Creation
# Jacky Shen  25/05/2011      Hardcode the JOB_ENV to transform
# Jacky Shen  27/09/2011      Add UOW_FROM and UOW_TO
# Ryan Wong   13/02/2012      Fixing logic for DW_SA_LOG, check if CURR_DATE is appended
#                             Adding values for UOW_FROM/TO_DATE and UOW_FROM/TO_DATE_RFMT
# Ryan Wong   10/04/2013      Redhat changes
#------------------------------------------------------------------------------------------------

. /dw/etl/mstr_cfg/etlenv.setup

export DWI_CALLED=$0
export DWI_CALLED_ARGS=${@:-""}
export DWI_WHOAMI=$(whoami)
export DWI_START_DATETIME=$(date '+%Y%m%d-%H%M%S')

set +u
print_header
set -u

# Input Params
export _etl_id=
export _script=
export _param_list=

export UOW_FROM_FLAG=0
export UOW_TO_FLAG=0
export UOW_FROM=""
export UOW_TO=""

while getopts "i:f:t:s:p:" opt
do
case $opt in
   i)   _etl_id="$OPTARG";;
   f)   if [[ $UOW_FROM_FLAG -ne 0 ]]
          then
             print "Fatal Error: -f flag specified more than once" >&2
             usage
             exit 4
          fi
          print "Setting UOW_FROM_FLAG == 1"
          UOW_FROM_FLAG=1
          print "Setting UOW_FROM == $OPTARG"
          UOW_FROM=${OPTARG};;
   t)   if [[ $UOW_TO_FLAG -ne 0 ]]
          then
             print "FATAL ERROR: -t flag specified more than once" >&2
             usage
             exit 4
          fi
          print "Setting UOW_TO_FLAG == 1"
          UOW_TO_FLAG=1
          print "Setting UOW_TO == $OPTARG"
          UOW_TO=$OPTARG;;
   s)   _script="$OPTARG";;
   p)   _param_list="$OPTARG";;
   \?)  print >&2 "Usage: $0 -i <ETL_ID> -f <UOW_FROM> -t <UOW_TO> -s <SHELL_EXE> -p \"P1 P2 P3\""
   return 1;;
esac
done
shift $(($OPTIND - 1))


if [[ X"$_etl_id" = X"" || X"$_script" = X"" ]]
  then
    print "Usage: $0 -i <ETL_ID> -f <UOW_FROM> -t <UOW_TO> -s <SHELL_EXE> -p \"P1 P2 P3\"
    <ETL_ID> <SHELL_EXE> is mandatory
    
    Example: $0 -i dw_infra.test -e extract -u 20110512 -s single_field_normalize.ksh -p \"<TRGT_ETL_ID> <SRC_BATCH_SEQ_NUM> <SRC_ETL_ID> <SRC_JOB_ENV> <SRC_JOB_TYPE> <SRC_JOB_TYPE_ID> <INPUT_DML_FILENAME> <OUTPUT_DML_FILENAME>\"
    "
  exit 4
fi


export ETL_ID=$_etl_id
export JOB_ENV=transform
export JOB_TYPE=transform
export JOB_TYPE_ID=tr
export SHELL_EXE=$_script
export SCRIPT_NAME=${0##*/}
export SHELL_EXE_NAME=${SHELL_EXE##*/}
export SUBJECT_AREA=${ETL_ID%%.*}
export TABLE_ID=${ETL_ID##*.}
export CURR_DATETIME=$(date '+%Y%m%d-%H%M%S')



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
   export UOW_FROM_DATE=$(print $UOW_FROM | cut -c1-8)
   export UOW_TO_DATE=$(print $UOW_TO | cut -c1-8)
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


export PARENT_ERROR_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.${SHELL_EXE_NAME%.ksh}${UOW_APPEND}.$CURR_DATETIME.err
export PARENT_LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.single_transform_run.${SHELL_EXE_NAME%.ksh}${UOW_APPEND}.$CURR_DATETIME.log

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
   (time $DW_MASTER_BIN/single_transform_run.ksh) > $PARENT_LOG_FILE 2>&1
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

print "Transform Complete"

exit 0
