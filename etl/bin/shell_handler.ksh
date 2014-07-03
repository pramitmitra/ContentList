#!/bin/ksh -eu

#------------------------------------------------------------------------------------------------
# Filename:     shell_handler.ksh
# Description:  Basic wrapper for shell scripts. Provides logging and error handling
# Developer:    Ryan Wong
# Created on:   10/06/2005
# Location:     $DW_EXE/
#
# Execution:    $DW_EXE/shell_handler.ksh <ETL_ID> <JOB_ENV> <SHELL_EXE> <Param1> <Param2> <Param3> ... <ParamX>
#
# Parameters:   ETL_ID = <SUBJECT_AREA.TABLE_ID>
#               JOB_ENV = <extract|primary|secondary>
#               SHELL_EXE = <shell executable>
#               Param[1-X] = <parameters for shell executable>
#
# Revision History:
#
# Name             Date            Description
# ---------------  --------------  ---------------------------------------------------
# Ryan Wong        10/06/2005      Initial Creation
# Ryan Wong        02/05/2006      Updated some export variables.
# Jacky Shen       12/27/2011      Adding UOW_ID, infra_log_copy
# Ryan Wong        02/13/1012      Fixing logic for DW_SA_LOG, check if CURR_DATE is appended
#                                  Adding values for UOW_FROM/TO_DATE and UOW_FROM/TO_DATE_RFMT
# Jacky Shen       10/22/2012      Add an additional call to etlenv.setup after assignment for ETL_ID
# Ryan Wong        11/26/2012      Add UOW_[FROM/TO]_TIME, SHELL_UOW_[FROM/TO]_DATE_RFMT_CODE
# Jacky Shen       03/25/2013      Add a check to Infra_Run complete file to fail job if the complete file still exists
# Ryan Wong        07/16/2013      Update UOW variable definition to use $DW_MASTER_EXE/dw_etl_common_defs_uow.cfg
# Ryan Wong        10/04/2013      Redhat changes
# John Hackley     06/02/2014      Included $servername as part of log file name, since logs
#                                  are on shared storage and this job runs concurrently on many
#                                  hosts
# John Hackley     07/03/2014      Added optional input arguments to avoid file collisions on Tempo hosts:
#                                     -ul (unique logfile name) - include the host name as part of the log file name
#                                     -ut (unique touchfile name) - include the host name as part of the touch file name
#                                     -st (suppress touchfile) - skip creation of touchfile at end of job
#------------------------------------------------------------------------------------------------

typeset -fu usage

function usage {
   print "Usage:  $0 <ETL_ID> <JOB_ENV> [ -f <UOW_FROM> -t <UOW_TO> ] [ -ul ] [ -ut ] [ -st ] <SHELL_EXE> <Param1> <Param2> <Param3> ... <ParamX>
	ETL_ID =                   <SUBJECT_AREA.TABLE_ID>
	JOB_ENV =                  <extract|primary|secondary>
       -f and -t =                specify Unit of Work From and To dates, in YYYYMMDDHHMMSS format
       -ul (unique logfile) =     include ETL host name as part of log file name
       -ut (unique touchfile) =   include ETL host name as part of touch file name
       -st (suppress touchfile) = don't create touchfile at end of job
	SHELL_EXE =                <shell executable>
	Param[1-X] =               <parameters for shell executable>"
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

export ETL_ID=$1
export JOB_ENV=$2
export SHELL_EXE=$3

if [ $JOB_ENV = extract ]
then
	export JOB_TYPE=extract
	export JOB_TYPE_ID=ex
else
	export JOB_TYPE=load
	export JOB_TYPE_ID=ld
        export JOB_ENV_UPPER=$(print $JOB_ENV | tr [:lower:] [:upper:])
        export LOAD_DB=$(eval print \$DW_${JOB_ENV_UPPER}_DB)
fi

. /dw/etl/mstr_cfg/etlenv.setup

# Check for optional UOW
export UOW_FROM=""
export UOW_TO=""
export UOW_FROM_FLAG=0
export UOW_TO_FLAG=0

if [[ $SHELL_EXE = "-t" || $SHELL_EXE = "-f" ]]
then
   if [[ $SHELL_EXE = "-f" ]]
   then
     if [[ $UOW_FROM_FLAG -ne 0 || $UOW_TO_FLAG -ne 0 ]]
     then
        print "Fatal Error: -f/-t flag specified more than once" >&2
        usage
        exit 4
      else
        UOW_FROM=$4
        UOW_TO=$6
        UOW_FROM_FLAG=1
        UOW_TO_FLAG=1
      fi
   else
     if [[ $UOW_FROM_FLAG -ne 0 || $UOW_TO_FLAG -ne 0 ]]
     then
        print "Fatal Error: -f/-t flag specified more than once" >&2
        usage
        exit 4
      else
        UOW_FROM=$6
        UOW_TO=$4
        UOW_FROM_FLAG=1
        UOW_TO_FLAG=1
      fi
   fi
   export SHELL_EXE=$7
   shift 7
else
   shift 3
fi

# Check for optional -ul (unique log file name)
export SH_UNIQUE_LOG_FILE=0
if [[ $SHELL_EXE = "-ul" ]]
then
   export SH_UNIQUE_LOG_FILE=1
   export SHELL_EXE=$1
   shift 1
fi

# Check for optional -ut (unique touch file name)
export SH_UNIQUE_TOUCH_FILE=0
if [[ $SHELL_EXE = "-ut" ]]
then
   export SH_UNIQUE_TOUCH_FILE=1
   export SHELL_EXE=$1
   shift 1
fi

# Check for optional -st (suppress touch file)
export SH_SKIP_TOUCH_FILE=0
if [[ $SHELL_EXE = "-st" ]]
then
   export SH_SKIP_TOUCH_FILE=1
   export SHELL_EXE=$1
   shift 1
fi

export PARAMS=""
if [ $# -gt 0 ]
then
	export PARAMS=$*
fi

export SHELL_EXE_NAME=${SHELL_EXE##*/}
export SUBJECT_AREA=${ETL_ID%%.*}
export TABLE_ID=${ETL_ID##*.}
export CURR_DATETIME=$(date '+%Y%m%d-%H%M%S')



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
   assignTagValue SHELL_UOW_FROM_DATE_RFMT_CODE SHELL_UOW_FROM_DATE_RFMT_CODE $ETL_CFG_FILE W 0
   assignTagValue SHELL_UOW_TO_DATE_RFMT_CODE SHELL_UOW_TO_DATE_RFMT_CODE $ETL_CFG_FILE W 0
   export UOW_FROM_DATE_RFMT=$($DW_MASTER_EXE/dw_infra.reformat_date.ksh $UOW_FROM_DATE $SHELL_UOW_FROM_DATE_RFMT_CODE)
   export UOW_TO_DATE_RFMT=$($DW_MASTER_EXE/dw_infra.reformat_date.ksh $UOW_TO_DATE $SHELL_UOW_TO_DATE_RFMT_CODE)
   export UOW_DATE=$UOW_TO_DATE
elif [[ ($UOW_FROM_FLAG -eq 1 && $UOW_TO_FLAG -eq 0) || ($UOW_TO_FLAG -eq 1 && $UOW_FROM_FLAG -eq 0) ]]
then
   print "Missing required UOW (FROM or TO) parameter."
   usage
   exit 1
fi

if [[ $SH_UNIQUE_LOG_FILE -eq 1 ]]
then
  export PARENT_ERROR_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.$servername.${SHELL_EXE_NAME%.ksh}${UOW_APPEND}.$CURR_DATETIME.err
  export PARENT_LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.$servername.shell_run.${SHELL_EXE_NAME%.ksh}${UOW_APPEND}.$CURR_DATETIME.log
else
  export PARENT_ERROR_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.${SHELL_EXE_NAME%.ksh}${UOW_APPEND}.$CURR_DATETIME.err
  export PARENT_LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.shell_run.${SHELL_EXE_NAME%.ksh}${UOW_APPEND}.$CURR_DATETIME.log
fi

# Comp File Handler
export UC4_JOB_NAME=${UC4_JOB_NAME:-""}
export UC4_JOB_NAME_APPEND=""
if [[ -n $UC4_JOB_NAME ]]
then
  export UC4_JOB_NAME_APPEND=".$UC4_JOB_NAME"
fi
HANDLER_COMP_FILE=$DW_SA_TMP/$TABLE_ID.$JOB_TYPE.${SHELL_EXE_NAME%.ksh}${UC4_JOB_NAME_APPEND}.handler.complete

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
   (time $DW_MASTER_BIN/dw_infra.shell_run.ksh) > $PARENT_LOG_FILE 2>&1
   RUN_RCODE=$?
   export DWI_END_DATETIME=$(date '+%Y%m%d-%H%M%S')
   print "$DWI_INFRA_IND DWI_END_DATETIME=$DWI_END_DATETIME" >> $PARENT_LOG_FILE
   set -e

   if [[ $RUN_RCODE -eq 0 ]]
   then
    if [[ -f $DW_SA_TMP/$TABLE_ID.$JOB_ENV.${SHELL_EXE_NAME%.ksh}${UC4_JOB_NAME_APPEND}.complete ]]
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

print "Shell Complete"

exit 0
