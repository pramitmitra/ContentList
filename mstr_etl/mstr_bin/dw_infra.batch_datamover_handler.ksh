#!/bin/ksh -eu
###################################################################################################################
#
# Title:        DW_INFRA Batch DataMover Handler
# File Name:    dw_infra.batch_datamover_handler.ksh
# Description:  Handler for (Teradata) data replication/movement across platforms
# Developer:    Kevin Oaks
# Created on:   2010-10-11
# Location:     $DW_MASTER_EXE
#
# Date         Ver#   Modified By(Name)            Change and Reason for Change
#---------    -----  ---------------------------  ------------------------------
# 2010-12-10   1.0    Kevin Oaks                    Initial Prod Version
# 2011-12-20   1.1    Ryan Wong                     Modify DW_SA_LOG to a date dir based on CURR_DATE
# 2012-02-13   1.2    Ryan Wong                     Fixing logic for DW_SA_LOG, check if CURR_DATE is appended
#                                                   Adding values for UOW_FROM/TO_DATE and UOW_FROM/TO_DATE_RFMT
# 2012-06-05   1.3    Kevin Oaks                    Added support for optional user-defined passed in parameters
# 2012-11-26   1.4    Ryan Wong                     Add UOW_[FROM/TO]_TIME, DM_UOW_[FROM/TO]_DATE_RFMT_CODE
# 2013-07-16   1.5    Ryan Wong                     Update UOW variable definition to use $DW_MASTER_EXE/dw_etl_common_defs_uow.cfg
# 2013-10-04   1.6    Ryan Wong                     Redhat changes
# 2014-08-20   1.7    Ryan Wong                     Update statement if DM_UTILITY_TYPE = tdbridge to use double brackets
###################################################################################################################
###################################################################################################################
####
#### This handler encapsulates the modules for batch level data movement/replication.
#### Currently Teradata based, but functionality may be extended in future to accomodate
#### ingest/acquistition/hadoop/etc...
####
#### Module supports UOW functionality.
####
#### When running as source, data is extracted to file. When running as target, the data that was extracted
#### to file is then loaded to the target. There must be a job plan for the source as well as one for each 
#### target. This handler self identifies whether it is running as a source or a target based on the JOB_ENV
#### provided in conjunction with the DM_(SRC|TRGT)_ENV tags present in the $ETL_ID cfg file. 
####
#### The components available are:
#### 1: Execute SQL to stage data on the source system prior to extracting for
####    replication/transformation elsewhere. - Optional (s)
#### 2: Execute Stage to Base SQL on Source - Paypal scenario is use case. - Optional (r)
#### 3: Extract from source. - Required
#### 4: Load to Target. Can be loaded directly to final target or to stage prior to
####    transforming to target. If source is UTF8 data, then data should always be staged
####    on load. Load as ascii to stage, then use the common Teradata conversion UDF for
####    converting to unicode during Stage to Base on Target phase.
####    Current load modes are Truncate-Insert and Append. These should be considered when
####    deciding how/where to load, and special care used when Truncate-Insert is the mode.
####    Note that a source may have multiple targets but a job plan will need to be created
####    for each. - Required
#### 5: Execute Stage to Base SQL on Target. The same sql executed in item 2 will be
####    executed here by default. Will provide override for this - Optional (R)
####
#### Replication graph currently supports Teradata Source/Targets only.
#### Addititional functionality may be added later to support other platforms.
####
#### Valid options (source values are lowercase, target values are uppercase):
#### s - Extract to stage on source
#### r - Execute runSQL on source ( typically a load/transform to final table )
#### R - execute runSQL on target ( typically a load/transform to final table )
####
#### f <UOW_FROM> - a UOW_FROM value generated in UC4
#### t <UOW_TO> - a UOW_TO value generated in UC4
#### p <PARAM=VALUE> - Optional parameters and associated values passed in on command line. Multiple params
####                   are allowed, but each must use its own -p <PARAM=VALUE> 
####
###################################################################################################################
###################################################################################################################
#
# Functions:

typeset -fu usage

function usage {
  print "
  Usage:
  $DW_MASTER_EXE/dw_infra.batch_datamover_handler.ksh <ETL_ID> <JOB_ENV> [ -s -r -R ] [ -f <UOW_FROM> -t <UOW_TO> ] [ [ -p <param=value> ] [ -p <param=value>] ... ]
  Note: Optional parameters without arguments can be stacked, i.e. -srR.
"
}

#
###################################################################################################################

. /dw/etl/mstr_cfg/etlenv.setup

 

export DWI_CALLED=$0
export DWI_CALLED_ARGS=${@:-""}
export DWI_WHOAMI=$(whoami)
export DWI_START_DATETIME=$(date '+%Y%m%d-%H%M%S')

set +u
print_header
set -u

if [ $# -lt 2 ]
then
  usage
  exit 4
fi

export ETL_ID=$1
export JOB_ENV=$2

export JOB_TYPE=datamove
export JOB_TYPE_ID=dm

export SUBJECT_AREA=${ETL_ID%%.*}
export TABLE_ID=${ETL_ID##*.}

. /dw/etl/mstr_cfg/etlenv.setup


shift 2

# Set default Option values

export SRC_STAGE_DATA=0
export SRC_LOAD_TO_BASE=0
export TRGT_LOAD_TO_BASE=0
export UOW_FROM=""
export UOW_TO=""
export UOW_FROM_FLAG=0
export UOW_TO_FLAG=0

# Process Options
print "Processing Options"
while getopts "srRf:t:p:" opt
do
   case $opt in
      s ) print "Setting SRC_STAGE_DATA == 1"
          SRC_STAGE_DATA=1;;
      r ) print "Setting SRC_LOAD_TO_BASE == 1"
          SRC_LOAD_TO_BASE=1;;
      R ) print "Setting TRGT_LOAD_TO_BASE == 1"
          TRGT_LOAD_TO_BASE=1;;
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
      \? ) print "Invalid option."
           usage
           exit 1;;
   esac
done
shift $(($OPTIND - 1))



# Instantiate Environment
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
   assignTagValue DM_UOW_FROM_DATE_RFMT_CODE DM_UOW_FROM_DATE_RFMT_CODE $ETL_CFG_FILE W 0
   assignTagValue DM_UOW_TO_DATE_RFMT_CODE DM_UOW_TO_DATE_RFMT_CODE $ETL_CFG_FILE W 0
   export UOW_FROM_DATE_RFMT=$($DW_MASTER_EXE/dw_infra.reformat_date.ksh $UOW_FROM_DATE $DM_UOW_FROM_DATE_RFMT_CODE)
   export UOW_TO_DATE_RFMT=$($DW_MASTER_EXE/dw_infra.reformat_date.ksh $UOW_TO_DATE $DM_UOW_TO_DATE_RFMT_CODE)
   export UOW_DATE=$UOW_TO_DATE
elif [[ ($UOW_FROM_FLAG -eq 1 && $UOW_TO_FLAG -eq 0) || ($UOW_TO_FLAG -eq 1 && $UOW_FROM_FLAG -eq 0) ]]
then
   print "Missing required UOW (FROM or TO) parameter."
   usage
   exit 1
fi

# Define parent error file

export PARENT_ERROR_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.datamove_handler${UOW_APPEND}.$CURR_DATETIME.err
export PARENT_LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.batch_datamover_run${UOW_APPEND}.$CURR_DATETIME.log

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
   assignTagValue DM_UTILITY_TYPE DM_UTILITY_TYPE $ETL_CFG_FILE W ""  > /dev/null 2>&1
   
   
   set +e
   if [[ ${DM_UTILITY_TYPE} = "tdbridge" ]]
   then
    (time $DW_MASTER_BIN/dw_infra.batch_td_bridge_run.ksh) > $PARENT_LOG_FILE 2>&1	
    RUN_RCODE=$?
   else
    (time $DW_MASTER_BIN/dw_infra.batch_datamover_run.ksh) > $PARENT_LOG_FILE 2>&1
    RUN_RCODE=$?
   fi
   
   export DWI_END_DATETIME=$(date '+%Y%m%d-%H%M%S')
   print "$DWI_INFRA_IND DWI_END_DATETIME=$DWI_END_DATETIME" >> $PARENT_LOG_FILE
   set -e
   

   if [[ $RUN_RCODE -eq 0 ]]
   then
    if [[ -f $DW_SA_TMP/$TABLE_ID.$JOB_TYPE_ID.$ENV_TYPE.complete ]]
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

print "Execution of $0 Complete"

exit 0
