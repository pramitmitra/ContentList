#!/bin/ksh -eu
# Title:        Watch File Check
# File Name:    dw_infra.watch_file_check.ksh
# Description:  Checks if a file exists (local).  REQUIRES UOW.  Exits successfully if file is
#               found.  Loop will continue to check for file every 60 seconds.
# Developer:    Ryan Wong
# Created on:
# Location:     $DW_MASTER_BIN
# Logic:
#
#
# Called by:    UC4/Unix
#
# Date         Ver#   Modified By(Name)            Change and Reason for Change
#---------    -----  ---------------------------  ------------------------------
# 2012-03-05   1.0    Ryan Wong                    Initial version
# 2012-11-26   1.1    Ryan Wong                    Add UOW_[FROM/TO]_TIME, WF_UOW_[FROM/TO]_DATE_RFMT_CODE
# 2013-07-16   1.2    Ryan Wong                    Update UOW variable definition to use $DW_MASTER_EXE/dw_etl_common_defs_uow.cfg
# 2013-10-04   1.3    Ryan Wong                    Redhat changes
####################################################################################################

typeset -fu usage

function usage {
  print "Usage:  $SCRIPTNAME -i <ETL_ID> -e <JOB_ENV> -f <UOW_FROM> -t <UOW_TO> -j <WF_JOB_ENV> -w <WF_NAME>
REQUIRED:
  -i ETL_ID          [subject_area.table_id]
  -e JOB_ENV         [extract|td1|td2|td3|td4|...]
  -f UOW_FROM        [YYYYMMDD24hrmiss] Unit Of Work
  -t UOW_TO          [YYYYMMDD24hrmiss] Unit Of Work
  -j WF_JOB_ENV      [extract|td1|td2|td3|td4|...] Watch file JOB_ENV
  -w WF_NAME         [file name] File name you are looking for.  Should not include UOW suffix
Examples:
  Staging table load wait for extract touch file:
    $SCRIPTNAME -i dw_bid.dw_bid -e td1 -f 20120301000000 -t 20120302000000 -j extract -w dw_bid.dw_bid.extract.done
  Target table load wait for staging touch file:
    $SCRIPTNAME -i dw_et.ods_cmc_unica_trtmnt_user -e td1 -f 20120110000000 -t 20120111000000 -j td1 -w dw_et.ods_cmc_unica_trtmnt_user.load.done
  Target table load wait for upstream target table load touch file:
    $SCRIPTNAME -i dw_attr.item_attr_info -e td1 -f 20120218000000 -t 20120219000000 -j td2 -w dw_motors.motors_my_vehicle.ups.done
  Target table load wait for datamove touch file:
    $SCRIPTNAME -i dw_infra.dm_test -e td2 -f 20120304000000 -t 20120305000000 -j td1 -w dw_infra.dm_test.datamove.src.done
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

export SCRIPTNAME=${0##*/}
export BASENAME=${SCRIPTNAME%.*}
export JOB_TYPE=watch_file
export JOB_TYPE_ID=wf

# Check parameters
export ETL_ID=""
export JOB_ENV=""
export UOW_FROM=""
export UOW_TO=""
export WF_JOB_ENV=""
export WF_NAME=""

export ETL_ID_FLAG=0
export JOB_ENV_FLAG=0
export UOW_FROM_FLAG=0
export UOW_TO_FLAG=0
export WF_JOB_ENV_FLAG=0
export WF_NAME_FLAG=0

print "Processing Options"
while getopts "i:e:f:t:j:w:" opt
do
   case $opt in
      i ) if [ $ETL_ID_FLAG -ne 0 ]
          then
            print "FATAL ERROR: -$opt flag specified more than once" >&2
            exit 8
          fi
          ETL_ID_FLAG=1
          ETL_ID=$OPTARG;;
      e ) if [ $JOB_ENV_FLAG -ne 0 ]
          then
            print "FATAL ERROR: -$opt flag specified more than once" >&2
            exit 8
          fi
          JOB_ENV_FLAG=1
          JOB_ENV=$OPTARG;;
      f ) if [ $UOW_FROM_FLAG -ne 0 ]
          then
            print "FATAL ERROR: -$opt flag specified more than once" >&2
            exit 8
          fi
          UOW_FROM_FLAG=1
          UOW_FROM=$OPTARG;;
      t ) if [ $UOW_TO_FLAG -ne 0 ]
          then
            print "FATAL ERROR: -$opt flag specified more than once" >&2
            exit 8
          fi
          UOW_TO_FLAG=1
          UOW_TO=$OPTARG;;
      j ) if [ $WF_JOB_ENV_FLAG -ne 0 ]
          then
            print "FATAL ERROR: -$opt flag specified more than once" >&2
            exit 8
          fi
          WF_JOB_ENV_FLAG=1
          WF_JOB_ENV=$OPTARG;;
      w ) if [ $WF_NAME_FLAG -ne 0 ]
          then
            print "FATAL ERROR: -$opt flag specified more than once" >&2
            exit 8
          fi
          WF_NAME_FLAG=1
          WF_NAME=$OPTARG;;
      \? ) usage
           exit 1 ;;
   esac
done

if [ $ETL_ID_FLAG -eq 0 ]
then
  print "FATAL ERROR: ETL_ID required"
  usage
  exit 1
fi

if [ $JOB_ENV_FLAG -eq 0 ]
then
  print "FATAL ERROR: JOB_ENV required"
  usage
  exit 1
fi

if [ $WF_NAME_FLAG -eq 0 ]
then
  print "FATAL ERROR: WF_NAME required"
  usage
  exit 1
fi


.  /dw/etl/mstr_cfg/etlenv.setup


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
   assignTagValue WF_UOW_FROM_DATE_RFMT_CODE WF_UOW_FROM_DATE_RFMT_CODE $ETL_CFG_FILE W 0
   assignTagValue WF_UOW_TO_DATE_RFMT_CODE WF_UOW_TO_DATE_RFMT_CODE $ETL_CFG_FILE W 0
   export UOW_FROM_DATE_RFMT=$($DW_MASTER_EXE/dw_infra.reformat_date.ksh $UOW_FROM_DATE $WF_UOW_FROM_DATE_RFMT_CODE)
   export UOW_TO_DATE_RFMT=$($DW_MASTER_EXE/dw_infra.reformat_date.ksh $UOW_TO_DATE $WF_UOW_TO_DATE_RFMT_CODE)
   export UOW_DATE=$UOW_TO_DATE
else
   print "FATAL ERROR: UOW_FROM and UOW_TO required"
   usage
   exit 1
fi

export PARENT_ERROR_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.$WF_NAME.${BASENAME}${UOW_APPEND}.$CURR_DATETIME.err

. $DW_LIB/message_handler

if [ $WF_JOB_ENV_FLAG -eq 0 ]
then
  print "FATAL ERROR: WF_JOB_ENV required"
  usage
  exit 1
fi


WATCH_FILE=$DW_WATCH/$WF_JOB_ENV/$UOW_DATE/$WF_NAME.$UOW_TO

print "Checking for watch file"
print "WATCH_FILE=$WATCH_FILE"

while true
do
  if [ -f $WATCH_FILE ]
  then
    print "Found watch file $(date '+%Y%m%d-%H%M%S')"
    break
  else
    print "Watch file not found waiting 60 seconds $(date '+%Y%m%d-%H%M%S')"
    sleep 60
  fi
done

tcode=0
exit
