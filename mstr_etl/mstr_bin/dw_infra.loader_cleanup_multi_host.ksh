#!/bin/ksh -eu
#############################################################################################################
# Title:        Loader Cleanup Multi Host
# Filename:     dw_infra.loader_cleanup_multi_host.ksh
# Description:  Handler to call dw_infra.loader_cleanup.ksh remotely for Multi Host
#
# Developer:    Ryan Wong
# Created on:   2012-11-11
# Location:     $DW_MASTER_BIN/
#
# Execution:    $DW_MASTER_BIN/dw_infra.loader_cleanup_tpt.ksh <ETL_ID> <JOB_ENV> <JOB_TYPE_ID> [[ -f <UOW_FROM> -t <UOW_TO> ] -b <MIN_LOAD_BATCH_SEQ_NUM> -u <MIN_LOAD_UNIT_OF_WORK>]
#
# Revision History:
#
# Name             Date            Description
# ---------------  --------------  --------------------------------------------------------------
# Ryan Wong        2012-11-11      Initial Creation
# Ryan Wong        2013-05-30      Add MIN_LOAD_UNIT_OF_WORK, for UOW cleanup
# Ryan Wong        2013-10-04      Redhat changes
#############################################################################################################

typeset -fu usage_tpt

function usage_tpt {
   print "Usage:  $0 <ETL_ID> <JOB_ENV> <JOB_TYPE_ID> [ -b <MIN_LOAD_BATCH_SEQ_NUM> -u <MIN_LOAD_UNIT_OF_WORK -f <UOW_FROM> -t <UOW_TO> ]
NOTE: UOW_FROM and UOW_TO are optional but must be used in tandem."
}

export ETL_ID=$1
export JOB_ENV=$2        # extract, td1, td2, td3, td4, etc... ( primary, secondary, all -- deprecated )
export JOB_TYPE_ID=$3    # ex, ld, bt, dm

. /dw/etl/mstr_cfg/etlenv.setup

if [[ $# -lt 3 ]]
then
   usage_tpt
   exit 4
fi

shift 3

export MIN_LOAD_BATCH_SEQ_NUM=""
export MIN_LOAD_BATCH_SEQ_NUM_FLAG=0
export MIN_LOAD_UNIT_OF_WORK=""
export MIN_LOAD_UNIT_OF_WORK_FLAG=0


# Check for optional UOW
export UOW_FROM=""
export UOW_TO=""
export UOW_FROM_FLAG=0
export UOW_TO_FLAG=0

print "Processing Options"
while getopts "f:t:b:u:" opt
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
      b ) if [ $MIN_LOAD_BATCH_SEQ_NUM_FLAG -ne 0 ]
          then
            print "FATAL ERROR: -b flag specified more than once" >&2
            exit 8
          fi
          print "Setting MIN_LOAD_BATCH_SEQ_NUM_FLAG == 1"  
          MIN_LOAD_BATCH_SEQ_NUM_FLAG=1
          print "Setting MIN_LOAD_BATCH_SEQ_NUM == $OPTARG"
	  MIN_LOAD_BATCH_SEQ_NUM=$OPTARG;;
      u ) if [ $MIN_LOAD_UNIT_OF_WORK_FLAG -ne 0 ]
          then
            print "FATAL ERROR: -u flag specified more than once" >&2
            exit 8
          fi
          print "Setting MIN_LOAD_UNIT_OF_WORK_FLAG == 1"
          MIN_LOAD_UNIT_OF_WORK=1
          print "Setting MIN_LOAD_UNIT_OF_WORK == $OPTARG"
          MIN_LOAD_UNIT_OF_WORK=$OPTARG;;
      \? ) usage_tpt
           exit 1 ;;
   esac
done
shift $(($OPTIND - 1))

if [ $JOB_TYPE_ID = "ex" ]
then
  export JOB_TYPE=extract
elif [ $JOB_TYPE_ID = "ld" ]
then
  export JOB_TYPE=load
fi

# Setup common definitions
. $DW_MASTER_CFG/dw_etl_common_defs.cfg

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
   export UOW_FROM_DATE=$(print $UOW_FROM | cut -c1-8)
   export UOW_FROM_HH=$(print $UOW_FROM | cut -c9-10)
   export UOW_FROM_MI=$(print $UOW_FROM | cut -c11-12)
   export UOW_FROM_SS=$(print $UOW_FROM | cut -c13-14)
   export UOW_TO_DATE=$(print $UOW_TO | cut -c1-8)
   export UOW_TO_HH=$(print $UOW_TO | cut -c9-10)
   export UOW_TO_MI=$(print $UOW_TO | cut -c11-12)
   export UOW_TO_SS=$(print $UOW_TO | cut -c13-14)
   export UOW_DATE=$UOW_TO_DATE
   export IN_DIR=$IN_DIR/$TABLE_ID/$UOW_TO_DATE/$UOW_TO_HH/$UOW_TO_MI/$UOW_TO_SS
elif [[ ($UOW_FROM_FLAG -eq 1 && $UOW_TO_FLAG -eq 0) || ($UOW_TO_FLAG -eq 1 && $UOW_FROM_FLAG -eq 0) ]]
then
   print "Missing required UOW (FROM or TO) parameter."
   usage_tpt
   exit 1
fi

set +e
$DW_MASTER_BIN/dw_infra.loader_cleanup.ksh $JOB_ENV $JOB_TYPE_ID
RCODE=$?
set -e

if [ $RCODE != 0 ]
then
  print "${0##*/}: FATAL ERROR: Executing $DW_MASTER_BIN/dw_infra.loader_cleanup.ksh" >2
  exit 4
fi

exit 0
