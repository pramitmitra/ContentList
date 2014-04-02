#!/bin/ksh -eu

#------------------------------------------------------------------------------------------------
# Filename:     post_extract_cleanup_handler.ksh 
# Description:  Sets up the environment to run $DW_EXE/loader_cleanup.ksh for post extract
#               jobs. Intended for use in conjunction with single_table_extract_handler.ksh
#               for situational post extract jobs. For example, a post extract job that takes
#               the original extract(s) created by single_table_extract_handler.ksh and uses
#               it to create a separate data file using its own ETL_ID for loading could
#               leverage this handler to clean up from previous runs. This process would run
#               prior to the main post extract job.
#                
#
# Developer:    Kevin Oaks
# Created on:   04/20/2006 
# Location:     $DW_EXE/
#
# Execution:    $DW_EXE/post_extract_cleanup_handler.ksh <ETL_ID> <JOB_ENV> <JOB_TYPE> <JOB_TYPE_ID>
#               In normal use, all but ETL_ID are inherited from single_table_extract.ksh
#
# Parameters:   JOB_TYPE_ID = <ex|ld|bt>
#
# Revision History:
#
# Name             Date            Description
# ---------------  --------------  --------------------------------------------------------------
# Kevin Oaks       04/20/2006      Initial Creation
# Ryan Wong        01/10/2012      Switched to etlenv.setup, also add date based log dir
# Ryan Wong        02/13/2012      Fixing logic for DW_SA_LOG, check if CURR_DATE is appended
# Ryan Wong        10/04/2013      Redhat changes
#
#------------------------------------------------------------------------------------------------


export ETL_ID=$1
export JOB_ENV=${2:-$JOB_ENV}
export JOB_TYPE=${3:-$JOB_TYPE}
export JOB_TYPE_ID=${4:-$JOB_TYPE_ID}

. /dw/etl/mstr_cfg/etlenv.setup

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

# Need to run the clean up process for the subsequent post extract process.
LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.loader_cleanup.$CURR_DATETIME.log

print "Running loader_cleanup.ksh for JOB_ENV: $JOB_ENV, JOB_TYPE_ID: $JOB_TYPE_ID  `date`"

set +e
$DW_EXE/loader_cleanup.ksh $JOB_ENV $JOB_TYPE_ID > $LOG_FILE 2>&1
rcode=$?
set -e

if [ $rcode != 0 ]
then
    print "${0##*/}:  ERROR, see log file $LOG_FILE" >&2
    exit 4
else
    print "${0##*/}: Cleanup complete." >&2
fi

exit
