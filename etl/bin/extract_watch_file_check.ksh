#!/bin/ksh -eu
#------------------------------------------------------------------------------------------------
# This script will determine the watch file name that needs to exist for the current load process to run.
# It will check for the watch file and if found exit successfully.  If the watch file is not found, it
# will sleep for 60 seconds and look again.  It will continue in this loop until the watch file is found.
#
# Change Log:
# 2007/02/12  Orlando Jin  Change for User Host Splict - Adding 20th Host project
# 2013/10/04  Ryan Wong    Redhat changes
#
#------------------------------------------------------------------------------------------------

if [ $# != 2 ]
then
  print "Usage:  $0 <ETL_ID> <JOB_ENV>   # JOB_ENV = extract|primary|secondary"
  exit 4
fi

export ETL_ID=$1
JOB_ENV=$2

export SUBJECT_AREA=${ETL_ID%%.*}
export TABLE_ID=${ETL_ID##*.}
SCRIPT_NAME=${0##*/}

if [ $JOB_ENV = extract ]
then
  export JOB_TYPE=extract
  export JOB_TYPE_ID=ex
else
  export JOB_TYPE=load
  export JOB_TYPE_ID=ld
fi

#. /export/home/abinitio/cfg/abinitio.setup
.  /dw/etl/mstr_cfg/etlenv.setup

CURR_DATETIME=$(date '+%Y%m%d-%H%M%S')
export DW_SA_DAT=$DW_DAT/$JOB_ENV/$SUBJECT_AREA
export DW_SA_LOG=$DW_LOG/$JOB_ENV/$SUBJECT_AREA
PARENT_ERROR_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.${SCRIPT_NAME%.ksh}.$CURR_DATETIME.err

. $DW_LIB/message_handler

# Get the BATCH_SEQ_NUM for the last extract file to be loaded (may increment by more than one for frequent extracts)
BATCH_SEQ_NUM=$($DW_EXE/get_batch_seq_num.ksh)

# Add for UHS project 2007/02/12
set +e
grep "^USER_HOST_SET\>" $DW_CFG/$ETL_ID.cfg | read PARAM USER_HOST_SET COMMENT
rcode=$?
set -e

if [ $rcode != 0 ]
then
  WATCH_FILE=$DW_WATCH/extract/$ETL_ID.extract.$BATCH_SEQ_NUM.done
else
  WATCH_FILE=$DW_WATCH/extract/$ETL_ID.extract.userhost.$BATCH_SEQ_NUM.done
fi
# End for UHS project change

WATCH_FILE_NAME=${WATCH_FILE##*/}

#print $WATCH_FILE
#print $WATCH_FILE_NAME

while true
do
  if [ ! -f $WATCH_FILE ]
  then
    print "Waiting for watch file $WATCH_FILE_NAME  `date`"
    sleep 60
  else
    print "Found watch file $WATCH_FILE_NAME  `date`"
    break
  fi
done

tcode=0
exit
