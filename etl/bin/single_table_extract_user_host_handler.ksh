#!/bin/ksh -eu
######################################################################################
# Title:       User Host Split specific single table extract module
# Filename:    single_table_extract_user_host_handler.ksh
# Description: it's a wrapper around single_table_extract_hanlder.ksh for user host
#              split project specific requirement.
#
# Developer:   Orlando Jin
# Created on:  02/13/2007
# Location:    $DW_EXE/
# Logic:
#
# Called BY    Appworx
#
# Input
#   Parameters          : <ETL_ID>
#   Prev. Set Variables :
#   Tables, Views       : N/A
#
# Output/Return Code    :
#   0 - success
#   otherwise error
#
# Last Error Number:
#
# Date        Modified By(Name)       Change and Reason for Change
# ----------  ----------------------  ---------------------------------------
# 02/13/2007  Orlando Jin             Initial Program
# 10/04/2013  Ryan Wong               Redhat changes
######################################################################################

SCRIPT_NAME=${0##*/}

if [ $# != 1 ]
then
  print "Usage: $SCRIPT_NAME <ETL_ID>"
  exit 4
fi

export ETL_ID=$1
export JOB_ENV=extract
export JOB_TYPE=extract
export JOB_TYPE_ID=exuhs
export SUBJECT_AREA=${ETL_ID%%.*}
export TABLE_ID=${ETL_ID##*.}

. /dw/etl/mstr_cfg/etlenv.setup

export DW_SA_DAT=$DW_DAT/$JOB_ENV/$SUBJECT_AREA
export DW_SA_IN=$DW_IN/$JOB_ENV/$SUBJECT_AREA
export DW_SA_LOG=$DW_LOG/$JOB_ENV/$SUBJECT_AREA
export DW_SA_TMP=$DW_TMP/$JOB_ENV/$SUBJECT_AREA

export CURR_DATETIME=$(date '+%Y%m%d-%H%M%S')

UHS_COMP_FILE=$DW_SA_TMP/$TABLE_ID.uhs.complete
BATCH_SEQ_NUM_FILE=$DW_SA_DAT/$TABLE_ID.$JOB_TYPE.batch_seq_num.dat
BATCH_SEQ_NUM_FILE_BAK=$DW_SA_TMP/$TABLE_ID.$JOB_TYPE.batch_seq_num.dat.bak
TABLE_LIS_FILE=$DW_CFG/$ETL_ID.sources.lis
export CURR_DATETIME=$(date '+%Y%m%d-%H%M%S')
export CURR_DATE=$(date '+%Y%m%d')
PARENT_ERROR_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.single_table_extract_user_host_handler.$CURR_DATETIME.err

#-------------------------------------------------------------------------------------
# Source the error message handling logic.  On failure, trap will send the contents
# of the PARENT_ERROR_FILE to the subject area designated email addresses.
#-------------------------------------------------------------------------------------
. $DW_LIB/message_handler

set +e
grep "^USER_HOST_SET\>" $DW_CFG/$ETL_ID.cfg | read PARAM USER_HOST_SET COMMENT
rcode=$?
set -e

if [ $rcode != 0 ]
then
  print "$SCRIPT_NAME: ERROR, failure determining value for USER_HOST_SET from $DW_CFG/$ETL_ID.cfg" >&2
  exit 4
fi

# Wait for the init job done.
while [ ! -f $DW_WATCH/$JOB_ENV/init_user_host_sources_lis.done.$CURR_DATE ]
do
  sleep 30
  continue
done

if [ ! -f $DW_CFG/$USER_HOST_SET.sources.lis ]
then
  print "$SCRIPT_NAME: ERROR, $DW_CFG/$USER_HOST_SET.sources.lis doesn't existed!" >&2
  exit 4
fi

wc -l $DW_CFG/$USER_HOST_SET.sources.lis | read LINE_NUM_BASE FN

cat $DW_CFG/$ETL_ID.sources.lis.uhs.* | wc -l | read LINE_NUM_SP FN

if [ $LINE_NUM_BASE != $LINE_NUM_SP ]
then
  print "$SCRIPT_NAME: ERROR, $ETL_ID.sources.lis.uhs.* aren't correct!" >&2
  exit 4
fi

if [ ! -f $UHS_COMP_FILE ]
then
  # UHS_COMP_FILE does not exist. 1st run for this processing period.
  FIRST_RUN=Y
else
  FIRST_RUN=N
fi

# get BATCH_SEQ_NUM
if [ $FIRST_RUN = "Y" ]
then
  PREV_BATCH_SEQ_NUM=$(<$BATCH_SEQ_NUM_FILE)
else
  PREV_BATCH_SEQ_NUM=$(<$BATCH_SEQ_NUM_FILE_BAK)
fi
((BATCH_SEQ_NUM=PREV_BATCH_SEQ_NUM+1))
export BATCH_SEQ_NUM

print "
#####################################################################################################
#
# Beginning ordered multiple-host extract for ETL_ID: $ETL_ID, BATCH_SEQ_NUM: $BATCH_SEQ_NUM  `date`
#
#####################################################################################################
"

if [ $FIRST_RUN = "Y" ]
then
  # Backup batch sequence number dat files
  print "Backup $BATCH_SEQ_NUM_FILE files `date`"
  cp $BATCH_SEQ_NUM_FILE $BATCH_SEQ_NUM_FILE_BAK

  print "Running loader_cleanup.ksh for JOB_ENV: $JOB_ENV, JOB_TYPE_ID: $JOB_TYPE_ID  `date`"
  LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.loader_cleanup.$CURR_DATETIME.log

  set +e
  $DW_EXE/loader_cleanup.ksh $JOB_ENV $JOB_TYPE_ID > $LOG_FILE 2>&1
  rcode=$?
  set -e

  if [ $rcode != 0 ]
  then
    print "${0##*/}:  ERROR, see log file $LOG_FILE" >&2
    exit 4
  fi

  # Create COMP file
  print "Generate $UHS_COMP_FILE `date`"
  > $UHS_COMP_FILE
else
  print "Preparation process is already complete."
fi

######################################################################################
#
# Sequential extract processing
#
######################################################################################
ls $DW_CFG/$ETL_ID.sources.lis.uhs.* | wc -l | read FILE_COUNT FN
if [ $FILE_COUNT = 0 ]
then
  print "$SCRIPT_NAME: ERROR, failure finding $ETL_ID.sources.lis.uhs.* files." >&2
  exit 4
fi

print "Processing multiple host sequential extract for TABLE_ID: $TABLE_ID `date`"
for FILE in $(ls $DW_CFG/$ETL_ID.sources.lis.uhs.* | sort -t . -n -k 6)
do
  HOST_ORDER=${FILE##*.}

  set +e
  grep "^$FILE$" $UHS_COMP_FILE >/dev/null
  rcode=$?
  set -e

  if [ $rcode = 1 ]
  then
    # Copy related files
    cp $BATCH_SEQ_NUM_FILE_BAK $BATCH_SEQ_NUM_FILE
    cp $FILE $TABLE_LIS_FILE

    export LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.single_table_extract_handler.$HOST_ORDER.$CURR_DATETIME.log

    print "$ETL_ID.$HOST_ORDER extract starting `date`"
    set +e
    eval $DW_EXE/single_table_extract_handler.ksh $ETL_ID > $LOG_FILE 2>&1
    rcode=$?
    set -e

    if [ $rcode != 0 ]
    then
      print "ERROR, failure processing for $ETL_ID.$HOST_ORDER extract  `date`"
      print "$SCRIPT_NAME: ERROR, failure processing for $ETL_ID.$HOST_ORDER extract, see log file $LOG_FILE" >&2
      exit 4
    else
      cp $DW_SA_IN/$TABLE_ID.record_count.dat.$BATCH_SEQ_NUM $DW_SA_TMP/$TABLE_ID.record_count.dat.$BATCH_SEQ_NUM.$HOST_ORDER
      print "$FILE" >> $UHS_COMP_FILE
      print "$ETL_ID.$HOST_ORDER extract complete `date`"
    fi
  else
    print "$ETL_ID.$HOST_ORDER extract has been completed `date`"
    continue
  fi
done

integer j
integer i
j=0
i=0

print "Sum the actual record count `date`"
for RECORD_COUNT_FILE in $DW_SA_TMP/$TABLE_ID.record_count.dat.$BATCH_SEQ_NUM.*
do
i=$(<$RECORD_COUNT_FILE)
((j+=$i))
done

print "$j" > $DW_SA_IN/$TABLE_ID.record_count.dat.$BATCH_SEQ_NUM

print "Concatenate $ETL_ID.sources.lis.uhs.* files to $ETL_ID.sources.lis `date` "
cat $DW_CFG/$ETL_ID.sources.lis.uhs.* > $TABLE_LIS_FILE

print "Creating the watch file $ETL_ID.$JOB_TYPE.userhost.$BATCH_SEQ_NUM.done `date`"
> $DW_WATCH/$JOB_ENV/$ETL_ID.$JOB_TYPE.userhost.$BATCH_SEQ_NUM.done

print "Removing the temporary record count & batch seq num backup files `date`"
rm -f $DW_SA_TMP/$TABLE_ID.record_count.dat.$BATCH_SEQ_NUM.*
rm -f $BATCH_SEQ_NUM_FILE_BAK

print "Removing the complete file `date`"
rm -f $UHS_COMP_FILE

print "
#####################################################################################################
#
# Extract for ETL_ID: $ETL_ID, BATCH_SEQ_NUM: $BATCH_SEQ_NUM complete  `date`
#
#####################################################################################################
"

tcode=0
exit 0
