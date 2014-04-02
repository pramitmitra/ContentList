#!/bin/ksh -eu
#------------------------------------------------------------------------------------------------
# Title:        Hadoop extract file Handler
# File Name:    hadoop_extract_file_handler.ksh
# Description:  Handler submits multiple instances of extract job on different hosts.
# Developer:
# Created on:
# Location:     $DW_BIN
# Logic:
#
# Revision History:
#
# Name             Date            Description
# ---------------  --------------  ---------------------------------------------------
# ???              ??/??/????      Initial Creation
# Ryan Wong        10/04/2013      Redhat changes
#
#------------------------------------------------------------------------------------------------

# Input Params
_etl_id=
_uow_id=

while getopts "i:u:" opt
do
case $opt in
   i)   _etl_id="$OPTARG";;
   u)   _uow_id="$OPTARG";;
   \?)  print >&2 "Usage: $0 -i <ETL_ID> -u [UOW_ID]"
   return 1;;
esac
done
shift $(($OPTIND - 1))


if [[ X"$_etl_id" = X""  ]]
  then
    print "Usage: $0 -i <ETL_ID> -u [UOW_ID]"
  exit 4
fi
export ETL_ID=$_etl_id
export UOW_ID=${_uow_id:-""}
export JOB_ENV=extract
export JOB_TYPE="hdp_file_extract"
export JOB_TYPE_ID="hfex"

. /dw/etl/mstr_cfg/etlenv.setup
. $DW_MASTER_CFG/dw_etl_common_defs.cfg
. $DW_MASTER_LIB/dw_etl_common_functions.lib

COMP_FILE=$DW_SA_TMP/$TABLE_ID.$JOB_TYPE.complete
BATCH_SEQ_NUM_FILE=$DW_SA_DAT/$TABLE_ID.$JOB_TYPE.batch_seq_num.dat
TABLE_LIS_FILE=$DW_CFG/$ETL_ID.sources.lis
PARENT_ERROR_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.hadoop_extract_file_handler.$CURR_DATETIME.err
HADOOP_EXTRACT_JOB="$DW_BIN/hadoop_extract_file.ksh"

# get UOW_ID
PREV_UOW_ID=""
if [ -z $UOW_ID ]
then
  if [ -s $BATCH_SEQ_NUM_FILE ]
  then
    PREV_UOW_ID=$(<$BATCH_SEQ_NUM_FILE)
    ((UOW_ID=PREV_UOW_ID+1))
  else
    print "${0##*/}:  ERROR, UOW_ID or BATCH_SEQ_NUM_FILE has to be provided!" >&2
    exit 4
  fi
fi
export UOW_ID

assignTagValue N_WAY_HOST N_WAY_HOST $ETL_CFG_FILE W 0

SRC_NODE_LIST=""
if [ $N_WAY_HOST = 0 ]
then
  export INGESTS_LIST_FILE=$DW_CFG/$ETL_ID.ingests.lis
elif [[ $N_WAY_HOST == @(1|2|4|6|8|16|32) ]]
then
  export INGESTS_LIST_FILE=$DW_CFG/ingest_${N_WAY_HOST}ways.host.lis
else
  print "${0##*/}:  ERROR, N_WAY_HOST only support 1, 2, 4, 6, 8, 16 & 32 for now." >&2
  exit 4
fi

if [ -f $INGESTS_LIST_FILE ]
then
  while read SRC_NODE junk
  do
    SRC_NODE_LIST="${SRC_NODE_LIST} ${SRC_NODE}"
  done < $INGESTS_LIST_FILE
else
  print "${0##*/}:  ERROR, Couldn't find $INGESTS_LIST_FILE file." >&2
  exit 4
fi

set -A SRC_HOSTS $SRC_NODE_LIST
export SRC_NODE_LIST
export SRC_HOST_CNT=${#SRC_HOSTS[*]}

if [ ! -f $COMP_FILE ]
then
  # COMP_FILE does not exist.  1st run for this processing period.
  FIRST_RUN=Y
else
  FIRST_RUN=N
fi

# Source the error message handling logic.  On failure, trap will send the contents of the PARENT_ERROR_FILE to the
# subject area designated email addresses.
. $DW_MASTER_LIB/message_handler

print "
##########################################################################################################
#
# Beginning Hadoop file extract for ETL_ID: $ETL_ID, UOW_ID: $UOW_ID   `date`
#
###########################################################################################################
"

if [ $FIRST_RUN = Y ]
then
  # Need to run the clean up process since this is the first run for the current processing period.
  COMMAND_SCRIPT="$DW_SA_TMP/$TABLE_ID.cleanup.$JOB_TYPE_ID.command.dat"
  > $COMMAND_SCRIPT
  host_idx=0
  while [[ $host_idx -lt $SRC_HOST_CNT ]];
  do
    HOST_NAME=${SRC_HOSTS[${host_idx}]}
    LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.loader_cleanup.$HOST_NAME.$CURR_DATETIME.log
    print "(/usr/bin/ssh -q $HOST_NAME \"$DW_EXE/loader_cleanup_sg.ksh $JOB_ENV $JOB_TYPE_ID $ETL_ID $JOB_TYPE $CURR_DATETIME\" 2>$LOG_FILE) || (print \"ERROR - Failure run loader_cleanup process on $HOST_NAME\" >>$LOG_FILE) &">>$COMMAND_SCRIPT
    host_idx=$(( $host_idx + 1 ))
  done
  print "wait" >>$COMMAND_SCRIPT
  set +e
  . $COMMAND_SCRIPT
  RCODE=$?
  set -e

  host_idx=0;
  error_yn=0;
  while [[ $host_idx -lt $SRC_HOST_CNT ]]
  do
    HOST_NAME=${SRC_HOSTS[${host_idx}]}
    LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.loader_cleanup.$HOST_NAME.$CURR_DATETIME.log
    if [[ -s $LOG_FILE ]]
    then
      print "${0##*/}:  ERROR, see log file $LOG_FILE" >&2
      error_yn=1;
    fi
    host_idx=$(( $host_idx + 1 ))
  done
  if [[ $error_yn -ne 0 || $RCODE -ne 0 ]]
  then
    exit 4
  fi
  > $COMP_FILE
else
  print "loader_cleanup.ksh process already complete"
fi

PROCESS_MSG="hadoop_extract_file"
RCODE=`grepCompFile "$PROCESS_MSG" $COMP_FILE`
if [ $RCODE = 1 ]
then

  # run wc -l on $ETL_ID.sources.lis file to know how many tables to unload (1 or > 1).
  wc -l $TABLE_LIS_FILE | read TABLE_COUNT FN
  if [[ $TABLE_COUNT -eq 1 ]]
  then
    print "Processing single hadoop file extract for TABLE_ID: $TABLE_ID  `date`"

    read RECORD_ID HDP_CONN PARALLEL_NUM SOURCE_FILE TARGET_FILE MISC < $TABLE_LIS_FILE

    LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.single_hadoop_extract_file.$CURR_DATETIME.log
    set +e
    eval $DW_EXE/single_hadoop_extract_file.ksh $ETL_ID $RECORD_ID $HDP_CONN > $LOG_FILE 2>&1
    rcode=$?
    set -e

    if [ $rcode != 0 ]
    then
      print "${0##*/}:  ERROR, see log file $LOG_FILE" >&2
      exit 4
    fi
  elif [[ $TABLE_COUNT -gt 1 ]]
  then
    print "Processing multiple hadoop file extract for TABLE_ID: $TABLE_ID `date`"

    export MULTI_COMP_FILE=$DW_SA_TMP/$TABLE_ID.multi_hadoop_extract_file.complete
    export PARENT_LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.run_hadoop_extract_file.$CURR_DATETIME.log
    export ERROR_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.multi_hadoop_extract_file.$CURR_DATETIME.err  # job error file

    # If the MULTI_COMP_FILE does not exist, this is the first run, otherwise it is a restart.
    if [ ! -f $MULTI_COMP_FILE ]
    then
      > $MULTI_COMP_FILE
    fi

    # remove previous $EXTRACT_CONN_TYPE list files to ensure looking for the correct set of data files for this run.
    rm -f $DW_SA_TMP/$TABLE_ID.*.hdp.*.lis

    # Create a list of files to be processed per extract database server.

    while read RECORD_ID HDP_CONN PARALLEL_NUM SOURCE_FILE TARGET_FILE PARAM_LIST
    do
      eval HDP_CONN=$HDP_CONN

      if [ ! -f $DW_SA_TMP/$TABLE_ID.$HDP_CONN.*.lis ]
      then
        eval print $RECORD_ID $HDP_CONN > $DW_SA_TMP/$TABLE_ID.$HDP_CONN.$PARALLEL_NUM.lis
      else
        eval print $RECORD_ID $HDP_CONN >> $DW_SA_TMP/$TABLE_ID.$HDP_CONN.$PARALLEL_NUM.lis
      fi
    done < $TABLE_LIS_FILE

    for FILE in $(ls $DW_SA_TMP/$TABLE_ID.*.hdp.*.lis)
    do
      HDP_CONN=${FILE#$DW_SA_TMP/$TABLE_ID.}
      HDP_CONN=${HDP_CONN%%.*}

      LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.$HDP_CONN.run_multi_hadoop_extract_file.$CURR_DATETIME.log
      print "Running run_multi_hadoop_extract_file.ksh $FILE  `date`"
      COMMAND="$DW_EXE/run_multi_hadoop_extract_file.ksh $FILE $PARENT_LOG_FILE > $LOG_FILE 2>&1"

      set +e
      eval $COMMAND || print "${0##*/}: ERROR, failure processing for $FILE, see log file $LOG_FILE" >>$ERROR_FILE &
      set -e
    done

    wait

    if [ -f $ERROR_FILE ]
    then
      cat $ERROR_FILE >&2
      exit 4
    fi

    rm $MULTI_COMP_FILE

  else
    print "${0##*/}:  ERROR, no rows exist in file $TABLE_LIS_FILE" >&2
    exit 4
  fi

  print "$PROCESS_MSG" >> $COMP_FILE

elif [ $RCODE = 0 ]
then
  print "$PROCESS_MSG process already complete"
else
  exit $RCODE
fi

######################################################################################################
#
#                                Increment BSN (Optional)
#
#  This section updates the batch_seq_number and creates the watch_file.  It is now in a non-repeatable
#  Section to avoid issues of restartability.
#
######################################################################################################
PROCESS_MSG="Increment_BSN"
RCODE=`grepCompFile $PROCESS_MSG $COMP_FILE`

if [ $RCODE = 1 ]
then

 print "Updating the batch sequence number file  `date`"
 print $UOW_ID > $BATCH_SEQ_NUM_FILE

 print "$PROCESS_MSG" >> $COMP_FILE

elif [ $RCODE = 0 ]
then
  print "$PROCESS_MSG already complete"
else
  exit $RCODE
fi

print "Removing the complete file  `date`"
rm -f $COMP_FILE

print "
##########################################################################################################
#
# Extract for ETL_ID: $ETL_ID , UOW_ID: $UOW_ID  is successfully Completed    `date`
#
##########################################################################################################"

tcode=0
exit
