#!/bin/ksh -eu
#------------------------------------------------------------------------------------------------
# Filename:     dw_infra.multi_etl_to_hdfs_copy.ksh
# Description:  Wrapper to submit copy jobs on multiple hosts in parallel
# Location:     $DW_MASTER_BIN
#
# Revision History:
#
# Name             Date            Description
# ---------------  --------------  ---------------------------------------------------
# Michael Weng     10/26/2017      Initial
# Michael Weng     06/12/2018      Update command line parameters
#
#------------------------------------------------------------------------------------------------

export ETL_ID=$1
HD_CLUSTER=$2
ETL_DIR=$3
ETL_TABLE=$4
HD_PATH=$5
HD_TABLE=$6
UOW_TO_FLAG=$7

if [[ $# != 7 ]]
then
  print "${0##*/}: INFRA_ERROR - Invalid command line options"
  print "Usage: $0 <ETL_ID> <HD_CLUSTER> <ETL_DIR> <ETL_TABLE> <HD_PATH> <HD_TABLE> <UOW_TO_FLAG>"
  exit 4
fi

. /dw/etl/mstr_cfg/etlenv.setup
. $DW_MASTER_LIB/dw_etl_common_functions.lib
. $DW_MASTER_CFG/.${HD_CLUSTER}_env.sh
. $DW_MASTER_CFG/hadoop.login

### Cleanup target stage table directory and re-create a new one on HDFS
print "Cleaning up target and re-creating new directory on $HD_CLUSTER: $HD_PATH"
set +e
hadoop fs -rm -r -skipTrash $HD_PATH
hadoop fs -mkdir -p $HD_PATH
retcode=$?
set -e

if [ $retcode != 0 ]
then
  print "${0##*/}: WARNING - Failed to cleanup and re-create target directory $HD_CLUSTER:$HD_PATH"
  exit 4
fi

assignTagValue MULTI_HOST MULTI_HOST $ETL_CFG_FILE W 1

SRC_NODE_LIST=""
if [ $MULTI_HOST = 0 ]
then
  HOSTS_LIST_FILE=$DW_CFG/$ETL_ID.host.lis
  if [ ! -f $HOSTS_LIST_FILE ]
  then
    print "${0##*/}: WARNING: MULTI_HOST is zero, and $HOSTS_LIST_FILE does not exist" >&2
    exit 4
  fi
elif [[ $MULTI_HOST = 1 ]]
then
  SRC_NODE_LIST=$servername
elif [[ $MULTI_HOST = @(2||4||6||8||16||32) ]]
then
  HOSTS_LIST_FILE=$DW_MASTER_CFG/${servername%%.*}.${MULTI_HOST}ways.host.lis
  if [ ! -f $HOSTS_LIST_FILE ]
  then
    print "${0##*/}: WARNING: MULTI_HOST is set to $MULTI_HOST, and $HOSTS_LIST_FILE does not exist" >&2
    exit 4
  fi
else
  print "${0##*/}: WARNING: MULTI_HOST value not valid $MULTI_HOST" >&2
  exit 4
fi

if [[ $MULTI_HOST != 1 ]]
then
  while read SRC_NODE junk
  do
    SRC_NODE_LIST="${SRC_NODE_LIST} ${SRC_NODE}"
  done < $HOSTS_LIST_FILE
fi

set -A SRC_HOSTS $SRC_NODE_LIST
export SRC_NODE_LIST
export SRC_HOST_CNT=${#SRC_HOSTS[*]}

print "SRC_NODE_LIST is $SRC_NODE_LIST"
print "SRC_HOST_CNT is $SRC_HOST_CNT"

HADOOP_LOAD_JOB="$DW_MASTER_BIN/dw_infra.single_etl_to_hdfs_copy.ksh"
PROCESS="single_etl_to_hdfs_copy"

### Launch the instances at different hosts
print "Begin job launching for ETL to HDFS copy for table $HD_TABLE ... `date`"
set -A pid_list
set -A pid_list_rcode

UOWTO_BATCHSEQNUM=$UOW_TO
if [ $UOW_TO_FLAG = 0 ]
then
  UOWTO_BATCHSEQNUM=$BATCH_SEQ_NUM
fi

instance_idx=0
while [[ $instance_idx -lt $SRC_HOST_CNT ]]
do
  HOST_NAME=${SRC_HOSTS[${instance_idx}]}
  LOG_FILE=$DW_SA_LOG/$HD_TABLE.$JOB_TYPE_ID.$PROCESS.$HD_CLUSTER.$instance_idx.$CURR_DATETIME.log

  ### Calling into $HADOOP_LOAD_JOB, for the last two parameters, if $UOW_TO_FLAG is 1, 
  ### pass in $UOW_TO. Otherwise if UOW_TO_FLAG is 0, pass in $BATCH_SEQ_NUM instead.
  print "***********************************************************************"
  print "Launching instance $instance_idx on $HOST_NAME with log file: $LOG_FILE"
  if [[ ${HOST_NAME%%.*} == ${servername%%.*} ]]
  then
    set +e
    $HADOOP_LOAD_JOB $ETL_ID $JOB_ENV $HD_CLUSTER $HD_TABLE $CURR_DATETIME $SRC_HOST_CNT $instance_idx $ETL_DIR $ETL_TABLE $HD_PATH $UOW_TO_FLAG $UOWTO_BATCHSEQNUM > $LOG_FILE 2>&1 &
    pid_list[$instance_idx]=$!
    set -e
  else
    set +e
    ssh -nq $HOST_NAME $HADOOP_LOAD_JOB $ETL_ID $JOB_ENV $HD_CLUSTER $HD_TABLE $CURR_DATETIME $SRC_HOST_CNT $instance_idx $ETL_DIR $ETL_TABLE $HD_PATH $UOW_TO_FLAG $UOWTO_BATCHSEQNUM > $LOG_FILE 2>&1 &
    pid_list[$instance_idx]=$!
    set -e
  fi
  print "***********************************************************************"
  ((instance_idx++))
done

### Wait and capture all pids
instance_idx=0
while [[ $instance_idx -lt ${#pid_list[*]} ]]
do
  set +e
  wait ${pid_list[$instance_idx]}
  pid_list_rcode[$instance_idx]=$?
  set -e
  ((instance_idx++))
done

### Check return code for all pids
error_yn=0
instance_idx=0
while [[ $instance_idx -lt ${#pid_list_rcode[*]} ]]
do
  LOG_FILE=$DW_SA_LOG/$HD_TABLE.$JOB_TYPE_ID.$PROCESS.$HD_CLUSTER.$instance_idx.$CURR_DATETIME.log
  if [ ${pid_list_rcode[$instance_idx]} = 0 ]
  then
    print "Instance $instance_idx is complete"
  else
    print "${0##*/}:  WARNING, instance $instance_idx is not complete. See log file $LOG_FILE" >&2
    error_yn=1
  fi
  ((instance_idx++))
done

if [[ $error_yn != 0 ]]
then
  print "${0##*/}:  WARNING, not all copy jobs finished successfully for table $HD_TABLE. `date`"
  exit 4
fi

print "${0##*/}:  all copy jobs finished successfully for table $HD_TABLE. `date`"
exit 0
