#!/bin/ksh -eu
#------------------------------------------------------------------------------------------------
# Filename:     dw_infra.single_ingest_hdfs_load.ksh
#
# Revision History:
#
# Name             Date            Description
# ---------------  --------------  ---------------------------------------------------
# ???              ??/??/????      Initial Creation
# Ryan Wong        10/04/2013      Redhat changes
#
#------------------------------------------------------------------------------------------------

ETL_ID=$1
JOB_ENV=$2
shift 2
UOW_PARAM_LIST_AB=$*

. $DW_MASTER_LIB/dw_etl_common_functions.lib

assignTagValue MULTI_HOST MULTI_HOST $ETL_CFG_FILE W 0
if [ $MULTI_HOST = 0 ]
then
  HOSTS_LIST_FILE=$DW_CFG/$ETL_ID.host.lis
  if [ ! -f $HOSTS_LIST_FILE ]
  then
    print "${0##*/}:  FATAL INFRA_ERROR: MULTI_HOST is zero, and $HOST_LIST_FILE does not exist" >&2
    exit 4
  fi
elif [[ $MULTI_HOST = 1 ]]
then
  LOAD_RUN_NODE=$servername
elif [[ $NWAY_HOSTS = @(2||4||6||8||16||32) ]]
then
  HOSTS_LIST_FILE=$DW_MASTER_CFG/${servername%%.*}.${MULTI_HOST}ways.host.lis
else
  print "${0##*/}:  FATAL INFRA_ERROR: MULTI_HOST valid value $MULTI_HOST" >&2
  exit 4
fi

SRC_NODE_LIST=""

if [[ $MULTI_HOST = 1 ]]
then
  SRC_NODE_LIST=$servername
elif [ -f $HOSTS_LIST_FILE ]
then
  while read SRC_NODE junk
  do
    SRC_NODE_LIST="${SRC_NODE_LIST} ${SRC_NODE}"
  done < $HOSTS_LIST_FILE
else
  print "${0##*/}:  INFRA_ERROR, Couldn't find $HOSTS_LIST_FILE file." >&2
  exit 4
fi

set -A SRC_HOSTS $SRC_NODE_LIST
export SRC_NODE_LIST
export SRC_HOST_CNT=${#SRC_HOSTS[*]}


HADOOP_LOAD_JOB="$DW_MASTER_BIN/dw_infra.ingest_hadoop_load_file.ksh"

export N_WAY_COMP_FILE=$DW_SA_TMP/$TABLE_ID.$JOB_TYPE_ID.single_hadoop_load.complete
if [ ! -f $N_WAY_COMP_FILE ]
then
  > $N_WAY_COMP_FILE
fi

#Launch the instances at different hosts
COMMAND_SCRIPT="$DW_SA_TMP/$TABLE_ID.$JOB_TYPE_ID.hadoop_load_file.command.dat"
> $COMMAND_SCRIPT
instance_idx=0
while [[ $instance_idx -lt $SRC_HOST_CNT ]]
do
  RCODE=`grepCompFile "hdp_load_file for $instance_idx" $N_WAY_COMP_FILE`
  if [ $RCODE = 1 ]
  then
    HOST_NAME=${SRC_HOSTS[${instance_idx}]}

    LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.hdp_load_file.$instance_idx.$CURR_DATETIME.log
    if [[ ${HOST_NAME%%.*} != `hostname` ]]
    then
      print "print \"Launching instance $instance_idx on $HOST_NAME ...\"" >> $COMMAND_SCRIPT
      print "(/usr/bin/ssh -q $HOST_NAME \". $HOME/.profile;ksh $HADOOP_LOAD_JOB $ETL_ID $JOB_ENV $BATCH_SEQ_NUM $instance_idx $CURR_DATETIME $UOW_PARAM_LIST_AB\" > $LOG_FILE 2>&1) || (print \"INFRA_ERROR - Failure run hadoop_load_file process on $HOST_NAME\" >>$LOG_FILE) &" >>$COMMAND_SCRIPT
    else
      print "print \"Launching instance $instance_idx on $HOST_NAME ...\"" >> $COMMAND_SCRIPT
      print "ksh $HADOOP_LOAD_JOB $ETL_ID $JOB_ENV $BATCH_SEQ_NUM $instance_idx $CURR_DATETIME $UOW_PARAM_LIST_AB > $LOG_FILE 2>&1 &" >> $COMMAND_SCRIPT
    fi
  fi
  instance_idx=$(( $instance_idx + 1 ))
done

print "wait" >>$COMMAND_SCRIPT
set +e
. $COMMAND_SCRIPT
csrcode=0
set -e

#########################################################################################################
#
#       loop through each instance, wait for it to finish, and capture return code
#
#########################################################################################################
max_rc=0
instance_idx=0
error_yn=0
while [[ $instance_idx -lt $SRC_HOST_CNT ]]
do
  RCODE=`grepCompFile "hdp_load_file for $instance_idx" $N_WAY_COMP_FILE`
  if [ $RCODE = 1 ]
  then
    LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.hdp_load_file.$instance_idx.$CURR_DATETIME.log

    set +e
    grep 'INFRA_ERROR' $LOG_FILE >/dev/null 2>&1
    errcnt=$?
    set -e
    if [[ $errcnt -ne 0 && $csrcode -eq 0 ]]
    then
      print "Instance $instance_idx is complete"
      print "hdp_load_file for $instance_idx" >> $N_WAY_COMP_FILE
    else
      print "${0##*/}:  INFRA_ERROR, see log file $LOG_FILE" >&2
      error_yn=1
    fi
  fi

  instance_idx=$(( $instance_idx + 1 ))
done

if [[ $error_yn -ne 0 || $csrcode -ne 0 ]]
then
  exit 4
else
  rm $N_WAY_COMP_FILE
  exit 0
fi
