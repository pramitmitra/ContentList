#!/bin/ksh -eu
#------------------------------------------------------------------------------------------------
# Filename:     single_hadoop_load_file.ksh
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
RECORD_ID=$2
HDP_CONN=$3

set -A SRC_HOSTS $SRC_NODE_LIST

. $DW_MASTER_LIB/dw_etl_common_functions.lib

HADOOP_LOAD_JOB="$DW_EXE/hadoop_load_file.ksh"

export N_WAY_COMP_FILE=$DW_SA_TMP/$TABLE_ID.$JOB_TYPE_ID.single_hadoop_load.$RECORD_ID.complete
if [ ! -f $N_WAY_COMP_FILE ]
then
  > $N_WAY_COMP_FILE
fi

#Launch the instances at different hosts
COMMAND_SCRIPT="$DW_SA_TMP/$TABLE_ID.$JOB_TYPE_ID.hadoop_load_file.$RECORD_ID.command.dat"
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
      print "(/usr/bin/ssh -q $HOST_NAME \"ksh $HADOOP_LOAD_JOB $ETL_ID $JOB_ENV $UOW_ID $HDP_CONN $RECORD_ID $instance_idx $CURR_DATETIME\" > $LOG_FILE 2>&1) || (print \"ERROR - Failure run hadoop_load_file process on $HOST_NAME\" >>$LOG_FILE) &" >>$COMMAND_SCRIPT
    else
      print "print \"Launching instance $instance_idx on $HOST_NAME ...\"" >> $COMMAND_SCRIPT
      print "ksh $HADOOP_LOAD_JOB $ETL_ID $JOB_ENV $UOW_ID $HDP_CONN $RECORD_ID $instance_idx $CURR_DATETIME > $LOG_FILE 2>&1 &" >> $COMMAND_SCRIPT
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
    grep -i 'ERROR' $LOG_FILE >/dev/null 2>&1
    errcnt=$?
    set -e
    if [[ $errcnt -ne 0 && $csrcode -eq 0 ]]
    then
      print "Instance $instance_idx is complete"
      print "hdp_load_file for $instance_idx" >> $N_WAY_COMP_FILE
    else
      print "${0##*/}:  ERROR, see log file $LOG_FILE" >&2
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
