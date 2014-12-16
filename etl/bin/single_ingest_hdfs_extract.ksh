#!/bin/ksh -eu
#------------------------------------------------------------------------------------------------
# Filename:     single_ingest_hdfs_extract.ksh
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
shift 3

while [ $# -gt 0 ]
do
  DWI_KWD="${1}"
  shift
  case $DWI_KWD in
    -UOW_FROM )
      export UOW_FROM="${1}"
      shift
      ;;
    -UOW_TO )
      export UOW_TO="${1}"
      shift
      ;;
    -PARAM1 )
      export PARAM1="${1}"
      shift
      ;;
    -PARAM2 )
      export PARAM2="${1}"
      shift
      ;;
    -PARAM3 )
      export PARAM3="${1}"
      shift
      ;;
    -PARAM4 )
      export PARAM4="${1}"
      shift
      ;;
    * )
      print "FATAL INFRA_ERROR:  Unexpected command line argument"
      print "Usage: single_ingest_hdfs_extract.ksh <ETL_ID> <FILE_ID> <DBC_FILE> [<TABLE_NAME> <DATA_FILENAME> -UOW_FROM <UOW_FROM> -UOW_TO <UOW_TO> -PARAM1 <PARAM1> -PARAM2 <PARAM2> -PARAM3 <PARAM3> -PARAM4 <PARAM4>]"
      exit 4
  esac
done

. $DW_MASTER_LIB/dw_etl_common_functions.lib

assignTagValue MULTI_HOST MULTI_HOST $ETL_CFG_FILE W 0
if [ $MULTI_HOST = 0 ]
then
  export HOSTS_LIST_FILE=$DW_CFG/$ETL_ID.host.lis
  if [ ! -f $HOSTS_LIST_FILE ]
  then
    print "${0##*/}:  FATAL INFRA_ERROR: MULTI_HOST is zero, and $HOST_LIST_FILE does not exist" >&2
    exit 4
  fi
elif [[ $MULTI_HOST = 1 ]]
then
  JOB_RUN_NODE=$servername
elif [[ $MULTI_HOST = @(2||4||6||8||16||32) ]]
then
  export HOSTS_LIST_FILE=$DW_MASTER_CFG/${servername%%.*}.${MULTI_HOST}ways.host.lis
else
  print "${0##*/}:  FATAL INFRA_ERROR: TPT_EXTRACT_NHOST not valid value $MULTI_HOST" >&2
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

HADOOP_EXTRACT_JOB="$DW_MASTER_EXE/dw_infra.ingest_hadoop_extract_file.ksh"

export N_WAY_COMP_FILE=$DW_SA_TMP/$TABLE_ID.$JOB_TYPE_ID.single_hadoop_extract.$RECORD_ID.complete
if [ ! -f $N_WAY_COMP_FILE ]
then
  > $N_WAY_COMP_FILE
fi

#Launch the instances at different hosts
COMMAND_SCRIPT="$DW_SA_TMP/$TABLE_ID.$JOB_TYPE_ID.hadoop_extract_file.$RECORD_ID.command.dat"
> $COMMAND_SCRIPT
instance_idx=0
while [[ $instance_idx -lt $SRC_HOST_CNT ]]
do
  RCODE=`grepCompFile "hdp_extract_file for $instance_idx" $N_WAY_COMP_FILE`
  if [ $RCODE = 1 ]
  then
    HOST_NAME=${SRC_HOSTS[${instance_idx}]}

    LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.hdp_extract_file.$instance_idx.$CURR_DATETIME.log
    if [[ ${HOST_NAME%%.*} != `hostname` ]]
    then
      print "print \"Launching instance $instance_idx on $HOST_NAME ...\"" >> $COMMAND_SCRIPT
      print "(/usr/bin/ssh -q $HOST_NAME \"ksh $HADOOP_EXTRACT_JOB $ETL_ID $JOB_ENV $BATCH_SEQ_NUM $HDP_CONN $RECORD_ID $instance_idx $SRC_HOST_CNT $CURR_DATETIME $UOW_TO $UOW_FROM\" > $LOG_FILE 2>&1) || (print \"INFRA_ERROR - Failure run hadoop_extract_file process on $HOST_NAME\" >>$LOG_FILE) &" >>$COMMAND_SCRIPT
    else
      print "print \"Launching instance $instance_idx on $HOST_NAME ...\"" >> $COMMAND_SCRIPT
      print "ksh $HADOOP_EXTRACT_JOB $ETL_ID $JOB_ENV $BATCH_SEQ_NUM $HDP_CONN $RECORD_ID $instance_idx $SRC_HOST_CNT $CURR_DATETIME $UOW_TO $UOW_FROM > $LOG_FILE 2>&1 &" >> $COMMAND_SCRIPT
    fi
  fi
  instance_idx=$(( $instance_idx + 1 ))
done

print "wait" >>$COMMAND_SCRIPT
set +e
. $COMMAND_SCRIPT
csrcode=$?
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
  RCODE=`grepCompFile "hdp_extract_file for $instance_idx" $N_WAY_COMP_FILE`
  if [ $RCODE = 1 ]
  then
    LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.hdp_extract_file.$instance_idx.$CURR_DATETIME.log

    set +e
    grep 'INFRA_ERROR' $LOG_FILE >/dev/null 2>&1
    errcnt=$?
    set -e
    if [[ $errcnt -ne 0 && $csrcode -eq 0 ]]
    then
      print "Instance $instance_idx is complete"
      print "hdp_extract_file for $instance_idx" >> $N_WAY_COMP_FILE
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
