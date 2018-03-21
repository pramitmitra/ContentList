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
# Michael Weng     03/08/2018      Hdfs folder cleanup option causes multi-clean on hdfs
#
#------------------------------------------------------------------------------------------------

ETL_ID=$1
JOB_ENV=$2
shift 2
UOW_PARAM_LIST_AB=$*

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
      print "Usage: single_ingest_hdfs_load.ksh <ETL_ID> <JOB_ENV> [-UOW_FROM <UOW_FROM> -UOW_TO <UOW_TO> -PARAM1 <PARAM1> -PARAM2 <PARAM2> -PARAM3 <PARAM3> -PARAM4 <PARAM4>]"
      exit 4
  esac
done

. /dw/etl/mstr_cfg/etlenv.setup
. $DW_MASTER_CFG/dw_etl_common_defs.cfg
. $DW_MASTER_LIB/dw_etl_common_functions.lib

# Login into hadoop
. $DW_MASTER_CFG/hadoop.login

assignTagValue HDFS_URL HDFS_URL $ETL_CFG_FILE W ""

if [[ "X"$HDFS_URL == "X" ]]
then
  export HDFS_URL=$HADOOP_NN_URL
fi

export PATH=$JAVA_HOME/bin:$PATH:$HADOOP_HOME/bin
export HADOOP_COMMAND="$HADOOP_HOME/bin/hadoop"

if [[ X"$UOW_TO" != X ]]
then
   UOW_APPEND=.$UOW_TO
   UOW_PARAM_LIST="-f $UOW_FROM -t $UOW_TO"
   UOW_PARAM_LIST_AB="-UOW_FROM $UOW_FROM -UOW_TO $UOW_TO"
   is_valid_ts $UOW_FROM
   is_valid_ts $UOW_TO
   . $DW_MASTER_CFG/dw_etl_common_defs_uow.cfg
   assignTagValue UOW_FROM_DATE_RFMT_CODE UOW_FROM_DATE_RFMT_CODE $ETL_CFG_FILE W 0
   assignTagValue UOW_TO_DATE_RFMT_CODE UOW_TO_DATE_RFMT_CODE $ETL_CFG_FILE W 0
   export UOW_FROM_DATE_RFMT=$($DW_MASTER_EXE/dw_infra.reformat_date.ksh $UOW_FROM_DATE $UOW_FROM_DATE_RFMT_CODE)
   export UOW_TO_DATE_RFMT=$($DW_MASTER_EXE/dw_infra.reformat_date.ksh $UOW_TO_DATE $UOW_TO_DATE_RFMT_CODE)
   export UOW_DATE=$UOW_TO_DATE
fi

assignTagValue MULTI_HOST MULTI_HOST $ETL_CFG_FILE W 1
assignTagValue HDFS_URL HDFS_URL $ETL_CFG_FILE W "$HDFS_URL"
assignTagValue HDFS_PATH HDFS_PATH $ETL_CFG_FILE W ""
assignTagValue HDFS_PATH_CLEANUP HDFS_PATH_CLEANUP $ETL_CFG_FILE W 0

if [[ X"$UOW_TO" != X ]]
then
   assignTagValue HDFS_PATH_UOW HDFS_PATH_UOW $ETL_CFG_FILE W 0
   if [ $HDFS_PATH_UOW != 0 ]
   then
     HDFS_PATH=$HDFS_PATH/$UOW_TO_DATE/$UOW_TO_HH/$UOW_TO_MI/$UOW_TO_SS/
   fi
fi

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

# Do not cleanup hdfs folder if this is a resuming job
RCODE=`grepCompFile "hdp_load_file for" $N_WAY_COMP_FILE`
if [[ $RCODE = 1 && $HDFS_PATH_CLEANUP != 0 ]]
then
  print "Cleaning up target folder: $HDFS_URL/${HDFS_PATH} ..."
  set +e
  ${HADOOP_COMMAND} fs -rm -r -skipTrash $HDFS_URL/${HDFS_PATH}
  set -e
fi

# Create target hdfs folder if not exists
set +e
${HADOOP_COMMAND} fs -mkdir -p $HDFS_URL/${HDFS_PATH}
set -e

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
    if [[ ${HOST_NAME%%.*} != $servername ]]
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
