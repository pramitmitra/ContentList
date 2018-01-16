#!/bin/ksh -eu
# Title:        Parallel HDFS To ETL Copy
# File Name:    dw_infra.parallel_hdfs_to_etl_copy.ksh
# Description:  Wrapper to submit copy jobs in parallel
# Developer:    Ryan Wong
# Created on:
# Location:     $DW_MASTER_BIN
# Logic:
#
#
# Called by:    UC4/Unix
#
# Date         Ver#   Modified By(Name)            Change and Reason for Change
#---------    -----  ---------------------------  -------------
# 2017-10-24   1.0    Ryan Wong                     Initial
# 2017-11-07   1.1    Ryan Wong                     If hdfs file has common compression suffix propagate
#                                                     this to etl file name;  gz, bz2, sz
# 2017-11-08   1.2    Ryan Wong                     Enhance hdfs copy to exclude directories and hidden objects
# 2017-12-19   1.3    Ryan Wong                     Add error check, if local launch host is not in the host list
#############################################################################################################

. /dw/etl/mstr_cfg/etlenv.setup
. $DW_MASTER_LIB/dw_etl_common_functions.lib
. $DW_MASTER_CFG/.${HDFS_CLUSTER}_env.sh
. $DW_MASTER_CFG/hadoop.login


assignTagValue STT_WORKING_PARALLEL_COPY_YN STT_WORKING_PARALLEL_COPY_YN $ETL_CFG_FILE W 0
if [[ $STT_WORKING_PARALLEL_COPY_YN == 0 ]]
then
  N_WAY_PER_HOST=1
  MULTI_HOST=1
else
  assignTagValue N_WAY_PER_HOST N_WAY_PER_HOST $ETL_CFG_FILE W 1
  assignTagValue MULTI_HOST MULTI_HOST $ETL_CFG_FILE W 1
fi

SRC_NODE_LIST=""
if [[ $MULTI_HOST = 0 ]]
then
  HOSTS_LIST_FILE=$DW_CFG/$ETL_ID.host.lis
  if [ ! -f $HOSTS_LIST_FILE ]
  then
    print "${0##*/}:  FATAL ERROR: MULTI_HOST is zero, and $HOST_LIST_FILE does not exist" >&2
    exit 4
  fi
elif [[ $MULTI_HOST = 1 ]]
then
  SRC_NODE_LIST=$servername

  print "MULTI_HOST is 1, copy to local ETL server only, $SRC_NODE_LIST"
elif [[ $MULTI_HOST = @(2||4||6||8||16||32) ]]
then
  HOSTS_LIST_FILE=$DW_MASTER_CFG/${servername%%.*}.${MULTI_HOST}ways.host.lis
  if [ ! -f $HOSTS_LIST_FILE ]
  then
    print "${0##*/}:  FATAL ERROR: MULTI_HOST is not 0 or 1 , and $HOST_LIST_FILE does not exist" >&2
    exit 4
  fi
else
  print "${0##*/}:  FATAL ERROR: MULTI_HOST not valid value $MULTI_HOST" >&2
  exit 4
fi

if [[ $MULTI_HOST != 1 ]]
then
  MASTER_NODE_FOUND=0
  while read SRC_NODE junk
  do
    SRC_NODE_LIST="${SRC_NODE_LIST} ${SRC_NODE}"
    if [[ ${servername%%.*} = ${SRC_NODE%%.*} ]]
    then
      MASTER_NODE_FOUND=1
    fi
  done < $HOSTS_LIST_FILE

  if [[ $MASTER_NODE_FOUND == 0 ]]
  then
    print "${0##*/}:  FATAL ERROR: Launch server $servername, not found in host file $HOSTS_LIST_FILE" >&2
    exit 4
  fi
fi

set -A SRC_HOSTS $SRC_NODE_LIST
SRC_HOST_CNT=${#SRC_HOSTS[*]}
((INSTANCE_TOTAL=N_WAY_PER_HOST*SRC_HOST_CNT))

print "SRC_NODE_LIST is $SRC_NODE_LIST"
print "SRC_HOST_CNT is $SRC_HOST_CNT"
print "INSTANCE_TOTAL is $INSTANCE_TOTAL"


# Generate master list file
DATA_LIS_FILE_PREFIX=$DW_SA_TMP/$TABLE_ID.$JOB_TYPE_ID.hdfs_to_etl_copy_list.$STT_TABLE.dat
HADOOP_SOURCE_FILE_LIST_TMP=$DW_SA_TMP/$TABLE_ID.$JOB_TYPE_ID.hdfs_to_etl_copy_list.$STT_TABLE.lis.tmp
${HADOOP_HOME2}/bin/hadoop fs -ls $SOURCE_PATH/ | tail -n +2 | grep -v '^d' | awk '{ print $8 }' | sort -d 1>$HADOOP_SOURCE_FILE_LIST_TMP


HADOOP_SOURCE_FILE_LIST=""
while read data_file_entry 
do
  SRC_FILENAME=${data_file_entry##*/}
  if [[ ${SRC_FILENAME:0:1} != "." ]]
  then
    HADOOP_SOURCE_FILE_LIST="$HADOOP_SOURCE_FILE_LIST $data_file_entry"
  fi
done < $HADOOP_SOURCE_FILE_LIST_TMP


print "#############################################################################"
print "HADOOP_SOURCE_FILE_LIST is $HADOOP_SOURCE_FILE_LIST"
print "#############################################################################"

# Check for empty HADOOP_SOURCE_FILE_LIST
if [[ -z $HADOOP_SOURCE_FILE_LIST ]]
then
  print "${0##*/}:  FATAL ERROR: HADOOP_SOURCE_FILE_LIST is empty." >&2
  exit 4
fi

# Initialize and generate multi-list files
instance_idx=0
while [[ $instance_idx -lt $INSTANCE_TOTAL ]]
do
  >  $DATA_LIS_FILE_PREFIX.$instance_idx
  ((instance_idx++))
done

instance_idx=0
for data_file_entry in $HADOOP_SOURCE_FILE_LIST
do
  MOD_NUM=$(( $instance_idx % $INSTANCE_TOTAL ))
  FILE_SUFFIX=${data_file_entry##*.}
  if [[ $FILE_SUFFIX == "gz" ]]
  then
    print "$data_file_entry $ETL_DIR/$STT_TABLE.$instance_idx.dat.gz" >> $DATA_LIS_FILE_PREFIX.$MOD_NUM
  elif [[ $FILE_SUFFIX == "bz2" ]]
  then
    print "$data_file_entry $ETL_DIR/$STT_TABLE.$instance_idx.dat.bz2" >> $DATA_LIS_FILE_PREFIX.$MOD_NUM
  elif [[ $FILE_SUFFIX == "sz" ]]
  then
    print "$data_file_entry $ETL_DIR/$STT_TABLE.$instance_idx.dat.sz" >> $DATA_LIS_FILE_PREFIX.$MOD_NUM
  else
    print "$data_file_entry $ETL_DIR/$STT_TABLE.$instance_idx.dat" >> $DATA_LIS_FILE_PREFIX.$MOD_NUM
  fi
  ((instance_idx++))
done

# Transfering multi-list files
print "Transfering multi-list files"
instance_idx=0
while [[ $instance_idx -lt $INSTANCE_TOTAL ]]
do
  MOD_NUM=$(( $instance_idx % $SRC_HOST_CNT ))
  HOST_NAME=${SRC_HOSTS[${MOD_NUM}]}
  if [[ ${HOST_NAME%%.*} !=  ${servername%%.*} ]]
  then
    print "scp $DATA_LIS_FILE_PREFIX.$instance_idx $HOST_NAME:$DATA_LIS_FILE_PREFIX.$instance_idx"
    scp $DATA_LIS_FILE_PREFIX.$instance_idx $HOST_NAME:$DATA_LIS_FILE_PREFIX.$instance_idx
  fi
  ((instance_idx++))
done

print "Beginning job launch `date`"
LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.dw_infra.single_hdfs_to_etl_copy.$STT_TABLE.$CURR_DATETIME.log
set -A pid_list

instance_idx=0
while [[ $instance_idx -lt $INSTANCE_TOTAL ]]
do
  MOD_NUM=$(( $instance_idx % $SRC_HOST_CNT ))
  HOST_NAME=${SRC_HOSTS[${MOD_NUM}]}

  print "********************************************************************************"
  print "instance_idx is $instance_idx"
  print "HOST_NAME is $HOST_NAME"
  print "Log File: $LOG_FILE.$instance_idx"

  if [[ ${HOST_NAME%%.*} == ${servername%%.*} ]]
  then
    print "Local launch $DW_MASTER_BIN/dw_infra.single_hdfs_to_etl_copy.ksh $ETL_ID $JOB_ENV $HDFS_CLUSTER $ETL_DIR $DATA_LIS_FILE_PREFIX.$instance_idx"
    set +e
    $DW_MASTER_BIN/dw_infra.single_hdfs_to_etl_copy.ksh $ETL_ID $JOB_ENV $HDFS_CLUSTER $ETL_DIR $DATA_LIS_FILE_PREFIX.$instance_idx > $LOG_FILE.$instance_idx 2>&1 &
    pid_list[$instance_idx]=$!
    set -e
  else
    print "Remote launch ssh -nq $HOST_NAME $DW_MASTER_BIN/dw_infra.single_hdfs_to_etl_copy.ksh $ETL_ID $JOB_ENV $HDFS_CLUSTER $ETL_DIR $DATA_LIS_FILE_PREFIX.$instance_idx"
    set +e
    ssh -nq $HOST_NAME $DW_MASTER_BIN/dw_infra.single_hdfs_to_etl_copy.ksh $ETL_ID $JOB_ENV $HDFS_CLUSTER $ETL_DIR $DATA_LIS_FILE_PREFIX.$instance_idx > $LOG_FILE.$instance_idx 2>&1 &
    pid_list[$instance_idx]=$!
    set -e
  fi
  print "********************************************************************************"

  ((instance_idx++))
done


# Wait and capture all pids
set -A pid_list_rcode
instance_idx=0
while [[ $instance_idx -lt ${#pid_list[*]} ]]
do
  set +e
  wait ${pid_list[$instance_idx]}
  pid_list_rcode[$instance_idx]=$?
  set -e
  ((instance_idx++))
done

# Check return codes for all pids
instance_idx=0
while [[ $instance_idx -lt ${#pid_list_rcode[*]} ]]
do
  if [ ${pid_list_rcode[$instance_idx]} != 0 ]
  then
    print "${0##*/}:  FATAL ERROR, see log file $LOG_FILE.$instance_idx" >&2
    exit 4
  fi
  ((instance_idx++))
done


print "\nCreating record count file as $STT_TABLE.record_count.dat\n"

RECORD_COUNT=0
instance_idx=0
while [[ $instance_idx -lt $INSTANCE_TOTAL ]]
do
  grep "^TOTAL_FILE_SIZE " $LOG_FILE.$instance_idx | read junk FILE_SIZE
  ((RECORD_COUNT+=FILE_SIZE))
  ((instance_idx++))
done

print $RECORD_COUNT > $ETL_DIR/$STT_TABLE.record_count.dat

print "Finish of script ${0##*/} `date`"

exit 0
