#!/bin/ksh -eu
#------------------------------------------------------------------------------------------------
# Title:        hadoop file extract
# File Name:    hadoop_extract_file.ksh
# Description:  This script is to run one instance of extract at a specified host.
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

export ETL_ID=$1
export JOB_ENV=$2
export UOW_ID=$3
export HDP_CONN=$4
export RECORD_ID=$5
export HOST_ID=$6
export SRC_HOST_CNT=$7
export CURR_DATETIME_TMP=$8

export HADOOP_HOME=/export/home/sg_adm/hadoop-0.20
export JAVA_HOME=/export/home/sg_adm/jdk1.6.0_16

export PATH=$JAVA_HOME/bin:$PATH:$HADOOP_HOME/bin
export HADOOP_COMMAND="$HADOOP_HOME/bin/hadoop"
export JOB_TYPE_ID="hfex"

. /dw/etl/mstr_cfg/etlenv.setup
. $DW_MASTER_CFG/dw_etl_common_defs.cfg
. $DW_MASTER_LIB/dw_etl_common_functions.lib

export TABLE_LIS_FILE=$DW_CFG/$ETL_ID.sources.lis
assignTagValue IN_DIR IN_DIR $ETL_CFG_FILE
export DW_SA_IN=`eval print $IN_DIR`/$JOB_ENV/$SUBJECT_AREA

IN_DIR=$DW_SA_IN

set +e
grep "^$RECORD_ID[ 	]*$HDP_CONN\>" $TABLE_LIS_FILE | read RECORD_ID_T HDP_CONN_T PARALLEL_NUM SOURCE_FILE TARGET_FILE MISC
rcode=$?
set -e
if [ $rcode != 0 ]
then
  print "${0##*/}:  ERROR, failure determining value for $RECORD_ID parameter from $TABLE_LIS_FILE" >&2
  exit 4
fi
if [ ! -z $CURR_DATETIME_TMP ]
then
  CURR_DATETIME=$CURR_DATETIME_TMP
fi

assignTagValue N_WAY_PER_HOST N_WAY_PER_HOST $ETL_CFG_FILE W 1

set +e
grep "^$HDP_CONN\>" $DW_LOGINS/hadoop_logins.dat | read HDP_CONN_NAME HDFS_URL HDP_USERNAME HDP_GROUPNAME HDP_PASSWORD HDFS_PATH
rcode=$?
set -e

if [ $rcode != 0 ]
then
  print "${0##*/}:  ERROR, failure determining value for $HDP_CONN parameter from $DW_LOGINS/hadoop_logins.dat" >&2
  exit 4
fi

SOURCE_FILE=`print $(eval print $SOURCE_FILE)`
IN_DIR=`print $(eval print $IN_DIR)`

export MULTI_HDP_COMP_FILE=$DW_SA_TMP/$TABLE_ID.$JOB_TYPE_ID.multi_hdp_ex.$RECORD_ID.$HOST_ID.complete
if [ ! -f $MULTI_HDP_COMP_FILE ]
then
  > $MULTI_HDP_COMP_FILE
fi

export DATA_LIS_FILE=$DW_SA_TMP/$TABLE_ID.$JOB_TYPE_ID.hdp_ex_file_list.$RECORD_ID.$HOST_ID.dat

FILE_ID=0
> $DATA_LIS_FILE
for data_file_entry in `${HADOOP_COMMAND} fs -ls $HDFS_URL$HDFS_PATH/$SOURCE_FILE | awk '{ printf $8" " }' | sort`
do
  MOD_NUM=$(( $FILE_ID % $SRC_HOST_CNT ))
  if [ $MOD_NUM -eq $HOST_ID ]
  then
    print "$data_file_entry" '$TARGET_FILE.$HOST_ID.$FILE_ID.$UOW_ID' >> $DATA_LIS_FILE
  fi
  FILE_ID=$(( FILE_ID + 1 ))
done

LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.single_hdfs_extract.$RECORD_ID.$HOST_ID.$CURR_DATETIME.log

FILE_ID=0
while read SOURCE_FILE_TMP TARGET_FILE_TMP
do
  SOURCE_FILE_NAME=${SOURCE_FILE_TMP##*/}
  RCODE=`grepCompFile "$SOURCE_FILE_NAME" $MULTI_HDP_COMP_FILE`
  if [ $RCODE = 1 ]
  then
    while [ $(jobs -p | wc -l) -ge $N_WAY_PER_HOST ]
    do
      sleep 30
      continue
    done
  
    COMMAND="${HADOOP_COMMAND} fs -Dhadoop.job.ugi=$HDP_USERNAME,$HDP_GROUPNAME -copyToLocal $HDFS_URL$SOURCE_FILE_TMP $IN_DIR/`print $(eval print $TARGET_FILE_TMP)` >> $LOG_FILE"
    #COMMAND="print fs -Dhadoop.job.ugi=$HDP_USERNAME,$HDP_GROUPNAME -copyToLocal $HDFS_URL$SOURCE_FILE_TMP $IN_DIR/`print $(eval print $TARGET_FILE_TMP)` >> $LOG_FILE"
    set +e
    eval $COMMAND && (print "Extract completion of FILE: $SOURCE_FILE_NAME."; print "$SOURCE_FILE_NAME" >> $MULTI_HDP_COMP_FILE) || (print "ERROR - Failure processing FILE: $SOURCE_FILE_NAME, HDFS: $HDFS_URL") &
    set -e
  elif [ $RCODE = 0 ]
  then
    print "Extracting of FILE: $SOURCE_FILE_NAME is already complete"
  fi
  FILE_ID=$(( FILE_ID + 1 ))
done < $DATA_LIS_FILE

wait

set +e
wc -l $MULTI_HDP_COMP_FILE | read DATA_FILE_COMP_COUNT FN
rcode=$?
set -e

if [ rcode -eq 0 ]
then
  if [ $DATA_FILE_COMP_COUNT -eq $FILE_ID ]
  then
    rm -rf $MULTI_HDP_COMP_FILE
  else
    print "${0##*/}: ERROR - Multiple hadoop file extract does not completely finish"
  fi
else
  print "${0##*/}: ERROR - Faileure on processing $MULTI_HDP_COMP_FILE"
fi
