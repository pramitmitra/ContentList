#!/bin/ksh -eu
#------------------------------------------------------------------------------------------------
# Title:        hadoop file load
# File Name:    hadoop_load_file.ksh
# Description:  This script is to run one instance of load at a specified host.
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
# Jiankang Liu     05/13/2015      Remove the grepCompFile extra regex 
# Michael Weng     06/20/2016      Remove invalid code and hadoop_logins.dat related change
#------------------------------------------------------------------------------------------------

export ETL_ID=$1
export JOB_ENV=$2
export UOW_ID=$3
export HDP_CONN=$4
export RECORD_ID=$5
export HOST_ID=$6
export CURR_DATETIME_TMP=$7

export HADOOP_COMMAND="hadoop"
export JOB_TYPE_ID="hfld"

. /dw/etl/mstr_cfg/etlenv.setup
. $DW_MASTER_CFG/dw_etl_common_defs.cfg
. $DW_MASTER_LIB/dw_etl_common_functions.lib

export TABLE_LIS_FILE=$DW_CFG/$ETL_ID.targets.lis
assignTagValue OUT_DIR OUT_DIR $ETL_CFG_FILE
export DW_SA_OUT=`eval print $OUT_DIR`/$JOB_ENV/$SUBJECT_AREA

IN_DIR=$DW_SA_OUT

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
grep "^$HDP_CONN\>" $DW_LOGINS/hadoop_logins.dat | read HDP_CONN_NAME HDP_CLUSTER HDP_USERNAME HDP_GROUPNAME HDP_PASSWORD HDFS_PATH
rcode=$?
set -e

if [ $rcode != 0 ]
then
  print "${0##*/}:  ERROR, failure determining value for $HDP_CONN parameter from $DW_LOGINS/hadoop_logins.dat" >&2
  exit 4
fi

if [[ -z $HDP_CLUSTER ]]
then
  print "INFRA_ERROR: can't determine hadoop cluster to connect to"
  exit 4
fi

# Hadoop env - require hadoop cluster name instead of HDFS_URL in $DW_LOGINS/hadoop_logins.dat
HDP_CLUSTER_ENV=$DW_MASTER_CFG/.${HDP_CLUSTER}_env.sh

if ! [ -f $HDP_CLUSTER_ENV ]
then
  print "INFRA_ERROR: missing hadoop cluster env file: $HDP_CLUSTER_ENV"
  exit 4
fi

. $HDP_CLUSTER_ENV

# Get the HDFS_URL
export HDFS_URL=$HADOOP_NN_URL

SOURCE_FILE=`print $(eval print $SOURCE_FILE)`
IN_DIR=`print $(eval print $IN_DIR)`

export MULTI_HDP_COMP_FILE=$DW_SA_TMP/$TABLE_ID.$JOB_TYPE_ID.multi_hdp_ld.$RECORD_ID.$HOST_ID.complete
if [ ! -f $MULTI_HDP_COMP_FILE ]
then
  > $MULTI_HDP_COMP_FILE
fi

export DATA_LIS_FILE=$DW_SA_TMP/$TABLE_ID.$JOB_TYPE_ID.hdp_ld_file_list.$RECORD_ID.$HOST_ID.dat

FILE_ID=0
> $DATA_LIS_FILE
if [ -f $IN_DIR/$SOURCE_FILE ]
then
  for data_file_entry in `find ${IN_DIR} -name ${SOURCE_FILE} | sort | grep $UOW_ID`
  do
    print "$data_file_entry" '$TARGET_FILE.$HOST_ID.$FILE_ID' >> $DATA_LIS_FILE
    FILE_ID=$(( FILE_ID + 1 ))
  done
fi

LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.single_hdfs_load.$RECORD_ID.$HOST_ID.$CURR_DATETIME.log

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
  
    COMMAND="${HADOOP_COMMAND} fs -Dhadoop.job.ugi=$HDP_USERNAME,$HDP_GROUPNAME -copyFromLocal $SOURCE_FILE_TMP $HDFS_URL$HDFS_PATH/`print $(eval print $TARGET_FILE_TMP)` >> $LOG_FILE"
    #COMMAND="print fs -Dhadoop.job.ugi=$HDP_USERNAME,$HDP_GROUPNAME -copyFromLocal $SOURCE_FILE_TMP $HDFS_URL$HDFS_PATH/`print $(eval print $TARGET_FILE_TMP)` >> $LOG_FILE"
    set +e
    eval $COMMAND && (print "Load completion of FILE: $SOURCE_FILE_NAME."; print "$SOURCE_FILE_NAME" >> $MULTI_HDP_COMP_FILE) || (print "ERROR - Failure processing FILE: $SOURCE_FILE_NAME, HDFS: $HDFS_URL") &
    set -e
  elif [ $RCODE = 0 ]
  then
    print "Loading of FILE: $SOURCE_FILE_NAME is already complete"
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
    print "${0##*/}: ERROR - Multiple hadoop file copy does not completely finish"
  fi
else
  print "${0##*/}: ERROR - Faileure on processing $MULTI_HDP_COMP_FILE"
fi
