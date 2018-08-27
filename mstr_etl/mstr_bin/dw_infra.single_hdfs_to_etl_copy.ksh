#!/bin/ksh -eu
# Title:        Single HDFS To ETL Copy
# File Name:    dw_infra.single_hdfs_to_etl_copy.ksh
# Description:  Submit a single hdfs to etl copy job
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
# 2018-01-18   1.1    Michael Weng                  Export HD_CLUSTER for handling hadoop login
# 2018-07-12   1.2    Michael Weng                  Enable multi-host local retention cleanup
#############################################################################################################

export ETL_ID=$1
export JOB_ENV=$2
HDFS_CLUSTER=$3
ETL_DIR=$4
DATA_LIS_FILE=$5
ETL_PURGE_PARENT_DIR=$6
ETL_PURGE_DEL_DATE=$7

if [[ $# -ne 7 ]]
then
  print "FATAL ERROR: Usage: $0: <ETL_ID> <JOB_ENV> <HDFS_CLUSTER> <ETL_DIR> <DATA_LIS_FILE> <ETL_PURGE_PARENT_DIR> <ETL_PURGE_DEL_DATE>"
  exit 4
fi

. /dw/etl/mstr_cfg/etlenv.setup

if ! [[ -f $DW_MASTER_CFG/.${HDFS_CLUSTER}_env.sh ]]
then
  print "${0##*/}:  FATAL ERROR:  Environment file not found:   $DW_MASTER_CFG/.${HDFS_CLUSTER}_env.sh" >&2
  exit 4
fi

export HD_CLUSTER=$HDFS_CLUSTER
. $DW_MASTER_CFG/.${HDFS_CLUSTER}_env.sh
. $DW_MASTER_CFG/hadoop.login

### Purge older UOW data folders on ETL
if [[ $ETL_PURGE_PARENT_DIR != "NA" ]] && [[ -d $ETL_PURGE_PARENT_DIR ]]
then
  print "Purge UOW data folder on ETL older than $ETL_PURGE_DEL_DATE for ($servername:$ETL_PURGE_PARENT_DIR)"
  for FOLDER in $ETL_PURGE_PARENT_DIR/{8}-([0-9])
  do
    FOLDER_NUMBER=$(basename $FOLDER)
    if [[ -d $FOLDER ]] && [[ $FOLDER_NUMBER -lt $ETL_PURGE_DEL_DATE ]]
    then
      print "Cleaning up STT UOW LOCAL WORKING TABLE: $FOLDER"
      rm -rf $FOLDER
    fi
  done
fi

mkdir -p $ETL_DIR

TOTAL_FILE_SIZE=0
while read SOURCE_FILE DST_FILE
do
  # Cleanup previous file on ETL, if it exists
  if [[ -f $DST_FILE ]]
  then
    rm -f $DST_FILE
  fi
  print "Running: ${HADOOP_HOME2}/bin/hadoop fs -copyToLocal $SOURCE_FILE $DST_FILE"
  ${HADOOP_HOME2}/bin/hadoop fs -copyToLocal $SOURCE_FILE $DST_FILE
  FILE_SIZE=$(ls -l $DST_FILE | cut -d " " -f 5)
  TOTAL_FILE_SIZE=$TOTAL_FILE_SIZE+$FILE_SIZE
done < $DATA_LIS_FILE

print "Extract from HDFS completed `date`"

print "Printing Total File Size copied filed"
print TOTAL_FILE_SIZE $((TOTAL_FILE_SIZE/100))

exit 0
