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
#############################################################################################################

export ETL_ID=$1
export JOB_ENV=$2
HDFS_CLUSTER=$3
ETL_DIR=$4
DATA_LIS_FILE=$5

if [[ $# -ne 5 ]]
then
  print "FATAL ERROR: Usage: $0: <ETL_ID> <JOB_ENV> <HDFS_CLUSTER> <ETL_DIR> <DATA_LIS_FILE>"
  exit 4
fi

. /dw/etl/mstr_cfg/etlenv.setup

if ! [[ -f $DW_MASTER_CFG/.${HDFS_CLUSTER}_env.sh ]]
then
  print "${0##*/}:  FATAL ERROR:  Environment file not found:   $DW_MASTER_CFG/.${HDFS_CLUSTER}_env.sh" >&2
  exit 4
fi

. $DW_MASTER_CFG/.${HDFS_CLUSTER}_env.sh
. $DW_MASTER_CFG/hadoop.login


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
