#!/bin/ksh -eu
#------------------------------------------------------------------------------------------------
# Filename:     dw_infra.hadoop_copy_from_local.ksh
# Description:  Copy one file to hdfs, collect failed source file name into a file
# Location:     $DW_MASTER_BIN
#
# Revision History:
#
# Name             Date            Description
# ---------------  --------------  ---------------------------------------------------
# Michael Weng     10/26/2017      Initial
# Michael Weng     01/18/2018      Export HD_CLUSTER for handling hadoop login
#
#------------------------------------------------------------------------------------------------

export ETL_ID=$1
export JOB_ENV=$2
CLUSTER=$3
SOURCE_FILE=$4
HDFS_PATH=$5
FAILED_FILE=$6

if [[ $# -ne 6 ]]
then
  print "${0##*/}: INFRA_ERROR - Invalid command line options"
  print "Usage: $0 <ETL_ID> <JOB_ENV> <CLUSTER> <SOURCE_FILE> <HDFS_PATH> <FAILED_FILE>"
  print "$SOURCE_FILE" >> $FAILED_FILE
  exit 4
fi

. /dw/etl/mstr_cfg/etlenv.setup
export HD_CLUSTER=$CLUSTER

if ! [[ -f $DW_MASTER_CFG/.${HD_CLUSTER}_env.sh ]]
then
  print "${0##*/}: INFRA_ERROR - Environment file not found: $DW_MASTER_CFG/.${HD_CLUSTER}_env.sh"
  print "$SOURCE_FILE" >> $FAILED_FILE
  exit 4
fi

. $DW_MASTER_CFG/.${HD_CLUSTER}_env.sh
. $DW_MASTER_CFG/hadoop.login

set +e
hadoop fs -copyFromLocal $SOURCE_FILE $HDFS_PATH/${SOURCE_FILE##*/}
retcode=$?
set -e

if [ $retcode = 0 ]
then
  print "Successfully loaded file: $SOURCE_FILE"
else
  print "${0##*/}: INFRA_ERROR - Failed to load file: $SOURCE_FILE"
  print "$SOURCE_FILE" >> $FAILED_FILE
fi
