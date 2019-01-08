#!/bin/ksh -eu
#------------------------------------------------------------------------------------------------
# Filename:     dw_infra.single_etl_to_hdfs_copy.ksh
# Description:  Wrapper to submit copy jobs on a single host
# Location:     $DW_MASTER_BIN
#
# Revision History:
#
# Name             Date            Description
# ---------------  --------------  ---------------------------------------------------
# Michael Weng     10/26/2017      Initial
# Michael Weng     01/18/2018      Export HD_CLUSTER for handling hadoop login
# Michael Weng     03/05/2018      Fix code error when DW_IN resides on shared storage
# Michael Weng     04/24/2018      Skip TABLE_ID check on source files for UOW based
# Michael Weng     05/02/2018      Skip TABLE_ID check for non-UOW when STE_CURRDATE_TO_UOW specified
# Michael Weng     06/06/2018      Add TABLE_ID check for non-UOW when STE_CURRDATE_TO_UOW specified
# Michael Weng     06/12/2018      Enable Abinitio HDFS load
# Michael Weng     07/23/2018      Fix STE HDFS copy file pattern for non-UOW case
# Michael Weng     11/07/2018      Add retry on loading data from ETL to HDFS
#------------------------------------------------------------------------------------------------

export ETL_ID=$1
export JOB_ENV=$2
CLUSTER=$3
HD_TABLE=$4
CURR_DATETIME=$5
SRC_HOST_CNT=$6
HOST_ID=$7
ETL_DIR=$8
ETL_TABLE=$9
HD_PATH=${10}
UOW_TO_FLAG=${11}
UOWTO_BATCHSEQNUM=${12}

if [ $# != 12 ]
then
  print "${0##*/}: INFRA_ERROR - Invalid command line options"
  print "Usage: $0 <ETL_ID> <JOB_ENV> <CLUSTER> <HD_TABLE> <CURR_DATETIME> <SRC_HOST_CNT> <HOST_ID> <ETL_DIR> <ETL_TABLE> <HD_PATH> <UOW_TO_FLAG> <UOWTO_BATCHSEQNUM>"
  exit 4
fi

if [ $UOW_TO_FLAG = 1 ]
then
  UOW_TO=${UOWTO_BATCHSEQNUM}
elif [ $UOW_TO_FLAG = 0 ]
then
  BATCH_SEQ_NUM=${UOWTO_BATCHSEQNUM}
else
  print "${0##*/}: INFRA_ERROR - Invalid command line option for UOW_TO_FLAG: $UOW_TO_FLAG"
  exit 4
fi

. /dw/etl/mstr_cfg/etlenv.setup
. $DW_MASTER_CFG/dw_etl_common_defs.cfg
. $DW_MASTER_LIB/dw_etl_common_functions.lib
export HD_CLUSTER=$CLUSTER

if ! [[ -f $DW_MASTER_CFG/.${HD_CLUSTER}_env.sh ]]
then
  print "${0##*/}: INFRA_ERROR - Environment file not found: $DW_MASTER_CFG/.${HD_CLUSTER}_env.sh" >&2
  exit 4
fi

assignTagValue N_WAY_PER_HOST N_WAY_PER_HOST $ETL_CFG_FILE W 1   
assignTagValue USE_DISTR_TABLE USE_DISTR_TABLE $ETL_CFG_FILE W 0
assignTagValue CNDTL_COMPRESSION CNDTL_COMPRESSION $ETL_CFG_FILE W 0
assignTagValue CNDTL_COMPRESSION_SFX CNDTL_COMPRESSION_SFX $ETL_CFG_FILE W ""
assignTagValue HDFS_LOAD_PROCESS_TYPE HDFS_LOAD_PROCESS_TYPE $ETL_CFG_FILE W F

if [ $CNDTL_COMPRESSION != 1 ]
then
  CNDTL_COMPRESSION_SFX=""
fi

### Data files to be loaded onto HDFS
export DATA_LIS_FILE=$DW_SA_TMP/$HD_TABLE.$JOB_TYPE_ID.single_etl_to_hdfs_copy.$HOST_ID.$CURR_DATETIME.dat
> $DATA_LIS_FILE

### Check if $ETL_DIR is on shared storage
IS_SHARED=0
SRC_STORAGE=$(df -P -T $ETL_DIR | tail -n +2 | awk '{print $2}')
if [ $SRC_STORAGE = "nfs" ]
then
 IS_SHARED=1
fi

if [ $UOW_TO_FLAG = 1 ]
then
  DATA_FILE_PATTERN=$ETL_DIR/*.dat*
  print "N_WAY_PER_HOST is $N_WAY_PER_HOST"
  print "DATA_FILE_PATTERN is $DATA_FILE_PATTERN"

  if ls $DATA_FILE_PATTERN 1> /dev/null 2>&1
  then
    FILE_ID=0
    for data_file_entry in `ls $DATA_FILE_PATTERN |grep -v ".record_count."`
    do
      if [[ $IS_SHARED = 0 ]] || [[ $IS_SHARED = 1 && $(($FILE_ID % $SRC_HOST_CNT)) = $HOST_ID ]]
      then
        print "$data_file_entry" >> $DATA_LIS_FILE
      fi
      ((FILE_ID++))
    done
  else
    print "${0##*/}: WARNING - Failed to find source file(s) on $servername (Host Index: $HOST_ID)"
    exit 4
  fi

else
  FILE_ID=0
  if [ $USE_DISTR_TABLE = 1 ]
  then
    read TABLE_NAME DATA_FILENAME PARAM < $DW_CFG/$ETL_ID.sources.lis
    while read FILE_ID DBC_FILE
    do
      if [[ $IS_SHARED = 0 ]] || [[ $IS_SHARED = 1 && $(($FILE_ID % $SRC_HOST_CNT)) = $HOST_ID ]]
      then
        print $(eval print $ETL_DIR/$DATA_FILENAME.$BATCH_SEQ_NUM$CNDTL_COMPRESSION_SFX) >> $DATA_LIS_FILE
      fi
      ((FILE_ID++))
    done < $DW_CFG/$DISTR_TABLE_LIS_FILE.sources.lis
  else
    while read FILE_ID DBC_FILE PARALLEL_NUM TABLE_NAME DATA_FILENAME PARAM
    do
      if [[ $IS_SHARED = 0 ]] || [[ $IS_SHARED = 1 && $(($FILE_ID % $SRC_HOST_CNT)) = $HOST_ID ]]
      then
        print $(eval print $ETL_DIR/$DATA_FILENAME.$BATCH_SEQ_NUM$CNDTL_COMPRESSION_SFX) >> $DATA_LIS_FILE
      fi
      ((FILE_ID++))
    done < $DW_CFG/$ETL_ID.sources.lis
  fi
fi

. $DW_MASTER_CFG/.${HD_CLUSTER}_env.sh
. $DW_MASTER_CFG/hadoop.login

### Launching concurrent copys on the host
if [ $HDFS_LOAD_PROCESS_TYPE = F ]
then
  export JOBMAX=$N_WAY_PER_HOST
  FAILED_FILE=$DW_SA_LOG/$HD_TABLE.$JOB_TYPE_ID.single_etl_to_hdfs_copy.$HOST_ID.$CURR_DATETIME.failed.log
  > $FAILED_FILE

  while read SOURCE_FILE_TMP 
  do
    print "Loading data using file copy for $SOURCE_FILE_TMP on $servername (HOST Index: $HOST_ID) ... `date`"
    $DW_MASTER_BIN/dw_infra.hadoop_copy_from_local.ksh $ETL_ID $JOB_ENV $HD_CLUSTER $SOURCE_FILE_TMP $HD_PATH $FAILED_FILE &
    print "Process count is: $(jobs -p | wc -l)"
  done < $DATA_LIS_FILE

  wait

  ### Retry
  if [ -s $FAILED_FILE ]
  then
    while read SOURCE_FILE_TMP
    do
      print "${0##*/}: WARNING - Failed to load data using file copy for $SOURCE_FILE_TMP on $servername (HOST Index: $HOST_ID), retrying ..."
      set +e
      $DW_MASTER_BIN/dw_infra.hadoop_copy_from_local.ksh $ETL_ID $JOB_ENV $HD_CLUSTER $SOURCE_FILE_TMP $HD_PATH /dev/null
      rcode=$?
      set -e

      if [ $rcode != 0 ]
      then
        print "${0##*/}: INFRA_ERROR - Failed to load file $SOURCE_FILE_TMP on $servername (HOST Index: $HOST_ID)"
        exit 4
      fi
    done < $FAILED_FILE
  fi

elif [ $HDFS_LOAD_PROCESS_TYPE = H ]
then
  LOG_FILE=$DW_SA_LOG/$HD_TABLE.$JOB_TYPE_ID.single_etl_to_hdfs_ab_copy.$HOST_ID.$UOWTO_BATCHSEQNUM.$CURR_DATETIME.log
  print "Loading data using Abinitio load for $DATA_LIS_FILE on $servername (HOST Index: $HOST_ID) ... `date`"
  set +e
  $DW_EXE/single_etl_to_hdfs_ab_copy.ksh $ETL_ID $JOB_ENV $DATA_LIS_FILE $HD_PATH/$ETL_TABLE.dat > $LOG_FILE 2>&1
  rcode=$?
  set -e

  if [ $rcode  != 0 ]
  then
    print "${0##*/}:  ERROR, Abinitio load failed. See log file $LOG_FILE" >&2
    exit 4
  fi

else
  print "${0##*/}: INFRA_ERROR - Invalid HDFS_LOAD_PROCESS_TYPE ($HDFS_LOAD_PROCESS_TYPE)"
  exit 4
fi

exit 0
