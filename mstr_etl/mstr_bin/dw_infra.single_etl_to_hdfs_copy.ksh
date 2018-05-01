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
# Michael Weng     03/05/2018      Fix code error when DW_IN resides on shared storage
# Michael Weng     04/24/2018      Skip TABLE_ID check on source files for UOW based
#------------------------------------------------------------------------------------------------

export ETL_ID=$1
export JOB_ENV=$2
HD_CLUSTER=$3
HD_TABLE=$4
CURR_DATETIME=$5
SRC_HOST_CNT=$6
HOST_ID=$7
ETL_DIR=$8
HD_PATH=$9
UOW_TO_FLAG=${10}
UOWTO_BATCHSEQNUM=${11}

if [ $# != 11 ]
then
  print "${0##*/}: INFRA_ERROR - Invalid command line options"
  print "Usage: $0 <ETL_ID> <JOB_ENV> <HD_CLUSTER> <HD_TABLE> <CURR_DATETIME> <SRC_HOST_CNT> <HOST_ID> <ETL_DIR> <HD_PATH> <UOW_TO_FLAG> <UOWTO_BATCHSEQNUM>"
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

if ! [[ -f $DW_MASTER_CFG/.${HD_CLUSTER}_env.sh ]]
then
  print "${0##*/}: INFRA_ERROR - Environment file not found: $DW_MASTER_CFG/.${HD_CLUSTER}_env.sh" >&2
  exit 4
fi

. $DW_MASTER_CFG/.${HD_CLUSTER}_env.sh
. $DW_MASTER_CFG/hadoop.login

assignTagValue N_WAY_PER_HOST N_WAY_PER_HOST $ETL_CFG_FILE W 1   

# When source lis file provided, data file pattern is not limited to HD_TABLE. Load all data files for UOW based.
DATA_FILE_PATTERN=$ETL_DIR/*.dat*
if [ "X$UOW_TO" = "X" ]
then
  DATA_FILE_PATTERN=$ETL_DIR/$HD_TABLE.*.dat*.${BATCH_SEQ_NUM}
fi

export DATA_LIS_FILE=$DW_SA_TMP/$HD_TABLE.$JOB_TYPE_ID.single_etl_to_hdfs_copy.$HOST_ID.$CURR_DATETIME.dat
> $DATA_LIS_FILE

print "N_WAY_PER_HOST is $N_WAY_PER_HOST"
print "DATA_FILE_PATTERN is $DATA_FILE_PATTERN"

### Check if $ETL_DIR is on shared storage
IS_SHARED=0
SRC_STORAGE=$(df -P -T $ETL_DIR | tail -n +2 | awk '{print $2}')
if [ $SRC_STORAGE = "nfs" ]
then
 IS_SHARED=1
fi

### Fail the job if source file is not available
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

### Launching concurrent copys on the host
export JOBMAX=$N_WAY_PER_HOST
FAILED_FILE=$DW_SA_LOG/$HD_TABLE.$JOB_TYPE_ID.single_etl_to_hdfs_copy.$HOST_ID.$CURR_DATETIME.failed.log
> $FAILED_FILE

while read SOURCE_FILE_TMP 
do
  print "Copying $SOURCE_FILE_TMP on $servername (HOST Index: $HOST_ID) ..."
  $DW_MASTER_BIN/dw_infra.hadoop_copy_from_local.ksh $ETL_ID $JOB_ENV $HD_CLUSTER $SOURCE_FILE_TMP $HD_PATH $FAILED_FILE &
  print "Process count is: $(jobs -p | wc -l)"
done < $DATA_LIS_FILE

wait

if [ -s $FAILED_FILE ]
then
  print "${0##*/}: WARNING - Failed to load files on $servername (HOST Index: $HOST_ID)"
  exit 4
fi

exit 0
