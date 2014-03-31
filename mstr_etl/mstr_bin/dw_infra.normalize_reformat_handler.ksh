#!/bin/ksh -eu
#------------------------------------------------------------------------------------------------
# Filename:     dw_infra.normalize_reformat_handler.ksh
#
# Revision History:
#
# Name             Date            Description
# ---------------  --------------  ---------------------------------------------------
# ???              ??/??/????      Initial Creation
# Ryan Wong        10/04/2013      Redhat changes
#
#------------------------------------------------------------------------------------------------

if [[ $# -lt 2 ]]
then
   print "ERROR: Three arguments are required, SRC_ETL_ID and SRC_JOB_ENV"
   exit 4
fi

export TRGT_ETL_ID=$ETL_ID
export SRC_ETL_ID=$1
export SRC_JOB_ENV=$2

if [ $SRC_JOB_ENV = extract ]
then
   export SRC_JOB_TYPE=extract
   export SRC_JOB_TYPE_ID=ex
else
   export SRC_JOB_TYPE=load
   export SRC_JOB_TYPE_ID=ld
fi

export INPUT_DML_FILENAME=$SRC_ETL_ID.read.dml
export OUTPUT_DML_FILENAME=$TRGT_ETL_ID.read.dml

. $DW_MASTER_LIB/dw_etl_common_functions.lib

SRC_SA=${SRC_ETL_ID%%.*}
SRC_TBL=${SRC_ETL_ID##*.}

SRC_BATCH_SEQ_NUM_FILE=$DW_DAT/$SRC_JOB_ENV/$SRC_SA/$SRC_TBL.$SRC_JOB_TYPE.batch_seq_num.dat
export SRC_BATCH_SEQ_NUM=$(<$SRC_BATCH_SEQ_NUM_FILE)

# check indicator value to determine run normalize or reformat ( 1 - normalize; 2 - reformat )
assignTagValue IS_NORM_REFO IS_NORM_REFO $DW_CFG/$TRGT_ETL_ID.cfg W 1

if [ $IS_NORM_REFO = 1 ]
then
   eval $DW_EXE/single_field_normalize.ksh $TRGT_ETL_ID $SRC_BATCH_SEQ_NUM $SRC_ETL_ID $SRC_JOB_ENV $SRC_JOB_TYPE $SRC_JOB_TYPE_ID $INPUT_DML_FILENAME $OUTPUT_DML_FILENAME
elif [ $IS_NORM_REFO = 2 ]
then
   eval $DW_MASTER_BIN/dw_infra.record_reformat.ksh $TRGT_ETL_ID $SRC_BATCH_SEQ_NUM $SRC_ETL_ID $SRC_JOB_ENV $SRC_JOB_TYPE $SRC_JOB_TYPE_ID $INPUT_DML_FILENAME $OUTPUT_DML_FILENAME
else
   print "ERROR: Can't determine what process to run. Please check value in cfg for tag IS_NORM_REFO ( 1 - normalize; 2 - reformat )"
   exit 500
fi

