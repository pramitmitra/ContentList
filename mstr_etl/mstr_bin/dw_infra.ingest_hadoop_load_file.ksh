#!/bin/ksh -eu
#------------------------------------------------------------------------------------------------
# Title:        hadoop file load
# File Name:    dw_infra.ingest_hadoop_load_file.ksh
# Description:  This script is to run one instance of load at a specified host.
# Developer:
# Created on:
# Location:     $DW_MASTER_EXE
# Logic:
#
#
# Revision History:
#
# Name             Date            Description
# ---------------  --------------  ---------------------------------------------------
# ???              ??/??/????      Initial Creation
# Ryan Wong        10/04/2013      Redhat changes
# Ryan Wong        11/21/2013      Update hd login method, consolidate to use dw_adm
# George Xiong     09/30/2014      Modifications by George
# Michael Weng     04/21/2016      Retrieve HDFS_URL based on JOB_ENV if not defined
# Michael Weng     09/09/2016      Enable use of batch account keytab
# Michael Weng     10/12/2016      Add hadoop authentication
# Michael Weng     10/10/2017      Add optional UOW to the HDFS path
#------------------------------------------------------------------------------------------------

export ETL_ID=$1
export JOB_ENV=$2
export BATCH_SEQ_NUM=$3
export HOST_ID=$4
export CURR_DATETIME=$5

shift 5

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
      print "Usage: single_ingest_hdfs_extract.ksh <ETL_ID> <FILE_ID> <DBC_FILE> [<TABLE_NAME> <DATA_FILENAME> -UOW_FROM <UOW_FROM> -UOW_TO <UOW_TO> -PARAM1 <PARAM1> -PARAM2 <PARAM2> -PARAM3 <PARAM3> -PARAM4 <PARAM4>]"
      exit 4
  esac
done

export JOB_TYPE="load"
export JOB_TYPE_ID="ld"

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

print "HADOOP_HOME is $HADOOP_HOME"
print "HDFS_URL is $HDFS_URL"

export PATH=$JAVA_HOME/bin:$PATH:$HADOOP_HOME/bin
export HADOOP_COMMAND="$HADOOP_HOME/bin/hadoop"
export JOB_TYPE="load"
export JOB_TYPE_ID="ld"

assignTagValue IN_DIR IN_DIR $ETL_CFG_FILE W $DW_IN
export IN_DIR=$IN_DIR/extract/$SUBJECT_AREA
UOW_APPEND=""

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
   export IN_DIR=$IN_DIR/$TABLE_ID/$UOW_TO_DATE/$UOW_TO_HH/$UOW_TO_MI/$UOW_TO_SS
   export UOW_DATE=$UOW_TO_DATE
fi

assignTagValue N_WAY_PER_HOST N_WAY_PER_HOST $ETL_CFG_FILE W 1   
assignTagValue HDFS_URL HDFS_URL $ETL_CFG_FILE W "$HDFS_URL"
assignTagValue HDFS_PATH HDFS_PATH $ETL_CFG_FILE W ""
assignTagValue EXTRACT_PROCESS_TYPE EXTRACT_PROCESS_TYPE $ETL_CFG_FILE W "D"
assignTagValue CNDTL_COMPRESSION CNDTL_COMPRESSION $ETL_CFG_FILE W "0"
assignTagValue CNDTL_COMPRESSION_SFX CNDTL_COMPRESSION_SFX $ETL_CFG_FILE W ".gz"

if [[ X"$UOW_TO" != X ]]
then
   assignTagValue HDFS_PATH_UOW HDFS_PATH_UOW $ETL_CFG_FILE W 0
   if [ $HDFS_PATH_UOW != 0 ]
   then
     HDFS_PATH=$HDFS_PATH/$UOW_TO_DATE/$UOW_TO_HH/$UOW_TO_MI/$UOW_TO_SS/
   fi
fi

export MULTI_HDP_COMP_FILE=$DW_SA_TMP/$TABLE_ID.$JOB_TYPE_ID.multi_hdp_ld.$HOST_ID$UOW_APPEND.complete
if [ ! -f $MULTI_HDP_COMP_FILE ]
then
  > $MULTI_HDP_COMP_FILE
fi

export DATA_LIS_FILE=$DW_SA_TMP/$TABLE_ID.$JOB_TYPE_ID.hdp_ld_file_list.$HOST_ID$UOW_APPEND.dat

> $DATA_LIS_FILE

DATA_FILE_PATTERN="$IN_DIR/$TABLE_ID.*.dat*"
if [[ "X$UOW_TO" != "X" ]]
then
  DATA_FILE_PATTERN="$DATA_FILE_PATTERN${FILE_EXTN}"
else
  DATA_FILE_PATTERN="$DATA_FILE_PATTERN.$BATCH_SEQ_NUM${FILE_EXTN}"
fi

print "DATA_FILE_PATTERN is $DATA_FILE_PATTERN"

# Fail the job if source file is not available
if ls $DATA_FILE_PATTERN 1> /dev/null 2>&1
then
  for data_file_entry in `ls $DATA_FILE_PATTERN |grep -v ".record_count."`
  do
    print "$data_file_entry" >> $DATA_LIS_FILE
  done
else
  print "${0##*/}: INFRA_ERROR - Failed to find source file(s) on $servername (Host Index: $HOST_ID)"
  exit 4
fi

LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.single_hdfs_load.$HOST_ID.$CURR_DATETIME.log

CLASSPATH=`$HADOOP_HOME/bin/hadoop classpath`
CLASSPATH=$CLASSPATH:$DW_MASTER_LIB/hadoop_ext/DataplatformETLHandlerUtil.jar

assignTagValue USE_DATACONVERTER_JAR USE_DATACONVERTER_JAR $ETL_CFG_FILE W 0 


if [ $USE_DATACONVERTER_JAR = 0 ]
  then
    FILE_ID=0
    while read SOURCE_FILE_TMP 
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
   
      COMMAND="${HADOOP_COMMAND} fs -mkdir -p $HDFS_URL/${HDFS_PATH}; ${HADOOP_COMMAND} fs -copyFromLocal $SOURCE_FILE_TMP $HDFS_URL/${HDFS_PATH}${SOURCE_FILE_NAME}.$HOST_ID.$FILE_ID$UOW_APPEND >> $LOG_FILE"
      set +e
      eval $COMMAND && (print "Load completion of FILE: $SOURCE_FILE_NAME."; print "$SOURCE_FILE_NAME" >> $MULTI_HDP_COMP_FILE) || (print "INFRA_ERROR - Failure processing FILE: $SOURCE_FILE_NAME, HDFS: $HDFS_URL") &
      set -e
      elif [ $RCODE = 0 ]
      then
        print "Loading of FILE: $SOURCE_FILE_NAME is already complete"
      fi
          FILE_ID=$(( FILE_ID + 1 ))
    done < $DATA_LIS_FILE
  else
    hadoop jar $DW_LIB/DataConverter.jar sojbinaryconverter.SOJBinaryConverterToHDFSThreaded \
                                      -inputDir $IN_DIR/ \
                                      -inputFileList $DATA_LIS_FILE \
                                      -outputDir $HDFS_PATH \
                                      -ignoreEmptyFile true \
                                      -numberOfThread $N_WAY_PER_HOST
  fi




if [ $? != 0 ]
then
  print "${0##*/}: INFRA_ERROR - Failed to load data to HDFS on $servername (Host Index: $HOST_ID)"
fi

wait
