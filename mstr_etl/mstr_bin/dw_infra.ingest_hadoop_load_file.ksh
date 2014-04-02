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
#
# Revision History:
#
# Name             Date            Description
# ---------------  --------------  ---------------------------------------------------
# ???              ??/??/????      Initial Creation
# Ryan Wong        10/04/2013      Redhat changes
# Ryan Wong        11/21/2013      Update hd login method, consolidate to use dw_adm
#
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

# Initialize HADOOP_HOME and Kerberos keytab
function switchApollo {
        print "set target to Apollo"
        HADOOP_HOME=$HD2_HADOOP_HOME
        HADOOP_CONF_DIR="$HADOOP_HOME/conf"
        export HADOOP_HOME
        export HADOOP_CONF_DIR
        PATH="$HADOOP_HOME/bin:$PATH"
}

function switchAres {
        print "set target to Ares"
        HADOOP_HOME=$HD1_HADOOP_HOME
        HADOOP_CONF_DIR="$HADOOP_HOME/conf"
        export HADOOP_HOME
        export HADOOP_CONF_DIR
        PATH="$HADOOP_HOME/bin:$PATH"
}

assignTagValue HDFS_URL HDFS_URL $ETL_CFG_FILE W ""

if [[ "X"$HDFS_URL != "X" ]]
then
  set +e
  print $HDFS_URL | grep -i $DW_HD2_DB
  isAres=$?
  set -e
  
  if [ $isAres != 0 ]
  then
    switchAres
  else
    switchApollo
  fi
else
  set +e
  print $HADOOP_HOME | grep $DW_HD2_DB
  isAres=$?
  set -e
  if [ $isAres != 0 ]
  then
    export HDFS_URL=$HD1_NN_URL
  else
    export HDFS_URL=$HD2_NN_URL
  fi
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

if [[ -z $HD_USERNAME ]]
  then
    print "INFRA_ERROR: can't not deterine batch account the connect hadoop cluster"
    exit 4
fi

set +e
myName=$(whoami)
if [[ $myName == @(sg_adm|dw_adm) ]]
then
  kinit -k -t /export/home/$myName/.keytabs/apd.sg_adm.keytab sg_adm@APD.EBAY.COM
fi
set -e

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

for data_file_entry in `ls $DATA_FILE_PATTERN |grep -v ".record_count."`
do
  print "$data_file_entry" >> $DATA_LIS_FILE
done

LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.single_hdfs_load.$HOST_ID.$CURR_DATETIME.log

CLASSPATH=`$HADOOP_HOME/bin/hadoop classpath`
CLASSPATH=$CLASSPATH:$DW_MASTER_LIB/hadoop_ext/DataplatformETLHandlerUtil.jar
HADOOP_CORE_JAR=`ls $HADOOP_HOME/hadoop*core*.jar`
HADOOP_FS_CMD="$HADOOP_CORE_JAR org.apache.hadoop.fs.FsShell"

assignTagValue USE_DATACONVERTER_JAR USE_DATACONVERTER_JAR $ETL_CFG_FILE W 0 

if [[ $myName == $HD_USERNAME ]]
then
  if [ $USE_DATACONVERTER_JAR = 0 ]
  then
    FILE_ID=0
    while read SOURCE_FILE_TMP 
    do
      SOURCE_FILE_NAME=${SOURCE_FILE_TMP##*/}
      RCODE=`grepCompFile "^$SOURCE_FILE_NAME\>" $MULTI_HDP_COMP_FILE`
      if [ $RCODE = 1 ]
      then
        while [ $(jobs -p | wc -l) -ge $N_WAY_PER_HOST ]
        do
         sleep 30
         continue
       done
   
      COMMAND="${HADOOP_COMMAND} fs -copyFromLocal $SOURCE_FILE_TMP $HDFS_URL/${HDFS_PATH}${SOURCE_FILE_NAME}.$HOST_ID.$FILE_ID$UOW_APPEND >> $LOG_FILE"
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

else

  if [ $USE_DATACONVERTER_JAR = 0 ]
  then
    FILE_ID=0
    while read SOURCE_FILE_TMP 
    do
      SOURCE_FILE_NAME=${SOURCE_FILE_TMP##*/}
      RCODE=`grepCompFile "^$SOURCE_FILE_NAME\>" $MULTI_HDP_COMP_FILE`
      if [ $RCODE = 1 ]
      then
        while [ $(jobs -p | wc -l) -ge $N_WAY_PER_HOST ]
        do
         sleep 30
         continue
       done

      ################################################################################
      # API: DataplatformRunJar <realUser> <keytabFilePath> <effectiveUser> <jarFile> [mainClass] [args...]
      ################################################################################
      COMMAND="exec $JAVA_HOME/bin/java -classpath "$CLASSPATH" DataplatformRunJar sg_adm ~dw_adm/.keytabs/apd.sg_adm.keytab $HD_USERNAME $HADOOP_FS_CMD -copyFromLocal $SOURCE_FILE_TMP $HDFS_URL/${HDFS_PATH}${SOURCE_FILE_NAME}.$HOST_ID.$FILE_ID$UOW_APPEND >> $LOG_FILE"
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
    exec $JAVA_HOME/bin/java -classpath "$CLASSPATH" DataplatformRunJar sg_adm ~dw_adm/.keytabs/apd.sg_adm.keytab $HD_USERNAME \
                                      $DW_LIB/DataConverter.jar sojbinaryconverter.SOJBinaryConverterToHDFSThreaded \
                                      -inputDir $IN_DIR/ \
                                      -inputFileList $DATA_LIS_FILE \
                                                                          -outputDir $HDFS_PATH \
                                      -ignoreEmptyFile true \
                                      -numberOfThread $N_WAY_PER_HOST
  fi
fi

if [ $? != 0 ]
then
  print "${0##*/}: INFRA_ERROR - Failed to load data to HDFS on $servername (Host Index: $HOST_ID)"
fi

wait
