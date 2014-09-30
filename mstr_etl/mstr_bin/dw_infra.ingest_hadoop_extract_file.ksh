#!/bin/ksh -eu
#------------------------------------------------------------------------------------------------
# Title:        hadoop file extract
# File Name:    dw_infra.ingest_hadoop_extract_file.ksh
# Description:  This script is to run one instance of extract at a specified host.
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
#
#------------------------------------------------------------------------------------------------

export ETL_ID=$1
export JOB_ENV=$2
export BATCH_SEQ_NUM=$3
export HDP_CONN=$4
export RECORD_ID=$5
export HOST_ID=$6
export SRC_HOST_CNT=$7
export CURR_DATETIME_TMP=$8
export UOW_TO=${9:-""}


. /dw/etl/mstr_cfg/etlenv.setup
. $DW_MASTER_CFG/dw_etl_common_defs.cfg
. $DW_MASTER_LIB/dw_etl_common_functions.lib


set +e 
myName=$(whoami)
if [[ $myName == @(sg_adm|dw_adm) ]]
then
  myName=sg_adm
  kinit -k -t ~/.keytabs/apd.$myName.keytab $myName@APD.EBAY.COM
fi
set -e


export JOB_TYPE_ID="ex"

export TABLE_LIS_FILE=$DW_CFG/$ETL_ID.sources.lis
assignTagValue IN_DIR IN_DIR $ETL_CFG_FILE

if [[ X"$UOW_TO" != X ]]
then
   is_valid_ts $UOW_FROM
   is_valid_ts $UOW_TO
   . $DW_MASTER_CFG/dw_etl_common_defs_uow.cfg
   assignTagValue UOW_FROM_DATE_RFMT_CODE UOW_FROM_DATE_RFMT_CODE $ETL_CFG_FILE W 0
   assignTagValue UOW_TO_DATE_RFMT_CODE UOW_TO_DATE_RFMT_CODE $ETL_CFG_FILE W 0
   export UOW_FROM_DATE_RFMT=$($DW_MASTER_EXE/dw_infra.reformat_date.ksh $UOW_FROM_DATE $UOW_FROM_DATE_RFMT_CODE)
   export UOW_TO_DATE_RFMT=$($DW_MASTER_EXE/dw_infra.reformat_date.ksh $UOW_TO_DATE $UOW_TO_DATE_RFMT_CODE)
   export UOW_DATE=$UOW_TO_DATE
   export DW_SA_IN=$IN_DIR/$JOB_ENV/$SUBJECT_AREA/$TABLE_ID/$UOW_TO_DATE/$UOW_TO_HH/$UOW_TO_MI/$UOW_TO_SS
else
   export DW_SA_IN=`eval print $IN_DIR/$JOB_ENV/$SUBJECT_AREA`
fi

export IN_DIR=$DW_SA_IN

if [[ ! -d $IN_DIR ]]
then
   mkdir -p $IN_DIR
fi

set +e
grep "^$RECORD_ID[ 	]*$HDP_CONN\>" $TABLE_LIS_FILE | read RECORD_ID_T HDP_CONN_T PARALLEL_NUM SOURCE_FILE TARGET_FILE MISC
rcode=$?
set -e
if [ $rcode != 0 ]
then
  print "${0##*/}:  INFRA_ERROR, failure determining value for $RECORD_ID parameter from $TABLE_LIS_FILE" >&2
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
  print "${0##*/}:  INFRA_ERROR, failure determining value for $HDP_CONN parameter from $DW_LOGINS/hadoop_logins.dat" >&2
  exit 4
fi

if [[ -z $HD_USERNAME ]]
  then
    print "INFRA_ERROR: can't not deterine batch account the connect hadoop cluster"
    exit 4
fi

if [[ "X"$HDFS_URL != "X" ]]
then
  set +e
  print $HDFS_URL | grep -i $DW_HD1_DB
  isAres=$?
  print $HDFS_URL | grep -i $DW_HD2_DB
  isApollo=$?
  print $HDFS_URL | grep -i $DW_HD3_DB
  isArtemis=$?
  set -e

  if [ $isAres == 0 ]
  then
   . $DW_MASTER_CFG/.${DW_HD1_DB}_env.sh
  elif [ $isApollo == 0 ]
  then
   . $DW_MASTER_CFG/.${DW_HD2_DB}_env.sh
  elif [ $isArtemis == 0 ]
  then
   . $DW_MASTER_CFG/.${DW_HD3_DB}_env.sh
  fi
else
  set +e
  print $HADOOP_HOME | grep -i $DW_HD1_DB
  isAres=$?
  print $HADOOP_HOME | grep -i $DW_HD2_DB
  isApollo=$?
  print $HADOOP_HOME | grep -i $DW_HD3_DB
  isArtemis=$?
  set -e

  if [ $isAres == 0 ]
  then
    export HDFS_URL=$HD1_NN_URL
  elif [ $isApollo == 0 ]
  then
    export HDFS_URL=$HD2_NN_URL
  elif [ $isArtemis == 0 ]
  then
    export HDFS_URL=$HD3_NN_URL
  fi
fi

export PATH=$JAVA_HOME/bin:$PATH:$HADOOP_HOME/bin
CLASSPATH=`$HADOOP_HOME/bin/hadoop classpath`
CLASSPATH=$CLASSPATH:$DW_MASTER_LIB/hadoop_ext/DataplatformETLHandlerUtil.jar
Dataplatform_SEQ_FILE_JAR=$DW_MASTER_LIB/hadoop_ext/DataplatformETLHandlerUtil.jar
export HADOOP_COMMAND="$JAVA_HOME/bin/java -cp $CLASSPATH DataplatformRunJar sg_adm ~sg_adm/.keytabs/apd.sg_adm.keytab $HD_USERNAME"
dwi_assignTagValue -p HD_SEQUENCE_FILE -t HD_SEQUENCE_FILE -f $ETL_CFG_FILE -s N -d 0
dwi_assignTagValue -p HD_FILE_FORMAT -t HD_FILE_FORMAT -f $ETL_CFG_FILE -s N -d "F"

set +e
grep "^CNDTL_COMPRESSION\>" $DW_CFG/$ETL_ID.cfg | read PARAM VALUE COMMENT;  IS_COMPRESS=${VALUE:-0}
rcode=$?
set -e

if [ $rcode != 0 ]
then
  print "${0##*/}: WARNING, failure determining value for CNDTL_COMPRESSION parameter from $DW_CFG/$ETL_ID.cfg" >&2
fi

if [ $IS_COMPRESS = 1 ]
then
  set +e
  grep "^CNDTL_COMPRESSION_SFXN\>" $DW_CFG/$ETL_ID.cfg | read PARAM VALUE COMMENT; COMPRESS_SFX=${VALUE:-".gz"}
  rcode=$?
  set -e

  if [ $rcode != 0 ]
  then
   print "${0##*/}: WARNING, failure determining value for CNDTL_COMPRESSION_SFX parameter from $DW_CFG/$ETL_ID.cfg" >&2
  fi
else
   COMPRESS_SFX=""
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


if [[ $myName != $HD_USERNAME ]]
    then
        export HADOOP_PROXY_USER=$HD_USERNAME
        print "Running ex job via user $HD_USERNAME"
fi

$HADOOP_HOME/bin/hadoop  fs -ls $HDFS_URL$HDFS_PATH/$SOURCE_FILE    > /dev/null

for data_file_entry in `$HADOOP_HOME/bin/hadoop  fs -ls $HDFS_URL$HDFS_PATH/$SOURCE_FILE | awk '{ print $8 }' | sort -d | awk '{ printf $1" " }'`
do
  MOD_NUM=$(( $FILE_ID % $SRC_HOST_CNT ))
  if [ $MOD_NUM -eq $HOST_ID ]
  then
    if [[ -n $UOW_TO ]]
    then
      OUTPUT_FILE=$TARGET_FILE.$HOST_ID.$FILE_ID.dat
    else
      OUTPUT_FILE=$TARGET_FILE.$HOST_ID.$FILE_ID.dat.$BATCH_SEQ_NUM
    fi
    
    print "$data_file_entry" "$OUTPUT_FILE" >> $DATA_LIS_FILE
  fi
  FILE_ID=$(( FILE_ID + 1 ))
done

LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.single_hdfs_extract.$RECORD_ID.$HOST_ID.$CURR_DATETIME.log

FILE_ID=0
while read SOURCE_FILE_TMP TARGET_FILE_TMP
do
  SOURCE_FILE_NAME=${SOURCE_FILE_TMP##*/}
  #RCODE=`grepCompFile "^$SOURCE_FILE_NAME\>" $MULTI_HDP_COMP_FILE`
  set +e
  grep -s "^$SOURCE_FILE_NAME\>" $MULTI_HDP_COMP_FILE > /dev/null
  RCODE=$?
  set -e

  if [ $RCODE = 1 ]
  then
    while [ $(jobs -p | wc -l) -ge $N_WAY_PER_HOST ]
    do
      sleep 30
      continue
    done
    
    
    #Add the step to delete the target file in case the job failed in the middle of process
    #Hadoop client does not automatically roll back the purge the un-completed data file
    if [[ -f $IN_DIR/`print $(eval print $TARGET_FILE_TMP)` ]]
    then
    	rm -f $IN_DIR/`print $(eval print $TARGET_FILE_TMP)`
    fi
    if [[ $HD_FILE_FORMAT == "A" ]]
      then
        dwi_assignTagValue -p PARSE_JSON_VALUE -t PARSE_JSON_VALUE -f $ETL_CFG_FILE -s N -d ""
        if [[ X"$PARSE_JSON_VALUE" != X ]]
        then
          COMMAND="$HADOOP_HOME/bin/hadoop jar $DW_HOME/jar/DSSEtlUtils.jar com.ebay.dss.etl.AvroToText -D parse.json.value=$PARSE_JSON_VALUE $SOURCE_FILE_TMP | gzip > $IN_DIR/`print $(eval print $TARGET_FILE_TMP$COMPRESS_SFX)`"
        else
          COMMAND="$HADOOP_HOME/bin/hadoop jar $DW_HOME/jar/DSSEtlUtils.jar com.ebay.dss.etl.AvroToText $SOURCE_FILE_TMP | gzip > $IN_DIR/`print $(eval print $TARGET_FILE_TMP$COMPRESS_SFX)`"
        fi
      else
        if [[ $HD_SEQUENCE_FILE = 0 ]]
        then
          COMMAND="$HADOOP_HOME/bin/hadoop fs -copyToLocal $SOURCE_FILE_TMP $IN_DIR/`print $(eval print $TARGET_FILE_TMP$COMPRESS_SFX)`"
        else
          COMMAND="$HADOOP_HOME/bin/hadoop jar $Dataplatform_SEQ_FILE_JAR DataplatformReadSeqFile $SOURCE_FILE_TMP > $IN_DIR/`print $(eval print $TARGET_FILE_TMP$COMPRESS_SFX)`"
        fi
      fi        
    set +e
    eval $COMMAND && (print "Extract completion of FILE: $SOURCE_FILE_NAME."; print "$SOURCE_FILE_NAME" >> $MULTI_HDP_COMP_FILE) || (print "INFRA_ERROR - Failure processing FILE: $SOURCE_FILE_NAME, HDFS: $HDFS_URL") &
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
    print "${0##*/}: INFRA_ERROR - Multiple hadoop file extract does not completely finish"
  fi
else
  print "${0##*/}: INFRA_ERROR - Failure on processing $MULTI_HDP_COMP_FILE"
fi
