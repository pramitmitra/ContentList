#!/bin/ksh -eu
#------------------------------------------------------------------------------------------------
# Filename:     dw_infra.single_sft_push.ksh
#
# Revision History:
#
# Name             Date            Description
# ---------------  --------------  ---------------------------------------------------
# ???              ??/??/????      Initial Creation
# Ryan Wong        10/04/2013      Redhat changes
# John Hackley     09/11/2015      Password encryption changes
#
#------------------------------------------------------------------------------------------------

while getopts "e:i:c:s:t:" opt
do
case $opt in
     e)   _etl_id="$OPTARG";;
     i)   _file_id="$OPTARG";;
     c)   _sft_conn="$OPTARG";;
     s)   _src_file="$OPTARG";;
     t)   _tgt_file="$OPTARG";;
     \?)  print >&2 "Usage: $0 -f <EXTRACT_LIS_FILE> -c <SFT_CONN> -l <PARENT_LOG_FILE>"
     exit 1;;
esac
done
shift $(($OPTIND - 1))


 
ETL_ID=${_etl_id}
FILE_ID=${_file_id}
SFT_CONN=${_sft_conn}
SOURCE_FILE=${_src_file}
TARGET_FILE=${_tgt_file}
now=`date '+20%y%m%d-%H:%M:%S'`

SUBJECT_AREA=${ETL_ID%%.*}
TABLE_ID=${ETL_ID##*.}

. $DW_MASTER_LIB/dw_etl_common_functions.lib

DWI_fetch_pw $ETL_ID sft $SFT_CONN
DWIrc=$?

if [[ -z $SFT_PASSWORD ]]
then
  print "Unable to retrieve SFT password, exiting; ETL_ID=$ETL_ID; SFT_CONN=$SFT_CONN"
  exit $DWIrc
else
  SFT_PORT=$RMT_SFT_PORT
fi

set +e
grep "^CNDTL_SFT_COMPRESSION\>" $DW_CFG/$ETL_ID.cfg | read PARAM CNDTL_SFT_COMPRESSION COMMENT
rcode=$?
set -e

if [ $rcode != 0 ]
then
  CNDTL_SFT_COMPRESSION=0
fi

if [ $CNDTL_SFT_COMPRESSION != 1 ]
then
  export CNDTL_SFT_COMPRESSION_SFX=''
else
  set +e
  grep "^CNDTL_SFT_COMPRESSION_SFX\>" $DW_CFG/$ETL_ID.cfg | read PARAM CNDTL_SFT_COMPRESSION_SFX COMMENT
  rcode=$?
  set -e
  if [ $rcode != 0 ]
  then
    CNDTL_SFT_COMPRESSION_SFX=".gz"
  fi
fi

assignTagValue SFT_BANDWIDTH SFT_BANDWIDTH $ETL_CFG_FILE W 32

SOURCE_FILE_TMP=`print $(eval print $SOURCE_FILE)`
TARGET_FILE_TMP=`print $(eval print $TARGET_FILE)`

if [[ -n $UOW_TO ]]
then
   SOURCE_FILE_TMP_NAME=$SOURCE_FILE_TMP${CNDTL_SFT_COMPRESSION_SFX}
else
   SOURCE_FILE_TMP_NAME=$SOURCE_FILE_TMP.$BATCH_SEQ_NUM${CNDTL_SFT_COMPRESSION_SFX}
fi

sg_file_xfr_client -d 2 -r $REMOTE_DIR/$TARGET_FILE_TMP -t $DW_SA_OUT/$SOURCE_FILE_TMP_NAME -s $SFT_HOST -p $SFT_PORT -b $SFT_BANDWIDTH -l /dev/null >&2       


if [[ $CNDTL_SFT_PUSH_TO_EXTRACT_VALUE = 1 ]]
then
        print $TO_EXTRACT_VALUE > $DW_SA_DAT/$TABLE_ID.$FILE_ID.last_sft_value.dat
fi
