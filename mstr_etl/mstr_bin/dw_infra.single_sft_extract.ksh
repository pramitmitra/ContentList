#!/bin/ksh -eu
#------------------------------------------------------------------------------------------------
# Filename:     dw_infra.single_sft_extract.ksh
#
# Revision History:
#
# Name             Date            Description
# ---------------  --------------  ---------------------------------------------------
# ???              ??/??/????      Initial Creation
# Ryan Wong        10/04/2013      Redhat changes
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
     \?)  print >&2 "Usage: $0 -e <ETL_ID> -i <FILE_ID> -c <SFT_CONN> -s <SOURCE_FILE> -t <TARGET_FILE>"
     exit 1;;
esac
done
shift $(($OPTIND - 1))

ETL_ID=${_etl_id}
FILE_ID=${_file_id}
SFT_CONN=${_sft_conn}
SOURCE_FILE=${_src_file}
TARGET_FILE=${_tgt_file}

. $DW_MASTER_LIB/dw_etl_common_functions.lib

set +e
grep "^$SFT_CONN\>" $DW_LOGINS/sft_logins.dat | read SFT_NAME SFT_HOST SFT_USERNAME SFT_PASSWORD REMOTE_DIR RMT_SFT_PORT
rcode=$?
set -e

if [ $rcode != 0 ]
then
	print "${0##*/}:  ERROR, failure determining value for SFT_NAME parameter from $DW_LOGINS/sft_logins.dat" >&2
	exit 4
fi

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

assignTagValue SFT_BANDWIDTH SFT_BANDWIDTH $ETL_CFG_FILE W 32

SOURCE_FILE_TMP=`print $(eval print $SOURCE_FILE)`
SOURCE_FILE_TMP=$SOURCE_FILE_TMP$COMPRESS_SFX
TARGET_FILE_TMP=`print $(eval print $TARGET_FILE)`

if [[ -n $UOW_TO ]]
then
   TARGET_FILE_TMP=${TARGET_FILE_TMP%%$COMPRESS_SFX}$COMPRESS_SFX
else 
   TARGET_FILE_TMP=${TARGET_FILE_TMP%%$COMPRESS_SFX}.$BATCH_SEQ_NUM$COMPRESS_SFX
fi

sg_file_xfr_client -r $REMOTE_DIR/$SOURCE_FILE_TMP -t $IN_DIR/$TARGET_FILE_TMP -s $SFT_HOST -p $RMT_SFT_PORT -b $SFT_BANDWIDTH -l /dev/null >&2
 
((FILE_REC_COUNT=`ls -l $DW_SA_IN/$TARGET_FILE_TMP | tr -s ' '| cut -d' ' -f5`/100))

print $FILE_REC_COUNT > $DW_SA_TMP/$TABLE_ID.$JOB_TYPE_ID.$FILE_ID.record_count.dat

if [[ $LAST_EXTRACT_TYPE = "V" ]]
then
	print $TO_EXTRACT_VALUE > $DW_SA_DAT/$TABLE_ID.$FILE_ID.last_extract_value.dat
fi
