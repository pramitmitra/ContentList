#!/bin/ksh -eu
#------------------------------------------------------------------------------------------------
# Filename:     single_scp_extract.ksh
#
# Revision History:
#
# Name             Date            Description
# ---------------  --------------  ---------------------------------------------------
# ???              ??/??/????      Initial Creation
# Ryan Wong        10/04/2013      Redhat changes
# John Hackley     08/25/2015      Password encryption changes
# John Hackley     02/15/2018      Enable SFTP Proxy as part of Gauls decommissioning
#
#------------------------------------------------------------------------------------------------

ETL_ID=$1
FILE_ID=$2
SCP_CONN=$3
SOURCE_FILE=$4
TARGET_FILE=$5	

# Retrieve and decrypt password for $SCP_NAME
DWI_fetch_pw $ETL_ID scp $SCP_CONN
DWIrc=$?

if [[ -z $SCP_PASSWORD ]]
then
  print "Unable to retrieve SCP password, exiting; ETL_ID=$ETL_ID; SCP_CONN=$SCP_CONN"
  exit $DWIrc
fi


set +e
grep "^EXTRACT_USE_SFTP_PROXY\>" $DW_CFG/$ETL_ID.cfg | read PARAM VALUE COMMENT;  EXTRACT_USE_SFTP_PROXY=${VALUE:-0}
rcode=$?
set -e

if [ $rcode != 0 ]
then
    print "${0##*/}: WARNING, failure determining value for EXTRACT_USE_SFTP_PROXY parameter from $DW_CFG/$ETL_ID.cfg" >&2
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

SOURCE_FILE_TMP=`print $(eval print $SOURCE_FILE)`
SOURCE_FILE_TMP=$SOURCE_FILE_TMP$COMPRESS_SFX
TARGET_FILE_TMP=`print $(eval print $TARGET_FILE)`
#TARGET_FILE_TMP=${TARGET_FILE_TMP%%$COMPRESS_SFX}.$BATCH_SEQ_NUM$COMPRESS_SFX

  if [[ -n $UOW_TO ]]
  then
  	 TARGET_FILE_TMP=${TARGET_FILE_TMP%%$COMPRESS_SFX}$COMPRESS_SFX
  else
  	 TARGET_FILE_TMP=${TARGET_FILE_TMP%%$COMPRESS_SFX}.$BATCH_SEQ_NUM$COMPRESS_SFX
  fi

# Note that the name and port for the SFTP Proxy host are hard-coded here; a better home to hard-code would be etlenv.setup but
# too many of us are modifying it simultaneously this month

if [ $EXTRACT_USE_SFTP_PROXY = 1 ]
then
  print "${0##*/}: INFO, transfer command line is: scp -v -B -o 'ProxyCommand nc -X connect -x sftpproxy.vip.ebay.com:2222 %h %p' \
    $SCP_USERNAME@$SCP_HOST:$REMOTE_DIR/$SOURCE_FILE_TMP $IN_DIR/$TARGET_FILE_TMP" >&2
  scp -v -B -o "ProxyCommand nc -X connect -x sftpproxy.vip.ebay.com:2222 %h %p" \
    $SCP_USERNAME@$SCP_HOST:$REMOTE_DIR/$SOURCE_FILE_TMP $IN_DIR/$TARGET_FILE_TMP >&2
else
  print "${0##*/}: INFO, transfer command line is: scp -v -B $SCP_USERNAME@$SCP_HOST:$REMOTE_DIR/$SOURCE_FILE_TMP $IN_DIR/$TARGET_FILE_TMP" >&2
  scp -v -B $SCP_USERNAME@$SCP_HOST:$REMOTE_DIR/$SOURCE_FILE_TMP $IN_DIR/$TARGET_FILE_TMP >&2
fi
 
((FILE_REC_COUNT=`ls -l $IN_DIR/$TARGET_FILE_TMP | tr -s ' '| cut -d' ' -f5`/100))

print $FILE_REC_COUNT > $DW_SA_TMP/$TABLE_ID.$JOB_TYPE_ID.$FILE_ID.record_count.dat

if [[ $LAST_EXTRACT_TYPE = "V" ]]
then
	print $TO_EXTRACT_VALUE > $DW_SA_DAT/$TABLE_ID.$FILE_ID.last_extract_value.dat
fi
