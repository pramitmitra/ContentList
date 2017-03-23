#!/bin/ksh -eu
# Title:        Single Secure File Transfer Extract
# File Name:    dw_infra.single_secure_file_transfer_extract.ksh
# Description:  singleton scp script - called by run
#                 File transfer script to be used by Secure File Transfer batch accounts.
#                 Standardize and limit execution of secure accounts.  Least access possible.
# Developer:    Ryan Wong
# Created on:   2016-12-08
# Location:     $DW_MASTER_BIN
# Logic:        Current approved transfer protocols are sftp and scp.
#                 This only supports scp, since it's more suitable for batch than sftp.
#
#
# Called by:    UC4/Unix
#
# Date         Ver#   Modified By(Name)            Change and Reason for Change
#---------    -----  ---------------------------  ------------------------------
# 2016-12-08   1.0    Ryan Wong                    Initital
# 2017-01-18   1.1    Ryan Wong                    Add scp port option, and changed default to 10022
#############################################################################################################

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

set +e
grep "^EXTRACT_SCP_PORT\>" $DW_CFG/$ETL_ID.cfg | read PARAM VALUE COMMENT
rcode=$?
set -e

if [ $rcode != 0 ]
then
   print "${0##*/}: INFO, no value for EXTRACT_SCP_PORT parameter from $DW_CFG/$ETL_ID.cfg, defaulting to 10022" >&2
   EXTRACT_SCP_PORT=10022
else
   print "${0##*/}: INFO, found EXTRACT_SCP_PORT parameter from $DW_CFG/$ETL_ID.cfg, value is $VALUE" >&2
   EXTRACT_SCP_PORT=$VALUE
fi


SOURCE_FILE_TMP=`print $(eval print $SOURCE_FILE)`
SOURCE_FILE_TMP=$SOURCE_FILE_TMP$COMPRESS_SFX
TARGET_FILE_TMP=`print $(eval print $TARGET_FILE)`

  if [[ -n $UOW_TO ]]
  then
  	 TARGET_FILE_TMP=${TARGET_FILE_TMP%%$COMPRESS_SFX}$COMPRESS_SFX
  else
  	 TARGET_FILE_TMP=${TARGET_FILE_TMP%%$COMPRESS_SFX}.$BATCH_SEQ_NUM$COMPRESS_SFX
  fi

print "#############################################################################################################"
print "Running scp command from Source to Target"
print "Source:  $SCP_USERNAME@$SCP_HOST:$REMOTE_DIR/$SOURCE_FILE_TMP"
print "Target:  $IN_DIR/$TARGET_FILE_TMP"
print "#############################################################################################################"

scp -v -B -P $EXTRACT_SCP_PORT $SCP_USERNAME@$SCP_HOST:$REMOTE_DIR/$SOURCE_FILE_TMP $IN_DIR/$TARGET_FILE_TMP >&2
 
((FILE_REC_COUNT=`ls -l $IN_DIR/$TARGET_FILE_TMP | tr -s ' '| cut -d' ' -f5`/100))

print $FILE_REC_COUNT > $DW_SA_TMP/$TABLE_ID.$JOB_TYPE_ID.$FILE_ID.record_count.dat

if [[ $LAST_EXTRACT_TYPE = "V" ]]
then
	print $TO_EXTRACT_VALUE > $DW_SA_DAT/$TABLE_ID.$FILE_ID.last_extract_value.dat
fi
