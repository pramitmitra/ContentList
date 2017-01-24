#!/bin/ksh -eu
#############################################################################################################
# Title:        Single Secure File Transfer load
# File Name:    dw_infra.single_secure_file_transfer_load.ksh
# Description:  singleton scp script - called by multi
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
# 2016-12-09   1.0    Ryan Wong                    Initital
# 2017-01-18   1.1    Ryan Wong                    Add scp port option, and changed default to 10022
#############################################################################################################

ETL_ID=$1
FILE_ID=$2
SCP_CONN=$3
SOURCE_FILE=$4
TARGET_FILE=$5	
now=`date '+20%y%m%d-%H:%M:%S'`

SUBJECT_AREA=${ETL_ID%%.*}
TABLE_ID=${ETL_ID##*.}

# Retrieve and decrypt password
DWI_fetch_pw $ETL_ID scp $SCP_CONN
DWIrc=$?

if [[ -n $SCP_PASSWORD ]]
then
  FTP_URL=$URL
else
  print "Unable to retrieve SCP password, exiting; ETL_ID=$ETL_ID; SCP_CONN=$SCP_CONN"
  exit $DWIrc
fi

export TRANSFER_PROCESS_TYPE=S

set +e
grep "^CNDTL_SCP_COMPRESSION\>" $DW_CFG/$ETL_ID.cfg | read PARAM CNDTL_SCP_COMPRESSION COMMENT
rcode=$?
set -e

if [ $rcode != 0 ]
then
  CNDTL_SCP_COMPRESSION=0
fi

if [ $CNDTL_SCP_COMPRESSION != 1 ]
then
  export CNDTL_SCP_COMPRESSION_SFX=""
else
  set +e
  grep "^CNDTL_SCP_COMPRESSION_SFX\>" $DW_CFG/$ETL_ID.cfg | read PARAM CNDTL_SCP_COMPRESSION_SFX COMMENT
  rcode=$?
  set -e
  if [ $rcode != 0 ]
  then
    CNDTL_SCP_COMPRESSION_SFX=".gz"
  fi
fi

set +e
grep "^LOAD_SCP_PORT\>" $DW_CFG/$ETL_ID.cfg | read PARAM VALUE COMMENT
rcode=$?
set -e

if [ $rcode != 0 ]
then
   print "${0##*/}: INFO, no value for LOAD_SCP_PORT parameter from $DW_CFG/$ETL_ID.cfg, defaulting to 10022" >&2
   LOAD_SCP_PORT=10022
else
   print "${0##*/}: INFO, found LOAD_SCP_PORT parameter from $DW_CFG/$ETL_ID.cfg, value is $VALUE" >&2
   LOAD_SCP_PORT=$VALUE
fi


SOURCE_FILE_TMP=`print $(eval print $SOURCE_FILE)`
TARGET_FILE_TMP=`print $(eval print $TARGET_FILE)`

if [[ -n $UOW_TO ]]
then
       SOURCE_FILE_TMP=$SOURCE_FILE_TMP
else
       SOURCE_FILE_TMP=$SOURCE_FILE_TMP.$BATCH_SEQ_NUM
fi

set +e
grep "^SCP_INCOMPLETE_SUFFIX\>" $DW_CFG/$ETL_ID.cfg | read PARAM SCP_INCOMPLETE_SUFFIX COMMENT
rcode=$?
set -e
if [ $rcode != 0 ]
then
  export SCP_INCOMPLETE_SUFFIX=""
else
  SCP_INCOMPLETE_SUFFIX=.$SCP_INCOMPLETE_SUFFIX
fi

print "#############################################################################################################"
print "Running scp command from Source to Target"
print "Source:  $DW_SA_OUT/$SOURCE_FILE_TMP${CNDTL_SCP_COMPRESSION_SFX}"
print "Target:  $SCP_USERNAME@$SCP_HOST:$REMOTE_DIR/$TARGET_FILE_TMP$SCP_INCOMPLETE_SUFFIX"
print "#############################################################################################################"
    
scp -v -B -P $LOAD_SCP_PORT $DW_SA_OUT/$SOURCE_FILE_TMP${CNDTL_SCP_COMPRESSION_SFX} $SCP_USERNAME@$SCP_HOST:$REMOTE_DIR/$TARGET_FILE_TMP$SCP_INCOMPLETE_SUFFIX >&2
 
if [ ! -z "$SCP_INCOMPLETE_SUFFIX" ]
then
  ssh $SCP_USERNAME@$SCP_HOST "mv $REMOTE_DIR/$TARGET_FILE_TMP$SCP_INCOMPLETE_SUFFIX $REMOTE_DIR/$TARGET_FILE_TMP" >&2
fi

if [[ $CNDTL_SCP_PUSH_TO_EXTRACT_VALUE = 1 ]]
then
        print $TO_EXTRACT_VALUE > $DW_SA_DAT/$TABLE_ID.$FILE_ID.last_scp_value.dat
fi

exit
