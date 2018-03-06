#!/bin/ksh -eu
#
# Ported to Redhat by koaks, 20120820
# ETL password encryption added by jhackley, 20150825
# Add SCP_PUSH_PORT cfg parameter to add port option for scp, 20170331
# Enable Site Https access through Web Proxy, 20170419
# Enable SFTP Proxy as part of Gauls decommissioning, Hackley, 20180221

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

set +e
grep "^LOAD_USE_SFTP_PROXY\>" $DW_CFG/$ETL_ID.cfg | read PARAM LOAD_USE_SFTP_PROXY COMMENT
rcode=$?
set -e

if [ $rcode != 0 ]
then
  print "${0##*/}: INFO, no value for LOAD_USE_SFTP_PROXY parameter from $DW_CFG/$ETL_ID.cfg, defaulting to 0" >&2
  LOAD_USE_SFTP_PROXY=0
else
  print "${0##*/}: INFO, found LOAD_USE_SFTP_PROXY parameter from $DW_CFG/$ETL_ID.cfg, value is $LOAD_USE_SFTP_PROXY" >&2
fi

set +e
grep "^TRANSFER_PROCESS_TYPE\>" $DW_CFG/$ETL_ID.cfg | read PARAM TRANSFER_PROCESS_TYPE COMMENT
rcode=$?
set -e

if [ $rcode != 0 ]
then
   export TRANSFER_PROCESS_TYPE=S
fi

set +e
grep "^SCP_PUSH_PORT\>" $DW_CFG/$ETL_ID.cfg | read PARAM VALUE COMMENT
rcode=$?
set -e

if [ $rcode != 0 ]
then
   print "${0##*/}: INFO, no value for SCP_PUSH_PORT parameter from $DW_CFG/$ETL_ID.cfg, defaulting to 22" >&2
   SCP_PUSH_PORT=22
else
   print "${0##*/}: INFO, found SCP_PUSH_PORT parameter from $DW_CFG/$ETL_ID.cfg, value is $VALUE" >&2
   SCP_PUSH_PORT=$VALUE
fi

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

SOURCE_FILE_TMP=`print $(eval print $SOURCE_FILE)`
TARGET_FILE_TMP=`print $(eval print $TARGET_FILE)`

if [[ -n $UOW_TO ]]
then
       SOURCE_FILE_TMP=$SOURCE_FILE_TMP
else
       SOURCE_FILE_TMP=$SOURCE_FILE_TMP.$BATCH_SEQ_NUM
fi

if  [ $TRANSFER_PROCESS_TYPE = S ]
then
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
     
#   Note that the name and port for the SFTP Proxy host are hard-coded here; a better home to hard-code would be etlenv.setup but
#   too many of us are modifying it simultaneously this month

    if [ $LOAD_USE_SFTP_PROXY = 1 ]
    then
      print "${0##*/}: INFO, transfer command line is: scp -v -B -P $SCP_PUSH_PORT -o 'ProxyCommand nc -X connect -x sftpproxy.vip.ebay.com:2222 %h %p' $DW_SA_OUT/$SOURCE_FILE_TMP${CNDTL_SCP_COMPRESSION_SFX} $SCP_USERNAME@$SCP_HOST:$REMOTE_DIR/$TARGET_FILE_TMP$SCP_INCOMPLETE_SUFFIX" >&2
      scp -v -B -P $SCP_PUSH_PORT -o "ProxyCommand nc -X connect -x sftpproxy.vip.ebay.com:2222 %h %p" $DW_SA_OUT/$SOURCE_FILE_TMP${CNDTL_SCP_COMPRESSION_SFX} $SCP_USERNAME@$SCP_HOST:$REMOTE_DIR/$TARGET_FILE_TMP$SCP_INCOMPLETE_SUFFIX >&2
	 
      if [ ! -z "$SCP_INCOMPLETE_SUFFIX" ]
      then
        ssh -o "ProxyCommand nc -X connect -x sftpproxy.vip.ebay.com:2222 %h %p" $SCP_USERNAME@$SCP_HOST "mv $REMOTE_DIR/$TARGET_FILE_TMP$SCP_INCOMPLETE_SUFFIX $REMOTE_DIR/$TARGET_FILE_TMP" >&2
      fi
    else
      print "${0##*/}: INFO, transfer command line is: scp -v -B -P $SCP_PUSH_PORT $DW_SA_OUT/$SOURCE_FILE_TMP${CNDTL_SCP_COMPRESSION_SFX} $SCP_USERNAME@$SCP_HOST:$REMOTE_DIR/$TARGET_FILE_TMP$SCP_INCOMPLETE_SUFFIX" >&2
      scp -v -B -P $SCP_PUSH_PORT $DW_SA_OUT/$SOURCE_FILE_TMP${CNDTL_SCP_COMPRESSION_SFX} $SCP_USERNAME@$SCP_HOST:$REMOTE_DIR/$TARGET_FILE_TMP$SCP_INCOMPLETE_SUFFIX >&2
      if [ ! -z "$SCP_INCOMPLETE_SUFFIX" ]
      then
        ssh $SCP_USERNAME@$SCP_HOST "mv $REMOTE_DIR/$TARGET_FILE_TMP$SCP_INCOMPLETE_SUFFIX $REMOTE_DIR/$TARGET_FILE_TMP" >&2
      fi
    fi

elif [ $TRANSFER_PROCESS_TYPE = SF ] 
then
    # Creatation of a batch file is needed in order to catch errors. stdin does not error out.
    print "cd $REMOTE_DIR" > $DW_SA_TMP/$ETL_ID.sftp.$SOURCE_FILE_TMP.$BATCH_SEQ_NUM
    print "put $DW_SA_OUT/$SOURCE_FILE_TMP $TARGET_FILE_TMP" >> $DW_SA_TMP/$ETL_ID.sftp.$SOURCE_FILE_TMP.$BATCH_SEQ_NUM 

    if [ $LOAD_USE_SFTP_PROXY = 1 ]
    then
      sftp -o "ProxyCommand nc -X connect -x sftpproxy.vip.ebay.com:2222 %h %p" -b $DW_SA_TMP/$ETL_ID.sftp.$SOURCE_FILE_TMP.$BATCH_SEQ_NUM $SCP_USERNAME@$SCP_HOST 
    else
      sftp -b $DW_SA_TMP/$ETL_ID.sftp.$SOURCE_FILE_TMP.$BATCH_SEQ_NUM $SCP_USERNAME@$SCP_HOST
    fi

    rm $DW_SA_TMP/$ETL_ID.sftp.$SOURCE_FILE_TMP.$BATCH_SEQ_NUM

elif [ $TRANSFER_PROCESS_TYPE = W ] 
then

    TARGET_FILE_TMP_SYM=`print $TARGET_FILE_TMP | sed 's/\%\%amp\%\%/\&/g'`
    $DW_ETL_WGET -d --delete-after -o $DW_SA_LOG/$TABLE_ID.$SOURCE_FILE.wget.$CURR_DATETIME.log --post-file=$DW_SA_OUT/$SOURCE_FILE_TMP "$FTP_URL$TARGET_FILE_TMP_SYM"

elif [ $TRANSFER_PROCESS_TYPE = F ] 
then
    scp -v -B -P $SCP_PUSH_PORT $DW_SA_OUT/$SOURCE_FILE_TMP $SCP_USERNAME@$SCP_HOST:$REMOTE_DIR/$TARGET_FILE_TMP >&2

    print $FTP_URL | sed 's#[|:;>@/]# #g' | read dummy _ext_user _ext_pwd _ext_host _ext_path  

    SOURCE_FILE_LOG=`print $(eval print $SOURCE_FILE | sed 's/\//_/g')`
    tmp_log_file=$REMOTE_DIR/$ETL_ID.$SOURCE_FILE_LOG.$CURR_DATETIME.log
    tmp_tempo_file=$REMOTE_DIR/$ETL_ID.$SOURCE_FILE_LOG.$CURR_DATETIME.tempo
    tmp_ext_path=`print  $(eval print ./$_ext_path | sed 's/ /\//g')` 
    ret=`ssh "${SCP_USERNAME}@${SCP_HOST}" "cd ${REMOTE_DIR};print \"open ${_ext_host}\\\nuser ${_ext_user} ${_ext_pwd}\\\nprompt\\\ncd ${tmp_ext_path}\\\nput ${TARGET_FILE}\\\nclose\" > ${tmp_tempo_file};cat ${tmp_tempo_file}|ftp -p -i -n -v -T300 > ${tmp_log_file};egrep -e \"No such file or directory|Unknown host|Not connected|Login failed|Not logged in|Permission denied|Connection timeout\" $tmp_log_file|wc -l"`

      if [ $ret != 0 ]
      then
	 print "${0##*/}:  ERROR, failed to FTP files" >&2
         ssh "$SCP_USERNAME@$SCP_HOST" "cat $tmp_log_file"
         exit 500
      else
         print "${0##*/}:  SUCCESSFULLY Transfered File to FTP server" >&2
         ssh "$SCP_USERNAME@$SCP_HOST" "cat $tmp_log_file"
       fi 


       # Remove file on ssh server.
       ssh "$SCP_USERNAME@$SCP_HOST" "rm ${tmp_log_file}"
       ssh "$SCP_USERNAME@$SCP_HOST" "rm ${tmp_tempo_file}"


elif  [ $TRANSFER_PROCESS_TYPE = R ]
then
       rsync -avvW -e ssh $DW_SA_OUT/$SOURCE_FILE_TMP $SCP_USERNAME@$SCP_HOST:$REMOTE_DIR/$TARGET_FILE_TMP  >&2
fi

if [[ $CNDTL_SCP_PUSH_TO_EXTRACT_VALUE = 1 ]]
then
        print $TO_EXTRACT_VALUE > $DW_SA_DAT/$TABLE_ID.$FILE_ID.last_scp_value.dat
fi

exit
