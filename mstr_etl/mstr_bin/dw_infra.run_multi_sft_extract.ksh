#!/bin/ksh -eu
# Title:        Run Multi SFT Extract
# File Name:    dw_infra.run_multi_sft_extract.ksh
# Description:  Run multiple sft extract in throttled parallel
# Developer:    ???
# Created on:
# Location:     $DW_MASTER_BIN
# Logic:
#
#
# Called by:    UC4/Unix
#
# Date         Ver#   Modified By(Name)            Change and Reason for Change
#---------    -----  ---------------------------  ------------------------------
# ????-??-??   1.0    ???                           Initial
# 2012-12-11   1.1    Ryan Wong                     If USE_GROUP_EXTRACT, remove GROUP_NUM before finding PLIM
# 2013-10-04   1.2    Ryan Wong                     Redhat changes
#
#############################################################################################################

while getopts "c:f:l:" opt
do
case $opt in
     c)   _sft_conn="$OPTARG";;
     f)   _lis_file="$OPTARG";;
     l)   _log_file="$OPTARG";;
     \?)  print >> $ERROR_FILE "Usage: $0 -c <EXTRACT_TYPE> -f <EXTRACT_LIS_FILE> -l <PARENT_LOG_FILE>"
     exit 1;;
esac
done
shift $(($OPTIND - 1))

SFT_CONN=${_sft_conn}.sft
TABLE_LIS_FILE=${_lis_file}
PARENT_LOG_FILE=${_log_file}

. $DW_MASTER_LIB/dw_etl_common_functions.lib

DWI_fetch_pw $ETL_ID sft $SFT_CONN
DWIrc=$?

if [[ -z $SFT_PASSWORD ]]
then
  print "Unable to retrieve SFT password, exiting; ETL_ID=$ETL_ID; SFT_CONN=$SFT_CONN"
  exit $DWIrc
fi
       
ERROR_FILE="${PARENT_LOG_FILE%.log}.err"
if [ -f $ERROR_FILE ]
then
	print "Moving the error file to r4a"
	mv $ERROR_FILE $ERROR_FILE.r4a
fi


# added the following piece to make the previous err file as r4a for the monitor

PREV_ERR_FILE=${PARENT_LOG_FILE%_extract.*log}_extract*err  

set +e
eval ls -tr $PREV_ERR_FILE|tail -1 |read PREV_ERR_FILE1
rcode=$?
set -e

if [ $rcode = 0 ]
then 
	set +e
	mv $PREV_ERR_FILE1 $PREV_ERR_FILE1.r4a 
	set -e
fi

#move to error file done

#tcode=4

#trap 'if [ $tcode = 4 ]; then cat $ERROR_FILE; cat $ERROR_FILE | mailx -s "$email_subject" $EMAIL_ERR_GROUP; fi' 0

integer PLIM
if [ ${USE_GROUP_EXTRACT:-0} -eq 1 ]
then
  PLIM_TMP=${EXTRACT_LIS_FILE%.lis.*}
else
  PLIM_TMP=${EXTRACT_LIS_FILE%.lis}
fi
PLIM=${PLIM_TMP##*.}  # parallel process count limit
#PLIS=$$               # process id list, initialized to current process id
#PID=$$
#((PLIM+=1))           # adjustment for header row and parent in ps output

CURR_DATETIME=$(date '+%Y%m%d-%H%M%S')

set +e
grep "^CNDTL_COMPRESSION\>" $DW_CFG/$ETL_ID.cfg | read PARAM VALUE COMMENT;  IS_COMPRESS=${VALUE:-0}
rcode=$?
set -e

if [ $rcode != 0 ]
then
  print "${0##*/}: WARNING, failure determining value for CNDTL_COMPRESSION parameter from $DW_CFG/$ETL_ID.cfg" >> $PARENT_LOG_FILE 2>&1
fi

if [ $IS_COMPRESS = 1 ]
then
  set +e
  grep "^CNDTL_COMPRESSION_SFXN\>" $DW_CFG/$ETL_ID.cfg | read PARAM VALUE COMMENT; COMPRESS_SFX=${VALUE:-".gz"}
  rcode=$?
  set -e

  if [ $rcode != 0 ]
  then
   print "${0##*/}: WARNING, failure determining value for CNDTL_COMPRESSION_SFX parameter from $DW_CFG/$ETL_ID.cfg" >> $PARENT_LOG_FILE 2>&1
  fi
else
   COMPRESS_SFX=""
fi

assignTagValue SFT_BANDWIDTH SFT_BANDWIDTH $ETL_CFG_FILE W 32

SFILE=$DW_SA_TMP/$TABLE_ID.${_sft_conn}.run_multi_sft_extract.lis.$BATCH_SEQ_NUM
XFILE=$DW_SA_TMP/$TABLE_ID.${_sft_conn}.run_multi_sft_extract.xpt.$BATCH_SEQ_NUM

>$SFILE

# If exception file exists and has size greater than 0, will restart the job with files in exception file
if [ -s $XFILE ]
then
   mv $XFILE $SFILE
else
   while read FILE_ID SFT_CONN SRC_FILENAME TGT_FILENAME PARAM_LIST
     do

   SOURCE_FILE_TMP=`print $(eval print $SRC_FILENAME)`
   SOURCE_FILE_TMP=$SOURCE_FILE_TMP$COMPRESS_SFX
   TARGET_FILE_TMP=`print $(eval print $TGT_FILENAME)`
   if [[ -n $UOW_TO ]]
   then 
      TARGET_FILE_TMP=${TARGET_FILE_TMP%%$COMPRESS_SFX}$COMPRESS_SFX
   else
      TARGET_FILE_TMP=${TARGET_FILE_TMP%%$COMPRESS_SFX}.$BATCH_SEQ_NUM$COMPRESS_SFX
   fi

   if [ ! -f $SFILE ]
   then
      eval print $SFT_HOST:$REMOTE_DIR/$SOURCE_FILE_TMP,$IN_DIR/$TARGET_FILE_TMP > $SFILE
   else
      eval print $SFT_HOST:$REMOTE_DIR/$SOURCE_FILE_TMP,$IN_DIR/$TARGET_FILE_TMP >> $SFILE
   fi
done < $TABLE_LIS_FILE
fi

set +e
sg_file_xfr_client -f $SFILE -x $XFILE -l /dev/null -p $RMT_SFT_PORT -n $PLIM -b $SFT_BANDWIDTH
rcode=$?
set -e

if [ $rcode != 0 ]
then
  print "${0##*/}:  ERROR, see log file $PARENT_LOG_FILE" >> $ERROR_FILE
  exit $rcode
elif [ -s $XFILE ]
then
  print "${0##*/}:  ERROR, there are files rejected, see exception file $XFILE" >> $ERROR_FILE
  exit 4
else
  rm -f $XFILE
  rm -f $SFILE
fi

while read FILE_ID SFT_CONN SRC_FILENAME TGT_FILENAME PARAM_LIST
do
  TARGET_FILE_TMP=`print $(eval print $TGT_FILENAME)`
  TARGET_FILE_TMP=${TARGET_FILE_TMP%%$COMPRESS_SFX}.$BATCH_SEQ_NUM$COMPRESS_SFX
  ((FILE_REC_COUNT=`ls -l $DW_SA_IN/$TARGET_FILE_TMP | tr -s ' '| cut -d' ' -f5`/100))

  print $FILE_REC_COUNT > $DW_SA_TMP/$TABLE_ID.$JOB_TYPE_ID.$FILE_ID.record_count.dat

  if [[ $LAST_EXTRACT_TYPE = "V" ]]
  then
	  print $TO_EXTRACT_VALUE > $DW_SA_DAT/$TABLE_ID.$FILE_ID.last_extract_value.dat
  fi
done < $TABLE_LIS_FILE

exit 0
