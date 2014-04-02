#!/bin/ksh -eu
# Title:        Run Multi SFT Push
# File Name:    dw_infra.run_multi_sft_push.ksh
# Description:  Run multiple sft push in throttled parallel
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
 
while getopts "f:c:l:" opt
do
case $opt in
     f)   _lis_file="$OPTARG";;
     c)   _sft_conn="$OPTARG";;
     l)   _log_file="$OPTARG";;
     \?)  print >&2 "Usage: $0 -f <EXTRACT_LIS_FILE> -c <SFT_CONN> -l <PARENT_LOG_FILE>"
     exit 1;;
esac
done
shift $(($OPTIND - 1))

export TABLE_LIS_FILE=${_lis_file}
export SFT_CONN=${_sft_conn}.sft
export PARENT_LOG_FILE=${_log_file}

. $DW_MASTER_LIB/dw_etl_common_functions.lib

set +e
grep "^$SFT_CONN\>" $DW_LOGINS/sft_logins.dat | read SFT_NAME SFT_HOST SFT_USERNAME SFT_PASSWORD REMOTE_DIR RMT_SFT_PORT
rcode=$?
set -e

if [ $rcode != 0 ]
then
	print "${0##*/}:  ERROR, failure determining value for SFT_NAME parameter from $DW_LOGINS/sft_logins.dat" >> $ERROR_FILE
	exit 4
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

integer PLIM
if [ ${USE_GROUP_EXTRACT:-0} -eq 1 ]
then
  PLIM_TMP=${SFT_LIS_FILE%.lis.*}
else
  PLIM_TMP=${SFT_LIS_FILE%.lis}
fi
PLIM=${PLIM_TMP##*.}  # parallel process count limit
#PLIS=$$               # process id list, initialized to current process id
#((PLIM+=2))           # adjustment for header row and parent in ps output


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

SFILE=$DW_SA_TMP/$TABLE_ID.$SFT_CONN.run_multi_sft_push.lis.$BATCH_SEQ_NUM
XFILE=$DW_SA_TMP/$TABLE_ID.$SFT_CONN.run_multi_sft_push.xpt.$BATCH_SEQ_NUM

>$SFILE

if [ -s $XFILE ]
then
   mv $XFILE $SFILE
else
   while read FILE_ID SFT_CONN_TMP SOURCE_FILE TARGET_FILE PARAM_LIST
   do  
      SOURCE_FILE_TMP=`print $(eval print $SOURCE_FILE)`
      TARGET_FILE_TMP=`print $(eval print $TARGET_FILE)`   
      
      if [[ -n $UOW_TO ]]
      then
         SOURCE_FILE_TMP_NAME=$SOURCE_FILE_TMP$CNDTL_SFT_COMPRESSION_SFX
      else
         SOURCE_FILE_TMP_NAME=$SOURCE_FILE_TMP.$BATCH_SEQ_NUM$CNDTL_SFT_COMPRESSION_SFX
      fi

      if [ ! -f $SFILE ]
      then
         eval print $SFT_HOST:$REMOTE_DIR/$TARGET_FILE_TMP,$DW_SA_OUT/$SOURCE_FILE_TMP_NAME > $SFILE
      else
         eval print $SFT_HOST:$REMOTE_DIR/$TARGET_FILE_TMP,$DW_SA_OUT/$SOURCE_FILE_TMP_NAME >> $SFILE   
      fi
        
   done < $TABLE_LIS_FILE
fi
COMMAND="$DW_MASTER_BIN/sg_file_xfr_client -d 2 -f $SFILE -x $XFILE -l /dev/null -p $RMT_SFT_PORT -n $PLIM -b $SFT_BANDWIDTH"

set +e
grep "^$SFT_CONN\>" $MULTI_COMP_FILE >/dev/null
rcode=$?
set -e            

if [ $rcode = 1 ]
then
  
  set +e
  eval $COMMAND
  rc=$?
  set -e
  
  if [ $rc != 0 ]
  then
    print "${0##*/}:  ERROR, see log file $PARENT_LOG_FILE" >> $ERROR_FILE
    exit $rc
  elif [ -s $XFILE ]
  then
    print "${0##*/}:  ERROR, there are files rejected, see exception file $XFILE" >> $ERROR_FILE
    exit 4
  else
    rm -f $XFILE
    rm -f $SFILE
    
    if [[ $CNDTL_SFT_PUSH_TO_EXTRACT_VALUE = 1 ]]
    then
        while read FILE_ID BALABALA
        do
        	if [ -f $DW_SA_DAT/$TABLE_ID.$FILE_ID.last_sft_value.dat ]; then
             export FROM_EXTRACT_VALUE=$(<$DW_SA_DAT/$TABLE_ID.$FILE_ID.last_sft_value.dat)
             print $FROM_EXTRACT_VALUE
          fi
          set +e
	        grep "^SFT_PUSH_TO_EXTRACT_VALUE_FUNCTION\>" $DW_CFG/$ETL_ID.cfg | read PARAM SFT_PUSH_TO_EXTRACT_VALUE_FUNCTION COMMENT
	        rcode=$?
	        set -e

	        if [ $rcode != 0 ]
	        then
		        print "${0##*/}:  ERROR, failure determining value for SFT_PUSH_TO_EXTRACT_VALUE_FUNCTION parameter from $DW_CFG/$ETL_ID.cfg" >&2
		        exit 4
	        fi

	        export TO_EXTRACT_VALUE=`eval $(eval print $SFT_PUSH_TO_EXTRACT_VALUE_FUNCTION)`
	        print $TO_EXTRACT_VALUE
          
          print $TO_EXTRACT_VALUE > $DW_SA_DAT/$TABLE_ID.$FILE_ID.last_sft_value.dat
        done < $TABLE_LIS_FILE
    fi

    print "Logging completion of TARGET: $SFT_CONN, to $MULTI_COMP_FILE"
    print "$SFT_CONN" >> $MULTI_COMP_FILE
  fi

else
print "Push to TARGET: $SFT_CONN already complete" >> $PARENT_LOG_FILE
fi

exit 0
