#!/bin/ksh -eu
# Title:        Single TPT Extract
# File Name:    single_tpt_extract.ksh
# Description:  Handle submiting a tpt extract job
# Developer:    Ryan Wong
# Created on:
# Location:     $DW_MASTER_EXE
# Logic:
#
#
# Called by:    UC4/Unix
#
# Date         Ver#   Modified By(Name)            Change and Reason for Change
#---------    -----  ---------------------------  ------------------------------
# 2012-11-01   1.0    Ryan Wong                     Initial
# 2012-11-26   1.1    Ryan Wong                     Updated to append QUERY_BAND and QUERY_BAND_STRING
# 2013-02-21   1.2    Ryan Wong                     Fix RECORD_COUNT logfile scrape in case of one instance.
# 2013-08-21   1.3    George Xiong                  Netstat on Redhat 
# 2013-10-08   1.4    Ryan Wong                     Redhat changes
# 2016-04-19   1.5    Ryan Wong                     Passing UOW_FROM and UOW_TO for SQL variables
#
#############################################################################################################

ETL_ID=$1
FILE_ID=$2
DBC_FILE=$3
TABLE_NAME=$4
DATA_FILENAME_TMP=$5
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
      print "FATAL ERROR:  Unexpected command line argument"
      print "Usage: single_tpt_extract.ksh <ETL_ID> <FILE_ID> <DBC_FILE> <TABLE_NAME> <DATA_FILENAME> -UOW_FROM <UOW_FROM> -UOW_TO <UOW_TO> -PARAM1 <PARAM1> -PARAM2 <PARAM2> -PARAM3 <PARAM3> -PARAM4 <PARAM4>"
      exit 4
  esac
done

. /dw/etl/mstr_cfg/etlenv.setup
. $DW_MASTER_LIB/dw_etl_common_functions.lib

EXTRACT_LOG_FILE=$DW_SA_LOG/$TABLE_ID.extract.$FILE_ID.single_tpt_extract$UOW_APPEND.$CURR_DATETIME.log
EXTRACT_TABLE_LOG_FILE=$DW_SA_LOG/$TABLE_ID.extract.$FILE_ID.utility_extract$UOW_APPEND.$CURR_DATETIME.log
RECORD_COUNT_FILE=$DW_SA_TMP/$TABLE_ID.ex.$FILE_ID.record_count.dat

# Handle tpt configuration parameters
# 
set -A TPT_CFG_VALUES TPT_EXTRACT_WORKING_DB TPT_EXTRACT_PORT TPT_EXTRACT_NWAYS TPT_EXTRACT_HEX_DELIMITER TPT_EXTRACT_CHAR_DELIMITER TPT_EXTRACT_CHARSET TPT_EXTRACT_TENACITY_HOURS TPT_EXTRACT_TENACITY_SLEEP TPT_EXTRACT_SESSIONS TPT_EXTRACT_DATE_FORMAT TPT_EXTRACT_COMPRESS_FLAG TPT_EXTRACT_COMPRESS_LEVEL TPT_EXTRACT_SPOOLMODE TPT_EXTRACT_BUFFERMODE TPT_EXTRACT_REMOVE_DELIMITER TPT_EXTRACT_TEXTOUT TPT_EXTRACT_VERBOSE TPT_EXTRACT_PRINT_INTERVAL

set -A TPT_MAP_VALUES WORKING_DB PORT NWAYS HEX_DELIMITER CHAR_DELIMITER CHARSET TENACITY_HOURS TENACITY_SLEEP SESSIONS DATE_FORMAT COMPRESS_FLAG COMPRESS_LEVEL SPOOLMODE BUFFERMODE REMOVE_DELIMITER TEXTOUT VERBOSE PRINT_INTERVAL

TPT_ARG=""
TPT_IDX=0
while [[ $TPT_IDX -lt ${#TPT_CFG_VALUES[*]} ]]
do
  TPT_NAME=${TPT_CFG_VALUES[$TPT_IDX]}
  assignTagValue $TPT_NAME $TPT_NAME $ETL_CFG_FILE W ""
  TPT_VALUE=$(eval print "\${$TPT_NAME}")
  if [[ "X$TPT_VALUE" != "X" ]]
  then
    TPT_ARG="$TPT_ARG -${TPT_MAP_VALUES[$TPT_IDX]} $TPT_VALUE"
  fi
  ((TPT_IDX+=1))
done

# If TPT_EXTRACT_NWAYS is not assigned, then error.
if [[ "X${TPT_EXTRACT_NWAYS:-}" = "X" ]]
then
  print "FATAL ERROR: Could not assign TPT_EXTRACT_NWAYS from $ETL_CFG_FILE" >&2
  exit 4
fi

# If TPT_EXTRACT_PORT is not assigned, then use random number
if [[ "X${TPT_EXTRACT_PORT:-}" = "X" ]]
then
  # if not set use random
  TPT_EXTRACT_PORT=$((6000+$RANDOM%2000))
  TPT_ARG="$TPT_ARG -PORT $TPT_EXTRACT_PORT"
fi

# Set MASTER_NODE
export MASTER_NODE=$servername
TPT_ARG="$TPT_ARG -MASTER_NODE $MASTER_NODE"



set +e
 netstat  -t|awk '{print $4}'|grep ${MASTER_NODE%%.*}|grep TPT_EXTRACT_PORT
 rcode=$?
set -e

if [ $rcode = 0 ]
then
 print "FATAL ERROR: Port number $TPT_EXTRACT_PORT is already in use" >&2       	
 exit 4                                                                        	
fi

# Check for query band
eval set -A myqb $(grep TPT_EXTRACT_QUERY_BAND $ETL_CFG_FILE)
export QUERY_BAND=${myqb[1]:-}
export QUERY_BAND_STRING="UTILITYNAME=TPTEXP;${QUERY_BAND_STRING%% *}$QUERY_BAND"
if [[ "X$QUERY_BAND" != "X" ]]
then
  TPT_ARG="$TPT_ARG -QUERY_BAND \"$QUERY_BAND_STRING\""
fi

# Pull db_name from dbc file
assignTagValue DB_NAME "^db_name" $DW_DBC/$DBC_FILE W ""
if [[ "X${DB_NAME:-}" = "X" ]]
then
  print "FATAL ERROR: Could not assign DB_NAME from $DBC_FILE" >&2
  exit 4
fi

# If character set not assigned, pull from dbc file.
if [[ "X$TPT_EXTRACT_CHARSET" = "X" ]]
then
  assignTagValue TPT_EXTRACT_CHARSET "teradata_character_set:" $DW_DBC/$DBC_FILE W ""
  if [[ "X$TPT_EXTRACT_CHARSET" != "X" ]]
  then
    TPT_ARG="$TPT_ARG -CHARSET $TPT_EXTRACT_CHARSET"
  fi
fi

# Calculate file extension
if [[ $TPT_EXTRACT_COMPRESS_FLAG = 1 ]]
then
  FILE_EXTN=".gz"
elif [[ $TPT_EXTRACT_COMPRESS_FLAG = 2 ]]
then
  FILE_EXTN=".bz2"
else
  FILE_EXTN=""
fi

if [[ $DATA_FILENAME_TMP = "N" ]]
then
  assignTagValue DATA_FILENAME DATA_FILENAME $ETL_CFG_FILE
else
  DATA_FILENAME=$DATA_FILENAME_TMP
fi

if [[ "X$UOW_TO" != "X" ]]
then
  TPT_ARG="$TPT_ARG -UOW_FROM $UOW_FROM -UOW_TO $UOW_TO"
  DATA_FILENAME_SFX=${FILE_EXTN}
else
  DATA_FILENAME_SFX=.$BATCH_SEQ_NUM${FILE_EXTN}
fi

#************************************************************************************************************
# Forward Params
#************************************************************************************************************
if [[ "X${PARAM1:-}" != "X" ]]
then
  TPT_ARG="$TPT_ARG -PARAM1 $PARAM1"
fi

if [[ "X${PARAM2:-}" != "X" ]]
then
  TPT_ARG="$TPT_ARG -PARAM2 $PARAM2"
fi

if [[ "X${PARAM3:-}" != "X" ]]
then
  TPT_ARG="$TPT_ARG -PARAM3 $PARAM3"
fi

if [[ "X${PARAM4:-}" != "X" ]]
then
  TPT_ARG="$TPT_ARG -PARAM4 $PARAM4"
fi

# For extract: Calculate FROM_EXTRACT_VALUE and TO_EXTRACT_VALUE
if [[ $LAST_EXTRACT_TYPE == "V" ]]
then
  assignTagValue TO_EXTRACT_VALUE_FUNCTION TO_EXTRACT_VALUE_FUNCTION $ETL_CFG_FILE
  LAST_EXTRACT_VALUE_FILE=$DW_SA_DAT/$TABLE_ID.$FILE_ID.last_extract_value.dat
  export FROM_EXTRACT_VALUE=$(<$LAST_EXTRACT_VALUE_FILE)
  export TO_EXTRACT_VALUE=$($TO_EXTRACT_VALUE_FUNCTION)
elif [[ $LAST_EXTRACT_TYPE == "U" ]]
then
  assignTagValue UOW_FROM_REFORMAT_CODE UOW_FROM_REFORMAT_CODE $ETL_CFG_FILE W 0
  assignTagValue UOW_TO_REFORMAT_CODE UOW_TO_REFORMAT_CODE $ETL_CFG_FILE W 0
  export FROM_EXTRACT_VALUE=$($DW_MASTER_BIN/dw_infra.reformat_timestamp.ksh $UOW_FROM $UOW_FROM_REFORMAT_CODE)
  export TO_EXTRACT_VALUE=$($DW_MASTER_BIN/dw_infra.reformat_timestamp.ksh $UOW_TO $UOW_TO_REFORMAT_CODE)
fi

if [[ "X$FROM_EXTRACT_VALUE" != "X" ]]
then
  TPT_ARG="$TPT_ARG -FROM_EXTRACT_VALUE \"$FROM_EXTRACT_VALUE\""
fi

if [[ "X$TO_EXTRACT_VALUE" != "X" ]]
then
  TPT_ARG="$TPT_ARG -TO_EXTRACT_VALUE \"$TO_EXTRACT_VALUE\""
fi

assignTagValue MULTI_HOST MULTI_HOST $ETL_CFG_FILE W 0
if [ $MULTI_HOST = 0 ]
  then
  HOSTS_LIST_FILE=$DW_CFG/$ETL_ID.host.lis
  if [ ! -f $HOSTS_LIST_FILE ]
  then
    print "${0##*/}:  FATAL ERROR: MULTI_HOST is zero, and $HOSTS_LIST_FILE does not exist" >&2
    exit 4
  fi
elif [[ $MULTI_HOST = 1 ]]
then
  TPT_NODE=$MASTER_NODE
elif [[ $MULTI_HOST = @(2||4||6||8||16||32) ]]
then
  HOSTS_LIST_FILE=$DW_MASTER_CFG/${MASTER_NODE%%.*}.${MULTI_HOST}ways.host.lis
else
  print "${0##*/}:  FATAL ERROR: MULTI_HOST not valid value $MULTI_HOST" >&2
  exit 4
fi

set -A TPT_HOSTS
if [ $MULTI_HOST = 1 ]
then
  TPT_HOSTS[0]=$TPT_NODE
else
  TPT_IDX=0
  while read TPT_NODE junk
  do
    TPT_HOSTS[$TPT_IDX]=$TPT_NODE
    ((TPT_IDX+=1))
  done < $HOSTS_LIST_FILE
fi

TPT_HOST_CNT=${#TPT_HOSTS[*]}

if [ $TPT_EXTRACT_NWAYS -lt $TPT_HOST_CNT ]
then
  print "FATAL ERROR: TPT_EXTRACT_NWAYS $TPT_EXTRACT_NWAYS is less than TPT_HOST_CNT $TPT_HOST_CNT" >&2
  exit 4
fi

# MASTER_NODE must be the first entry of the host list
MASTER_NODE_FOUND=0
instance_idx=0
while [[ $instance_idx -lt $TPT_HOST_CNT ]]
do
  if [ ${MASTER_NODE%%.*} = ${TPT_HOSTS[$instance_idx]%%.*} ]
  then
    MASTER_NODE_FOUND=1
    HOST_TMP=${TPT_HOSTS[0]}
    TPT_HOSTS[0]=${TPT_HOSTS[${instance_idx}]}
    TPT_HOSTS[${instance_idx}]=$HOST_TMP
    break
  fi
  ((instance_idx+=1))
done

if [ $MASTER_NODE_FOUND = 0 ]
then
  print "FATAL ERROR: Master Node not found in host file $HOSTS_LIST_FILE" >&2
  exit 4
fi

set -A host_instance_total
set -A host_instance_cnt

# Initialize array
instance_idx=0
while [[ $instance_idx -lt $TPT_HOST_CNT ]];
do
  host_instance_total[$instance_idx]=0
  host_instance_cnt[$instance_idx]=0
  ((instance_idx+=1))
done

# Determine number of instances per host
instance_idx=0
while [[ $instance_idx -lt $TPT_EXTRACT_NWAYS ]]
do
  host_idx=$(( $instance_idx % $TPT_HOST_CNT ))
  host_instance_total[$host_idx]=$(( ${host_instance_total[$host_idx]}+1))
  ((instance_idx+=1))
done

# Loop through all remote hosts and create remote log directories.
instance_idx=0
while [[ $instance_idx -lt $TPT_HOST_CNT ]];
do
  host_name=${TPT_HOSTS[${instance_idx}]}
  set +e
  ssh -nq $host_name "mkdir -p $DW_SA_LOG" > /dev/null
  set -e
  ((instance_idx+=1))
done


set -A pid_list
instance_idx=0
while [[ $instance_idx -lt $TPT_EXTRACT_NWAYS ]]
do
  host_idx=$(( $instance_idx % $TPT_HOST_CNT ))
  host_name=${TPT_HOSTS[${host_idx}]}
  instance_nbr=$(( $instance_idx + 1))

  host_instance_cnt[$host_idx]=$(( ${host_instance_cnt[$host_idx]} + 1 ))
  print "********************************************************************************"
  print "host_idx is $host_idx"
  print "host_name is $host_name"
  print "instance_nbr is $instance_nbr"
  print "Launching (${host_instance_cnt[$host_idx]}/${host_instance_total[$host_idx]})"

  if [ $MULTI_HOST = 1 ]
  then
    print "Local launch $DW_MASTER_EXE/dw_infra.single_tpt_run.ksh -OPERATOR_TYPE 2 -ETL_ID $ETL_ID -JOB_ENV $JOB_ENV -INSTANCE_NUM $instance_nbr -SQLFILE $DW_SA_TMP/$TABLE_ID.ex.$FILE_ID.sel.sql -FILENAME $IN_DIR/$DATA_FILENAME.$instance_nbr$DATA_FILENAME_SFX -LOGFILE $EXTRACT_TABLE_LOG_FILE.$instance_nbr -SERVER $DB_NAME -IN_DIR $IN_DIR $TPT_ARG"

    set +e
    eval $DW_MASTER_EXE/dw_infra.single_tpt_run.ksh -OPERATOR_TYPE 2 -ETL_ID $ETL_ID -JOB_ENV $JOB_ENV -INSTANCE_NUM $instance_nbr -SQLFILE $DW_SA_TMP/$TABLE_ID.ex.$FILE_ID.sel.sql -FILENAME $IN_DIR/$DATA_FILENAME.$instance_nbr$DATA_FILENAME_SFX -LOGFILE $EXTRACT_TABLE_LOG_FILE.$instance_nbr -SERVER $DB_NAME -IN_DIR $IN_DIR $TPT_ARG > $EXTRACT_LOG_FILE.$instance_nbr 2>&1 &
    pid_list[$instance_idx]=$!
    set -e

  else 
    print "Remote launch ssh -nq $host_name $DW_MASTER_EXE/dw_infra.single_tpt_run.ksh -OPERATOR_TYPE 2 -ETL_ID $ETL_ID -JOB_ENV $JOB_ENV -INSTANCE_NUM $instance_nbr -SQLFILE $DW_SA_TMP/$TABLE_ID.ex.$FILE_ID.sel.sql -FILENAME $IN_DIR/$DATA_FILENAME.$instance_nbr$DATA_FILENAME_SFX -LOGFILE $EXTRACT_TABLE_LOG_FILE.$instance_nbr -SERVER $DB_NAME -IN_DIR $IN_DIR $TPT_ARG"

    set +e
    ssh -nq $host_name "$DW_MASTER_EXE/dw_infra.single_tpt_run.ksh -OPERATOR_TYPE 2 -ETL_ID $ETL_ID -JOB_ENV $JOB_ENV -INSTANCE_NUM $instance_nbr -SQLFILE $DW_SA_TMP/$TABLE_ID.ex.$FILE_ID.sel.sql -FILENAME $IN_DIR/$DATA_FILENAME.$instance_nbr$DATA_FILENAME_SFX -LOGFILE $EXTRACT_TABLE_LOG_FILE.$instance_nbr -SERVER $DB_NAME -IN_DIR $IN_DIR $TPT_ARG" > $EXTRACT_LOG_FILE.$instance_nbr 2>&1 &
    pid_list[$instance_idx]=$!
    set -e

  fi

  ((instance_idx+=1))
  print "********************************************************************************"
done

# Wait and capture all pids
set -A pid_list_rcode
TPT_IDX=0
while [[ $TPT_IDX -lt ${#pid_list[*]} ]]
do
  set +e
  wait ${pid_list[$TPT_IDX]}
  pid_list_rcode[$TPT_IDX]=$?
  set -e
  ((TPT_IDX+=1))
done

# Check return codes for all pids
TPT_IDX=0
while [[ $TPT_IDX -lt ${#pid_list_rcode[*]} ]]
do
  if [ ${pid_list_rcode[$TPT_IDX]} != 0 ]
  then
    TPT_NUM=$(( $TPT_IDX + 1))
    print "${0##*/}:  FATAL ERROR, see log file $EXTRACT_LOG_FILE.$TPT_NUM" >&2
    exit 4
  fi
  ((TPT_IDX+=1))
done

# Scrape master log for total record count
if [ $TPT_EXTRACT_NWAYS = 1 ]
then
  RECORD_COUNT=$(grep "EVT: Number of rows                 :" $EXTRACT_LOG_FILE.1 | cut -d'|' -f4 | cut -d':' -f3)
else
  RECORD_COUNT=$(grep "CEVT:0" $EXTRACT_LOG_FILE.1 | cut -d'|' -f4 | cut -d'=' -f5)
fi

if [[ "X$RECORD_COUNT" != "X" ]]
then
  print $RECORD_COUNT > $RECORD_COUNT_FILE
else
  print "${0##*/}:  FATAL ERROR, Problem scraping record count from $EXTRACT_LOG_FILE.1" >&2
  exit 4
fi

if [[ $LAST_EXTRACT_TYPE = "V" ]]
then
  print $TO_EXTRACT_VALUE > $DW_SA_DAT/$TABLE_ID.$FILE_ID.last_extract_value.dat
fi

exit 0
