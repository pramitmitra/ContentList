#!/bin/ksh -eu
# Title:        Single TPT Load.ksh
# File Name:    single_tpt_load.ksh
# Description:  Handle submiting a tpt load job
# Developer:    Ryan Wong
# Created on:
# Location:     $DW_EXE
# Logic:
#
#
# Called by:    UC4/Unix
#
# Date         Ver#   Modified By(Name)            Change and Reason for Change
#---------    -----  ---------------------------  ------------------------------
# 2012-11-01   1.0    Ryan Wong                     Initial
# 2012-11-26   1.1    Ryan Wong                     Updated to append QUERY_BAND and QUERY_BAND_STRING
# 2013-02-25   1.2    George Xiong                  Add  grep -v record_count to exclude recored count file in TD load
# 2013-08-21   1.3    George Xiong                  Netstat on Redhat 
# 2013-10-08   1.4    Ryan Wong                     Redhat changes
# 2013-10-29   1.5    Ryan Wong                     Add TPT_LOAD_ERROR_LIMIT option
# 2014-08-07   1.6    Ryan Wong                     Fix data file search by calling dw_infra.single_tpt_load_find_files.ksh
# 2014-09-03   1.7    Ryan Wong                     Fixing dw_infra.single_tpt_load_find_files.ksh to handle NON-UOW files
#############################################################################################################

ETL_ID=$1
JOB_ENV=$2
shift 2

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
    * )
      print "FATAL ERROR:  Unexpected command line argument"
      print "Usage: single_tpt_load.ksh <ETL_ID> <JOB_ENV> -UOW_FROM <UOW_FROM> -UOW_TO <UOW_TO>"
      exit 4
  esac
done

. /dw/etl/mstr_cfg/etlenv.setup
. $DW_MASTER_LIB/dw_etl_common_functions.lib

LOAD_LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.single_tpt_load${UOW_APPEND}.$CURR_DATETIME.log
LOAD_TABLE_LOG_FILE=$DW_SA_LOG/$TABLE_ID.ld.utility_load${UOW_APPEND}.$CURR_DATETIME.log
export UTILITY_LOAD_TPT_CHECK_LOG_FILE=$DW_SA_LOG/$TABLE_ID.ld.utility_load_tpt_check${UOW_APPEND}.$CURR_DATETIME.log

# Handle tpt configuration parameters
# 
set -A TPT_CFG_VALUES TPT_LOAD_WORKING_DB TPT_LOAD_PORT TPT_LOAD_NWAYS TPT_LOAD_HEX_DELIMITER TPT_LOAD_CHAR_DELIMITER TPT_LOAD_CHARSET TPT_LOAD_TENACITY_HOURS TPT_LOAD_TENACITY_SLEEP TPT_LOAD_SESSIONS TPT_LOAD_ERROR_LIMIT TPT_LOAD_DATE_FORMAT TPT_LOAD_COMPRESS_FLAG TPT_LOAD_COMPRESS_LEVEL TPT_LOAD_TEXTOUT TPT_LOAD_VERBOSE TPT_LOAD_PRINT_INTERVAL

set -A TPT_MAP_VALUES WORKING_DB PORT NWAYS HEX_DELIMITER CHAR_DELIMITER CHARSET TENACITY_HOURS TENACITY_SLEEP SESSIONS ERROR_LIMIT DATE_FORMAT COMPRESS_FLAG COMPRESS_LEVEL TEXTOUT VERBOSE PRINT_INTERVAL

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

# If TPT_LOAD_NWAYS is not assigned, then error.
if [[ "X${TPT_LOAD_NWAYS:-}" = "X" ]]
then
  print "FATAL ERROR: Could not assign TPT_LOAD_NWAYS from $ETL_CFG_FILE" >&2
  exit 4
fi

# If TPT_LOAD_PORT is not assigned, then use random number
if [[ "X${TPT_LOAD_PORT:-}" = "X" ]]
then
  # if not set use random
  TPT_LOAD_PORT=$((6000+$RANDOM%2000))
  TPT_ARG="$TPT_ARG -PORT $TPT_LOAD_PORT"
fi

# Set MASTER_NODE
export MASTER_NODE=$servername
TPT_ARG="$TPT_ARG -MASTER_NODE $MASTER_NODE"

# Check whether the port number already in use to initiate the instances or fail the extract
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
eval set -A myqb $(grep TPT_LOAD_QUERY_BAND $ETL_CFG_FILE)
export QUERY_BAND=${myqb[1]:-}
export QUERY_BAND_STRING="UTILITYNAME=TPTLOAD;${QUERY_BAND_STRING%% *}$QUERY_BAND"
if [[ "X$QUERY_BAND" != "X" ]]
then
  TPT_ARG="$TPT_ARG -QUERY_BAND \"$QUERY_BAND_STRING\""
fi

# Pull db_name from environment
export TD_DB_NAME=$(JOB_ENV_UPPER=$(print $JOB_ENV | tr "[:lower:]" "[:upper:]"); eval echo \$DW_${JOB_ENV_UPPER}_DB)
if [[ "X${TD_DB_NAME:-}" = "X" ]]
then
  print "FATAL ERROR: Could not compute TD_DB_NAME from DW_${JOB_ENV_UPPER}_DB" >&2
  exit 4
fi

# If character set not assigned, pull from dbc file.
DBC_FILE=$(JOB_ENV_LOWER=$(print $JOB_ENV | tr "[:lower:]" "[:upper:]"); echo teradata_${JOB_ENV_LOWER}.dbc)
if [[ "X$TPT_LOAD_CHARSET" = "X" ]]
then
  assignTagValue TPT_LOAD_CHARSET "teradata_character_set:" $DW_DBC/$DBC_FILE W ""
  if [[ "X$TPT_LOAD_CHARSET" != "X" ]]
  then
    TPT_ARG="$TPT_ARG -CHARSET $TPT_LOAD_CHARSET"
  fi
fi

# Calculate file extension
if [[ $TPT_LOAD_COMPRESS_FLAG = 1 ]]
then
  FILE_EXTN=".gz"
elif [[ $TPT_LOAD_COMPRESS_FLAG = 2 ]]
then
  FILE_EXTN=".bz2"
else
  FILE_EXTN=""
fi

DATA_FILE_PATTERN="$TABLE_ID.*.dat*"
UOW_PARAM_LIST_FIND=""
if [[ "X$UOW_TO" != "X" ]]
then
  DATA_FILE_PATTERN="$DATA_FILE_PATTERN${FILE_EXTN}"
  UOW_PARAM_LIST_FIND="-UOW_FROM $UOW_FROM -UOW_TO $UOW_TO"
else
  DATA_FILE_PATTERN="$DATA_FILE_PATTERN.$BATCH_SEQ_NUM${FILE_EXTN}"
  UOW_PARAM_LIST_FIND=""
fi

print "IN_DIR is $IN_DIR"
print "DATA_FILE_PATTERN is $DATA_FILE_PATTERN"

assignTagValue STAGE_DB STAGE_DB $ETL_CFG_FILE
assignTagValue STAGE_TABLE STAGE_TABLE $ETL_CFG_FILE
TPT_ARG="$TPT_ARG -STAGE_DB $STAGE_DB -STAGE_TABLE $STAGE_TABLE"

#--------------------------------------------------------------------------------
# Run tpt utility check
#--------------------------------------------------------------------------------
set +e
$DW_MASTER_BIN/dw_infra.utility_load_tpt_check.ksh
RCODE=$?
set -e

if [ $RCODE != 0 ]
then
  print "${0##*/}:  FATAL ERROR, see log file $UTILITY_LOAD_TPT_CHECK_LOG_FILE" >&2
  exit 4
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

if [ $TPT_LOAD_NWAYS -lt $TPT_HOST_CNT ]
then
  print "FATAL ERROR: TPT_LOAD_NWAYS $TPT_LOAD_NWAYS is less than TPT_HOST_CNT $TPT_HOST_CNT" >&2
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
  print "FATAL ERROR: Master Node not found in host file $DW_CFG/$ETL_ID.host.lis" >&2
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
while [[ $instance_idx -lt $TPT_LOAD_NWAYS ]]
do
  host_idx=$(( $instance_idx % $TPT_HOST_CNT ))
  host_instance_total[$host_idx]=$(( ${host_instance_total[$host_idx]}+1))
  ((instance_idx+=1))
done

# Loop through all remote hosts and create remote log directories.
# Also create master load list files
MASTER_LISTFILE=$DW_SA_TMP/$TABLE_ID.load.master.lis
instance_idx=0
while [[ $instance_idx -lt $TPT_HOST_CNT ]];
do
  host_name=${TPT_HOSTS[${instance_idx}]}
  set +e
  ssh -nq $host_name "mkdir -p $DW_SA_LOG" > /dev/null
  set -e
  ssh -nq $host_name "$DW_MASTER_BIN/dw_infra.single_tpt_load_find_files.ksh -ETL_ID $ETL_ID -DATA_FILE_PATTERN $DATA_FILE_PATTERN -IN_DIR $IN_DIR $UOW_PARAM_LIST_FIND > $MASTER_LISTFILE.$instance_idx"
  ((instance_idx+=1))
done

set -A pid_list
instance_idx=0
while [[ $instance_idx -lt $TPT_LOAD_NWAYS ]]
do
  host_idx=$(( $instance_idx % $TPT_HOST_CNT ))
  host_name=${TPT_HOSTS[$host_idx]}
  instance_nbr=$(( $instance_idx + 1 ))

  host_instance_cnt[$host_idx]=$(( ${host_instance_cnt[$host_idx]} + 1 ))
  print "********************************************************************************"
  print "host_idx is $host_idx"
  print "host_name is $host_name"
  print "instance_nbr is $instance_nbr"
  print "Launching (${host_instance_cnt[$host_idx]}/${host_instance_total[$host_idx]})"

  if [ $MULTI_HOST = 1 ]
  then
    print "Local launch $DW_MASTER_EXE/dw_infra.single_tpt_run.ksh -OPERATOR_TYPE 1 -ETL_ID $ETL_ID -JOB_ENV $JOB_ENV -HOST_INSTANCE_NUM ${host_instance_cnt[$host_idx]} -HOST_INSTANCE_TOTAL ${host_instance_total[$host_idx]} -INSTANCE_NUM $instance_nbr -LISTFILE $MASTER_LISTFILE.$host_idx -LOGFILE $LOAD_TABLE_LOG_FILE.$instance_nbr -SERVER $TD_DB_NAME -IN_DIR $IN_DIR $TPT_ARG"

    set +e
    eval $DW_MASTER_EXE/dw_infra.single_tpt_run.ksh -OPERATOR_TYPE 1 -ETL_ID $ETL_ID -JOB_ENV $JOB_ENV -HOST_INSTANCE_NUM ${host_instance_cnt[$host_idx]} -HOST_INSTANCE_TOTAL ${host_instance_total[$host_idx]} -INSTANCE_NUM $instance_nbr -LISTFILE $MASTER_LISTFILE.$host_idx -LOGFILE $LOAD_TABLE_LOG_FILE.$instance_nbr -SERVER $TD_DB_NAME -IN_DIR $IN_DIR $TPT_ARG > $LOAD_LOG_FILE.$instance_nbr 2>&1 &
    pid_list[$instance_idx]=$!
    set -e

  else
    print "Remote launch ssh -nq $host_name $DW_MASTER_EXE/dw_infra.single_tpt_run.ksh -OPERATOR_TYPE 1 -ETL_ID $ETL_ID -JOB_ENV $JOB_ENV -HOST_INSTANCE_NUM ${host_instance_cnt[$host_idx]} -HOST_INSTANCE_TOTAL ${host_instance_total[$host_idx]} -INSTANCE_NUM $instance_nbr -LISTFILE $MASTER_LISTFILE.$host_idx -LOGFILE $LOAD_TABLE_LOG_FILE.$instance_nbr -SERVER $TD_DB_NAME -IN_DIR $IN_DIR $TPT_ARG"

    set +e
    ssh -nq $host_name "$DW_MASTER_EXE/dw_infra.single_tpt_run.ksh -OPERATOR_TYPE 1 -ETL_ID $ETL_ID -JOB_ENV $JOB_ENV -HOST_INSTANCE_NUM ${host_instance_cnt[$host_idx]} -HOST_INSTANCE_TOTAL ${host_instance_total[$host_idx]} -INSTANCE_NUM $instance_nbr -LISTFILE $MASTER_LISTFILE.$host_idx -LOGFILE $LOAD_TABLE_LOG_FILE.$instance_nbr -SERVER $TD_DB_NAME -IN_DIR $IN_DIR $TPT_ARG" > $LOAD_LOG_FILE.$instance_nbr 2>&1 &
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
    print "${0##*/}:  FATAL ERROR, see log file $LOAD_LOG_FILE.$TPT_NUM" >&2
    exit 4
  fi
  ((TPT_IDX+=1))
done

exit 0
