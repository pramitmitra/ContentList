#!/bin/ksh -eu
# Title:        Teradata tpt extract Handler
# File Name:    td_extract_tpt_handler.ksh
# Description:  Handler submits multiple instances of extract job on different hosts based on the instance_cnt.
# Developer:
# Created on:
# Location:     $DW_BIN
# Logic:
#
# Date         Ver#   Modified By(Name)            Change and Reason for Change
#---------    -----  ---------------------------  ------------------------------
# 2012-05-14  1.1    Ryan Wong                    Exposing query band option
# 2013-10-04  1.2    Ryan Wong                    Redhat changes
# 2013-10-08  1.3    Ryan Wong                    Netstat on Redhat
####################################################################################################

if [ $# -lt 2 ]
then
  print "Usage: $0 <ETL_ID> <JOB_ENV> [UOW_ID]"
  exit 4
fi

export ETL_ID=$1
export JOB_ENV=$2
export JOB_TYPE="extract_tpt"
export JOB_TYPE_ID="ex"

. /dw/etl/mstr_cfg/etlenv.setup
. $DW_MASTER_CFG/dw_etl_common_defs.cfg
. $DW_MASTER_LIB/dw_etl_common_functions.lib

COMP_FILE=$DW_SA_TMP/$TABLE_ID.$JOB_TYPE.complete
BATCH_SEQ_NUM_FILE=$DW_SA_DAT/$TABLE_ID.$JOB_TYPE.batch_seq_num.dat
INGESTS_LIST_FILE=$DW_CFG/$ETL_ID.ingests.lis
PARENT_ERROR_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.td_extract_tpt_handler.$CURR_DATETIME.err
TPT_EXTRACT_JOB="$DW_BIN/td_extract_tpt.ksh"

# get UOW_ID
UOW_ID=${3:-""}
PREV_UOW_ID=""
if [ -z $UOW_ID ]
then
  if [ -s $BATCH_SEQ_NUM_FILE ]
  then
    PREV_UOW_ID=$(<$BATCH_SEQ_NUM_FILE)
    ((UOW_ID=PREV_UOW_ID+1))
  else
    print "${0##*/}:  ERROR, UOW_ID or BATCH_SEQ_NUM_FILE has to be provided!" >&2
    exit 4
  fi
fi
export UOW_ID

# generate INGEST node list
assignTagValue N_WAY_HOST N_WAY_HOST $ETL_CFG_FILE W 0
assignTagValue DATA_FILE DATA_FILE $ETL_CFG_FILE W "$TABLE_ID"

TPT_NODE_LIST=""
DATA_FILE_LIST=""

if [ $N_WAY_HOST = 0 ]
then
  if [ -f $INGESTS_LIST_FILE ]
  then
    while read TPT_NODE DATA_FILE junk
    do
      TPT_NODE_LIST="${TPT_NODE_LIST} $TPT_NODE"
      DATA_FILE_T=`eval print ${DATA_FILE:-$TABLE_ID}`
      DATA_FILE_LIST="${DATA_FILE_LIST} ${DATA_FILE_T}"
    done < $INGESTS_LIST_FILE
  else
    TPT_NODE_LIST=`hostname`
    DATA_FILE_LIST=`eval print ${DATA_FILE}`
  fi
elif [[ $N_WAY_HOST = 2 || $N_WAY_HOST = 4 || $N_WAY_HOST = 6 || $N_WAY_HOST = 8 || $N_WAY_HOST = 16 || $N_WAY_HOST = 32 ]]
then
  while read TPT_NODE junk
  do
    TPT_NODE_LIST="${TPT_NODE_LIST} ${TPT_NODE}"
    DATA_FILE_T=`eval print $DATA_FILE`
    DATA_FILE_LIST="${DATA_FILE_LIST} ${DATA_FILE_T}"
  done < $DW_CFG/ingest_${N_WAY_HOST}ways.host.lis
else
  print print "${0##*/}:  ERROR, Can't Determine How Many Hosts Involved" >&2
  exit 4
fi

set -A TPT_HOSTS $TPT_NODE_LIST
TPT_HOST_CNT=${#TPT_HOSTS[*]}
set -A DATA_FILES $DATA_FILE_LIST

# TPT parameters
set -A tpt_normal_args wd mn po dl dc c ns z od n qb
set -A tpt_custom_args WORKING_DATABASE MASTER_NODE PORT HEX_DELIMITER CHAR_DELIMITER CHARSET SESSIONS COMPRESS_FLAG OUTPUT_DIR INSTANCE_CNT QUERY_BAND

assignTagValue QUERY_BAND QUERY_BAND $ETL_CFG_FILE W
assignTagValue WORKING_DATABASE WORKING_DATABASE $ETL_CFG_FILE
assignTagValue MASTER_NODE MASTER_NODE $ETL_CFG_FILE W `hostname`
assignTagValue PORT PORT $ETL_CFG_FILE W $((6000+$RANDOM%2000))
assignTagValue INSTANCE_CNT INSTANCE_CNT $ETL_CFG_FILE W $TPT_HOST_CNT
assignTagValue HEX_DELIMITER HEX_DELIMITER $ETL_CFG_FILE W
assignTagValue CHAR_DELIMITER CHAR_DELIMITER $ETL_CFG_FILE W
assignTagValue CHARSET CHARSET $ETL_CFG_FILE W "ASCII"
assignTagValue SESSIONS SESSIONS $ETL_CFG_FILE W $(($INSTANCE_CNT*2))
assignTagValue COMPRESS_FLAG COMPRESS_FLAG $ETL_CFG_FILE W "0"
assignTagValue IN_DIR IN_DIR $ETL_CFG_FILE

# Output Dir
export OUTPUT_DIR=`eval print $IN_DIR`/$JOB_ENV/$SUBJECT_AREA

# Check INSTANCE_CNT
if [[ $INSTANCE_CNT -lt $TPT_HOST_CNT ]]
then
  print "${0##*/}:  ERROR, The INSTANCE_CNT should equal or large than ingest node number." >&2
  exit 4
fi

# Check SESSIONS
min_sessions=$(( $INSTANCE_CNT * 2 ))
if [[ $SESSIONS -lt $min_sessions ]]
then
  print "${0##*/}:  ERROR, The SESSIONS should at least twice of INSTANCE_CNT" >&2
  exit 4
fi

#check delimiter
if [[ X$HEX_DELIMITER != "X" && X$CHAR_DELIMITER != "X" ]]
then
  print "${0##*/}:  ERROR, Only one of HEX_DELIMITER and CHAR_DELIMITER could be set in CFG file!" >&2
  exit 4
fi

if [[ X$HEX_DELIMITER = "X" && X$CHAR_DELIMITER = "X" ]]
then
  HEX_DELIMITER="7C"
fi


if [ ! -f $COMP_FILE ]
then
  # COMP_FILE does not exist.  1st run for this processing period.
  FIRST_RUN=Y
else
  FIRST_RUN=N
fi

# Source the error message handling logic.  On failure, trap will send the contents of the PARENT_ERROR_FILE to the
# subject area designated email addresses.
. $DW_MASTER_LIB/message_handler

print "
##########################################################################################################
#
# Beginning TPT extract for ETL_ID: $ETL_ID, UOW_ID: $UOW_ID   `date`
#
##########################################################################################################
"

if [ $FIRST_RUN = Y ]
then
  # Need to run the clean up process since this is the first run for the current processing period.
  if [[ $TPT_HOST_CNT -gt 1 ]]
  then
    COMMAND_SCRIPT="$DW_TMP/$JOB_ENV/$SUBJECT_AREA/$TABLE_ID.$JOB_TYPE.cleanup.command.dat"
    > $COMMAND_SCRIPT
    host_idx=0
    while [[ $host_idx -lt $TPT_HOST_CNT ]];
    do
      host_name=${TPT_HOSTS[${host_idx}]}
      LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.loader_cleanup.$host_name.$CURR_DATETIME.log
      print "/usr/bin/ssh -q $host_name \"$DW_EXE/loader_cleanup_sg.ksh $JOB_ENV $JOB_TYPE_ID $ETL_ID \" 2>$LOG_FILE &">>$COMMAND_SCRIPT
      host_idx=$(( $host_idx + 1 ))
    done
    print "wait" >>$COMMAND_SCRIPT
    set +e
    . $COMMAND_SCRIPT
    RCODE=$?
    set -e
    rm -f $COMMAND_SCRIPT

    host_idx=0;
    error_yn=0;
    while [[ $host_idx -lt $TPT_HOST_CNT ]];
    do
      host_name=${TPT_HOSTS[${host_idx}]}
      LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.loader_cleanup.$host_name.$CURR_DATETIME.log
      if [ -s $LOG_FILE ]
      then
        print "${0##*/}:  ERROR, see log file $LOG_FILE" >&2
        error_yn=1;
      fi
      host_idx=$(( $host_idx + 1 ))
    done
    if [[ $error_yn -ne 0 || $RCODE -ne 0 ]]
    then
      exit 4
    fi
  else
    LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.loader_cleanup.$CURR_DATETIME.log
    set +e
    $DW_EXE/loader_cleanup_sg.ksh $JOB_ENV $JOB_TYPE_ID $ETL_ID > $LOG_FILE 2>&1
    rc=$?
    set -e
    if [[ $rc -gt 0 ]]
    then
      print "${0##*/}:  ERROR, see log file $LOG_FILE" >&2
      exit 4
    fi
  fi
  > $COMP_FILE
else
  print "loader_cleanup.ksh process already complete"
fi

EXTRACT_PROCESS_MSG="td_extract_tpt"
RCODE=`grepCompFile "$EXTRACT_PROCESS_MSG" $COMP_FILE`
if [ $RCODE = 1 ]
then
  print "Processing TPT table extract for TABLE_ID: $TABLE_ID `date`"

  arg_count=${#tpt_normal_args[@]}
  tpt_arg=""
  tpt_args_idx=0
  while [[ $tpt_args_idx -lt $arg_count ]]
  do
    if [[ -n $(eval "print \$${tpt_custom_args[$tpt_args_idx]}") ]]
    then
      tpt_arg="$tpt_arg -${tpt_normal_args[$tpt_args_idx]}  \"$(eval "print \$${tpt_custom_args[$tpt_args_idx]}")\""
    fi
    tpt_args_idx=$((tpt_args_idx+1))
  done
  #########################################################################################################
  #
  #       Launching the Instances in different Hosts based on the Instant_cnt and Number of Hosts avaiable.
  #
  #########################################################################################################
  #Put the MASTER NODE into the first item of the node list, so that the INSTANCE NO.1 alwaoys on MASTER NODE
  host_idx=0
  while [[ $host_idx -lt $TPT_HOST_CNT ]]
  do
    if [ ${MASTER_NODE%%.*} = ${TPT_HOSTS[$host_idx]%%.*} ]
    then
      break
    fi
    host_idx=$(($host_idx+1))
  done

  if [ ${host_idx} -gt 0 ]
  then
    HOST_TMP=${TPT_HOSTS[0]}
    TPT_HOSTS[0]=${TPT_HOSTS[${host_idx}]}
    TPT_HOSTS[${host_idx}]=$HOST_TMP
    HOST_TMP=${DATA_FILES[0]}
    DATA_FILES[0]=${DATA_FILES[${host_idx}]}
    DATA_FILES[${host_idx}]=$HOST_TMP
  fi

  #loop through each instance and use mod to determine host to calculate total instances per host
  set -A host_instance_total
  #Total number of instances per host
  set -A host_instance_cnt
  #running number of instances per host

  instance_idx=0

  while [[ $instance_idx -lt $TPT_HOST_CNT ]];
  do
    host_instance_total[$instance_idx]=0
    host_instance_cnt[$instance_idx]=0
    instance_idx=$(($instance_idx+1))
  done

  instance_idx=0
  while [[ $instance_idx -lt $INSTANCE_CNT ]]
  do
    host_idx=$(( $instance_idx % $TPT_HOST_CNT ))
    host_instance_total[$host_idx]=$(( ${host_instance_total[$host_idx]}+1))
    instance_idx=$(($instance_idx+1))
  done

  # Check whether the port number already in use to initiate the instances or fail the extract
  set +e
    netstat  -t|awk '{print $4}'|grep ${MASTER_NODE%%.*}|grep PORT
    rcode=$?
  set -e

  if [ $rcode = 0 ]
  then
    print "FATAL ERROR: Port number $PORT is already in use" >&2
    exit 4
  fi

  #Launch the instances at different hosts
  instance_idx=0
  while [[ $instance_idx -lt $INSTANCE_CNT ]]
  do
    host_idx=$(( $instance_idx % $TPT_HOST_CNT ))
    host_name=${TPT_HOSTS[${host_idx}]}
    data_file_name=${DATA_FILES[${host_idx}]}
    instance_nbr=$(( $instance_idx + 1))

    host_instance_cnt[$host_idx]=$(( ${host_instance_cnt[$host_idx]} + 1 ))
    LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.td_extract_tpt.$instance_idx.$CURR_DATETIME.log
    TPT_LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.tpt_extract.$instance_idx.$CURR_DATETIME.log
    if [[ $TPT_HOST_CNT -gt 1 ]]
    then
      print "Launching instance $instance_nbr on $host_name (${host_instance_cnt[$host_idx]}/${host_instance_total[$host_idx]})..."
      /usr/bin/ssh -q $host_name "ksh $TPT_EXTRACT_JOB $tpt_arg -ei $ETL_ID -je $JOB_ENV -ui $UOW_ID -l $TPT_LOG_FILE -i $instance_nbr -f $data_file_name" > $LOG_FILE 2>&1 &
    else
      print "Launching instance $instance_nbr on $host_name (${host_instance_cnt[$host_idx]}/${host_instance_total[$host_idx]})...."
      eval ksh $TPT_EXTRACT_JOB $tpt_arg -ei $ETL_ID -je $JOB_ENV -ui $UOW_ID -l $TPT_LOG_FILE -i $instance_nbr -f $data_file_name > $LOG_FILE 2>&1 &
    fi
    instance_idx=$(( $instance_idx+1))
  done

  wait
  #########################################################################################################
  #
  #       loop through each instance, wait for it to finish, and capture return code
  #
  #########################################################################################################
  max_rc=0
  instance_idx=0
  error_yn=0
  while [[ $instance_idx -lt $INSTANCE_CNT ]]
  do
    LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.td_extract_tpt.$instance_idx.$CURR_DATETIME.log
    set +e
    grep -c '^ERR' $LOG_FILE >/dev/null 2>&1
    errcnt=$?
    set -e
    if [[ $errcnt -ne 0 ]]
    then
      print "Instance $instance_nbr is complete"
    else
      print "${0##*/}:  ERROR, see log file $LOG_FILE" >&2
      error_yn=1
    fi

    instance_idx=$(( $instance_idx + 1 ))
  done

  if [[ $error_yn -ne 0 ]]
  then
    exit 4
  else
    print "$EXTRACT_PROCESS_MSG" >> $COMP_FILE
  fi
elif [ $RCODE = 0 ]
then
  print "$EXTRACT_PROCESS_MSG process already complete"
else
  exit $RCODE
fi

######################################################################################################
#
#                                Increment BSN (Optional)
#
#  This section updates the batch_seq_number and creates the watch_file.  It is now in a non-repeatable
#  Section to avoid issues of restartability.
#
######################################################################################################
if [ X$PREV_UOW_ID != "X" ]
then
  PROCESS=Increment_BSN
  RCODE=`grepCompFile $PROCESS $COMP_FILE`

  if [ $RCODE = 1 ]
  then

   print "Updating the batch sequence number file  `date`"
   print $UOW_ID > $BATCH_SEQ_NUM_FILE

   print "$PROCESS" >> $COMP_FILE

  elif [ $RCODE = 0 ]
  then
    print "$PROCESS already complete"
  else
    exit $RCODE
  fi
fi

print "Removing the complete file  `date`"
rm -f $COMP_FILE

print "
##########################################################################################################
#
# extract for ETL_ID: $ETL_ID , UOW_ID: $UOW_ID  is successfully Completed    `date`
#
##########################################################################################################"

tcode=0
exit
