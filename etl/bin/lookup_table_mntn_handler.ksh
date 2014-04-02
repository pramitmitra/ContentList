#!/usr/bin/ksh -eu
#############################################################################
# Title:        Lookup table maintain Handler
# File Name:    lookup_table_mntn_handler.ksh
# Description:  Handle submiting one or several lookup table maintian job.
# Developer:    Orlando Jin
# Created on:   Dec 31, 2008
# Location:     $DW_EXE/
# Logic:
#
# Called by:    UC4/Unix
#
# Date         Ver#   Modified By(Name)            Change and Reason for Change
# ---------    -----  ---------------------------  ------------------------------
#
#############################################################################
if [ $# != 2 ]
then
  echo "Usage: $0 <ETL_ID> <JOB_ENV>"
  exit 4
fi

export ETL_ID=$1
export JOB_ENV=$2             # dual-active database environment (primary or secondary)
export JOB_TYPE=lookup
export JOB_TYPE_ID=lkp
export SUBJECT_AREA=${ETL_ID%%.*}
export TABLE_ID=${ETL_ID##*.}

#if [[ X$JOB_ENV == 'Xprimary' ]] || [[ X$JOB_ENV == 'Xsecondary' ]]; then
#  :
#else
#  echo "${0##*/}: ERROR, database environment must be 'primary' or 'secondary'." >&2
#  exit 4
#fi

. /dw/etl/mstr_cfg/etlenv.setup

export DW_SA_DAT=$DW_DAT/$JOB_ENV/$SUBJECT_AREA
export DW_SA_LOG=$DW_LOG/$JOB_ENV/$SUBJECT_AREA
export DW_SA_TMP=$DW_TMP/$JOB_ENV/$SUBJECT_AREA

export COMP_FILE=$DW_SA_TMP/$TABLE_ID.$JOB_TYPE.complete
LOOKUP_LIS_FILE=$DW_CFG/$ETL_ID.lookup.lis
export CURR_DATETIME=$(date '+%Y%m%d-%H%M%S')
PARENT_ERROR_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.lookup_table_mntn_handler.$CURR_DATETIME.err

if [ ! -f $COMP_FILE ]
then
  # COMP_FILE does not exist.  1st run for this processing period.
  FIRST_RUN=Y
else
  FIRST_RUN=N
fi

export FIRST_RUN

# Source the error message handling logic.  On failure, trap will send the contents of the PARENT_ERROR_FILE to the
# subject area designated email addresses.
. $DW_LIB/message_handler

#=============================================================================
# Parse parameters in CFG file
# End paramter parsing
#=============================================================================

echo "
####################################################################################################################
#
# Beginning lookup table maintain for ETL_ID: $ETL_ID   `date`
#
####################################################################################################################
"

if [ $FIRST_RUN = Y ]
then   
  # Need to run the clean up process since this is the first run for the current processing period.
  echo "Running loader_cleanup.ksh for JOB_ENV: $JOB_ENV, JOB_TYPE_ID: $JOB_TYPE_ID  `date`"
  LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.loader_cleanup.$CURR_DATETIME.log

  set +e
  $DW_EXE/loader_cleanup.ksh $JOB_ENV $JOB_TYPE_ID > $LOG_FILE 2>&1
  rcode=$?
  set -e

  if [ $rcode != 0 ]
  then
    echo "${0##*/}:  ERROR, see log file $LOG_FILE" >&2
    exit 4
  fi

  > $COMP_FILE
else
  echo "loader_cleanup.ksh process already complete"
fi

export LOOKUP_PROCESS_MSG=lookup_table_mntn

# check to see if the extract processing has completed yet
set +e
grep -s "^$LOOKUP_PROCESS_MSG\>" $COMP_FILE >/dev/null
RCODE=$?
set -e

if [ $RCODE = 1 ]
then
  ############################################################################################################
  #
  #                                   LOOKUP TABLE(S) MAINTAIN PROCESSING
  #
  #  A list of files is read from $LOOKUP_LIS_FILE.  It has one row for each table that is being maintained
  #  This list file contains a PARALLEL_NUM MAIN_TB MAIN_TBL_DESC LKP_TB LKP_TBL_CODE LKP_TBL_DESC and an optional parameter
  #  PARAM_LIST(MAIL_DL,MAIN_DB,LKP_DB,LKP_TBL_ID,SURRGT_ID_YN). 
  #
  ############################################################################################################

  wc -l $LOOKUP_LIS_FILE | read TABLE_COUNT FN

  LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.single_$LOOKUP_PROCESS_MSG.$CURR_DATETIME.log

  if [ $TABLE_COUNT -eq 1 ]
  then

    echo "Processing single lookup table maintain for TABLE_ID: $TABLE_ID  `date`"

    read PARALLEL_NUM MAIN_TB MAIN_TBL_DESC LKP_TB LKP_TBL_CODE LKP_TBL_DESC PARAM_LIST < $LOOKUP_LIS_FILE

    set +e
    eval $DW_EXE/single_lookup_table_mntn.ksh $ETL_ID $JOB_ENV $MAIN_TB $MAIN_TBL_DESC $LKP_TB $LKP_TBL_CODE $LKP_TBL_DESC $PARAM_LIST > $LOG_FILE 2>&1
    rcode=$?
    set -e

    if [ $rcode != 0 ]
    then
      echo "${0##*/}:  ERROR, see log file $LOG_FILE" >&2
      exit 4
    fi

  elif [ $TABLE_COUNT -gt 1 ]
  then
    echo "Processing multiple lookup table maintain for TABLE_ID: $TABLE_ID  `date`"

    export MULTI_COMP_FILE=$DW_SA_TMP/$TABLE_ID.multi_lookup_table_mntn.complete
    #export PARENT_LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.run_multi_lookup_table_mntn.$CURR_DATETIME.log
    export ERROR_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.multi_$LOOKUP_PROCESS_MSG.$CURR_DATETIME.err

    # If the MULTI_COMP_FILE does not exist, this is the first run, otherwise it is a restart.
    if [ ! -f $MULTI_COMP_FILE ]
    then
      > $MULTI_COMP_FILE
    fi

    # remove previous $EXTRACT_CONN_TYPE list files to ensure looking for the correct set of data files for this run.
    rm -f $DW_SA_TMP/$TABLE_ID.$JOB_TYPE_ID.*.lis

    while read PARALLEL_NUM MAIN_TB MAIN_TBL_DESC LKP_TB LKP_TBL_CODE LKP_TBL_DESC PARAM_LIST
    do

      if [ ! -f $DW_SA_TMP/$TABLE_ID.$JOB_TYPE_ID.$MAIN_TB.*.lis ]
      then
        eval echo $MAIN_TB $MAIN_TBL_DESC $LKP_TB $LKP_TBL_CODE $LKP_TBL_DESC $PARAM_LIST > $DW_SA_TMP/$TABLE_ID.$JOB_TYPE_ID.$MAIN_TB.$PARALLEL_NUM.lis
      else
        ls $DW_SA_TMP/$TABLE_ID.$JOB_TYPE_ID.$MAIN_TB.*.lis|read MAINTAIN_TABLE_NAME
        eval echo $MAIN_TB $MAIN_TBL_DESC $LKP_TB $LKP_TBL_CODE $LKP_TBL_DESC $PARAM_LIST >> $MAINTAIN_TABLE_NAME
      fi
    done < $LOOKUP_LIS_FILE

    integer mpcnt=0

    for FILE in $(ls $DW_SA_TMP/$TABLE_ID.$JOB_TYPE_ID.*.lis)
    do
      MTN_TB=${FILE#$DW_SA_TMP/$TABLE_ID.$JOB_TYPE_ID.}
      MTN_TB=${MTN_TB%%.*}

      LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.$MTN_TB.run_multi_lookup_table_mntn.$CURR_DATETIME.log
      echo "Running run_multi_lookup_table_mntn.ksh for $MTN_TB $FILE  `date`"
      COMMAND="$DW_EXE/run_multi_lookup_table_mntn.ksh $MTN_TB $FILE $LOG_FILE > $LOG_FILE 2>&1"

      set +e
      eval $COMMAND &
      MPLIS_PID[mpcnt]=$!
      MPLIS_DBC_FILE[mpcnt]=$MTN_TB
      MPLIS_PPID[mpcnt]=$$
      set -e

      ((mpcnt+=1))
    done

    wait

    SUB_ERROR_FILE_LIS="$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.*.run_multi_lookup_table_mntn.$CURR_DATETIME.err"

    if [ -f $SUB_ERROR_FILE_LIS ]
    then
      if [ -f $ERROR_FILE ]
      then
        cat $SUB_ERROR_FILE_LIS >> $ERROR_FILE
      else
        cat $SUB_ERROR_FILE_LIS > $ERROR_FILE
      fi
    fi

    if [ -f $ERROR_FILE ]
    then
      cat $ERROR_FILE >&2
      exit 4
    fi

    rm $MULTI_COMP_FILE
  else
    echo "${0##*/}:  ERROR, no rows exist in file $LOOKUP_LIS_FILE" >&2
    exit 4
  fi

  echo "$LOOKUP_PROCESS_MSG" >> $COMP_FILE

elif [ $RCODE = 0 ]
then
   echo "$LOOKUP_PROCESS_MSG process already complete"
else
   exit $RCODE
fi

echo "Removing the complete file  `date`"
rm -f $COMP_FILE

echo "
##########################################################################################################
#
# Lookup table maintain for ETL_ID: $ETL_ID   complete   `date`
#
##########################################################################################################"

tcode=0
exit
