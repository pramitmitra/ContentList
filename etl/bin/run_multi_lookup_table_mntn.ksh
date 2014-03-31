#!/usr/bin/ksh -eu

MTN_TB=$1
LOOKUP_TABLE_LIS_FILE=$2
PARENT_LOG_FILE=$3

ERROR_FILE="${PARENT_LOG_FILE%.log}.err"
if [ -f $ERROR_FILE ]
then
  echo "Moving the error file to r4a"
  mv $ERROR_FILE $ERROR_FILE.r4a
fi

# added the following piece to make the previous err file as r4a for the monitor

PREV_ERR_FILE=${PARENT_LOG_FILE%_mntn.*log}_mntn*err

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
PLIM_TMP=${LOOKUP_TABLE_LIS_FILE%.lis}
PLIM=${PLIM_TMP##*.}  # parallel process count limit
#PLIS=$$               # process id list, initialized to current process id
PID=$$
((PLIM+=1))           # adjustment for header row and parent in ps output

CURR_DATETIME=$(date '+%Y%m%d-%H%M%S')

integer TOTAL_LIS_FILES
TOTAL_LIS_FILES=0

while read LOOKUP_TABLE_LIS_DATA
do
  LOOKUP_TABLE_LIS_ARRAY[$TOTAL_LIS_FILES]=$LOOKUP_TABLE_LIS_DATA
  ((TOTAL_LIS_FILES+=1))
done < $LOOKUP_TABLE_LIS_FILE

integer j
j=0
until [ $j -ge $TOTAL_LIS_FILES ]
do
  echo ${LOOKUP_TABLE_LIS_ARRAY[$j]} | read MAIN_TB MAIN_TBL_DESC LKP_TB LKP_TBL_CODE LKP_TBL_DESC PARAM_LIST

  # check to see if the $FILE_ID process has already been run (exists in the complete file).  If so, skip it.

  set +e
  grep "^$MAIN_TB $LKP_TB" $MULTI_COMP_FILE >/dev/null
  rcode=$?
  set -e

  if [ $rcode = 1 ]
  then
    #while [ $(ps -eo'pid ppid' | grep " $PID$" | wc -l) -ge $PLIM ]
    while [ $(jobs -l | wc -l) -ge $PLIM ]
    do
      sleep 30
      continue
    done

    LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.$MTN_TB.single_lookup_table_mntn.$CURR_DATETIME.log
    echo "Processing MAIN_TB: $MAIN_TB, LKP_TB: $LKP_TB  `date`"

    COMMAND="$DW_EXE/single_lookup_table_mntn.ksh $ETL_ID $JOB_ENV $MAIN_TB $MAIN_TBL_DESC $LKP_TB $LKP_TBL_CODE $LKP_TBL_DESC $PARAM_LIST > $LOG_FILE 2>&1"

    set +e
    eval $COMMAND && (echo "Logging completion of MAIN_TB: $MAIN_TB, LKP_TB: $LKP_TB, to $MULTI_COMP_FILE"; echo "$MAIN_TB $LKP_TB" >> $MULTI_COMP_FILE) >>$LOG_FILE 2>&1 || echo "\n${0##*/}: Failure processing MAIN_TB: $MAIN_TB, LKP_TB: $LKP_TB\nsee log file $LOG_FILE" >>$ERROR_FILE &
    set -e

  elif [ $rcode = 0 ]
  then
    echo "Lookup table maintain for MTN_TB: $MTN_TB, LKP_TB: $LKP_TB already complete" >> $PARENT_LOG_FILE
  else
    exit $rcode
  fi

  ((j+=1))
done

wait

exit 0
