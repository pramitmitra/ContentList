#!/bin/ksh -eu
#------------------------------------------------------------------------------------------------
# Filename:     run_multi_hadoop_extract_file.ksh
#
# Revision History:
#
# Name             Date            Description
# ---------------  --------------  ---------------------------------------------------
# ???              ??/??/????      Initial Creation
# Ryan Wong        10/04/2013      Redhat changes
#
#------------------------------------------------------------------------------------------------

HDP_LIS_FILE=$1
PARENT_LOG_FILE=$2

integer PLIM
PLIM_TMP=${HDP_LIS_FILE%.lis}
PLIM=${PLIM_TMP##*.}  # parallel process count limit

while read RECORD_ID HDP_CONN
do

  # check to see if the $FILE_ID process has already been run (exists in the complete file).  If so, skip it.
  set +e
  grep "^$RECORD_ID $SOURCE_FILE" $MULTI_COMP_FILE >/dev/null
  rcode=$?
  set -e

  if [ $rcode = 1 ]
  then
    while [ $(jobs -p | wc -l) -ge $PLIM ]
    do
      sleep 30
      continue
    done

    LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.$RECORD_ID.single_hadoop_extract_file.$CURR_DATETIME.log

    print "Processing RECORD_ID: $RECORD_ID, SOURCE_FILE: $SOURCE_FILE, HADOOP CONN: $HDP_CONN  `date`"

    COMMAND="$DW_EXE/single_hadoop_extract_file.ksh $ETL_ID $RECORD_ID $HDP_CONN > $LOG_FILE 2>&1"

    set +e
    eval $COMMAND && (print "Logging completion of RECORD_ID: $RECORD_ID, SOURCE_FILE: $SOURCE_FILE, to $MULTI_COMP_FILE"; print "$RECORD_ID $SOURCE_FILE" >> $MULTI_COMP_FILE) >>$LOG_FILE 2>&1 || print "\n${0##*/}: Failure processing RECORD_ID: $RECORD_ID, SOURCE_FILE: $SOURCE_FILE, HADOOP CONN: $HDP_CONN\nsee log file $LOG_FILE" >>$ERROR_FILE &
    set -e

  elif [ $rcode = 0 ]
  then
    print "Extract for RECORD_ID: $RECORD_ID, SOURCE_FILE: $SOURCE_FILE, HADOOP CONN: $HDP_CONN already complete" >> $PARENT_LOG_FILE
  else
    exit $rcode
  fi

done < $HDP_LIS_FILE

wait

exit
