#!/bin/ksh -eu
# Title:        Run Multi Unload
# File Name:    run_multi_unload.ksh
# Description:  Run multiple unload in throttled parallel
# Developer:    ???
# Created on:
# Location:     $DW_EXE
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

DBC_TABLES_LIS_FILE=$1
PARENT_LOG_FILE=$2

integer PLIM
if [ ${USE_GROUP_EXTRACT:-0} -eq 1 ]
then
  PLIM_TMP=${DBC_TABLES_LIS_FILE%.lis.*}
else
  PLIM_TMP=${DBC_TABLES_LIS_FILE%.lis}
fi
PLIM=${PLIM_TMP##*.}  # parallel process count limit
PLIS=$$               # process id list, initialized to current process id
((PLIM+=2))           # adjustment for header row and parent in ps output

while read FILE_ID DBC_FILE TABLE_NAME DATA_FILENAME PARAM_LIST
do

	# check to see if the $FILE_ID process has already been run (exists in the complete file).  If so, skip it.
	set +e
	grep "^$FILE_ID $TABLE_NAME" $MULTI_COMP_FILE >/dev/null
	rcode=$?
	set -e

	if [ $rcode = 1 ]
	then
		while [ $(ps -p$PLIS | wc -l) -ge $PLIM ]
		do
			sleep 30
			continue
		done

		LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.$FILE_ID.single_td_unload.$CURR_DATETIME.log

		print "Processing FILE_ID: $FILE_ID, TABLE: $TABLE_NAME, DBC FILE: $DBC_FILE  `date`"

		COMMAND="$DW_EXE/single_td_unload.ksh $ETL_ID $JOB_ENV $FILE_ID $DATA_FILENAME $PARAM_LIST > $LOG_FILE 2>&1"

		set +e
		eval $COMMAND && (print "Logging completion of FILE_ID: $FILE_ID, TABLE: $TABLE_NAME, to $MULTI_COMP_FILE"; print "$FILE_ID $TABLE_NAME" >> $MULTI_COMP_FILE) >>$LOG_FILE 2>&1 || print "\n${0##*/}: Failure processing FILE_ID: $FILE_ID, TABLE: $TABLE_NAME, DBC FILE: $DBC_FILE\nsee log file $LOG_FILE" >>$ERROR_FILE &
		PLIS=$PLIS,$!
		set -e

	elif [ $rcode = 0 ]
	then
		print "Extract for FILE_ID: $FILE_ID, TABLE: $TABLE_NAME, DBC FILE: $DBC_FILE already complete" >> $PARENT_LOG_FILE
	else
		exit $rcode
	fi

done < $DBC_TABLES_LIS_FILE

wait

exit
