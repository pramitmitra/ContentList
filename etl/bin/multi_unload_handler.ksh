#!/bin/ksh -eu
#------------------------------------------------------------------------------------------------
# Filename:     multi_unload_handler.ksh
#
# Revision History:
#
# Name             Date            Description
# ---------------  --------------  ---------------------------------------------------
# ???              ??/??/????      Initial Creation
# Ryan Wong        10/04/2013      Redhat changes
#
#------------------------------------------------------------------------------------------------

if [[ $# -lt 2 || $# -gt 3 ]]
then
        print "Usage:  $0 <ETL_ID> <JOB_ENV> <INPUT_DML> # NOTE:  INPUT_DML is optional"
        exit 4
fi

export ETL_ID=$1
export JOB_ENV=$2
export INPUT_DML=${3:-""}
export JOB_TYPE=unload
export JOB_TYPE_ID=unload

export SUBJECT_AREA=${ETL_ID%%.*}
export TABLE_ID=${ETL_ID##*.}

. /dw/etl/mstr_cfg/etlenv.setup

export DW_SA_DAT=$DW_DAT/$JOB_ENV/$SUBJECT_AREA
export DW_SA_IN=$DW_IN/$JOB_ENV/$SUBJECT_AREA
export DW_SA_IN04=$DW_IN04/$JOB_ENV/$SUBJECT_AREA
export DW_SA_IN08=$DW_IN08/$JOB_ENV/$SUBJECT_AREA
export DW_SA_IN16=$DW_IN16/$JOB_ENV/$SUBJECT_AREA
export DW_SA_LOG=$DW_LOG/$JOB_ENV/$SUBJECT_AREA
export DW_SA_TMP=$DW_TMP/$JOB_ENV/$SUBJECT_AREA


export DW_SA_OUT=$DW_IN/$JOB_ENV/$SUBJECT_AREA

COMP_FILE=$DW_SA_TMP/$TABLE_ID.$JOB_TYPE.complete
BATCH_SEQ_NUM_FILE=$DW_SA_DAT/$TABLE_ID.$JOB_TYPE.batch_seq_num.dat
TABLE_LIS_FILE=$DW_CFG/$ETL_ID.sources.lis
export CURR_DATETIME=$(date '+%Y%m%d-%H%M%S')
PARENT_ERROR_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.multi_unload_handler.$CURR_DATETIME.err

if [ ! -f $COMP_FILE ]
then
	# COMP_FILE does not exist.  1st run for this processing period.
	FIRST_RUN=Y
else
	FIRST_RUN=N
fi

# Source the error message handling logic.  On failure, trap will send the contents of the PARENT_ERROR_FILE to the
# subject area designated email addresses.
. $DW_LIB/message_handler

# get BATCH_SEQ_NUM
PREV_BATCH_SEQ_NUM=$(<$BATCH_SEQ_NUM_FILE)
((BATCH_SEQ_NUM=PREV_BATCH_SEQ_NUM+1))

print "
##########################################################################################################
#
# Beginning unload for ETL_ID: $ETL_ID, BATCH_SEQ_NUM: $BATCH_SEQ_NUM   `date`
#
##########################################################################################################
"

if [ $FIRST_RUN = Y ]
then
	# Need to run the clean up process since this is the first run for the current processing period.

	print "Running loader_cleanup.ksh for JOB_ENV: $JOB_ENV, JOB_TYPE_ID: $JOB_TYPE_ID  `date`"
	LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.loader_cleanup.$CURR_DATETIME.log

	set +e
	$DW_EXE/loader_cleanup.ksh $JOB_ENV $JOB_TYPE_ID > $LOG_FILE 2>&1
	rcode=$?
	set -e

	if [ $rcode != 0 ]
	then
		print "${0##*/}:  ERROR, see log file $LOG_FILE" >&2
		exit 4
	fi

	> $COMP_FILE
else
	print "loader_cleanup.ksh process already complete"
fi

######################################################################################################
#
#       Setting up unload process specific variables for database unload
#
######################################################################################################

set +e
grep "^OUT_DIR\>" $DW_CFG/$ETL_ID.cfg | read PARAM OUT_DIR COMMENT
ecode=$?
set -e

if [ $rcode != 0 ]
then
        print "${0##*/}:  ERROR, failure determining value for OUT_DIR parameter from $DW_CFG/$ETL_ID.cfg" >&2
        exit 4
fi

export DW_SA_OUT=`eval print $OUT_DIR`/$JOB_ENV/$SUBJECT_AREA


# check to see if the unload processing has completed yet
set +e
grep -s "multi_unload" $COMP_FILE >/dev/null
RCODE=$?
set -e

if [ $RCODE = 1 ]
then
	############################################################################################################
	#
	#                                   MULTIPLE TABLE PROCESSING
	#
	#  A list of files is read from $TABLE_LIS_FILE.  It has one row for each table that is being unloaded
	#  from the source.  This list file contains a FILE_ID, DBC_FILE, PARALLEL_NUM, TABLE_NAME, DATA_FILENAME
	#  and an optional parameter PARAM for passing into the Ab Initio script. 
	#
	#  The tables will be grouped by DBC_FILE where each DBC_FILE represents a thread for processing.
	#  These threads will be run in parallel.  Within each thread, the PARALLEL_NUM parameter is used to
	#  determine how many table unload can be run at one time.  The run_multi_single_table_unload.ksh
	#  script is run once per thread and manages the parallel processin withing a thread.
	#
	############################################################################################################

	# run wc -l on $ETL_ID.sources.lis file to know how many tables to unload (1 or > 1).
	wc -l $TABLE_LIS_FILE | read TABLE_COUNT FN

	if [[ $TABLE_COUNT -eq 1  ]]
	then
		print "Processing single unload for TABLE_ID: $TABLE_ID  `date`"

		LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.single_td_unload.$CURR_DATETIME.log

		read FILE_ID DBC_FILE PARALLEL_NUM TABLE_NAME DATA_FILENAME PARAM_LIST < $TABLE_LIS_FILE

		set +e
		eval $DW_EXE/single_td_unload.ksh $ETL_ID $JOB_ENV $FILE_ID $DATA_FILENAME $PARAM_LIST > $LOG_FILE 2>&1
		rcode=$?
		set -e

		if [ $rcode != 0 ]
		then
			print "${0##*/}:  ERROR, see log file $LOG_FILE" >&2
			exit 4
		fi

	elif [[ $TABLE_COUNT -gt 1 ]]
	then
		print "Processing multiple table unload for TABLE_ID: $TABLE_ID  `date`"

		export MULTI_COMP_FILE=$DW_SA_TMP/$TABLE_ID.multi_unload.complete
		export PARENT_LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.run_td_unload.$CURR_DATETIME.log
		export ERROR_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.single_td_unload.$CURR_DATETIME.err  # job error file

		# If the MULTI_COMP_FILE does not exist, this is the first run, otherwise it is a restart.
		if [ ! -f $MULTI_COMP_FILE ]
		then
			> $MULTI_COMP_FILE
		fi

		# remove previous dbc list files to ensure looking for the correct set of data files for this run.
		rm -f $DW_SA_TMP/$TABLE_ID.*.unload.*.lis

		while read FILE_ID DBC_FILE PARALLEL_NUM TABLE_NAME DATA_FILENAME PARAM_LIST
		do
			eval DBC_FILE=$DBC_FILE

			if [ ! -f $DW_SA_TMP/$TABLE_ID.$DBC_FILE.*.lis ]
			then
				eval print $FILE_ID $DBC_FILE $TABLE_NAME $DATA_FILENAME $PARAM_LIST > $DW_SA_TMP/$TABLE_ID.$DBC_FILE.unload.$PARALLEL_NUM.lis
			else
				eval print $FILE_ID $DBC_FILE $TABLE_NAME $DATA_FILENAME $PARAM_LIST >> $DW_SA_TMP/$TABLE_ID.$DBC_FILE.unload.$PARALLEL_NUM.lis
			fi
		done < $TABLE_LIS_FILE

		for FILE in $(ls $DW_SA_TMP/$TABLE_ID.*.unload.*.lis)
		do
			DBC_FILE=${FILE#$DW_SA_TMP/$TABLE_ID.}
			DBC_FILE=${DBC_FILE%%.*}
			LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.$DBC_FILE.run_multi_unload.$CURR_DATETIME.log

			print "Running run_multi_unload.ksh $FILE  `date`"
			COMMAND="$DW_EXE/run_multi_unload.ksh $FILE $LOG_FILE > $LOG_FILE 2>&1"

			set +e
			eval $COMMAND || print "${0##*/}: ERROR, failure processing for $FILE, see log file $LOG_FILE" >>$ERROR_FILE &
			set -e
		done

		wait

		if [ -f $ERROR_FILE ]
		then
			cat $ERROR_FILE >&2
			exit 4
		fi

		rm $MULTI_COMP_FILE

	else
		print "${0##*/}:  ERROR, no rows exist in file $TABLE_LIS_FILE" >&2
		exit 4
	fi

	print "multi_unload" >> $COMP_FILE

elif [ $RCODE = 0 ]
then
	print "multi_unload process already complete"
else
	exit $RCODE
fi

# check to see if the create DW_IN record count file process has completed yet
set +e
grep -s "create DW_SA_OUT record count file" $COMP_FILE >/dev/null
RCODE=$?
set -e

if [ $RCODE = 1 ]
then
	print "Creating the record count file  `date`"
	
	# sum contents of individual record count files into a master record count file for the load graph
	integer RECORD_COUNT=0
	integer RECORD_CNT=0

	RC_LIS_FILE=$TABLE_LIS_FILE

	while read FILE_ID ZZZ
	do
		((RECORD_COUNT+=$(<$DW_SA_TMP/$TABLE_ID.$JOB_TYPE_ID.$FILE_ID.record_count.dat)))
	done < $RC_LIS_FILE

	# create the record count file in the DW_SA_OUT directory including BATCH_SEQ_NUM so the load process
	# can manage it.  Needed if the load processing falls behind the unload processing.
	print $RECORD_COUNT > $DW_SA_OUT/$TABLE_ID.record_count.dat.$BATCH_SEQ_NUM

	print "create DW_SA_OUT record count file" >> $COMP_FILE
elif [ $RCODE = 0 ]
then
	print "create DW_SA_OUT record count file process already complete"
else
	exit $RCODE
fi

print "Updating the batch sequence number file  `date`"
print $BATCH_SEQ_NUM > $BATCH_SEQ_NUM_FILE

print "Removing the complete file  `date`"
rm -f $COMP_FILE

print "
##########################################################################################################
#
# Extract for ETL_ID: $ETL_ID, BATCH_SEQ_NUM: $BATCH_SEQ_NUM complete   `date`
#
##########################################################################################################"

tcode=0
exit
