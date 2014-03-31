#!/bin/ksh -eu
#------------------------------------------------------------------------------------------------
# Filename:     dw_infra.multi_scp_push_run.ksh
#
# Revision History:
#
# Name             Date            Description
# ---------------  --------------  ---------------------------------------------------
# ???              ??/??/????      Initial Creation
# Ryan Wong        10/04/2013      Redhat changes
#
#------------------------------------------------------------------------------------------------

. $DW_MASTER_LIB/dw_etl_common_functions.lib

COMP_FILE=$DW_SA_TMP/$TABLE_ID.$JOB_TYPE.complete
BATCH_SEQ_NUM_FILE=$DW_SA_DAT/$TABLE_ID.$JOB_TYPE.batch_seq_num.dat
UNIT_OF_WORK_FILE=$DW_SA_DAT/$TABLE_ID.$JOB_TYPE.uow.dat
TABLE_LIS_FILE=$DW_CFG/$ETL_ID.target.lis


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
export BATCH_SEQ_NUM

# get LAST_EXTRACT_VALUE from last_scp_value.dat
if [ -f $DW_SA_DAT/$TABLE_ID.*.last_scp_value.dat ]; then
export FROM_EXTRACT_VALUE=$(<`ls $DW_SA_DAT/$TABLE_ID.*.last_scp_value.dat | cut -d' ' -f1 | head -1`)
fi


print "
##########################################################################################################
#
# Beginning extract for ETL_ID: $ETL_ID, BATCH_SEQ_NUM: $BATCH_SEQ_NUM   `date`
#
##########################################################################################################
"

if [ $FIRST_RUN = Y ]
then
	# Need to run the clean up process since this is the first run for the current processing period.

	print "Running scp_cleanup.ksh for JOB_ENV: $JOB_ENV, JOB_TYPE_ID: $JOB_TYPE_ID  `date`"
	LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.loader_cleanup${UOW_APPEND}.$CURR_DATETIME.log

	set +e
	$DW_MASTER_EXE/dw_infra.loader_cleanup.ksh $JOB_ENV $JOB_TYPE_ID > $LOG_FILE 2>&1
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
#	Setting up extract process specific variables for database and file extraction
#
######################################################################################################

set +e
grep "^SCP_PUSH_RUN_ENV\>" $DW_CFG/$ETL_ID.cfg | read PARAM SCP_PUSH_RUN_ENV COMMENT
ecode=$?
set -e

if [ $rcode != 0 ]
then
        print "${0##*/}:  ERROR, failure determining value for SCP_PUSH_RUN_ENV parameter from $DW_CFG/$ETL_ID.cfg" >&2
        exit 4
fi

if [ $SCP_PUSH_RUN_ENV != $JOB_ENV ]
then
	print "${0##*/}: The scp will not take place in this job environment run."
	tcode=0
	exit
fi

set +e
grep "^SCP_PUSH_SOURCE_ENV\>" $DW_CFG/$ETL_ID.cfg | read PARAM SCP_PUSH_SOURCE_ENV COMMENT
rcode=$?
set -e

if [ $rcode != 0 ]
then
        print "${0##*/}:  ERROR, failure determining value for SCP_PUSH_SOURCE_ENV parameter from $DW_CFG/$ETL_ID.cfg" >&2
        exit 4
fi

export EXTRACT_PROCESS_MSG=single_scp_push
export EXTRACT_CONN_TYPE=scp

set +e
grep "^CNDTL_SCP_PUSH_TO_EXTRACT_VALUE\>" $DW_CFG/$ETL_ID.cfg | read PARAM CNDTL_SCP_PUSH_TO_EXTRACT_VALUE COMMENT
rcode=$?
set -e

if [ $rcode != 0 ]
then
	print "${0##*/}:  ERROR, failure determining value for CNDTL_SCP_PUSH_TO_EXTRACT_VALUE parameter from $DW_CFG/$ETL_ID.cfg" >&2
	exit 4
fi

export CNDTL_SCP_PUSH_TO_EXTRACT_VALUE

if [ $CNDTL_SCP_PUSH_TO_EXTRACT_VALUE = 1 ]
then
	set +e
	grep "^SCP_PUSH_TO_EXTRACT_VALUE_FUNCTION\>" $DW_CFG/$ETL_ID.cfg | read PARAM SCP_PUSH_TO_EXTRACT_VALUE_FUNCTION COMMENT
	rcode=$?
	set -e

	if [ $rcode != 0 ]
	then
		print "${0##*/}:  ERROR, failure determining value for SCP_PUSH_TO_EXTRACT_VALUE_FUNCTION parameter from $DW_CFG/$ETL_ID.cfg" >&2
		exit 4
	fi

	export TO_EXTRACT_VALUE=`eval $(eval print $SCP_PUSH_TO_EXTRACT_VALUE_FUNCTION)`

fi



# check to see if the extract processing has completed yet
set +e
grep -s "^$EXTRACT_PROCESS_MSG\>" $COMP_FILE >/dev/null
RCODE=$?
set -e

if [ $RCODE = 1 ]
then
	############################################################################################################
	#
	#                                   MULTIPLE TABLE PROCESSING
	#
	#  A list of files is read from $TABLE_LIS_FILE.  It has one row for each table that is being extracted
	#  from the source.  This list file contains a FILE_ID, DBC_FILE, PARALLEL_NUM, TABLE_NAME, DATA_FILENAME
	#  and an optional parameter PARAM for passing into the Ab Initio script. 
	#
	#  The tables will be grouped by DBC_FILE where each DBC_FILE represents a thread for processing.
	#  These threads will be run in parallel.  Within each thread, the PARALLEL_NUM parameter is used to
	#  determine how many table extracts can be run at one time.  The run_multi_single_table_extract.ksh
	#  script is run once per thread and manages the parallel processin withing a thread.
	#
	############################################################################################################

	# run wc -l on $ETL_ID.sources.lis file to know how many tables to unload (1 or > 1).
	wc -l $TABLE_LIS_FILE | read TABLE_COUNT FN

	if [[ $TABLE_COUNT -eq 1 ]]
	then
		print "Processing single table extract for TABLE_ID: $TABLE_ID  `date`"

		read FILE_ID DBC_FILE PARALLEL_NUM TABLE_NAME DATA_FILENAME PARAM_LIST < $TABLE_LIS_FILE

		LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.single_scp_push.$CURR_DATETIME.log

		set +e
		eval $DW_EXE/single_scp_push.ksh $ETL_ID $FILE_ID $DBC_FILE $TABLE_NAME $DATA_FILENAME $PARAM_LIST > $LOG_FILE 2>&1
		rcode=$?
		set -e

		if [ $rcode != 0 ]
		then
			print "${0##*/}:  ERROR, see log file $LOG_FILE" >&2
			exit 4
		fi

	elif [[ $TABLE_COUNT -gt 1 ]]
	then
		print "Processing multiple scp extracts for SCP_FILE_ID: $TABLE_ID  `date`"

		export MULTI_COMP_FILE=$DW_SA_TMP/$TABLE_ID.multi_scp_push.complete
		export PARENT_LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.run_scp_push.$CURR_DATETIME.log
		export ERROR_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.single_scp_push.$CURR_DATETIME.err  # job error file

		# If the MULTI_COMP_FILE does not exist, this is the first run, otherwise it is a restart.
		if [ ! -f $MULTI_COMP_FILE ]
		then
			> $MULTI_COMP_FILE
		fi

		# remove previous $EXTRACT_CONN_TYPE list files to ensure looking for the correct set of data files for this run.
		rm -f $DW_SA_TMP/$TABLE_ID.*.$EXTRACT_CONN_TYPE.*.lis

		# Create a list of files to be processed per extract database server.

		while read FILE_ID DBC_FILE PARALLEL_NUM TABLE_NAME DATA_FILENAME PARAM_LIST
		do
			eval DBC_FILE=$DBC_FILE

			if [ ! -f $DW_SA_TMP/$TABLE_ID.$DBC_FILE.*.lis ]
			then
				eval print $FILE_ID $DBC_FILE $TABLE_NAME $DATA_FILENAME $PARAM_LIST > $DW_SA_TMP/$TABLE_ID.$DBC_FILE.$PARALLEL_NUM.lis
			else
				eval print $FILE_ID $DBC_FILE $TABLE_NAME $DATA_FILENAME $PARAM_LIST >> $DW_SA_TMP/$TABLE_ID.$DBC_FILE.$PARALLEL_NUM.lis
			fi
		done < $TABLE_LIS_FILE

		for FILE in $(ls $DW_SA_TMP/$TABLE_ID.*.$EXTRACT_CONN_TYPE.*.lis)
		do
			DBC_FILE=${FILE#$DW_SA_TMP/$TABLE_ID.}
			DBC_FILE=${DBC_FILE%%.*}

			LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.$DBC_FILE.run_multi_scp_push.$CURR_DATETIME.log
			print "Running run_multi_scp_push.ksh $FILE  `date`"
			COMMAND="$DW_EXE/run_multi_scp_push.ksh $FILE $LOG_FILE > $LOG_FILE 2>&1"

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

	print "$EXTRACT_PROCESS_MSG" >> $COMP_FILE

elif [ $RCODE = 0 ]
then
	print "$EXTRACT_PROCESS_MSG process already complete"
else
	exit $RCODE
fi

######################################################################################################
#
#                                Increment BSN
#
#  This section updates the batch_seq_number.  It is now in a non-repeatable
#  Section to avoid issues of restartability.
#
######################################################################################################

PROCESS=Increment_BSN
RCODE=`grepCompFile $PROCESS $COMP_FILE`

if [ $RCODE = 1 ]
then

   print "Updating the batch sequence number file  `date`"
   print $BATCH_SEQ_NUM > $BATCH_SEQ_NUM_FILE
   if [[ "X$UOW_TO" != "X" ]]
   then
      print "Updating the unit of work file  `date`"
      print $UOW_TO > $UNIT_OF_WORK_FILE
   fi

   print "$PROCESS" >> $COMP_FILE

elif [ $RCODE = 0 ]
then
   print "$PROCESS already complete"
else
   exit $RCODE
fi

PROCESS=touch_watchfile
RCODE=`grepCompFile $PROCESS $COMP_FILE`

if [ $RCODE -eq 1 ]
then

   LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.$PROCESS${UOW_APPEND}.$CURR_DATETIME.log
   TFILE_NAME=$ETL_ID.$JOB_TYPE.$BATCH_SEQ_NUM.done

   print "Touching Watchfile $TFILE_NAME$UOW_APPEND"

   set +e
   $DW_MASTER_EXE/touchWatchFile.ksh $ETL_ID $JOB_TYPE $JOB_ENV $TFILE_NAME $UOW_PARAM_LIST > $LOG_FILE 2>&1
   rcode=$?
   set -e

   if [ $rcode -ne 0 ]
   then
      print "${0##*/}:  ERROR, see log file $LOG_FILE" >&2
      exit 4
   fi

   print $PROCESS >> $COMP_FILE

elif [ $RCODE -eq 0 ]
then
   print "$PROCESS already complete"
else
   exit $RCODE
fi 

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
