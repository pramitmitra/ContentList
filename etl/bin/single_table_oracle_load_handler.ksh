#!/bin/ksh -eu
#------------------------------------------------------------------------------------------------
# Filename:     single_table_oracle_load_handler.ksh
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

ETL_ID=$1
JOB_ENV=$2             # dual-active database environment (primary or secondary)
INPUT_DML=${3:-""}
JOB_TYPE=load
JOB_TYPE_ID=ld

export SUBJECT_AREA=${ETL_ID%%.*}
export TABLE_ID=${ETL_ID##*.}

. /dw/etl/mstr_cfg/etlenv.setup

export DW_SA_DAT=$DW_DAT/$JOB_ENV/$SUBJECT_AREA
export DW_SA_LOG=$DW_LOG/$JOB_ENV/$SUBJECT_AREA
export DW_SA_TMP=$DW_TMP/$JOB_ENV/$SUBJECT_AREA

COMP_FILE=$DW_SA_TMP/$TABLE_ID.load.complete
export BATCH_SEQ_NUM_FILE=$DW_SA_DAT/$TABLE_ID.load.batch_seq_num.dat
export CURR_DATETIME=$(date '+%Y%m%d-%H%M%S')
PARENT_ERROR_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.single_table_load_handler.$CURR_DATETIME.err

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
####################################################################################################################
#
# Beginning single table load for ETL_ID: $ETL_ID, BATCH_SEQ_NUM: $BATCH_SEQ_NUM   `date`
#
####################################################################################################################
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

set +e
grep -s "single_table_load" $COMP_FILE
RCODE=$?
set -e

if [ $RCODE = 1 ]
then
	print "Processing single table load for TABLE_ID: $TABLE_ID  `date`"

	LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.single_table_load.$CURR_DATETIME.log

	set +e
	$DW_EXE/single_table_oracle_load.ksh $ETL_ID $JOB_ENV $INPUT_DML > $LOG_FILE 2>&1
	rcode=$?
	set -e

	if [ $rcode != 0 ]
	then
		print "${0##*/}:  ERROR, see log file $LOG_FILE" >&2
		exit 4
	fi

	print "single_table_load" >> $COMP_FILE
elif [ $RCODE = 0 ]
then
	print "single_table_load process already complete"
else
	exit $RCODE
fi

print "Updating the batch sequence number file  `date`"
print $BATCH_SEQ_NUM > $BATCH_SEQ_NUM_FILE

print "Removing the complete file  `date`"
rm -f $COMP_FILE

print "
####################################################################################################################
#
# Single table load for ETL_ID: $ETL_ID, BATCH_SEQ_NUM: $BATCH_SEQ_NUM complete   `date`
#
####################################################################################################################"

tcode=0
exit
