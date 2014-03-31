#!/bin/ksh -eu
#------------------------------------------------------------------------------------------------
# Filename:     target_table_oracle_load_handler.ksh
#
# Revision History:
#
# Name             Date            Description
# ---------------  --------------  ---------------------------------------------------
# ???              ??/??/????      Initial Creation
# Ryan Wong        10/04/2013      Redhat changes
#
#------------------------------------------------------------------------------------------------

if [ $# -lt 3 ]
then
	print "Usage:  $0 <ETL_ID> <JOB_ENV> <SQL_FILE> [<PARAM_NAME1=PARAM_VALUE1> <PARAM_NAME2=PARAM_VALUE2> ...]"
	exit 4
fi

ETL_ID=$1
JOB_ENV=$2            # dual-active database environment (primary or secondary)
SQL_FILE=$3
JOB_TYPE=bteq
JOB_TYPE_ID=bt

export SUBJECT_AREA=${ETL_ID%%.*}
export TABLE_ID=${ETL_ID##*.}
export SQL_FILENAME=${SQL_FILE##*/}

. /home/abinitio/cfg/abinitio.setup

export DW_SA_LOG=$DW_LOG/$JOB_ENV/$SUBJECT_AREA
export DW_SA_TMP=$DW_TMP/$JOB_ENV/$SUBJECT_AREA

shift 3

if [ $# -ge 1 ]
then
	for param in $*
	do
		if [ ${param%=*} = $param ]
		then
			print "${0##*/}: ERROR, parameter definition $param is not of form <PARAM_NAME=PARAM_VALUE>"
			exit 4
		else
			export $param
		fi
	done
fi

export CURR_DATETIME=$(date '+%Y%m%d-%H%M%S')
PARENT_ERROR_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.target_table_load_handler.${SQL_FILENAME%.sql}.$CURR_DATETIME.err

COMP_FILE=$DW_SA_TMP/$TABLE_ID.bteq.${SQL_FILENAME%.sql}.complete

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

print "
##########################################################################################################
#
# Beginning target table load for ETL_ID: $ETL_ID   `date`
#
##########################################################################################################
"

if [ $FIRST_RUN = Y ]
then
	# Need to run the clean up process since this is the first run for the current processing period.

	print "Running loader_cleanup.ksh for JOB_ENV: $JOB_ENV, JOB_TYPE_ID: $JOB_TYPE_ID  `date`"
	LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.loader_cleanup.${SQL_FILENAME%.sql}.$CURR_DATETIME.log

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
grep -s "target_table_load" $COMP_FILE
RCODE=$?
set -e

if [ $RCODE = 1 ]
then
	print "Processing target table load for TABLE_ID: $TABLE_ID  `date`"

	LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.target_table_load.${SQL_FILENAME%.sql}.$CURR_DATETIME.log

	set +e
	$DW_EXE/target_table_oracle_load.ksh $ETL_ID $JOB_ENV $SQL_FILE > $LOG_FILE 2>&1
	rcode=$?
	set -e

	if [ $rcode != 0 ]
	then
		print "${0##*/}:  ERROR, see log file $LOG_FILE" >&2
		exit 4
	fi

	print "target_table_load" >> $COMP_FILE

elif [ $RCODE = 0 ]
then
	print "target_table_load process already complete"
else
	exit $RCODE
fi

print "Removing the complete file  `date`"
rm -f $COMP_FILE

print "
##########################################################################################################
#
# Target table load for ETL_ID: $ETL_ID complete   `date`
#
##########################################################################################################"

tcode=0
exit
