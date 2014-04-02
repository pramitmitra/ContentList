#!/bin/ksh -eu
#
# This script will determine the next batch_seq_num for the process.
# It is primarily for loads of frequent extracts where the batch_seq_num needs to be incremented by the number of
# extract files to be loaded instead of just by 1.  The extract (even frequent extract) will always increment by only one.
#
# Ported to RedHat by koaks, 20120821
# - using /bin/ksh rather than /usr/bin/ksh
# - converted echo statements to print

BATCH_SEQ_NUM_FILE=$DW_SA_DAT/$TABLE_ID.$JOB_TYPE.batch_seq_num.dat
PREV_BATCH_SEQ_NUM=$(<$BATCH_SEQ_NUM_FILE)

if [ $JOB_TYPE = "extract" ]
then
	((BATCH_SEQ_NUM=PREV_BATCH_SEQ_NUM+1))
else
	# if this is a frequent batch load calculate the number of file sets to wait for, otherwise use 1
	# EXTRACT_FREQUENCY should only exist for frequent batch loaders, do not error if it is not found only
	# if it cannot be determined whether it exists (e.g. cfg file missing)
	set +e
	grep "^EXTRACT_FREQUENCY\>" $DW_CFG/$ETL_ID.cfg | read PARAM EXTRACT_FREQUENCY COMMENT
	rcode=$?
	set -e

	if [ $rcode -gt 1 ]
	then
	       	print "${0##*/}:  ERROR, failure determining whether EXTRACT_FREQUENCY parameter exists in $DW_CFG/$ETL_ID.cfg" >&2
        	exit 4
	fi

	if [ $rcode -eq 0 ]
	then
        	cat $DW_DAT/extract/$SUBJECT_AREA/$TABLE_ID.extract.batch_frequency_info.dat | read EXTRACT_FREQUENCY EXTRACT_RANGE
        	cat $DW_SA_DAT/$TABLE_ID.load.batch_frequency_info.dat | read LOAD_FREQUENCY LOAD_RANGE
        	((NUM_BATCH_SETS=EXTRACT_FREQUENCY/LOAD_FREQUENCY))
	else
        	NUM_BATCH_SETS=1
	fi

	# calculate batch_seq_num to wait for based on number of extract file sets expected for load
	((BATCH_SEQ_NUM=PREV_BATCH_SEQ_NUM+NUM_BATCH_SETS))
fi

print $BATCH_SEQ_NUM

exit
