#!/bin/ksh -eu
#############################################################################################################
# Title:        Run Multi Secure File Load
# File Name:    dw_infra.multi_secure_file_transfer_load.ksh
# Description:  multi scp script - called by run
#                 File transfer script to be used by Secure File Transfer batch accounts.
#                 Standardize and limit execution of secure accounts.  Least access possible.
# Developer:    Ryan Wong
# Created on:   2016-12-09
# Location:     $DW_MASTER_BIN
# Logic:        Current approved transfer protocols are sftp and scp.
#                 This only supports scp, since it's more suitable for batch than sftp.
#
#
# Called by:    UC4/Unix
#
# Date         Ver#   Modified By(Name)            Change and Reason for Change
#---------    -----  ---------------------------  ------------------------------
# 2016-12-09   1.0    Ryan Wong                    Initital
#############################################################################################################

SCP_LIS_FILE=$1
PARENT_LOG_FILE=$2

integer PLIM
if [ ${USE_GROUP_EXTRACT:-0} -eq 1 ]
then
  PLIM_TMP=${SCP_LIS_FILE%.lis.*}
else
  PLIM_TMP=${SCP_LIS_FILE%.lis}
fi
PLIM=${PLIM_TMP##*.}  # parallel process count limit
#PLIS=$$               # process id list, initialized to current process id
#PPID=$$               # current process id which is the ppid of scp process
#((PLIM+=2))           # adjustment for header row and parent in ps output

while read FILE_ID SCP_CONN SOURCE_FILE TARGET_FILE PARAM_LIST
do

	# check to see if the $FILE_ID process has already been run (exists in the complete file).  If so, skip it.
	set +e
	grep "^$FILE_ID $SOURCE_FILE" $MULTI_COMP_FILE >/dev/null
	rcode=$?
	set -e

	if [ $rcode = 1 ]
	then
                # while [ $(ps -p$PLIS -eo'pid ppid' | grep " $PPID$" | wc -l) -ge $PLIM ]
                # using ksh build-in job control function to do parallel check
                while [ $(jobs -p | wc -l) -ge $PLIM ]
		do
			sleep 30
			continue
		done
                #rebuild the process list to only caputer the live process (unnecessary while using jobs)
		#PLIS_TMP=`ps -eo'pid ppid' | grep " $PPID$" | awk '{printf $1","}'`
		#PLIS=${PLIS_TMP%*,}

		LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.$FILE_ID.single_secure_file_transfer_load.$CURR_DATETIME.log

		print "Processing FILE_ID: $FILE_ID, SOURCE_FILE: $SOURCE_FILE, SCP FILE: $SCP_CONN  `date`"

		COMMAND="$DW_MASTER_BIN/dw_infra.single_secure_file_transfer_load.ksh $ETL_ID $FILE_ID $SCP_CONN $SOURCE_FILE $TARGET_FILE > $LOG_FILE 2>&1"

		set +e
		eval $COMMAND && (print "Logging completion of FILE_ID: $FILE_ID, SOURCE_FILE: $SOURCE_FILE, to $MULTI_COMP_FILE"; print "$FILE_ID $SOURCE_FILE" >> $MULTI_COMP_FILE) >>$LOG_FILE 2>&1 || print "\n${0##*/}: Failure processing FILE_ID: $FILE_ID, SOURCE_FILE: $SOURCE_FILE, SCP FILE: $SCP_CONN\nsee log file $LOG_FILE" >>$ERROR_FILE &
		#PLIS=$PLIS,$!
		set -e

	elif [ $rcode = 0 ]
	then
		print "Extract for FILE_ID: $FILE_ID, SOURCE_FILE: $SOURCE_FILE, SCP FILE: $SCP_CONN already complete" >> $PARENT_LOG_FILE
	else
		exit $rcode
	fi

done < $SCP_LIS_FILE

wait

exit
