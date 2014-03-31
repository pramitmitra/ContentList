#!/bin/ksh -eu

#------------------------------------------------------------------------------------------------
# Filename:     reject_file_check.ksh
# Description:  Checks for existence of reject files after loader execution. Copies files
#               to $DW_SA_LOG if reject files contain reject records (all reject files are
#               created with a header and trailer). If EMAIL_NOTIFY_GROUP is detected either 
#				in the $ETL_ID.cfg or due to the existence of an email notify group list then
#               notification is sent to EMAIL_NOTIFY_GROUP. 
#				Does not return a failure (don't have a reject threshhold if that is the
#				behavior you are looking for) just designed to alert for follow up.
#
# Developer:    Brian Knauss
# Created on:   01/16/2008
# Location:     $DW_EXE/
#
# Execution:    $DW_EXE/reject_file_check.ksh
#
# Parameters:   
#
# Revision History:
#
# Name             Date            Description
# ---------------  --------------  --------------------------------------------------------------
# Brian Knauss     01/16/2008      Initial Creation
# Kevin Oaks	   10/01/2008	   Refined for production. Is called via single_table_load_handler.ksh
#                                  but can also be used with reject_file_check_handler.ksh
# 2012-08-09   1.3    Kevin Oaks                   Port to RedHat:
#                                                   - now using /bin/ksh rather than /usr/bin/ksh
#                                                   - converted echo statements to print
#------------------------------------------------------------------------------------------------

REJ_CHECK=0					# indicates rejects were detected (or reject file did not exist)
EMAIL_NOTIFY_GROUP=""
EMAIL_MESSAGE=""

#------------------------------------------------------------
# Check for notification flag in cfg file - disabling for
# initial implementation. For now we will simply look for
# a non-zero length EMAIL_NOTIFY_GROUP
#------------------------------------------------------------

#set +e
#grep "^LOAD_REJ_NOTIFY\>" $DW_CFG/$ETL_ID.cfg | read PARAM LOAD_REJ_NOTIFY COMMENT
#rcode=$?
#set -e

#if [ $rcode != 0 ]
#then
#	LOAD_REJ_NOTIFY=0
#fi

#if [ $LOAD_REJ_NOTIFY = 1 ]
#then

	#------------------------------------------------------------
	# Check for email notify group, either in cfg file or 
	# subject area mail list
	#------------------------------------------------------------

	set +e
	grep "^EMAIL_NOTIFY_GROUP\>" $DW_CFG/$ETL_ID.cfg | read PARAM EMAIL_NOTIFY_GROUP COMMENT
	rcode=$?
	set -e

	if [[ $rcode != 0 && -s $DW_CFG/$SUBJECT_AREA.email_notify_group.lis ]]
	then
		EMAIL_NOTIFY_GROUP=$(<$DW_CFG/$SUBJECT_AREA.email_notify_group.lis)
	fi

#	if [ -z $EMAIL_NOTIFY_GROUP ]
#	then
#		print "${0##*/}:  ERROR, Unable to determine EMAIL_NOTIFY_GROUP." >&2 
#		exit 4
#	fi
#fi



#------------------------------------------------------------
# Check number of records in default reformat reject file,
# if configuration file indicates conditional reformat.
# More than 2 (header and trailer) indicates rejects.
#------------------------------------------------------------
set +e
grep "^CNDTL_REFORMAT\>" $DW_CFG/$ETL_ID.cfg | read PARAM CNDTL_REFORMAT COMMENT
rcode=$?
set -e

if [ $CNDTL_REFORMAT = 1 ]
then
	if [ -f $DW_SA_TMP/$TABLE_ID.ld.reformat.rej ]
	then
		integer REF_REJ=$(wc -c < $DW_SA_TMP/$TABLE_ID.ld.reformat.rej)-$(head -1 $DW_SA_TMP/$TABLE_ID.ld.reformat.rej | wc -c)*2+1

		if [ $REF_REJ -gt 0 ]
		then
			print "Copying reformat reject and error files from Temp Dir to Log Dir."
			cp $DW_SA_TMP/$TABLE_ID.ld.reformat.rej $DW_SA_LOG/$TABLE_ID.ld.reformat.$BATCH_SEQ_NUM.rej
			cp $DW_SA_TMP/$TABLE_ID.ld.reformat.err $DW_SA_LOG/$TABLE_ID.ld.reformat.$BATCH_SEQ_NUM.err
			EMAIL_MESSAGE="Load transform rejected "$REF_REJ" record(s).\nTemp reject and error files copied as "$DW_SA_LOG/$TABLE_ID.ld.reformat.$BATCH_SEQ_NUM.rej" and "$DW_SA_LOG/$TABLE_ID.ld.reformat.$BATCH_SEQ_NUM.err".\nThis file will be subject to standard .r4a tagging and archiving practices.\n\n"

			REJ_CHECK=1
		fi
	else
		EMAIL_MESSAGE="Expected reformat reject file "$DW_SA_TMP/$TABLE_ID.ld.reformat.rej" not found, load may not have completed.\n"
		REJ_CHECK=1
	fi
fi

#------------------------------------------------------------
# Check number of records in default loader reject file.
# More than 2 (header and trailer) indicates rejects.
#------------------------------------------------------------
if [ -f $DW_SA_TMP/$TABLE_ID.ld.utility_load.rej ]
then
	integer LOAD_REJ=$(wc -c < $DW_SA_TMP/$TABLE_ID.ld.utility_load.rej)-$(head -1 $DW_SA_TMP/$TABLE_ID.ld.utility_load.rej|wc -c)*2+1

	if [ $LOAD_REJ -gt 0 ]
	then
		print "Copying load reject and error files from Temp to Log."
		cp $DW_SA_TMP/$TABLE_ID.ld.utility_load.rej $DW_SA_LOG/$TABLE_ID.ld.utility_load.$BATCH_SEQ_NUM.rej
		cp $DW_SA_TMP/$TABLE_ID.ld.utility_load.err $DW_SA_LOG/$TABLE_ID.ld.utility_load.$BATCH_SEQ_NUM.err
		EMAIL_MESSAGE=$EMAIL_MESSAGE"Load mechanism rejected "$LOAD_REJ" record(s).\nTemp reject and error files copied as "$DW_SA_LOG/$TABLE_ID.ld.utility_load.$BATCH_SEQ_NUM.rej" and "$DW_SA_LOG/$TABLE_ID.ld.utility_load.$BATCH_SEQ_NUM.err".\nThis file will be subject to standard .r4a tagging and archiving practices.\n\n"
		REJ_CHECK=1
	fi
else
	EMAIL_MESSAGE=$EMAIL_MESSAGE"Expected loader reject file "$DW_SA_TMP/$TABLE_ID.ld.utility_load.rej" not found, load may not have completed.\n"
	REJ_CHECK=1
fi

if [ $REJ_CHECK -eq 1 ]
then
	print $EMAIL_MESSAGE

	ETL_SERVER=$(uname -n)

	if [ ! -z $EMAIL_NOTIFY_GROUP ]
	then
		print $EMAIL_MESSAGE | mailx -s "$ETL_SERVER: WARNING ONLY... Reject records found for $ETL_ID in $JOB_ENV, Batch Sequence Number $BATCH_SEQ_NUM" $EMAIL_NOTIFY_GROUP
	fi
else
	print "No reject records found."
fi

exit 0
