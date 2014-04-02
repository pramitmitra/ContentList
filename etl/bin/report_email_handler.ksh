#!/bin/ksh -eu
#------------------------------------------------------------------------------------------------
# Filename:     report_email_handler.ksh
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
	print "\nUsage:  $0 <ETL_ID> <JOB_ENV> <SQL_FILE>[<RPT_FILE = REPORT_FILE_NAME> <EMAIL_SUBJECT = EMAIL_SUBJECT> <ADD_EMAIL=email,email,...> <SEVERITY_LEVEL=NOTIFY,WARNING,ERROR> <PARAM_NAME1=PARAM_VALUE1> <PARAM_NAME2=PARAM_VALUE2> ...]\n"
	exit 4
fi

ETL_ID=$1
JOB_ENV=$2            # dual-active database environment (primary or secondary)
SQL_FILE=$3
JOB_TYPE=bteq
JOB_TYPE_ID=email

TABLE_ID=${ETL_ID##*.}
SQL_FILENAME=${SQL_FILE##*/}
SUBJECT_AREA=${ETL_ID%%.*}

. /export/home/abinitio/cfg/abinitio.setup

DW_SA_LOG=$DW_LOG/$JOB_ENV/$SUBJECT_AREA
DW_SA_TMP=$DW_TMP/$JOB_ENV/$SUBJECT_AREA
export RPT_FILE=$DW_TMP/$JOB_ENV/$SUBJECT_AREA/$SQL_FILE.rpt

set +e
grep "^$SUBJECT_AREA\>" $DW_CFG/subject_area_email_list.dat | read PARAM EMAIL_GROUP
rcode=$?
set -e

if [ $rcode != 0 ]
then
	print "${0##*/}:  ERROR, failure determining value for EMAIL_GROUP parameter from $DW_CFG/subject_area_email_
list.dat"
        exit 4
fi


EMAIL_SUBJECT="${0##*/} job report"
ADD_EMAIL=""
SEVERITY_LEVEL=""
shift 3
if [ $# -ge 1 ]
then
	for param in "$@" 
	do
		if [ ${param%=*} = ${param#*=} ]
		then
			print "${0##*/}: ERROR, parameter definition $param is not of form <PARAM_NAME=PARAM_VALUE>"
			exit 4
		else
			export ${param%=*}="${param#*=}"
		fi
	done
fi

case $SEVERITY_LEVEL in
NOTIFY|notify)
    EMAIL_GROUP=`print $EMAIL_GROUP|tr -s ' ' ' '|cut -f1 -d' '` ;;
WARNING|warning)
    EMAIL_GROUP=`print $EMAIL_GROUP|tr -s ' ' ' '|cut -f1,2 -d' '` ;;
esac

CURR_DATETIME=$(date '+%Y%m%d-%H%M%S')
PARENT_ERROR_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.target_table_email_handler.${SQL_FILENAME%.sql}.$CURR_DATETIME.err
COMPLETE_FILE=$DW_SA_TMP/$TABLE_ID.email.${SQL_FILENAME%.sql}.complete

if [ ! -f $COMPLETE_FILE ]
then
	# COMP_FILE does not exist.  1st run for this processing period.
	INITIAL_RUN=Y
else
	INITIAL_RUN=N
fi


if [ -f $RPT_FILE ]
then
                rm -f $RPT_FILE
fi

# Source the error message handling logic.  On failure, trap will send the contents of the PARENT_ERROR_FILE to the
# subject area designated email addresses.
. $DW_LIB/message_handler

#----------------------------------------------------------------------
# Remove previous files
#----------------------------------------------------------------------
if [ $INITIAL_RUN = Y ]
then
	LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.target_table_email.${SQL_FILENAME%.sql}.$CURR_DATETIME.log

       #------------------------------------------------------------------------
       #  Move email log/err files to the archive directory.
       #------------------------------------------------------------------------
        print "Marking log files from previous processing periods as .r4a"

        if [ -f $DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.*${SQL_FILENAME%.sql}.!(*.r4a|*$CURR_DATETIME.*) ]
        then
                for fn in $(ls $DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.*${SQL_FILENAME%.sql}.!(*.r4a|*$CURR_DATETIME.*))
                do
                        if [[ ${fn##*.} == err && ! -s $fn ]]
                        then
                                rm -f $fn     # remove empty error files
                        else
                                mv -f $fn $fn.r4a
                        fi
                done
        fi
	> $COMPLETE_FILE
else
	print "clean up already complete"
fi

set +e
grep -s "target_table_email" $COMPLETE_FILE
RCODE=$?
set -e


if [ $RCODE = 1 ]
then
	print "Processing target table email handler for TABLE_ID: $TABLE_ID  `date`"

	LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.target_table_email.${SQL_FILENAME%.sql}.$CURR_DATETIME.log

	set +e
	$DW_EXE/target_table_load_handler.ksh $ETL_ID $JOB_ENV $SQL_FILE > $LOG_FILE 2>&1
	rcode=$?
	set -e

	if [ $rcode != 0 ]
	then
		print "${0##*/}:  ERROR, see log file $LOG_FILE" >&2
		if [ -s $RPT_FILE ]
		then
			print "Job Failed:Mailing the $RPT_FILE file to $SUBJECT_AREA mailing list recipients"
			cat $RPT_FILE | mailx -s "${0##*/} JOB FAILED $EMAIL_SUBJECT" $EMAIL_GROUP
		else   
			print "No report file found"
		fi
		exit 4
	fi

	print "target_table_email" >> $COMPLETE_FILE

elif [ $RCODE = 0 ]
then
	print "target_table_email process already complete"
else
	exit $RCODE
fi

print "Removing the complete file  `date`"
rm -f $COMPLETE_FILE

#------------------------------------------------------------------------
#  Email Report file to the email group.
#------------------------------------------------------------------------

if [ -s $RPT_FILE ]
then
	print "Mailing the $RPT_FILE file to $SUBJECT_AREA mailing list recipients"
	cat $RPT_FILE | mailx -s "$EMAIL_SUBJECT" $EMAIL_GROUP $ADD_EMAIL
else   
	print "No report file found"
fi
tcode=0

exit
