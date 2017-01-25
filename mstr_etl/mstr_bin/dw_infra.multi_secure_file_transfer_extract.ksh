#!/bin/ksh -eu
#############################################################################################################
# Title:        Run Multi Secure File Extract
# File Name:    dw_infra.multi_secure_file_transfer_extract.ksh
# Description:  multi scp script - called by run
#                 File transfer script to be used by Secure File Transfer batch accounts.
#                 Standardize and limit execution of secure accounts.  Least access possible.
# Developer:    Ryan Wong
# Created on:   2016-12-08
# Location:     $DW_MASTER_BIN
# Logic:        Current approved transfer protocols are sftp and scp.
#                 This only supports scp, since it's more suitable for batch than sftp.
#
#
# Called by:    UC4/Unix
#
# Date         Ver#   Modified By(Name)            Change and Reason for Change
#---------    -----  ---------------------------  ------------------------------
# 2016-12-08   1.0    Ryan Wong                    Initital
#############################################################################################################

EXTRACT_TYPE=$1
EXTRACT_LIS_FILE=$2
PARENT_LOG_FILE=$3
shift 3

UOW_FROM=""
UOW_TO=""
UOW_FROM_FLAG=0
UOW_TO_FLAG=0
print "Processing Options"
while getopts "f:t:" opt
do
   case $opt in
      f ) if [ $UOW_FROM_FLAG -ne 0 ]
          then
            print "FATAL ERROR: -f flag specified more than once" >&2
            exit 8
          fi
          print "Setting UOW_FROM_FLAG == 1"
          UOW_FROM_FLAG=1
          print "Setting UOW_FROM == $OPTARG"
          UOW_FROM=$OPTARG;;
      t ) if [ $UOW_TO_FLAG -ne 0 ]
          then
            print "FATAL ERROR: -t flag specified more than once" >&2
            exit 8
          fi
          print "Setting UOW_TO_FLAG == 1" 
          UOW_TO_FLAG=1
          print "Setting UOW_TO == $OPTARG" 
          UOW_TO=$OPTARG;;
      \? ) print "FATAL ERROR: Unrecognized flag" >&2
           print "Usage:  $0 <EXTRACT_TYPE> <EXTRACT_LIS_FILE> <PARENT_LOG_FILE> [-f <UOW_FROM> -t <UOW_TO>]" >&2
           exit 1 ;;
   esac
done
shift $(($OPTIND - 1))

# Calculate UOW values
UOW_APPEND=""
UOW_PARAM_LIST=""
UOW_PARAM_LIST_AB=""
if [[ $UOW_FROM_FLAG -eq 1 && $UOW_TO_FLAG -eq 1 ]]
then
   UOW_APPEND=.$UOW_TO
   UOW_PARAM_LIST="-f $UOW_FROM -t $UOW_TO"
   UOW_PARAM_LIST_AB="-UOW_FROM $UOW_FROM -UOW_TO $UOW_TO"
elif [[ ($UOW_FROM_FLAG -eq 1 && $UOW_TO_FLAG -eq 0) || ($UOW_TO_FLAG -eq 1 && $UOW_FROM_FLAG -eq 0) ]]
then
   print "Missing required UOW (FROM or TO) parameter."
   print "Usage:  $0 <EXTRACT_TYPE> <EXTRACT_LIS_FILE> <PARENT_LOG_FILE> [-f <UOW_FROM> -t <UOW_TO>]" >&2
   exit 1
fi

ERROR_FILE="${PARENT_LOG_FILE%.log}.err"
if [ -f $ERROR_FILE ]
then
	print "Moving the error file to r4a"
	mv $ERROR_FILE $ERROR_FILE.r4a
fi

# added the following piece to make the previous err file as r4a for the monitor

PREV_ERR_FILE=${PARENT_LOG_FILE%_extract.*log}_extract*err  

set +e
eval ls -tr $PREV_ERR_FILE|tail -1 |read PREV_ERR_FILE1
rcode=$?
set -e

if [ $rcode = 0 ]
then 
	set +e
	mv $PREV_ERR_FILE1 $PREV_ERR_FILE1.r4a 
	set -e
fi

integer PLIM
if [ ${USE_GROUP_EXTRACT:-0} -eq 1 ]
then
  PLIM_TMP=${EXTRACT_LIS_FILE%.lis.*}
else
  PLIM_TMP=${EXTRACT_LIS_FILE%.lis}
fi
PLIM=${PLIM_TMP##*.}  # parallel process count limit
PLIS=$$               # process id list, initialized to current process id
PID=$$
((PLIM+=1))           # adjustment for header row and parent in ps output

export CURR_DATETIME=${CURR_DATETIME:-$(date '+%Y%m%d-%H%M%S')}

integer TOTAL_LIS_FILES 
TOTAL_LIS_FILES=0

while read EXTRACT_LIS_DATA
do
	EXTRACT_LIS_ARRAY[$TOTAL_LIS_FILES]=$EXTRACT_LIS_DATA
	((TOTAL_LIS_FILES+=1))
done < $EXTRACT_LIS_FILE

integer j
j=0
until [ $j -ge $TOTAL_LIS_FILES ] 
do
	print ${EXTRACT_LIS_ARRAY[$j]} | read FILE_ID CONN_FILE SOURCE_NAME DATA_FILENAME PARAM_LIST

	# check to see if the $FILE_ID process has already been run (exists in the complete file).  If so, skip it.

	set +e
	grep "^$FILE_ID $SOURCE_NAME" $MULTI_COMP_FILE >/dev/null
	rcode=$?
	set -e

	if [ $rcode = 1 ]
	then
		while [ $(jobs -l | wc -l) -ge $PLIM ]
		do
			if [ $IS_RESUBMITTABLE = 1 ]
			then
				if [ -f $DW_SA_TMP/$ETL_ID.$JOB_TYPE_ID.$CONN_FILE.restart ]
				then 
					break 2 #break out 2 levels.
				fi
			fi

			sleep 30 
			continue
		done

		LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.$FILE_ID.single_${EXTRACT_TYPE}_extract${UOW_APPEND}.$CURR_DATETIME.log
		print "Processing FILE_ID: $FILE_ID, Source Extract: $SOURCE_NAME, CONNECTION FILE: $CONN_FILE  `date`"

		COMMAND="$DW_MASTER_BIN/dw_infra.single_secure_file_transfer_extract.ksh $ETL_ID $FILE_ID $CONN_FILE $SOURCE_NAME $DATA_FILENAME $UOW_PARAM_LIST_AB $PARAM_LIST > $LOG_FILE 2>&1"


		set +e
		eval $COMMAND && (print "Logging completion of FILE_ID: $FILE_ID, SOURCE EXTRACT: $SOURCE_NAME, to $MULTI_COMP_FILE"; print "$FILE_ID $SOURCE_NAME" >> $MULTI_COMP_FILE) >>$LOG_FILE 2>&1 || print "\n${0##*/}: Failure processing FILE_ID: $FILE_ID, SOURCE EXTRACT: $SOURCE_NAME, CONNECTION FILE: $CONN_FILE\nsee log file $LOG_FILE" >>$ERROR_FILE &
		PLIS=$PLIS,$!
		set -e

	elif [ $rcode = 0 ]
	then
		print "Extract for FILE_ID: $FILE_ID, SOURCE EXTRACT: $SOURCE_NAME, CONNECTION FILE: $CONN_FILE already complete" >> $PARENT_LOG_FILE
	else
		exit $rcode
	fi

	((j+=1))
done

if [ $IS_RESUBMITTABLE = 1 ]
then
	# While a process is still running, check for kill file
	while [ $(jobs -l | wc -l) -ge 2 ]
	do
		# add check here to see if the job needs to be cancelled.

		if [ -f $DW_SA_TMP/$ETL_ID.$JOB_TYPE_ID.$CONN_FILE.restart ]
		then

			# Operations has requested a restart - kill jobs, and exit process
			# PLIS contains processes which can be killed.
			# Strip each one off and kill it if its running.

			KLIS=${PLIS} 
			print "Extract for FILE_ID: $FILE_ID, SOURCE EXTRACT: $SOURCE_NAME, CONNECTION FILE: $CONN_FILE is being killed"
			print "due to existence of $DW_SA_TMP/$ETL_ID.$JOB_TYPE_ID.$CONN_FILE.restart restart file" 
			print "The FILE_ID killed may be $FILE_ID - 1 for multiple extracts run against the same host"  

			while [ ${KLIS%,*} != $KLIS ]
			do
				KPID=${KLIS##*,}
				KLIS=${KLIS%,*}

				if [ $(ps -o'pid ppid' -p$KPID | grep " $PID$" | wc -l) -ge 1 ] #there are processes to kill
				then
					# The eval process's PID is stored in the PLIS
					# we get all the children's PIDs and the grandchilren's PIDs and kill them and later kill children

					for child_proc in $(ps -o pid,ppid,time,comm -aef|awk "{ if ( \$2 == $KPID ) { print \$1 }}")
					do  
						for grand_child_proc in $(ps -o pid,ppid,time,comm -aef|awk "{ if ( \$2 == $child_proc ) { print \$1 }}")
						do
							set +e
							kill $grand_child_proc
							rcode=$?
							set -e
							print "the return code from grand child kill is $rcode"
						done

						set +e
						kill $child_proc
						rcode=$?
						set -e

						print "the return code from child kill is $rcode"
					done

					# Parent Kill was removed
					#child killing end
				fi
			done

			exit 4 
		fi

		sleep 30 
	done
fi

wait

exit 0
