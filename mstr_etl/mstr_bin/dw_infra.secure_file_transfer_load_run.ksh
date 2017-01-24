#!/bin/ksh -eu
# Title:        Secure File Transfer Load Run
# File Name:    dw_infra.secure_file_transfer_load_run.ksh
# Description:  Run script - called by handler
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

. $DW_MASTER_LIB/dw_etl_common_functions.lib


COMP_FILE=$DW_SA_TMP/$TABLE_ID.$JOB_TYPE.complete
export BATCH_SEQ_NUM_FILE=$DW_SA_DAT/$TABLE_ID.$JOB_TYPE.batch_seq_num.dat     
UNIT_OF_WORK_FILE=$DW_SA_DAT/$TABLE_ID.$JOB_TYPE.uow.dat

if [[ ! -f $COMP_FILE ]]
then
	# COMP_FILE does not exist.  1st run for this processing period.
	FIRST_RUN=Y
else
	FIRST_RUN=N

fi

#-------------------------------------------------------------------------------------
# Source the error message handling logic.  On failure, trap will send the contents
# of the PARENT_ERROR_FILE to the subject area designated email addresses.
#-------------------------------------------------------------------------------------
. $DW_LIB/message_handler

# get BATCH_SEQ_NUM
if [[ $FIRST_RUN = Y ]]
then
   export BATCH_SEQ_NUM=$($DW_EXE/get_batch_seq_num.ksh)
else
   # In case of a restart - Check if BSN file has been incremented
   PROCESS=Increment_BSN
   RCODE=`grepCompFile $PROCESS $COMP_FILE`

   if [[ $RCODE -eq 1 ]]
   then
      export BATCH_SEQ_NUM=$($DW_EXE/get_batch_seq_num.ksh)
   elif [[ $RCODE -eq 0 ]]
   then
      export BATCH_SEQ_NUM=$(<$BATCH_SEQ_NUM_FILE)
   else
      exit $RCODE
   fi
fi

# Print standard environment variables
set +u
print_standard_env
set -u

print "
####################################################################################################################
#
# Beginning single table load for ETL_ID: $ETL_ID, BATCH_SEQ_NUM: $BATCH_SEQ_NUM   `date`
#
####################################################################################################################
"

set +e
 grep "^LOAD_PROCESS_TYPE\>" $ETL_CFG_FILE | read PARAM LOAD_PROCESS_TYPE COMMENT
 rcode=$?
set -e

if [ $rcode != 0 ]
then
    print "${0##*/}:  ERROR, LOAD_PROCESS_TYPE not set" >&2
    exit $rcode
fi

if [[ $LOAD_PROCESS_TYPE != F ]]
then
    print "${0##*/}:  ERROR, LOAD_PROCESS_TYPE may only be F, not $LOAD_PROCESS_TYPE" >&2
    exit 4
fi

export LOAD_PROCESS_TYPE

if [ $FIRST_RUN = Y ]
then
	# Need to run the clean up process since this is the first run for the current processing period.
	  print "Running dw_infra.loader_cleanup.ksh for JOB_ENV: $JOB_ENV, JOB_TYPE_ID: $JOB_TYPE_ID  `date`"
	  LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.dw_infra.loader_cleanup${UOW_APPEND}.$CURR_DATETIME.log

	  set +e
	  $DW_MASTER_BIN/dw_infra.loader_cleanup.ksh $JOB_ENV $JOB_TYPE_ID > $LOG_FILE 2>&1
	  rcode=$?
	  set -e

	  if [ $rcode != 0 ]
	  then
		print "${0##*/}:  ERROR, see log file $LOG_FILE" >&2
		exit 4
	  fi

	> $COMP_FILE

else
	print "dw_infra.loader_cleanup.ksh process already complete"
fi



###################################################################################################################
#	Data File Loading process
###################################################################################################################

PROCESS=single_table_load
RCODE=`grepCompFile $PROCESS $COMP_FILE`

if [ $RCODE = 1 ]
then
	##SCP Load ##
	##Generate TABLE_LIS_FILE
			
		TABLE_LIS_FILE=$DW_SA_TMP/$ETL_ID.target.lis			
		rm -f $TABLE_LIS_FILE


			
		assignTagValue LOAD_SCP_USE_TARGET_LIS LOAD_SCP_USE_TARGET_LIS $ETL_CFG_FILE W 0		
		export CNDTL_SCP_PUSH_TO_EXTRACT_VALUE=0 			## add to reuse module single_scp_push.ksh
		
		if [ $LOAD_SCP_USE_TARGET_LIS = 1 ]
		then
			cp $DW_CFG/$ETL_ID.target.lis  $TABLE_LIS_FILE		
		else 
			assignTagValue EXTRACT_PROCESS_TYPE EXTRACT_PROCESS_TYPE $ETL_CFG_FILE
			assignTagValue USE_DISTR_TABLE USE_DISTR_TABLE $ETL_CFG_FILE
			assignTagValue LOAD_SCP_PARALLEL_NUM LOAD_SCP_PARALLEL_NUM  $ETL_CFG_FILE 
			assignTagValue LOAD_SCP_CONN LOAD_SCP_CONN $ETL_CFG_FILE  

			if [ $EXTRACT_PROCESS_TYPE = D ]
			then
				if [ $USE_DISTR_TABLE -eq 0 ]
				then
					if [[ -n $UOW_TO ]]
					then
						while read FILE_ID DBC_FILE PARALLEL_NUM TABLE_NAME DATA_FILENAME PARAM
						do
							if [ ! -f $TABLE_LIS_FILE ]
							then
								eval print $FILE_ID $LOAD_SCP_CONN $LOAD_SCP_PARALLEL_NUM $DATA_FILENAME $DATA_FILENAME $PARAM > $TABLE_LIS_FILE
							else
								eval print $FILE_ID $LOAD_SCP_CONN $LOAD_SCP_PARALLEL_NUM $DATA_FILENAME $DATA_FILENAME $PARAM >> $TABLE_LIS_FILE
							fi
						done < $DW_CFG/$ETL_ID.sources.lis
					else
						while read FILE_ID DBC_FILE PARALLEL_NUM TABLE_NAME DATA_FILENAME PARAM
						do
							if [ ! -f $TABLE_LIS_FILE ]
							then
								eval print $FILE_ID $LOAD_SCP_CONN $LOAD_SCP_PARALLEL_NUM $DATA_FILENAME $DATA_FILENAME.$BATCH_SEQ_NUM $PARAM > $TABLE_LIS_FILE
							else
								eval print $FILE_ID $LOAD_SCP_CONN $LOAD_SCP_PARALLEL_NUM $DATA_FILENAME $DATA_FILENAME.$BATCH_SEQ_NUM $PARAM >> $TABLE_LIS_FILE
							fi
						done < $DW_CFG/$ETL_ID.sources.lis
					fi
				else
					print "USE_DISTR_TABLE type scp not support yet!"
				fi	
			
			elif [ $EXTRACT_PROCESS_TYPE = L ]
			then		
					
					if [[ -n $UOW_TO ]]
					then
						while read FILE_ID DATA_FILENAME OLD_FILENAME DONE_FILENAME
						do
							if [ ! -f $TABLE_LIS_FILE ]
							then
								eval print $FILE_ID $LOAD_SCP_CONN $LOAD_SCP_PARALLEL_NUM $DATA_FILENAME $DATA_FILENAME $PARAM > $TABLE_LIS_FILE
							else
								eval print $FILE_ID $LOAD_SCP_CONN $LOAD_SCP_PARALLEL_NUM $DATA_FILENAME $DATA_FILENAME $PARAM >> $TABLE_LIS_FILE
							fi
						done < $DW_DAT/extract/$SUBJECT_AREA/$ETL_ID.sources.lis.$UOW_TO
					else
						while read FILE_ID DATA_FILENAME OLD_FILENAME DONE_FILENAME
				        	do
				            		if [ ! -f $TABLE_LIS_FILE ]
							then
								eval print $FILE_ID $LOAD_SCP_CONN $LOAD_SCP_PARALLEL_NUM $DATA_FILENAME $DATA_FILENAME.$BATCH_SEQ_NUM $PARAM > $TABLE_LIS_FILE
							else
								eval print $FILE_ID $LOAD_SCP_CONN $LOAD_SCP_PARALLEL_NUM $DATA_FILENAME $DATA_FILENAME.$BATCH_SEQ_NUM $PARAM >> $TABLE_LIS_FILE							 
				            		fi            
				        	done < $DW_DAT/extract/$SUBJECT_AREA/$ETL_ID.sources.lis.$BATCH_SEQ_NUM	
					fi
					
			fi
		fi
		
		##RUN MULTI SCP      
		
		wc -l $TABLE_LIS_FILE | read TABLE_COUNT FN
	
		if [[ $TABLE_COUNT -gt 0 ]]
		then
	                print "Processing multiple scp load for SCP_FILE_ID: $TABLE_ID  `date`"
	                export DW_SA_OUT=$IN_DIR      
	                export LOAD_CONN_TYPE=scp
			export MULTI_COMP_FILE=$DW_SA_TMP/$TABLE_ID.multi_secure_file_transfer_load.complete
			export PARENT_LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.run_scp_push.$CURR_DATETIME.log
			export ERROR_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.run_scp_push.$CURR_DATETIME.err  # job error file
		
			# If the MULTI_COMP_FILE does not exist, this is the first run, otherwise it is a restart.
			if [ ! -f $MULTI_COMP_FILE ]
			then
				> $MULTI_COMP_FILE
			fi
		
			# remove previous $LOAD_CONN_TYPE list files to ensure looking for the correct set of data files for this run.
			rm -f $DW_SA_TMP/$TABLE_ID.*.$LOAD_CONN_TYPE.*.lis
		
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
		
			for FILE in $(ls $DW_SA_TMP/$TABLE_ID.*.$LOAD_CONN_TYPE.*.lis)
			do
				DBC_FILE=${FILE#$DW_SA_TMP/$TABLE_ID.}
				DBC_FILE=${DBC_FILE%%.*}
		
				LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.$DBC_FILE.multi_secure_file_transfer_load.$CURR_DATETIME.log
				print "Running dw_infra.multi_secure_file_transfer_load.ksh $FILE  `date`"
				COMMAND="$DW_MASTER_BIN/dw_infra.multi_secure_file_transfer_load.ksh $FILE $LOG_FILE > $LOG_FILE 2>&1"
	
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

			rm -f $MULTI_COMP_FILE
		
		else
			print "${0##*/}:  ERROR, no rows exist in file $TABLE_LIS_FILE" >&2
			exit 4
		fi
		
	print "$PROCESS" >> $COMP_FILE	
	
elif [ $RCODE = 0 ]
then
	print "$PROCESS process already complete"
else
	print "${0##*/}:  ERROR, Unable to grep for single_table_load in $COMP_FILE"
	exit $RCODE
fi

###################################################################################################################
#
#	Run load reject check. First incarnation only checks for abinitio rejects in load/transform reject file.
#	If they exist, file(s) will be moved from $DW_SA_TMP to $DW_SA_LOG and will be archived along with the
#	log files. Notification will be sent to an EMAIL_NOTIFY_LIST only if reject notification is set in 
#	the ETL_ID configuration file. 
#
###################################################################################################################
PROCESS=reject_file_check
RCODE=`grepCompFile $PROCESS $COMP_FILE`

if [ $RCODE = 1 ]
then

	print "Processing reject file check for  TABLE_ID: $TABLE_ID  `date`"
	LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.reject_file_check${UOW_APPEND}.$CURR_DATETIME.log

	set +e
	eval $DW_EXE/reject_file_check.ksh > $LOG_FILE 2>&1
	RCODE=$?
	set -e

	if [ $RCODE != 0 ]
	then
		print "${0##*/}:  ERROR, see log file $LOG_FILE" >&2
		exit 4
	fi

	print "$PROCESS" >> $COMP_FILE

elif [ $RCODE = 0 ]
then
	print "$PROCESS process already complete"
else
	print "${0##*/}:  ERROR, Unable to grep for $PROCESS in $COMP_FILE" 
	exit $RCODE
fi


###################################################################################################################
#
#       Delete obsolete historical data from partitioned Work tables (if any).
#
###################################################################################################################
PROCESS=delete_partitioned_work
RCODE=`grepCompFile $PROCESS $COMP_FILE`

if [ $RCODE = 1 ]
then

        print "Deleting from partitioned work table(s) for ETL_ID: $ETL_ID  `date`"
        LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.delete_partitioned_work${UOW_APPEND}.$CURR_DATETIME.log

        set +e
        $DW_MASTER_BIN/dw_infra.partitioned_work_cleanup.ksh $ETL_ID $JOB_ENV > $LOG_FILE 2>&1
        RCODE=$?
        set -e

        if [ $RCODE != 0 ]
        then
                print "${0##*/}:  ERROR, see log file $LOG_FILE" >&2
                exit 4
        fi

        print "$PROCESS" >> $COMP_FILE

elif [ $RCODE = 0 ]
then
        print "$PROCESS process already complete"
else
        print "${0##*/}:  ERROR, Unable to grep for $PROCESS in $COMP_FILE" 
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

######################################################################################################
#
#                                Finalize Processing
#
#  This section creates the watch_file.  It is now in a non-repeatable
#  Section to avoid issues of restartability, more likely to occur now that there is a syncing step
#  following it
#
######################################################################################################


PROCESS=finalize_processing
RCODE=`grepCompFile $PROCESS $COMP_FILE`

if [ $RCODE = 1 ]
then

   # read current batch seq num from file in case this is a restart
#   BATCH_SEQ_NUM=$(<$BATCH_SEQ_NUM_FILE)
#   export BATCH_SEQ_NUM

   WATCH_FILE=$ETL_ID.$JOB_TYPE.$BATCH_SEQ_NUM.done
   LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.touchWatchFile${UOW_APPEND}.$CURR_DATETIME.log
   print "Running $DW_MASTER_EXE/touchWatchFile.ksh $ETL_ID $JOB_TYPE $JOB_ENV $WATCH_FILE $UOW_PARAM_LIST"

   set _e
   $DW_MASTER_EXE/touchWatchFile.ksh $ETL_ID $JOB_TYPE $JOB_ENV $WATCH_FILE $UOW_PARAM_LIST > $LOG_FILE 2>&1
   rcode=$?
   set -e

   if [ $rcode -ne 0 ]
   then
      print "${0##*/}:  ERROR, see log file $LOG_FILE" >&2
      exit 4
   fi

   print "$PROCESS" >> $COMP_FILE

elif [ $RCODE = 0 ]
then
   print "$PROCESS process already complete"
else
   exit $RCODE
fi

######################################################################################################
#
#                                Synch Files
#
#  This section determines the files to be synced and syncs the to the HA/DR servers.
#  BATCH_SEQ_NUM_FILE has already been updated to the processed BATCH_SEQ_NUM.  To avoid
#  differences on restart, re-read the BATCH_SEQ_NUM_FILE and use its value.
#
######################################################################################################

PROCESS=HADR_Synch
RCODE=`grepCompFile $PROCESS $COMP_FILE`

if [ $RCODE = 1 ]
then

   # extract synch files add on.
   if [ $HADR_ACTIVE = 1 ]
   then
      # file synching is on,
#      BATCH_SEQ_NUM=$(<$BATCH_SEQ_NUM_FILE)
#      export BATCH_SEQ_NUM

      #export HADR_SYNCH_FILE=$DW_SA_TMP/$ETL_ID.$JOB_ENV.$JOB_TYPE_ID.sync_files.$BATCH_SEQ_NUM.dat
      if [[ -n $UOW_TO ]]
      then
         PCGID=$ETL_ID.$JOB_ENV.$JOB_TYPE_ID.$UOW_TO
      else
         PCGID=$ETL_ID.$JOB_ENV.$JOB_TYPE_ID.$BATCH_SEQ_NUM
      fi
      HADR_STATELIS=$DW_SA_TMP/$PCGID.synch_files.state.dat
      print "${0##*/}:  INFO, HA DR Synch File : HADR_STATELIS = $HADR_STATELIS"
      LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.hadr${UOW_APPEND}.$CURR_DATETIME.log
      
      print $BATCH_SEQ_NUM_FILE > $HADR_STATELIS

      if [[ -n $UOW_TO ]]
      then
         set +e
         eval $DW_MASTER_BIN/dw_infra.synch_hadr_node_handler.ksh -i $ETL_ID -e $JOB_ENV -t $JOB_TYPE_ID -l $LOG_FILE -u $UOW_TO
         HADR_RCODE=$?
         set -e
      else
         set +e
         eval $DW_MASTER_BIN/dw_infra.synch_hadr_node_handler.ksh -i $ETL_ID -e $JOB_ENV -t $JOB_TYPE_ID -l $LOG_FILE -u $BATCH_SEQ_NUM
         HADR_RCODE=$?
         set -e
      fi

      if [ $HADR_RCODE != 0 ]
      then
         print "${0##*/}:  ERROR, see log file $LOG_FILE" >&2
         exit 4
      fi
   else
      #HADR synching is not on
      print "${0##*/}:  INFO, HA DR processing is not enabled."
   fi
   print "$PROCESS" $COMP_FILE
   print "${0##*/}:  INFO, $PROCESS phase complete  `date`"

elif [ $RCODE = 0 ]
then
   print "${0##*/}:  INFO, $PROCESS phase already complete"
else
   exit $RCODE
fi


print "Removing the complete file  `date`"

rm -f $COMP_FILE

print "${0##*/}:  INFO, 
####################################################################################################################
#
# Secure File Transfer Load for ETL_ID: $ETL_ID, BATCH_SEQ_NUM: $BATCH_SEQ_NUM complete   `date`
#
####################################################################################################################"

tcode=0
exit
