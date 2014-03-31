#!/bin/ksh -eu
# Title:        Single Table Load Run
# File Name:    single_table_load_run.ksh
# Description:  Handle submiting a single table load job.
# Developer:    Craig Werre
# Created on:
# Location:     $DW_MASTER_BIN
# Logic:
#
#
# Called by:    UC4/Unix
#
# Date         Ver#   Modified By(Name)            Change and Reason for Change
#---------    -----  ---------------------------  ------------------------------
# 2011-10-10   1.0    Ryan Wong                    Split main code to single_table_load_run.ksh
#                                                  Allow use of time and a redirect for log
# 2011-12-20   1.1    Ryan Wong                    Change loader_cleanup to use dw_infra.loader_cleanup.ksh
#
# 2012-01-10   1.2    George Xiong                 add scp as load type into single_table_load_run.ksh
# 2012-09-12   1.3    Ryan Wong                    Removing BSN from UOW type processing
# 2012-10-31   1.4    Jacky Shen                   add abinitio hdfs load
# 2012-11-11   1.5    Ryan Wong                    Adding TPT functionality
# 2012-12-07   1.6    John Hackley                 Add a step to delete from partitioned work table(s)
# 2013-01-15   1.7    Jacky Shen                   Adding Ingest Hadoop Load
# 2013-03-01   1.8    Jacky Shen                   Consolidate multi NHOSTS var to MULTI_HOST
# 2013-04-19   1.9    Ryan Wong                    Adding UNIT_OF_WORK_FILE for cleanup
# 2013-10-04   1.10   Ryan Wong                    Redhat changes
#
####################################################################################################

. $DW_MASTER_LIB/dw_etl_common_functions.lib


COMP_FILE=$DW_SA_TMP/$TABLE_ID.load.complete
export BATCH_SEQ_NUM_FILE=$DW_SA_DAT/$TABLE_ID.load.batch_seq_num.dat     
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
    LOAD_PROCESS_TYPE="D"
fi


set -A LOAD_PROCESS_TYPE_ARR `echo "$LOAD_PROCESS_TYPE"| awk -F'|' '{for(i=1; i<=NF; i++){printf("%s ", $i)}}'`

if [ ${#LOAD_PROCESS_TYPE_ARR[*]} = 2 ]
then
         if  [[ ${JOB_ENV} == @(hd1||hd2||hd3) ]]
         then
                   if [[ ${LOAD_PROCESS_TYPE_ARR[0]} == @(H||INGEST_H) ]]
                   then
                     LOAD_PROCESS_TYPE=${LOAD_PROCESS_TYPE_ARR[0]} 
                   elif [[ ${LOAD_PROCESS_TYPE_ARR[1]} == @(H||INGEST_H) ]]
                   then
                     LOAD_PROCESS_TYPE=${LOAD_PROCESS_TYPE_ARR[1]}
                   else
                     echo "error! JOB_ENV = ${JOB_ENV} but LOAD_PROCESS_TYPE option is not H or INGEST_H"
                     exit 4
                   fi
         else
                   if [[ ${LOAD_PROCESS_TYPE_ARR[0]} != @(H||INGEST_H) ]]
                   then
                     LOAD_PROCESS_TYPE=${LOAD_PROCESS_TYPE_ARR[0]}
                   elif [[ ${LOAD_PROCESS_TYPE_ARR[1]} != @(H||INGEST_H) ]]
                   then
                     LOAD_PROCESS_TYPE=${LOAD_PROCESS_TYPE_ARR[1]} 
                   else
                     echo "error! JOB_ENV = ${JOB_ENV} but LOAD_PROCESS_TYPE option is H or INGEST_H"
                     exit 4
                   fi
         fi


elif [ ${#LOAD_PROCESS_TYPE_ARR[*]} -gt 2 ]
  then echo "error! not support more than 2 load types, but LOAD_PROCESS_TYPE=$LOAD_PROCESS_TYPE"
       exit 4
fi


export LOAD_PROCESS_TYPE


if [ $FIRST_RUN = Y ]
then
	# Need to run the clean up process since this is the first run for the current processing period.
	if [[ $LOAD_PROCESS_TYPE == @(T|INGEST_H) ]]
	then
	  if [[ -n $UOW_TO ]]
	  then
	    MULTI_HOST_CLEANUP_APPEND=$UOW_PARAM_LIST
	  else
	    MULTI_HOST_CLEANUP_APPEND=""
	  fi

          assignTagValue MULTI_HOST MULTI_HOST $ETL_CFG_FILE W 0
          if [ $MULTI_HOST = 0 ]
          then
            HOSTS_LIST_FILE=$DW_CFG/$ETL_ID.host.lis
            if [ ! -f $HOSTS_LIST_FILE ]
            then
              print "${0##*/}:  FATAL ERROR: MULTI_HOST is zero, and $HOST_LIST_FILE does not exist" >&2
              exit 4
            fi
          elif [[ $MULTI_HOST = 1 ]]
          then
            JOB_RUN_NODE=$servername
          elif [[ $MULTI_HOST = @(2||4||6||8||16||32) ]]
          then
            HOSTS_LIST_FILE=$DW_MASTER_CFG/${servername%%.*}.${MULTI_HOST}ways.host.lis
          else
            print "${0##*/}:  FATAL ERROR: MULTI_HOST not valid value $MULTI_HOST" >&2
            exit 4
          fi

          if [ $MULTI_HOST = 1 ]
          then
	    print "Running local dw_infra.loader_cleanup_multi_host.ksh for JOB_RUN_NODE: $JOB_RUN_NODE ETL_ID: $ETL_ID JOB_ENV: $JOB_ENV, JOB_TYPE_ID: $JOB_TYPE_ID  `date`"
	    LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.dw_infra.loader_cleanup_multi_host.${JOB_RUN_NODE%%.*}${UOW_APPEND}.$CURR_DATETIME.log

	    set +e
	    $DW_MASTER_BIN/dw_infra.loader_cleanup_multi_host.ksh $ETL_ID $JOB_ENV $JOB_TYPE_ID $MULTI_HOST_CLEANUP_APPEND > $LOG_FILE 2>&1
	    rcode=$?
	    set -e

	    if [ $rcode != 0 ]
	    then
	      print "${0##*/}:  ERROR, see log file $LOG_FILE" >&2
	      exit 4
	    fi
          else
	    while read JOB_RUN_NODE junk
	    do
	      print "Running dw_infra.loader_cleanup_multi_host.ksh for JOB_RUN_NODE: $JOB_RUN_NODE ETL_ID: $ETL_ID JOB_ENV: $JOB_ENV, JOB_TYPE_ID: $JOB_TYPE_ID  `date`"
	      LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.dw_infra.loader_cleanup_multi_host.${JOB_RUN_NODE%%.*}${UOW_APPEND}.$CURR_DATETIME.log

	      set +e
	      ssh -nq $JOB_RUN_NODE "$DW_MASTER_BIN/dw_infra.loader_cleanup_multi_host.ksh $ETL_ID $JOB_ENV $JOB_TYPE_ID $MULTI_HOST_CLEANUP_APPEND" > $LOG_FILE 2>&1
	      rcode=$?
	      set -e

	      if [ $rcode != 0 ]
	      then
	        print "${0##*/}:  ERROR, see log file $LOG_FILE" >&2
	        exit 4
	      fi
	    done < $HOSTS_LIST_FILE
          fi
	else
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
	fi

	> $COMP_FILE

else
	print "dw_infra.loader_cleanup.ksh process already complete"
fi



###################################################################################################################
#	Data File Loading process
#	 if LOAD_PROCESS_TYPE = F, load via SCP
#	  else Load via single_table_load.ksh
###################################################################################################################

PROCESS=single_table_load
RCODE=`grepCompFile $PROCESS $COMP_FILE`

if [ $RCODE = 1 ]
then

 		# assignTagValue LOAD_PROCESS_TYPE LOAD_PROCESS_TYPE $ETL_CFG_FILE W "D"		
			
		if [ $LOAD_PROCESS_TYPE = F ]
		then    ##SCP Load ##
			
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
				export MULTI_COMP_FILE=$DW_SA_TMP/$TABLE_ID.multi_scp_push.complete
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
		
				rm -f $MULTI_COMP_FILE
		
			else
				print "${0##*/}:  ERROR, no rows exist in file $TABLE_LIS_FILE" >&2
				exit 4
			fi
			
		elif [ $LOAD_PROCESS_TYPE = H ]
		then
		   print "Processing HDFS load from TABLE_ID: $TABLE_ID `date`"
 
		   LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.single_hdfs_load${UOW_APPEND}.$CURR_DATETIME.log
		   set +e
		   $DW_EXE/single_hdfs_load.ksh $ETL_ID $JOB_ENV $INPUT_DML $UOW_PARAM_LIST_AB > $LOG_FILE 2>&1
		   rcode=$?
		   set -e
		   
		   if [ $rcode != 0 ]
		   then
		      print "${0##*/}:  ERROR, see log file $LOG_FILE" >&2
		      exit 4
		   fi
              
		elif [ $LOAD_PROCESS_TYPE = T ]
		then
			##Single TPT Load ##
			print "Processing single tpt load for TABLE_ID: $TABLE_ID  `date`"

			LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.single_tpt_load${UOW_APPEND}.$CURR_DATETIME.log

			set +e
			$DW_EXE/single_tpt_load.ksh $ETL_ID $JOB_ENV $UOW_PARAM_LIST_AB > $LOG_FILE 2>&1
			rcode=$?
			set -e
		
			if [ $rcode != 0 ]
			then
				print "${0##*/}:  ERROR, see log file $LOG_FILE" >&2
				exit 4
			fi

                elif [ $LOAD_PROCESS_TYPE = "INGEST_H" ]
                then
                        print "Processing single Ingest HDFS load for TABLE_ID: $TABLE_ID  `date`"

                        LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.single_ingest_hdfs_load${UOW_APPEND}.$CURR_DATETIME.log

                        set +e
                        $DW_MASTER_EXE/dw_infra.single_ingest_hdfs_load.ksh $ETL_ID $JOB_ENV $UOW_PARAM_LIST_AB > $LOG_FILE 2>&1
                        rcode=$?
                        set -e

                        if [ $rcode != 0 ]
                        then
                                print "${0##*/}:  ERROR, see log file $LOG_FILE" >&2
                                exit 4
                        fi  

               
                else        
		
			##Single Table Load ##
			print "Processing single table load for TABLE_ID: $TABLE_ID  `date`"
		
			LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.single_table_load${UOW_APPEND}.$CURR_DATETIME.log
			UTILITY_TABLE_LOGFILE=$DW_SA_LOG/$TABLE_ID.ld.utility_load${UOW_APPEND}.$CURR_DATETIME.log
		
			set +e
			$DW_EXE/single_table_load.ksh $ETL_ID $JOB_ENV $INPUT_DML $UOW_PARAM_LIST_AB > $LOG_FILE 2>&1
			rcode=$?
			set -e
		
			if [ $rcode != 0 ]
			then
				print "${0##*/}:  ERROR, see log file $LOG_FILE" >&2
				exit 4
			else
				set +e
				grep -s "Please restart this graph" $UTILITY_TABLE_LOGFILE >/dev/null
				RCODE=$?
				set -e
		
				if [ $RCODE = 0 ]
				then
					print "${0##*/}:  ERROR, see log file $LOG_FILE & utility log file $UTILITY_TABLE_LOGFILE" >&2
					exit 4
				fi
			fi
		
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
# Single table load for ETL_ID: $ETL_ID, BATCH_SEQ_NUM: $BATCH_SEQ_NUM complete   `date`
#
####################################################################################################################"

tcode=0
exit
