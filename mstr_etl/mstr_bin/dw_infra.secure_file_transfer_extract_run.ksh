#!/bin/ksh -eu
# Title:        Secure File Transfer Extract Run
# File Name:    dw_infra.secure_file_transfer_extract_run.ksh
# Description:  Run script - called by handler
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

. $DW_MASTER_LIB/dw_etl_common_functions.lib

COMP_FILE=$DW_SA_TMP/$TABLE_ID.$JOB_TYPE.complete
BATCH_SEQ_NUM_FILE=$DW_SA_DAT/$TABLE_ID.$JOB_TYPE.batch_seq_num.dat
UNIT_OF_WORK_FILE=$DW_SA_DAT/$TABLE_ID.$JOB_TYPE.uow.dat
TABLE_LIS_FILE=$DW_CFG/$ETL_ID.sources.lis

if [[ ! -f $COMP_FILE ]]
then
   # COMP_FILE does not exist.  1st run for this processing period.
   FIRST_RUN=Y
else
   FIRST_RUN=N
fi
export PWD_VAR=$PWD

export FIRST_RUN

#-------------------------------------------------------------------------------------
# Source the error message handling logic.  On failure, trap will send the contents
# of the PARENT_ERROR_FILE to the subject area designated email addresses.
#-------------------------------------------------------------------------------------
. $DW_LIB/message_handler

# get BATCH_SEQ_NUM
if [[ $FIRST_RUN = Y ]]
then
   PREV_BATCH_SEQ_NUM=$(<$BATCH_SEQ_NUM_FILE)
   ((BATCH_SEQ_NUM=PREV_BATCH_SEQ_NUM+1))
   export BATCH_SEQ_NUM
else
   # In case of a restart - Check if BSN file has been incremented
   PROCESS=Increment_BSN
   RCODE=`grepCompFile $PROCESS $COMP_FILE`

   if [[ $RCODE -eq 1 ]]
   then
      PREV_BATCH_SEQ_NUM=$(<$BATCH_SEQ_NUM_FILE)
      ((BATCH_SEQ_NUM=PREV_BATCH_SEQ_NUM+1))
      export BATCH_SEQ_NUM
   elif [[ $RCODE -eq 0 ]]
   then
      export BATCH_SEQ_NUM=$(<$BATCH_SEQ_NUM_FILE)
   else
      exit $RCODE
   fi
fi

assignTagValue USE_GROUP_EXTRACT USE_GROUP_EXTRACT $ETL_CFG_FILE W 0

#read the CFG file to get the IS_RESUBMITTABLE variable
assignTagValue IS_RESUBMITTABLE IS_RESUBMITTABLE $ETL_CFG_FILE W 0

# Print standard environment variables
set +u
print_standard_env
set -u

print "
##########################################################################################################
#
# Beginning extract for ETL_ID: $ETL_ID, BATCH_SEQ_NUM: $BATCH_SEQ_NUM   `date`
#
##########################################################################################################
"

set +e
 grep "^EXTRACT_PROCESS_TYPE\>" $ETL_CFG_FILE | read PARAM EXTRACT_PROCESS_TYPE COMMENT
 rcode=$?
set -e

if [ $rcode != 0 ]
then
    print "${0##*/}:  ERROR, EXTRACT_PROCESS_TYPE not set" >&2
    exit $rcode
fi

if [[ $EXTRACT_PROCESS_TYPE != F ]]
then
    print "${0##*/}:  ERROR, EXTRACT_PROCESS_TYPE may only be F, not $EXTRACT_PROCESS_TYPE" >&2
    exit 4
fi

export EXTRACT_PROCESS_TYPE

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

######################################################################################################
#
#                                Pre extract processing
#
#  Jobs that need scripts to be executed before the extract process has started to create the 
#  sources.lis file for file extracts can be run here.
#
#  To run a pre extract process, set the PRE_EXTRACT_JOBS parameter = 1 in the $ETL_ID.cfg file.
#  This will cause the handler to loop through the file 
#  $DW_CFG/$ETL_ID.pre_extract_jobs.lis 
#  and run any scripts in serial that exist in this lis file.
#
#  Examples of pre extract processes are data file validations or unique trigger file processes.
#
######################################################################################################

assignTagValue PRE_EXTRACT_JOBS PRE_EXTRACT_JOBS $ETL_CFG_FILE W 0

if [ $PRE_EXTRACT_JOBS = 1 ]
then
   print "FATAL ERROR:  Secure File Transfer Does NOT Support Post Extract Jobs." >&2
   print "                Minimal processing allowed for Secure Batch Accounts" >&2
   exit 4
fi


######################################################################################################
# 
#	Setting up extract process specific variables for database and file extraction
#
######################################################################################################

export EXTRACT_PROCESS_MSG=single_scp_extract
export EXTRACT_CONN_TYPE=scp
export EXTRACT_TYPE=scp

 assignTagValue LAST_EXTRACT_TYPE LAST_EXTRACT_TYPE $ETL_CFG_FILE

if [[ $LAST_EXTRACT_TYPE = "V" ]]
then
   if [[ $UOW_FROM_FLAG -eq 1 ]]
   then
      print "${0##*/}: ERROR, Invalid combination: LAST_EXTRACT_TYPE = 'V' and UOW (Unit Of Work) was passed." >&2
      exit 4
   else
      assignTagValue TO_EXTRACT_VALUE_FUNCTION TO_EXTRACT_VALUE_FUNCTION $ETL_CFG_FILE
      export TO_EXTRACT_VALUE=`eval $(eval print $TO_EXTRACT_VALUE_FUNCTION)`
   fi
elif [[ $LAST_EXTRACT_TYPE = "U" ]]
then
   if [[ $UOW_FROM_FLAG -ne 1 ]]
   then
      print "${0##*/}: ERROR, Invalid combination: LAST_EXTRACT_TYPE = 'U' and UOW (Unit Of Work) was not passed." >&2
      exit 4
   else
      print "Executing with Unit Of Work"
   fi
else
   print "${0##*/}: ERROR, Invalid combination: Invalid LAST_EXTRACT_TYPE.  Secure File transfer, may only be V or U." >&2
   exit 4
fi


# check to see if the extract processing has completed yet
RCODE=`grepCompFile $EXTRACT_PROCESS_MSG $COMP_FILE`

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

   LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.$EXTRACT_PROCESS_MSG${UOW_APPEND}.$CURR_DATETIME.log

   ############################################################################################################
   # Single extract
   ############################################################################################################
   if [[ $TABLE_COUNT -eq 1 ]]
   then

     print "Processing single secure file transfer extract for TABLE_ID: $TABLE_ID  `date`"

     if [ $USE_GROUP_EXTRACT -eq 1 ]
     then
       read FILE_ID DBC_FILE PARALLEL_NUM TABLE_NAME DATA_FILENAME GROUP_NUM PARAM_LIST < $TABLE_LIS_FILE
     else
       read FILE_ID DBC_FILE PARALLEL_NUM TABLE_NAME DATA_FILENAME PARAM_LIST < $TABLE_LIS_FILE
     fi

     set +e
     eval $DW_MASTER_BIN/dw_infra.single_secure_file_transfer_extract.ksh $ETL_ID $FILE_ID $DBC_FILE $TABLE_NAME $DATA_FILENAME > $LOG_FILE 2>&1
     rcode=$?
     set -e

     if [ $rcode != 0 ]
     then
       print "${0##*/}:  ERROR, see log file $LOG_FILE" >&2
       exit 4
     fi

   ############################################################################################################
   # Process multiple file transfer extracts in parallel
   ############################################################################################################
   elif [[ $TABLE_COUNT -gt 1 ]]
   then
     print "Processing multiple table extracts for TABLE_ID: $TABLE_ID  `date`"

     export MULTI_COMP_FILE=$DW_SA_TMP/$TABLE_ID.multi_secure_file_transfer_extract.complete
     export PARENT_LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.run_multi_secure_file_transfer_extract${UOW_APPEND}.$CURR_DATETIME.log
     export ERROR_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.secure_file_transfer_extract${UOW_APPEND}.$CURR_DATETIME.err  # job error file

     # If the MULTI_COMP_FILE does not exist, this is the first run, otherwise it is a restart.
     if [ ! -f $MULTI_COMP_FILE ]
     then
	> $MULTI_COMP_FILE
     fi

     # remove previous $EXTRACT_CONN_TYPE list files to ensure looking for the correct set of data files for this run.
     rm -f $DW_SA_TMP/$TABLE_ID.*.$EXTRACT_CONN_TYPE.*.lis

     # Create a list of files to be processed per extract database server.
     if [ $USE_GROUP_EXTRACT -eq 1 ]
     then
        # remove previous sources.lis.X and EXTRACT_CONN_TYPE list files
        rm -f $DW_SA_TMP/$TABLE_ID.*.$EXTRACT_CONN_TYPE.*.lis.*
        rm -f $DW_SA_TMP/$TABLE_ID.sources.lis.*

        while read FILE_ID DBC_FILE PARALLEL_NUM TABLE_NAME DATA_FILENAME GROUP_NUM PARAM_LIST
        do
           print $FILE_ID $DBC_FILE $PARALLEL_NUM $TABLE_NAME $DATA_FILENAME $PARAM_LIST >> $DW_SA_TMP/$TABLE_ID.sources.lis.$GROUP_NUM
        done < $TABLE_LIS_FILE

     else

        while read FILE_ID DBC_FILE PARALLEL_NUM TABLE_NAME DATA_FILENAME PARAM_LIST
        do
           eval DBC_FILE=$DBC_FILE

           # check for restart file, if it exists here (before we start) then simply remove it - no STBY for non-distr_table
           if [ -f $DW_SA_TMP/$ETL_ID.$JOB_TYPE_ID.$DBC_FILE.restart ]
           then
              mv $DW_SA_TMP/$ETL_ID.$JOB_TYPE_ID.$DBC_FILE.restart $DW_SA_TMP/$ETL_ID.$JOB_TYPE_ID.$DBC_FILE.restarted.$(date '+%Y%m%d-%H%M%S')
           fi

           if [ ! -f $DW_SA_TMP/$TABLE_ID.$DBC_FILE.*.lis ]
           then
              eval print $FILE_ID $DBC_FILE $TABLE_NAME $DATA_FILENAME $PARAM_LIST > $DW_SA_TMP/$TABLE_ID.$DBC_FILE.$PARALLEL_NUM.lis
           else
              ls $DW_SA_TMP/$TABLE_ID.$DBC_FILE.*.lis|read DBC_FILE_NAME
              eval print $FILE_ID $DBC_FILE $TABLE_NAME $DATA_FILENAME $PARAM_LIST >> $DBC_FILE_NAME
           fi
        done < $TABLE_LIS_FILE
     fi



     if [ $USE_GROUP_EXTRACT -eq 1 ]
     then
        set -A GROUP_ARRAY $(ls $DW_SA_TMP/$TABLE_ID.sources.lis.*)
        if [ ${#GROUP_ARRAY[*]} -le 0 ]
        then
           print "${0##*/}:  ERROR, USE_GROUP_EXTRACT is 1, but no files matching $DW_SA_TMP/$TABLE_ID.sources.lis.*" >&2
           exit 4
        else
           GROUP_CNT=${#GROUP_ARRAY[*]}
           print "Group extract: Found this many temp sources.lis.X: $GROUP_CNT"
        fi
     else
        GROUP_CNT=1
     fi

     # Loop through Groups
     integer gpcnt=0
     while [ $gpcnt -lt $GROUP_CNT ]
     do
        export GROUP_APPEND=""
        if [ $USE_GROUP_EXTRACT -eq 1 ]
        then
           GROUP_LIS_FILE=${GROUP_ARRAY[$gpcnt]}
           GROUP_APPEND=.${GROUP_LIS_FILE##*.}
           print "Preparing for Group list: $GROUP_LIS_FILE `date`"
           while read FILE_ID DBC_FILE PARALLEL_NUM TABLE_NAME DATA_FILENAME PARAM_LIST
           do
              if [ ! -f $DW_SA_TMP/$TABLE_ID.$DBC_FILE.*.lis${GROUP_APPEND} ]
              then
                 eval print $FILE_ID $DBC_FILE $TABLE_NAME $DATA_FILENAME $PARAM_LIST > $DW_SA_TMP/$TABLE_ID.$DBC_FILE.$PARALLEL_NUM.lis${GROUP_APPEND}
              else
               ls $DW_SA_TMP/$TABLE_ID.$DBC_FILE.*.lis${GROUP_APPEND}|read DBC_FILE_NAME
               eval print $FILE_ID $DBC_FILE $TABLE_NAME $DATA_FILENAME $PARAM_LIST >> $DBC_FILE_NAME
              fi
           done < $GROUP_LIS_FILE
        fi

        integer mpcnt=0
        for FILE in $(ls $DW_SA_TMP/$TABLE_ID.*.$EXTRACT_CONN_TYPE.*.lis${GROUP_APPEND})
        do
           DBC_FILE=${FILE#$DW_SA_TMP/$TABLE_ID.}
           DBC_FILE=${DBC_FILE%%.*}

           LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.$DBC_FILE.run_multi_single_scp_extract${UOW_APPEND}.$CURR_DATETIME.log
           print "Running run_multi_single_extract.ksh for $EXTRACT_TYPE $FILE  `date`"
           COMMAND="$DW_MASTER_BIN/dw_infra.multi_secure_file_transfer_extract.ksh $EXTRACT_TYPE $FILE $LOG_FILE $UOW_PARAM_LIST > $LOG_FILE 2>&1"

           set +e
           #eval $COMMAND || print "${0##*/}: ERROR, failure processing for $FILE, see log file $LOG_FILE" >>$ERROR_FILE &
           eval $COMMAND  &
           MPLIS_PID[mpcnt]=$!
           MPLIS_DBC_FILE[mpcnt]=$DBC_FILE.dbc
           MPLIS_PPID[mpcnt]=$$ # Add by Orlando for track PPID
           set -e

           ((mpcnt+=1))
        done

        # add script here that will check for any restart files for the ETL_ID, and if so - resubmit those
        # the script will end when no other subprocesses are running. 
        # This will run only if the IS_RESUBMITTABLE parameter is set
      
         if [ $IS_RESUBMITTABLE = 1 ]
         then
             $DW_MASTER_BIN/dw_infra.secure_file_transfer_extract_resubmit.ksh  > $DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.secure_file_transfer_extract_resubmit.log "${MPLIS_PID[*]}" "${MPLIS_DBC_FILE[*]}" "${MPLIS_PPID[*]}" 2>&1 &
         fi
         wait

         SUB_ERROR_FILE_LIS="$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.*.run_multi_single_${EXTRACT_TYPE}_extract${UOW_APPEND}.$CURR_DATETIME.err"

         if [ -f $SUB_ERROR_FILE_LIS ]
         then
            if [ -f $ERROR_FILE ]
            then
               cat $SUB_ERROR_FILE_LIS >> $ERROR_FILE
            else
               cat $SUB_ERROR_FILE_LIS > $ERROR_FILE
            fi
         fi

         if [ -f $ERROR_FILE ]
         then
            cat $ERROR_FILE >&2
            exit 4
         fi

         ((gpcnt+=1))
      done # Group loop

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

#
# check to see if the create DW_IN record count file process has completed yet
#
PROCESS="create DW_SA_IN record count file"
RCODE=`grepCompFile "$PROCESS" $COMP_FILE`

if [ $RCODE = 1 ]
then
   print "Creating the record count file  `date`"
	
   # sum contents of individual record count files into a master record count file for the load graph
   integer RECORD_COUNT=0
   integer RECORD_CNT=0

   RC_LIS_FILE=$TABLE_LIS_FILE

   while read FILE_ID ZZZ
   do
         ((RECORD_COUNT+=$(<$DW_SA_TMP/$TABLE_ID.$JOB_TYPE_ID.$FILE_ID.record_count.dat)))
   done < $RC_LIS_FILE

   if [[ -n $UOW_TO ]]
   then
      RECORD_COUNT_FILE=$REC_CNT_IN_DIR/$TABLE_ID.record_count.dat
   else
      RECORD_COUNT_FILE=$REC_CNT_IN_DIR/$TABLE_ID.record_count.dat.$BATCH_SEQ_NUM
   fi
   print $RECORD_COUNT > $RECORD_COUNT_FILE
   print "RECORD_COUNT_FILE $RECORD_COUNT_FILE" > $DW_SA_TMP/$TABLE_ID.$JOB_TYPE_ID.record_count_file.lis

   print "$PROCESS" >> $COMP_FILE
	
elif [ $RCODE -eq 0 ]
then
   print "$PROCESS already complete"
else
   exit $RCODE
fi

######################################################################################################
#
#                                Post extract processing
#
#  Jobs that need scripts to be executed after the extract process has completed can be run here.
#  To run a post extract process, set the POST_EXTRACT_JOBS parameter = 1 in the $ETL_ID.cfg file.
#  This will cause the handler to loop through the file $DW_CFG/$ETL_ID.post_extract_jobs.lis and
#  run any scripts in serial that exist in this lis file.
#
#  Examples of post extract processes are data file validations or unique trigger file processes.
#
######################################################################################################

assignTagValue POST_EXTRACT_JOBS POST_EXTRACT_JOBS $ETL_CFG_FILE W 0

if [ $POST_EXTRACT_JOBS = 1 ]
then
   print "FATAL ERROR:  Secure File Transfer Does NOT Support Post Extract Jobs." >&2
   print "                Minimal processing allowed for Secure Batch Accounts" >&2
   exit 4
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

PROCESS=Finalize_Processing
RCODE=`grepCompFile $PROCESS $COMP_FILE`

if [ $RCODE = 1 ]
then

   WATCH_FILE=$ETL_ID.$JOB_TYPE.$BATCH_SEQ_NUM.done
   LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.touchWatchFile${UOW_APPEND}.$CURR_DATETIME.log
   print "Running $DW_MASTER_BIN/touchWatchFile.ksh $ETL_ID $JOB_TYPE $JOB_ENV $WATCH_FILE $UOW_PARAM_LIST"

   set _e
   $DW_MASTER_BIN/touchWatchFile.ksh $ETL_ID $JOB_TYPE $JOB_ENV $WATCH_FILE $UOW_PARAM_LIST > $LOG_FILE 2>&1
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
   print "$PROCESS already complete"
else
   exit $RCODE
fi

######################################################################################################
#
#                                Run SFT if specified
#
#  This section send files out via SFT if the data file need to be sent.
#
######################################################################################################

assignTagValue RUN_SFT_AFTER_EX RUN_SFT_AFTER_EX $ETL_CFG_FILE W 0
if [ $RUN_SFT_AFTER_EX = 1 ]
then
   print "FATAL ERROR:  Secure File Transfer Does NOT Support Post Extract Jobs." >&2
   print "                Minimal processing allowed for Secure Batch Accounts" >&2
   exit 4
fi     


######################################################################################################
#
#                                Synch Files
#
#  This section determines gets the files to be synced and syncs the to the HA/DR servers.
#  BATCH_SEQ_NUM_FILE has already been updated to the processed BATCH_SEQ_NUM.  To avoid
#  differences on restart, re-read the BATCH_SEQ_NUM_FILE and use its value.
#
######################################################################################################


# extract synch files add on.
if [ $HADR_ACTIVE = 1 ]
then
   # file synching is on 
   PROCESS=HADR_Synch 
   RCODE=`grepCompFile $PROCESS $COMP_FILE`

   # modify logic here to check specifically if mode is ON 
   if [ $RCODE = 1 ]
   then

#      BATCH_SEQ_NUM=$(<$BATCH_SEQ_NUM_FILE)
#      export BATCH_SEQ_NUM

      #export HADR_SYNCH_FILE=$DW_SA_TMP/$ETL_ID.$JOB_ENV.$JOB_TYPE_ID.synch_files.$BATCH_SEQ_NUM.dat
      if [[ -n $UOW_TO ]]
      then
         PCGID=$ETL_ID.$JOB_ENV.$JOB_TYPE_ID.$UOW_TO
      else
         PCGID=$ETL_ID.$JOB_ENV.$JOB_TYPE_ID.$BATCH_SEQ_NUM
      fi
      HADR_DATALIS=$DW_SA_TMP/$PCGID.synch_files.data.dat
      HADR_STATELIS=$DW_SA_TMP/$PCGID.synch_files.state.dat
      HADR_MFSLIS=$DW_SA_TMP/$PCGID.synch_files.mfs.dat

      
      print "${0##*/}:  INFO, HA DR Synch Data File : HADR_DATALIS = $HADR_DATALIS"
      print "${0##*/}:  INFO, HA DR Synch State File : HADR_STATELIS = $HADR_STATELIS"
      print "${0##*/}:  INFO, HA DR Synch MFS File : HADR_MFSLIS = $HADR_MFSLIS"
      
      LOG_FILE=$DW_SA_LOG/$PCGID.hadr${UOW_APPEND}.$CURR_DATETIME.log

      > $HADR_DATALIS
      > $HADR_STATELIS
      > $HADR_MFSLIS

      #get record count value - can vary due to prev logic - so pull here in case of restart
      if [ -f $DW_SA_TMP/$TABLE_ID.$JOB_TYPE_ID.record_count_file.lis ]
      then
         assignTagValue RECORD_COUNT_FILE RECORD_COUNT_FILE $DW_SA_TMP/$TABLE_ID.$JOB_TYPE_ID.record_count_file.lis
      else
         if [[ -n $UOW_TO ]]
         then
            RECORD_COUNT_FILE=$REC_CNT_IN_DIR/$TABLE_ID.record_count.dat
         else
            RECORD_COUNT_FILE=$REC_CNT_IN_DIR/$TABLE_ID.record_count.dat.$BATCH_SEQ_NUM
         fi
      fi
      
      print $RECORD_COUNT_FILE >> $HADR_STATELIS
      print $BATCH_SEQ_NUM_FILE >> $HADR_STATELIS

      #get the list of extracted files
      if [ ${IN_DIR} != ${IN_DIR%mfs*} ]; then
         read FILE_ID DBC_FILE PARALLEL_NUM TABLE_NAME DATA_FILENAME PARAM_LIST < $TABLE_LIS_FILE
         eval DATFILE=$DATA_FILENAME
         if [[ -n $UOW_TO ]]
         then
            print $IN_DIR/.$DATFILE.mfctl > $HADR_MFSLIS
         else
            print $IN_DIR/.$DATFILE.$BATCH_SEQ_NUM.mfctl > $HADR_MFSLIS
         fi
      else
         # determine the file set
          
         if [[ -n $UOW_TO ]]
         then
            if [ -f $IN_DIR/$TABLE_ID.*.dat* ]
            then
               for HADRFN in $IN_DIR/$TABLE_ID.*.dat*
               do
                  if [[ $HADRFN != $RECORD_COUNT_FILE ]]; then
                     print $HADRFN >> $HADR_DATALIS
                  fi
               done
            fi
            while read FILE_ID DBC_FILE PARALLEL_NUM TABLE_NAME DATA_FILENAME PARAM_LIST
            do
               if [ -f $IN_DIR/$DATA_FILENAME* ]
               then
                  for HADRFN in $IN_DIR/$DATA_FILENAME*
                  do
                     if [[ $HADRFN != $RECORD_COUNT_FILE ]]; then
                        print $HADRFN >> $HADR_DATALIS
                     fi
                  done
               fi
            done < $TABLE_LIS_FILE
         else
	    if [ $EXTRACT_PROCESS_TYPE = "T" ]
	    then
	      HADRFN_PATTERN=".*.$BATCH_SEQ_NUM*"
	    else
	      HADRFN_PATTERN=".$BATCH_SEQ_NUM*"
	    fi

            if [ -f $IN_DIR/$TABLE_ID.*.dat${HADRFN_PATTERN} ]
            then
               for HADRFN in $IN_DIR/$TABLE_ID.*.dat${HADRFN_PATTERN}
               do
                  if [[ $HADRFN != $RECORD_COUNT_FILE ]]; then
                     print $HADRFN >> $HADR_DATALIS
                  fi
               done
            fi
            while read FILE_ID DBC_FILE PARALLEL_NUM TABLE_NAME DATA_FILENAME PARAM_LIST
            do
               if [ -f $IN_DIR/$DATA_FILENAME${HADRFN_PATTERN} ]
               then
                  for HADRFN in $IN_DIR/$DATA_FILENAME${HADRFN_PATTERN}
                  do
                     if [[ $HADRFN != $RECORD_COUNT_FILE ]]; then
                        print $HADRFN >> $HADR_DATALIS
                     fi
                  done
               fi
            done < $TABLE_LIS_FILE
         fi

      fi
      
      
      if [[ -n $UOW_TO ]]
      then
         set +e
         eval $DW_MASTER_BIN/dw_infra.synch_hadr_node_handler.ksh -i $ETL_ID -e $JOB_ENV -t $JOB_TYPE_ID -l /dev/null -u $UOW_TO > $LOG_FILE 2>&1
         HADR_RCODE=$?
         set -e
      else
         set +e
         eval $DW_MASTER_BIN/dw_infra.synch_hadr_node_handler.ksh -i $ETL_ID -e $JOB_ENV -t $JOB_TYPE_ID -l /dev/null -u $BATCH_SEQ_NUM > $LOG_FILE 2>&1
         HADR_RCODE=$?
         set -e
      fi

      if [ $HADR_RCODE != 0 ]
      then
         print "${0##*/}:  ERROR, see log file $LOG_FILE" >&2
         exit 4
      fi 

      print "$PROCESS" $COMP_FILE 
      print "$PROCESS phase complete"
   elif [ $RCODE = 0 ]
   then
      print "$PROCESS phase already complete"
   else
      exit $RCODE
   fi
else
   #HADR synching is not on
   print " HA DR processing is not enabled."
fi

if [ -f $DW_SA_TMP/$TABLE_ID.$JOB_TYPE_ID.record_count_file.lis ]
then
   rm -f $DW_SA_TMP/$TABLE_ID.$JOB_TYPE_ID.record_count_file.lis
fi

print "HA DR synching complete `date`"

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
