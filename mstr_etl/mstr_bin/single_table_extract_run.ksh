#!/bin/ksh -eu
# Title:        Single Table Extract Run
# File Name:    single_table_extract_run.ksh
# Description:  Handle submiting a single table extract job.  Will also spawn multi-table processing.
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
# 2011-10-10   1.0    Ryan Wong                     Split main code to single_table_extract_run.ksh
#                                                   Allow use of time and a redirect for log
# 2011-12-15   1.1    Ryan Wong                     Standardize IN_DIR and REC_CNT_IN_DIR in handler,
#                                                   propagate these variables
# 2011-12-16   1.2    Ryan Wong                     Fix HADR logic for distr table with mfs
# 2011-12-20   1.3    Ryan Wong                     Change loader_cleanup to use dw_infra.loader_cleanup.ksh
# 2012-09-12   1.4    Ryan Wong                     Removing BSN from UOW type processing
# 2012-09-20   1.5    Ryan Wong                     Adding group table extract (use case, user host split)
# 2012-10-31   1.6    Jacky Shen                    Add abinitio hadoop extract
# 2012-11-11   1.6    Ryan Wong                     Adding TPT functionality
# 2012-12-11   1.7    Ryan Wong                     Group table extract - remove eval when creating grouped tmp sources.lis.X
# 2013-01-04   1.8    George Xiong                  Add Oracle OCI Extract
# 2013-01-05   1.9    Jacky Shen                    Add Ingest Hadoop File Extract
# 2013-02-21   1.10   Ryan Wong                     Fix HADR reported log file on error
# 2013-03-01   1.11   Jacky Shen                    Consolidate multi NHOSTS var to MULTI_HOST
# 2013-04-19   1.12   Ryan Wong                     Adding UNIT_OF_WORK_FILE for cleanup
# 2013-05-30   1.13   Ryan Wong                     Adding UOW cleanup using UNIT_OF_WORK_FILE
# 2013-08-20   1.14   George Xiong                  export DBC_FILE_NAME to resolve wildcard character issue on redhad
# 2013-10-04   1.15   Ryan Wong                     Redhat changes
# 2013-08-20   1.16   George Xiong                  $DW_EXE/$SUBJECT_AREA scirpt support of pre/post jobs
# 2015-09-11   1.17   John Hackley                  Password encryption changes
# 2017-06-06   1.18   Michael Weng                  Load extracted data onto HDFS
# 2017-07-20   1.19   Michael Weng                  Differentiate STE and STT, add UOW onto HDFS path
# 2017-08-28   1.20   Kevin Oaks                    Add UOW PARAM LIST to wget/iwget COMMAND
# 2017-09-14   1.21   Michael Weng                  Support multiple HDFS copy and optional flag for copy failure
# 2017-10-04   1.22   Michael Weng                  Add restart logic for hdfs load
# 2017-10-10   1.23   Michael Weng                  Add support for sp* on loading data to hdfs
# 2017-10-10   1.24   Ryan Wong                     Add optional hdfs copy to support post extract normalize files
# 2017-10-26   1.24   Michael Weng                  Add parallel copy feature from etl to hdfs
# 2018-04-18   1.25   Michael Weng                  Support SA variable overwrite
# 2018-06-12   1.29   Michael Weng                  Update dw_infra.multi_etl_to_hdfs_copy.ksh command line
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

assignTagValue USE_DISTR_TABLE USE_DISTR_TABLE $ETL_CFG_FILE
assignTagValue USE_GROUP_EXTRACT USE_GROUP_EXTRACT $ETL_CFG_FILE W 0

if [ $USE_DISTR_TABLE -eq 1 ]
then

   if [ $USE_GROUP_EXTRACT -eq 1 ]
   then
      print "${0##*/}:  ERROR, Invalid combination - USE_DISTR_TABLE is 1 and USE_GROUP_EXTRACT is 1" >&2
      exit 4
   fi
   assignTagValue DISTR_TABLE_LIS_FILE DISTR_TABLE_LIS_FILE $ETL_CFG_FILE
   assignTagValue USE_DISTR_TABLE_STBY USE_DISTR_TABLE_STBY $ETL_CFG_FILE
fi

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

if [ $FIRST_RUN = Y ]
then
   # Need to run the clean up process since this is the first run for the current processing period.

   assignTagValue EXTRACT_PROCESS_TYPE EXTRACT_PROCESS_TYPE $ETL_CFG_FILE

   if [[ $EXTRACT_PROCESS_TYPE == @(T|INGEST_H|O) ]]
   then
     #----------------------------------------------------------------------------------------------------------
     # Calculate MIN_LOAD_BATCH_SEQ_NUM and MIN_LOAD_UNIT_OF_WORK
     #----------------------------------------------------------------------------------------------------------
     # LOAD_JOB_ENV is a pipe delimited list of all targets in use. A single target is stand alone sans pipe.
     # primary, secondary, all are deprecated in favor of td1, td2, etc... all resolves to td1|td2 until
     # functionality is completely deprecated from the production environment. Determines which load
     # BATCH_SEQ_NUM file(s) to use to determine when data files have been loaded and can be archived.
     #----------------------------------------------------------------------------------------------------------
     set +e
     grep "^LOAD_JOB_ENV\>" $ETL_CFG_FILE | read PARAM LOAD_JOB_ENV COMMENT
     rcode=$?
     set -e

     if [ $rcode != 0 ]
     then
       print "${0##*/}:  FATAL ERROR, failure determining value for LOAD_JOB_ENV parameter from $ETL_CFG_FILE" >&2
       exit 4
     fi

     # Convert legacy dual values to current multi env values
     case $LOAD_JOB_ENV in
             all)   LOAD_JOB_ENV="td1|td2";;
         primary)   LOAD_JOB_ENV=td1;;
       secondary)   LOAD_JOB_ENV=td2;;
     esac

     # Fill job environment array, count elements and initialize loop index to 0
     set -A LOAD_JOB_ENV_ARR `print "$LOAD_JOB_ENV"| awk -F'|' '{for(i=1; i<=NF; i++){printf("%s ", $i)}}'`
     integer ARR_ELEMS=${#LOAD_JOB_ENV_ARR[*]}
     integer idx=0

     # Make sure we have at least one array element
     if ((ARR_ELEMS == 0))
     then
       print "${0##*/}:  FATAL ERROR, invalid value for parameter LOAD_JOB_ENV: ($LOAD_JOB_ENV)" >&2
       exit 4
     fi

     # find minimum load batch sequence number to direct cleanup
     while ((idx < ARR_ELEMS))
     do
       bsn_file=$DW_DAT/${LOAD_JOB_ENV_ARR[idx]}/$SUBJECT_AREA/$TABLE_ID.load.batch_seq_num.dat
       if [ -f $bsn_file ]
       then
         cat $bsn_file | read LOAD_BATCH_SEQ_NUM
       else
         print "${0##*/}:  FATAL ERROR, BATCH SEQUENCE NUMBER FILE $bsn_file does not exist." >&2
         exit 4
       fi

       if ((idx == 0))
       then
         MIN_LOAD_BATCH_SEQ_NUM=$LOAD_BATCH_SEQ_NUM
       elif [ $LOAD_BATCH_SEQ_NUM -lt $MIN_LOAD_BATCH_SEQ_NUM ]
       then
         MIN_LOAD_BATCH_SEQ_NUM=$LOAD_BATCH_SEQ_NUM
       fi

       if [[ "X$UOW_TO" != "X" ]]
       then
         uow_file=$DW_DAT/${LOAD_JOB_ENV_ARR[idx]}/$SUBJECT_AREA/$TABLE_ID.load.uow.dat
         #
         # If uow_file does not exist, default = UOW_TO_DATE - 31
         #
         if [ -f $uow_file ]
         then
           cat $uow_file | read LOAD_UNIT_OF_WORK
           LOAD_UNIT_OF_WORK=$(print $LOAD_UNIT_OF_WORK | cut -c1-8)
         else
           UOW_TO_DATE_TMP=$(print $UOW_TO | cut -c1-8)
           LOAD_UNIT_OF_WORK=$($DW_EXE/add_days $UOW_TO_DATE_TMP -31)
           print "${LOAD_UNIT_OF_WORK}000000" > $uow_file
         fi

         if ((idx == 0))
         then
           MIN_LOAD_UNIT_OF_WORK=$LOAD_UNIT_OF_WORK
         elif [ $LOAD_UNIT_OF_WORK -lt $MIN_LOAD_UNIT_OF_WORK ]
         then
           MIN_LOAD_UNIT_OF_WORK=$LOAD_UNIT_OF_WORK
         fi
       fi

       ((idx+=1))
     done

     print "--------------------------------------------------------------------------------------"
     print "LOAD JOB ENVIRONMENTS for this ETL_ID: ${LOAD_JOB_ENV_ARR[*]}"
     print "minimum LOAD_BATCH_SEQ_NUM = $MIN_LOAD_BATCH_SEQ_NUM"
     if [[ "X$UOW_TO" != "X" ]]
     then
       print "minimum LOAD_UNIT_OF_WORK = $MIN_LOAD_UNIT_OF_WORK"
     fi
     print "--------------------------------------------------------------------------------------"

     if [[ "X$UOW_TO" != "X" ]]
     then
       MULTI_HOST_CLEANUP_APPEND="-b $MIN_LOAD_BATCH_SEQ_NUM -u $MIN_LOAD_UNIT_OF_WORK $UOW_PARAM_LIST"
     else
       MULTI_HOST_CLEANUP_APPEND="-b $MIN_LOAD_BATCH_SEQ_NUM"
     fi

     assignTagValue MULTI_HOST MULTI_HOST $ETL_CFG_FILE W 0
     if [ $MULTI_HOST = 0 ]
     then
       export HOSTS_LIST_FILE=$DW_CFG/$ETL_ID.host.lis
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
       export HOSTS_LIST_FILE=$DW_MASTER_CFG/${servername%%.*}.${EXTRACT_NHOSTS}ways.host.lis
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
   while read SCRIPT PARAMS
   do
      # check to see if the pre extract job has completed yet
      set +e
      grep -s "^$SCRIPT $PARAMS\>" $COMP_FILE >/dev/null
      RCODE=$?
      set -e

      if [ $RCODE = 1 ]
      then
         print "Running Pre $SCRIPT $PARAMS `date`"
         LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.${SCRIPT%.*}${UOW_APPEND}.$CURR_DATETIME.log

         set +e
                 if [ -f $DW_EXE/$SUBJECT_AREA/$SCRIPT ]
                 then
                    eval $DW_EXE/$SUBJECT_AREA/$SCRIPT $PARAMS > $LOG_FILE 2>&1
                 else    
                    eval $DW_EXE/$SCRIPT $PARAMS > $LOG_FILE 2>&1
                 fi 
         rcode=$?
         set -e

         if [ $rcode != 0 ]
         then
            print "${0##*/}:  ERROR, see log file $LOG_FILE" >&2
            exit 4
         fi

         print "$SCRIPT $PARAMS" >> $COMP_FILE

      elif [ $RCODE = 0 ]
      then
         print "Pre extract $SCRIPT $PARAMS process already complete"

      else
         exit $RCODE
      fi

   done < $DW_CFG/$ETL_ID.pre_extract_jobs.lis
fi


######################################################################################################
# 
#	Setting up extract process specific variables for database and file extraction
#
######################################################################################################

assignTagValue EXTRACT_PROCESS_TYPE EXTRACT_PROCESS_TYPE $ETL_CFG_FILE

if [ $EXTRACT_PROCESS_TYPE = "D" ]
then
   export EXTRACT_PROCESS_MSG=single_table_extract
   export EXTRACT_CONN_TYPE=dbc
   export EXTRACT_TYPE=table
elif [ $EXTRACT_PROCESS_TYPE = "F" ]
then
   export EXTRACT_PROCESS_MSG=single_scp_extract
   export EXTRACT_CONN_TYPE=scp
   export EXTRACT_TYPE=scp
elif [ $EXTRACT_PROCESS_TYPE = "I" ]
then
   export EXTRACT_PROCESS_MSG=single_wget_internal_extract
   export EXTRACT_CONN_TYPE=scp
   export EXTRACT_TYPE=wget_internal
elif [ $EXTRACT_PROCESS_TYPE = "W" ]
then
   export EXTRACT_PROCESS_MSG=single_wget_extract
   export EXTRACT_CONN_TYPE=scp
   export EXTRACT_TYPE=wget
elif [ $EXTRACT_PROCESS_TYPE = "L" ]
then
   export EXTRACT_PROCESS_MSG=local_file_extract
   export EXTRACT_CONN_TYPE=lcl
   export EXTRACT_TYPE=lcl
elif [ $EXTRACT_PROCESS_TYPE = "S" ]
then
   export EXTRACT_PROCESS_MSG=socparc_file_transfer
   export EXTRACT_CONN_TYPE=sft
   export EXTRACT_TYPE=sft
elif [ $EXTRACT_PROCESS_TYPE = "H" ]
then
   export EXTRACT_PROCESS_MSG=single_hdfs_extract
   export EXTRACT_CONN_TYPE=hdfs
   export EXTRACT_TYPE=hdfs
elif [ $EXTRACT_PROCESS_TYPE = "T" ]
then
   export EXTRACT_PROCESS_MSG=tpt_table_extract
   export EXTRACT_CONN_TYPE=dbc
   export EXTRACT_TYPE=tpt
elif [ $EXTRACT_PROCESS_TYPE = "O" ]
then
	export EXTRACT_PROCESS_MSG=oraoci_table_extract
	export EXTRACT_CONN_TYPE=dbc
	export EXTRACT_TYPE=oraoci
elif [ $EXTRACT_PROCESS_TYPE = "INGEST_H" ]
then
   export EXTRACT_PROCESS_MSG=ingest_hdfs_extract
   export EXTRACT_CONN_TYPE=hdfs
   export EXTRACT_TYPE=ingest_hdfs
else                
   print "${0##*/}: ERROR, Invalid EXTRACT_PROCESS_TYPE value from $ETL_CFG_FILE" >&2
   exit 4
fi

if [[ $EXTRACT_PROCESS_TYPE = "F" || $EXTRACT_PROCESS_TYPE = "I" || $EXTRACT_PROCESS_TYPE = "W" || $EXTRACT_PROCESS_TYPE = "L" || $EXTRACT_PROCESS_TYPE = "S" || $EXTRACT_PROCESS_TYPE = "H" || $EXTRACT_PROCESS_TYPE = "T" || $EXTRACT_PROCESS_TYPE = "O" || $EXTRACT_PROCESS_TYPE = "INGEST_H" ]] 
then

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
   elif [[ $LAST_EXTRACT_TYPE = "U" && $UOW_FROM_FLAG -ne 1 ]]
   then
      print "${0##*/}: ERROR, Invalid combination: LAST_EXTRACT_TYPE = 'U' and UOW (Unit Of Work) was not passed." >&2
      exit 4
   fi

elif [[ $EXTRACT_PROCESS_TYPE = "D" ]]
then

   assignTagValue LAST_EXTRACT_TYPE LAST_EXTRACT_TYPE $ETL_CFG_FILE
   if [[ $LAST_EXTRACT_TYPE = "V" && $UOW_FROM_FLAG -eq 1 ]]
   then
      print "${0##*/}: ERROR, Invalid combination: LAST_EXTRACT_TYPE = 'V' and UOW (Unit Of Work) was passed." >&2
      exit 4
   elif [[ $LAST_EXTRACT_TYPE = "U" && $UOW_FROM_FLAG -ne 1 ]]
   then
      print "${0##*/}: ERROR, Invalid combination: LAST_EXTRACT_TYPE = 'U' and UOW (Unit Of Work) was not passed." >&2
      exit 4
   fi

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
   # if $EXTRACT_PROCESS_TYPE = "L", we assume Single Table Processing

   wc -l $TABLE_LIS_FILE | read TABLE_COUNT FN

   LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.$EXTRACT_PROCESS_MSG${UOW_APPEND}.$CURR_DATETIME.log

   if [ $EXTRACT_PROCESS_TYPE = "L" ]
   then

      if [[ -n $UOW_TO ]]
      then
         if [[ -f $DW_SA_DAT/$ETL_ID.sources.lis.$UOW_TO ]]
         then
            print "sources file $DW_SA_DAT/$ETL_ID.sources.lis.$UOW_TO already exists so using it"
         else
            set +e
            eval $DW_EXE/local_file_extract.ksh $BATCH_SEQ_NUM > $LOG_FILE 2>&1
            rcode=$?
            set -e

            if [ $rcode != 0 ]
            then
               print "${0##*/}:  ERROR, failure determining file set for ETL_ID $ETL_ID, UOW_TO: $UOW_TO " >&2
               print "${0##*/}:  ERROR, see log file $LOG_FILE" >&2
               exit 4
            fi
         fi
      else
         if [[ -f $DW_SA_DAT/$ETL_ID.sources.lis.$BATCH_SEQ_NUM ]]
         then
            print "sources file $DW_SA_DAT/$ETL_ID.sources.lis.$BATCH_SEQ_NUM already exists so using it"
         else
            set +e
            eval $DW_EXE/local_file_extract.ksh $BATCH_SEQ_NUM > $LOG_FILE 2>&1
            rcode=$?
            set -e

            if [ $rcode != 0 ]
            then
               print "${0##*/}:  ERROR, failure determining file set for ETL_ID $ETL_ID, BATCH_SEQ_NUM: $BATCH_SEQ_NUM " >&2
               print "${0##*/}:  ERROR, see log file $LOG_FILE" >&2
               exit 4
            fi
         fi
      fi

   elif [[ $TABLE_COUNT -eq 1 && $USE_DISTR_TABLE -ne 1 ]]
   then

     print "Processing single table extract for TABLE_ID: $TABLE_ID  `date`"

     if [ $USE_GROUP_EXTRACT -eq 1 ]
     then
       read FILE_ID DBC_FILE PARALLEL_NUM TABLE_NAME DATA_FILENAME GROUP_NUM PARAM_LIST < $TABLE_LIS_FILE
     else
       read FILE_ID DBC_FILE PARALLEL_NUM TABLE_NAME DATA_FILENAME PARAM_LIST < $TABLE_LIS_FILE
     fi

     if [ $EXTRACT_PROCESS_TYPE = "D" ] 
     then

       set +e
       eval $DW_EXE/single_table_extract.ksh $ETL_ID $FILE_ID $DBC_FILE $TABLE_NAME $DATA_FILENAME $UOW_PARAM_LIST_AB $PARAM_LIST > $LOG_FILE 2>&1
       rcode=$?
       set -e

     elif [ $EXTRACT_PROCESS_TYPE = "F" ] 
     then

       set +e
       eval $DW_EXE/single_scp_extract.ksh $ETL_ID $FILE_ID $DBC_FILE $TABLE_NAME $DATA_FILENAME > $LOG_FILE 2>&1
       rcode=$?
       set -e

     elif [ $EXTRACT_PROCESS_TYPE = "W" ]
     then
   
   	set +e
   	eval $DW_EXE/single_wget_extract.ksh $ETL_ID $FILE_ID $DBC_FILE $TABLE_NAME $DATA_FILENAME $UOW_PARAM_LIST > $LOG_FILE 2>&1
  	rcode=$?
 	set -e

     elif [ $EXTRACT_PROCESS_TYPE = "I" ]
     then
   
   	set +e
   	eval $DW_EXE/single_wget_internal_extract.ksh $ETL_ID $FILE_ID $DBC_FILE $TABLE_NAME $DATA_FILENAME $UOW_PARAM_LIST > $LOG_FILE 2>&1
  	rcode=$?
 	set -e
     
     elif [ $EXTRACT_PROCESS_TYPE = "S" ]
     then

        set +e
        eval $DW_MASTER_BIN/dw_infra.single_sft_extract.ksh -e $ETL_ID -i $FILE_ID -c $DBC_FILE -s $TABLE_NAME -t $DATA_FILENAME > $LOG_FILE 2>&1
        rcode=$?
        set -e
        
     elif [ $EXTRACT_PROCESS_TYPE = "H" ]
     then

        set +e
        eval $DW_EXE/single_hdfs_extract.ksh $ETL_ID $FILE_ID $DBC_FILE $TABLE_NAME $DATA_FILENAME > $LOG_FILE 2>&1
        rcode=$?
        set -e

     elif [ $EXTRACT_PROCESS_TYPE = "T" ]
     then

       set +e
       eval $DW_EXE/single_tpt_extract.ksh $ETL_ID $FILE_ID $DBC_FILE $TABLE_NAME $DATA_FILENAME $UOW_PARAM_LIST_AB $PARAM_LIST > $LOG_FILE 2>&1
       rcode=$?
       set -e
     
    
    
     elif [ $EXTRACT_PROCESS_TYPE = "O" ]
     then

       set +e   
		   eval $DW_EXE/single_oraoci_extract.ksh $ETL_ID $FILE_ID $DBC_FILE $TABLE_NAME $DATA_FILENAME $UOW_PARAM_LIST_AB $PARAM_LIST > $LOG_FILE 2>&1
       rcode=$?
       set -e
     elif [ $EXTRACT_PROCESS_TYPE = "INGEST_H" ]
     then
       set +e
       eval $DW_EXE/single_ingest_hdfs_extract.ksh $ETL_ID $FILE_ID $DBC_FILE $UOW_PARAM_LIST_AB $PARAM_LIST > $LOG_FILE 2>&1
       rcode=$?
       set -e    
     fi

     if [ $rcode != 0 ]
     then
       print "${0##*/}:  ERROR, see log file $LOG_FILE" >&2
       exit 4
     fi

   elif [[ $TABLE_COUNT -gt 1 || $USE_DISTR_TABLE -eq 1 ]]
   then
     print "Processing multiple table extracts for TABLE_ID: $TABLE_ID  `date`"

     export MULTI_COMP_FILE=$DW_SA_TMP/$TABLE_ID.multi_extract.complete
     export PARENT_LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.run_table_extract${UOW_APPEND}.$CURR_DATETIME.log
     export ERROR_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.single_table_extract${UOW_APPEND}.$CURR_DATETIME.err  # job error file

     # If the MULTI_COMP_FILE does not exist, this is the first run, otherwise it is a restart.
     if [ ! -f $MULTI_COMP_FILE ]
     then
	> $MULTI_COMP_FILE
     fi

     # remove previous $EXTRACT_CONN_TYPE list files to ensure looking for the correct set of data files for this run.
     rm -f $DW_SA_TMP/$TABLE_ID.*.$EXTRACT_CONN_TYPE.*.lis

     # Create a list of files to be processed per extract database server.
     if [ $USE_DISTR_TABLE -eq 1 ]
     then
        if [ $TABLE_COUNT -ne 1 ]
        then
           print "${0##*/}:  ERROR, USE_DISTR_TABLE is 1 but more than 1 row in $TABLE_LIS_FILE" >&2
           exit 4
        fi

	while read FILE_ID DBC_FILE STBY_DBC_FILE
	do
	   PARALLEL_NUM=1
           DISTR_TABLE_STBY=$USE_DISTR_TABLE_STBY
  	   read TABLE_NAME DATA_FILENAME PARAM_LIST < $TABLE_LIS_FILE

           if [ $USE_DISTR_TABLE_STBY -eq 0 ]
           then
              eval STD_FILE=$DBC_FILE
           else
              eval STD_FILE=$STBY_DBC_FILE
           fi

           # check for restart file, if it exists here (before we start) then check contents to override USE_DISTR_TABLE_STBY
           if [ -f $DW_SA_TMP/$ETL_ID.ex.$DBC_FILE.restart ]
           then
              read DISTR_TABLE_STBY < $DW_SA_TMP/$ETL_ID.ex.$DBC_FILE.restart
              mv $DW_SA_TMP/$ETL_ID.ex.$DBC_FILE.restart $DW_SA_TMP/$ETL_ID.ex.$DBC_FILE.restarted.$(date '+%Y%m%d-%H%M%S')
           fi

           if [ $DISTR_TABLE_STBY -eq 0 ]
           then
              eval DBC_FILE=$DBC_FILE
           else
              eval DBC_FILE=$STBY_DBC_FILE
           fi

           if [ ! -f $DW_SA_TMP/$TABLE_ID.$DBC_FILE.*.lis ]
           then
              eval print $FILE_ID $DBC_FILE $TABLE_NAME $DATA_FILENAME $PARAM_LIST > $DW_SA_TMP/$TABLE_ID.$DBC_FILE.$PARALLEL_NUM.lis
           else
              ls $DW_SA_TMP/$TABLE_ID.$DBC_FILE.*.lis|read DBC_FILE_NAME
              eval print $FILE_ID $DBC_FILE $TABLE_NAME $DATA_FILENAME $PARAM_LIST >> $DBC_FILE_NAME	
           fi
        done < $DW_CFG/$DISTR_TABLE_LIS_FILE.sources.lis
     
     
     elif [ $USE_GROUP_EXTRACT -eq 1 ]
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
           if [ -f $DW_SA_TMP/$ETL_ID.ex.$DBC_FILE.restart ]
           then
              mv $DW_SA_TMP/$ETL_ID.ex.$DBC_FILE.restart $DW_SA_TMP/$ETL_ID.ex.$DBC_FILE.restarted.$(date '+%Y%m%d-%H%M%S')
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

           if [ $EXTRACT_PROCESS_TYPE = "D" ]
           then
              LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.$DBC_FILE.run_multi_single_table_extract${UOW_APPEND}.$CURR_DATETIME.log
              print "Running run_multi_single_extract.ksh for $EXTRACT_TYPE $FILE  `date`"
              COMMAND="$DW_EXE/run_multi_single_extract.ksh $EXTRACT_TYPE $FILE $LOG_FILE $UOW_PARAM_LIST > $LOG_FILE 2>&1"
           elif [ $EXTRACT_PROCESS_TYPE = "F" ] 
           then
              LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.$DBC_FILE.run_multi_single_scp_extract${UOW_APPEND}.$CURR_DATETIME.log
              print "Running run_multi_single_extract.ksh for $EXTRACT_TYPE $FILE  `date`"
              COMMAND="$DW_EXE/run_multi_single_extract.ksh $EXTRACT_TYPE $FILE $LOG_FILE $UOW_PARAM_LIST > $LOG_FILE 2>&1"
           elif [ $EXTRACT_PROCESS_TYPE = "W" ]
           then
              LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.$DBC_FILE.run_multi_single_wget_extract${UOW_APPEND}.$CURR_DATETIME.log
              print "Running run_multi_single_extract.ksh for $EXTRACT_TYPE $FILE  `date`"
              COMMAND="$DW_EXE/run_multi_single_extract.ksh $EXTRACT_TYPE $FILE $LOG_FILE $UOW_PARAM_LIST > $LOG_FILE 2>&1"
           elif [ $EXTRACT_PROCESS_TYPE = "I" ]
           then
              LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.$DBC_FILE.run_multi_single_wget_internal_extract${UOW_APPEND}.$CURR_DATETIME.log
              print "Running run_multi_single_internal_extract.ksh for $EXTRACT_TYPE $FILE  `date`"
              COMMAND="$DW_EXE/run_multi_single_extract.ksh $EXTRACT_TYPE $FILE $LOG_FILE $UOW_PARAM_LIST > $LOG_FILE 2>&1"
           elif [ $EXTRACT_PROCESS_TYPE = "S" ]
           then
              LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.$DBC_FILE.run_multi_single_sft_extract${UOW_APPEND}.$CURR_DATETIME.log
              print "Running run_multi_single_extract.ksh for $EXTRACT_TYPE $FILE  `date`"
              COMMAND="$DW_MASTER_BIN/dw_infra.run_multi_sft_extract.ksh -c $DBC_FILE -f $FILE -l $LOG_FILE > $LOG_FILE 2>&1"
           elif [ $EXTRACT_PROCESS_TYPE = "H" ]
           then
              LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.$DBC_FILE.run_multi_single_hdfs_extract${UOW_APPEND}.$CURR_DATETIME.log
              print "Running run_multi_single_extract.ksh for $EXTRACT_TYPE $FILE  `date`"
              COMMAND="$DW_EXE/run_multi_single_extract.ksh $EXTRACT_TYPE $FILE $LOG_FILE $UOW_PARAM_LIST > $LOG_FILE 2>&1"
           elif [ $EXTRACT_PROCESS_TYPE = "T" ]
           then
              LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.$DBC_FILE.run_multi_single_tpt_extract${UOW_APPEND}.$CURR_DATETIME.log
              print "Running run_multi_single_extract.ksh for $EXTRACT_TYPE $FILE  `date`"
              COMMAND="$DW_EXE/run_multi_single_extract.ksh $EXTRACT_TYPE $FILE $LOG_FILE $UOW_PARAM_LIST > $LOG_FILE 2>&1"

	   elif [ $EXTRACT_PROCESS_TYPE = "O" ]
           then
             LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.$DBC_FILE.run_multi_single_oraoci_extract${UOW_APPEND}.$CURR_DATETIME.log
              print "Running run_multi_single_oraoci_extract.ksh for $EXTRACT_TYPE $FILE  `date`"
              COMMAND="$DW_EXE/run_multi_single_extract.ksh $EXTRACT_TYPE $FILE $LOG_FILE $UOW_PARAM_LIST > $LOG_FILE 2>&1"
              print $COMMAND
           elif [ $EXTRACT_PROCESS_TYPE = "INGEST_H" ]
           then
              LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.$DBC_FILE.run_multi_single_ingest_hdfs_extract${UOW_APPEND}.$CURR_DATETIME.log
              print "Running run_multi_single_extract.ksh for $EXTRACT_TYPE $FILE  `date`"
              COMMAND="$DW_EXE/run_multi_single_extract.ksh $EXTRACT_TYPE $FILE $LOG_FILE $UOW_PARAM_LIST > $LOG_FILE 2>&1"   
           fi

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
             $DW_EXE/single_table_extract_resubmit_handler.ksh  > $DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.single_table_extract_resubmit.log "${MPLIS_PID[*]}" "${MPLIS_DBC_FILE[*]}" "${MPLIS_PPID[*]}" 2>&1 &
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

   # record count file will already be created by local_file_extract.ksh due to performance issues with large number of files
   if [[ $EXTRACT_PROCESS_TYPE != @(L|INGEST_H) ]]
   then
      if [ $USE_DISTR_TABLE -eq 1 ]
      then
         RC_LIS_FILE=$DW_CFG/$DISTR_TABLE_LIS_FILE.sources.lis
      else
         RC_LIS_FILE=$TABLE_LIS_FILE
      fi

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
   fi

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
   while read SCRIPT PARAMS
   do
      # check to see if the post extract job has completed yet
      set +e
      grep -s "^$SCRIPT $PARAMS\>" $COMP_FILE >/dev/null
      RCODE=$?
      set -e

      if [ $RCODE = 1 ]
      then
         print "Running $SCRIPT  $PARAMS `date`"
         LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.${SCRIPT%.*}${UOW_APPEND}.$CURR_DATETIME.log

         set +e
                 if [ -f $DW_EXE/$SUBJECT_AREA/$SCRIPT ]
                 then
                    eval $DW_EXE/$SUBJECT_AREA/$SCRIPT $PARAMS > $LOG_FILE 2>&1
                 else    
                    eval $DW_EXE/$SCRIPT $PARAMS > $LOG_FILE 2>&1
                 fi 
         rcode=$?
         set -e

         if [ $rcode != 0 ]
         then
            print "${0##*/}:  ERROR, see log file $LOG_FILE" >&2
            exit 4
         fi

         print "$SCRIPT $PARAMS" >> $COMP_FILE

      elif [ $RCODE = 0 ]
      then
         print "$SCRIPT $PARAMS process already complete"
      else
         exit $RCODE
      fi
   done < $DW_CFG/$ETL_ID.post_extract_jobs.lis
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

   #read the current BSN
#   BATCH_SEQ_NUM=$(<$BATCH_SEQ_NUM_FILE)
#   export BATCH_SEQ_NUM

#   DW_SA_WATCHFILE=$DW_WATCH/$JOB_ENV/$ETL_ID.$JOB_TYPE.$BATCH_SEQ_NUM.done
#
#   Deprecating in favor of calling $DW_MASTER_EXE/touchWatchFile.ksh
#   print "Creating the watch file $ETL_ID.$JOB_TYPE.$BATCH_SEQ_NUM.done  `date`"
#   > $DW_SA_WATCHFILE
#
#   SSH_USER=$(whoami)
#   if [[ ${DWI_RMT_SERVER:-"NA"} != "NA" ]]
#   then
#      #on hadr platform - touch file to dr system (which copies it to system in other data center)
#      ssh $SSH_USER@$DWI_RMT_SERVER "touch $DW_SA_WATCHFILE"
#   fi

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
   PROCESS=Run_Sft_Push_After_Extract
   RCODE=`grepCompFile $PROCESS $COMP_FILE`
   if [ $RCODE = 1 ]
   then
      print "Run SFT Push after Extract"
      assignTagValue SFT_PUSH_CONN SFT_PUSH_CONN $ETL_CFG_FILE
      assignTagValue SFT_PUSH_NWAYS SFT_PUSH_NWAYS $ETL_CFG_FILE W 1
      assignTagValue SFT_BANDWIDTH SFT_BANDWIDTH $ETL_CFG_FILE W 32

      DWI_fetch_pw $ETL_ID sft $SFT_PUSH_CONN
      DWIrc=$?

      if [[ -z $SFT_PASSWORD ]]
      then
        print "Unable to retrieve SFT password, exiting; ETL_ID=$ETL_ID; SFT_PUSH_CONN=$SFT_PUSH_CONN"
        exit $DWIrc
      fi

#      BATCH_SEQ_NUM=$(<$BATCH_SEQ_NUM_FILE)
#      export BATCH_SEQ_NUM
      if [[ -n $UOW_TO ]]
      then
         SFILE=$DW_SA_TMP/$TABLE_ID.$SFT_PUSH_CONN.run_multi_sft_push.lis.$UOW_TO
         XFILE=$DW_SA_TMP/$TABLE_ID.$SFT_PUSH_CONN.runmulti_sft_push.xpt.$UOW_TO
      else
         SFILE=$DW_SA_TMP/$TABLE_ID.$SFT_PUSH_CONN.run_multi_sft_push.lis.$BATCH_SEQ_NUM
         XFILE=$DW_SA_TMP/$TABLE_ID.$SFT_PUSH_CONN.runmulti_sft_push.xpt.$BATCH_SEQ_NUM
      fi
      
      >$SFILE

     if [ -s $XFILE ]
     then
        mv $XFILE $SFILE
     else
        if [[ -n $UOW_TO ]]
        then
           if [ -f $IN_DIR/$TABLE_ID.*.dat* ]
           then
              for FN in $IN_DIR/$TABLE_ID.*.dat*
              do
                 if [ ! -f $SFILE ]
                 then
                    eval print $SFT_HOST:$REMOTE_DIR/${FN##*/},$FN > $SFILE
                 else
                    eval print $SFT_HOST:$REMOTE_DIR/${FN##*/},$FN >> $SFILE
                 fi
              done
           fi
        else
           if [ -f $IN_DIR/$TABLE_ID.*.dat.$BATCH_SEQ_NUM* ]
           then
              for FN in $IN_DIR/$TABLE_ID.*.dat.$BATCH_SEQ_NUM*
              do
                 if [ ! -f $SFILE ]
                 then
                    eval print $SFT_HOST:$REMOTE_DIR/${FN##*/},$FN > $SFILE
                 else
                    eval print $SFT_HOST:$REMOTE_DIR/${FN##*/},$FN >> $SFILE   
                 fi
              done
           fi
        fi
     fi
     set +e
     sg_file_xfr_client -d 2 -f $SFILE -x $XFILE -l /dev/null -p $RMT_SFT_PORT -n $SFT_PUSH_NWAYS -b $SFT_BANDWIDTH
     rcode=$?
     set -e
     
     if [ $rcode != 0 ]
     then
       print "${0##*/}:  ERROR, see log file $PARENT_LOG_FILE" >&2
       exit $rcode
     elif [ -s $XFILE ]
     then
       print "${0##*/}:  ERROR, there are files rejected, see exception file $XFILE" >&2
       exit 4
     else
       rm -f $XFILE
       rm -f $SFILE
       print "$PROCESS" >> $COMP_FILE
       print "SFT Push after Extract has been finished"
     fi
   elif [ $RCODE = 0 ]
   then
      print "$PROCESS already complete"
   else
      exit $RCODE
   fi
fi     


#########################################################################################################
#
#                                Synch Files
#
#  This section calls a script that backs up extracted files to shared storage if DR_ACTIVE is set to 1.
#  Only works when  UOW is in use.
#
##########################################################################################################

# extract synch files add on
if [[ $DR_ACTIVE = 1 && -n $UOW_TO ]]
then
   # file synching is on
   PROCESS=Local_DR_Synch
   RCODE=`grepCompFile $PROCESS $COMP_FILE`
   LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.dw_infra.single_table_extract.local_to_dr${UOW_APPEND}.$CURR_DATETIME.log

   if [ $RCODE = 1 ]
   then
      print "Executing DR Process"
     set +e
     $DW_MASTER_EXE/dw_infra.single_table_extract.local_to_dr.ksh $SUBJECT_AREA $TABLE_ID $IN_DIR $DW_DR_BASE $DW_SA_LOG $UOW_APPEND $CURR_DATETIME > $LOG_FILE 2>&1
     rcode=$?
     set -e

     if [ $rcode -ne 0 ]
     then
        print "${0##*/}:  ERROR, see log file $LOG_FILE"
        print "Sending email to dw_infra SAE"
        email_subject="$servername: INFO: DR Recovery Copy Failed"
        email_body="DR Recovery Copy Failed. See log file $LOG_FILE"
        grep "^dw_infra\>" $DW_CFG/subject_area_email_list.dat | read PARAM EMAIL_ERR_GROUP
        print $email_body | mailx -s "$email_subject" $EMAIL_ERR_GROUP
     fi
   elif [ $RCODE = 0 ]
   then
      print "$PROCESS phase already complete"
   else
      exit $RCODE
   fi
else
   #DR sync is not on
   print "Copy to DR is not enabled."
fi

if [ -f $DW_SA_TMP/$TABLE_ID.$JOB_TYPE_ID.record_count_file.lis ]
then
   rm -f $DW_SA_TMP/$TABLE_ID.$JOB_TYPE_ID.record_count_file.lis
fi

print "HA DR synching complete `date`"


print "
##########################################################################################################
#
# Extract for ETL_ID: $ETL_ID, BATCH_SEQ_NUM: $BATCH_SEQ_NUM complete   `date`
#
##########################################################################################################"


######################################################################################################
#
#                                ADPO: copy data to HDFS
#
#  This section is specific for ADPO. If the parameter STE_STAGE_TARGET in $DW_CFG/$ETL_ID.cfg is set
#  to [hd1|hd2|hd3|...], data files from extracting job will be also loaded into HDFS.
#
#    STE_STAGE_TARGET        # used to determine whether and which hadoop cluster to load
#    STE_STAGE_PATH          # STE hdfs path
#
#  Set STE_STAGE_TABLE to copy extract files
#  Destination HDFS path will be:
#  $STE_STAGE_PATH/$STE_SA/$STE_STAGE_TABLE/$UOW_TO_DATE/$UOW_TO_HH/$UOW_TO_MI/$UOW_TO_SS
#
#    STE_STAGE_TABLE         # the stage table
#
#  Set STE_STAGE_TABLE_NORMAL and STE_STAGE_ETL_ID_NORMAL to copy post-extract normalize files (single_field_normalize.ksh)
#  Destination HDFS path will be:
#  $STE_STAGE_PATH/$STE_SA_NORMAL/$STE_STAGE_TABLE_NORMAL/$UOW_TO_DATE/$UOW_TO_HH/$UOW_TO_MI/$UOW_TO_SS
#
#    STE_STAGE_TABLE_NORMAL  # the stage table for normalized data
#    STE_STAGE_ETL_ID_NORMAL # use this ETL_ID to locate the normalized folders
#
######################################################################################################
assignTagValue STE_STAGE_TARGET STE_STAGE_TARGET $ETL_CFG_FILE W ""

set -eu
if [[ -n ${STE_STAGE_TARGET:-""} ]]
then
  assignTagValue STE_STAGE_PATH STE_STAGE_PATH $ETL_CFG_FILE
  assignTagValue STE_STAGE_TABLE STE_STAGE_TABLE $ETL_CFG_FILE W
  assignTagValue STE_STAGE_TABLE_NORMAL STE_STAGE_TABLE_NORMAL $ETL_CFG_FILE W

  export UOW_TO_FLAG=0
  STE_COPY=0
  if [[ X"$STE_STAGE_TABLE" != X ]]
  then
    assignTagValue STE_SA STE_STAGE_SA $ETL_CFG_FILE W "${SUBJECT_AREA#*_}"
    STE_STAGE_PATH=${STE_STAGE_PATH}/${STE_SA}/${STE_STAGE_TABLE}

    if [[ X"$UOW_TO" != X ]]
    then
      UOW_TO_FLAG=1
      STE_STAGE_PATH=${STE_STAGE_PATH}/$UOW_TO_DATE/$UOW_TO_HH/$UOW_TO_MI/$UOW_TO_SS
    fi

    STE_COPY=1
  fi

  STE_COPY_NORMAL=0
  if [[ X"$STE_STAGE_TABLE_NORMAL" != X ]]
  then
    assignTagValue STE_STAGE_ETL_ID_NORMAL STE_STAGE_ETL_ID_NORMAL $ETL_CFG_FILE
    assignTagValue STE_STAGE_PATH_NORMAL STE_STAGE_PATH $ETL_CFG_FILE
    SUBJECT_AREA_NORMAL=${STE_STAGE_ETL_ID_NORMAL%%.*}
    TABLE_ID_NORMAL=${STE_STAGE_ETL_ID_NORMAL##*.}
    STE_SA_NORMAL=${SUBJECT_AREA_NORMAL#*_}
    STE_STAGE_PATH_NORMAL=${STE_STAGE_PATH_NORMAL}/${STE_SA_NORMAL}/${STE_STAGE_TABLE_NORMAL}

    # Get IN_DIR_NORMAL from STE_STAGE_ETL_ID_NORMAL cfg
    ETL_ID_CFG_NORMAL=$DW_HOME/cfg/$SUBJECT_AREA_NORMAL/${STE_STAGE_ETL_ID_NORMAL}.cfg
    if [ ! -f $ETL_ID_CFG_NORMAL ]
    then
      print "${0##*/}:  FATAL ERROR ETL_ID_CFG_NORMAL does not exist.  $ETL_ID_CFG_NORMAL" >&2
      exit 5
    fi
    assignTagValue IN_DIR_NORMAL IN_DIR $ETL_ID_CFG_NORMAL W $DW_IN
    IN_DIR_NORMAL=$IN_DIR_NORMAL/$JOB_ENV/$SUBJECT_AREA_NORMAL

    if [[ X"$UOW_TO" != X ]]
    then
      UOW_TO_FLAG=1
      STE_STAGE_PATH_NORMAL=${STE_STAGE_PATH_NORMAL}/$UOW_TO_DATE/$UOW_TO_HH/$UOW_TO_MI/$UOW_TO_SS
      IN_DIR_NORMAL=$IN_DIR_NORMAL/$TABLE_ID_NORMAL/$UOW_TO_DATE/$UOW_TO_HH/$UOW_TO_MI/$UOW_TO_SS
    fi

    STE_COPY_NORMAL=1
  fi

  if [[ $STE_COPY == 0 && $STE_COPY_NORMAL == 0 ]]
  then
    print "${0##*/}:  FATAL ERROR, Both STE_STAGE_TABLE and STE_STAGE_TABLE_NORMAL are not set" >&2
    exit 8
  fi

  ### STE_STAGE_TARGET is comma separated for multiple hadoop cluster support. The flag is for user to
  ### specify whether failure copying to hdfs should be ignored. Default is 0 (no ignore, meaning copy
  ### failed makes job failed). For example,
  ### hd1,hd2       - load data onto both hd1 and hd2. Fail the job if either copy failed.
  ### hd1{0},hd2{1} - load data onto both hd1 and hd2. Copy to hd1 failed causes job failed but not hd2
  jobfailed=0
  retcode=0
  for TARGET in $(echo $STE_STAGE_TARGET | sed "s/,/ /g")
  do
    HD_ENV=${TARGET%\{*}
    HD_FLAG=0
    if [[ "$TARGET" = *\{*\} ]]
    then
      HD_FLAG=$(echo $TARGET | cut -d\{ -f2 | cut -d\} -f1)
    fi
    CLUSTER=$(HD_ENV_UPPER=$(print $HD_ENV | tr [:lower:] [:upper:]); eval print \$DW_${HD_ENV_UPPER}_DB)

    STE_STAGE_TARGET_STATUS=0
    if [[ $HD_ENV == @(hd*|sp*) ]] && [[ -f $DW_MASTER_CFG/.${CLUSTER}_env.sh ]]
    then
      STE_COPY_STATUS=0
      if [[ $STE_COPY == 1 ]]
      then
        PROCESS=ste_hdfs_load_${HD_ENV}
        RCODE=`grepCompFile $PROCESS $COMP_FILE`

        if [ $RCODE != 0 ]
        then
          LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.dw_infra.multi_etl_to_hdfs_copy${UOW_APPEND}.$PROCESS.$CURR_DATETIME.log
          print "Copy to HDFS is started."
          print "    Source (${IN_DIR})"
          print "    Destination ($CLUSTER:$STE_STAGE_PATH)"
          print "    Log file: $LOG_FILE"

          set +e
          $DW_MASTER_BIN/dw_infra.multi_etl_to_hdfs_copy.ksh $ETL_ID $CLUSTER $IN_DIR $TABLE_ID $STE_STAGE_PATH $TABLE_ID $UOW_TO_FLAG > $LOG_FILE 2>&1
          retcode=$?
          set -e

          if [ $retcode != 0 ]
          then
            print "WARNING - Copy to HDFS failed: "
            print "    Source (${IN_DIR})"
            print "    Destination ($CLUSTER:$STE_STAGE_PATH)"
            STE_COPY_STATUS=1
          else
            print "##########################################################################################################"
            print "# Loaded data to HDFS for ETL_ID: $ETL_ID, BATCH_SEQ_NUM: $BATCH_SEQ_NUM, complete `date`"
            print "#   HDFS - $CLUSTER: $STE_STAGE_PATH"
            print "##########################################################################################################"
            print "$PROCESS" >> $COMP_FILE
          fi

        else
          print "$PROCESS already complete"
        fi
      fi  ### End of STE_COPY

      STE_COPY_NORMAL_STATUS=0
      if [[ $STE_COPY_NORMAL == 1 ]]
      then
        PROCESS=ste_hdfs_load_normal_${HD_ENV}
        RCODE=`grepCompFile $PROCESS $COMP_FILE`

        if [ $RCODE != 0 ]
        then
          LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.dw_infra.multi_etl_to_hdfs_copy${UOW_APPEND}.$PROCESS.$CURR_DATETIME.log
          print "Copy to HDFS is started."
          print "    Source (${IN_DIR_NORMAL})"
          print "    Destination ($CLUSTER:$STE_STAGE_PATH_NORMAL)"
          print "    Log file: $LOG_FILE"

          set +e
          $DW_MASTER_BIN/dw_infra.multi_etl_to_hdfs_copy.ksh $ETL_ID $CLUSTER $IN_DIR_NORMAL $TABLE_ID_NORMAL $STE_STAGE_PATH_NORMAL $TABLE_ID_NORMAL $UOW_TO_FLAG > $LOG_FILE 2>&1
          retcode=$?
          set -e

          if [ $retcode != 0 ]
          then
            print "WARNING - Copy to HDFS failed: "
            print "    Source (${IN_DIR_NORMAL})"
            print "    Destination ($CLUSTER:$STE_STAGE_PATH_NORMAL)"
            STE_COPY_NORMAL_STATUS=1
          else
            print "##########################################################################################################"
            print "# Loaded data to HDFS for Normal ETL_ID: $STE_STAGE_ETL_ID_NORMAL, BATCH_SEQ_NUM: $BATCH_SEQ_NUM, complete `date`"
            print "#   HDFS - $CLUSTER: $STE_STAGE_PATH_NORMAL"
            print "##########################################################################################################"
            print "$PROCESS" >> $COMP_FILE
          fi

        else
          print "$PROCESS already complete"
        fi
      fi  ### End of STE_COPY_NORMAL

      if [[ $STE_COPY_STATUS == 0 && $STE_COPY_NORMAL_STATUS == 0 ]]
      then
        ### Create Done file for $HD_ENV (hd1 | hd2 | hd3 | ...)
        LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.touchWatchFile${UOW_APPEND}.ste_hdfs_load_success.$CURR_DATETIME.log
        $DW_MASTER_EXE/touchWatchFile.ksh $ETL_ID $JOB_TYPE $HD_ENV ${ETL_ID}.ste_hdfs_load_success.done $UOW_PARAM_LIST > $LOG_FILE 2>&1
      fi

    else
      print "WARNING - invalid STE_STAGE_TARGET value ($TARGET) in $ETL_CFG_FILE"
      STE_STAGE_TARGET_STATUS=1
    fi

    if [[ $STE_STAGE_TARGET_STATUS == 1 || $STE_COPY_STATUS == 1 || $STE_COPY_NORMAL_STATUS == 1 ]]
    then
      if [ $HD_FLAG = 0 ]
      then
        print "${0##*/}: INFRA_ERROR - Failed to load data to HDFS ($HD_ENV)"
        jobfailed=1
      else
        print "WARNING - Failed to load data to HDFS ($HD_ENV)"
      fi
    fi
  done

  if [ $jobfailed != 0 ]
  then
    exit 4
  fi

fi

print "Removing the complete file  `date`"
rm -f $COMP_FILE


tcode=0
exit
