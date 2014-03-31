#!/bin/ksh -eu

#------------------------------------------------------------------------------------------------
# Filename:     single_transform_run.ksh
# Description:  Basic wrapper for customized transform scripts. Provides logging and error handling,
#                      cleanup and process control.
# Developer:    Jacky Shen
# Created on:   20/05/2011
# Location:     $DW_MASTER_BIN/
#
# Execution:    $DW_MASTER_BIN/single_transform_handler.ksh -i <ETL_ID>  -f <UOW_FROM> -t <UOW_TO> -s <SCRIPT_NAME> -p "<Param1> <Param2> <Param3> ... <ParamX>"
#
# Parameters:   ETL_ID = <SUBJECT_AREA.TABLE_ID>
#                       SHELL_EXE = <shell executable>
#                       Param[1-X] = <parameters for shell executable>
#
# Revision History:
#
# Name             Date            Description
# ---------------  --------------  ---------------------------------------------------
# Jacky Shen  11/10/2011     Split main code to single_table_load_run.ksh
#                                            Allow use of time and a redirect for log
# Ryan Wong   23/01/2012     Fixed end if (fi), which was commented out
# Ryan Wong   04/19/2013           Adding UNIT_OF_WORK_FILE for cleanup
# Ryan Wong   10/04/2013     Redhat changes
####################################################################################################

. $DW_MASTER_LIB/dw_etl_common_functions.lib

COMP_FILE=$DW_SA_TMP/$TABLE_ID.$JOB_TYPE.complete
export BATCH_SEQ_NUM_FILE=$DW_SA_DAT/$TABLE_ID.$JOB_TYPE.batch_seq_num.dat
UNIT_OF_WORK_FILE=$DW_SA_DAT/$TABLE_ID.$JOB_TYPE.uow.dat

# Source the error message handling logic.  On failure, trap will send the contents of the PARENT_ERROR_FILE to the
# subject area designated email addresses.
. $DW_LIB/message_handler


if [ ! -f $COMP_FILE ]
then
   # COMP_FILE does not exist.  1st run for this processing period.
   FIRST_RUN=Y
else
   FIRST_RUN=N
fi
export PWD_VAR=$PWD

export FIRST_RUN

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
# Beginning transform handler ETL_ID: $ETL_ID, JOB_ID: $JOB_ENV, TRANSFORM: $SHELL_EXE   `date`
#
####################################################################################################################
"


# Clean Up Old data files/logs
if [ $FIRST_RUN = Y ]
then
   # Need to run the clean up process since this is the first run for the current processing period.
   # Need to re-write cleanup process for transform handler since it's hard to cleanup log/data based on etl_id
   #
   #
   
   print "Running loader_cleanup.ksh for JOB_ENV: $JOB_ENV, JOB_TYPE_ID: $JOB_TYPE_ID  `date`"
   LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.${SHELL_EXE_NAME%.ksh}dw_infra.loader_cleanup.${UOW_APPEND}.$CURR_DATETIME.log

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

export PROCESS_MSG=${SHELL_EXE_NAME%.ksh}
RCODE=`grepCompFile $PROCESS_MSG $COMP_FILE`
if [ $RCODE = 1 ]
then
    print "Processing customized transform script : $SHELL_EXE  `date`"
    LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.${SHELL_EXE_NAME%.ksh}${UOW_APPEND}.$CURR_DATETIME.log
    PARAMS=`eval print $_param_list`

    set +e
    $SHELL_EXE $PARAMS > $LOG_FILE 2>&1
    rcode=$?
    set -e
    
    if [ $rcode != 0 ]
    then
    	print "${0##*/}:  ERROR running $SHELL_EXE_NAME, see log file $LOG_FILE" >&2
    	exit $rcode
    else
        print "$PROCESS_MSG" >> $COMP_FILE
    fi
elif [ $RCODE = 0 ]
then
   print "$PROCESS_MSG already complete"
else
   exit $RCODE
fi
# Increment BSN/UOW_ID
# BSN file is binded with etl_id?
if [ X"$BATCH_SEQ_NUM" != X"" ]
  then
    PROCESS=Finalize_Processing
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
    
    #   DW_SA_WATCHFILE=$DW_WATCH/$JOB_ENV/$ETL_ID.$JOB_TYPE.$BATCH_SEQ_NUM.done
    
    #   print "Creating the watch file $ETL_ID.$JOB_TYPE.$BATCH_SEQ_NUM.done  `date`"
    #   > $DW_SA_WATCHFILE
    #
    #   SSH_USER=$(whoami)
    #   if [[ $NODE_NONSPEC = 0 && $DRACTIVE = 1 ]]
    #   then
    #      #on hadr platform - touch file to dr system (which copies it to system in other data center)
    #      ssh $SSH_USER@$DRNODE "touch $DW_SA_WATCHFILE"
    #   fi
    print "$PROCESS" >> $COMP_FILE
    elif [ $RCODE = 0 ]
    then
       print "$PROCESS already complete"
    else
       exit $RCODE
    fi
fi

# Touch watch file
#
PROCESS=touch_watchfile
RCODE=`grepCompFile $PROCESS $COMP_FILE`

if [ $RCODE -eq 1 ]
then

   LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.${SHELL_EXE_NAME%.ksh}${UOW_APPEND}.$PROCESS.$CURR_DATETIME.log
   TFILE_NAME=$TABLE_ID.$JOB_TYPE_ID.${SHELL_EXE_NAME%.ksh}.done

   print "Touching Watchfile $TFILE_NAME$UOW_APPEND"

   set +e
   $DW_MASTER_EXE/touchWatchFile.ksh $ETL_ID $JOB_TYPE $JOB_ENV $TFILE_NAME $UOW_PARAM_LIST > $LOG_FILE 2>&1
   rcode=$?
   set -e

   if [ $rcode -ne 0 ]
   then
      print "${0##*/}:  ERROR, see log file $LOG_FILE" >&2
      exit 4
   fi

   print $PROCESS >> $COMP_FILE

elif [ $RCODE -eq 0 ]
then
   print "$PROCESS already complete"
else
   exit $RCODE
fi 

# HA/DR Synch
# how to get the outputs: if indicating a list in cfg, then go through the list, otherwise looking for the outputs by etl_id
# if on ingest, how to determine UOW_ID, pass from command line or store in a file
#
# 
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
      # Read BSN  again to avoid any confusion
      BATCH_SEQ_NUM=""
      BATCH_SEQ_NUM_FILE=$DW_SA_DAT/$TABLE_ID.$JOB_TYPE.batch_seq_num.dat
      if [ -s $BATCH_SEQ_NUM_FILE ]
        then
           PREV_BATCH_SEQ_NUM=$(<$BATCH_SEQ_NUM_FILE)
           ((BATCH_SEQ_NUM=PREV_BATCH_SEQ_NUM+1))
        else
           BATCH_SEQ_NUM=$UOW_ID
      fi
      export BATCH_SEQ_NUM

      #export HADR_SYNCH_FILE=$DW_SA_TMP/$ETL_ID.$JOB_ENV.$JOB_TYPE_ID.synch_files.$BATCH_SEQ_NUM.dat
      PCGID=$ETL_ID.$JOB_ENV.$JOB_TYPE_ID.$BATCH_SEQ_NUM
      HADR_DATALIS=$DW_SA_TMP/$PCGID.synch_files.data.dat
      HADR_STATELIS=$DW_SA_TMP/$PCGID.synch_files.state.dat
      HADR_MFSLIS=$DW_SA_TMP/$PCGID.synch_files.mfs.dat

      
      print "${0##*/}:  INFO, HA DR Synch Data File : HADR_DATALIS = $HADR_DATALIS"
      print "${0##*/}:  INFO, HA DR Synch State File : HADR_STATELIS = $HADR_STATELIS"
      print "${0##*/}:  INFO, HA DR Synch MFS File : HADR_MFSLIS = $HADR_MFSLIS"
      
      LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.hadr${UOW_APPEND}.$CURR_DATETIME.log

      > $HADR_DATALIS
      > $HADR_STATELIS
      

      #get record count value - can vary due to prev logic - so pull here in case of restart
      #logic to get BSN file/data file
      #
      #
      if [ -f $DW_SA_TMP/$TABLE_ID.$JOB_TYPE_ID.record_count_file.lis ]
      then
         assignTagValue RECORD_COUNT_FILE RECORD_COUNT_FILE $DW_SA_TMP/$TABLE_ID.$JOB_TYPE_ID.record_count_file.lis
      else
         RECORD_COUNT_FILE=$DW_SA_IN/$TABLE_ID.record_count.dat.$BATCH_SEQ_NUM
      fi
      if [ -f $RECORD_COUNT_FILE ]
        then
          print $RECORD_COUNT_FILE >> $HADR_STATELIS
      fi
      if [ -f $BATCH_SEQ_NUM_FILE ]
        then
          print $BATCH_SEQ_NUM_FILE >> $HADR_STATELIS
      fi

      #get the list of extracted files
      assignTagValue IN_DIR IN_DIR $ETL_CFG_FILE W $DW_IN

      if [ ${IN_DIR} != ${IN_DIR%mfs*} ]
      then
          print $IN_DIR/$JOB_ENV/.$ETL_ID.$JOB_TYPE.$BATCH_SEQ_NUM.mfctl > $HADR_MFSLIS
      else
         # determine the file set
          
         if [ -f $IN_DIR/$JOB_ENV/$SUBJECT_AREA/$TABLE_ID.*.dat.$BATCH_SEQ_NUM* ]
         then
            for HADRFN in $IN_DIR/$JOB_ENV/$SUBJECT_AREA/$TABLE_ID.*.dat.$BATCH_SEQ_NUM*
            do
               if [[ $HADRFN != $RECORD_COUNT_FILE ]]; then
                 print $HADRFN >> $HADR_DATALIS
               fi
            done
         fi
         assignTagValue TRANS_ARC_FILENAME TRANS_ARC_FILENAME $DW_CFG/$ETL_ID.cfg W
         if [ X"$TRANS_ARC_FILENAME" != X"" ]
          then
            if [ -f $IN_DIR/$JOB_ENV/$SUBJECT_AREA/$TRANS_ARC_FILENAME.*.dat.$BATCH_SEQ_NUM* ]
              then
                for HADRFN in $IN_DIR/$JOB_ENV/$SUBJECT_AREA/$TRANS_ARC_FILENAME.*.dat.$BATCH_SEQ_NUM*
                  do
                    if [[ $HADRFN != $RECORD_COUNT_FILE ]]; then
                      print $HADRFN >> $HADR_DATALIS
                    fi
                 done
            fi
         fi
         #the sources.lis is not mandatory for customized transform script          
         while read FILE_ID DBC_FILE PARALLEL_NUM TABLE_NAME DATA_FILENAME PARAM_LIST
         do
            if [ -f $IN_DIR/$JOB_ENV/$SUBJECT_AREA/$DATA_FILENAME.$BATCH_SEQ_NUM* ]
            then
               for HADRFN in $IN_DIR/$JOB_ENV/$SUBJECT_AREA/$DATA_FILENAME.$BATCH_SEQ_NUM*
               do
                   if [[ $HADRFN != $RECORD_COUNT_FILE ]]; then
                     print $HADRFN >> $HADR_DATALIS
                   fi
               done
            fi
          done < $TABLE_LIS_FILE

###########################################################################################################
#########################
######################### Assume DEV will provide a output list file for the customized transform
#########################
###########################################################################################################
#         if [ -s $DW_CFG/$ETL_ID.transform.target.lis ]
#           then
#             while read HADRFN
#               do
#                 if [ -f $HADRFN ]
#                  then
#                    print $HADRFN  >> $HADR_DATALIS
#                 fi
#             done < $DW_CFG/$ETL_ID.transform.target.lis
#         fi

      fi
      
      
      set +e
      eval $DW_MASTER_BIN/dw_infra.synch_hadr_node_handler.ksh -i $ETL_ID -e $JOB_ENV -t $JOB_TYPE_ID -l $LOG_FILE -u $BATCH_SEQ_NUM
      HADR_RCODE=$?
      set -e
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
####################################################################################################################
#
# Transform handler for ETL_ID: $ETL_ID, JOB_ID: $JOB_ENV, TRANSFORM: $SHELL_EXE complete   `date`
#
####################################################################################################################
"

tcode=0
exit
