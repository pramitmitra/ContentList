#!/bin/ksh -eu
#------------------------------------------------------------------------------------------------
# Filename:     dw_infra.batch_td_bridge_run.ksh
#
# Revision History:
#
# Date         Ver#   Modified By(Name)            Change and Reason for Change
#---------    -----  ---------------------------  ------------------------------
# 2013-05-23  1.1    George Xiong                    Initial
# 2013-09-08  1.2    George Xiong                    add env for Artemis
# 2013-10-04  1.3    Ryan Wong                       Redhat changes
# 2015-03-20  1.3    Jiankang Liu                    Fix the print: command not found bug in remote server
# 2015-04-22  1.4    Ryan Wong                       Change apollo-cli to use apollo-devour, after decommission
#------------------------------------------------------------------------------------------------

typeset -fu processCommand

function processCommand {

_process=$1
 shift 1
_processCommand=$@
_logFile=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.$ENV_TYPE.$_process${UOW_APPEND}.$CURR_DATETIME.log

  set +e
  eval $_processCommand > $_logFile 2>&1
  _pcrcode=$?
  set -e

  if [ $_pcrcode -ne 0 ]
  then
    print "${0##*/}:  FATAL ERROR running $_processCommand. See log file $_logFile" >&2
  fi

  print $_pcrcode
}

#
###################################################################################################################

. $DW_MASTER_LIB/dw_etl_common_functions.lib


. $DW_MASTER_LIB/message_handler



# Print standard environment variables
set +u
print_standard_env
set -u

print ""



COMP_FILE=$DW_SA_TMP/$TABLE_ID.$JOB_TYPE_ID.$ENV_TYPE.complete
CFG_VAR_LIST=$DW_CFG/$ETL_ID.td_bridge.variables.lis



if [ ! -f $COMP_FILE ]
then
 # COMP_FILE does not exist.  1st run for this processing period.
 IS_RESTART=N
 > $COMP_FILE
else
 IS_RESTART=Y
fi


DW_WATCHFILE=$ETL_ID.$JOB_TYPE.$ENV_TYPE.td_bridge.done

assignTagValue DATABASE_NAME DM_BRIDGE_TD_DATABASE  $ETL_CFG_FILE  
assignTagValue TABLE_NAME DM_BRIDGE_TD_TABLE  $ETL_CFG_FILE  
assignTagValue USE_CFG_VAR_LIS USE_CFG_VAR_LIS  $ETL_CFG_FILE  "W" "0"
 
 
assignTagValue PRE_TD_BRIDGE_JOBS PRE_TD_BRIDGE_JOBS $ETL_CFG_FILE W 0

if [ $PRE_TD_BRIDGE_JOBS = 1 ]
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
         #eval $DW_EXE/$SCRIPT $PARAMS > $LOG_FILE 2>&1
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
         print "Pre td_birdge job $SCRIPT $PARAMS process already complete"

      else
         exit $RCODE
      fi

   done < $DW_CFG/$ETL_ID.pre_td_bridge_jobs.lis
fi

 
 
 
 
 
 
if [ $USE_CFG_VAR_LIS -eq 1 ]
then
  print "Using $CFG_VAR_LIST to instantiate job specific variables."
  cat $CFG_VAR_LIST
  . $CFG_VAR_LIST
fi	 
 
 

if [[ $JOB_ENV = @(td1||td2||td3||td4||td5) ]]
then
  export  TERADATA_SYSTEM=$(JOB_ENV_UPPER=$(print $JOB_ENV | tr "[:lower:]" "[:upper:]"); eval print \$DW_${JOB_ENV_UPPER}_DB)  
  TD_JOB_ENV=$JOB_ENV
  assignTagValue DM_BRIDGE_HADOOP_SYSTEM DM_BRIDGE_HADOOP_SYSTEM $ETL_CFG_FILE "W"
  HADOOP_SYSTEM=$(JOB_ENV_UPPER=$(print $DM_BRIDGE_HADOOP_SYSTEM | tr "[:lower:]" "[:upper:]"); eval print \$DW_${JOB_ENV_UPPER}_DB)
  EXPORT_IMPORT_TYPE=IMPORT
 
  TD_BRIDGE_SQL=$DW_SQL/${ETL_ID}.td_bridge.${HADOOP_SYSTEM}_to_${TERADATA_SYSTEM}.sql
  TD_BRIDGE_DYNAMIC_SQL=$DW_SQL/${ETL_ID}.td_bridge.${HADOOP_SYSTEM}_to_${TERADATA_SYSTEM}.dynamic.sql

  
elif [[ $JOB_ENV = @(hd1||hd2||hd3) ]]
then
  HADOOP_SYSTEM=$(JOB_ENV_UPPER=$(print $JOB_ENV | tr "[:lower:]" "[:upper:]"); eval print \$DW_${JOB_ENV_UPPER}_DB)
  assignTagValue DM_BRIDGE_TD_SYSTEM DM_BRIDGE_TD_SYSTEM $ETL_CFG_FILE "W"
  export TERADATA_SYSTEM=$(JOB_ENV_UPPER=$(print $DM_BRIDGE_TD_SYSTEM | tr "[:lower:]" "[:upper:]"); eval print \$DW_${JOB_ENV_UPPER}_DB)
  TD_JOB_ENV=$DM_BRIDGE_TD_SYSTEM
  EXPORT_IMPORT_TYPE=EXPORT
  TD_BRIDGE_SQL=$DW_SQL/${ETL_ID}.td_bridge.${TERADATA_SYSTEM}_to_${HADOOP_SYSTEM}.sql
  TD_BRIDGE_DYNAMIC_SQL=$DW_SQL/${ETL_ID}.td_bridge.${TERADATA_SYSTEM}_to_${HADOOP_SYSTEM}.dynamic.sql
  
  set +e
  	grep DW_BRIDGE_HD_DATAPATH $ETL_CFG_FILE|read A DATAPATH_CFG
  	eval print $DATAPATH_CFG |read DATAPATH_CFG
  set -e

  DATAPATH=${DATAPATH:-$DATAPATH_CFG}
  
  HADOOP_TARGET_FOLDER=`dirname ${DATAPATH}`
  
  SUCCESS_FILE="${HADOOP_TARGET_FOLDER}/_SUCCESS"
  
  
  if [ $HADOOP_SYSTEM = 'ares' ]  
  then 
  	HADOOP_CLI="ares-cli.vip.ebay.com"
  elif [ $HADOOP_SYSTEM = 'apollo' ]  
  then 
  	HADOOP_CLI="apollo-devour.vip.ebay.com"	
  elif [ $HADOOP_SYSTEM = 'artemis' ]  
  then 
  	HADOOP_CLI="artemis-cli.vip.ebay.com"		
  fi
  
  SSH_USER=$TD_USERNAME
  
else
  print "ony support JOB_ENV:  	td1||td2||td3||td5||hd1||hd2||hd3"  >&2
  exit 4  
fi
	
	

	

	

# Datamove Process
PROCESS=run_td_bridge
grcode=`grepCompFile $PROCESS $COMP_FILE`

if [ $grcode != 0 ]
then
	
	
   if [[ -f $TD_BRIDGE_SQL ]] && [[ -s ${TD_BRIDGE_SQL} ]]
   then
      print "use $TD_BRIDGE_SQL to run TD Bridge job"
   else 
      LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.dw_infra.gen_td_bridge_sql${UOW_APPEND}.$CURR_DATETIME.log
      print "Running dw_infra.gen_td_bridge_sql.ksh $ETL_ID  $JOB_ENV `date`, see log: $LOG_FILE"
     
      set +e	     
       $DW_MASTER_BIN/dw_infra.gen_td_bridge_sql.ksh $ETL_ID  $JOB_ENV  > $LOG_FILE 2>&1
       gencode=$?
      set -e
      
      if [ $gencode != 0 ]
      then
      	  print "${0##*/}:  FATAL ERROR, $TD_BRIDGE_SQL not exist, and Gen dynamic TD Bridge SQL failed, see log: $LOG_FILE" >&2
     	  exit $gencode
      else
          print "Running dw_infra.gen_td_bridge_sql.ksh $ETL_ID  $JOB_ENV  already complete"	  
      fi	   	   
   fi	
	
	



  if [[ $EXPORT_IMPORT_TYPE = "IMPORT" ]]  #check td table  exits and do truncate
  then
  	export UTILITY_LOAD_TABLE_CHECK_LOGFILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.utility_load_table_check.$CURR_DATETIME.log
  	dw_infra.td_bridge_utility_load_table_check.ksh
  	
  elif [[ $EXPORT_IMPORT_TYPE = "EXPORT" ]]  #check hadoop fodler exits and do cleanup
  then
  		CLENA_UP_HADOOP_TARGET_FOLDER_LOG=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.cleanup_hadoop_target_folder.$CURR_DATETIME.log
  		print "Cleaning up Hadoop Target folder $HADOOP_TARGET_FOLDER on $HADOOP_SYSTEM, see log $CLENA_UP_HADOOP_TARGET_FOLDER_LOG"
	 	
		set +e
		   print  "ssh $SSH_USER@$HADOOP_CLI \" .  ~$SSH_USER/.profile;hadoop fs -rm $HADOOP_TARGET_FOLDER/*||echo 0 \""  > $CLENA_UP_HADOOP_TARGET_FOLDER_LOG
		   ssh $SSH_USER@$HADOOP_CLI ". ~$SSH_USER/.profile;hadoop fs -rm $HADOOP_TARGET_FOLDER/*||echo 0"  >> $CLENA_UP_HADOOP_TARGET_FOLDER_LOG
		   rcode=$?
		   print $rcode
		set -e
		
		if [ $rcode != 0 ]
		then
			print "Failed Clean up Hadoop Target folder $HADOOP_TARGET_FOLDER, Please setup auto SSH connection for ssh $TD_USERNAME@$HADOOP_CLI via $DWI_WHOAMI on `hostname`
			and setup .profle in HOME DIR"
			exit 4
		fi
  	
  fi  	
  
  if [[ -f $TD_BRIDGE_SQL ]] && [[ -s ${TD_BRIDGE_SQL} ]]
  then
     RUN_TD_BRIDGE_SQL=`basename $TD_BRIDGE_SQL`
  else 
     RUN_TD_BRIDGE_SQL=`basename $TD_BRIDGE_DYNAMIC_SQL`
  fi
  
  
   
  PROCESS_COMMAND="$DW_MASTER_EXE/dw_infra.runTDSQL.ksh $ETL_ID $TD_JOB_ENV $RUN_TD_BRIDGE_SQL  $UOW_PARAM_LIST"
  print "Executing $PROCESS phase"
  
  #print $PROCESS_COMMAND

  rcode=`processCommand $PROCESS $PROCESS_COMMAND`

  if [ $rcode != 0 ]
  then
    exit $rcode
  else
    print "$PROCESS phase complete"
    print $PROCESS >> $COMP_FILE
  fi
else
  print "$PROCESS already complete"
fi

  
if [[ $EXPORT_IMPORT_TYPE = "EXPORT" ]]
then
	
	# Touch Watchfile
	PROCESS=touch_successfile
	grcode=`grepCompFile $PROCESS $COMP_FILE`
	
 	if [ $grcode != 0 ]
	then
	  

		
	 	print "Touching successfile $SUCCESS_FILE on $HADOOP_SYSTEM"
	 	
		set +e
		 
		   ssh $SSH_USER@$HADOOP_CLI ". ~$SSH_USER/.profile;hadoop fs -touchz $SUCCESS_FILE"
		   rcode=$?
		   print $rcode
		set -e
		
		if [ $rcode != 0 ]
		then
			print "Failed touching file $SUCCESS_FILE on ${HADOOP_SYSTEM}, Please setup auto SSH connection for ssh $TD_USERNAME@$HADOOP_CLI via $DWI_WHOAMI on `hostname`
			and setup .profle in HOME DIR" 
			exit 4
		fi
	else
	  print "$PROCESS already complete"
	fi
		

fi  	
  
  
  

assignTagValue POST_TD_BIRDGE_JOBS POST_TD_BIRDGE_JOBS $ETL_CFG_FILE W 0

if [ $POST_TD_BIRDGE_JOBS = 1 ]
then
   while read SCRIPT PARAMS
   do
      # check to see if the post td bridge job has completed yet
      set +e
      grep -s "^$SCRIPT $PARAMS\>" $COMP_FILE >/dev/null
      RCODE=$?
      set -e

      if [ $RCODE = 1 ]
      then
         print "Running $SCRIPT  $PARAMS `date`"
         LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.${SCRIPT%.*}${UOW_APPEND}.$CURR_DATETIME.log

         
         set +e
         #eval $DW_EXE/$SCRIPT $PARAMS > $LOG_FILE 2>&1
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
   done < $DW_CFG/$ETL_ID.post_td_bridge_jobs.lis
fi



# Touch Watchfile
PROCESS=touch_watch
grcode=`grepCompFile $PROCESS $COMP_FILE`

if [ $grcode != 0 ]
then
  PROCESS_COMMAND="$DW_MASTER_EXE/touchWatchFile.ksh $ETL_ID $JOB_TYPE $JOB_ENV $DW_WATCHFILE $UOW_PARAM_LIST"
  print "Executing $PROCESS phase"

  rcode=`processCommand $PROCESS $PROCESS_COMMAND`

  if [ $rcode != 0 ]
  then
    exit $rcode
  else
    print "$PROCESS phase complete"
    print $PROCESS >> $COMP_FILE
  fi
else
  print "$PROCESS already complete"
fi



print "Removing the complete file"
rm -f $COMP_FILE


 
tcode=0
exit
