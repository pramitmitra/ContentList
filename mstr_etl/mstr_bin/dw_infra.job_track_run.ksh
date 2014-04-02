#!/bin/ksh -eu
#------------------------------------------------------------------------------------------------
# Title:        dw_infra.job_track_run.ksh
# File Name:    dw_infra.job_track_run.ksh
# Description:  parse and format ETL job log on $DW_MASTER_LOG/jobtrack/land
# Developer:    George Xiong
# Created on:
# Location:     $DW_MASTER_BIN
#
#
# Parameters:   none
#
# Revision History:
#
# Name             Date            Description
# ---------------  --------------  ---------------------------------------------------
# George Xiong     10/10/2011      Initial Creation
# Ryan Wong        10/04/2013      Redhat changes
#------------------------------------------------------------------------------------------------

 
. $DW_MASTER_LIB/dw_etl_common_functions.lib

 

#-------------------------------------------------------------------------------------
# Source the error message handling logic.  On failure, trap will send the contents
# of the PARENT_ERROR_FILE to the subject area designated email addresses.
#-------------------------------------------------------------------------------------
 . $DW_LIB/message_handler

# Print standard environment variables
set +u
print_standard_env
set -u  	
  	
  	

SHELL_EXE_NAME=${0##*/}
CURRENT_DATE=$(date '+%Y%m%d')
CURRENT_HOUR=$(date '+%H')
LOG_BASE=$DW_MASTER_LOG/jobtrack
LOG_LAND=$LOG_BASE/land
LOG_INPROGRESS=$LOG_BASE/inprogress
LOG_R4USE=$LOG_BASE/r4use
LOG_R4CP=$LOG_BASE/r4cp
LOG_COLUMNS="DWI_CALLED  DWI_CALLED_ARGS  DWI_WHOAMI  DWI_START_DATETIME  servername  TABLE_ID  ETL_ID  SUBJECT_AREA  BATCH_SEQ_NUM  UOW_FROM  UOW_TO  UC4_JOB_NAME  UC4_PRNT_CNTR_NAME  UC4_TOP_LVL_CNTR_NAME  UC4_JOB_RUN_ID  CURR_DATETIME  DWI_END_DATETIME"
 
 
 
#--------------------------------------------------------------------------------------
# Determine if there is already an job track process running
#-------------------------------------------------------------------------------------- 
 
while [ $(/usr/ucb/ps -auxwwwl | grep "$SHELL_EXE_NAME" | grep -v "shell_handler.ksh"|  grep -v "grep $SHELL_EXE_NAME"| wc -l) -ge 2 ]
do
   print "There is already a job track process running. Sleeping for 30 seconds"
   sleep 30
   continue
done

print "####################################################################################"
print "#"
print "# Beginning jobtrack process at `date`"
print "#"
print "####################################################################################"
print ""  


mkdirifnotexist $LOG_R4CP
mkdirifnotexist $LOG_R4USE
 
for LOGDATE in ` ls -F $LOG_LAND/| grep "/" | sed 's/.$//'|sort `
do 
 
  
 
print "# Beginning jobtrack process for    ${LOGDATE}"
 
  
  mkdirifnotexist $LOG_INPROGRESS/${LOGDATE}
  mkdirifnotexist $LOG_R4USE/${LOGDATE}
  

  
  
  for LOGHOUR in ` ls -F $LOG_LAND/${LOGDATE}| grep "/" | sed 's/.$//'|sort `
		  do  	
			  	if [[ ${LOGDATE} -lt ${CURRENT_DATE} ||(${LOGDATE} -eq ${CURRENT_DATE} && ${LOGHOUR} -lt ${CURRENT_HOUR})]]
			  	then 
			 	 
				  print "# 	Beginning jobtrack process for  ${LOGDATE}${LOGHOUR}"
			    	  
				  if   [ -f $LOG_INPROGRESS/${LOGDATE}/jobtrack.processed.${LOGHOUR}.logfile  ]
					then
					    rm -f $LOG_INPROGRESS/${LOGDATE}/jobtrack.processed.${LOGHOUR}.logfile 
				  fi

			   	  touch $LOG_INPROGRESS/${LOGDATE}/jobtrack.processed.${LOGHOUR}.logfile
			 
			   	  for LOGFILE in `ls $LOG_LAND/${LOGDATE}/${LOGHOUR}|sort `
				   	  do
	   	  	  			   ROW_VALUES=$(date '+%Y%m%d-%H%M%S')
			 
				 		  
				 		  
				 		   set +e      
##                                                    grep INFRA_LINE $LOG_LAND/${LOGDATE}/${LOGHOUR}/${LOGFILE}|awk  '{print $2}'|read COLUMN_VALUE 
						      grep INFRA_LINE $LOG_LAND/${LOGDATE}/${LOGHOUR}/${LOGFILE}| sed  's/|INFRA_LINE|//g' |read COLUMN_VALUE 
                                                    rcode=$?
                                                   set -e
                                                    
                                                    if [ $rcode != 0 ]
                                                    then
                                                            ROW_VALUES="${ROW_VALUES}||||||||||||||||"
                                                    else         
                                                            ROW_VALUES="${ROW_VALUES}|${COLUMN_VALUE}"
                                                    fi
				 		  
				 		  
				 		  
				 		    set +e      
                                                     grep DWI_END_DATETIME $LOG_LAND/${LOGDATE}/${LOGHOUR}/${LOGFILE}|awk -F\= '{print $2}'|read COLUMN_VALUE
                                                     rcode=$?
                                                    set -e
                                                    
                                                    if [ $rcode != 0 ]
                                                    then
                                                            ROW_VALUES="${ROW_VALUES}|"
                                                    else         
                                                            ROW_VALUES="${ROW_VALUES}|${COLUMN_VALUE}"
                                                    fi
				 		  
				 		  
				 		  
				 		   set +e       
                                                           tail -4 $LOG_LAND/${LOGDATE}/${LOGHOUR}/${LOGFILE}|grep -w real |read COLUMN_NAME  COLUMN_VALUE    
                                                           rcode=$?
                                                   set -e
                                                    
                                                   if [ $rcode != 0 ]
                                                   then
                                                            ROW_VALUES="${ROW_VALUES}|"
                                                    else         
                                                            ROW_VALUES="${ROW_VALUES}|${COLUMN_VALUE}"
                                                    fi
                                                    
                                                   set +e       
                                                           tail -4 $LOG_LAND/${LOGDATE}/${LOGHOUR}/${LOGFILE}|grep -w user |read COLUMN_NAME  COLUMN_VALUE    
                                                           rcode=$?
                                                   set -e
                                                    
                                                   if [ $rcode != 0 ]
                                                   then
                                                            ROW_VALUES="${ROW_VALUES}|"
                                                    else         
                                                            ROW_VALUES="${ROW_VALUES}|${COLUMN_VALUE}"
                                                    fi
                                                    
                                                    
                                                   set +e       
                                                           tail -4 $LOG_LAND/${LOGDATE}/${LOGHOUR}/${LOGFILE}|grep -w sys |read COLUMN_NAME  COLUMN_VALUE    
                                                           rcode=$?
                                                   set -e
                                                    
                                                   if [ $rcode != 0 ]
                                                   then
                                                            ROW_VALUES="${ROW_VALUES}|"
                                                   else         
                                                            ROW_VALUES="${ROW_VALUES}|${COLUMN_VALUE}"                                                            
                                                   fi
                                                   
                                                   
                                                   print ${ROW_VALUES} >>  $LOG_INPROGRESS/${LOGDATE}/jobtrack.processed.${LOGHOUR}.logfile 
				   	        
				
				   	  done
			   	  
			   	  #copy the formatted log output file to r4use and another copy to r4cp    
			   	  mv  $LOG_INPROGRESS/${LOGDATE}/jobtrack.processed.${LOGHOUR}.logfile $LOG_R4USE/${LOGDATE}/${LOGDATE}${LOGHOUR}.log&&cp  $LOG_R4USE/${LOGDATE}/${LOGDATE}${LOGHOUR}.log  $LOG_R4CP/ 
			 
			   	  rcode=$?
			   	        
			   	  if [ $rcode = 0 ]
			   	        then
			   	     	   #  drop the appropriate land/temp directories for $LOG_LAND/${LOGDATE}/${LOGHOUR}      
				   	   set +e	 
				   	    rm -rf $LOG_LAND/${LOGDATE}/${LOGHOUR} 
				   	   set -e	 
			   	 fi
		   	  	  
		 	fi 
		  done 
		
		 	 
			       # drop the folder $LOG_LAND/${LOGDATE} if empty
	 
		
		is_empty_log_dir=$(ls "$LOG_LAND/${LOGDATE}") 
		[[ -z "$is_empty_log_dir" && ${LOGDATE} -lt ${CURRENT_DATE} ]] && rmdir  "$LOG_LAND/${LOGDATE}" && print "Successfuly remove log directory $LOG_LAND/${LOGDATE}"
		 
		
		 is_empty_proc_dir=$(ls "$LOG_INPROGRESS/${LOGDATE}") 
		[ -z "$is_empty_proc_dir" ] && rmdir  "$LOG_INPROGRESS/${LOGDATE}" && print "Successfuly remove inprogress directory $LOG_INPROGRESS/${LOGDATE}"
		 
  
done

 
print "####################################################################################"
print "#"
print "# End jobtrack process Complete at `date`"
print "#"
print "####################################################################################"
print ""  

tcode=0 
exit
