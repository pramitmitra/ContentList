#!/bin/ksh -eu
#------------------------------------------------------------------------------------------------
# Title:        dw_infra.job_track_log_copy.ksh
# File Name:    dw_infra.job_track_log_copy.ksh
# Description:  combine jobtrack log files and scp to the centralized server
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
# George Xiong     11/20/2011      Initial Creation
# Ryan Wong        10/04/2013      Redhat changes
# Ryan Wong        10/15/2014      Update ps command
#------------------------------------------------------------------------------------------------

 
. $DW_MASTER_LIB/dw_etl_common_functions.lib

 

SHELL_EXE_NAME=${0##*/}

REMOTE_SERVER=$1
CURRENT_DATETIME=$(date '+%Y%m%d%H%M%S') 
LOG_BASE=$DW_MASTER_LOG/jobtrack
LOG_R4CP=$LOG_BASE/r4cp
 
 
#--------------------------------------------------------------------------------------
# Determine if there is already a jobtrack logs copy  process running
#-------------------------------------------------------------------------------------- 
 
while [ $(ps -fu$USER | grep "$SHELL_EXE_NAME" | grep "shell_handler.ksh" | grep -v "grep $SHELL_EXE_NAME" | grep -v "ssh" | wc -l) -ge 2 ]
do
   print "There is already a job track logs copy process running. Sleeping for 30 seconds"
   sleep 30
   continue
done


 
ls  $LOG_R4CP/*.log|wc -l|read LOGFILECNT
 
if [ $LOGFILECNT -eq 0 ]
then
 print "No job track log file to copy, Exit!"
 tcode=0 
 exit
fi


print "####################################################################################"
print "#"
print "# Beginning jobtrack logs copy  process at `date`"
print "#"
print "####################################################################################"
print ""  

touch $LOG_R4CP/jobtrack_logfile_$(hostname)_$CURRENT_DATETIME.dat
 
 
 
#--------------------------------------------------------------------------------------
# Append all log files to $LOG_R4CP/jobtrack_logfile_$(hostname)_$CURRENT_DATETIME.dat
#-------------------------------------------------------------------------------------- 
  
for LOGFILE in ` ls  $LOG_R4CP/*.log `
do  
  cat $LOGFILE >> $LOG_R4CP/jobtrack_logfile_$(hostname)_$CURRENT_DATETIME.dat  	   	
done

#--------------------------------------------------------------------------------------
# REMOVE the individual log files
#-------------------------------------------------------------------------------------- 
  
 

rm  $LOG_R4CP/*.log 

#--------------------------------------------------------------------------------------
# SCP  $LOG_R4CP/jobtrack_logfile_$(hostname)_$CURRENT_DATETIME.dat to Centralized Server 
#-------------------------------------------------------------------------------------- 
  
 
for DATFILE in ` ls   $LOG_R4CP/jobtrack_logfile_*.dat `
do  
  scp $DATFILE  $REMOTE_SERVER:/dw/etl/mstr_log/jobtrack/master/dat&&rm $DATFILE
done


 
 
print "####################################################################################"
print "#"
print "# End jobtrack logs copy process Complete at `date`"
print "#"
print "####################################################################################"
print ""  

tcode=0 
exit
