#!/bin/ksh -eu
# Title:        dw_infra.hadr_out_extract_cleanup.ksh
# File Name:    dw_infra.hadr_out_extract_cleanup.ksh
# Description:  Clean up Files on non-production nodes
# Developer:    Brian Wenner
# Created on:
# Location:     $DW_EXE
#
# Execution:    $DW_EXE/shell_handler.ksh dw_infra.cleanup td1 $DW_MASTER_BIN/dw_infra.hadr_out_extract_cleanup.ksh
#
# Parameters:   none
#
# Revision History:
#
# Name             Date            Description
# ---------------  --------------  ---------------------------------------------------
# Brian Wenner     11/03/2010      Initial Creation
# Ryan Wong        10/04/2013      Redhat changes
#------------------------------------------------------------------------------------------------


print "####################################################################################"
print "#"
print "# Beginning cleanup process for data files  `date`"
print "#"
print "####################################################################################"
print ""
. /dw/etl/mstr_cfg/etlenv.setup

SHELL_EXE_NAME=${0##*/}
DW_SA_LOG=$DW_LOG/td1/dw_infra

#--------------------------------------------------------------------------------------
# Determine if there is already an cleanup process running
#--------------------------------------------------------------------------------------
while [ $(/usr/ucb/ps -auxwwwl | grep "$SHELL_EXE_NAME" | grep "shell_handler.ksh" | grep -v "grep $SHELL_EXE_NAME"| wc -l) -ge 2 ]
do
   print "There is already a cleanup process running. Sleeping for 30 seconds"
   sleep 30
   continue
done

#--------------------------------------------------------------------------------------
# print starting usage
#--------------------------------------------------------------------------------------
df -k $DW_OUT/extract/
dwi_assignTagValue -p MIN_DATA_RET_DAYS -t MIN_DATA_RET_DAYS -f $DW_MASTER_CFG/dw_infra.cleanup.cfg -s W -d 1
dwi_assignTagValue -p MAX_DATA_RET_DAYS -t MAX_DATA_RET_DAYS -f $DW_MASTER_CFG/dw_infra.cleanup.cfg -s W -d 7

#--------------------------------------------------------------------------------------
# Step through each directory in $DW_IN/extract
#--------------------------------------------------------------------------------------
ls -d $DW_OUT/extract/* | while read SADIR
do
  #get the max data retention days for SA's matching SADIR
  #default of MAX_DATA_RET_DAYS, min of MIN_DATA_RET_DAYS
  SA=${SADIR##*/}
  integer SA_DATA_RET_DAYS=-1 
  if [ -f $DW_CFG/$SA.*.cfg ]
  then
    for fn in $DW_CFG/$SA.*.cfg
    do
      assignTagValue TMP_DATA_RET_DAYS DATA_RET_DAYS $fn W 0
      [[ $TMP_DATA_RET_DAYS -gt $SA_DATA_RET_DAYS ]] && SA_DATA_RET_DAYS=$TMP_DATA_RET_DAYS
    done
  fi

  [[ $SA_DATA_RET_DAYS == -1 || $SA_DATA_RET_DAYS -gt $MAX_DATA_RET_DAYS ]] &&  SA_DATA_RET_DAYS=$MAX_DATA_RET_DAYS
  [[ $SA_DATA_RET_DAYS -lt $MIN_DATA_RET_DAYS ]] &&  SA_DATA_RET_DAYS=$MIN_DATA_RET_DAYS
  print "For SA dir $SADIR Data Retention is set to $SA_DATA_RET_DAYS"

   # remove files from SADIR that are older than SA_DATA_RET_DAYS
   #find $SADIR -type f -mtime $SA_DATA_RET_DAYS 
   find $SADIR -type f -mtime +$SA_DATA_RET_DAYS -exec rm -f {} \;
done 

#--------------------------------------------------------------------------------------
# print ending usage
#--------------------------------------------------------------------------------------
df -k $DW_OUT/extract/

