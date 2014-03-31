#!/bin/ksh -eu
# Title:        dw_infra.hadr_mstr_dat_sft_cleanup.ksh
# File Name:    dw_infra.hadr_mstr_dat_sft_cleanup.ksh
# Description:  Clean up sft hadr Files on non-production nodes
# Developer:    Brian Wenner
# Created on:
# Location:     $DW_EXE
#
# Execution:    $DW_EXE/shell_handler.ksh dw_infra.cleanup td1 $DW_MASTER_BIN/dw_infra.hadr_mstr_dat_sft_cleanup.ksh
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
set -x

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
#   sleep 30
#   continue
exit 0
done

#--------------------------------------------------------------------------------------
#  Remove files older than 1 day in $DW_MASTER_DAT/sft
#--------------------------------------------------------------------------------------
   find $DW_MASTER_DAT/sft -type f -mtime +1 -exec rm -f {} \; 

exit 0

