#!/bin/ksh -eu

# ---------------------------------------------------------------------------------------
# Title:       Data Warehouse ETL Environment Non-Standard Files Clean up 
# Filename:    dw_infra.etl_clean_up.ksh
# Description: This script cleans up DW ETL Environment Non-Standard Files, especailly
#              clean up DW_TMP/DW_LOG directory.
#
# Developer:   Jacky Shen
# Created on:  06/21/2010 
# Location:    $DW_EXE/ 
# Logic:       The script is called by shell_handler.ksh
# Usage:       shell_handler.ksh dw_infra primary dw_infra.etl_clean_up.ksh
#
# Input
#   Parameters          : $DW_CFG/dw_infra.etl_clean_up.cfg 
#   Prev. Set Variables :
#   Tables, Views       : N/A
#
# Output/Return Code    : 
#   0 - success
#   otherwise error
# 
# Last Error Number:
#
# Date        Modified By(Name)       Change and Reason for Change
# ----------  ----------------------  ---------------------------------------
# 06/21/2010  Jacky Shen              Initial Program
# 10/04/2013  Ryan Wong               Redhat changes
# 11/19/2014 Jiankang Liu			  Add parameter to determine the storage type: local|shared
##########################################################################################################

typeset -fu usage

function usage {
   	print "Usage:  $0 <STORAGE_TYPE>
	STORAGE_TYPE =   <local|shared>"
}

if [ $# -ne 1 ]
then
   usage 
   exit 4
fi

STORAGE_TYPE=$1

if [ $STORAGE_TYPE = shared ]
then
	export STORAGE_TYPE="shared"
else
	export STORAGE_TYPE="local"
fi

. /dw/etl/mstr_cfg/etlenv.setup 


print "####################################################################################"
print "#"
print "# Beginning deleting process for nonstandard data and log files  `date`"
print "#"
print "####################################################################################"
print ""


egrep -v '^#|^ *$' $DW_MASTER_CFG/dw_infra.etl_clean_up.$STORAGE_TYPE.cfg | while read DIR_NAME REC_DEPTH RET_DAY
do
    DIR_LEVEL=`print $(eval print $DIR_NAME) | awk -F\/ '{print NF}'`                  # Calculate Target Dir Level
    ((REC_LEVEL=DIR_LEVEL+REC_DEPTH))                                  # Calculate Max Depth
    find $(eval print $DIR_NAME) -type f -mtime +$RET_DAY | nawk -F \/ -v rec_level="$REC_LEVEL" '{if(NF <= rec_level){scmd="rm -f " $0; print $0; system(scmd);}}'
done

print ""
print "####################################################################################"
print "#"
print "# Deleting of nonstandard data and log files complete  `date`"
print "#"
print "####################################################################################"

exit
