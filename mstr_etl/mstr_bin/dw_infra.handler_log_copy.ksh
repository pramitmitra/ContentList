#!/bin/ksh -eu
# Title:        Handler Log Copy
# File Name:    dw_infra.handler_log_copy.ksh
# Description:  Helper script to copy PARENG_LOG_FILE
# Developer:    Ryan Wong
# Created on:   
# Location:     $DW_MASTER_BIN
# Logic:       
#
#
# Called by:    UC4/Unix
#
# Date         Ver#   Modified By(Name)            Change and Reason for Change
#---------    -----  ---------------------------  ------------------------------
# 2011-10-14   1.0    Ryan Wong                     Initial version
# 2011-10-19   1.1    Ryan Wong                     Ignore errors from mkdir.  Use case where mkdir is called
#                                                   by two+ running process, it will fail.
# 2013-10-04   1.2    Ryan Wong                     Redhat changes
#
#############################################################################################################

print "Copy to log to infra log location"
DWI_END_DATE=${DWI_END_DATETIME%-*}
DWI_END_HOUR=$(print $DWI_END_DATETIME | cut -c10-11)
DWI_JOBTRACK_DATEDIR=$DW_MASTER_LOG/jobtrack/land/$DWI_END_DATE
DWI_JOBTRACK_DATEHOURDIR=$DW_MASTER_LOG/jobtrack/land/$DWI_END_DATE/$DWI_END_HOUR
PARENT_LOG_FILENAME=${PARENT_LOG_FILE##*/}
DWI_JOBTRACK_LOGFILE=$DWI_JOBTRACK_DATEHOURDIR/${JOB_ENV:-"other"}.$PARENT_LOG_FILENAME

if [[ ! -d $DWI_JOBTRACK_DATEDIR ]]
then
   set +e
   mkdir -m 0775 $DWI_JOBTRACK_DATEDIR
   set -e
fi

if [[ ! -d $DWI_JOBTRACK_DATEHOURDIR ]]
then
   set +e
   mkdir -m 0775 $DWI_JOBTRACK_DATEHOURDIR
   set -e
fi

cp $PARENT_LOG_FILE $DWI_JOBTRACK_LOGFILE
chmod 775 $DWI_JOBTRACK_LOGFILE

exit 0
