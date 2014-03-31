#!/bin/ksh -eu
#------------------------------------------------------------------------------------------------
# Filename:     check_load_logfile.ksh
#
# Revision History:
#
# Name             Date            Description
# ---------------  --------------  ---------------------------------------------------
# ???              ??/??/????      Initial Creation
# Ryan Wong        10/04/2013      Redhat changes
# Ryan Wong        02/25/2014      Add move utility log from DW_SA_TMP to DW_SA_LOG
#
#------------------------------------------------------------------------------------------------

print "Start FastLoad uitlity log file checking abnormal exits"

set +e

grep  -i "\.LOGOFF"  $LOAD_LOGFILE|wc -l |read LOGOFFLINES  OTHER >/dev/null 2>&1

grep  -i "Normal end to loading"  $LOAD_LOGFILE|wc -l |read NORMAL_END  OTHER >/dev/null 2>&1

set -e

# Move LOAD_LOGFILE to $DW_SA_LOG
if [ ${LOAD_LOGFILE%/*} = $DW_SA_TMP ]
then
   mv $LOAD_LOGFILE $DW_SA_LOG
   export LOAD_LOGFILE=$DW_SA_LOG/${LOAD_LOGFILE##*/}
fi

    
if [[ $LOGOFFLINES -lt 2 || $NORMAL_END -lt 1 ]]
then
   print "FastLoad ending with abnormal exits, please check \n $LOAD_LOGFILE \nfor details , it may caused by loading utility runtime issue, Normally the job will pass after reset"
   print "FastLoad ending with abnormal exits, please check \n $LOAD_LOGFILE \nfor details , it may caused by loading utility runtime issue, Normally the job will pass after reset" > ${UTILITY_LOAD_LOGFILE_CHECK_ERRFILE} 
   exit 4
fi

print "End FastLoad uitlity log file checking abnormal exits"

exit 0
