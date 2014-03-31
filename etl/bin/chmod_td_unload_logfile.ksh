#!/bin/ksh -eu

##########################################################################################
#
#  Added to the single_table_load graph because the parameter log file is being written
#  with permissions 660.  This will give everyone the ability to read real-time data in
#  the parameter log file once the permissions are modified.
#
# Revision History:
#
# Name             Date            Description
# ---------------  --------------  ---------------------------------------------------
# ???              ??/??/????      Initial Creation
# Ryan Wong        10/04/2013      Redhat changes
#
##########################################################################################

cnt=1

while [[ ! -f $FEXP_LOGFILE && $cnt -le 120 ]]
do
   sleep 30
	((cnt+=1))
done

if [ -f $FEXP_LOGFILE ]
then
	chmod 664 $FEXP_LOGFILE
fi

exit
