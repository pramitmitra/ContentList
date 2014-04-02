#!/bin/ksh -eu

##########################################################################################
#
#  Added to the single_table_load graph because the parameter log file is being written
#  with permissions 660.  This will give everyone the ability to read real-time data in
#  the parameter log file once the permissions are modified.
#
#  Ported to RedHat by koaks, 20120821
#
##########################################################################################

cnt=1

while [[ ! -f $LOAD_LOGFILE && $cnt -le 120 ]]
do
   sleep 30
	((cnt+=1))
done

if [ -f $LOAD_LOGFILE ]
then
	chmod 664 $LOAD_LOGFILE
fi

exit
