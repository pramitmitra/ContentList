#!/bin/ksh -eu
#==========================================================================================
# Filename:    touchWatchFile.ksh
# Description: touch a watch (trigger) file 
#              If a simple filename is passed, the file is created under $WatchFileDir
#
# Developer:   Brian Wenner
# Created on:  2010-04-01 (modified/ammemded from prev incarnation from Lucian Masalar)
#
# Revision History:
#
# Name             Date            Description
# ---------------  --------------  ---------------------------------------------------
# ???              ??/??/????      Initial Creation
# Ryan Wong        10/04/2013      Redhat changes
#
# Called By:    from Unix or UC4

USAGE_touchw () {
cat << EOF
#
# USAGE : $SCRIPT_NAME help    <-- will give you a full description
#       : $SCRIPT_NAME <etl_id> <job_type> <primary|secondary> <touch_file_name> <uow>
# WHERE : 
#       : <etl_id> = the etl id area of the job that creates the touch file, reference cfg for sending out emails
#       : <job_type> = load or transform
#       : <primary|secondary> = the specified directory that you want to create the file in.
#       : <touch_file_name> = the name of the file you want to create
#       :   If a full path is passed, it touches the file according to that path.
#       :   If a simple filename is passed, the file is created under \$WatchFileDir
#       : <uow> = the uow id of the batch
# EXAMPLES 
#       : $SCRIPT_NAME dw_dim.dw_countries transform primary aaa 20100601
#       :   (will create \$WatchFileDir/primary/dw_dim/aaa.20100601)
#       :
#       :
# NOTES : all other regular ELF options (on_exception, log_dir, etc) have been intentionally
#       : left out for brevity 
#
EOF
}
#         
#
#==============================================================================


SCRIPT_NAME=${0##*/}

if [ $# != 5 ]
then
  USAGE_touchw
  exit 101
fi

ETL_ID=$1
JOB_TYPE=$2
JOB_ENV=$3
WATCH_FILE=$4
UOW=$5

SSH_USER=$(whoami)

if [ $JOB_TYPE = load ]
then
        JOB_TYPE_ID=ld
elif [ $JOB_TYPE = extract ]
then
        JOB_TYPE_ID=ex
else
        JOB_TYPE=transform
        JOB_TYPE_ID=bt
fi

. /dw/etl/mstr_cfg/etlenv.setup
. /$DW_MASTER_CFG/dw_etl_common_defs.cfg

CURR_DATETIME=$(date '+%Y%m%d-%H%M%S')
PARENT_ERROR_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.$WATCH_FILE.${SCRIPT_NAME%.ksh}.$CURR_DATETIME.err
DW_SA_WATCHFILE=$DW_WATCH/$JOB_ENV/$WATCH_FILE.$UOW

#-------------------------------------------------------------------------------------
# Source the error message handling logic.  On failure, trap will send the contents
# of the PARENT_ERROR_FILE to the subject area designated email addresses.
#-------------------------------------------------------------------------------------
. $DW_MASTER_LIB/message_handler

COMP_FILE=$DW_SA_TMP/$TABLE_ID.$JOB_TYPE.$WATCH_FILE.watch.complete
mkfileifnotexist $COMP_FILE

#-------------------------------------------------------------------------------------
print "Removing previous err and marking log files .r4a"
#-------------------------------------------------------------------------------------
PROCESS=err_and_log_file_cleanup
RCODE=`grepCompFile $PROCESS $COMP_FILE`

if [ $RCODE = 1 ]
then

   if [ -f $DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.$WATCH_FILE.${SCRIPT_NAME%.ksh}.!(*.r4a|*$CURR_DATETIME.*) ]
   then
      for fn in $DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.$WATCH_FILE.${SCRIPT_NAME%.ksh}.!(*.r4a|*$CURR_DATETIME.*)
      do
         if [[ ${fn##*.} == err && ! -s $fn ]]
         then
            rm -f $fn     # remove empty error files
         else
            mv -f $fn $fn.r4a
         fi
      done
   fi

   print "$PROCESS process complete"
   print "$PROCESS" >> $COMP_FILE

elif [ $RCODE = 0 ]
then
   print "$PROCESS process already completed"
else
   print "${0##*/}:  ERROR, Unable to grep for $PROCESS in $COMP_FILE"
   exit $RCODE
fi



#-------------------------------------------------------------------------------------
print "creating local touchfile"
#-------------------------------------------------------------------------------------
PROCESS=create_local_touchfile
RCODE=`grepCompFile $PROCESS $COMP_FILE`

if [ $RCODE = 1 ]
then

   touch $DW_SA_WATCHFILE

   chmod 666 $DW_SA_WATCHFILE

   print "$PROCESS process completed"
   print "$PROCESS" >> $COMP_FILE

elif [ $RCODE = 0 ]
then
   print "$PROCESS process already completed"
else
   print "${0##*/}:  ERROR, Unable to grep for $PROCESS in $COMP_FILE"
   exit $RCODE
fi

#-------------------------------------------------------------------------------------
print "processing dr touchfile"
#-------------------------------------------------------------------------------------
PROCESS=process_dr_touchfile
RCODE=`grepCompFile $PROCESS $COMP_FILE`

if [ $RCODE = 1 ]
then
   if [[ $HADR_ACTIVE = 1 && $NODE_NONSPEC = 0 && $DRACTIVE = 1 ]]
   then
      #on hadr platform - touch file to dr system (which copies it to system in other data center)
      ssh -n $SSH_USER@$DRNODE "touch $DW_SA_WATCHFILE"
   fi

   print "$PROCESS process completed"
   print "$PROCESS" >> $COMP_FILE

elif [ $RCODE = 0 ]
then
   print "$PROCESS process already completed"
else
   print "${0##*/}:  ERROR, Unable to grep for $PROCESS in $COMP_FILE"
   exit $RCODE
fi

#-------------------------------------------------------------------------------------
print "creating remote custom touchfile"
#-------------------------------------------------------------------------------------
PROCESS=process_remote_touchfile
RCODE=`grepCompFile $PROCESS $COMP_FILE`

if [ $RCODE = 1 ]
then

   if [ -f $DW_MASTER_CFG/$servername.watchfile.cfg ]
   then

      REMOTE_COMP_FILE=$DW_SA_TMP/$TABLE_ID.$JOB_TYPE.$WATCH_FILE.remotewatch.complete
      mkfileifnotexist $REMOTE_COMP_FILE

      while read RMTSERVER RMTUSER
      do
         RCODE=`grepCompFile $RMTSERVER $REMOTE_COMP_FILE`

         if [ $RCODE = 1 ]
         then
            if [ X"$RMTUSER" != X"" ]
            then
               SSH_USER=$RMTUSER
            fi
            ssh -n $SSH_USER@$RMTSERVER "touch $DW_SA_WATCHFILE"
            print "touchfile processed to server: $RMTSERVER"
            print "$RMTSERVER" >> $REMOTE_COMP_FILE
         elif [ $RCODE = 0 ]
         then
            print "touchfile already processed to server: $RMTSERVER"
         else
            print "${0##*/}:  ERROR, Unable to grep for $RMTSERVER in $REMOTE_COMP_FILE"
            exit $RCODE
         fi

      done < $DW_MASTER_CFG/$servername.watchfile.cfg
      rm -f $REMOTE_COMP_FILE
   fi

   print "$PROCESS COMPLETED"
   print "$PROCESS" >> $COMP_FILE


elif [ $RCODE = 0 ]
then
   print "$PROCESS already completed"
else
   print "${0##*/}:  ERROR, Unable to grep for $PROCESS in $COMP_FILE"
   exit $RCODE
fi

print "Removing the complete file  `date`"
rm -f $COMP_FILE

tcode=0
exit 
