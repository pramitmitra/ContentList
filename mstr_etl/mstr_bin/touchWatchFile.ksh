#!/bin/ksh -eu
#==========================================================================================
# Filename:    touchWatchFile.ksh
# Description: touch a watch (trigger) file 
#              If a simple filename is passed, the file is created under $WatchFileDir
#
# Developer:   Brian Wenner
# Created on:  2010-04-01 (modified/ammemded from prev incarnation from Lucian Masalar)
#
# Called By:    from Unix or UC4
# History
#
#  Date         Modified By            Description
# _________________________________________________________________________________________
#
# 2010-11-08    Kevin Oaks             Updated usage for multi env
#                                      Added logic for datamove watchfiles
# 2011-09-21    Ryan Wong              Add uow.  Interim, will push out two touch files for
#                                      any jobs using uow
# 2011-09-26    Kevin Oaks             Use UOW_TO as UOW_APPEND
# 2011-10-05    Ryan Wong              If remote directories exist, do not run mkdir
# 2012-03-12    Ryan Wong              Remove BSN from UOW type touch files.  Only for
#                                      JOB_TYPE = [extract|load|scp_push|sft_push|datamove]
# 2013-03-26    George Xiong           set remote user for remote touch file process
# 2013-10-04    Ryan Wong              Redhat changes
# 2014-07-17    John Hackley           Added new step to copy touch file to Ingest if
#                                      specified in new <ETL_ID>.cfg tag
# 2014-04-06    Ryan Wong              Secure Batch ID. Add group write perms for mkdir
#                                      Cannot use mkdirifnotexist
###########################################################################################

USAGE_touchw () {
cat << EOF
#
# USAGE : $SCRIPT_NAME <etl_id> <job_type> <extract|td1|td2|td3|td4> <touch_file_name> [-f <UOW_FROM> -t <UOW_TO>]
# WHERE : 
#       : <etl_id> = the etl id area of the job that creates the touch file, reference cfg for sending out emails
#       : <job_type> = extract, load, datamove or transform
#       : <extract|td1|td2|td3|td4> = the specified directory that you want to create the file in.
#       : <touch_file_name> = the name of the file you want to create, should not include path
# EXAMPLES 
#       : $SCRIPT_NAME dw_dim.dw_countries load td1 dw_dim.dw_countries.load.22.done
#       :   (will create \$DW_WATCH/td1/dw_dim.dw_countries.load.22.done)
# 
#       : Invoking with UOW will create two touch files for backwards compatibility
#       : Note the BSN is stripped from UOW touch file for job_types [extract|load|scp_push|sft_push|datamove]
#       : $SCRIPT_NAME dw_dim.dw_countries load td1 dw_dim.dw_countries.load.22.done -f 20110101000000 -t 20110102000000
#       :   (will create both \$DW_WATCH/td1/dw_dim.dw_countries.load.22.done and
#       :    \$DW_WATCH/td1/20100102/dw_dim.dw_countries.load.done.20110102000000)
#
EOF
}
#         
#
#==============================================================================


SCRIPT_NAME=${0##*/}

if [ $# -lt 4 ]
then
  USAGE_touchw
  exit 101
fi

ETL_ID=$1
JOB_TYPE=$2
JOB_ENV=$3
WATCH_FILE=$4
shift 4

UOW_FROM=""
UOW_TO=""
UOW_FROM_FLAG=0
UOW_TO_FLAG=0
print "Processing Options"
while getopts "f:t:" opt
do
   case $opt in
      f ) if [ $UOW_FROM_FLAG -ne 0 ]
          then
            print "FATAL ERROR: -f flag specified more than once" >&2
            exit 8
          fi
          print "Setting UOW_FROM_FLAG == 1"
          UOW_FROM_FLAG=1
          print "Setting UOW_FROM == $OPTARG"
          UOW_FROM=$OPTARG;;
      t ) if [ $UOW_TO_FLAG -ne 0 ]
          then
            print "FATAL ERROR: -t flag specified more than once" >&2
            exit 8
          fi
          print "Setting UOW_TO_FLAG == 1"
          UOW_TO_FLAG=1
          print "Setting UOW_TO == $OPTARG"
          UOW_TO=$OPTARG;;
      \? ) USAGE_touchw
           exit 1 ;;
   esac
done
shift $(($OPTIND - 1))

SSH_USER=$(whoami)

case $JOB_TYPE in
  extract )
    JOB_TYPE_ID=ex;;
  load )
    JOB_TYPE_ID=ld;;
  scp_push )
    JOB_TYPE_ID=scp_push;;
  sft_push )
    JOB_TYPE_ID=sft_push;;
  datamove )
    JOB_TYPE_ID=dm;;
  bteq )
    JOB_TYPE_ID=bt;;
  transform )
    JOB_TYPE_ID=tr;;
  * )
    JOB_TYPE_ID=na;;
esac

. /dw/etl/mstr_cfg/etlenv.setup
. $DW_MASTER_CFG/dw_etl_common_defs.cfg
. $DW_MASTER_LIB/dw_etl_common_functions.lib

# Calculate UOW values
UOW_APPEND=""
UOW_PARAM_LIST=""
UOW_PARAM_LIST_AB=""
if [[ $UOW_FROM_FLAG -eq 1 && $UOW_TO_FLAG -eq 1 ]]
then
   UOW_APPEND=.$UOW_TO
   UOW_PARAM_LIST="-f $UOW_FROM -t $UOW_TO"
   UOW_PARAM_LIST_AB="-UOW_FROM $UOW_FROM -UOW_TO $UOW_TO"
   is_valid_ts $UOW_FROM
   is_valid_ts $UOW_TO
elif [[ ($UOW_FROM_FLAG -eq 1 && $UOW_TO_FLAG -eq 0) || ($UOW_TO_FLAG -eq 1 && $UOW_FROM_FLAG -eq 0) ]]
then
   print "Missing required UOW (FROM or TO) parameter."
   USAGE_touchw
   exit 1
fi

export CURR_DATETIME=${CURR_DATETIME:-$(date '+%Y%m%d-%H%M%S')}
PARENT_ERROR_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.$WATCH_FILE.${SCRIPT_NAME%.ksh}${UOW_APPEND}.$CURR_DATETIME.err

#-------------------------------------------------------------------------------------
# Source the error message handling logic.  On failure, trap will send the contents
# of the PARENT_ERROR_FILE to the subject area designated email addresses.
#-------------------------------------------------------------------------------------
. $DW_MASTER_LIB/message_handler

#-------------------------------------------------------------------------------------
# Set up watch file variables
#-------------------------------------------------------------------------------------
OLD_DW_SA_WATCHFILEDIR=$DW_WATCH/$JOB_ENV
OLD_DW_SA_WATCHFILE=$OLD_DW_SA_WATCHFILEDIR/$WATCH_FILE

if [[ $UOW_TO_FLAG -eq 0 ]]
then
        DW_SA_WATCHFILEDIR=$DW_WATCH/$JOB_ENV
        DW_SA_WATCHFILE=$DW_SA_WATCHFILEDIR/$WATCH_FILE
else
        #-------------------------------------------------------------------------------------
        # Remove BSN for UOW watch files where JOB_TYPE = [extract|load|scp_push|sft_push|datamove]
        # WATCH_FILE pattern should be in form:
        # $ETL_ID.$JOB_TYPE.$BATCH_SEQ_NUM.done or $ETL_ID.$JOB_TYPE.$ENV_TYPE.$BATCH_SEQ_NUM.done
        #-------------------------------------------------------------------------------------
        UOW_WATCH_FILE=$WATCH_FILE
        case $JOB_TYPE in
          extract|load|scp_push|sft_push|datamove )
            UOW_WATCH_FILE_WO_DONE=${UOW_WATCH_FILE%.done}
            if [[ $UOW_WATCH_FILE_WO_DONE != $WATCH_FILE ]]
            then
              BATCH_SEQ_NUM=${UOW_WATCH_FILE_WO_DONE##*.}
              if [[ $BATCH_SEQ_NUM == +([0-9]) ]]
              then
                UOW_WATCH_FILE=${UOW_WATCH_FILE_WO_DONE%.*}.done
              fi
            fi
            ;;
        esac
        UOW_DATE=${UOW_DATE:-$(print $UOW_TO | cut -c1-8)}
        DW_SA_WATCHFILEDIR=$DW_WATCH/$JOB_ENV/$UOW_DATE
        DW_SA_WATCHFILE=$DW_SA_WATCHFILEDIR/$UOW_WATCH_FILE.$UOW_TO
fi

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

   touch $OLD_DW_SA_WATCHFILE
   chmod 666 $OLD_DW_SA_WATCHFILE

   if [[ $UOW_TO_FLAG -eq 1 ]]
   then

      if [ ! -d $DW_SA_WATCHFILEDIR ]
      then
        set +e
        mkdir -p $DW_SA_WATCHFILEDIR
        mkdir_rcode=$?
        set -e

        if [ $mkdir_rcode != 0 ]
        then
          print "${0##*/}:  FATAL ERROR, Unable to make directory $DW_SA_WATCHFILEDIR." >&2
          return 4
        else
          print "Successfuly made directory $DW_SA_WATCHFILEDIR"
        fi

        chmod g+w $DW_SA_WATCHFILEDIR
      else
        print "directory $DW_SA_WATCHFILEDIR already exists"
      fi

      touch $DW_SA_WATCHFILE
      chmod 666 $DW_SA_WATCHFILE
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
print "processing dr touchfile"
#-------------------------------------------------------------------------------------
PROCESS=process_dr_touchfile
RCODE=`grepCompFile $PROCESS $COMP_FILE`

if [ $RCODE = 1 ]
then
   if [[ ${DWI_RMT_SERVER:-"NA"} != "NA" ]]
   then
      #on hadr platform - touch file to dr system (which copies it to system in other data center)
      ssh -n $SSH_USER@$DWI_RMT_SERVER "touch $OLD_DW_SA_WATCHFILE"
      if [[ $UOW_TO_FLAG -eq 1 ]]
      then
         set +e
         ssh -n $SSH_USER@$DWI_RMT_SERVER "mkdir -p $DW_SA_WATCHFILEDIR; chmod g+w $DW_SA_WATCHFILEDIR" > /dev/null
         set -e
         ssh -n $SSH_USER@$DWI_RMT_SERVER "touch $DW_SA_WATCHFILE"
      fi
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
            if [ X"$RMTUSER" = X"" ]
            then
               RMTUSER=$SSH_USER
            fi	
            	
            ssh -n $RMTUSER@$RMTSERVER "touch $OLD_DW_SA_WATCHFILE"
            if [[ $UOW_TO_FLAG -eq 1 ]]
            then
               set +e
               ssh -n $RMTUSER@$RMTSERVER "mkdir -p $DW_SA_WATCHFILEDIR; chmod g+w $DW_SA_WATCHFILEDIR" > /dev/null
               set -e
               ssh -n $RMTUSER@$RMTSERVER "touch $DW_SA_WATCHFILE"
            fi
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


#-------------------------------------------------------------------------------------
print "creating remote ingest touchfile"
#-------------------------------------------------------------------------------------
PROCESS=process_remote_ing_touchfile
RCODE=`grepCompFile $PROCESS $COMP_FILE`

typeset -l COPY_WATCHFILES_TO_INGEST
typeset -l RMTDC

if [ $RCODE = 1 ]
then

   assignTagValue COPY_WATCHFILES_TO_INGEST COPY_WATCHFILES_TO_INGEST $ETL_CFG_FILE W "n/a"

   if [ -f $DW_MASTER_CFG/ingest_watchfile.$ETL_ENV.lis ]
   then

      REMOTE_COMP_FILE=$DW_SA_TMP/$TABLE_ID.$JOB_TYPE.$WATCH_FILE.remoteingwatch.complete
      mkfileifnotexist $REMOTE_COMP_FILE

      while read RMTSERVER RMTDC
      do

#        see if the tag in <ETL_ID>.cfg contains the datacenter in ingest_watchfile.lis
         if test "${COPY_WATCHFILES_TO_INGEST#*""$RMTDC""}" != "$COPY_WATCHFILES_TO_INGEST"
         then

            RCODE=`grepCompFile $RMTSERVER $REMOTE_COMP_FILE`

            if [ $RCODE = 1 ]
            then
               ssh -n sg_adm@$RMTSERVER "touch $DW_SA_WATCHFILE"
               print "touchfile processed to server: $RMTSERVER"
               print "$RMTSERVER" >> $REMOTE_COMP_FILE
            elif [ $RCODE = 0 ]
            then
               print "touchfile already processed to server: $RMTSERVER"
            else
               print "${0##*/}:  ERROR, Unable to grep for $RMTSERVER in $REMOTE_COMP_FILE"
               exit $RCODE
            fi
         fi

      done < $DW_MASTER_CFG/ingest_watchfile.$ETL_ENV.lis
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
