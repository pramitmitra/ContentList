#!/bin/ksh -eu
#------------------------------------------------------------------------------------------------
# Title:        Synch_HA_Node.ksh
# File Name:    synch_ha_node.ksh
# Description:  Synch a passed list of files from HA server to DR
#               Will only process files if HA server has DR synch files from PR
# Developer:    Brian Wenner
# Created on:   
# Location:     $DW_MASTER_BIN  
# Logic:       
#
#
# Called by:    Appworx/Unix
#
# Date         Ver#   Modified By(Name)            Change and Reason for Change
#---------    -----  ---------------------------  ------------------------------
# 2009-10-21   1.0    Brian Wenner                  initial version
# 2013-10-04   1.1    Ryan Wong                     Redhat changes
#------------------------------------------------------------------------------------------------

if [ $# != 0 ]
then
        print "Usage:  $0 "
        print "Process takes no parameters"
        exit 4
fi

RUNTIME=$(date '+%Y%m%d-%H%M')
RUNDATE=$(date '+%Y%m%d')


. /dw/etl/mstr_cfg/etlenv.setup
. $DW_MASTER_LIB/dw_etl_common_functions.lib

SFT_LOG=$DW_MASTER_LOG/sft/$RUNDATE/
LOGFILE=$SFT_LOG/synch_ha_node.synch.$RUNTIME.log

mkdirifnotexist $DW_MASTER_LOG/sft/$RUNDATE/

if [ $SNODEISPROD = 1 ]
then
   SFTSETTING="LOW"
else
   SFTSETTING="REG"
fi

assignSFTSettings $SFTSETTING CMP BW VL PI NW



# extract synch files add on.
if [ $HADR_ACTIVE = 1 ]
then
   if [[ $HAACTIVE = 1 ]]
   then
      print "Synching to $HANODE from $SFT_SNODE " > $LOGFILE 

     if [ -f $DW_MASTER_DAT/sft/*HA.sft.lis ]
     then
        #Process the list of HA lists
       for SFILE in $DW_MASTER_DAT/sft/*HA.sft.lis 
       do
         print "Processing HA synch file $SFILE" >> $LOGFILE
         XFILE=${SFILE%.lis}.exc.lis
         TFILE=${SFILE##*/}
         LFILE=$DW_MASTER_LOG/sft/$RUNDATE/${TFILE%.lis}.$RUNTIME.log 
         SYNCHTMPDIR=$DW_IN/sft/${TFILE%HA.sft.lis}
         SYNCHCOMPFILE=$SYNCHTMPDIR/sft.complete

         PT=$SFT_HA_PORT

         #synch the files
         set +e
         $DW_MASTER_BIN/sg_file_xfr_client -d 2 -f $SFILE -x $XFILE -l $LFILE -p $PT -c $CMP -b $BW -i $PI -v $VL -n $NW
         RCODE=$?
         set -e

         if [ $RCODE != 0 ]
         then
            print " process failed with return code ( $RCODE ) - view log at $LFILE" > $LOGFILE
            exit 4
         elif [[ -f $XFILE ]]
         then
             print "Exception file ( $XFILE ) exists after process ended.  Process did not complete successfully"
             print "View log at $LFILE"
         else 
            print "Synch file $SFILE processed successfully"
            rm -f $SFILE
         fi

         if [[ -d $SYNCHTMPDIR ]]
         then
            print "HA" >> $SYNCHCOMPFILE
            RCODE=`grepCompFile "DR" $SYNCHCOMPFILE`
            if [ $RCODE = 0 ]
            then
               # DR is already complete - remove the temp synch dir 
               rmdirtreeifexist $SYNCHTMPDIR
            fi
         fi

       done
     fi 
   else
      print "HA is not active - there is nothing to synch"
      exit 0
   fi
else
   print "HA/DR processing is turned off"
   exit 0
fi

exit 0
