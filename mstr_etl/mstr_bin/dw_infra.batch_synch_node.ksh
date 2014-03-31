#!/bin/ksh -eu
#------------------------------------------------------------------------------------------------
# Title:        dw_infra.batch_synch_node.ksh
# File Name:    dw_infra.batch_synch_node.ksh
# Description:  Search for files to be processed in batch mode from current server, and process them
#               per the master list file contents.
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
# 2011-01-18   1.0    Brian Wenner                  initial version
# 2013-10-04   1.1    Ryan Wong                     Redhat changes
#
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
. /dw/etl/mstr_cfg/etlenv.hadr.batch.setup

SHELL_EXE_NAME=${0##*/}

#--------------------------------------------------------------------------------------
# Determine if there is already a batch synch process running, if so, exit
#--------------------------------------------------------------------------------------
while [ $(/usr/ucb/ps -auxwwwl | grep "$SHELL_EXE_NAME" | grep -v "shell_handler.ksh" | grep -v "grep $SHELL_EXE_NAME"| wc -l) -ge 2 ]
do
   print "${0##*/}:  INFO, There is already a batch synch process running. Exiting"
   exit
done


SFT_LOG=$DW_MASTER_LOG/sft/$RUNDATE/
LOGFILE=$SFT_LOG/batch_synch_node.synch.$RUNTIME.log

if (( HADR_BATCH_ACTIVE )); then
  # process the files in $DW_MASTER_DAT/sft/pending 

  mkdirifnotexist $DW_MASTER_LOG/sft/$RUNDATE/

  (( HADR_BATCH_ISPROD )) && SFTSETTING="LOW" || SFTSETTING="REG"

  assignSFTSettings $SFTSETTING CMP BW VL PI NW

  print "${0##*/}:  INFO,
##########################################################################################################
# Begin Synching process `date`
##########################################################################################################
" >> $LOGFILE

   MAXINST=1
   INST=0

   while (( INST <= MAXINST ))
   do
   
     if [[ ${HADR_BATCH_MODE[$INST]} != "N" ]]; then
   
     print "${0##*/}:  INFO,
   ##########################################################################################################
   # Starting Instance $INST Source ${HADR_TRGT[$INST]} to Target ${HADR_TRGT[$INST]} process `date`
   ##########################################################################################################
   " >> $LOGFILE
   
       #Process the master files.
       if [ -f $DW_MASTER_DAT/sft/pending/*.$INST.master.dat ]; then
          for MASTERFILE in $DW_MASTER_DAT/sft/pending/*.$INST.master.dat
          do
            print "${0##*/}:  INFO, processing master file $MASTERFILE `date` " >> $LOGFILE
      
            TMPMASTERFILE=${MASTERFILE##*/}
            HDR=${TMPMASTERFILE%.master.dat}
            PCGID=${HDR%.*}
            
            print "${0##*/}:  INFO, Processing master synch file $MASTERFILE" >> $LOGFILE
            read OPTIONLIS < $MASTERFILE
            XFILE=$DW_MASTER_DAT/sft/excpt/$HDR.excpt.lis
            LFILE=$SFT_LOG/$HDR.batch_synch.$RUNTIME.log
            print "$DW_MASTER_BIN/dw_infra.synch_hadr_node.ksh $OPTIONLIS -x $XFILE -l $LFILE -p ${HADR_SFT_PORT[$INST]}"
            
            print "${0##*/}:  INFO, Running dw_infra.synch_hadr_node.ksh for Instance BATCH $HDR `date`" >> $LOGFILE
            set +e
            $DW_MASTER_BIN/dw_infra.synch_hadr_node.ksh $OPTIONLIS -x $XFILE -l $LFILE -p ${HADR_SFT_PORT[$INST]} >> $LOGFILE 2>&1
            rcode=$?
            set -e
      
            if [ $rcode != 0 ]
            then
              print "${0##*/}:  ERROR, failed processing master file $MASTERFILE see instance log file $LFILE"  >> $LOGFILE
            else
               #nremove the source files
               rmdirtreeifexist $DW_IN/sft/$HDR/
            fi
            print "${0##*/}:  INFO, completed master file $MASTERFILE `date` " >> $LOGFILE
          done
       fi
     print "${0##*/}:  INFO,
   ##########################################################################################################
   #  Completed Instance $INST Source ${HADR_TRGT[$INST]} to Target ${HADR_TRGT[$INST]} process `date`
   ##########################################################################################################
   " >> $LOGFILE
   
     else
       print "${0##*/}:  INFO,
   ##########################################################################################################
   # Instance $INST Source ${HADR_TRGT[$INST]} to Target ${HADR_TRGT[$INST]} is not active process `date`
   ##########################################################################################################
   " >> $LOGFILE
   
     fi
     (( INST += 1 ))
   done
fi
exit 0
