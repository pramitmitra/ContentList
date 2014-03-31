#!/bin/ksh -eu
# ---------------------------------------------------------------------------------------
# Title:       Data Warehouse ETL Environment Initialization of Load (td1|td2|td3|td4) 
# Filename:    initialize_batch.ksh
# Description: This script cleans up DW ETL Environment Logs 
#              for daily batch load.
#
# Developer:   Cesar Valenzuela
# Created on:  08/30/2005 
# Location:    $DW_EXE/ 
# Logic:       
#
# Called BY    Appworx
#
# Input
#   Parameters          : none 
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
# 08/30/2005  Cesar Valenzuela        Initial Program
# 09/21/2005  Craig Werre             added message_handler
# 03/18/2010  Kevin Oaks              Moved SUBJECT_AREA assignment before environment initialization
# 05/08/2010  Brian Wenner            Only clears out watchfiles if init touchfile for current date does not exist
# 06/23/2010  Kevin Oaks              Changed usage to reflect multi env paradigm
#                                     Source message_handler from $DW_MASTER_LIB
# 10/04/2013  Ryan Wong               Redhad changes
##########################################################################################################

SCRIPT_NAME=${0##*/}

if [ $# != 1 ]
then
        print "Usage:  $SCRIPT_NAME <td1|td2|td3|td4>"
        exit 4
fi

SUBJECT_AREA=dw_infra

. /dw/etl/mstr_cfg/etlenv.setup 

CURR_DATE=$(date '+%Y%m%d')
CURR_DATETIME=$(date '+%Y%m%d-%H%M%S')
WATCHFILE_DIR=$1
PARENT_ERROR_FILE=$DW_LOG/$WATCHFILE_DIR/$SUBJECT_AREA/${SCRIPT_NAME%.ksh}.$CURR_DATETIME.err
START_TOUCHFILE=$DW_WATCH/$WATCHFILE_DIR/start_of_batch_$CURR_DATE

#-------------------------------------------------------------------------------------
# Source the error message handling logic.  On failure, trap will send the contents
# of the PARENT_ERROR_FILE to the subject area designated email addresses.
#-------------------------------------------------------------------------------------
. $DW_MASTER_LIB/message_handler

#-------------------------------------------------------------
# Remove all the INFA Watch Files 
#-------------------------------------------------------------
if [ ! -f $START_TOUCHFILE ] 
then
   for fn in $DW_WATCH/$WATCHFILE_DIR/*
   do
     if [[ -f $fn ]]
     then
       rm -f $fn
     fi
   done

   touch $DW_WATCH/$WATCHFILE_DIR/start_of_batch_$CURR_DATE
fi

tcode=0
exit
