#!/bin/ksh -eu

##########################################################################################
#
# Title:        Single Table Extract Local to DR Copy 
# File Name:    dw_infra.single_table_extract.local_to_dr.ksh 
# Description:  Replicates data to shared nfs mount for extract jobs using UOW 
# Developer:    Kevin Oaks 
# Created on:   2018-02-27
# Location:     $DW_MASTER_BIN
# Logic:        Process is meant to be called by $DW_MASTER_EXE/single_table_extract_run.ksh
#               Parameters are pass in from the calling script so that local and nfs dir
#               structures can be determined. If determination is made and qualifying
#               conditions are met, data is copied from local to nfs for DR Recovery
#               purposes.
#
#
# Called by:    $DW_MASTER_EXE/single_table_extract_run.ksh 
#
# Date         Ver#   Modified By(Name)            Change and Reason for Change
#---------    -----  ---------------------------  ------------------------------
#
# 2018-02-27   1.0    Kevin Oaks                   Initial Prod Version
#
###########################################################################################


SUBJECT_AREA=$1
TABLE_ID=$2
LOCAL_DATA_DIR=$3
DW_DR_BASE=$4
DW_SA_LOG=$5
UOW_APPEND=${6:-""}
CURR_DATETIME=${7:-$(date '+%Y%m%d-%H%M%S')}

JOB_TYPE_ID=dr
LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.local_to_dr${UOW_APPEND}.$CURR_DATETIME.log

DW_DR_ETL_ID_BASE=$DW_DR_BASE/$SUBJECT_AREA/$TABLE_ID  
DW_DR_ETL_ID_UOW_DIR=$DW_DR_ETL_ID_BASE${LOCAL_DATA_DIR##*/$TABLE_ID}

if [[ ! -d $DW_DR_ETL_ID_UOW_DIR ]]
then
  print "Creating $DW_DR_ETL_ID_UOW_DIR" >> $LOG_FILE
  set +e
  mkdir -p $DW_DR_ETL_ID_UOW_DIR 
  rcode=$?
  set -e

  if [[ $rcode != 0 ]]
  then
    print "${0##*/}:  ERROR, unable to make directory $DW_DR_ETL_ID_UOW_DIR" >> $LOG_FILE 
    exit 4
  fi

else
  print "$DW_DR_ETL_ID_UOW_DIR exists. Using for Data Copy" >> $LOG_FILE
fi
  
  # Copying data
  print "Copying Data from $LOCAL_DATA_DIR to $DW_DR_ETL_ID_UOW_DIR" >> $LOG_FILE
  set +e
  cp -a $LOCAL_DATA_DIR/* $DW_DR_ETL_ID_UOW_DIR
  rcode=$?
  set -e

  if [[ $rcode != 0 ]]
  then
    print "${0##*/}:  ERROR, unable to copy data from $LOCAL_DATA_DIR to $DW_DR_ETL_ID_UOW_DIR" >> $LOG_FILE
    exit 4
  fi

print "DR Data Copy complete" >> $LOG_FILE

exit
