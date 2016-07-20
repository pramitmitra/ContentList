#!/bin/ksh -eu

##########################################################################################################
##########################################################################################################
#
#
# Title:        Subject Area Data to Datashare Migration Utility 
# File Name:    dw_infra.migrate_sa_datashare_utility.ksh 
# Description:  This script facilitates the migration of data (in, out, tmp, and arc) files from the 
#               original /data mount on Tempo hardware to the shared /datashare mount.
#               This script should be run on the node where data is currently stored locally. This will
#               move the local data to the shared data mount and create a pointer from in, out, tmp, arc
#               at the subject area level to the shared datamount on that local node. To take advantage
#               of built in failover, however, the SAE should move the process flows for the SA to
#               a dedicated shared data node and UC4 Agent Group
#
#   ************ NOTE: THIS SCRIPT MOVES AND REMOVES ENTIRE DIRECTORY STRUCTURES! ************
#   ******************************* USE WITH EXTREME CAUTION!!! ******************************
#
# Developer:    Kevin Oaks
# Created on:
# Location:     $DW_MASTER_BIN/infra
#
# Logic:
#
# Date         Ver#   Modified By(Name)            Change and Reason for Change
# ---------   -----  ---------------------------  ------------------------------
# 20160712      .90  Kevin Oaks                   Created from dw_infra.migrate_sa_data2_utility.ksh
#
###########################################################################################################

typeset -fu usage
typeset -fu mvdatatodatashare
typeset -fu lntodatashare

usage () {
  print "Fatal Error: Invalid Usage"
  print "Usage: <ETL ID>"
  print "Note: ETL ID cannot be empty string."
  exit 4
}


mvdatatodatashare () {

data_dir=$1
datashare_dir=$2

print "INFO: Attempting move of $data_dir to $datashare_dir." 2>&1 | tee -a $LOG_FILE

if [[ -L $data_dir ]]
then
  print "INFO: $data_dir is symbolic link. Assuming this is a restart." 2>&1 | tee -a $LOG_FILE
elif [[ ! -d $data_dir ]]
then
  print "WARNING: $data_dir does not exist. Nothing to move" 2>&1 | tee -a $LOG_FILE
else

  print "INFO: Checking ownership of $data_dir." 2>&1 | tee -a $LOG_FILE
  if [[ ! -O $data_dir ]]
  then
    print "FATAL_ERROR: $myName not owner of $data_dir. Please resolve ownership issue before proceeding." 2>&1 | tee -a $LOG_FILE
    exit 205
  fi

  # if the target dir already exists, move the data from source dir to target dir, otherwise move the dir

  if [[ -d $data_dir && ! -d $datashare_dir ]]
  then
    set +e
    mv $data_dir $datashare_dir 2>&1 | tee -a $LOG_FILE
    _rcode=$?
    set -e
  elif [[  -d $data_dir && -d $datashare_dir ]]
  then
    set +e
    cp -a --backup=t $data_dir/* $datashare_dir 2>&1 | tee -a $LOG_FILE
    _rcode=$?
    set -e
  fi

  if [[ $_rcode -eq 0 ]]
  then
    print "INFO: Successful move of $data_dir to $datashare_dir." 2>&1 | tee -a $LOG_FILE
    if [[ -d $data_dir ]]
    then
      print "INFO: Removing original $data_dir after successful move to $datashare_dir." 2>&1 | tee -a $LOG_FILE
      rm -fR $data_dir
    fi
  else
    print "FATAL ERROR: Unable to move $data_dir to $datashare_dir." 2>&1 | tee -a $LOG_FILE
    exit 201
  fi

fi

}


lntodatashare () {

datashare_dir=$1
data_dir=$2

print "INFO: Attempting to link $datashare_dir to $data_dir." 2>&1 | tee -a $LOG_FILE

if [[ -L $data_dir ]]
then
  print "INFO: symlink $data_dir already exists. Assuming this is restart." 2>&1 | tee -a $LOG_FILE
elif [[ ! -d $data_dir ]]
then

  # In some cases original environment setup is incomplete. If datashare dir does not exist, it is
  # because data dir never existed. In this case, create datashare dir so that created link will not
  # be a pointer to nothing


  if [[ ! -d $datashare_dir ]]
  then

    print "WARNING: $datashare_dir does not exist. Creating $datashare_dir." 2>&1 | tee -a $LOG_FILE

    set +e
    mkdir -p $datashare_dir 2>&1 | tee -a $LOG_FILE
    _rcode=$?
    set -e

    if [[ $_rcode -ne 0 ]]
    then
      print "FATAL ERROR: $datashare_dir does not exist and unable to create." 2>&1 | tee -a $LOG_FILE
      exit 304 
    fi

  fi

  print "INFO: Creating symlink from $data_dir to $datashare_dir."  2>&1 | tee -a $LOG_FILE

  set +e
  ln -s $datashare_dir $data_dir 2>&1 | tee -a $LOG_FILE
  _rcode=$?
  set -e

  if [[ $_rcode -eq 0 ]]
  then
    print "INFO: Successfuly linked $datashare_dir to $data_dir." 2>&1 | tee -a $LOG_FILE
  else
    print "FATAL ERROR: Unable to link $datashare_dir to $data_dir." 2>&1 | tee -a $LOG_FILE
    exit 305 
  fi

else
  print "FATAL ERROR: Undefined error - Unable to link $datashare_dir to $data_dir." 2>&1 | tee -a $LOG_FILE
  exit 306 
fi
  
}

ARGC=$#
if [[ $ARGC -ne 1 ]]
then
        print "usage: ${0##*/} <ETL_ID>"
        print "ETL_ID can be , mock up as long as subject area is valid, i.e. <subject_area>.foo"
        exit 1
fi

ETL_ID=$1

. /dw/etl/mstr_cfg/etlenv.setup

export LOG_TIME=`date +%Y%m%d%H%M%S`
export RUN_DIR=$DW_MASTER_EXE/infra
export LOG_FILE=$DW_LOG/extract/$SUBJECT_AREA/${0##*/}.${SUBJECT_AREA}.${servername}.$LOG_TIME.log
export myName=$(whoami)

print "ETL_ID=$SUBJECT_AREA" 2>&1 | tee -a $LOG_FILE
print "ETL_ENV=$ETL_ENV" 2>&1 | tee -a $LOG_FILE
print "LOG_FILE=$LOG_FILE" 2>&1 | tee -a $LOG_FILE

# Define Data and Datashare base structures

print "INFO: Defining base source data structures" 2>&1 | tee -a $LOG_FILE

export DATA_IN=/data/etl/home/${ETL_ENV}/in
export DATA_OUT=/data/etl/home/${ETL_ENV}/out
export DATA_ARC=/data/etl/home/${ETL_ENV}/arc
export DATA_TMP=/data/etl/home/${ETL_ENV}/tmp

env | grep "^DATA_" 2>&1 | tee -a $LOG_FILE

print "INFO: Defining base target data structures" 2>&1 | tee -a $LOG_FILE

export DATASHARE_IN=/datashare/etl/${ETL_ENV}/in 
export DATASHARE_OUT=/datashare/etl/${ETL_ENV}/out
export DATASHARE_ARC=/datashare/etl/${ETL_ENV}/arc
export DATASHARE_TMP=/datashare/etl/${ETL_ENV}/tmp

env | grep "^DATASHARE_" 2>&1 | tee -a $LOG_FILE

# move existing datasets on data mount to datashare mount and then link datashare to data

mvdatatodatashare $DATA_IN/extract/$SUBJECT_AREA $DATASHARE_IN/extract/$SUBJECT_AREA
lntodatashare $DATASHARE_IN/extract/$SUBJECT_AREA $DATA_IN/extract/$SUBJECT_AREA

mvdatatodatashare $DATA_OUT/extract/$SUBJECT_AREA $DATASHARE_OUT/extract/$SUBJECT_AREA
lntodatashare $DATASHARE_OUT/extract/$SUBJECT_AREA $DATA_OUT/extract/$SUBJECT_AREA

mvdatatodatashare $DATA_ARC/extract/$SUBJECT_AREA $DATASHARE_ARC/extract/$SUBJECT_AREA
lntodatashare $DATASHARE_ARC/extract/$SUBJECT_AREA $DATA_ARC/extract/$SUBJECT_AREA

mvdatatodatashare $DATA_TMP/extract/$SUBJECT_AREA $DATASHARE_TMP/extract/$SUBJECT_AREA
lntodatashare $DATASHARE_TMP/extract/$SUBJECT_AREA $DATA_TMP/extract/$SUBJECT_AREA


while read JOB_ENV
do

  mvdatatodatashare $DATA_ARC/$JOB_ENV/$SUBJECT_AREA $DATASHARE_ARC/$JOB_ENV/$SUBJECT_AREA
  lntodatashare $DATASHARE_ARC/$JOB_ENV/$SUBJECT_AREA $DATA_ARC/$JOB_ENV/$SUBJECT_AREA

  mvdatatodatashare $DATA_TMP/$JOB_ENV/$SUBJECT_AREA $DATASHARE_TMP/$JOB_ENV/$SUBJECT_AREA
  lntodatashare $DATASHARE_TMP/$JOB_ENV/$SUBJECT_AREA $DATA_TMP/$JOB_ENV/$SUBJECT_AREA

done < $RUN_DIR/dw_infra.create_infra_env_x86.targets.lis


print "INFO: Data migration from /data to /datashare complete for subject area $SUBJECT_AREA on server ${servername}." 2>&1 | tee -a $LOG_FILE

exit
