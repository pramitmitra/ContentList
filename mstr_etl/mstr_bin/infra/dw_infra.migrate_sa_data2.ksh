#!/bin/ksh -eu

##########################################################################################################
##########################################################################################################
#
#
# Title:        Subject Area Data to Data2 Migration Utility 
# File Name:    dw_infra.migrate_sa_data2i_utility.ksh 
# Description:  This script facilitates the migration of data (in, out), tmp, and arc files from the 
#               original /data mount on Tempo hardware to the newer /data2 mount. This is intended to
#               alleviate space concerns on the Tempo hardware, and should only be used with guidance
#               from the GDI-DINT ETL Infrastucture team (DL-eBay-GDI-DINT@ebay.com).
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
#---------    -----  ---------------------------  ------------------------------
# 2015-05-18   0.1    Kevin Oaks                   Initial
# 2015-05-22   0.2    Kevin Oaks                   Tested logic on test server
# 2015-05-28   0.3    Kevin Oaks                   Converted logic to functions
# 2015-06-18   0.5    Kevin Oaks                   Simplified logic after testing to
#                                                  simply mv existing data. Performance
#                                                  is comparible to cp -rp; rm -fR with
#                                                  less risk of catastrophic data removal
# 2015-07-23   1.0    Kevin Oaks                   Added logging and distinct error codes
#                                                  for troubleshooting failures. Should be
#                                                  ready for primetime.
#
###############################################################################################

typeset -fu usage
typeset -fu mvdatatodata2
typeset -fu lndata2data

usage () {
  print "Fatal Error: Invalid Usage"
  print "Usage: <ETL ID> [etl env]"
  print "Note: ETL ID cannot be empty string."
  exit 4
}


mvdatatodata2 () {

data_dir=$1
data2_dir=$2

print "INFO: Attempting move of $data_dir to $data2_dir." 2>&1 | tee -a $LOG_FILE

if [[ -L $data_dir ]]
then
  print "INFO: $data_dir is symbolic link. Assuming this is a restart." 2>&1 | tee -a $LOG_FILE
elif [[ ! -d $data_dir ]]
then
  print "WARNING: $data_dir does not exist. Nothing to move" 2>&1 | tee -a $LOG_FILE
elif [[ -d $data_dir && -d $data2_dir ]]
then
  print "FATAL ERROR: $data_dir and $data2_dir both exist! May need manual recovery from previous run."  2>&1 | tee -a $LOG_FILE
  exit 204
else

  set +e
  mv $data_dir $data2_dir 2>&1 | tee -a $LOG_FILE
  _rcode=$?
  set -e

  if [[ $_rcode -eq 0 ]]
  then
    print "INFO: Successful move of $data_dir to $data2_dir." 2>&1 | tee -a $LOG_FILE
  else
    print "FATAL ERROR: Unable to move $data_dir to $data2_dir." 2>&1 | tee -a $LOG_FILE
    exit 205
  fi

fi

}


lndata2data () {

data2_dir=$1
data_dir=$2

print "INFO: Attempting to link $data2_dir to $data_dir." 2>&1 | tee -a $LOG_FILE

if [[ -L $data_dir ]]
then
  print "INFO: symlink $data_dir already exists. Assuming this is restart." 2>&1 | tee -a $LOG_FILE
elif [[ ! -d $data_dir ]]
then

  # In some cases original environment setup is incomplete. If data2 dir does not exist, it is
  # because data dir never existed. In this case, create data2 dir so that created link will not
  # be a pointer to nothing


  if [[ ! -d $data2_dir ]]
  then

    print "WARNING: $data2_dir does not exist. Creating $data2_dir." 2>&1 | tee -a $LOG_FILE

    set +e
    mkdir -p $data2_dir 2>&1 | tee -a $LOG_FILE
    _rcode=$?
    set -e

    if [[ $_rcode -ne 0 ]]
    then
      print "FATAL ERROR: $data2_dir does not exist and unable to create." 2>&1 | tee -a $LOG_FILE
      exit 304 
    fi

  fi

  print "INFO: Creating symlink from $data_dir to $data2_dir."  2>&1 | tee -a $LOG_FILE

  set +e
  ln -s $data2_dir $data_dir 2>&1 | tee -a $LOG_FILE
  _rcode=$?
  set -e

  if [[ $_rcode -eq 0 ]]
  then
    print "INFO: Successfuly linked $data2_dir to $data_dir." 2>&1 | tee -a $LOG_FILE
  else
    print "FATAL ERROR: Unable to link $data2_dir to $data_dir." 2>&1 | tee -a $LOG_FILE
    exit 305 
  fi

else
  print "FATAL ERROR: Undefined error - Unable to link $data2_dir to $data_dir." 2>&1 | tee -a $LOG_FILE
  exit 306 
fi
  
}

ARGC=$#
if [[ $ARGC -lt 1 || $ARGC -gt 2 || -z $1 ]]
then
  usage
fi

ETL_ID=$1
T_ETL_ENV=${2:-prod}

. /dw/etl/mstr_cfg/etlenv.setup

export LOG_TIME=`date +%Y%m%d%H%M%S`
export RUN_DIR=$DW_MASTER_EXE/infra
export LOG_FILE=$DW_MASTER_LOG/${0##*/}.${SUBJECT_AREA}.$LOG_TIME.log

print "ETL_ID=$ETL_ID" 2>&1 | tee -a $LOG_FILE
print "T_ETL_ENV=$T_ETL_ENV" 2>&1 | tee -a $LOG_FILE
print "LOG_FILE=$LOG_FILE" 2>&1 | tee -a $LOG_FILE

# Define Data and Data2 base structures

print "INFO: Defining base source data structures" 2>&1 | tee -a $LOG_FILE

export DATA_IN=/data/etl/home/${T_ETL_ENV}/in
export DATA_OUT=/data/etl/home/${T_ETL_ENV}/out
export DATA_ARC=/data/etl/home/${T_ETL_ENV}/arc
export DATA_TMP=/data/etl/home/${T_ETL_ENV}/tmp

env | grep DATA_ 2>&1 | tee -a $LOG_FILE

print "INFO: Defining base target data structures" 2>&1 | tee -a $LOG_FILE

export DATA2_IN=/data2/etl/home/${T_ETL_ENV}/in 
export DATA2_OUT=/data2/etl/home/${T_ETL_ENV}/out
export DATA2_ARC=/data2/etl/home/${T_ETL_ENV}/arc
export DATA2_TMP=/data2/etl/home/${T_ETL_ENV}/tmp

env | grep DATA2_ 2>&1 | tee -a $LOG_FILE

# move existing datasets on data mount to data2 mount and then link data2 to data

mvdatatodata2 $DATA_IN/extract/$SUBJECT_AREA $DATA2_IN/extract/$SUBJECT_AREA
lndata2data $DATA2_IN/extract/$SUBJECT_AREA $DATA_IN/extract/$SUBJECT_AREA

mvdatatodata2 $DATA_OUT/extract/$SUBJECT_AREA $DATA2_OUT/extract/$SUBJECT_AREA
lndata2data $DATA2_OUT/extract/$SUBJECT_AREA $DATA_OUT/extract/$SUBJECT_AREA

mvdatatodata2 $DATA_ARC/extract/$SUBJECT_AREA $DATA2_ARC/extract/$SUBJECT_AREA
lndata2data $DATA2_ARC/extract/$SUBJECT_AREA $DATA_ARC/extract/$SUBJECT_AREA

mvdatatodata2 $DATA_TMP/extract/$SUBJECT_AREA $DATA2_TMP/extract/$SUBJECT_AREA
lndata2data $DATA2_TMP/extract/$SUBJECT_AREA $DATA_TMP/extract/$SUBJECT_AREA


while read JOB_ENV
do

  mvdatatodata2 $DATA_ARC/$JOB_ENV/$SUBJECT_AREA $DATA2_ARC/$JOB_ENV/$SUBJECT_AREA
  lndata2data $DATA2_ARC/$JOB_ENV/$SUBJECT_AREA $DATA_ARC/$JOB_ENV/$SUBJECT_AREA

  mvdatatodata2 $DATA_TMP/$JOB_ENV/$SUBJECT_AREA $DATA2_TMP/$JOB_ENV/$SUBJECT_AREA
  lndata2data $DATA2_TMP/$JOB_ENV/$SUBJECT_AREA $DATA_TMP/$JOB_ENV/$SUBJECT_AREA

done < $RUN_DIR/dw_infra.create_infra_env_x86.targets.lis


print "INFO: Data migration from /data to /data2 complete for subject area $SUBJECT_AREA on server ${servername}." 2>&1 | tee -a $LOG_FILE

exit
