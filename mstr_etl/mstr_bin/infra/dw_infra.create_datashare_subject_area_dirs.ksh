#!/usr/bin/ksh -eux
###################################################################################################################
#
# Title:        DW_INFRA Create Datashare Subject Area Extension
# File Name:    dw_infra.create_datashare_subject_area_dirs.ksh
# Description:  Script for creating SUBJECT_AREA directories under /datashare hierarchy
# on standard ETL systems
#
# Recommended instructions for running this script:
#
#		This script assumes that the standard /datashare/etl/home/$ETL_ENV hierarchy exists
#
# Developer:    Kevin Oaks 
# Created on:   2016-01-14
# Location:     $DW_MASTER_EXE/infra
#
# Date         Ver#   Modified By(Name)            Change and Reason for Change
# ----------   -----  ---------------------------  ------------------------------
# 2016-01-14   1.0    Kevin Oaks                   Initial Creation 
# 2016-06-23   1.1    Kevin Oaks                   Made ETL_ENV discovery dynamic
#
###################################################################################################################

function mkdirifnotexist {
_dir=$1

if [ ! -d $_dir ]
then
  mkdir $_dir
  echo "created directory $_dir"
else
  echo "directory $_dir already exists"
fi
}

ARGC=$#
if [[ $ARGC -ne 1 ]]
then
        print "usage: ${0##*/} <SUBJECT_AREA>"
        exit 1
fi

. /dw/etl/mstr_cfg/etlenv.setup

SUBJECT_AREA=$1

# Define Mount Devices (these can be logical or physical) 
BASE_DIR=/datashare/etl/$ETL_ENV

# create extract directories
mkdirifnotexist $BASE_DIR/tmp/extract/$SUBJECT_AREA
mkdirifnotexist $BASE_DIR/arc/extract/$SUBJECT_AREA

# make data directories
mkdirifnotexist $BASE_DIR/in/extract/$SUBJECT_AREA
mkdirifnotexist $BASE_DIR/out/extract/$SUBJECT_AREA

# make additional target directories
while read TARGET
do

  # state directories
  mkdirifnotexist $BASE_DIR/tmp/$TARGET/$SUBJECT_AREA

  # make archive directories
  mkdirifnotexist $BASE_DIR/arc/$TARGET/$SUBJECT_AREA

done < $DW_MASTER_EXE/infra/dw_infra.create_infra_env_x86.targets.lis

exit
