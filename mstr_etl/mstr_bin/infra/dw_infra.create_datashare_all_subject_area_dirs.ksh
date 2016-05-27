#!/usr/bin/ksh -eux
###################################################################################################################
#
# Title:        DW_INFRA Create Datashare ALL Subject Area Extension
# File Name:    dw_infra.create_datashare_all_subject_area_dirs.ksh
# Description:  Script for creating ALL SUBJECT_AREA directories under /datashare hierarchy
# on standard ETL systems. This creates subject area directories in this hierachy based on their
# current existence in $DW_CFG
#
# Recommended instructions for running this script:
#
#	This script assumes that the standard /datashare/etl/$ETL_ENV hierarchy exists
#	Should use dw_adm or system specific operational user to execute
#
# Developer:    Kevin Oaks 
# Created on:   2016-05-20
# Location:     $DW_MASTER_EXE/infra
#
# Date         Ver#   Modified By(Name)            Change and Reason for Change
# ----------   -----  ---------------------------  ------------------------------
# 2016-05-20   1.0    Kevin Oaks                   Initial Creation from dw_infra.create_infra_datashare_subject_area_dirs.ksh
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
if [[ $ARGC -gt 0 ]]
then
        print "usage: ${0##*/}"
        exit 1
fi

. /dw/etl/mstr_cfg/etlenv.setup

# Define Mount Devices (these can be logical or physical) 
BASE_DIR=/datashare/etl/$ETL_ENV

for fn in $DW_CFG/*
do

  if [[ -d $fn ]]
  then
    SUBJECT_AREA=${fn#$DW_CFG/}

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

  fi

done

exit
