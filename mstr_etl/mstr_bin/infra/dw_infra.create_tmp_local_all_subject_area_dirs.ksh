#!/usr/bin/ksh -eux
###################################################################################################################
#
# Title:        DW_INFRA Create tmp_local all subject area dirs Extension 
# File Name:    dw_infra.create_tmp_local_all_subject_area_dirs.ksh 
# Description:  Script for extending tmp_local to the base infrastructure hierarchy
# on standard ETL systems  
#
#
# To add more target directories (e.g., td9, hd7, etc.) later, see extend_x86_multi_env.ksh
#
#
# Recommended instructions for running this script:
#
#		This script assumes that the standard base infrastructure hierarchy exists and
#               is owned by dw_infra:dw_ops.
#               This script should be executed using dw_adm
#
# Developer:    Kevin Oaks 
# Created on:   2016-05-18
# Location:     $DW_MASTER_EXE/infra
#
# Date         Ver#   Modified By(Name)            Change and Reason for Change
# ----------   -----  ---------------------------  ------------------------------
# 2016-05-18   1.0    Kevin Oaks                   Initial Creation 
#
###################################################################################################################



ARGC=$#
if [ $ARGC -gt 0 ]
then
        print "usage: ${0##*/}"
        exit 1
fi

. /dw/etl/mstr_cfg/etlenv.setup

function mkdirifnotexist {
_dir=$1

if [ ! -d $_dir ]
then
  mkdir -p -m $DEF_PERMS $_dir
  echo "created directory $_dir"
else
  echo "directory $_dir already exists"
fi
}

function lnifnotexist {
_dir1=$1 _dir2=$2

if [ ! -L $_dir2 ]
then
  ln -s $_dir1 $_dir2
  echo "created link $_dir2 --> $_dir1"
else
  echo "link $_dir2 --> $_dir1 already exists"
fi
}

# Set default directory permission, depending on ETL_ENV
# In infra and dev environments, directory permissions should default to 777; in qa and prod, default to 775
# At end of script, directories which should *not* follow default permissions are explicitly set with chmod

if [[ $ETL_ENV == @(infra|dev) ]]
then
  DEF_PERMS=1777
else
  DEF_PERMS=1775
fi

# Define BASE directory 
BASE_DIR=/dw/etl/home/$ETL_ENV/tmp_local


# create extract directories
mkdirifnotexist $BASE_DIR/extract

# make /link target directories
while read TARGET
do

  # state directories
  mkdirifnotexist $BASE_DIR/$TARGET

done < $DW_MASTER_EXE/infra/dw_infra.create_infra_env_x86.targets.lis

for fn in $DW_CFG/*
do

  if [[ -d $fn ]]
  then
    SUBJECT_AREA=${fn#$DW_CFG/}

    # create extract directories
    mkdirifnotexist $BASE_DIR/extract/$SUBJECT_AREA

    # make additional target directories
    while read TARGET
    do

      # state directories
      mkdirifnotexist $BASE_DIR/$TARGET/$SUBJECT_AREA

    done < $DW_MASTER_EXE/infra/dw_infra.create_infra_env_x86.targets.lis

  fi

done

exit
