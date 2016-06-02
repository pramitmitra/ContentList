#!/usr/bin/ksh -eux
###################################################################################################################
#
# Title:        DW_INFRA Create Datashare Extension 
# File Name:    dw_infra.create_datashare_extension.ksh  
# Description:  Script for creating extending base infrastructure data hierarchy to datashare
# on standard ETL systems  
#
#
# To add more target directories (e.g., td9, hd7, etc.) later, see extend_x86_multi_env.ksh
#
#
# Recommended instructions for running this script:
#
#		This script assumes that /datashare exists and is owned by dw_infra:dw_ops. If it does not exist,
#		sudo as root and create it, granting ownership to dw_infra:dw_ops.
#		This script should be run directly from $DW_MASTER_EXE/infra
#
# Developer:    Kevin Oaks 
# Created on:   2015-10-13
# Location:     $DW_MASTER_EXE/infra
#
# Date         Ver#   Modified By(Name)            Change and Reason for Change
# ----------   -----  ---------------------------  ------------------------------
# 2015-11-03   1.0    Kevin Oaks                   Initial Creation 
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

# Define Mount Devices (these can be logical or physical) 
BASE_DIR=/datashare/etl/$ETL_ENV

mkdirifnotexist $BASE_DIR

# code directories
mkdirifnotexist $BASE_DIR/arc
mkdirifnotexist $BASE_DIR/in
mkdirifnotexist $BASE_DIR/out
mkdirifnotexist $BASE_DIR/tmp

# create extract directories
mkdirifnotexist $BASE_DIR/tmp/extract
mkdirifnotexist $BASE_DIR/arc/extract

# make data directories
mkdirifnotexist $BASE_DIR/in/extract
mkdirifnotexist $BASE_DIR/out/extract

# make /link target directories
while read TARGET
do

  # state directories
  mkdirifnotexist $BASE_DIR/tmp/$TARGET

  # make archive directories
  mkdirifnotexist $BASE_DIR/arc/$TARGET

  # link data directories
  lnifnotexist $BASE_DIR/out/extract $BASE_DIR/out/$TARGET
  lnifnotexist $BASE_DIR/in/extract $BASE_DIR/in/$TARGET

done < $DW_MASTER_EXE/infra/dw_infra.create_infra_env_x86.targets.lis


# set permissions on any directories which differ from default permissions
chmod 1755 $BASE_DIR

exit
