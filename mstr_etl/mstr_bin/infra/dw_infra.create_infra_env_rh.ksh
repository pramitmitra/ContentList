#!/bin/ksh -eu
###################################################################################################################
#
#
#  P l e a s e   d o n ' t   u s e   t h i s   s c r i p t ! 
#  P l e a s e   d o n ' t   u s e   t h i s   s c r i p t ! 
#  P l e a s e   d o n ' t   u s e   t h i s   s c r i p t ! 
#  P l e a s e   d o n ' t   u s e   t h i s   s c r i p t ! 
#
#  The correct script for defining dirs in RedHat/CentOS is dw_infra.create_infra_env_x86.ksh in the RedHat-Port repo
#  The correct script for defining dirs in RedHat/CentOS is dw_infra.create_infra_env_x86.ksh in the RedHat-Port repo
#  The correct script for defining dirs in RedHat/CentOS is dw_infra.create_infra_env_x86.ksh in the RedHat-Port repo
#  The correct script for defining dirs in RedHat/CentOS is dw_infra.create_infra_env_x86.ksh in the RedHat-Port repo
#
#
#
#
#
#
#
#
#
#
#
#
# Title:        DW_INFRA Create Infrastructure Environment 
# File Name:    dw_infra.create_infra_env_rh.ksh  
# Description:  Script for creating base infrastructure directory hierarchy on standard ETL systems  
# Developer:    Kevin Oaks
# Created on:   2012-09-01
# Location:     $DW_MASTER_EXE
#
# Date         Ver#   Modified By(Name)            Change and Reason for Change
#---------    -----  ---------------------------  ------------------------------
# 2012-09-01   1.0    Kevin Oaks                    Ported to RedHat from original dw_infra.create_infra_env.ksh:
#                                                    - now using /bin/ksh rather than /usr/bin/ksh
#                                                    - converted echo statements to print
#                                                    - deprecated legacy in01/out01 links
#                                                    - deprecated primary/secondary links
###################################################################################################################

ARGC=$#
if [ $ARGC -lt 1 -o $ARGC -gt 2 ]
then
	print "usage: ${0##*/} <etl env> [owner id]"
	exit 1
fi

ETL_ENV=$1
OWNER_ID=${2:-}

function mkdirifnotexist {
_dir=$1

if [ ! -d $_dir ]
then
  mkdir -p $_dir
  print "created directory $_dir"
else
  print "directory $_dir already exists"
fi
}

#running dir
RUN_DIR=`dirname $0`

# Define Mount Devices (these can be logical or physical) 
BASE_DIR=/dw/etl/home
DATA_DIR=/dw/etl
ARC_DIR=/dw/etl/arc
LOG_DIR=/dw/etl/log

# Define Home ETL Dirs that land on mounts
HOME_DIR=$BASE_DIR/$ETL_ENV
HOME_ARC_DIR=$ARC_DIR/$ETL_ENV
HOME_LOG_DIR=$LOG_DIR/$ETL_ENV
HOME_IN_DIR=$DATA_DIR/in/$ETL_ENV
HOME_OUT_DIR=$DATA_DIR/out/$ETL_ENV

# Define legacy IN01/OUT01 dirs - Deprecated for RedHat
# HOME_IN01_DIR=$DATA_DIR/in01/$ETL_ENV
# HOME_OUT01_DIR=$DATA_DIR/out01/$ETL_ENV

# Remove Home ETL dirs if they exist
rm -fR $HOME_DIR
rm -fR $HOME_ARC_DIR
rm -fR $HOME_LOG_DIR
rm -fR $HOME_IN_DIR
rm -fR $HOME_OUT_DIR

# Master Level directories - Create if they do not already exist
mkdirifnotexist $DATA_DIR/mstr_bin
mkdirifnotexist $DATA_DIR/mstr_cfg
mkdirifnotexist $DATA_DIR/mstr_dat
mkdirifnotexist $DATA_DIR/mstr_dat/sft
mkdirifnotexist $DATA_DIR/mstr_lib
mkdirifnotexist $DATA_DIR/mstr_src
mkdirifnotexist $DATA_DIR/mstr_tmp
mkdirifnotexist $DATA_DIR/mstr_log
mkdirifnotexist $DATA_DIR/mstr_log/sft

# code directories
mkdir -p $HOME_DIR/bin
mkdir -p $HOME_DIR/cfg
mkdir -p $HOME_DIR/cmp
mkdir -p $HOME_DIR/dbc
mkdir -p $HOME_DIR/dml
mkdir -p $HOME_DIR/lib
mkdir -p $HOME_DIR/.logins
mkdir -p $HOME_DIR/mp
mkdir -p $HOME_DIR/sql
mkdir -p $HOME_DIR/src
mkdir -p $HOME_DIR/xfr

STATE_DIR=$DATA_DIR/state
HOME_STATE_DIR=$STATE_DIR/$ETL_ENV

MFS_HOME=$DATA_DIR/mfs_home
HOME_MFS_DIR=$MFS_HOME/$ETL_ENV

rm -fR $HOME_STATE_DIR
rm -fR $HOME_MFS_DIR

# create extract directories
mkdir -p $HOME_STATE_DIR/dat/extract
mkdir -p $HOME_STATE_DIR/tmp/extract
mkdir -p $HOME_STATE_DIR/watch/extract
mkdir -p $HOME_LOG_DIR/extract
mkdir -p $HOME_ARC_DIR/extract

# make data directories
mkdir -p $HOME_IN_DIR/extract
mkdir -p $HOME_OUT_DIR/extract
mkdir -p $HOME_IN_DIR/land


# mfs
mkdir -p $HOME_MFS_DIR

# chown
if [ $OWNER_ID ]
then
	chown -fR $OWNER_ID $HOME_STATE_DIR
	chown -fR $OWNER_ID $HOME_MFS_DIR
	chown -fR $OWNER_ID $HOME_LOG_DIR
fi

# link state directories
ln -s $HOME_STATE_DIR/dat $HOME_DIR/dat
ln -s $HOME_STATE_DIR/tmp $HOME_DIR/tmp
ln -s $HOME_STATE_DIR/watch $HOME_DIR/watch

# link log directories
ln -s $HOME_LOG_DIR $HOME_DIR/log 

# link mfs dir
ln -s $HOME_MFS_DIR $HOME_DIR/mfs

# link land directory
ln -s $HOME_IN_DIR/land $HOME_DIR/land

# lin in/out dirs
ln -s $HOME_IN_DIR $HOME_DIR/in
ln -s $HOME_OUT_DIR $HOME_DIR/out

# link legacy 01 devices - Deprecated for RedHat
# ln -s $HOME_IN_DIR $HOME_DIR/in01
# ln -s $HOME_OUT_DIR $HOME_DIR/out01

# link archive directories
ln -s $HOME_ARC_DIR $HOME_DIR/arc

# make /link target directories
while read TARGET
do

#  Deprecated for RedHat
#  if [[ $TARGET == @(primary|secondary) ]]
#  then
#
#    case $TARGET in
#      primary) ln -s $HOME_STATE_DIR/dat/td1 $HOME_STATE_DIR/dat/$TARGET
#               ln -s $HOME_STATE_DIR/tmp/td1 $HOME_STATE_DIR/tmp/$TARGET
#               ln -s $HOME_STATE_DIR/watch/td1 $HOME_STATE_DIR/watch/$TARGET
#               ln -s $HOME_LOG_DIR/td1 $HOME_LOG_DIR/$TARGET
#               ln -s $HOME_ARC_DIR/td1 $HOME_ARC_DIR/$TARGET;;
#      secondary) ln -s $HOME_STATE_DIR/dat/td2 $HOME_STATE_DIR/dat/$TARGET
#                 ln -s $HOME_STATE_DIR/tmp/td2 $HOME_STATE_DIR/tmp/$TARGET
#                 ln -s $HOME_STATE_DIR/watch/td2 $HOME_STATE_DIR/watch/$TARGET
#                 ln -s $HOME_LOG_DIR/td2 $HOME_LOG_DIR/$TARGET
#                 ln -s $HOME_ARC_DIR/td2 $HOME_ARC_DIR/$TARGET;;
#    esac
#
#  else

    # state directories
    mkdir -p $HOME_STATE_DIR/dat/$TARGET
    mkdir -p $HOME_STATE_DIR/tmp/$TARGET
    mkdir -p $HOME_STATE_DIR/watch/$TARGET

    # log directories
    mkdir -p $HOME_LOG_DIR/$TARGET

    # make archive directories
    mkdir -p $HOME_ARC_DIR/$TARGET
#  fi

  # link data directories
  ln -s $HOME_OUT_DIR/extract $HOME_OUT_DIR/$TARGET
  ln -s $HOME_IN_DIR/extract $HOME_IN_DIR/$TARGET

done < $RUN_DIR/dw_infra.create_infra_env_rh.targets.lis

if [ $OWNER_ID ]
then
	chown -fR $OWNER_ID $HOME_DIR
	chown -fR $OWNER_ID $HOME_ARC_DIR
	chown -fR $OWNER_ID $HOME_IN_DIR
	chown -fR $OWNER_ID $HOME_OUT_DIR
fi

exit
