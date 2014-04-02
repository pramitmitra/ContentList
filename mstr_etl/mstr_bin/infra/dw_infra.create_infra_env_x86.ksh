#!/usr/bin/ksh -eux
###################################################################################################################
#
# Title:        DW_INFRA Create Infrastructure Environment 
# File Name:    dw_infra.create_infra_env_x86.ksh  
# Description:  Script for creating base infrastructure directory hierarchy on standard ETL systems  
#
# Caution:      N o t   r e - r u n n a b l e ! ! !    (Wipes out existing directory structure)
# Caution:      N o t   r e - r u n n a b l e ! ! !    (Wipes out existing directory structure)
# Caution:      N o t   r e - r u n n a b l e ! ! !    (Wipes out existing directory structure)
# Caution:      N o t   r e - r u n n a b l e ! ! !    (Wipes out existing directory structure)
#
# To add more target directories (e.g., td9, hd7, etc.) later, see extend_x86_multi_env.ksh
#
#
# Recommended instructions for running this script:
#
#  1. As root, manually create /dw directory and transfer ownership to dw_infra (before running this script)
#     a.  sudoroot
#     b.  mkdir /dw
#     c.  chmod 755 /dw
#     d.  chown dw_infra:dw_ops /dw
#  2. As root (still), manually transfer ownership of external mounts to dw_infra (before running this script)
#     a.  chown dw_infra:dw_ops /XXXcode
#     b.  chown dw_infra:dw_ops /XXXland
#     c.  chown dw_infra:dw_ops /XXXlogs
#     d.  chown dw_infra:dw_ops /XXXwatch
#     Where XXX is one of inf, dev, qa or prod
#  3. As dw_infra, execute this script
#     a.  sudodw_infra
#     b.  Create a copy of this script in /export/home/dw_infra (since directories don't exist yet)
#     c.  Create a copy of dw_infra.create_infra_env_x86.targets.lis in same directory
#     d.  ksh dw_infra.create_infra_env_x86.ksh <ETL ENV> > dw_infra.create_infra_env_x86.log
#         i. <ETL ENV> would typically be infra, dev, qa or prod
#
#
# Developer:    John Hackley
# Created on:   2013-09-15
# Location:     $DW_MASTER_EXE
#
# Date         Ver#   Modified By(Name)            Change and Reason for Change
# ---------    -----  ---------------------------  ------------------------------
# 2013-09-15   1.0    John Hackley                 Cloned from same file in legacy repository and changed:
#                                                    - now using 4 external mount points
#                                                    - deprecated legacy in01/out01 links
#                                                    - deprecated primary/secondary links
# 2013-11-06   1.1    John Hackley                 Added instructions; set directory-level perms
# 2013-11-13   1.2    John Hackley                 Changed perms for a few directories and set sticky bit for all
#                                                  Also added subdirectories below /dw/etl/mstr_log
# 2013-11-15   1.3    John Hackley                 Changed perms for /dw/etl/mstr_log/jobtrack and sft to 1775
# 2013-12-03   1.4    John Hackley                 Changed .logins from 1750 to 1770 in qa and prod
# 2014-01-15   1.5    John Hackley                 Changed /dw/etl/home/XXX/dat to be on external storage
#
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

#running dir
RUN_DIR=`dirname $0`


# Determine names of mount points based on ETL_ENV
#
# For prod,  mount points are prodcode, prodland, prodlogs and prodwatch
# For qa,    mount points are qacode,   qaland,   qalogs   and qawatch
# For dev,   mount points are devcode,  devland,  devlogs  and devwatch
# For infra, mount points are infcode,  infland,  invlogs  and infwatch

if [[ $ETL_ENV == infra ]]
then
  MOUNT_PREFIX=inf
else
  MOUNT_PREFIX=$ETL_ENV
fi

# Set default directory permission, depending on ETL_ENV
# In infra and dev environments, directory permissions should default to 777; in qa and prod, default to 775
# At end of script, directories which should *not* follow default permissions are explicitly set with chmod

if [[ $ETL_ENV == @(infra|dev) ]]
then
  DEF_PERMS=1777
else
  DEF_PERMS=1775
fi

# Define variables for external storage

# Path to "MASTER" infra code on external storage
EXT_MSTR_CODE_DIR=/${MOUNT_PREFIX}code/infra

# Path to ETL Developer code on external storage
EXT_USER_CODE_DIR=/${MOUNT_PREFIX}code/user

# Path to incoming data files on external storage
EXT_LAND_DIR=/${MOUNT_PREFIX}land

# Path to "MASTER" job logs on external storage
EXT_MSTR_LOG_DIR=/${MOUNT_PREFIX}logs/infra

# Path to job logs on external storage
EXT_USER_LOG_DIR=/${MOUNT_PREFIX}logs/user

# Path to Watch files on external storage
EXT_WATCH_DIR=/${MOUNT_PREFIX}watch


# Define Mount Devices (these can be logical or physical) 
BASE_DIR=/dw/etl/home
DATA_DIR=/dw/etl
ARC_DIR=/dw/etl/arc
LOG_DIR=/dw/etl/log

# Define Home ETL Dirs that land on mounts
HOME_DIR=$BASE_DIR/$ETL_ENV
HOME_ARC_DIR=$ARC_DIR/$ETL_ENV
HOME_IN_DIR=$DATA_DIR/in/$ETL_ENV
HOME_OUT_DIR=$DATA_DIR/out/$ETL_ENV

# Define legacy IN01/OUT01 dirs
HOME_IN01_DIR=$DATA_DIR/in01/$ETL_ENV
HOME_OUT01_DIR=$DATA_DIR/out01/$ETL_ENV

echo External Directories:
echo "  " EXT_MSTR_CODE_DIR=$EXT_MSTR_CODE_DIR
echo "  " EXT_USER_CODE_DIR=$EXT_USER_CODE_DIR
echo "  " EXT_LAND_DIR=$EXT_LAND_DIR
echo "  " EXT_MSTR_LOG_DIR=$EXT_MSTR_LOG_DIR
echo "  " EXT_USER_LOG_DIR=$EXT_USER_LOG_DIR
echo "  " EXT_WATCH_DIR=$EXT_WATCH_DIR

# Remove Home ETL dirs if they exist
rm -fR $HOME_DIR
#rm -fR $HOME_ARC_DIR
rm -fR $EXT_USER_LOG_DIR
#rm -fR $HOME_IN_DIR
#rm -fR $HOME_OUT_DIR

mkdir -p -m $DEF_PERMS $HOME_DIR

# Master Level directories - Create if they do not already exist
mkdirifnotexist $EXT_MSTR_CODE_DIR/bin
mkdirifnotexist $EXT_MSTR_CODE_DIR/cfg
mkdirifnotexist $EXT_MSTR_CODE_DIR/lib
mkdirifnotexist $EXT_MSTR_CODE_DIR/src
mkdirifnotexist $EXT_MSTR_LOG_DIR
mkdirifnotexist $EXT_MSTR_LOG_DIR/sft
mkdirifnotexist $EXT_MSTR_LOG_DIR/jobtrack/land

mkdirifnotexist $DATA_DIR/mstr_dat
mkdirifnotexist $DATA_DIR/mstr_dat/sft
mkdirifnotexist $DATA_DIR/mstr_tmp

# code directories
mkdir -p -m $DEF_PERMS $HOME_DIR/arc
mkdir -p -m $DEF_PERMS $EXT_USER_CODE_DIR/bin
mkdir -p -m $DEF_PERMS $EXT_USER_CODE_DIR/cfg
mkdir -p -m $DEF_PERMS $EXT_USER_CODE_DIR/cmp
mkdir -p -m $DEF_PERMS $EXT_WATCH_DIR/dat
mkdir -p -m $DEF_PERMS $EXT_USER_CODE_DIR/dbc
mkdir -p -m $DEF_PERMS $EXT_USER_CODE_DIR/dml
mkdir -p -m $DEF_PERMS $HOME_DIR/in
mkdir -p -m $DEF_PERMS $EXT_USER_CODE_DIR/lib
mkdir -p -m $DEF_PERMS $EXT_USER_CODE_DIR/.logins
mkdir -p -m $DEF_PERMS $EXT_USER_CODE_DIR/mp
mkdir -p -m $DEF_PERMS $HOME_DIR/out
mkdir -p -m $DEF_PERMS $EXT_USER_CODE_DIR/sql
mkdir -p -m $DEF_PERMS $EXT_USER_CODE_DIR/src
mkdir -p -m $DEF_PERMS $HOME_DIR/tmp
mkdir -p -m $DEF_PERMS $EXT_USER_CODE_DIR/xfr

STATE_DIR=$DATA_DIR/state
HOME_STATE_DIR=$STATE_DIR/$ETL_ENV

MFS_HOME=$DATA_DIR/mfs_home
HOME_MFS_DIR=$MFS_HOME/$ETL_ENV

#rm -fR $HOME_STATE_DIR
rm -fR $HOME_MFS_DIR

# create extract directories
mkdir -p -m $DEF_PERMS $HOME_DIR/dat/extract
mkdir -p -m $DEF_PERMS $HOME_DIR/tmp/extract
mkdir -p -m $DEF_PERMS $EXT_WATCH_DIR/extract
mkdir -p -m $DEF_PERMS $EXT_USER_LOG_DIR/extract
mkdir -p -m $DEF_PERMS $HOME_DIR/arc/extract

# make data directories
mkdir -p -m $DEF_PERMS $HOME_DIR/in/extract
mkdir -p -m $DEF_PERMS $HOME_DIR/out/extract


# mfs
mkdir -p -m $DEF_PERMS $HOME_MFS_DIR


# link state directories
ln -s $EXT_WATCH_DIR/dat $HOME_DIR/dat
#ln -s $HOME_STATE_DIR/tmp $HOME_DIR/tmp
ln -s $EXT_WATCH_DIR $HOME_DIR/watch

# link log directories
lnifnotexist $EXT_USER_LOG_DIR $HOME_DIR/log 
lnifnotexist $EXT_MSTR_LOG_DIR $DATA_DIR/mstr_log 

# link mfs dir
ln -s $HOME_MFS_DIR $HOME_DIR/mfs

# link land directory
ln -s $EXT_LAND_DIR $HOME_DIR/land

# link code directories
ln -s $EXT_USER_CODE_DIR/bin $HOME_DIR/bin
ln -s $EXT_USER_CODE_DIR/cfg $HOME_DIR/cfg
ln -s $EXT_USER_CODE_DIR/cmp $HOME_DIR/cmp
ln -s $EXT_USER_CODE_DIR/dbc $HOME_DIR/dbc
ln -s $EXT_USER_CODE_DIR/dml $HOME_DIR/dml
ln -s $EXT_USER_CODE_DIR/lib $HOME_DIR/lib
ln -s $EXT_USER_CODE_DIR/.logins $HOME_DIR/.logins
ln -s $EXT_USER_CODE_DIR/mp $HOME_DIR/mp
ln -s $EXT_USER_CODE_DIR/sql $HOME_DIR/sql
ln -s $EXT_USER_CODE_DIR/src $HOME_DIR/src
ln -s $EXT_USER_CODE_DIR/xfr $HOME_DIR/xfr

lnifnotexist $EXT_MSTR_CODE_DIR/bin $DATA_DIR/mstr_bin
lnifnotexist $EXT_MSTR_CODE_DIR/cfg $DATA_DIR/mstr_cfg
lnifnotexist $EXT_MSTR_CODE_DIR/lib $DATA_DIR/mstr_lib
lnifnotexist $EXT_MSTR_CODE_DIR/src $DATA_DIR/mstr_src

# link in/out dirs
#ln -s $HOME_IN_DIR $HOME_DIR/in
#ln -s $HOME_OUT_DIR $HOME_DIR/out

# link legacy 01 devices
#ln -s $HOME_IN_DIR $HOME_DIR/in01
#ln -s $HOME_OUT_DIR $HOME_DIR/out01

# link archive directories
#ln -s $HOME_ARC_DIR $HOME_DIR/arc

# make /link target directories
while read TARGET
do

  if [[ $TARGET == @(primary|secondary) ]]
  then

    case $TARGET in
      primary) lnifnotexist $EXT_WATCH_DIR/td1 $EXT_WATCH_DIR/$TARGET
               lnifnotexist $EXT_USER_LOG_DIR/td1 $EXT_USER_LOG_DIR/$TARGET;;
      secondary) lnifnotexist $EXT_WATCH_DIR/td2 $EXT_WATCH_DIR/$TARGET
                 lnifnotexist $EXT_USER_LOG_DIR/td2 $EXT_USER_LOG_DIR/$TARGET;;
    esac

  else

    # state directories
    mkdir -p -m $DEF_PERMS $HOME_DIR/dat/$TARGET
    mkdir -p -m $DEF_PERMS $HOME_DIR/tmp/$TARGET
    mkdir -p -m $DEF_PERMS $EXT_WATCH_DIR/$TARGET

    # log directories
    mkdir -p -m $DEF_PERMS $EXT_USER_LOG_DIR/$TARGET

    # make archive directories
    mkdir -p -m $DEF_PERMS $HOME_DIR/arc/$TARGET
  fi

  # link data directories
  ln -s $HOME_DIR/out/extract $HOME_DIR/out/$TARGET
  ln -s $HOME_DIR/in/extract $HOME_DIR/in/$TARGET

done < $RUN_DIR/dw_infra.create_infra_env_x86.targets.lis


# set permissions on any directories which differ from default permissions
chmod 1755 $DATA_DIR
chmod 1755 $BASE_DIR
chmod 1755 $HOME_DIR
chmod 1755 $HOME_DIR/arc
chmod 1755 $HOME_DIR/dat
chmod 1755 $HOME_DIR/in
chmod 1755 $EXT_USER_LOG_DIR
chmod 1755 $MFS_HOME
chmod 1755 $HOME_MFS_DIR
chmod 1755 $HOME_DIR/out
chmod 1755 $HOME_DIR/tmp
chmod 1755 $EXT_WATCH_DIR
chmod 1755 -R $EXT_MSTR_CODE_DIR
chmod 1755 -R $DATA_DIR/mstr_dat
chmod 1755 -R $DATA_DIR/mstr_tmp
chmod 1755 $EXT_MSTR_LOG_DIR
chmod 1775 $EXT_MSTR_LOG_DIR/jobtrack
chmod 1775 $EXT_MSTR_LOG_DIR/jobtrack/land
chmod 1775 $EXT_MSTR_LOG_DIR/sft
chmod 1755 /${MOUNT_PREFIX}code
chmod 1755 $EXT_USER_CODE_DIR
chmod 1755 /${MOUNT_PREFIX}logs

if [[ $ETL_ENV == @(qa|itg|prod) ]]
then
  chmod 1770 $EXT_USER_CODE_DIR/.logins
  chmod 1777 $EXT_LAND_DIR
fi


if [ $OWNER_ID ]
then
        chown -fR $OWNER_ID $HOME_DIR
        chown -fR $OWNER_ID $HOME_MFS_DIR
        chown -fR $OWNER_ID $EXT_MSTR_CODE_DIR
        chown -fR $OWNER_ID $EXT_LAND_DIR
        chown -fR $OWNER_ID $EXT_WATCH_DIR
        chown -fR $OWNER_ID $EXT_MSTR_LOG_DIR
        chown -fR $OWNER_ID $EXT_USER_LOG_DIR
#        chown -fR $OWNER_ID $HOME_STATE_DIR
#        chown -fR $OWNER_ID $HOME_ARC_DIR
#        chown -fR $OWNER_ID $HOME_IN_DIR
#        chown -fR $OWNER_ID $HOME_OUT_DIR
fi

exit
