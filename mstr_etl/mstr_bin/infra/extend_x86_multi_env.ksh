#!/usr/bin/ksh -eu
########################################################################
####
#### This script will add new target directory structures to the
#### standard DW ETL hierarchy. These will be sub-dirs created
#### under dat, tmp, log, arc, and watch as well as pointers created
#### under in, out.
####
#### This script should only be used by the Ops account that owns
#### the environment being updated.
#### 
#### Script assumes the existence of $DW_MASTER_CFG/dw_etl_job_env.lis
#### and will create any target directory structures in the hierarchy
#### for each entry in the list if they do not already exist. If
#### creating new structures, be sure to update this list file and
#### push to the appropriate system first.
####
#########################################################################
# Revision History:
#
# Name             Date            Description
# ---------------  --------------  --------------------------------------
# Kevin Oaks       06/02/2011      Inititial Version
#
#
#########################################################################

function usage {
   $DW_MASTER_EXE/infra/extend_x86_multi_env.ksh
}

. /dw/etl/mstr_cfg/etlenv.setup


# Create new Target dirs
echo "Creating Target Directories"

while read job_env
do

   # Create new Target dirs
   echo "Creating dirs for $job_env where they don't already exist"
   mkdirifnotexist $DW_DAT/$job_env
   mkdirifnotexist $DW_TMP/$job_env
   mkdirifnotexist $DW_LOG/$job_env
   mkdirifnotexist $DW_ARC/$job_env
   mkdirifnotexist $DW_WATCH/$job_env

   # Create new in/out links
   echo "Creating in/out links to extract for $job_env where they don't already exist"

   if [[ $job_env != extract && ! -L $DW_IN/$job_env ]]
   then
      ln -s $DW_IN/extract $DW_IN/$job_env
   fi

   if [[ $job_env != extract && ! -L $DW_OUT/$job_env ]]
   then
      ln -s $DW_OUT/extract $DW_OUT/$job_env
   fi

done < $DW_MASTER_CFG/dw_etl_job_env.lis

echo "Multi Env Extension complete"

exit
