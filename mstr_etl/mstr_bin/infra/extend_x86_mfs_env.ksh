#!/usr/bin/ksh
##########################################################################################
#
# Use this to extend the multi file system directory structure when adding new targets.
# Prerequisite is that $DW_MASTER_CFG/dw_etl_job_env.lis is up to date. This script
# will create new links to extract in each existing MFS/in parent and created
# new multifile directories for each job_env in MFS/tmp where they do not already
# exist.
#
# Created By: Kevin Oaks
# Created On: 06/02/2011
#
##########################################################################################

. /dw/etl/mstr_cfg/etlenv.setup
. /dw/etl/mstr_lib/dw_etl_common_abinitio_functions.lib

# Create list of mfs parent dirs
echo "Creating list of parent dirs"
ls -1d $DW_MFS/* > $DW_MASTER_TMP/mfs_parent_dir.lis

while read job_env
do
   while read parent_dir
   do

      if [[ $job_env != extract && ! -L $parent_dir/in/$job_env ]]
      then
         echo "Creating $parent_dir/in/$job_env link"
         ln -s $parent_dir/in/extract $parent_dir/in/$job_env
      else
         echo "$parent_dir/in/$job_env link already exists."
      fi

      echo "Creating $parent_dir/tmp/$job_env" 
      m_mkdirifnotexist $parent_dir/tmp/$job_env

   done < $DW_MASTER_TMP/mfs_parent_dir.lis
done < $DW_MASTER_CFG/dw_etl_job_env.lis

rm $DW_MASTER_TMP/mfs_parent_dir.lis

echo "MFS extension complete"

exit
