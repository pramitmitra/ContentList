#!/usr/bin/ksh

. /dw/etl/mstr_cfg/etlenv.setup

# Create list of mfs parent dirs
echo "Creating list of parent dirs"
ls -1d $DW_MFS/* > $DW_MASTER_TMP/mfs_parent_dir.lis

# Loop through list and re-build
# set -e is off, so any failures will appear in output
# but script won't fail. Any failures can be addressed
# manually after script runs. Primary reason for failure 
# would be if the tmpdir is not empty, in which case
# we would want that command to fail.

echo "Looping through list and rebuilding."

while read parent_dir
do
    ln -s $parent_dir/in/extract $parent_dir/in/td4
    m_rmdir $parent_dir/tmp/primary
    if [ $? == 0 ]
    then
        ln -s $parent_dir/tmp/td1 $parent_dir/tmp/primary
    fi

    m_rmdir $parent_dir/tmp/secondary
    if [ $? == 0 ]
    then
        ln -s $parent_dir/tmp/td2 $parent_dir/tmp/secondary
    fi

    m_mkdir $parent_dir/tmp/td4
done < $DW_MASTER_TMP/mfs_parent_dir.lis

rm $DW_MASTER_TMP/mfs_parent_dir.lis

echo "MFS Rebuild Complete"

exit
