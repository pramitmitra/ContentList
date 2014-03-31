#!/usr/bin/ksh -eux

. /dw/etl/mstr_cfg/etlenv.setup

# Backing Up Primary and Secondary dirs
echo "Backing up existing primary and secondary dirs"
cp -r $DW_DAT/primary $DW_DAT/primary_bak 
cp -r $DW_DAT/secondary $DW_DAT/secondary_bak 
cp -r $DW_TMP/primary $DW_TMP/primary_bak 
cp -r $DW_TMP/secondary $DW_TMP/secondary_bak 
#cp -r $DW_LOG/primary $DW_LOG/primary_bak 
#cp -r $DW_LOG/secondary $DW_LOG/secondary_bak 
#cp -r $DW_ARC/primary $DW_ARC/primary_bak 
#cp -r $DW_ARC/secondary $DW_ARC/secondary_bak 

# Remove existing td1/td2 dirs

echo "Removing existing unused dirs"
rmdirtreeifexist $DW_DAT/td1
rmdirtreeifexist $DW_DAT/td2
rmdirtreeifexist $DW_TMP/td1
rmdirtreeifexist $DW_TMP/td2
rmdirtreeifexist $DW_LOG/td1
rmdirtreeifexist $DW_LOG/td2
rmdirtreeifexist $DW_ARC/td1
rmdirtreeifexist $DW_ARC/td2
rm -f $DW_IN/td3
rm -f $DW_IN/td4
rm -f $DW_OUT/td3
rm -f $DW_OUT/td4

# Rename existing primary/secondary dirs
echo "Renaming existing dirs"
mv $DW_DAT/primary $DW_DAT/td1
mv $DW_DAT/secondary $DW_DAT/td2
mv $DW_TMP/primary $DW_TMP/td1
mv $DW_TMP/secondary $DW_TMP/td2
mv $DW_LOG/primary $DW_LOG/td1
mv $DW_LOG/secondary $DW_LOG/td2
mv $DW_ARC/primary $DW_ARC/td1
mv $DW_ARC/secondary $DW_ARC/td2

# Set up links for backwards compatibility
echo "Establishing links as old name to new dir"
ln -s $DW_DAT/td1 $DW_DAT/primary
ln -s $DW_DAT/td2 $DW_DAT/secondary
ln -s $DW_TMP/td1 $DW_TMP/primary
ln -s $DW_TMP/td2 $DW_TMP/secondary
ln -s $DW_LOG/td1 $DW_LOG/primary
ln -s $DW_LOG/td2 $DW_LOG/secondary
ln -s $DW_ARC/td1 $DW_ARC/primary
ln -s $DW_ARC/td2 $DW_ARC/secondary

# Create TD3,TD4 dirs
echo "Creating dirs for TD3, TD4 where they don't already exist"
mkdirifnotexist $DW_DAT/td3
mkdirifnotexist $DW_DAT/td4
mkdirifnotexist $DW_TMP/td3
mkdirifnotexist $DW_TMP/td4
mkdirifnotexist $DW_LOG/td3
mkdirifnotexist $DW_LOG/td4
mkdirifnotexist $DW_ARC/td3
mkdirifnotexist $DW_ARC/td4

# Create TD3, TD4 links in/out links
ln -s /dw/etl/in/prod/extract $DW_IN/td3
ln -s /dw/etl/out/prod/extract $DW_OUT/td3
ln -s /dw/etl/in/prod/extract $DW_IN/td4
ln -s /dw/etl/out/prod/extract $DW_OUT/td4

echo "Multi Env Remap complete"

exit
