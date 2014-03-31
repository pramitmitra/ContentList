#!/usr/bin/ksh -eu

. /dw/etl/mstr_cfg/etlenv.setup

# Remove existing td1/td2 dirs
echo "Removing existing unused dirs"
rmdirifexist $DW_WATCH/td1
rmdirifexist $DW_WATCH/td2

# Move existing primary/secondary to td1/td2
echo "Renaming existing dirs"
mv $DW_WATCH/primary $DW_WATCH/td1
mv $DW_WATCH/secondary $DW_WATCH/td2

# Create directories for td3/td4
mkdirifnotexist  $DW_WATCH/td3
mkdirifnotexist  $DW_WATCH/td4

# Create links from primary/secondary to td1/td2
echo "Establishing links as old name to new dir"
ln -s $DW_WATCH/td1 $DW_WATCH/primary
ln -s $DW_WATCH/td2 $DW_WATCH/secondary

echo "Watchfile dir creation/linking complete"

exit
