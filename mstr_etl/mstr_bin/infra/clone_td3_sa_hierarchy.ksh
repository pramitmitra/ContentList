#!/usr/bin/ksh

. /dw/etl/mstr_cfg/etlenv.setup

# Create td3 SA list in master tmp
echo "Creating td3 SA list"
if [ -f $DW_MASTER_TMP/td3_sa.lis ]
then
   rm $DW_MASTER_TMP/td3_sa.lis
fi

for sa in `ls -1d $DW_DAT/td3/*` 
do
   if [ -d $sa ]
   then
      sa=${sa##*/}
      echo $sa >> $DW_MASTER_TMP/td3_sa.lis
   fi
done

while read sa
do

   #Create SA Dirs in td4, td5, td6
   echo "Creating dirs in td4, td5, td6 for SA $sa" 
   mkdirifnotexist $DW_TMP/td4/$sa
   mkdirifnotexist $DW_TMP/td5/$sa
   mkdirifnotexist $DW_TMP/td6/$sa
   mkdirifnotexist $DW_ARC/td4/$sa
   mkdirifnotexist $DW_ARC/td5/$sa
   mkdirifnotexist $DW_ARC/td6/$sa
   mkdirifnotexist $DW_LOG/td4/$sa
   mkdirifnotexist $DW_LOG/td5/$sa
   mkdirifnotexist $DW_LOG/td6/$sa
   mkdirifnotexist $DW_DAT/td4/$sa
   mkdirifnotexist $DW_DAT/td5/$sa
   mkdirifnotexist $DW_DAT/td6/$sa
   #mkdirifnotexist $DW_WATCH/td4/$sa
   #mkdirifnotexist $DW_WATCH/td5/$sa
   #mkdirifnotexist $DW_WATCH/td6/$sa

done < $DW_MASTER_TMP/td3_sa.lis

rm $DW_MASTER_TMP/td3_sa.lis

exit
