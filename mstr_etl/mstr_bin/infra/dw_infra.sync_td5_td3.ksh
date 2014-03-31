#!/usr/bin/ksh -u

################################################################################
# Local exection on server
################################################################################
echo "Starting $0" `date`
################################################################################
. /dw/etl/mstr_cfg/etlenv.setup

. $DW_MASTER_BIN/infra/dw_infra.sync_td5_td3.date_func.lib
curr_dt=$(date '+%Y%m%d')

################################################################################
echo "Find all bsn dat files" `date`
################################################################################
td5_tmp=$DW_TMP/td5.lis.tmp
td3_tmp=$DW_TMP/td3.lis.tmp
find $DW_DAT/td5 -type f -name '*.load.batch_seq_num.dat' -mtime -91 -ls | awk '{print $11,"",$8,"",$9,"",$10}' | sort > $td5_tmp
find $DW_DAT/td3 -type f -name '*.load.batch_seq_num.dat' -ls | awk '{print $11,"",$8,"",$9,"",$10}' | sort > $td3_tmp

> $td5_tmp.nodate
while read x y
do
  echo ${x#$DW_DAT/*/} >> $td5_tmp.nodate
done < $td5_tmp

> $td3_tmp.nodate
while read x y
do
  echo ${x#$DW_DAT/*/} >> $td3_tmp.nodate
done < $td3_tmp

################################################################################
echo "Compare td5 to td3" `date`
################################################################################
td5_td3_comp=$DW_TMP/td5_td3_comp.tmp
comm -12 $td5_tmp.nodate $td3_tmp.nodate > $td5_td3_comp

cp -rp $DW_DAT/td3 $DW_DAT/td3.bak

################################################################################
echo "Handling copy files from td5 to td3" `date`
################################################################################
while read fn
do
  grep $fn $td5_tmp | read fn5 dt5
  grep $fn $td3_tmp | read fn3 dt3
  dt5=`conv_dt $dt5`
  dt3=`conv_dt $dt3`
  dt_diff_td5_td3=`grgdif $dt5 $dt3`
  if [[ $dt_diff_td5_td3 -gt 0 && $dt_diff_td5_td3 -le 90 ]]
  then
    echo "copying $DW_DAT/td5/$fn $DW_DAT/td3/$fn"
    cp $DW_DAT/td5/$fn $DW_DAT/td3/$fn
  fi
done < $td5_td3_comp

################################################################################
echo "Cleaning up" `date`
################################################################################
rm $td5_tmp $td3_tmp
rm $td5_tmp.nodate td3_tmp.nodate
rm $td5_td3_comp

exit 0
