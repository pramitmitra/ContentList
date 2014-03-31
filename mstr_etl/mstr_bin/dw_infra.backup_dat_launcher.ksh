#!/bin/ksh -eu


export CURR_DATETIME=$(date '+%Y%m%d-%H%M%S')

. /dw/etl/mstr_cfg/etlenv.setup

SERVERLIST=${1:-$DW_MASTER_BIN/infra/full_serverlist}

while read SN
do
   LFILE=$DW_MASTER_LOG/dw_infra.backup_dat.$SN.$CURR_DATETIME.log
   BCKDIR=$DW_MASTER_DAT/bckup/$SN/
   ssh -n $SN "mkdirifnotexist $BCKDIR; cp -r $DW_DAT/ $BCKDIR  > $LFILE 2>&1" &

done < $SERVERLIST

exit 0

