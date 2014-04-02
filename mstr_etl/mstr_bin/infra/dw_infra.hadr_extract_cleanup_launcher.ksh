#!/usr/bin/ksh -eu


export CURR_DATETIME=$(date '+%Y%m%d-%H%M%S')

. /dw/etl/mstr_cfg/etlenv.setup

SERVERLIST=${1:-$DW_MASTER_BIN/infra/ha_serverlist}

while read SN
do
   LFILE=$DW_MASTER_LOG/dw_infra.hadr_extract_cleanup.$SN.$CURR_DATETIME.log

   ssh -n $SN "$DW_MASTER_BIN/dw_infra.hadr_extract_cleanup.ksh > $LFILE 2>&1" &

done < $SERVERLIST

exit 0

