#!/usr/bin/ksh -eu

SERVERLIST=${1:-ha_serverlist}

export CURR_DATETIME=$(date '+%Y%m%d-%H%M%S')

. /dw/etl/mstr_cfg/etlenv.setup

while read SN
do

   ssh -n $SN "nohup $DW_BIN/shell_handler.ksh dw_infra.cleanup td1 $DW_MASTER_BIN/dw_infra.hadr_mstr_dat_sft_cleanup.ksh" &

done < $SERVERLIST

exit 0

