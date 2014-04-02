#!/usr/bin/ksh -eu

SERVERLIST=${1:-ha_serverlist}

export CURR_DATETIME=$(date '+%Y%m%d-%H%M%S')

. /dw/etl/mstr_cfg/etlenv.setup
LFILE=$DW_MASTER_LOG/dw_infra.dw_in_extract_usage_check.$CURR_DATETIME.log

while read SN
do
   ssh -n $SN 'df -k /dw/etl/home/prod/in/extract/' >> $LFILE

done < $SERVERLIST

exit 0

