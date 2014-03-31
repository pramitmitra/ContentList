#!/usr/bin/ksh -eu

SERVERLIST=${1:-full_serverlist}

export CURR_DATETIME=$(date '+%Y%m%d-%H%M%S')

. /dw/etl/mstr_cfg/etlenv.setup
LFILE=$DW_MASTER_LOG/dw_infra.tmp_usage_check.$CURR_DATETIME.log

while read SN
do
   ssh -n $SN 'echo $(uname -n):;df -h /tmp' >> $LFILE

done < $SERVERLIST

exit 0

