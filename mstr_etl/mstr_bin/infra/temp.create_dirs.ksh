#!/usr/bin/ksh -eu

SERVERLIST=${1:-full_serverlist}

export CURR_DATETIME=$(date '+%Y%m%d-%H%M%S')

. /dw/etl/mstr_cfg/etlenv.setup

while read SN
do
   echo Server: $SN
   ssh -n $SN '. /dw/etl/mstr_cfg/etlenv.setup;mkdirifnotexist /dw/etl/mstr_dat/sft;mkdirifnotexist /dw/etl/mstr_log/sft'

done < $SERVERLIST

exit 0

