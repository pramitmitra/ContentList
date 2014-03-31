#!/usr/bin/ksh -eu

. /dw/etl/mstr_cfg/etlenv.setup
export CURR_DATETIME=${CURR_DATETIME:-$(date '+%Y%m%d-%H%M%S')}

################################################################################
echo "Running $0" `date`
echo "Looping through all primary servers" `date`
################################################################################
while read servername
do
  echo "Running for $servername" `date`
  ssh -n $servername "$DW_MASTER_BIN/infra/dw_infra.sync_td5_td3.ksh > $DW_LOG/dw_infra/td5/dw_infra.sync_td5_td3.$CURR_DATETIME.log"
done < $DW_MASTER_BIN/infra/dw_infra.sync_td5_td3_server.lis

echo "End of program" `date`

exit 0
