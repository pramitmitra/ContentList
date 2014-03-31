#!/bin/ksh -eu

. /dw/etl/mstr_cfg/etlenv.setup
. /dw/etl/mstr_lib/dw_etl_common_functions.lib

#read SA list as ETL_ID, so that dw_etl_common_defs sets up dirs correctly
while read ETL_ID
do
  while read JOB_ENV
  do

    . /dw/etl/mstr_cfg/dw_etl_common_defs.cfg
    unset DW_SA_LOG
    unset DW_SA_OUT
    . /dw/etl/mstr_cfg/dw_etl_common_defs.cfg

    while read DIR
    do
        mkdirifnotexist $(eval print $DIR)
    done < $DW_MASTER_CFG/dw_etl_sub_dirs.lis
  done < $DW_MASTER_CFG/dw_etl_job_env.lis
done < $DW_MASTER_CFG/dw_etl_sa.lis
exit
