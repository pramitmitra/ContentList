#!/bin/ksh -eu

. /dw/etl/mstr_cfg/etlenv.setup
. /dw/etl/mstr_lib/dw_etl_common_functions.lib

ETL_ID=$1

while read JOB_ENV
do

    . /dw/etl/mstr_cfg/dw_etl_common_defs.cfg
    unset DW_SA_LOG
    unset DW_SA_OUT
    . /dw/etl/mstr_cfg/dw_etl_common_defs.cfg

    while read DIR
    do
        mkdirifnotexist $(eval print $DIR)
        chmod 700 $(eval print $DIR)
    done < $DW_MASTER_CFG/dw_etl_sub_dirs_secure_user.lis

    while read DIR
    do
        chmod g+s $(eval echo $DIR)
    done < $DW_MASTER_CFG/dw_etl_sub_dirs_secure_user_sft.lis

done < $DW_MASTER_CFG/dw_etl_job_env.lis

exit
