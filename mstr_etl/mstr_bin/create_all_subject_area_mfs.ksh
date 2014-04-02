#!/bin/ksh -eu

. /dw/etl/mstr_cfg/etlenv.setup
. /dw/etl/mstr_lib/dw_etl_common_abinitio_functions.lib

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
            MDIR=$(eval print $DIR)
            MDIR_ROOT=${MDIR%%$ETL_ID} 
            if [ -d $MDIR_ROOT ]
            then
                m_mkdirifnotexist $(eval print $DIR)
            else
                print "Cannot make $(eval print $DIR)\nRoot directory $MDIR_ROOT does not exist"
            fi
        done < $DW_MASTER_CFG/dw_etl_mfs_dirs.lis
    done < $DW_MASTER_CFG/dw_etl_job_env.lis
done < $DW_MASTER_CFG/dw_etl_sa.lis
exit
