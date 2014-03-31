#!/bin/ksh -eu
#------------------------------------------------------------------------------------------------
# Filename:     remove_subject_area_mfs.ksh
#
# Revision History:
#
# Name             Date            Description
# ---------------  --------------  ---------------------------------------------------
# ???              ??/??/????      Initial Creation
# Ryan Wong        10/04/2013      Redhat changes
#
#------------------------------------------------------------------------------------------------

ETL_ID=$1
FULL_WIPE=${2:-0}

. /dw/etl/mstr_cfg/etlenv.setup
. /dw/etl/mstr_lib/dw_etl_common_abinitio_functions.lib

while read JOB_ENV
do

    . /dw/etl/mstr_cfg/dw_etl_common_defs.cfg
    if [ $FULL_WIPE -eq 0 ]
    then
        while read DIR
        do
            m_rmdirifexist $(eval print $DIR)
        done < $DW_MASTER_CFG/dw_etl_mfs_dirs.lis
    elif [ $FULL_WIPE -eq 1 ]
    then
        while read DIR
        do
            m_rmdirtreeifexist $(eval print $DIR)
        done < $DW_MASTER_CFG/dw_etl_mfs_dirs.lis
    else
        print "Invalid option $2" >&2 
        exit 4
    fi
done < $DW_MASTER_CFG/dw_etl_job_env.lis

exit
