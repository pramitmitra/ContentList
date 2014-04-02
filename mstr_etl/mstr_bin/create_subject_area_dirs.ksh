#!/bin/ksh -eu

ETL_ID=$1

. /dw/etl/mstr_cfg/etlenv.setup
. /dw/etl/mstr_lib/dw_etl_common_functions.lib


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

ln -s $DW_HOME/cfg/tnsnames.ora $DW_HOME/cfg/$SUBJECT_AREA/tnsnames.ora
ln -s $DW_HOME/cfg/subject_area_email_list.dat $DW_HOME/cfg/$SUBJECT_AREA/subject_area_email_list.dat
ln -s $DW_HOME/cfg/dw_caty.sources.lis $DW_HOME/cfg/$SUBJECT_AREA/dw_caty.sources.lis

exit
