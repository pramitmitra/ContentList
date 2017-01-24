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

    umask 002
    while read DIR
    do
        mkdirifnotexist $(eval print $DIR)
    done < $DW_MASTER_CFG/dw_etl_sub_dirs_secure_dw_adm.lis

    while read DIR
    do
        chmod g+s $(eval echo $DIR)
    done < $DW_MASTER_CFG/dw_etl_sub_dirs_secure_dw_adm_sft.lis

done < $DW_MASTER_CFG/dw_etl_job_env.lis

ln -s $DW_HOME/cfg/tnsnames.ora $DW_HOME/cfg/$SUBJECT_AREA/tnsnames.ora
ln -s $DW_HOME/cfg/subject_area_email_list.dat $DW_HOME/cfg/$SUBJECT_AREA/subject_area_email_list.dat
ln -s $DW_HOME/cfg/dw_caty.sources.lis $DW_HOME/cfg/$SUBJECT_AREA/dw_caty.sources.lis

exit
