#!/bin/ksh
############################################################################################################
#
# Title:        DW_INFRA Create Subject Area Directories 
# File Name:    dw_infra.create_subject_area_dirs.ksh 
# Description:  Script for creating subject area directories beneath standard infrastructure hierarchy 
# Developer:    Kevin Oaks
# Created on:   2012-09-01
# Location:     $DW_MASTER_EXE
#
# Date         Ver#   Modified By(Name)            Change and Reason for Change
#---------    -----  ---------------------------  ------------------------------
# 2012-09-01   1.0    Kevin Oaks                    Ported to RedHat from original:
#                                                    - now using /bin/ksh rather than /usr/bin/ksh
#                                                    - converted echo statements to print
# 2016-12-13   1.1    Ryan Wong                     Add group sticky for secure file transfer
#
##############################################################################################################

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
        mkdirifnotexist $(eval echo $DIR)
    done < $DW_MASTER_CFG/dw_etl_sub_dirs.lis

    while read DIR
    do
        chmod 775 $(eval echo $DIR)
        chmod g+s $(eval echo $DIR)
    done < $DW_MASTER_CFG/dw_etl_sub_dirs_sft.lis

done < $DW_MASTER_CFG/dw_etl_job_env.lis

ln -s $DW_HOME/cfg/tnsnames.ora  $DW_HOME/cfg/$SUBJECT_AREA/tnsnames.ora
ln -s $DW_HOME/cfg/subject_area_email_list.dat  $DW_HOME/cfg/$SUBJECT_AREA/subject_area_email_list.dat
ln -s $DW_HOME/cfg/dw_caty.sources.lis  $DW_HOME/cfg/$SUBJECT_AREA/dw_caty.sources.lis

exit
