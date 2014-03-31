#!/bin/ksh -eu
############################################################################################################
#
# Title:        DW_INFRA Create Subject Area Multie File Directories
# File Name:    dw_infra.create_subject_area_mfs.ksh 
# Description:  Script for creating subject area multi file directories beneath infrastructure hierarchy
# Developer:    Kevin Oaks
# Created on:   2012-09-01
# Location:     $DW_MASTER_EXE
#
# Date         Ver#   Modified By(Name)            Change and Reason for Change
#---------    -----  ---------------------------  ------------------------------
# 2012-09-01   1.0    Kevin Oaks                    Ported to RedHat from original:
#                                                    - now using /bin/ksh rather than /usr/bin/ksh
#                                                    - converted echo statements to print
#
##############################################################################################################

ETL_ID=$1

. /dw/etl/mstr_cfg/etlenv.setup
. /dw/etl/mstr_lib/dw_etl_common_abinitio_functions.lib

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

exit
