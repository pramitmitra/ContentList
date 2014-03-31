#!/usr/bin/ksh -eu

export SUBJECT_AREA=$1
export REMOTE_PROD_HOST=zaisetlcore01

export ETL_ID=${SUBJECT_AREA}.push_sa_code

. /dw/etl/mstr_cfg/etlenv.setup

if [[ ! -d $DW_CFG || ! -d $DW_SQL ]]
then
   echo "please run dw_infra.create_subject_area_dirs.ksh create subject area dir first"
 fi     
  
  
 #########################################################################################################
 # SCP ETL_ID: ${ETL_ID} related script from $REMOTE_PROD_HOST  to ${WORK_DIR}/prod/
 ##########################################################################################################
   

 scp jxiong@$REMOTE_PROD_HOST:/dw/etl/home/prod/sql/${SUBJECT_AREA}.*.*        $DW_SQL       > /dev/null 2>&1
 scp jxiong@$REMOTE_PROD_HOST:/dw/etl/home/prod/cfg/${SUBJECT_AREA}.*.*         $DW_CGF       > /dev/null 2>&1


 set +e
 scp jxiong@$REMOTE_PROD_HOST:/dw/etl/home/prod/xfr/${SUBJECT_AREA}.*.*   $DW_XFR   > /dev/null 2>&1
 set -e

 set +e
 scp jxiong@$REMOTE_PROD_HOST:/dw/etl/home/prod/dml/${SUBJECT_AREA}.*.*   $DW_DML       > /dev/null 2>&1
 set -e

 set +e
 scp jxiong@$REMOTE_PROD_HOST:/dw/etl/home/prod/bin/${SUBJECT_AREA}.*.*   $DW_EXE/$SUBJECT_AREA       > /dev/null 2>&1
 set -e
  
