#!/bin/ksh -eu
# Title:        Utility Load TPT Check
# File Name:    dw_infra.utility_load_tpt_check.ksh
# Description:  Handle Teradata utility load tables
# Developer:    Ryan Wong
# Created on:
# Location:     $DW_MASTER_EXE
# Logic:
#
#
# Called by:    UC4/Unix
#
# Date         Ver#   Modified By(Name)            Change and Reason for Change
#---------    -----  ---------------------------  ------------------------------
# 2012-10-30   1.0    Ryan Wong                     Initial
# 2013-10-04   1.1    Ryan Wong                     Redhat changes
#
#############################################################################################################

WORKING_DB=${WORKING_DB:-TDLOG_TABLES}
LOG_TABLE=L_${STAGE_TABLE}
ERROR_TABLE1=E_${STAGE_TABLE}
ERROR_TABLE2=U_${STAGE_TABLE}

# Trim tablenames
if [[ ${#LOG_TABLE} -gt 30 ]]
then
  LOG_TABLE=$(print $LOG_TABLE | cut -c1-30)
fi

if [[ ${#ERROR_TABLE1} -gt 30 ]]
then
  ERROR_TABLE1=$(print $ERROR_TABLE1 | cut -c1-30)
fi

if [[ ${#ERROR_TABLE2} -gt 30 ]]
then
  ERROR_TABLE2=$(print $ERROR_TABLE2 | cut -c1-30)
fi

set +e
bteq <<EOF > $UTILITY_LOAD_TPT_CHECK_LOG_FILE
.SET ERROROUT STDOUT
.set session transaction btet
.logon $TD_DB_NAME/$TD_USERNAME,$TD_PASSWORD

.maxerror 1

-- bypass errorlevel for non-existent object and drop error tables if they exist
-- first determine existence of table to minimized drop table calls that may lock DBC tables for write
.errorlevel (3807,2652) severity 0
select 1 from ${WORKING_DB}.${LOG_TABLE} where 1=0;
.if errorcode <> 3807 then drop table ${WORKING_DB}.${LOG_TABLE};

select 1 from ${WORKING_DB}.${ERROR_TABLE1} where 1=0;
.if errorcode <> 3807 then drop table ${WORKING_DB}.${ERROR_TABLE1};

select 1 from ${WORKING_DB}.${ERROR_TABLE2} where 1=0;
.if errorcode <> 3807 then drop table ${WORKING_DB}.${ERROR_TABLE2};
.errorlevel 3807 severity 8 

-- bypass errorlevel for fastload locked table, and exit on successful delete
del ${STAGE_DB}.${STAGE_TABLE};
.if errorcode = 0 then .exit 0

.errorlevel 2652 severity 8
-- delete failed due to fastload locked table, so drop/recreate table
.width 65531
.titledashes off

.export file ${DW_SA_TMP}/${TABLE_ID}.ld.recreate.${STAGE_DB}.${STAGE_TABLE}.tmp.sql close
show table ${STAGE_DB}.${STAGE_TABLE};
.export reset

.exit 8

EOF
rcode=$?
set -e

if [ $rcode -eq 0 ]
then
  exit 0
fi
if [ ! -s ${DW_SA_TMP}/${TABLE_ID}.ld.recreate.${STAGE_DB}.${STAGE_TABLE}.tmp.sql ]
then
  print "\nTable DDL can't be saved on temp folder! Check $DW_SA_TMP folder to make sure it has enough space!!!"
  exit 1
fi

set +e
bteq <<EOF >> $UTILITY_LOAD_TPT_CHECK_LOG_FILE
.SET ERROROUT STDOUT
.set session transaction btet
.logon $TD_DB_NAME/$TD_USERNAME,$TD_PASSWORD

drop table ${STAGE_DB}.${STAGE_TABLE};
.run file ${DW_SA_TMP}/${TABLE_ID}.ld.recreate.${STAGE_DB}.${STAGE_TABLE}.tmp.sql

EOF
rcode=$?
set -e
exit $rcode
