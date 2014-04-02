#!/bin/ksh -eu

#########################################################################################
#
#  Added to the single_table_load graph to replace original runsql comp.
#  The shell script provided more flexibility and error checking mechanism.
#  The purpose of this shelll is to check the Teradata utility load table.
#
# Revision History:
#
# Name             Date            Description
# ---------------  --------------  ---------------------------------------------------
# ???              ??/??/????      Initial Creation
# Ryan Wong        10/04/2013      Redhat changes
#
##########################################################################################
export LOGIN_STR=$(JOB_ENV_UPPER=$(print $JOB_ENV | tr "[:lower:]" "[:upper:]"); eval print \$DW_${JOB_ENV_UPPER}_DB)/$TD_USERNAME,$TD_PASSWORD
export STAGE_DB=$DATABASE_NAME
export WORKING_DB=$WORKING_DATABASE
export STAGE_TABLE=$TABLE_NAME

print $LOGIN_STR

set +e
bteq <<EOF > $UTILITY_LOAD_TABLE_CHECK_LOGFILE
.SET ERROROUT STDOUT
.set session transaction btet
.logon $LOGIN_STR

.maxerror 1
-- bypass errorlevel for non-existent object and drop error tables if they exist
-- first determine existence of table to minimized drop table calls that may lock DBC tables for write
.errorlevel (3807,2652) severity 0
select 1 from ${WORKING_DB}.U_${STAGE_TABLE} where 1=0;
.if errorcode <> 3807 then drop table ${WORKING_DB}.U_${STAGE_TABLE};

select 1 from ${WORKING_DB}.L_${STAGE_TABLE} where 1=0;
.if errorcode <> 3807 then drop table ${WORKING_DB}.L_${STAGE_TABLE};

select 1 from ${WORKING_DB}.E_${STAGE_TABLE} where 1=0;
.if errorcode <> 3807 then drop table ${WORKING_DB}.E_${STAGE_TABLE};

select 1 from ${STAGE_DB}.${STAGE_TABLE} where 1=0;
. if errorcode = 3807 then .exit 3


.errorlevel 2652 severity 0

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

if [ $rcode -eq 3 ]
then
  exit 3
fi

if [ $rcode -eq 0 ]
then
  exit 0
fi
if [ ! -s ${DW_SA_TMP}/${TABLE_ID}.ld.recreate.${STAGE_DB}.${STAGE_TABLE}.tmp.sql ]
then
  print "\nTable DDL can't be saved on temp folder! Check $DW_SA_TMP folder to make sure it has enough space!!!" >> $UTILITY_LOAD_TABLE_CHECK_LOGFILE
  exit 1
fi

set +e
bteq <<EOF >> $UTILITY_LOAD_TABLE_CHECK_LOGFILE
.SET ERROROUT STDOUT
.set session transaction btet
.logon $LOGIN_STR

drop table ${STAGE_DB}.${STAGE_TABLE};
.run file ${DW_SA_TMP}/${TABLE_ID}.ld.recreate.${STAGE_DB}.${STAGE_TABLE}.tmp.sql

EOF
rcode=$?
set -e
exit $rcode
