#!/bin/ksh -eu

#########################################################################################
#
#  Added to the single_table_load graph to replace original runsql comp.
#  The shell script provided more flexibility and error checking mechanism.
#  The purpose of this shelll is to check the Teradata utility load table.
#
#  Ported to RedHat by koaks, 20120821
#  - using /bin/ksh rather than /usr/bin/ksh
#  - converted echo statements to print
#  - removed : from grep statements, as redhat sees that as a word boundary
#
##########################################################################################

export TD_DB_NAME=$(grep "^db_name\>" $DW_DBC/$AB_IDB_CONFIG | read PARAM VALUE COMMENT; eval print ${VALUE:-0})

set +e
bteq <<EOF > $UTILITY_LOAD_TABLE_CHECK_LOGFILE
.SET ERROROUT STDOUT
.set session transaction btet
.logon $TD_DB_NAME/$TD_USERNAME,$TD_PASSWORD

.maxerror 1

-- bypass errorlevel for non-existent object and drop error tables if they exist
-- first determine existence of table to minimized drop table calls that may lock DBC tables for write
.errorlevel (3807,2652) severity 0
select 1 from ${LOGTABLE_DATABASE}.${LOG_TABLE} where 1=0;
.if errorcode <> 3807 then drop table ${LOGTABLE_DATABASE}.${LOG_TABLE};

select 1 from ${ERRORTABLES_DATABASE}.${ERROR_TABLE}1 where 1=0;
.if errorcode <> 3807 then drop table ${ERRORTABLES_DATABASE}.${ERROR_TABLE}1;

select 1 from ${ERRORTABLES_DATABASE}.${ERROR_TABLE}2 where 1=0;
.if errorcode <> 3807 then drop table ${ERRORTABLES_DATABASE}.${ERROR_TABLE}2;
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
  print "\nTable DDL can't be saved on temp folder! Check $DW_TMP folder to make sure it has enough space!!!"
  exit 1
fi

set +e
bteq <<EOF >> $UTILITY_LOAD_TABLE_CHECK_LOGFILE
.SET ERROROUT STDOUT
.set session transaction btet
.logon $TD_DB_NAME/$TD_USERNAME,$TD_PASSWORD

drop table ${STAGE_DB}.${STAGE_TABLE};
.run file ${DW_SA_TMP}/${TABLE_ID}.ld.recreate.${STAGE_DB}.${STAGE_TABLE}.tmp.sql

EOF
rcode=$?
set -e
exit $rcode
