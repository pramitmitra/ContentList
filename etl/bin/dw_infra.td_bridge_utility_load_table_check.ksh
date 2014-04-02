#!/bin/ksh -eu
#------------------------------------------------------------------------------------------------
# Filename:     dw_infra.td_bridge_utility_load_table_check.ksh
#
# Revision History:
#
# Name             Date            Description
# ---------------  --------------  ---------------------------------------------------
# ???              ??/??/????      Initial Creation
# Ryan Wong        10/04/2013      Redhat changes
#
#------------------------------------------------------------------------------------------------

set +e
bteq <<EOF > $UTILITY_LOAD_TABLE_CHECK_LOGFILE
.SET ERROROUT STDOUT
.set session transaction btet
.logon $TERADATA_SYSTEM/$TD_USERNAME,$TD_PASSWORD

.maxerror 1



-- bypass errorlevel for fastload locked table, and exit on successful delete
del ${DATABASE_NAME}.${TABLE_NAME};
.if errorcode = 0 then .exit 0

.errorlevel 2652 severity 8
-- delete failed due to fastload locked table, so drop/recreate table
.width 65531
.titledashes off

.export file ${DW_SA_TMP}/${TABLE_ID}.ld.recreate.${DATABASE_NAME}.${TABLE_NAME}.tmp.sql close
show table ${DATABASE_NAME}.${TABLE_NAME};
.export reset

.exit 8

EOF
rcode=$?
set -e

if [ $rcode -eq 0 ]
then
  exit 0
fi
if [ ! -s ${DW_SA_TMP}/${TABLE_ID}.ld.recreate.${DATABASE_NAME}.${TABLE_NAME}.tmp.sql ]
then
  print "\nCreat Table DDL temp folder failed! Please if  ${DATABASE_NAME}.${TABLE_NAME} exists!"
  exit 1
fi

set +e
bteq <<EOF >> $UTILITY_LOAD_TABLE_CHECK_LOGFILE
.SET ERROROUT STDOUT
.set session transaction btet
.logon $TERADATA_SYSTEM/$TD_USERNAME,$TD_PASSWORD

drop table ${DATABASE_NAME}.${TABLE_NAME};
.run file ${DW_SA_TMP}/${TABLE_ID}.ld.recreate.${DATABASE_NAME}.${TABLE_NAME}.tmp.sql

EOF
rcode=$?
set -e
exit $rcode
