#!/bin/ksh -eu
###################################################################################################################
#
# Title:        DW_INFRA Batch DataMover Utility Load Table Check
# File Name:    dw_infra.batch_datamover_utility_load_table_check.ksh 
# Description:  This module is called by dw_infra.batch_teradata_datamover.ksh
#               Allows for graceful restarting of failed datamover jobs which
#               may have table locks and/or Log/Error tables persisting from a 
#               previous run. Preference is to remove locks and drop persisting
#               Log/Errr tables so that the extract/load begins fresh on restart.
#               All variables not explicitly set in this shell are inherited from
#               the calling job.
# Developer:    Kevin Oaks
# Created on:   2010-10-20
# Location:     $DW_MASTER_EXE
#
# Date         Ver#   Modified By(Name)            Change and Reason for Change
#---------    -----  ---------------------------  ------------------------------
# 2010-12-10   1.0    Kevin Oaks                  Initial Prod Version created from
#                                                 $DW_EXE/utility_load_table_check.ksh 
# 2012-09-25   1.1    Kevin Oaks                  Port to RedHat:
#                                                  - now using /bin/ksh rather than /usr/bin/ksh
#                                                  - converted echo statements to print
###################################################################################################################

set +e
bteq <<EOF > $UTILITY_LOAD_TABLE_CHECK_LOGFILE
.SET ERROROUT STDOUT
.set session transaction btet
.logon $TD_DB_NAME/$TD_USERNAME,$TD_PASSWORD

.maxerror 1

-- bypass errorlevel for non-existent object and drop error tables if they exist
-- first determine existence of table to minimized drop table calls that may lock DBC tables for write
.errorlevel 3807 severity 0
.errorlevel 2652 severity 0
select 1 from ${LOGTABLE_DATABASE}.${UTILITY_INTERFACE_LOG_TABLE} where 1=0;
.if errorcode <> 3807 then drop table ${LOGTABLE_DATABASE}.${UTILITY_INTERFACE_LOG_TABLE};

select 1 from ${ERRORTABLES_DATABASE}.${UTILITY_INTERFACE_ERROR_TABLE}1 where 1=0;
.if errorcode <> 3807 then drop table ${ERRORTABLES_DATABASE}.${UTILITY_INTERFACE_ERROR_TABLE}1;

select 1 from ${ERRORTABLES_DATABASE}.${UTILITY_INTERFACE_ERROR_TABLE}2 where 1=0;
.if errorcode <> 3807 then drop table ${ERRORTABLES_DATABASE}.${UTILITY_INTERFACE_ERROR_TABLE}2;
.errorlevel 3807 severity 8
.errorlevel 2652 severity 8 

.exit 0
EOF
rcode=$?
set -e

if [[ $rcode -ne 0 ]]
then
  print "Fatal Error $0 : Check for log and error tables not completed. Check log $UTILITY_LOAD_TABLE_CHECK_LOGFILE" >2
  exit 2
fi

if [[ $EXEC_MODE == L && $LOAD_TYPE == T ]]
then

set +e
bteq <<EOF >> $UTILITY_LOAD_TABLE_CHECK_LOGFILE
.SET ERROROUT STDOUT
.set session transaction btet
.logon $TD_DB_NAME/$TD_USERNAME,$TD_PASSWORD

.maxerror 1

.errorlevel 2652 severity 0
-- bypass errorlevel for fastload locked table, and exit on successful delete
del ${TRGT_TABLE};
.if errorcode = 0 then .exit 0

.errorlevel 2652 severity 8
-- delete failed due to fastload locked table, so drop/recreate table
.width 65531
.titledashes off

.export file ${DW_SA_TMP}/${TABLE_ID}.dm.recreate.${TRGT_TABLE}.tmp.sql close
show table ${TRGT_TABLE};
.export reset

.exit 8
EOF
rcode=$?
set -e
fi

if [ $rcode -eq 0 ]
then
  exit 0
fi

if [[ ! -s ${DW_SA_TMP}/${TABLE_ID}.dm.recreate.${TRGT_TABLE}.tmp.sql ]]
then
  print "\nTable DDL can't be saved on temp folder! Check $DW_TMP folder to make sure it has enough space!!!"
  exit 1
fi

set +e
bteq <<EOF >> $UTILITY_LOAD_TABLE_CHECK_LOGFILE
.SET ERROROUT STDOUT
.set session transaction btet
.logon $TD_DB_NAME/$TD_USERNAME,$TD_PASSWORD

drop table ${TRGT_TABLE};
.run file ${DW_SA_TMP}/${TABLE_ID}.dm.recreate.${TRGT_TABLE}.tmp.sql

EOF
rcode=$?
set -e

exit $rcode
