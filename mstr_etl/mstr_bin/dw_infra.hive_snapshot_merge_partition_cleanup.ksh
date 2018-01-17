#!/bin/ksh -eu
############################################################################################
# Developer   : Jacky Shen
#
# Description: this script accepts 6 parameters:
#              <ETL_ID> <JOB_ENV> <UOW_FROM> <UOW_TO> <DB_NAME.TABLE_NAME> <RETENTION_DAYS>
#              the maximum retention_days is 5. If user input is greater than 5, then use 5
#
#
# Output/Return Code: n/a
#
# Date        Ver#  Modified By(Name)            Change and Reason for Change
# ----------  ----- -------------------------   ---------------------------------------
# 2017-06-27  1.0   Jacky Shen                  Initial
# 2017-11-17  1.1   Pramit Mitra                Reduced arguments, UOW_TO length conversion
# -------------------------------------------------------------------------------------

typeset -fu usage

function usage {
   print "Usage:  $0 <ETL_ID> <JOB_ENV> <UOW_TO> <DB_NAME.TABLE_NAME> <RETENTION_DAYS>"
}

if [[ $# -lt 5 ]]
then
   usage
   exit 4
fi

export SCRIPTNAME=${0##*/}
export BASENAME=${SCRIPTNAME%.*}

export ETL_ID=$1
export JOB_ENV=$2
export UOW_TO=$3
export MERGE_TABLE=$4
export RET_DAYS=$5

UOW_TO_DATE=`echo $UOW_TO | cut -c1-8`
SA_DIR=`echo ${ETL_ID} | awk -F'.' '{ print $1; }'`
print "Value of SA_DIR = $SA_DIR"
DW_SA_TMP=${DW_TMP}/extract/${SA_DIR}
print "Value of DW_SA_TMP = $DW_SA_TMP"

. /dw/etl/mstr_cfg/etlenv.setup

. $DW_MASTER_CFG/dw_etl_common_defs.cfg
. $DW_MASTER_LIB/dw_etl_common_abinitio_functions.lib
. $DW_MASTER_CFG/dw_etl_common_defs_uow.cfg
. $DW_MASTER_CFG/hadoop.login

PURGE_PARTITONS_OLDER_THAN=$(add_days ${UOW_TO_DATE} -${RET_DAYS})

print "Partition dt <= ${PURGE_PARTITONS_OLDER_THAN} will be purged"

print "ALTER TABLE ${MERGE_TABLE} SET TBLPROPERTIES(\"EXTERNAL\"=\"FALSE\");" > ${DW_TMP}/extract/${SA_DIR}/${MERGE_TABLE}.PRUGE_PARTITIONS_OLDER_THAN_${PURGE_PARTITONS_OLDER_THAN}.hql.tmp
print "ALTER TABLE ${MERGE_TABLE} DROP IF EXISTS PARTITION(DT<${PURGE_PARTITONS_OLDER_THAN}) PURGE;" >> ${DW_TMP}/extract/${SA_DIR}/${MERGE_TABLE}.PRUGE_PARTITIONS_OLDER_THAN_${PURGE_PARTITONS_OLDER_THAN}.hql.tmp
print "ALTER TABLE ${MERGE_TABLE} SET TBLPROPERTIES(\"EXTERNAL\"=\"TRUE\");" >> ${DW_TMP}/extract/${SA_DIR}/${MERGE_TABLE}.PRUGE_PARTITIONS_OLDER_THAN_${PURGE_PARTITONS_OLDER_THAN}.hql.tmp

print "Purge old partition starting..."
print "Value of HIVE_HOME/bin/hive = $HIVE_HOME/bin/hive"
$HIVE_HOME/bin/hive -f ${DW_TMP}/extract/${SA_DIR}/${MERGE_TABLE}.PRUGE_PARTITIONS_OLDER_THAN_${PURGE_PARTITONS_OLDER_THAN}.hql.tmp
rcode=$?

if [[ $rcode -ne 0 ]]
then
   print "Purge old partition failed"
   exit $RUN_RCODE
fi

print "Purge old partition successfully finished"
rm -f $DW_SA_TMP/${MERGE_TABLE}.PRUGE_PARTITIONS_OLDER_THAN_${PURGE_PARTITONS_OLDER_THAN}.hql.tmp
exit 0
