#!/bin/ksh
# ==============================================================================
# Title:  Insert new value into lookup table and notify
# FileName:  dw_ab_lkp_mntn_email_notify.sh
#            (maintain_sql modified.)
#
# Description:  The script has two functions:
#               1. Maintain the lookup table, if there is new record with value
#                  'UNKNOWN' to in desc column
#               2. if there are 'UNKNOWN' columns, this process will generate a
#                  report of those new rows and email to corresponding people. The
#                  mail will be sent eachrun untill the issue fixed.
#               3. The values of optional parameters MAIN_DB & LKP_DB can be specified if the
#                  DB names are different from the default. Even if one of the 2 is specified
#                  the other DB will be the default $readDB
#
# Developer:  John Chen
# Create on:  Feb. 8, 2006
# Location:  $DW_EXE
# Called BY:  $DW_EXE/shell_handler.ksh or Appworx
# Run Env:  sjcitetl#.sjc.ebay.com
usage () {
cat << EOF
#   Parameters:
#       ETL_ID=<etl id>
#       JOB_ENV=<dual-active database environment:primary or secondary>
#       MAIN_TBL=<main table name>.<lookup code column name>:<default value>
#       LKP_TB=<lookup table name>
#       LKP_TBL_CODE=<lookup code column name>
#       LKP_TBL_DESC=<lookup desc column name>:<default value>
#       MAIL_DL=<emal list for notify, concatenated by comma>  --optional
#       MAIN_DB=<database name for main table> -- optional
#       LKP_DB=<database name for lookup table> -- optional
#
# EXAMPLES
#       : ${SCRIPTNAME} ETL_ID=dw_rtp.dw_rtp_rule_actn_code JOB_ENV=primary MAIN_TBL=stg_rtp_rule_actn_w.DISCOUNT_TYPE:-99 LKP_TB=dw_rtp_discnt_type_lkp LKP_TBL_CODE=discnt_type_id LKP_TBL_DESC=discnt_type_desc:UNKNOWN MAIL_DL=johnc@dxsolution.com,ychai@ebay.com
EOF
}
#
# Output/Return Code
#   Return Code:   0:success
#                  101:failed Usege Error
#                  104:failed to determining value for EMAIL_ERR_GROUP parameter from $DW_CFG/subject_area_email_list.dat
#                  105:failed to compose maintain sql
#                  106:failed to run maintain sql
#                  107:failed to compose report generating sql
#                  108:failed to run report generating sql
#
# Revision History:
# Date      Ver#     Modified By     Comments
# -------    -----    ------------    -------------------
# 02/08/06    1.0    John Chen        initial script
# 06/02/06    2.0    Orlando Jin      Include optional parameter 'DB' for another database
# 08/15/06    3.0    Richard Xu       Change the insert sql, group by the source maintain column first and then join to lkp table.
#                                     Added status collection on maintain column
# 03/21/13    3.1    Ryan Wong        Modify MTN_SQL_FILE and NOTIFY_SQL_FILE to include JOB_ENV in name to avoid runtime collisions.
# 10/04/13    3.2    Ryan Wong        Redhat changes
# ==============================================================================
SCRIPTNAME=`basename $0`
CURR_DATETIME=$(date '+%Y%m%d-%H%M%S')

if  [[ $# -lt 4 ]]
then
    usage
    exit 101
fi


. /export/home/abinitio/cfg/abinitio.setup

#------------------------------------------------
#------------ input params ----------------------
#------------------------------------------------
print "$*" > $DW_TMP/lkp.log
params="$*"
for p in $params;do
  eq=`print $p|grep = >/dev/null 2>&1;print $?`
  if (( eq == 0 )); then
    exc=`print $p|egrep -e "ETL_ID=|JOB_ENV=|MAIN_TBL=|LKP_TB=|LKP_TBL_CODE=|LKP_TBL_DESC=|MAIL_DL=|MAIN_DB=|LKP_DB
=" >/dev/null 2>&1;print $?`
    if (( exc == 0 )); then
      name=`print ${p%%=*}`
      value=`print ${p##*=}`
      eval `print $name=$value`
    fi
  fi
done

if [[ -n ${MAIN_DB} ]] then ## main table DB specified explicitly
  if [[ -n ${LKP_DB} ]] then ## lookup table DB specified explicitly
     gdwDB=${LKP_DB}
     readDB=${MAIN_DB}
  else
     readDB=${MAIN_DB}
  fi
fi

#------------------------------------------------
#------------ check input params ----------------
#------------------------------------------------
if [[ -n $ETL_ID && -n JOB_ENV && -n $MAIN_TBL && -n $LKP_TB && -n $LKP_TBL_CODE && -n $LKP_TBL_DESC ]]; then
  :
else
  print "One or more parameters are missing or mispelled. Command was : $SCRIPTNAME $*"
  print "Parameters or Env variables needed: ETL_ID,JOB_ENV,MAIN_TBL,LKP_TB,LKP_TBL_CODE,LKP_TBL_DESC"
  usage
  exit 101
fi

fmt1=`print $MAIN_TBL|grep . >/dev/null 2>&1;print $?`
fmt2=`print $MAIN_TBL|grep : >/dev/null 2>&1;print $?`
fmt3=`print $LKP_TBL_DESC|grep . >/dev/null 2>&1;print $?`
fmt4=`print $ETL_ID|grep . >/dev/null 2>&1;print $?`

if [[ $fmt1 == 0 || $fmt2 == 0 ]]; then
  :
else
  print "Main table miss format. The right format is : MAIN_TBL=<main table name>.<lookup code column name>:<default value>"
  usage
  exit 101
fi

if [[ $fmt3 == 0 ]]; then
  :
else
  print "Lookup table desc column miss format. The right format is : LKP_TBL_DESC=<lookup desc column name>:<default value>"
  usage
  exit 101
fi

if [[ $fmt4 == 0 ]]; then
  :
else
  print "Wrong ETL_ID format. The right format is : ETL_ID=<subject_area.table_id>"
  usage
  exit 101
fi

#if [[ $JOB_ENV == primary ]] || [[ $JOB_ENV == secondary ]]; then
#  :
#else
#  print "Database environment must be primary or secondary. The right format is : JOB_ENV=<dual-active database environment:primary or secondary>"
#  usage
#  exit 101
#fi

print 'check input params completed'
#----------------------------------------------------------
#------------ set variables with input params -------------
#----------------------------------------------------------
MTN_INFO=${MAIN_TBL%%:*}
MTN_DEF_VAL=${MAIN_TBL##*:}

MTN_TB=${MTN_INFO%%.*}
MTN_COL=${MTN_INFO##*.}

LKP_TBL_CODE=${LKP_TBL_CODE}
LKP_DESC_DEF=${LKP_TBL_DESC##*:}
LKP_TBL_DESC=${LKP_TBL_DESC%%:*}

SUBJECT="New lookup Entries added to ${LKP_TB} on ${JOB_ENV} DB"

SUBJECT_AREA=${ETL_ID%%.*}

grep "^$SUBJECT_AREA\>" $DW_CFG/subject_area_email_list.dat | read PARAM EMAIL_ERR_GROUP

EMAIL_ERR_GROUP=${EMAIL_ERR_GROUP},${MAIL_DL}

if [[ -n $EMAIL_ERR_GROUP ]]; then
 :
else
        print "${0##*/}:  ERROR, return 104, failure determining value for EMAIL_ERR_GROUP parameter from $DW_CFG/subject_area_email_list.dat"
        exit 104
fi


MTN_SQL_FILE=$DW_SQL/${MTN_TB}.${LKP_TB}.${JOB_ENV}.mntn.sql
#rm -f $MTN_SQL_FILE

print 'Set variables with input params completed'
#------------------------------------------------
#------------ compose maintain sql --------------
#------------------------------------------------

print "
.maxerror 1;

COLLECT STATISTICS ON ${workingDB}.${MTN_TB} COLUMN ${MTN_COL};

INSERT INTO ${gdwDB}.${LKP_TB}
(  ${LKP_TBL_CODE}
,  ${LKP_TBL_DESC}
, UPD_DATE
)
SELECT
  coalesce(a.${MTN_COL},'${MTN_DEF_VAL}')
,  '${LKP_DESC_DEF}'
, CURRENT_TIMESTAMP(0)
FROM  (SELECT ${MTN_COL} FROM ${readDB}.${MTN_TB} GROUP BY 1) a
LEFT OUTER JOIN
${readDB}.${LKP_TB} b
ON coalesce(a.${MTN_COL},'${MTN_DEF_VAL}') = b.${LKP_TBL_CODE}
--ON a.${MTN_COL} = b.${LKP_TBL_CODE}
WHERE b.${LKP_TBL_CODE} IS NULL 
;

" > $MTN_SQL_FILE

rcode=$?
if (( rcode > 0 ))
then
    print 'Return 105, failed to compose maintain sql'
        exit 105
fi
print 'compose maintain sql completed'
# ------------------------------------------------------------------------------
# ------------------- run maintain script --------------------------------------
# ------------------------------------------------------------------------------

print "start to run: $DW_EXE/target_table_load_handler.ksh $ETL_ID $JOB_ENV ${MTN_SQL_FILE##*/}"

$DW_EXE/target_table_load_handler.ksh $ETL_ID $JOB_ENV ${MTN_SQL_FILE##*/}

rcode=$?
if (( rcode > 0 ))
then
    print 'Return 106, failed to run maintain sql'
        exit 106
fi

print "Successfully maintained the ${MTN_TB} table"

rm -f $MTN_SQL_FILE

# ------------------------------------------------------------------------------
# ------------------- Mail Notification Function -------------------------------
# ------------------------------------------------------------------------------

function send_report {

mail -t ${EMAIL_ERR_GROUP} <<EOF
subject: "$SUBJECT"
Mime-Version: 1.0
Content-Type: multipart/mixed; boundary="myboundary"

--myboundary
Content-Type: text/plain; charset=us-ascii

:- See Attached Report

Please update the defaulted entries in the corresponding lookup tables

Lookup table:
  ${LKP_TB}
Affected table:
  ${MTN_TB}

Thanks,
DW Admin

--myboundary
Content-Type: text/plain; charset=us-ascii; name="`basename $RPTFILE`"
Content-Description: $RPTFILE

`cat $RPTFILE`

EOF
}

typeset -ft send_report

# ------------------------------------------------------------------------------
# -------------- Beginning of Notify Script ------------------------------------
# ------------------------------------------------------------------------------

RPTFILE="$DW_LOG/$JOB_ENV/$SUBJECT_AREA/dw_ab_lkp_mntn_${LKP_TB}_notify.${CURR_DATETIME}.report.txt"
rm -f $RPTFILE

NOTIFY_SQL_FILE=$DW_SQL/${MTN_TB}.${LKP_TB}.${JOB_ENV}.notify.sql


# Alert: Do not remove this, this is required to keep the lines
# out of print as is in the output .sql file
export TIFS=$IFS
export IFS=

# ------------------------------------------------------------------------------
# --------------  sql to generate report ---------------------------------------
# ------------------------------------------------------------------------------

print "

        .set format on
        .set echoreq off
  .EXPORT report FILE=$RPTFILE,open

  -- Identify new ${MTN_COL}
  .set heading '//Report of new entries in ${LKP_TB} : &DATE &TIME'
  select
    cast(${LKP_TBL_CODE} as char(10)) as ${LKP_TBL_CODE}
    ,substr(${LKP_TBL_DESC},1,30) as ${LKP_TBL_DESC}
    ,CRE_DATE as CREATION_DATE
  from
                ${readDB}.${LKP_TB}
        where
                ${LKP_TBL_DESC} = '${LKP_DESC_DEF}'
        and     ${LKP_TBL_CODE} <> '${MTN_DEF_VAL}'
        order by
                ${LKP_TBL_CODE}
        ;

  .EXPORT DATA FILE=$RPTFILE,close
" > $NOTIFY_SQL_FILE

rcode=$?
if (( rcode > 0 ))
then
    print 'Return 107, failed to compose report generating sql'
        exit 107
fi


# Restore
export IFS=$TIFS


print 'report generating script completed'

# ------------------------------------------------------------------------------
# ------------------- run report generating script -----------------------------
# ------------------------------------------------------------------------------

print "start to run $DW_EXE/target_table_load_handler.ksh $ETL_ID $JOB_ENV ${NOTIFY_SQL_FILE##*/}"
$DW_EXE/target_table_load_handler.ksh $ETL_ID $JOB_ENV ${NOTIFY_SQL_FILE##*/}

rcode=$?
if (( rcode > 0 ))
then
    print 'Return 108, failed to run report generating sql'
        exit 108
fi

rm -f $NOTIFY_SQL_FILE

# ------------------------------------------------------------------------------
# -------------- Sending report ------------------------------------------------
# ------------------------------------------------------------------------------


set `wc -l $RPTFILE`
if [[ -f $RPTFILE ]] && (( $1 > 2 )); then
      send_report  >> /dev/null 2>&1
      MSG="Report file for $PROJ $LKP_TB lookup: $RPTFILE"
      print INFO:"$MSG"
      MSG="Emailed new $PROJ $LKP_TB lookup entries to $EMAIL_ADDR"
      print INFO:"$MSG"
else
      MSG="No new entries for $PROJ $LKP_TB lookup to report"
      print INFO:"$MSG"
fi

rm -f $RPTFILE

exit

