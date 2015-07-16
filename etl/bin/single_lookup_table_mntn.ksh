#!/usr/bin/ksh

if [ $# -gt 0 -a X"$1" = X"-help" ]; then
print -r -- 'Usage: lookup_table_mntn.ksh <ETL_ID> <JOB_ENV> <MAIN_TB> <MAIN_TBL_DESC> <LKP_TB> <LKP_TBL_CODE> <LKP_TBL_DESC> [<PARAM_LIST>]'
exit 4
fi

ETL_ID=$1
JOB_ENV=$2
MAIN_TB=$3
MAIN_TBL_DESC=$4
LKP_TB=$5
LKP_TBL_CODE=$6
LKP_TBL_DESC=$7
shift 7
PARAM_LIST=$*

echo "$*" > $DW_TMP/$JOB_ENV/$SUBJECT_AREA/lkp.log

for p in $PARAM_LIST;do
  eq=`echo $p|grep = >/dev/null 2>&1;echo $?`
  if (( eq == 0 )); then
    exc=`echo $p|egrep -e "MAIL_DL=|MAIN_DB=|LKP_DB=|SURRGT_ID_YN=|LKP_TBL_ID=" >/dev/null 2>&1;echo $?`
    if (( exc == 0 )); then
      name=${p%%=*}
      value=${p##*=}
      eval `echo $name=$value`
    fi
  fi
done

echo $MAIN_DB
echo $LKP_DB

if [[ -z ${MAIN_DB} ]] then # main table DB specified explicitly
  if [[ -z ${LKP_DB} ]] then # lookup table DB specified explicitly
    #gdwDB=${LKP_DB}
    #readDB=${MAIN_DB}
    LKP_DB=${gdwDB}
    MAIN_DB=${readDB}
  else
    #readDB=${MAIN_DB}
    MAIN_DB=${readDB}
  fi
fi

#------------------------------------------------
#------------ check input params ----------------
#------------------------------------------------
if [[ -n $MAIN_TB && -n $MAIN_TBL_DESC && -n $LKP_TB && -n $LKP_TBL_CODE && -n $LKP_TBL_DESC ]]; then
  :
else
  echo "${0##*/}:  ERROR, One or more parameters are missing."
  echo "Parameters or Env variables needed: MAIN_TB,MAIN_TBL_DESC,LKP_TB,LKP_TBL_CODE,LKP_TBL_DESC"
  exit 4
fi

fmt1=`echo $MAIN_TBL_DESC|grep : >/dev/null 2>&1;echo $?`
fmt2=`echo $LKP_TBL_DESC|grep : >/dev/null 2>&1;echo $?`

if [[ $fmt1 == 0 ]]; then
  :
else
  echo "${0##*/}:  ERROR, Main table desc column miss format. The right format is : <lookup code column name>:<default value>"
  exit 4
fi

if [[ $fmt2 == 0 ]]; then
  :
else
  echo "${0##*/}:  ERROR, Lookup table desc column miss format. The right format is : <lookup desc column name>:<default value>"
  exit 4
fi
echo "check params completed for $LKP_TB"
#----------------------------------------------------------
#------------ set variables with input params -------------
#----------------------------------------------------------
MTN_TB=$MAIN_TB
MTN_COL=${MAIN_TBL_DESC%%:*}
MTN_DEF_VAL=${MAIN_TBL_DESC##*:}


LKP_TBL_CODE=${LKP_TBL_CODE}
LKP_DESC_DEF=${LKP_TBL_DESC##*:}
LKP_TBL_DESC=${LKP_TBL_DESC%%:*}

SUBJECT="New lookup Entries added to ${LKP_TB} on ${JOB_ENV} DB"

SUBJECT_AREA=${ETL_ID%%.*}

grep "^$SUBJECT_AREA\>" $DW_CFG/subject_area_email_list.dat | read PARAM EMAIL_ERR_GROUP

if [[ -z $MAIL_DL ]]
then
  :
else
  EMAIL_ERR_GROUP=${EMAIL_ERR_GROUP},${MAIL_DL}
fi

if [[ -n $EMAIL_ERR_GROUP ]]; then
  :
else
  echo "${0##*/}:  ERROR, failure determining value for EMAIL_ERR_GROUP parameter from $DW_CFG/subject_area_email_list.dat"
  exit 4
fi

MTN_SQL_FILE=$DW_SQL/${MAIN_DB}.${MTN_TB}.${MTN_COL}.${LKP_DB}.${LKP_TB}.mntn.sql
rm -f $MTN_SQL_FILE

echo "Set variables with input params completed"
#------------------------------------------------
#------------ compose maintain sql --------------
#------------------------------------------------
if [[ $SURRGT_ID_YN == 'Y' ]]
then
  echo "
  .maxerror 1;

  INSERT INTO ${LKP_DB}.${LKP_TB}
  (
  ${LKP_TBL_ID}
  ,${LKP_TBL_CODE}
  ,${LKP_TBL_DESC}
  ,UPD_DATE
  )
  SELECT
  CSUM(1, t.${MTN_COL}) + MAX_ID.MAX_ID
  ,t.${MTN_COL}
  ,'${LKP_DESC_DEF}'
  ,CURRENT_TIMESTAMP(0)
  FROM
  (
  SELECT coalesce(a.${MTN_COL},'${MTN_DEF_VAL}') as ${MTN_COL}
  FROM ${MAIN_DB}.${MTN_TB} a
  LEFT OUTER JOIN ${LKP_DB}.${LKP_TB} b
  --ON coalesce(a.${MTN_COL},'${MTN_DEF_VAL}') = b.${LKP_TBL_CODE}
  ON a.$MTN_COL = b.${LKP_TBL_CODE}
  WHERE b.${LKP_TBL_CODE} IS NULL 
  AND a.${MTN_COL} IS NOT NULL
  GROUP BY a.${MTN_COL}
  ) as t,
  (
  SELECT coalesce(MAX(${LKP_TBL_ID}),0) AS MAX_ID
  FROM  ${LKP_DB}.${LKP_TB}
  ) MAX_ID;
  " > $MTN_SQL_FILE
else
  echo "
  .maxerror 1;

  INSERT INTO ${LKP_DB}.${LKP_TB}
  (  ${LKP_TBL_CODE}
  ,  ${LKP_TBL_DESC}
  , UPD_DATE
  )
  SELECT
  coalesce(a.${MTN_COL},'${MTN_DEF_VAL}')
  , '${LKP_DESC_DEF}'
  , CURRENT_TIMESTAMP(0)
  FROM  ${MAIN_DB}.${MTN_TB} a
  LEFT OUTER JOIN
  ${LKP_DB}.${LKP_TB} b
  --ON coalesce(a.${MTN_COL},'${MTN_DEF_VAL}') = b.${LKP_TBL_CODE}
  ON a.$MTN_COL = b.${LKP_TBL_CODE}
  WHERE b.${LKP_TBL_CODE} IS NULL and a.${MTN_COL} IS NOT NULL
  GROUP BY a.${MTN_COL};
  " > $MTN_SQL_FILE
fi

rcode=$?
if (( rcode > 0 ))
then
  echo "${0##*/}:  ERROR, failed to compose maintain sql"
  exit 4
fi
echo "compose maintain sql completed"

# ------------------------------------------------------------------------------
# ------------------- run maintain script --------------------------------------
# ------------------------------------------------------------------------------
echo "start to run: $DW_EXE/target_table_load_handler.ksh $ETL_ID $JOB_ENV ${MTN_SQL_FILE##*/}"
$DW_EXE/target_table_load_handler.ksh $ETL_ID $JOB_ENV ${MTN_SQL_FILE##*/}

rcode=$?
if (( rcode > 0 ))
then
  echo "${0##*/}:  ERROR, failed to run maintain sql"
  exit 4
fi

echo "Successfully maintained the ${MTN_TB} table"

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
RPTFILE="$DW_LOG/$JOB_ENV/$SUBJECT_AREA/${LKP_TB}_by_${MTN_TB}_${MTN_COL}_notify.report.txt"
rm -f $RPTFILE

NOTIFY_SQL_FILE=$DW_SQL/${MTN_TB}_${LKP_TB}_${MTN_COL}_notify.sql


# Alert: Do not remove this, this is required to keep the lines
# out of echo as is in the output .sql file
export TIFS=$IFS
export IFS=

# ------------------------------------------------------------------------------
# --------------  sql to generate report ---------------------------------------
# ------------------------------------------------------------------------------
echo "

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
                ${LKP_DB}.${LKP_TB}
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
  echo "${0##*/}:  ERROR, failed to compose report generating sql"
  exit 4
fi

# Restore
export IFS=$TIFS

echo 'report generating script completed'

# ------------------------------------------------------------------------------
# ------------------- run report generating script -----------------------------
# ------------------------------------------------------------------------------
echo "start to run $DW_EXE/target_table_load_handler.ksh $ETL_ID $JOB_ENV ${NOTIFY_SQL_FILE##*/}"
$DW_EXE/target_table_load_handler.ksh $ETL_ID $JOB_ENV ${NOTIFY_SQL_FILE##*/}

rcode=$?
if (( rcode > 0 ))
then
  echo "${0##*/}:  ERROR,  failed to run report generating sql"
  exit 4
fi

# ------------------------------------------------------------------------------
# -------------- Sending report ------------------------------------------------
# ------------------------------------------------------------------------------
set `wc -l $RPTFILE`
if [[ -f $RPTFILE ]] && (( $1 > 2 )); then
  send_report  >> /dev/null 2>&1
  MSG="Report file for $PROJ $LKP_TB lookup: $RPTFILE"
  echo INFO:"$MSG"
  MSG="Emailed new $PROJ $LKP_TB lookup entries to $EMAIL_ADDR"
  echo INFO:"$MSG"
else
  MSG="No new entries for $PROJ $LKP_TB lookup to report"
  echo INFO:"$MSG"
fi

exit
