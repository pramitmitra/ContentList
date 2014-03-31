#!/bin/ksh -eu
# Title:        Teradata tpt extract Handler
# File Name:    td_extract_tpt.ksh
# Description:  This script is to run one instance of load at a specified host.
# Developer: rganesan
# Created on:
# Location:     $DW_BIN
# Logic:
#
# Date         Ver#   Modified By(Name)            Change and Reason for Change
#---------    -----  ---------------------------  ------------------------------
# 2012-05-14  1.1    Ryan Wong                    Exposing query band option
# 2012-09-28  1.2    Ryan Wong                    Modify logon method.  Using options u,p exposes
#                                                 user and password to a ps command.  Use le instead
# 2013-10-04  1.3    Ryan Wong                    Redhat changes
# 2013-10-17  1.4    Ryan Wong                    Changed tpt_load binary to point to DW_MASTER_EXE
#
####################################################################################################

set -A tpt_normal_args le wd mn po i n f l dl dc c ns z od ui ei sf je qb
set -A tpt_custom_args TD_LOGON_ENV WORKING_DATABASE MASTER_NODE PORT INSTANCE_NBR INSTANCE_CNT DATA_FILE LOG_FILE HEX_DELIMITER CHAR_DELIMITER CHARSET SESSIONS COMPRESS_FLAG OUTPUT_DIR UOW_ID ETL_ID SQL_FILE JOB_ENV QUERY_BAND
set -A tpt_arg_values

#--------------------------------------
# functions
#--------------------------------------
search_args() {
  val=$*
  search_args_idx=0
  while [[ $search_args_idx -lt ${#tpt_normal_args[*]} ]]
  do
    tpt_default_arg=${tpt_normal_args[$search_args_idx]}
    tpt_custom_arg=${tpt_custom_args[$search_args_idx]}
    if [[ $val = "-${tpt_normal_args[$search_args_idx]}" || $val = "-${tpt_custom_args[$search_args_idx]}" ]]
    then
      return $search_args_idx
    fi
    search_args_idx=$((search_args_idx+1))
  done
  return 255
}

#--------------------------------------
# parse arguments
#--------------------------------------
while [[ ${#} -gt 0 ]]
do
  #get option from command line
  option="$1"

  #test that option is valid
  search_args "$option"
  tpt_arg_idx=$?
  if [[ $tpt_arg_idx -eq 255 ]]
  then
    print "${0##*/}:  ERROR, Invalid argument: $option"
    exit 4
  fi

  #try to get value for previous option
  if [[ ${#} -gt 1 ]]
  then
    shift
    value="$1"
    tpt_arg_values[$tpt_arg_idx]="$value"
  else
    print "${0##*/}:  ERROR, No value for option: $option"
    exit 4;
  fi

  #move on to next option/value set
  if [[ ${#} -gt 0 ]]
  then
    shift
  fi
done

search_args "-ETL_ID"
search_arg_idx=$?
export ETL_ID=${tpt_arg_values[$search_arg_idx]}

search_args "-JOB_ENV"
search_arg_idx=$?
export JOB_ENV=${tpt_arg_values[$search_arg_idx]}

. /dw/etl/mstr_cfg/etlenv.setup
. $DW_MASTER_CFG/dw_etl_common_defs.cfg

TD_SERVERNAME=$(JOB_ENV_UPPER=$(print $JOB_ENV | tr "[:lower:]" "[:upper:]"); eval print \$DW_${JOB_ENV_UPPER}_DB)
export TD_LOGON_ENV=$TD_SERVERNAME/$TD_USERNAME,$TD_PASSWORD
search_args "-TD_LOGON_ENV"
search_arg_idx=$?
tpt_arg_values[$search_arg_idx]="TD_LOGON_ENV"

search_args "-INSTANCE_NBR"
search_arg_idx=$?
INSTANCE_NBR=${tpt_arg_values[$search_arg_idx]}

search_args "-UOW_ID"
search_arg_idx=$?
UOW_ID=${tpt_arg_values[$search_arg_idx]}

search_args "-OUTPUT_DIR"
search_arg_idx=$?
OUTPUT_DIR=`print ${tpt_arg_values[$search_arg_idx]}`

search_args "-COMPRESS_FLAG"
search_arg_idx=$?
COMPRESS_FLAG=${tpt_arg_values[$search_arg_idx]}
if [[ $COMPRESS_FLAG = 1 ]]
then
  FILE_EXTN=".gz"
elif [[ $COMPRESS_FLAG = 2 ]]
then
  FILE_EXTN=".bz2"
elif [[ $COMPRESS_FLAG = 0 ]]
then
  FILE_EXTN=""
fi

# Output file file name
search_args "-DATA_FILE"
search_arg_idx=$?
tpt_arg_values[$search_arg_idx]="$OUTPUT_DIR/${tpt_arg_values[$search_arg_idx]}.${UOW_ID}.${INSTANCE_NBR}.dat${FILE_EXTN}"

search_args "-LOG_FILE"
search_arg_idx=$?
LOG_FILE=${tpt_arg_values[$search_arg_idx]}

#generate sql file
SQL_FILE=$DW_SQL/$ETL_ID.sel.sql
EXE_SQL_FILE=$DW_SA_TMP/$TABLE_ID.ex.sql.${INSTANCE_NBR}
SQL_FILE_TMP=$DW_SA_TMP/$TABLE_ID.ex.tmp.${INSTANCE_NBR}

print "cat <<EOF" > $SQL_FILE_TMP
cat $SQL_FILE >> $SQL_FILE_TMP
print "\nEOF" >> $SQL_FILE_TMP
. $SQL_FILE_TMP > $SQL_FILE_TMP.2
mv $SQL_FILE_TMP.2 $EXE_SQL_FILE
rm -f $SQL_FILE_TMP

search_args "-SQL_FILE"
search_arg_idx=$?
tpt_arg_values[$search_arg_idx]=$EXE_SQL_FILE

#--------------------------------------
# build argument string
#--------------------------------------
#build argument string
tpt_arg=""
tpt_args_idx=0
while [[ $tpt_args_idx -lt ${#tpt_normal_args[*]} ]]
do
  tpt_default_arg=${tpt_normal_args[$tpt_args_idx]}
  tpt_arg_value=${tpt_arg_values[$tpt_args_idx]}
  #exclude custom args
  if [[ X$tpt_arg_value != "X" && $tpt_default_arg != @(od|ui|ei|je) ]]
  then
    tpt_arg="$tpt_arg -$tpt_default_arg $tpt_arg_value"
  fi
  tpt_args_idx=$(( tpt_args_idx + 1 ))
done

#--------------------------------------
# launch tpt
#--------------------------------------
set +e
$DW_MASTER_EXE/tpt_load.64 -ot 2 $tpt_arg
RCODE=$?
set -e

if [ $RCODE != 0 ]
then
  print "${0##*/}:  ERROR, see log file $LOG_FILE"
  exit 4
fi

rm -f ${EXE_SQL_FILE}
