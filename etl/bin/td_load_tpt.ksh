#!/bin/ksh -eu
# Title:        Teradata tpt load Handler
# File Name:    td_load_tpt.ksh
# Description:  This script is to run one instance of load at a specified host.
# Developer:
# Created on:
# Location:     $DW_BIN
# Logic:
#
# Date         Ver#   Modified By(Name)            Change and Reason for Change
#---------    -----  ---------------------------  ------------------------------
# 2012-09-28  1.1    Ryan Wong                    Modify logon method.  Using options u,p exposes
#                                                 user and password to a ps command.  Use le instead
# 2013-10-04  1.2    Ryan Wong                    Redhat changes
# 2013-10-17  1.3    Ryan Wong                    Changed tpt_load binary to point to DW_MASTER_EXE
#
####################################################################################################

set -A tpt_normal_args le d t wd mn po i n l dl dc c ns fp z md mr id ei je ui fl qb 
set -A tpt_custom_args TD_LOGON_ENV DATABASE_NAME TABLE_NAME WORKING_DATABASE MASTER_NODE PORT INSTANCE_NBR INSTANCE_CNT LOG_FILE HEX_DELIMITER CHAR_DELIMITER CHARSET SESSIONS FILE_PATTERN COMPRESS_FLAG MODULO_DIVISOR MODULO_REMAINDER IN_DIR ETL_ID JOB_ENV UOW_ID DATA_LIST_FILE QUERY_BAND
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
    search_args_idx=$(( search_args_idx + 1 ))
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
    print "ERROR: Invalid argument: $option"
    exit 1
  fi

  #try to get value for previous option
  if [[ ${#} -gt 1 ]]
  then
    shift
    value="$1"
    tpt_arg_values[$tpt_arg_idx]="$value"
  else
    print "ERROR: No value for option: $option"
  fi

  #move on to next option/value set
  if [[ ${#} -gt 0 ]]
  then
    shift
  fi
done

#--------------------------------------
# extract select arguments
#--------------------------------------
search_args "-ETL_ID"
search_arg_idx=$?
export ETL_ID=${tpt_arg_values[$search_arg_idx]}

search_args "-JOB_ENV"
search_arg_idx=$?
export JOB_ENV=${tpt_arg_values[$search_arg_idx]}

export SUBJECT_AREA=${ETL_ID%%.*}
export TABLE_ID=${ETL_ID##*.}

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
export UOW_ID=${tpt_arg_values[$search_arg_idx]}

search_args "-IN_DIR"
search_arg_idx=$?
IN_DIR=${tpt_arg_values[$search_arg_idx]}

search_args "-FILE_PATTERN"
search_arg_idx=$?
FILE_PATTERN=${tpt_arg_values[$search_arg_idx]}

search_args "-MODULO_DIVISOR"
search_arg_idx=$?
MODULO_DIVISOR=${tpt_arg_values[$search_arg_idx]}

search_args "-MODULO_REMAINDER"
search_arg_idx=$?
MODULO_REMAINDER=${tpt_arg_values[$search_arg_idx]}

search_args "-COMPRESS_FLAG"
search_arg_idx=$?
COMPRESS_FLAG=${tpt_arg_values[$search_arg_idx]}
if [[ $COMPRESS_FLAG = 1 ]]
then
  FILE_EXTN=".gz"
elif [[ $COMPRESS_FLAG = 2 ]]
then
  FILE_EXTN=".bz2"
elif [[ $compress_flag = 0 ]]
then
  FILE_EXTN=""
fi

#--------------------------------------
# prepare file list
#--------------------------------------
#file pattern for data files
instance_data_list_file="$DW_SA_TMP/$TABLE_ID.ld.${INSTANCE_NBR}"
> $instance_data_list_file

data_file_entry_idx=0
# for pattern in $patterns
#list_pattern=`print ${IN_DIR}/*${FILE_PATTERN}*${FILE_EXTN}`
#for data_file_entry in `ls ${list_pattern} | grep $UOW_ID 2>/dev/null`
list_pattern=`print "*${FILE_PATTERN}*${FILE_EXTN}"`
for data_file_entry in `find ${IN_DIR} -name "${list_pattern}" | sort | grep $UOW_ID`
do
  modulo_result=$(( ( $data_file_entry_idx % $MODULO_DIVISOR ) + 1 ))
  if [[ $modulo_result -eq $MODULO_REMAINDER ]]
  then
    print "$data_file_entry" >> $instance_data_list_file
  fi

  data_file_entry_idx=$(( data_file_entry_idx + 1 ))
done

search_args "-DATA_LIST_FILE"
search_arg_idx=$?
tpt_arg_values[$search_arg_idx]=$instance_data_list_file

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
  if [[ $tpt_default_arg != @(md|mr|fp|id|ei|je|ui) && X$tpt_arg_value != "X" ]]
  then
    tpt_arg="$tpt_arg -$tpt_default_arg $tpt_arg_value"
  fi
  tpt_args_idx=$(( tpt_args_idx + 1 ))
done

#--------------------------------------
# launch tpt
#--------------------------------------
set +e
$DW_MASTER_EXE/tpt_load.64 -ot 1 $tpt_arg
RCODE=$?
set -e

if [ $RCODE != 0 ]
then
  print "${0##*/}:  ERROR, see log file $LOG_FILE"
  exit 4
fi
