#!/bin/ksh -eu
####################################################################################################
# Title:        Single TPT Run
# File Name:    single_tpt_run.ksh
# Description:  This script will run one instance of tpt locally, extract or load
# Developer:    Ryan Wong
# Created on:
# Location:     $DW_MASTER_BIN
# Logic:
#
# Date         Ver#   Modified By(Name)            Change and Reason for Change
#---------    -----  ---------------------------  ------------------------------
# 2012-11-11  1.1    Ryan Wong                    Initial
# 2013-10-04  1.2    Ryan Wong                    Redhat changes
# 2013-10-29  1.3    Ryan Wong                    Add TPT_LOAD_ERROR_LIMIT option
#
####################################################################################################

# ALL POSSIBLE MAPPINGS.  Certain mappings are not exposed due to process control or security.
# tpt_arg_values s u p d t wd mn po i n f fl l dl dc c th ts ns e df le lf fp z zl qb sq sf sm bm rd to v pi ot
# tpt_cfg_values SERVER USER PASSWORD DB TABLE WORKING_DB MASTER_NODE PORT INSTANCE_NUM NWAYS FILENAME LISTFILE LOGFILE HEX_DELIMITER CHAR_DELIMITER CHARSET TENACITY_HOURS TENACITY_SLEEP NUMSESSIONS ERROR_LIMIT DATE_FORMAT LOGON_ENV LOGON_FILE FILE_PATTERN COMPRESS_FLAG COMPRESS_LEVEL QUERY_BAND SQL SQLFILE SPOOLMODE BUFFERMODE REMOVE_DELIMITER TEXTOUT VERBOSE PRINT_INTERVAL OPERATOR_TYPE
#

set -A TPT_MAP_VALUES MASTER_NODE STAGE_DB STAGE_TABLE WORKING_DB PORT NWAYS HEX_DELIMITER CHAR_DELIMITER CHARSET TENACITY_HOURS TENACITY_SLEEP SESSIONS ERROR_LIMIT DATE_FORMAT COMPRESS_FLAG COMPRESS_LEVEL QUERY_BAND SQLFILE SPOOLMODE BUFFERMODE REMOVE_DELIMITER TEXTOUT VERBOSE PRINT_INTERVAL OPERATOR_TYPE INSTANCE_NUM HOST_INSTANCE_NUM HOST_INSTANCE_TOTAL FILENAME LISTFILE LOGFILE SERVER ETL_ID JOB_ENV IN_DIR PARAM1 PARAM2 PARAM3 PARAM4 FROM_EXTRACT_VALUE TO_EXTRACT_VALUE
set -A TPT_NORMAL_VALUES mn d t wd po n dl dc c th ts ns e df z zl qb sf sm bm rd to v pi ot i hin hit f fl l s ei je id p1 p2 p3 p4 fev tev

set -A tpt_flg_values
set -A tpt_arg_values

export ETL_ID=""
export JOB_ENV=""
TPT_ARG=""
#--------------------------------------
# parse arguments
#--------------------------------------
tpt_idx=0
while [[ ${#} -gt 0 ]]
do
  # get option from command line
  option="$1"

  # test that option is valid
  args_idx=0
  rcode=255
  while [[ $args_idx -lt ${#TPT_MAP_VALUES[*]} ]]
  do
    if [[ $option = "-${TPT_MAP_VALUES[$args_idx]}" ]]
    then
      tpt_flg_values[$tpt_idx]="-${TPT_NORMAL_VALUES[$args_idx]}"
      rcode=0
      break
    fi
    ((args_idx+=1))
  done

  if [[ $rcode -eq 255 ]]
  then
    print "${0##*/}:  FATAL ERROR, Invalid option: $option"
    exit 4
  fi

  # try to get value for previous option
  if [[ ${#} -gt 1 ]]
  then
    shift
    value="$1"
    tpt_arg_values[$tpt_idx]="$value"
    shift
  else
    print "${0##*/}:  FATAL ERROR, No option value provided for: $option"
    exit 4;
  fi

  export ${option#-}="$value"

  if [[ "${option#-}" = @(MASTER_NODE|STAGE_DB|STAGE_TABLE|WORKING_DB|PORT|NWAYS|HEX_DELIMITER|CHAR_DELIMITER|CHARSET|TENACITY_HOURS|TENACITY_SLEEP|SESSIONS|ERROR_LIMIT|DATE_FORMAT|COMPRESS_FLAG|COMPRESS_LEVEL|SPOOLMODE|BUFFERMODE|REMOVE_DELIMITER|TEXTOUT|VERBOSE|PRINT_INTERVAL|OPERATOR_TYPE|INSTANCE_NUM|FILENAME|LOGFILE) ]]
  then
    TPT_ARG="$TPT_ARG -${TPT_NORMAL_VALUES[$args_idx]} $value"
  elif [[ "${option#-}" = "QUERY_BAND" ]]
  then
    TPT_ARG="$TPT_ARG -${TPT_NORMAL_VALUES[$args_idx]} \"$value\""
  fi

  ((tpt_idx+=1))
done

if [[ "X${ETL_ID:-}" = "X" ]]
then
  print "${0##*/}:  FATAL ERROR, ETL_ID not defined" >&2
  exit 4
fi

if [[ "X${JOB_ENV:-}" = "X" ]]
then
  print "${0##*/}:  FATAL ERROR, JOB_ENV not defined" >&2
  exit 4
fi

. /dw/etl/mstr_cfg/etlenv.setup
. $DW_MASTER_CFG/dw_etl_common_defs.cfg

if [[ "X${OPERATOR_TYPE:-}" = "X" ]]
then
  print "${0##*/}:  FATAL ERROR, OPERATOR_TYPE not defined" >&2
  exit 4
fi

if [[ "X${SERVER:-}" = "X" ]]
then
  print "${0##*/}:  FATAL ERROR, SERVER not defined" >&2
  exit 4
fi

# Define TD_LOGON_ENV
export TD_LOGON_ENV=$SERVER/$TD_USERNAME,$TD_PASSWORD
TPT_ARG="$TPT_ARG -le TD_LOGON_ENV"

if [[ $OPERATOR_TYPE = 2 ]]
then
  # For extract:  Generate SQL file, also create IN_DIR if not exist for UOW
  if [[ "X${SQLFILE:-}" = "X" ]]
  then
    print "${0##*/}:  FATAL ERROR, SQLFILE not defined" >&2
    exit 4
  fi
  SQLFILE_TMP=$SQLFILE.$INSTANCE_NUM.tmp
  mkdirifnotexist $IN_DIR
  print "cat <<EOF" > $SQLFILE_TMP
  cat $DW_SQL/$ETL_ID.sel.sql >> $SQLFILE_TMP
  print "\nEOF" >> $SQLFILE_TMP
  . $SQLFILE_TMP > ${SQLFILE_TMP}2
  mv ${SQLFILE_TMP}2 $SQLFILE_TMP
  TPT_ARG="$TPT_ARG -sf $SQLFILE_TMP"
else
  # For load:  Generate list file
  if [[ "X${LISTFILE:-}" = "X" ]]
  then
    print "${0##*/}:  FATAL ERROR, LISTFILE not defined" >&2
    exit 4
  fi
  if [[ "X${HOST_INSTANCE_NUM:-}" = "X" ]]
  then
    print "${0##*/}:  FATAL ERROR, HOST_INSTANCE_NUM not defined" >&2
    exit 4
  fi
  if [[ "X${HOST_INSTANCE_TOTAL:-}" = "X" ]]
  then
    print "${0##*/}:  FATAL ERROR, HOST_INSTANCE_TOTAL not defined" >&2
    exit 4
  fi

  INSTANCE_LISTFILE=$DW_SA_TMP/$TABLE_ID.load.lis.$INSTANCE_NUM
  > $INSTANCE_LISTFILE
  tpt_idx=0
  while read ln
  do
    INSTANCE_MOD=$(( ($tpt_idx % $HOST_INSTANCE_TOTAL) + 1 ))
    if [[ $INSTANCE_MOD = $HOST_INSTANCE_NUM ]]
    then
      print $ln >> $INSTANCE_LISTFILE
    fi
    ((tpt_idx+=1))
  done < $LISTFILE
  TPT_ARG="$TPT_ARG -fl $INSTANCE_LISTFILE"
fi


#--------------------------------------
# launch tpt
#--------------------------------------
print Running $DW_MASTER_EXE/tpt_load.64 $TPT_ARG
set +e
eval $DW_MASTER_EXE/tpt_load.64 $TPT_ARG
RCODE=$?
set -e

if [ $RCODE != 0 ]
then
  print "${0##*/}:  FATAL_ERROR, see log file $LOGFILE" >&2
  exit 4
fi

