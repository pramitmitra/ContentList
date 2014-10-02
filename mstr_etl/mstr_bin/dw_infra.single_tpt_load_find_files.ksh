#!/bin/ksh -eu
# Title:        Single TPT Load Find Files
# File Name:    dw_infra.single_tpt_load_find_files.ksh
# Description:  Find files for loading using TPT
# Developer:    Ryan Wong
# Created on:   2014-08-07
# Location:     $DW_MASTER_BIN
# Logic:
#
#
# Called by:    UC4/Unix
#
# Date         Ver#   Modified By(Name)            Change and Reason for Change
#---------    -----  ---------------------------  ------------------------------
# 2014-08-07   1.0    Ryan Wong                     Initial
# 2014-09-03   1.1    Ryan Wong                     Fix to handle NON-UOW files
# 2014-10-02   1.2    Ryan Wong                     Add special case for UOW_FROM equals UOW_TO
#############################################################################################################

while [ $# -gt 0 ]
do
  DWI_KWD="${1}"
  shift
  case $DWI_KWD in
    -ETL_ID )
      export ETL_ID="${1}"
      shift
      ;;
    -IN_DIR )
      export IN_DIR="${1}"
      shift
      ;;
    -DATA_FILE_PATTERN )
      export DATA_FILE_PATTERN="${1}"
      shift
      ;;
    -UOW_FROM )
      export UOW_FROM="${1}"
      shift
      ;;
    -UOW_TO )
      export UOW_TO="${1}"
      shift
      ;;
    * )
      print "FATAL ERROR:  Unexpected command line argument"
      print "Usage: $0 -ETL_ID <ETL_ID> -IN_DIR <IN_DIR> -DATA_FILE_PATTERN <DATA_FILE_PATTERN> [-UOW_FROM <UOW_FROM> -UOW_TO <UOW_TO>]"
      exit 4
  esac
done

# If ETL_ID is not assigned, then error.
if [[ "X${ETL_ID:-}" = "X" ]]
then
  print "FATAL ERROR: ETL_ID not passed to $0" >&2
  exit 4
fi

# If IN_DIR is not assigned, then error.
if [[ "X${IN_DIR:-}" = "X" ]]
then
  print "FATAL ERROR: IN_DIR not passed to $0" >&2
  exit 4
fi

# If DATA_FILE_PATTERN is not assigned, then error.
if [[ "X${DATA_FILE_PATTERN:-}" = "X" ]]
then
  print "FATAL ERROR: DATA_FILE_PATTERN not passed to $0" >&2
  exit 4
fi

. /dw/etl/mstr_cfg/etlenv.setup
. /dw/etl/mstr_cfg/dw_etl_common_defs_uow.cfg

if [[ "X$UOW_TO" != "X" ]]
then
   typeset -i UOW_ITER_DATE
   typeset -Z2 UOW_ITER_HH
   typeset -i UOW_ITER_DATEHH
   UOW_TO_DATEHH=$UOW_TO_DATE$UOW_TO_HH
   UOW_ITER_DATE=$UOW_FROM_DATE
   UOW_ITER_HH=$UOW_FROM_HH
   UOW_ITER_DATEHH=$UOW_ITER_DATE$UOW_FROM_HH
   UOW_IN_DIR=${IN_DIR%/$UOW_TO_DATE/$UOW_TO_HH/$UOW_TO_MI/$UOW_TO_SS}

   if [[ $UOW_FROM -eq $UOW_TO ]]
   then
      ls $UOW_IN_DIR/$UOW_TO_DATE/$UOW_TO_HH/$UOW_TO_MI/$UOW_TO_SS/$DATA_FILE_PATTERN | grep -v record_count
   else
      while [[ $UOW_ITER_DATEHH -le $UOW_TO_DATEHH ]]
      do
         if [[ $UOW_ITER_DATE -lt $UOW_TO_DATE ]]
         then
            UOW_ITER_HH_TO=23
         else
            UOW_ITER_HH_TO=$UOW_TO_HH
         fi

         while [[ $UOW_ITER_HH -le $UOW_ITER_HH_TO ]]
         do
            for IN_DIR_TMP in $(find $UOW_IN_DIR/$UOW_ITER_DATE/$UOW_ITER_HH/[0-5][0-9]/[0-5][0-9] -type d -prune 2>/dev/null)
            do
               if [[ "$IN_DIR_TMP" != "$UOW_IN_DIR/$UOW_FROM_DATE/$UOW_FROM_HH/$UOW_FROM_MI/$UOW_FROM_SS" ]]
               then
                  ls $IN_DIR_TMP/$DATA_FILE_PATTERN | grep -v record_count
               fi
            done
            ((UOW_ITER_HH=UOW_ITER_HH+1))
         done
         UOW_ITER_DATE=$($DW_BIN/add_days $UOW_ITER_DATE 1)
         UOW_ITER_HH=00
         UOW_ITER_DATEHH=$UOW_ITER_DATE$UOW_ITER_HH
      done
   fi
else
  ls $IN_DIR/$DATA_FILE_PATTERN | grep -v record_count
fi

exit 0
