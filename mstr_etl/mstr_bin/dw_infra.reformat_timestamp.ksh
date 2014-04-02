#!/bin/ksh -eu
# Title:        Reformat timestamp
# File Name:    dw_infra.reformat_timestamp.ksh
# Description:  Reformat timestamp assumed incoming format is of format 'YYYYMMDD24hrmmss'
# Developer:    Ryan Wong
# Created on:   2011-09-12
# Location:     $DW_MSTR_BIN
# Logic:
#
#
# Called by:    Unix
#
# Date         Ver#   Modified By(Name)            Change and Reason for Change
#---------    -----  ---------------------------  ------------------------------
# 2011-09-12   1.0   Ryan Wong                    Initial version
# 2013-07-15   1.1   Ryan Wong                    Add format MMDDYYYY
# 2013-10-04   1.2   Ryan Wong                    Redhat changes
#############################################################################################################

export SCRIPTNAME=${0##*/}

if [ $# -ne 2 ]
then
   print "USAGE: $SCRIPTNAME <uow> <format_type>, uow assumed to be format 'YYYYMMDD24hrmmss'" >&2
   exit 4
fi

UOW=$1
FORMAT_TYPE=$2

_yr=$(print $UOW | cut -c1-4)
_mo=$(print $UOW | cut -c5-6)
_dy=$(print $UOW | cut -c7-8)
_hr=$(print $UOW | cut -c9-10)
_mm=$(print $UOW | cut -c11-12)
_ss=$(print $UOW | cut -c13-14)

case $FORMAT_TYPE in
   0) print $UOW
      ;;
   1) print $_yr$_mo$_dy
      ;;
   2) print $_yr-$_mo-$_dy
      ;;
   3) print $_yr-$_mo-$_dy $_hr:$_mm:$_ss
      ;;
   4) print $_mo$_dy$_yr
      ;;
   *) print "Format Type not recognized: $FORMAT_TYPE" >&2
      print "USAGE: $SCRIPTNAME <uow> <format_type>, uow assumed to be format 'YYYYMMDD24hrmmss'" >&2
      exit 4
esac

exit 0
