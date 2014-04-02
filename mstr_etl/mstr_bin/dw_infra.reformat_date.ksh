#!/bin/ksh -eu
# Title:        Reformat date
# File Name:    dw_infra.reformat_date.ksh
# Description:  Reformat date assumed incoming format is of format 'YYYYMMDD'
# Developer:    Ryan Wong
# Created on:   2012-02-13
# Location:     $DW_MSTR_BIN
# Logic:
#
#
# Called by:    Unix
#
# Date         Ver#   Modified By(Name)            Change and Reason for Change
#---------    -----  ---------------------------  ------------------------------
# 2012-02-13   1.0   Ryan Wong                    Initial version
# 2013-07-15   1.1   Ryan Wong                    Add format MMDDYYYY
# 2013-10-04   1.2   Ryan Wong                    Redhat changes
#############################################################################################################

export SCRIPTNAME=${0##*/}

if [ $# -ne 2 ]
then
   print "USAGE: $SCRIPTNAME <date> <format_type>, date must be in format 'YYYYMMDD', format_type is a number" >&2
   exit 4
fi

MYDATE=$1
FORMAT_TYPE=$2

_yr=$(print $MYDATE | cut -c1-4)
_mo=$(print $MYDATE | cut -c5-6)
_dy=$(print $MYDATE | cut -c7-8)

case $FORMAT_TYPE in
   0) print $MYDATE
      ;;
   1) print $_yr-$_mo-$_dy
      ;;
   2) print $_mo$_dy$_yr
      ;;
   *) print "Format Type not recognized: $FORMAT_TYPE" >&2
      print "USAGE: $SCRIPTNAME <date> <format_type>, date must be in format 'YYYYMMDD', format_type is a number" >&2
      exit 4
esac

exit 0
