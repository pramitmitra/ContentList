#!/bin/ksh
#------------------------------------------------------------------------------------------------
# Filename:     create_extract_touch_file.ksh
#
# Revision History:
#
# Name             Date            Description
# ---------------  --------------  ---------------------------------------------------
# ???              ??/??/????      Initial Creation
# Ryan Wong        10/04/2013      Redhat changes
#
#------------------------------------------------------------------------------------------------
if [[ $# -lt 3 ]]
then
	        print "Usage:  $0 <filename> <extract_dt> <extract_tm> [soj_data_dt]"
			        exit 4
fi

eod_load_filename=$1
extract_dt=$2
extract_tm=$3
soj_data_dt=$4

print $eod_load_filename


extract_touch_file="/dw/etl/home/prod/watch/td3/dly_extract_"$soj_data_dt"_"$extract_dt"_"$extract_tm.complete

print $extract_touch_file

if [[ -s $eod_load_filename ]]
then 
print "Creating extract EOD touch file"
touch $extract_touch_file
fi


