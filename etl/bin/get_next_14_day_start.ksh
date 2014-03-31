#!/bin/ksh
# Filename:     get_next_14_day_start.ksh
# Description:  Provides the day for which the data is to be loaded next.
#               This can be substituted for "get_current_day_start.ksh" in the .cfg
#               to increment by 14 days from the last_extract_value.
#
# Date          Ver#   Modified By           Change and Reason for Change
# ----------    -----  ------------------    -----------------------------------
# 12/12/2007    1.0    Phil Nardi            Initial Script
# 10/04/2013    1.1    Ryan Wong             Redhat changes
# ---------------------------------------------------------------------------------------

last_extract_value_file=${DW_DAT}/${JOB_ENV}/${SUBJECT_AREA}/${TABLE_ID}.${FILE_ID}.last_extract_value.dat

last_extract_value=`cat ${last_extract_value_file}|cut -c1,2,3,4,6,7,9,10`

nday_to_load=` add_days $last_extract_value 14`

day_to_load_mm=`print "${nday_to_load}"|cut -c5-6`
day_to_load_dd=`print "${nday_to_load}"|cut -c7-8`
day_to_load_yyyy=`print "${nday_to_load}"|cut -c1-4`

nday_to_load_mmddyyyy=${day_to_load_yyyy}-${day_to_load_mm}-${day_to_load_dd} 

print "${nday_to_load_mmddyyyy}" 00:00:00 

exit

