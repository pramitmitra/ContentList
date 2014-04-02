#!/bin/ksh -eu

################################################################################
# Assumption - contents of $FROM_EXTRACT_VALUE is in the format of 'YYYY-MM-DD 24hh:mm:ss'
# Returns datetime plus 12 hours.
#
# Revision History:
#
# Name             Date            Description
# ---------------  --------------  ---------------------------------------------------
# ???              ??/??/????      Initial Creation
# Ryan Wong        10/04/2013      Redhat changes
###############################################################################

# FROM_EXTRACT_VALUE="2005-12-07 11:05:13"

EXTRACT_YEAR=$(print $FROM_EXTRACT_VALUE | cut -c1-4)
EXTRACT_MONTH=$(print $FROM_EXTRACT_VALUE | cut -c6-7)
EXTRACT_DAY=$(print $FROM_EXTRACT_VALUE | cut -c9-10)

EXTRACT_HOURS=$(print $FROM_EXTRACT_VALUE | cut -c12-13)
EXTRACT_DAYS=$(print $FROM_EXTRACT_VALUE | cut -c15-16)
EXTRACT_MINUTES=$(print $FROM_EXTRACT_VALUE | cut -c18-19)

((EXTRACT_HOURS+=12))

if [ $EXTRACT_HOURS -ge 24 ]
then
	((EXTRACT_HOURS-=24))
	NEW_DATE=$($DW_EXE/add_days $EXTRACT_YEAR$EXTRACT_MONTH$EXTRACT_DAY 1)
	EXTRACT_YEAR=$(print $NEW_DATE | cut -c1-4)
	EXTRACT_MONTH=$(print $NEW_DATE | cut -c5-6)
	EXTRACT_DAY=$(print $NEW_DATE | cut -c7-8)
fi

printf "%s-%s-%s %02d:%s:%s\n" $EXTRACT_YEAR $EXTRACT_MONTH $EXTRACT_DAY $EXTRACT_HOURS $EXTRACT_DAYS $EXTRACT_MINUTES

exit
