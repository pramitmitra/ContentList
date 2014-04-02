#!/bin/ksh -eu

################################################################################
# Assumption - contents of $FROM_EXTRACT_VALUE is in the format of 'YYYY-MM-DD 24hh:mm:ss'
# Returns datetime plus 24 hours.

# Revision History:
#
# Name             Date            Description
# ---------------  --------------  ---------------------------------------------------
# ???              ??/??/????      Initial Creation
# Ryan Wong        10/04/2013      Redhat changes
###############################################################################

EXTRACT_DATE=${FROM_EXTRACT_VALUE% *}
EXTRACT_TIME=${FROM_EXTRACT_VALUE#* }
YYYY=${EXTRACT_DATE%%-*}
MON_TMP=${EXTRACT_DATE%-*}
MON=${MON_TMP#*-}
DD=${EXTRACT_DATE##*-}

NEW_EXTRACT_DATE_TMP=$($DW_EXE/add_days ${YYYY}${MON}${DD} 1)

NEW_YYYY=${NEW_EXTRACT_DATE_TMP%????}
NEW_MON_TMP=${NEW_EXTRACT_DATE_TMP%??}
NEW_MON=${NEW_MON_TMP#????}
NEW_DD=${NEW_EXTRACT_DATE_TMP#??????}


print "${NEW_YYYY}-${NEW_MON}-${NEW_DD} ${EXTRACT_TIME}"

exit
