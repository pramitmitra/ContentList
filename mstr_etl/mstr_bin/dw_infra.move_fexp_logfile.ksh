#!/bin/ksh -eu
#------------------------------------------------------------------------------------------------
# Filename:     dw_infra.move_fexp_logfile.ksh
#
# Revision History:
#
# Name             Date            Description
# ---------------  --------------  ---------------------------------------------------
# Ryan Wong        02/26/2014      Initial Creation
#
#------------------------------------------------------------------------------------------------

print "Start FastExport move utility log file"

# Move FEXP_LOGFILE to $DW_SA_LOG
if [ ${FEXP_LOGFILE%/*} = $DW_SA_TMP ]
then
   mv $FEXP_LOGFILE $DW_SA_LOG
   export FEXP_LOGFILE=$DW_SA_LOG/${FEXP_LOGFILE##*/}
fi

print "End FastExport move utility log file"

exit 0
