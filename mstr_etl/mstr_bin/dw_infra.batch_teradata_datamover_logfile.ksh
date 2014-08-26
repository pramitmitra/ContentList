#!/bin/ksh -eu
#------------------------------------------------------------------------------------------------
# Filename:     dw_infra.batch_teradata_datamover_logfile.ksh
#
# Revision History:
#
# Name             Date            Description
# ---------------  --------------  ---------------------------------------------------
# Ryan Wong        08/19/2014      Initial Creation
#
#------------------------------------------------------------------------------------------------

print "Start Batch Teradata Datamover move utility log file"

# Move Logfile from tmp to log
if [[ $USE_TPT_EXTRACT -ne 0 ]]
then
   if [[ -d $DW_SA_LOG ]]
   then
      mv ${UTILITY_INTERFACE_EXTRACT_LOG_FILE%.*.log}.* $DW_SA_LOG
   else
      mv ${UTILITY_INTERFACE_EXTRACT_LOG_FILE%.*.log}.* $DW_LOG
   fi
elif [[ $TPT_APPEND || $TPT_TRUNCATE_INSERT -ne 0 ]]
then
   if [[ -d $DW_SA_LOG ]]
   then
      mv ${UTILITY_INTERFACE_LOAD_LOG_FILE%.*.log}.* $DW_SA_LOG
   else
      mv ${UTILITY_INTERFACE_LOAD_LOG_FILE%.*.log}.* $DW_LOG
   fi
fi

print "End Batch Teradata Datamover move utility log file"

exit 0
