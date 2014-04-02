#!/bin/ksh -eu
###################################################################################################################
#
# Title:        SFT Check Restart Run
# File Name:    sft_check_restart_run.ksh
# Description:  Run for Check Restart Socpart Server
# Developer:    Ryan Wong
# Created on:   2011-10-12
# Location:     $DW_MASTER_BIN
#
# Revision History
#
#  Date         Ver#   Modified By(Name)            Change and Reason for Change
# ----------    -----  ---------------------------  ----------------------------------------------------------
# 2011-10-12     1.0   Ryan Wong                    Initial Version
# 2013-10-04     1.1   Ryan Wong                    Redhat changes
#
###################################################################################################################

. $DW_MASTER_LIB/dw_etl_common_functions.lib

#-------------------------------------------------------------------------------------
# Source the error message handling logic.  On failure, trap will send the contents
# of the PARENT_ERROR_FILE to the subject area designated email addresses.
#-------------------------------------------------------------------------------------
. $DW_LIB/message_handler

# Print standard environment variables
set +u
print_standard_env
set -u

print "
##########################################################################################################
#
# Beginning SFT Check Restart `date`
#
##########################################################################################################
"

print "Check for core file:  $DW_MASTER_BIN/core"
if [[ -f $DW_MASTER_BIN/core ]]
then
   print "Core file found"
   print "Attempting to stop Socparc Server"
   set +e
   $DW_MASTER_BIN/sg_file_xfr_server.shutdown.ksh
   RCODE=$?
   set -e

   if [[ $RCODE -ne 101 && $RCODE -ne 0 ]]
   then
      print "FATAL ERROR: Could not stop Socparc Server" `date` >&2
      exit 5
   fi

   print "Success - Stopped Socparc Server"
   print "Remove core file"
   rm -f $DW_MASTER_BIN/core

   print "Attempting to restart Socparc Server"
   set +e
   $DW_MASTER_BIN/sg_file_xfr_server.startup.ksh
   RCODE=$?
   set -e

   if [[ $RCODE -ne 0 ]]
   then
      print "FATAL ERROR: Could not restart Socparc Server" `date` >&2
      exit 6
   fi

   # Send email notification
   print "Success - Start Socparc Server"
   print "Sending email notification to $EMAIL_ERR_GROUP"
   email_subject="$servername: INFO: Restarting Socparc Server due to core file"
   email_body="The associated Parent Log File is: $PARENT_LOG_FILE"
   grep "^dw_infra\>" $DW_CFG/subject_area_email_list.dat | read PARAM EMAIL_ERR_GROUP
   print $email_body | mailx -s "$email_subject" $EMAIL_ERR_GROUP
else
   print "NO core file found $DW_MASTER_BIN/core"
   print "Command running: $DW_MASTER_BIN/sg_file_xfr_server.startup.ksh"
   set +e
   $DW_MASTER_BIN/sg_file_xfr_server.startup.ksh
   RCODE=$?
   set -e

   if [[ $RCODE -ne 102 && $RCODE -ne 0 ]]
   then
      print "FATAL ERROR: Could not restart Socparc Server" `date` >&2
      exit 7
   fi
fi

print "
##########################################################################################################
#
# Finishing SFT Check Restart `date`
#
##########################################################################################################"

tcode=0
exit 0
