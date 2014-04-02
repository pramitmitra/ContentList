#!/bin/ksh
#------------------------------------------------------------------------------------------------
# Filename:     sg_file_xfr_server.shutdown.ksh
#
# Revision History:
#
# Name             Date            Description
# ---------------  --------------  ---------------------------------------------------
# ???              ??/??/????      Initial Creation
# Ryan Wong        10/04/2013      Redhat changes
#
#------------------------------------------------------------------------------------------------

# Strip path
typeset this=${0##*/}

. /dw/etl/mstr_cfg/etlenv.setup

up_down=$($DW_MASTER_BIN/sg_file_xfr_server.query.ksh)
up_down_10=$(print $up_down|sed -e 's/^\(.\{10\}\)..*/\1/g')
[[ "$up_down_10" == "SERVICE DO" ]] && print "SERVICE DOWN ALREADY ON $SFT_SNODE" && exit 101

[[ "$up_down" == "MULTIPLE INSTANCES UP ON $SFT_SNODE" ]] && print "$up_down. YOU NEED TO STOP THEM MANUALLY." && exit 103

pid=$(print $up_down|sed -e 's/^..*PID=//g')

((pid)) && kill -9 $pid || print "NO PID RECEIVED ON $SFT_SNODE"


up_down=$($DW_MASTER_BIN/sg_file_xfr_server.query.ksh)
up_down_10=$(print $up_down|sed -e 's/^\(.\{10\}\)..*/\1/g')
[[ "$up_down_10" == "SERVICE UP" ]] && print "SERVICE COULD NOT BE STOPPED ON $SFT_SNODE" || print "SERVICE STOPPED ON $SFT_SNODE"
