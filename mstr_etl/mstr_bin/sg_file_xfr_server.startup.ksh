#!/bin/ksh
#------------------------------------------------------------------------------------------------
# Filename:     sg_file_xfr_server.startup.ksh
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
[[ "$up_down_10" == "SERVICE UP" ]] && print "SERVICE UP ALREADY ON $SFT_SNODE" && exit 102


$DW_MASTER_EXE/sg_file_xfr_server -p $SFT_LCLPORT -l $DW_MASTER_LOG/sg_file_xfr.server.log


sleep 1

up_down=$($DW_MASTER_BIN/sg_file_xfr_server.query.ksh)
up_down_10=$(print $up_down|sed -e 's/^\(.\{10\}\)..*/\1/g')
[[ "$up_down_10" == "SERVICE DO" ]] && print "SERVICE COULD NOT BE STARTED ON $SFT_SNODE" || print "SERVICE STARTED ON $SFT_SNODE"

