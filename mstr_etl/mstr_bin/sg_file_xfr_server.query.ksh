#!/bin/ksh
#------------------------------------------------------------------------------------------------
# Filename:     sg_file_xfr_server.query.ksh
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

#use SFT_SNODE and SFT_SUSER for node and user on this server.

nr_of_srvs_running=$(ps -aef|grep "./sg_file_xfr_server -p" |grep $SFT_SUSER|egrep -ve "grep|$this"|wc -l)
if (( ! nr_of_srvs_running )); then
    print "SERVICE DOWN ON $SFT_SNODE"
else
    if (( nr_of_srvs_running == 1 )); then
        pid=$(ps -aef|grep "./sg_file_xfr_server -p"|grep $SFT_SUSER|egrep -ve "grep|$this"|awk '{print $2}')
        print "SERVICE UP ON $SFT_SNODE PID=$pid"
    else
        print "MULTIPLE INSTANCES UP ON $SFT_SNODE"
    fi
fi
