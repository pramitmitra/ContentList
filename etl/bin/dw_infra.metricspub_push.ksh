#!/bin/ksh -eu
#------------------------------------------------------------------------------------------------
# Filename:     dw_infra.metricspub_push.ksh
# Description:  Use curl to send the specified file to MetricsPub, as specified at the MetricsPub wiki:
#                        https://wiki.vip.corp.ebay.com/display/DW/Call+Web+Service+to+Upload+Metric+Data
#
# Developer:    John Hackley
# Created on:   April 16, 2013
# Location:     $DW_EXE/
#
# Execution:    $DW_EXE/dw_infra.metricspub_push.ksh <FILENAME> [FILETYPE] [OPERATION] [HEADERTEXT]
#               Typically would be executed as a post-Extract process
#
# Revision History:
#
# Name             Date               Description
# ---------------  --------------     --------------------------------------------------------------
# John Hackley     April 16, 2013     Initial Creation
# Ryan Wong        Oct    5, 2013     Redhat changes
# John Hackley     January 8, 2014    More Redhat changes; curl installation is "normal" on new hosts,
#                                     so previous workarounds aren't necessary (and don't work)
#------------------------------------------------------------------------------------------------

typeset -fu usage

function usage {
   print "Usage:  $0 <FILENAME> [FILETYPE] [OPERATION] [HEADERTEXT]
  NOTE: FILENAME is required and specifies the name of the file to be transferred to MetricsPub.
        FILETYPE is optional; allowable values are csv, xml or json; defaults to csv if omitted.
        OPERATION is optional; allowable values are POST or PUT; defaults to POST if omitted.
        HEADERTEXT is optional; defaults to 'Content-Type:text/xml' if omitted."
}


if [[ $# -lt 1 || $# -gt 4 ]]
then
   usage
   exit 4
fi

MPP_FILENAME=$1
MPP_FILETYPE=${2:-"csv"}
MPP_OPERATION=${3:-"POST"}
MPP_HEADERTEXT=${4:-"Content-Type:text/xml"}

if [[ $MPP_FILETYPE != "csv" && $MPP_FILETYPE != "xml" && $MPP_FILETYPE != "json" ]]
then
   MPP_HEADERTEXT=$MPP_OPERATION
   MPP_OPERATION=$MPP_FILETYPE
   MPP_FILETYPE="csv"
fi

if [[ $MPP_OPERATION != "POST" && $MPP_OPERATION != "PUT" ]]
then
   MPP_HEADERTEXT=$MPP_OPERATION
   MPP_OPERATION="POST"

   if [[ $# -eq 4 ]]
   then
      usage
      exit 4
   fi
fi

CURL_LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.curl${UOW_APPEND}.$CURR_DATETIME.log

print "Calling curl with the following command line arguments:"
print "curl -X $MPP_OPERATION -H '$MPP_HEADERTEXT' -T $MPP_FILENAME http://dataassets.corp.ebay.com/mtrdata/$MPP_FILETYPE"

set +e
curl -X $MPP_OPERATION -H "$MPP_HEADERTEXT" -T $MPP_FILENAME http://dataassets.corp.ebay.com/mtrdata/$MPP_FILETYPE > $CURL_LOG_FILE 2>&1
curlrcode=$?
set -e

print "" >> $CURL_LOG_FILE

# scrape curl log looking for success message, since it can still issue return code 0 for some errors
set +e
grep -s "^Successfully uploaded" $CURL_LOG_FILE >/dev/null
RCODE=$?
set -e


if [[ $curlrcode != 0 || $RCODE == 1 ]]
then
   print "ERROR: Failed to send file to MetricsPub; curl return code=$curlrcode"
   exit 4
else
   print "Successfully sent to MetricsPub"
   exit 0
fi
