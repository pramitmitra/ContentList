#!/bin/ksh
#------------------------------------------------------------------------------------------------
# Filename:     dw_infra.wget_report_refresh.ksh
#
# Revision History:
#
# Name             Date            Description
# ---------------  --------------  ---------------------------------------------------
# ???              ??/??/????      Initial Creation
# Ryan Wong        10/04/2013      Redhat changes
# Ryan Wong        10/23/2014      Changed wget command to not reference full path to binary
# Michael Weng     02/09/2017      Enable SSL Web Proxy Authentication
# Michael Weng     04/19/2017      Trigger code release due to previous rollout failure
#------------------------------------------------------------------------------------------------
 
print "Running dw_infra.wget_report_refresh.ksh for JOB_ENV: $JOB_ENV, JOB_TYPE_ID: $JOB_TYPE_ID  `date`"

 
REPORT_NAME=""                 
REPORT_URL="" 
FAIL_NOTIY_MAIL_ADDRESS=""                   
SUCCESS_NOTIY_MAIL_ADDRESS=""   
FORCE_RUN=0
 
while getopts "n:u:f:s:t:" opt
do
case $opt in
   n)	REPORT_NAME=$OPTARG;; 
   u)	REPORT_URL=$OPTARG;;    
   f)	FAIL_NOTIFY_MAIL=$OPTARG;;
   s)	SUCCESS_NOTIFY_MAIL=$OPTARG;;   
   t)   FORCE_RUN=$OPTARG;;   
   \?)  print >&2 "Usage: $0  valid options [n|u|f|s:t]"
   return 1;;
esac
done
shift $(($OPTIND - 1)) 

if [ "U"$REPORT_URL = "U" ]
 then
   print "ERROR: -u REPORT URL is required"    
   exit 4                                     
fi 

if [ "U"$REPORT_NAME = "U" ]
 then
   print "ERROR: -u REPORT NAME is required"    
   exit 4                                     
fi 


if [ $ETL_ENV = prod ]; then   
   WGET_CMD="$REPORT_URL" ;
elif  [ $FORCE_RUN = 1 ]; then
   WGET_CMD="$REPORT_URL" ;
else
   print "Alter: To refresh the report on non product enviroment, please set FORCE_RUN=1 and verify the report URL is right to refresh"
   exit 4 
fi


UC4_JOB_NAME_CALL=""
if [[ -n $UC4_JOB_NAME ]]
then
  export UC4_JOB_NAME_CALL=" in UC4 job $UC4_JOB_NAME"
fi


 
FAIL_EMAIL_SUBJECT="Request Failed: The Request to Refresh Report $REPORT_NAME Failed"
FAIL_EMAIL_BODY="The Request to refresh report $REPORT_NAME ${UC4_JOB_NAME_CALL} \n$REPORT_URL  was failed. \n \nOncall, please contact SAE to look at this issue! "

SUCCESS_EMAIL_SUBJECT="Request Success: The Request to Refresh Report $REPORT_NAME Responsed"
SUCCESS_EMAIL_BODY="The Request to refresh report $REPORT_NAME ${UC4_JOB_NAME_CALL} get responsed \n $REPORT_URL"


grep "^$SUBJECT_AREA\>" $DW_CFG/subject_area_email_list.dat | read PARAM SAE_EMAIL_GROUP


#FAIL_NOTIFY_MAIL="${FAIL_NOTIFY_MAIL},${SAE_EMAIL_GROUP},DL-eBay-APD-COE-DX-Oncall@corp.ebay.com,jxiong@ebay.com"
#SUCCESS_NOTIFY_MAIL="${SUCCESS_NOTIFY_MAIL},${SAE_EMAIL_GROUP},jxiong@ebay.com"

#FAIL_NOTIFY_MAIL="jxiong@ebay.com,,"
#SUCCESS_NOTIFY_MAIL="jxiong@ebay.com,,"

if [ "U"$FAIL_NOTIFY_MAIL = "U" ]
 then
   FAIL_NOTIFY_MAIL="${SAE_EMAIL_GROUP},DL-eBay-APD-COE-DX-Oncall@corp.ebay.com"
fi 


if [ "U"$FAIL_NOTIFY_MAIL = "U" ]
 then
   SUCCESS_NOTIFY_MAIL="${SAE_EMAIL_GROUP}"
fi 



MSTR_REFRESH_LOG=$DW_SA_LOG/wget_report_refresh.$REPORT_NAME.$CURR_DATETIME.log 


print "Wget command is being executed from $ETL_ENV, see log $MSTR_REFRESH_LOG "  


  
set +e
	$DW_ETL_WGET ${WGET_CMD} > $MSTR_REFRESH_LOG 2>&1;  
	RCODE=$?
set -e
 
if [ $RCODE != 0 ]
then
	 print "wget command get failed"  >> $MSTR_REFRESH_LOG
	 print $FAIL_EMAIL_BODY |mailx -s "$FAIL_EMAIL_SUBJECT"  $FAIL_NOTIFY_MAIL
	 print "send eamil to  $FAIL_NOTIFY_MAIL"  >>    $MSTR_REFRESH_LOG
	 exit 4
else
	 print "wget command get response sucessfully"  >> $MSTR_REFRESH_LOG
	 print $SUCCESS_EMAIL_BODY |mailx -s "$SUCCESS_EMAIL_SUBJECT"  $SUCCESS_NOTIFY_MAIL	 
	 print "send eamil to  $SUCCESS_NOTIFY_MAIL"  >> $MSTR_REFRESH_LOG
	 
	 exit 0	
fi



                                                                   
