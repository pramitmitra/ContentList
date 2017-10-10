#------------------------------------------------------------------------------------------------
# Title:        hadoop login
# File Name:    hadoop.login
# Description:  Login into hadoop using kerberos authentication
# Developer:
# Created on:
# Location:     $DW_MASTER_CFG
# Logic:
#
# Revision History:
#
# Name             Date            Description
# ---------------  --------------  ---------------------------------------------------
# Michael Weng     10/13/2016      Initial Creation
# Michael Weng     01/12/2017      Default Kerberos domain and exit on kinit failure
# Michael Weng     09/04/2017      Log HD_USERNAME, HD_QUEUE and HD_DOMAIN
#------------------------------------------------------------------------------------------------


set +e 
if [[ $HADOOP_AUTHENTICATED -ne 1 ]]
then

  # Check if HD_USERNAME has been configured
  if [[ -z $HD_USERNAME ]]
  then
    print "INFRA_ERROR: can't not determine batch account to hadoop cluster"
    exit 4
  fi

  # Determine keytab and kerberos login
  myName=$(whoami)
  myPrincipal=""
  myKeytabFile=""
  myDomain=$HD_DOMAIN

  if [[ -z $myDomain ]] || [[ X$myDomain == X ]]
  then
    myDomain=APD.EBAY.COM
  fi

  if [[ $myName == @(sg_adm|dw_adm) ]]
  then
    myPrincipal=sg_adm@$myDomain
    myKeytabFile=~/.keytabs/apd.sg_adm.keytab
    if ! [[ $HD_USERNAME == sg_adm ]]
    then
      export HADOOP_PROXY_USER=$HD_USERNAME
    fi
  else
    myPrincipal=$HD_USERNAME@$myDomain
    myKeytabFile=~/.keytabs/$HD_USERNAME.keytab
  fi

  if ! [ -f $myKeytabFile ]
  then
    print "INFRA_ERROR: missing keytab file: $myKeytabFile"
    exit 4
  fi

  kinit -k -t $myKeytabFile $myPrincipal

  if [[ $? == 0 ]]
  then
    print "INFRA_INFO: successfully login as $myPrincipal using keytab file: $myKeytabFile"
    print "            hadoop user ($HD_USERNAME), queue ($HD_QUEUE), domain ($HD_DOMAIN)"
  else
    print "INFRA_ERROR: login failed for $myPrincipal using keytab file: $myKeytabFile"
    exit 4
  fi

  HADOOP_AUTHENTICATED=1
  export HADOOP_AUTHENTICATED

fi
set -e
