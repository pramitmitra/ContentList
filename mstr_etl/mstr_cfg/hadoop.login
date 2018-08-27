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
# Michael Weng     01/18/2018      Special handling on Hercules-sub
# Michael Weng     05/31/2018      Export KRB5CCNAME to isolate kinit session
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

  # Check if HD_CLUSTER is valid
  if [[ -z $HD_CLUSTER ]]
  then
    print "INFRA_ERROR: can't not determine the hadoop cluster"
    exit 4
  fi

  # Determine keytab and kerberos domain
  myDomain=$HD_DOMAIN
  if [[ -z $myDomain ]] || [[ X$myDomain == X ]]
  then
    myDomain=APD.EBAY.COM
  fi

  # Substitue _sub batch user for Hercules-sub
  DW_LOGIN=sg_adm
  HD_LOGIN=$HD_USERNAME
  if [[ $HD_CLUSTER = "herculesqa" ]] && [[ $HD_LOGIN = b_* ]]
  then
    DW_LOGIN=${DW_LOGIN}_sub
    HD_LOGIN=${HD_USERNAME}_sub
  fi

  export KRB5CCNAME=/tmp/krb5cc_${DW_LOGIN}
  myName=$(whoami)
  myPrincipal=$HD_LOGIN@$myDomain
  myKeytabFile=~/.keytabs/$HD_LOGIN.keytab

  if [[ $myName == @(sg_adm|dw_adm|sg_adm_sub) ]]
  then
    myPrincipal=$DW_LOGIN@$myDomain
    myKeytabFile=~/.keytabs/apd.$DW_LOGIN.keytab
    if ! [[ $HD_LOGIN == $DW_LOGIN ]]
    then
      export HADOOP_PROXY_USER=$HD_LOGIN
    fi
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
    print "            hadoop user ($HD_LOGIN), queue ($HD_QUEUE), domain ($HD_DOMAIN)"
  else
    print "INFRA_ERROR: login failed for $myPrincipal using keytab file: $myKeytabFile"
    exit 4
  fi

  HADOOP_AUTHENTICATED=1
  export HADOOP_AUTHENTICATED

fi
set -e
