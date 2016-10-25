#------------------------------------------------------------------------------------------------
# Title:        hadoop login
# File Name:    hadoop.login
# Description:  Login into hadoop using kerberos authentication
# Developer:
# Created on:
# Location:     $DW_MASTER_BIN
# Logic:
#
# Revision History:
#
# Name             Date            Description
# ---------------  --------------  ---------------------------------------------------
# Michael Weng     10/13/2016      Initial Creation
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

  if [[ $myName == @(sg_adm|dw_adm) ]]
  then
    myPrincipal=sg_adm@APD.EBAY.COM
    myKeytabFile=~/.keytabs/apd.sg_adm.keytab
    export HADOOP_PROXY_USER=$HD_USERNAME
  else
    myPrincipal=$HD_USERNAME@CORP.EBAY.COM
    myKeytabFile=~/.keytabs/$HD_USERNAME.keytab
  fi

  if ! [ -f $myKeytabFile ]
  then
    print "INFRA_ERROR: missing keytab file: $myKeytabFile"
    exit 4
  fi

  kinit -k -t $myKeytabFile $myPrincipal

  HADOOP_AUTHENTICATED=1
  export HADOOP_AUTHENTICATED

fi
set -e
