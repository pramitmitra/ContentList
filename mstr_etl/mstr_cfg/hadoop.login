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
# Michael Weng     08/28/2018      fix KRB5CCNAME when batch account is using sg_adm keytab
# Michael Weng     09/12/2018      Fix UID not defined issue
# Michael Weng     10/10/2018      Enalbe PROD keytab login for RNO Hadoop clusters
#------------------------------------------------------------------------------------------------


set +e 

  # Check if HD_USERNAME has been configured
  if [[ -z $HD_USERNAME ]]
  then
    print "INFRA_ERROR: can't not determine batch account to hadoop cluster"
    exit 4
  fi

  # Accept optional HADOOP ENV parameter as hd* or sp*
  if [[ $# = 1 ]] && [[ -n ${1:-""} ]] && [[ $1 == hd* || $1 == sp* ]]
  then
    JENV_UPPER=$(print $1 | tr [:lower:] [:upper:])
    export HD_CLUSTER=$(eval print \$DW_${JENV_UPPER}_DB)
  fi

  # Check if HD_CLUSTER is valid
  if [[ -z $HD_CLUSTER ]]
  then
    print "INFRA_ERROR: can't not determine the hadoop cluster"
    exit 4
  fi

  # Who is doing the authentication
  IS_BATCH_LOGIN=1
  if [[ $(whoami) == @(sg_adm|dw_adm|sg_adm_sub) ]]
  then
    IS_BATCH_LOGIN=0
  fi

  # Substitue _sub batch user for Hercules-sub
  if [[ $HD_CLUSTER = "herculesqa" ]]
  then
    myKeytabFile=~/.keytabs/apd.sg_adm_sub.keytab
    myPrincipal=sg_adm_sub@APD.EBAY.COM
    export HADOOP_PROXY_USER=${HD_USERNAME}_sub
    export KRB5CCNAME=/tmp/krb5cc_$(whoami)_sg_adm_sub

  # PROD domain for RNO clusters
  elif [[ $HD_CLUSTER = *rno ]]
  then
    myKeytabFile=~/.keytabs/prod.${HD_USERNAME}.keytab
    myPrincipal=${HD_USERNAME}@PROD.EBAY.COM
    export KRB5CCNAME=/tmp/krb5cc_$(whoami)_${HD_USERNAME}_prod
    if [ $IS_BATCH_LOGIN = 0 ]
    then
      myKeytabFile=~/.keytabs/prod.sg_adm.keytab
      myPrincipal=sg_adm@PROD.EBAY.COM
      export HADOOP_PROXY_USER=${HD_USERNAME}
      export KRB5CCNAME=/tmp/krb5cc_$(whoami)_sg_adm_prod
    fi

  # APD domain
  else
    myKeytabFile=~/.keytabs/${HD_USERNAME}.keytab
    myPrincipal=${HD_USERNAME}@${HD_DOMAIN:-APD.EBAY.COM}
    export KRB5CCNAME=/tmp/krb5cc_$(whoami)_${HD_USERNAME}
    if [ $IS_BATCH_LOGIN = 0 ]
    then
      myKeytabFile=~/.keytabs/apd.sg_adm.keytab
      myPrincipal=sg_adm@APD.EBAY.COM
      export HADOOP_PROXY_USER=${HD_USERNAME}
      export KRB5CCNAME=/tmp/krb5cc_$(whoami)_sg_adm
    fi
  fi

  # Make sure keytab file exists
  if ! [ -f $myKeytabFile ]
  then
    print "INFRA_ERROR: missing keytab file: $myKeytabFile"
    exit 4
  fi

  # Do the kinit
  kinit -k -t $myKeytabFile $myPrincipal

  if [[ $? == 0 ]]
  then
    print "INFRA_INFO: successfully login onto $HD_CLUSTER as $myPrincipal using keytab file: $myKeytabFile"
    print "            hadoop user ($HD_USERNAME/_sub), queue ($HD_QUEUE), domain ($HD_DOMAIN)"
  else
    print "INFRA_ERROR: login onto $HD_CLUSTER failed for $myPrincipal using keytab file: $myKeytabFile"
    exit 4
  fi

  # Update the default kerberos cache file for custom scripts relying on that
  export DEFAULT_KRB5CCNAME=/tmp/krb5cc_$(id -u $(whoami))
  cp -p $KRB5CCNAME $DEFAULT_KRB5CCNAME

set -e
