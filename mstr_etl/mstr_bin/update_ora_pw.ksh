#!/usr/bin/ksh -eu

#
# Description:  This script is for changing one or more Oracle passwords (and Oracle accounts, optionally) in $DW_LOGINS/ora_logins.dat.  It
#               can be run on any Tempo server after sudoing as dw_infra.
#
#               K N O W N   L I M I T A T I O N S
#               K N O W N   L I M I T A T I O N S
#               K N O W N   L I M I T A T I O N S 
#
#               1. Certain special characters in passwords (e.g., "$") would probably not work as expected and should be tested thoroughly
#
#
# Developer:    John Hackley
# Location:     $DW_MASTER_BIN/update_ora_pw.ksh on all Tempo hosts
#
# Called by:    Unix command prompt
#
# Date           Ver#   Modified By(Name)            Change and Reason for Change
# ------------   -----  ---------------------------  ------------------------------
# Mar 27, 2015   1      John Hackley                 New script
# Apr 5, 2017    2      John Hackley                 Updated to comprehend encryption of ora_logins.dat


function usage {
  print ""
  print 'This script will apply a new password (and optionally a new generic account) to $DW_LOGINS/ora_logins.dat.gpg on the specified ETL server(s)'
  print ""
  print "Note that  A L L   p a r a m e t e r s   a r e   c A s E - s E n S i T i V e  except for <HOST_LIST>"
  print ""
  print "Usage:  update_ora_pw.ksh <OLD_ORA_ACCT> <OLD_PASS> <NEW_PASS> [-a <NEW_ORA_ACCT>] [-t <TNS_NAME>] [-h <HOST_LIST>]"
  print ""
  print "       Where:"
  print "         <OLD_ORA_ACCT> is the Oracle account whose password you want to change"
  print "         <OLD_PASS> is the existing Oracle password you want to change"
  print "         <NEW_PASS> is the new Oracle password you want to change to"
  print "         <NEW_ORA_ACCT> is the new generic Oracle account you want change to; if omitted, will not change the Oracle account"
  print "         <TNS_NAME> is the TNS Name for the Oracle instance where the account exists; if omitted, will change password regardless of TNS Name"
  print '         <HOST_LIST> is a pipe-delimited list of ETL servers; if omitted, will apply the change to all hosts listed in $DW_MASTER_CFG/ora_pw_hosts.lis'
  print ""
}

. /dw/etl/mstr_cfg/etlenv.setup

if [[ $# -eq 3 || $# -eq 5 || $# -eq 7 || $# -eq 9 ]]
then
  PARMS="OK"
else
  usage
  exit 4
fi

OLD_ORA_ACCT=$1
OLD_ORA_PASS=$2
NEW_ORA_PASS=$3
TNS_NAME="none"
NEW_ORA_ACCT="none"
HOST_LIST="none"

if [[ $# -ge 5 ]]
then
  case $4 in
    -t) TNS_NAME=$5;;
    -a) NEW_ORA_ACCT=$5;;
    -h) HOST_LIST=$5;;
     *)  usage && exit 4;;
  esac
fi

if [[ $# -ge 7 ]]
then
  case $6 in
    -t) TNS_NAME=$7;;
    -a) NEW_ORA_ACCT=$7;;
    -h) HOST_LIST=$7;;
     *)  usage && exit 4;;
  esac
fi

if [[ $# -eq 9 ]]
then
  case $8 in
    -t) TNS_NAME=$9;;
    -a) NEW_ORA_ACCT=$9;;
    -h) HOST_LIST=$9;;
     *)  usage && exit 4;;
  esac
fi

CURR_TS=$(date '+%Y%m%d_%H%M%S')
CURR_TS=${CURR_TS##* }
CURR_DT=${CURR_TS%%_*}

if [ "$HOST_LIST" == "none" ]
then
  cp $DW_MASTER_CFG/ora_pw_hosts.lis $DW_MASTER_CFG/ora_pw_hosts.lis.tmp
else
  echo $HOST_LIST | sed -e "s/|/\n/g" > $DW_MASTER_CFG/ora_pw_hosts.lis.tmp
fi

print "update_ora_pw.ksh called at $CURR_TS; parameters used:  old acct=$OLD_ORA_ACCT; old pass=$OLD_ORA_PASS; new pass=$NEW_ORA_PASS; new acct=$NEW_ORA_ACCT; tns name=$TNS_NAME; hosts=$HOST_LIST"
print "update_ora_pw.ksh called at $CURR_TS; parameters used:  old acct=$OLD_ORA_ACCT; old pass=$OLD_ORA_PASS; new pass=$NEW_ORA_PASS; new acct=$NEW_ORA_ACCT; tns name=$TNS_NAME; hosts=$HOST_LIST" >> $DW_LOGINS/pw_chg_logs/oracle_pw_update.$CURR_TS.log

if [ "$NEW_ORA_ACCT" == "none" ]
then
  NEW_ORA_ACCT=$OLD_ORA_ACCT
fi

if [ "$TNS_NAME" == "none" ]
then
  TNS_NAME=""
  TNS_NAME_CARET=""
else
  TNS_NAME_CARET="^$TNS_NAME"
fi


# Input parameters are all taken care of -- let's get cracking


for TGT_HOST in $(cat $DW_MASTER_CFG/ora_pw_hosts.lis.tmp)
do

  case $TGT_HOST in
    phxdpelega001.phx.ebay.com ) TGT_ENV=infra;;
    lvsdpelega001.lvs.ebay.com ) TGT_ENV=infra;;
    phxdpelega002.phx.ebay.com ) TGT_ENV=infra;;
    lvsdpelega002.lvs.ebay.com ) TGT_ENV=infra;;

    phxdpelega003.phx.ebay.com ) TGT_ENV=dev;;
    lvsdpelega003.lvs.ebay.com ) TGT_ENV=dev;;
    phxdpelega004.phx.ebay.com ) TGT_ENV=dev;;
    lvsdpelega004.lvs.ebay.com ) TGT_ENV=dev;;
    phxdpelega005.phx.ebay.com ) TGT_ENV=dev;;
    lvsdpelega005.lvs.ebay.com ) TGT_ENV=dev;;
    phxdpelega006.phx.ebay.com ) TGT_ENV=dev;;
    lvsdpelega006.lvs.ebay.com ) TGT_ENV=dev;;

    phxdpelega007.phx.ebay.com ) TGT_ENV=qa;;
    lvsdpelega007.lvs.ebay.com ) TGT_ENV=qa;;
    phxdpelega008.phx.ebay.com ) TGT_ENV=qa;;
    lvsdpelega008.lvs.ebay.com ) TGT_ENV=qa;;
    phxdpelega009.phx.ebay.com ) TGT_ENV=qa;;
    lvsdpelega009.lvs.ebay.com ) TGT_ENV=qa;;

                             * ) TGT_ENV=prod;;
  esac

  REMOTE_USER="dw_infra"


# N o t e   t h a t   t h e   s q u a r e   b r a c k e t s   b e l o w   e n c l o s e   a   s p a c e   f o l l o w e d   b y   a   t a b ,   n o t   s e v e r a l   s p a c e s !

  print ""
  print "Attempting to execute the following on $TGT_HOST as $REMOTE_USER:"
  print "sed -i.$CURR_TS \"s~$TNS_NAME_CARET[ 	][ 	]*$OLD_ORA_ACCT[ 	][ 	]*$OLD_ORA_PASS[ 	]*$~$TNS_NAME  $NEW_ORA_ACCT  $NEW_ORA_PASS~g\" $DW_LOGINS/ora_logins.dat"
  print "" >> $DW_LOGINS/pw_chg_logs/oracle_pw_update.$CURR_TS.log
  print "Attempting to execute the following on $TGT_HOST as $REMOTE_USER:" >> $DW_LOGINS/pw_chg_logs/oracle_pw_update.$CURR_TS.log
  print "sed -i.$CURR_TS \"s~$TNS_NAME_CARET[ 	][ 	]*$OLD_ORA_ACCT[ 	][ 	]*$OLD_ORA_PASS[ 	]*$~$TNS_NAME  $NEW_ORA_ACCT  $NEW_ORA_PASS~g\" $DW_LOGINS/ora_logins.dat" >> $DW_LOGINS/pw_chg_logs/oracle_pw_update.$CURR_TS.log

# ssh call is embedded in a retry loop due to frequent time-outs
  i=0
  while  [ $i -le 4 ]
  do
#
#   1. Decrypt ora_logins.dat.gpg into ora_logins.dat.tmp
#   2. Use sed to globally find and replace password, creating ora_logins.tmp.new
#   3. Make a backup of ora_logins.dat.gpg
#   4. Encrypt ora_logins.tmp.new into ora_logins.dat.gpg
#   5. Change permissions and ownership of ora_logins.dat.gpg
#   6. Remove temporary files
#
    set +e
    ssh -n $REMOTE_USER@$TGT_HOST ". /dw/etl/mstr_cfg/etlenv.setup && \
           gpg -d /dw/etl/home/$TGT_ENV/.logins/ora_logins.dat.gpg > /dw/etl/home/$TGT_ENV/.logins/ora_logins.dat.tmp && \
           sed \"s~$TNS_NAME_CARET[ 	][ 	]*$OLD_ORA_ACCT[ 	][ 	]*$OLD_ORA_PASS[ 	]*$~$TNS_NAME  $NEW_ORA_ACCT  $NEW_ORA_PASS~g\" \
           /dw/etl/home/$TGT_ENV/.logins/ora_logins.dat.tmp > /dw/etl/home/$TGT_ENV/.logins/ora_logins.tmp.new && \
           cp -p /dw/etl/home/$TGT_ENV/.logins/ora_logins.dat.gpg /dw/etl/home/$TGT_ENV/.logins/ora_logins.dat.gpg.$CURR_TS && \
           sudo chown dw_adm /dw/etl/home/$TGT_ENV/.logins/ora_logins.dat.gpg.$CURR_TS && \
           gpg --yes -r Allegro -o /dw/etl/home/$TGT_ENV/.logins/ora_logins.dat.gpg -e /dw/etl/home/$TGT_ENV/.logins/ora_logins.tmp.new && \
           sudo chmod 664 /dw/etl/home/$TGT_ENV/.logins/ora_logins.dat.gpg && sudo chown dw_adm /dw/etl/home/$TGT_ENV/.logins/ora_logins.dat.gpg && \
           rm /dw/etl/home/$TGT_ENV/.logins/ora_logins.dat.tmp && rm /dw/etl/home/$TGT_ENV/.logins/ora_logins.tmp.new"

    ssh_rcode=$?
    set -e

#   retry ssh if previous attempt timed out
    if [ $ssh_rcode -ge 16 ]
    then
      print "Retrying connection to $TGT_HOST" >> $DW_LOGINS/pw_chg_logs/oracle_pw_update.$CURR_TS.log 
      print "Retrying connection to $TGT_HOST"
      sleep 10
      i=i+1
    else
#     ssh succeeded, so break out of retry loop
      i=999
    fi
  done

  if [ $ssh_rcode -ge 16 ]
  then
    print "Exceeded 4 retry attempts; unable to connect to $TGT_HOST" >> $DW_LOGINS/pw_chg_logs/oracle_pw_update.$CURR_TS.log
    print "Exceeded 4 retry attempts; unable to connect to $TGT_HOST"
  fi

done


rm $DW_MASTER_CFG/ora_pw_hosts.lis.tmp

exit 0
