#!/bin/ksh
############################################################################################################
#
# Title:        DW_INFRA Update Hadoop Maintenance List
# File Name:    dw_infra.update_hd_maintenance_lis.ksh
# Description:  Script for updating HD_MAINTENANCE_LIS during hadoop system maintenance. 
# Developer:    Michael Weng
# Created on:   2018-10-10
# Location:     $DW_MASTER_EXE
#
# Date         Ver#   Modified By(Name)            Change and Reason for Change
#---------    -----  ---------------------------  ------------------------------
# 2018-10-10   1.0    Michael Weng                 Initial version
#
##############################################################################################################

[[ $(whoami) = dw_adm ]] || { print "Login as dw_adm to execute the script."; exit 1; }

function usage {
  print "Usage: $0 [ -a <JOB_ENV>] [ -r <JOB_ENV> ] [ -c ] [ -d ]" 
  print "    -a to append <JOB_ENV> to the list"
  print "    -r to remove <JOB_ENV> from the list"
  print "    -c to clear the entire list"
  print "    -d to display the current list"
  print "NOTE: No mix of multiple options. Only one option be accepted during each execution."
}

function display {
  print "  HD_MAINTENANCE_LIS=\"$(cat $HD_MAINTENANCE_LIS_FILE)\""
}

. /dw/etl/mstr_cfg/etlenv.setup

if ! [ -f $HD_MAINTENANCE_LIS_FILE ]
then
  > $HD_MAINTENANCE_LIS_FILE
fi

OPTIONS=0
while getopts "a:r:cd" opt
do
  case $opt in
    a ) OPTIONS=$((OPTIONS+1))
        OPT_CMD=$opt
        JOB_ENV=$OPTARG
        ;;

    r ) OPTIONS=$((OPTIONS+1))
        OPT_CMD=$opt
        JOB_ENV=$OPTARG
        ;;

    c ) OPTIONS=$((OPTIONS+1))
        OPT_CMD=$opt
        ;;

    d ) display
        exit 0
        ;;

    \? ) usage
        exit 1
        ;;
  esac
done

if [ $OPTIONS != 1 ]
then
  print "ERROR: invalid command options."
  usage
  exit 1
fi

print "Before change:"
display

case $OPT_CMD in
  a ) print -n " $JOB_ENV" >> $HD_MAINTENANCE_LIS_FILE
      ;;
  r ) sed -i "s/ $JOB_ENV//" $HD_MAINTENANCE_LIS_FILE
      ;;
  c ) > $HD_MAINTENANCE_LIS_FILE
      ;;
  \? ) print "ERROR: invalid command option: $OPT_CMD"
      exit 4
      ;;
esac

print "After change:"
display

if [[ $servername = phx* ]]
then
  REMOTE_HOST=${servername/phx/lvs}
else
  REMOTE_HOST=${servername/lvs/phx}
fi

print "Syncing from $servername to $REMOTE_HOST ..."
scp $HD_MAINTENANCE_LIS_FILE $REMOTE_HOST:$DW_LOGINS
