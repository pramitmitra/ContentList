#!/bin/ksh -eu
####################################################################################################
# Title:        SFT Account Test
# File Name:    dw_infra.sft_account_test.ksh
# Description:  This script will test the scp connection to Site File Transfer Hosts
# Developer:    Ryan Wong
# Created on:
# Location:     $DW_MASTER_BIN
# Logic:        This is only allowed to run by dw_infra or user with passwordless sudo root priviledge
#
# Date         Ver#   Modified By(Name)            Change and Reason for Change
#---------    -----  ---------------------------  ------------------------------
# 2017-01-31  1.1    Ryan Wong                    Initial
#
####################################################################################################

####################################################################################################
# Global Script Constants
####################################################################################################
SFT_PORT=10022
SFT_HOST_PHX=eaz2siteft.vip.phx.ebay.com
SFT_HOST_LVS=eaz2siteft.vip.lvs.ebay.com
####################################################################################################
####################################################################################################

typeset -fu usage

function usage {
   print "FATAL ERROR: Incorrect Call
  Usage:  $DWI_CALLED [--nousercheck] <SFT_USERID>
          Example:  $DWI_CALLED etl_infra_sft
    Check 1: This script must run by a priviledged user that has passwordless sudo root priviledges
"
}

. /dw/etl/mstr_cfg/etlenv.setup

set -e

export DWI_CALLED=$0
export DWI_CALLED_ARGS=${@:-""}
export DWI_WHOAMI=$(whoami)
export DWI_START_DATETIME=$(date '+%Y%m%d-%H%M%S')

set +u
print_header
set -u

# Argument error checking
if [[ $# -ne 1 && $# -ne 2 ]]
then
   print "FATAL ERROR: Too few or too many parameters passed" >&2
   usage
   exit 3
fi

# Check if nousercheck flag is set
EXCEPTIONFLAG=0
if [[ $# -eq 2 ]]
then
  if [[ "$1" == "--nousercheck" ]]
  then
    print "No User Check flag is set"
    EXCEPTIONFLAG=1
    shift 1
  else
    print "FATAL ERROR: Two parameters passed.  First parameter may only be flag --nocheckuser" >&2
    usage
    exit 4
  fi
fi

# Default only dw_infra batch account may use this tool
if [[ $EXCEPTIONFLAG -eq 0 ]]
then
  if [[ "$DWI_WHOAMI" != "dw_infra" ]]
  then
    print "FATAL ERROR: Trying to execute as user $DWI_WHOAMI.  Only dw_infra batch account may use this tool" >&2
    usage
    exit 5
  else
    print "User $DWI_WHOAMI Validation Successful"
  fi
else
  print "Exception flag is set.  Skipping user id check"
fi

SFT_USERID=$1

# Print parameters
print "SFT_USERID=$SFT_USERID"

print "####################################################################################################"
print "Test transferring a file"
print "4 STEPS TOTAL"
print "####################################################################################################"

# Test file variables
SFT_HOME=~$SFT_USERID
SFT_TESTFILE_NAME=sft_scp_testfile.dat
SFT_TESTFILE=$SFT_HOME/$SFT_TESTFILE_NAME
SFT_TESTFILE_PHX=$SFT_HOME/sft_scp_testfile_phx.dat
SFT_TESTFILE_LVS=$SFT_HOME/sft_scp_testfile_lvs.dat

print "####################################################################################################"
print "(1) Create the test file (local)"
print "####################################################################################################"
set -x
print "$servername" $(date '+%Y%m%d-%H%M%S') | sudo -u $SFT_USERID tee $SFT_TESTFILE > /dev/null
set +x

print "####################################################################################################"
print "(2) Transfer the file (scp push) from EAZ to SFTH"
print "####################################################################################################"
set -x
sudo -u $SFT_USERID scp -qp -o StrictHostKeyChecking=no -P $SFT_PORT $SFT_TESTFILE $SFT_HOST_PHX:
sudo -u $SFT_USERID scp -qp -o StrictHostKeyChecking=no -P $SFT_PORT $SFT_TESTFILE $SFT_HOST_LVS:
set +x

print "####################################################################################################"
print "(3) Transfer the file (scp pull) from SFTH to EAZ"
print "####################################################################################################"
set -x
sudo -u $SFT_USERID scp -qp -P $SFT_PORT $SFT_HOST_PHX:$SFT_TESTFILE_NAME $SFT_TESTFILE_PHX
sudo -u $SFT_USERID scp -qp -P $SFT_PORT $SFT_HOST_LVS:$SFT_TESTFILE_NAME $SFT_TESTFILE_LVS
set +x

print "####################################################################################################"
print "(4) Clean up the local and remote test files"
print "####################################################################################################"
set -x
sudo -u $SFT_USERID rm -v $SFT_TESTFILE
sudo -u $SFT_USERID rm -v $SFT_TESTFILE_PHX
sudo -u $SFT_USERID rm -v $SFT_TESTFILE_LVS
sudo -u $SFT_USERID ssh -q -p $SFT_PORT $SFT_HOST_PHX "rm -v $SFT_TESTFILE_NAME"
sudo -u $SFT_USERID ssh -q -p $SFT_PORT $SFT_HOST_LVS "rm -v $SFT_TESTFILE_NAME"
set +x

print "####################################################################################################"
print "!!!SUCCESS!!!"
print "Completed $DWI_CALLED at " $(date '+%Y%m%d-%H%M%S')
print "####################################################################################################"

exit 0
