#!/bin/ksh -eu
####################################################################################################
# Title:        SFT Account Setup
# File Name:    dw_infra.sft_account_setup.ksh
# Description:  This script will setup a secure file transfer (sft) account
# Developer:    Ryan Wong
# Created on:
# Location:     $DW_MASTER_BIN
# Logic:        This is only allowed to run by dw_infra or user with passwordless sudo root priviledge
#               There are several items required to setup a new sft service account.
#               Prerequisite:  Service account must alread exist in PET (pet.vip.ebay.com)
#               TODO:
#               (1) Create home directory.  Path for home is different on prod versus non-prod due to shared storage
#               (2) Add account to local group dw_ops
#               (3) Add standard .profile (dw_adm) and .etlenv files to HOME
#               (4) Create GPG keyring on DW_KEYS and key for Allegro
#               (5) Add SSH Key Pair (id_rsa+id_rsa.pub) from Site FTP Host
#               (6) Create sudo<USERNAME> file
#               (7) Create sudoers drop-in file
#               (8) Create userid.sa.logon file
#
# Date         Ver#   Modified By(Name)            Change and Reason for Change
#---------    -----  ---------------------------  ------------------------------
# 2017-02-02  1.1    Ryan Wong                    Initial
# 2017-03-07  1.2    Ryan Wong                    Add step to create sudoers drop-in file
# 2017-03-30  1.3    Ryan Wong                    Add step to add authorized_keys file
# 2018-02-26  1.4    Ryan Wong                    Update Dev home directory is at /home_service
#
####################################################################################################

typeset -fu usage

function usage {
   print "FATAL ERROR: Incorrect Call
  Usage:  $DWI_CALLED [--nousercheck] <SFT_USERID> <SFT_SSH_RSA_FILE> <SFT_SSH_RSA_PUB_FILE>
          Example:  $DWI_CALLED etl_infra_sft dev /home/dw_infra/etl_infra_sft_ssh
    Check 1: This script must run by a priviledged user that has passwordless sudo root priviledge
    Check 2: SFT Userid has been created in PET (pet.vip.ebay.com)
    Check 3: A copy of the SSH private and public rsa files exist
    Check 4: This relies on dw_adm is setup and .etlenv .profile and gpg keyring exist
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
if [[ $# -ne 3 && $# -ne 4 ]]
then
   print "FATAL ERROR: Too few or too many parameters passed" >&2
   usage
   exit 3
fi

# Check if nousercheck flag is set
EXCEPTIONFLAG=0
if [[ $# -eq 4 ]]
then
  if [[ "$1" == "--nousercheck" ]]
  then
    print "No User Check flag is set"
    EXCEPTIONFLAG=1
    shift 1
  else
    print "FATAL ERROR: Four parameters passed.  First parameter may only be flag --nocheckuser" >&2
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
SFT_SSH_RSA_FILE=$2
SFT_SSH_RSA_PUB_FILE=$3

SFT_HOME=~$SFT_USERID
DW_ADM_HOME=~dw_adm

# Print parameters
print "SFT_USERID=$SFT_USERID"
print "SFT_SSH_RSA_FILE=$SFT_SSH_RSA_FILE"
print "SFT_SSH_RSA_PUB_FILE=$SFT_SSH_RSA_PUB_FILE"
print "SFT_HOME=$SFT_HOME"
print "DW_ADM_HOME=$DW_ADM_HOME"

# Validate SFT_SSH_RSA_FILE and SFT_SSH_RSA_PUB_FILE
if [[ -f $SFT_SSH_RSA_FILE ]]
then
  print "$SFT_SSH_RSA_FILE is a valid file"
else
  print "FATAL ERROR: $SFT_SSH_RSA_FILE does not exist, is not a valid file, or read permission issue with $DWI_WHOAMI" >&2
  usage
  exit 6
fi
if [[ -f $SFT_SSH_RSA_PUB_FILE ]]
then
  print "$SFT_SSH_RSA_PUB_FILE is a valid file"
else
  print "FATAL ERROR: $SFT_SSH_RSA_PUB_FILE does not exist, is not a valid file, or read permission issue with $DWI_WHOAMI" >&2
  usage
  exit 6
fi

# Pull SFT_ENV from dw_adm's etlenv file (infra, dev, qa, prod)
if [[ -f $DW_ADM_HOME/.etlenv ]]
then
  SFT_ENV=$(<$DW_ADM_HOME/.etlenv)
  print "Validate existence of $DW_ADM_HOME/.etlenv"
  print "SFT_ENV=$SFT_ENV"
else
  print "FATAL ERROR: SFT_ENV undefined.  Check $DW_ADM_HOME/.etlenv exists, is valid, and can be read by $DWI_WHOAMI" >&2
  exit 7
fi

if [[ -f $DW_ADM_HOME/.profile ]]
then
  print "Validate existence of $DW_ADM_HOME/.profile"
else
  print "FATAL ERROR: Check $DW_ADM_HOME/.profile exists, is valid, and can be read by $DWI_WHOAMI" >&2
  exit 8
fi

print "####################################################################################################"
print "Start setting up account"
print "6 STEPS TOTAL"
print "####################################################################################################"

print "####################################################################################################"
print "(1) Create home directory"
print "####################################################################################################"
if [[ $SFT_ENV == dev || $SFT_ENV == prod ]]
then
  if [[ $SFT_ENV == prod ]]
  then
    SFT_HOME_BASE=data
  elif [[ $SFT_ENV == dev ]]
  then
    SFT_HOME_BASE=home_service
  fi

  if [[ -d /$SFT_HOME_BASE/$SFT_USERID ]]
  then
    print "INFO: Home directory already exists, skipping!!! /$SFT_HOME_BASE/$SFT_USERID"
  else
    set -x
    sudo mkdir -v /$SFT_HOME_BASE/$SFT_USERID
    sudo chown -v $SFT_USERID:$SFT_USERID /$SFT_HOME_BASE/$SFT_USERID
    set +x
  fi
  ####################
  # Make Symlink
  ####################
  if [[ -h $SFT_HOME ]]
  then
    print "INFO: *****$SFT_HOME Symlink Already Exists!!!  Skipping Creation*****"
  elif [[ -e $SFT_HOME ]]
  then
    print "FATAL ERROR: Object $SFT_HOME exists and is not a symlink, please investigate" >&2
    exit 9
  else
    set -x
    sudo ln -vsT /$SFT_HOME_BASE/$SFT_USERID $SFT_HOME
    set +x
  fi
else
  if [[ -d $SFT_HOME ]]
  then
    print "INFO: Home directory already exists, skipping!!! $SFT_HOME"
  else
    set -x
    sudo mkdir -v $SFT_HOME
    sudo chown -v $SFT_USERID:$SFT_USERID $SFT_HOME
    set +x
  fi
fi

print "####################################################################################################"
print "(2) Add account to local group dw_ops"
print "####################################################################################################"
set -x
sudo gpasswd --add $SFT_USERID dw_ops
set +x

print "####################################################################################################"
print "(3) Add standard .profile (dw_adm) and .etlenv files to HOME"
print "####################################################################################################"
set -x
sudo cp -nv $DW_ADM_HOME/.profile $SFT_HOME/.profile
sudo chown -v $SFT_USERID:$SFT_USERID $SFT_HOME/.profile
sudo cp -nv $DW_ADM_HOME/.etlenv $SFT_HOME/.etlenv
sudo chown -v $SFT_USERID:$SFT_USERID $SFT_HOME/.etlenv
set +x

print "####################################################################################################"
print "(4) Create GPG keyring on DW_KEYS, key for Allegro, copy gpg.conf file"
print "####################################################################################################"
GPG_TEMP_OUT=$SFT_HOME/gpg_temp.out
if [[ -d  $DW_KEYS/$SFT_USERID ]]
then
  print "INFO: *****Directory $DW_KEYS/$SFT_USERID Already Exists!!!  Skipping GPG Keyring Setup*****"
else
  set -x
  sudo mkdir --mode=700 -v $DW_KEYS/$SFT_USERID
  sudo chown -vR $SFT_USERID:$SFT_USERID $DW_KEYS/$SFT_USERID
  sudo mkdir --mode=700 -v $DW_KEYS/$SFT_USERID/.gnupg
  sudo chown -vR $SFT_USERID:$SFT_USERID $DW_KEYS/$SFT_USERID/.gnupg
  sudo cp -nv $DW_KEYS/dw_adm/.gnupg/gpg.conf $DW_KEYS/$SFT_USERID/.gnupg/gpg.conf
  sudo chown -vR $SFT_USERID:$SFT_USERID $DW_KEYS/$SFT_USERID/.gnupg/gpg.conf
  sudo gpg --verbose --homedir=$DW_KEYS/dw_adm/.gnupg --output $GPG_TEMP_OUT --armor --export-secret-key Allegro
  sudo -u $SFT_USERID gpg --verbose --homedir=$DW_KEYS/$SFT_USERID/.gnupg --import $GPG_TEMP_OUT
  sudo rm -v $GPG_TEMP_OUT
  set +x
fi

print "####################################################################################################"
print "(5) Add SSH Key Pair (id_rsa+id_rsa.pub) from Site FTP Host"
print "####################################################################################################"
if [[ -d $SFT_HOME/.ssh ]]
then
  print "INFO: *****SSH directory $SFT_HOME/.ssh Already Exists!!!  Skipping SSH Key Setup*****"
else
  set -x
  sudo mkdir --mode=700 -v $SFT_HOME/.ssh
  sudo chown -v $SFT_USERID:$SFT_USERID $SFT_HOME/.ssh
  sudo cp -nv $SFT_SSH_RSA_FILE $SFT_HOME/.ssh/id_rsa
  sudo chmod -v 600 $SFT_HOME/.ssh/id_rsa
  sudo chown -v $SFT_USERID:$SFT_USERID $SFT_HOME/.ssh/id_rsa
  sudo cp -nv $SFT_SSH_RSA_PUB_FILE $SFT_HOME/.ssh/id_rsa.pub
  sudo chmod -v 644 $SFT_HOME/.ssh/id_rsa.pub
  sudo chown -v $SFT_USERID:$SFT_USERID $SFT_HOME/.ssh/id_rsa.pub
  sudo cp -nv $SFT_HOME/.ssh/id_rsa $SFT_HOME/.ssh/"#id_rsa"
  sudo chmod -v 644 $SFT_HOME/.ssh/"#id_rsa"
  sudo cp -nv $SFT_HOME/.ssh/id_rsa.pub $SFT_HOME/.ssh/"#id_rsa.pub"
  sudo chmod -v 644 $SFT_HOME/.ssh/"#id_rsa.pub"
  sudo cp -nv $SFT_HOME/.ssh/id_rsa.pub $SFT_HOME/.ssh/authorized_keys
  sudo chmod -v 600 $SFT_HOME/.ssh/authorized_keys
  sudo chown -v $SFT_USERID:$SFT_USERID $SFT_HOME/.ssh/authorized_keys
  set +x
fi

print "####################################################################################################"
print "(6) Create sudo<USERNAME> file"
print "####################################################################################################"
set -x
printf "#!/bin/sh\n\n/usr/bin/sudo -u $SFT_USERID /usr/bin/sudosh\n\n#--DONE\n" | sudo tee /usr/local/bin/sudo$SFT_USERID > /dev/null
sudo chmod -v 755 /usr/local/bin/sudo$SFT_USERID
set +x

print "####################################################################################################"
#     "(7) Create sudoers drop-in file"
print "####################################################################################################"
set -x
printf "+dwoncall ALL=($SFT_USERID) SUDOSH\n" | sudo tee /etc/sudoers.d/$SFT_USERID > /dev/null
sudo chmod -v 440 /etc/sudoers.d/$SFT_USERID
set +x

print "####################################################################################################"
print "(8) Create <USERNAME>.sa.logon file"
print "####################################################################################################"
SA_LOGON=/dw/etl/home/$SFT_ENV/.logins/$SFT_USERID.sa.logon
if [[ -f  $SA_LOGON ]]
then
  print "INFO: *****File $SA_LOGON Already Exists!!!  Skipping <USERNAME>.sa.logon Setup*****"
else
  set -x
  printf "all 1\n" | sudo tee $SA_LOGON > /dev/null
  sudo chown -v $SFT_USERID:$SFT_USERID $SA_LOGON
  sudo chmod -v 644 $SA_LOGON
  set +x
fi

print "####################################################################################################"
print "!!!SUCCESS!!!"
print "Completed $DWI_CALLED at " $(date '+%Y%m%d-%H%M%S')
print "NOTE: Edits to sudoers, sudoers.d drop-in files, and access.conf  must be performed outside of this script for security"
print "####################################################################################################"

exit 0
