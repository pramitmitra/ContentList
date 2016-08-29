#!/bin/ksh -eu
####################################################################################################
# Title:        Decrypt Login Files
# File Name:    dw_infra.decrypt_login_files.ksh
# Description:  This script will decrypt files based upon a pattern list given
# Developer:    Ryan Wong
# Created on:
# Location:     $DW_MASTER_BIN
# Logic:        (1) Clone file, owner and permissions to new file, less .gpg extension
#               (2) Decrypt contents of gpg file, and write output to new file from #1
#               (3) Files must have .gpg extension, and should be encrypted
#               Should be executed by a user with sudo privileges, such as dw_infra OR (dw_adm, etl_*) with limited ability
#               Requires Allegro gpg key-pair that is trusted
#
# Date         Ver#   Modified By(Name)            Change and Reason for Change
#---------    -----  ---------------------------  ------------------------------
# 2016-06-23  1.1    Ryan Wong                    Initial
# 2016-06-28  1.2    Ryan Wong                    Add symlink check (it is a regular file)
# 2016-08-17  1.3    Ryan Wong                    Enhance tool to only work for dw_infra, dw_adm, or etl_* batch accounts
#
####################################################################################################

typeset -fu usage

function usage {
   print "FATAL ERROR: Incorrect Call
  Usage:  $DWI_CALLED <pattern|filename>
    Example patterns are .td.logon.gpg, .hd.logon.gpg, _logins.dat.gpg, or could be a single file
        Files passed should have a .gpg extension, or will fail the tool
    Check 1: Pattern with wild card MUST be double quoted
    Check 2: Environment variable GNUPGHOME must be set before executing
    Check 3: Requires Allegro gpg key-pair that is trusted"
}

. /dw/etl/mstr_cfg/etlenv.setup

export DWI_CALLED=$0
export DWI_CALLED_ARGS=${@:-""}
export DWI_WHOAMI=$(whoami)
export DWI_START_DATETIME=$(date '+%Y%m%d-%H%M%S')
FIRSTFOURCHAR=$(print $DWI_WHOAMI | cut -c1-4)

set +u
print_header
set -u

# Only dw_infra, dw_adm or etl_* batch accounts can use this tool
if [[ "$DWI_WHOAMI" != "dw_infra" && "$DWI_WHOAMI" != "dw_adm" && "$FIRSTFOURCHAR" != "etl_" ]]
then
  print "FATAL ERROR: Trying to execute as user $DWI_WHOAMI.  Only dw_infra, dw_adm, or etl_* batch accounts are allowed to use this tool"
  exit 3
else
  print "User ($DWI_WHOAMI) Validation Successful"
fi


# Argument error checking
if [[ $# -ne 1 ]]
then
   usage
   exit 4
fi

# Check if GNUPGHOME is set
if [[ -z ${GNUPGHOME:-""} ]]
then
  print "FATAL ERROR: Environment variable GNUPGHOME is not set"
  usage
  exit 5
else
  export GNUPGHOME
fi

DWI_LOOPLIST=$@

print "\n\nDEBUG:  Listing DWI_LOOPLIST"
ls -1 $DWI_LOOPLIST
print "\n\n"

# Loop through
for DWI_INPUTFILE in $(ls $DWI_LOOPLIST)
do

  # Check if file has a .gpg extension
  BASENAME=${DWI_INPUTFILE##*/}
  if [[ "${BASENAME%.gpg}" == "$BASENAME" ]]
  then
    print "FATAL ERROR: Input file ($DWI_INPUTFILE), does not have a .gpg extension\n"
    exit 6
  fi

  # If user is dw_adm, etl_, check file is owned by respective user
  if [[ "$DWI_WHOAMI" == "dw_adm" || "$FIRSTFOURCHAR" == "etl_" ]]
  then
    FILEOWNER=`stat -c %U $DWI_INPUTFILE`
    if [[ "$FILEOWNER" != "$DWI_WHOAMI" ]]
    then
      print "SKIPPING:  Input file ($DWI_INPUTFILE) is owned by $FILEOWNER, and not owned by $DWI_WHOAMI\n"
      continue
    fi
  fi

  # Check input file is not a regular file
  if [[ ! -f $DWI_INPUTFILE ]]
  then
    print "SKIPPING: Input file ($DWI_INPUTFILE) does not exist or is not a regular file\n"
    continue
  fi

  # Check input file is a symlink
  if [[ -h $DWI_INPUTFILE ]]
  then
    print "SKIPPING: Input file ($DWI_INPUTFILE) does not exist or is a symlink\n"
    continue
  fi

  # Check output file
  DWI_OUTPUTFILE=${DWI_INPUTFILE%.gpg}
  if [ -e $DWI_OUTPUTFILE ]
  then
    if [ ! -f $DWI_OUTPUTFILE ]
    then
      print "FATAL ERROR: Output file ($DWI_OUTPUTFILE) exists and is not a regular file\n"
      exit 7
    fi

    # If user is dw_adm, etl_, and output file exists, check output file is owned by respective user
    if [[ "$DWI_WHOAMI" == "dw_adm" || "$FIRSTFOURCHAR" == "etl_" ]]
    then
      FILEOWNER=`stat -c %U $DWI_OUTPUTFILE`
      if [[ "$FILEOWNER" != "$DWI_WHOAMI" ]]
      then
        print "FATAL ERROR:  Output file ($DWI_OUTPUTFILE) is owned by $FILEOWNER, and not owned by $DWI_WHOAMI\n"
        exit 7
      fi
    fi
  fi

  # Single decrypted file process
  # Create new file, set owner and permissions appropriately
  if [[ "$DWI_WHOAMI" == "dw_adm" || "$FIRSTFOURCHAR" == "etl_" ]]
  then
    print "Decrypting file $DWI_INPUTFILE"
    set -x
    touch $DWI_OUTPUTFILE
    chown --reference=$DWI_INPUTFILE $DWI_OUTPUTFILE
    chmod --reference=$DWI_INPUTFILE $DWI_OUTPUTFILE
    gpg --batch --yes -d -o $DWI_OUTPUTFILE $DWI_INPUTFILE
    RCODE=$?
    set +x

    if [[ $RCODE -ne 0 ]]
    then
      print "FATAL ERROR:  Gpg return code is nonzero, $RCODE"
      exit 8
    else
      print "SUCCESS: removing input file ($DWI_INPUTFILE)"
      rm -f $DWI_INPUTFILE
    fi
    print ""
  elif [[ "$DWI_WHOAMI" == "dw_infra" ]]
  then
    # These commands shall run as sudo if using dw_infra
    print "Decrypting file $DWI_INPUTFILE"
    set -x
    sudo touch $DWI_OUTPUTFILE
    sudo chown --reference=$DWI_INPUTFILE $DWI_OUTPUTFILE
    sudo chmod --reference=$DWI_INPUTFILE $DWI_OUTPUTFILE
    sudo -E gpg --batch --yes -d -o $DWI_OUTPUTFILE $DWI_INPUTFILE
    RCODE=$?
    set +x

    if [[ $RCODE -ne 0 ]]
    then
      print "FATAL ERROR:  Gpg return code is nonzero, $RCODE"
      exit 8
    else
      print "SUCCESS: removing input file ($DWI_INPUTFILE)"
      sudo rm -f $DWI_INPUTFILE
    fi
    print ""
  else
    print "FATAL ERROR:  User ($DWI_WHOAMI) is not valid.  Not sure how we got this far"
    exit 9
  fi

done  # END OF WHILE LOOP

print "Completed $DWI_CALLED at " $(date '+%Y%m%d-%H%M%S')

exit 0
