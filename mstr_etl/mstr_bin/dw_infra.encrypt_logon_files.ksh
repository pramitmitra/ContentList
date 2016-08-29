#!/bin/ksh -eu
####################################################################################################
# Title:        Encrypt Login Files
# File Name:    dw_infra.encrypt_login_files.ksh
# Description:  This script will encrypt files based upon pattern list given
# Developer:    Ryan Wong
# Created on:
# Location:     $DW_MASTER_BIN
# Logic:        (1) Clone file, owner and permissions to new file (extension .gpg)
#               (2) Encrypt contents of plain text file, and write output to new file from #1
#               (3) Shall protect user by default (override switch) from encrypting the following:
#                   a) hadoop_logins.dat
#                   b) td_logins.dat
#                   c) *.hd.logon or *.sa.logon
#                   d) *.gpg or *.gpg.*
#                   e) *.pem or *.pem.*
#               Should be executed by a user with sudo privileges, such as dw_infra OR (dw_adm, etl_*) with limited ability
#               Requires Allegro gpg key-pair that is trusted
#
# Date         Ver#   Modified By(Name)            Change and Reason for Change
#---------    -----  ---------------------------  ------------------------------
# 2016-06-14  1.1    Ryan Wong                    Initial
# 2016-06-28  1.2    Ryan Wong                    Add symlink check (it is a regular file)
# 2016-06-30  1.3    Ryan Wong                    Add exception for .hd.logon files
# 2016-08-11  1.4    Ryan Wong                    Enhance tool to only work for dw_infra, dw_adm, or etl_* batch accounts
#
####################################################################################################

typeset -fu usage

function usage {
   print "FATAL ERROR: Incorrect Call
  Usage:  $DWI_CALLED [--noexception] <pattern|filename>
    Example patterns are .td.logon, .mysql.logon, _logins.dat, or could be a single file
    --noexception, Allows encrypting protected types:
        hadoop_logins.dat, td_logins.dat, .hd.logon, .gpg, .gpg., .pem, .pem.
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
if [[ $# -ne 1 && $# -ne 2 ]]
then
   usage
   exit 4
fi

# Check if noexception flag is set
if [[ $# -ne 2 ]]
then
  EXCEPTIONFLAG=0
  DWI_LOOPLIST=$@
else
  if [[ "$1" == "--noexception" ]]
  then
    print "Exception flag is set"
    EXCEPTIONFLAG=1
    shift 1
    DWI_LOOPLIST=$@
  else
    print "FATAL ERROR: First parameter may only be flag --noexception"
    usage
    exit 5
  fi
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

# Static variables for this script
DWI_RECIPIENT=Allegro

print "\n\nDEBUG:  Listing DWI_LOOPLIST"
ls -1 $DWI_LOOPLIST
print "\n\n"

# Loop through
for DWI_INPUTFILE in $(ls $DWI_LOOPLIST)
do

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

  # Check for exceptions and skip
  if [[ $EXCEPTIONFLAG -eq 0 ]]
  then
    BASENAME=${DWI_INPUTFILE##*/}
    if [[    "hadoop_logins.dat" == "$BASENAME" || "td_logins.dat" == "$BASENAME" \
          || "${BASENAME%.hd.logon}" != "$BASENAME" || "${BASENAME%.sa.logon}" != "$BASENAME" \
          || "${BASENAME%.gpg}" != "$BASENAME" || "${BASENAME#*.gpg.}" != "$BASENAME" \
          || "${BASENAME%.pem}" != "$BASENAME" || "${BASENAME#*.pem.}" != "$BASENAME" ]]
    then
      print "SKIPPING: Input file ($DWI_INPUTFILE), this is file is an exception: hadoop_logins, td_logins, hd.logon, or pattern *.gpg *.gpg.* *.pem *.pem.*\n"
      continue
    fi
  fi

  # Check output file
  DWI_OUTPUTFILE=$DWI_INPUTFILE.gpg
  if [ -e $DWI_OUTPUTFILE ]
  then
    if [ ! -f $DWI_OUTPUTFILE ]
    then
      print "FATAL ERROR: Output file ($DWI_OUTPUTFILE) exists and is not a regular file\n"
      exit 6
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

  # Single encrypted file process
  # Create new file, set owner and permissions appropriately
  if [[ "$DWI_WHOAMI" == "dw_adm" || "$FIRSTFOURCHAR" == "etl_" ]]
  then
    # These commands shall run as sudo if using dw_infra
    print "Encrypting file $DWI_INPUTFILE"
    set -x
    touch $DWI_OUTPUTFILE
    chown --reference=$DWI_INPUTFILE $DWI_OUTPUTFILE
    chmod --reference=$DWI_INPUTFILE $DWI_OUTPUTFILE
    gpg --batch --yes -r $DWI_RECIPIENT -o $DWI_OUTPUTFILE -e $DWI_INPUTFILE
    RCODE=$?
    set +x

    if [[ $RCODE -ne 0 ]]
    then
      print "FATAL ERROR:  Gpg return code is nonzero, $RCODE"
      exit 8
    fi
    print ""
  elif [[ "$DWI_WHOAMI" == "dw_infra" ]]
  then
    # These commands shall run as sudo if using dw_infra
    print "Encrypting file $DWI_INPUTFILE"
    set -x
    sudo touch $DWI_OUTPUTFILE
    sudo chown --reference=$DWI_INPUTFILE $DWI_OUTPUTFILE
    sudo chmod --reference=$DWI_INPUTFILE $DWI_OUTPUTFILE
    sudo -E gpg --batch --yes -r $DWI_RECIPIENT -o $DWI_OUTPUTFILE -e $DWI_INPUTFILE
    RCODE=$?
    set +x

    if [[ $RCODE -ne 0 ]]
    then
      print "FATAL ERROR:  Gpg return code is nonzero, $RCODE"
      exit 8
    fi
    print ""
  else
    print "FATAL ERROR:  User ($DWI_WHOAMI) is not valid.  Not sure how we got this far"
    exit 9
  fi

done  # END OF WHILE LOOP

print "Completed $DWI_CALLED at " $(date '+%Y%m%d-%H%M%S')

exit 0
