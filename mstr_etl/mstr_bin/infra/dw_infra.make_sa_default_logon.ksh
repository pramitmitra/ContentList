#!/usr/bin/ksh -eu
###################################################################################################################
#
# Title:        DW_INFRA  Make Subject Area Default Logon
# File Name:    dw_infra.make_sa_default_logon.ksh
# Description:  Script to populate the default username and password for SAs by Logon File ID and DB type
# Developer:    Brian Wenner
# Created on:   2011-02-15
# Location:     $DW_MASTER_EXE/
#
# Date         Ver#   Modified By(Name)            Change and Reason for Change
#---------    -----  ---------------------------  ------------------------------
# 2011-02-15   1.0    Brian Wenner                  Initial Prod Version
###################################################################################################################
#
#  This script is meant to be run by oncall to populate the appropiate username/password into a Subject Areas Logon
#  File. This should be run via the appropriate admin user (typically dw_adm)
#  This script will use the information in the database type logins file that is current in existence in $DW_LOGINS
#  (td_logins.dat, ora_logins.dat). There is no mysql variant, so passing mysql will result in an error being reported.
#
#  Required option tags:
#  s - The Subject Area
#  t - Database type  - currently supports Teradata. MySql and oracle will be included here for future support
#      but is not presently used in the envs.
#
#
#  Optional option tags:
#  n - TNS name for Oracle DB lookup (required for Oracle DB type, not used if not).
#  o - Tag set to overwrite an existing logon file.  If included, then the process will overwrite
#      a file that already exists for the SA/Logon File ID. Otherwise, the process will fail and indicate the file
#      was not written and provide the option tag info to be included, should they want to overwrite the file.
#
#  Sample usage:
# $DW_MASTER_BIN/infra/dw_infra.make_sa_default_logon.ksh -s dw_infra_sample -i 1 -u dwi_user -p dwi_coolpswd -o 
#
###################################################################################################################


_ovrwrite=0
_name=""

while getopts "s:t:o?" opt
do
case $opt in
   s)   _sa="$OPTARG";;
   t)   _type="$OPTARG";;
#   n)   _name="$OPTARG";;
   o)   _ovrwrite=1;;
   \?)  print >&2 "${0##*/}:  ERROR, Usage: $0 -s subject_area -t db_type [ -o overwrite_file ]"
   exit 1;;
esac
done
shift $(($OPTIND - 1))

case $_type in
   T|t|TD|td|TERADATA|teradata) _dbtag="td" ;;
   #O|o|ORA|ora|ORACLE|oracle) _dbtag="ora" ;;
   O|o|ORA|ora|ORACLE|oracle)  echo "${0##*/}:  ERROR, Invalid Database Type  Oracle. Not implemented yet."
                 echo "${0##*/}:     Acceptable values are:  T|t|TD|td|TERADATA|teradata"
      exit 4;;
   MYSQL|mysql)  echo "${0##*/}:  ERROR, Invalid Database Type MySQL. (no $DW_LOGINS/mysql_logins.dat to source from)"
                 echo "${0##*/}:     Acceptable values are:  T|t|TD|td|TERADATA|teradata"
      exit 4;;
   *) echo "${0##*/}:  ERROR, Invalid Database Type. Acceptable values are:  T|t|TD|td|TERADATA|teradata"
      exit 4;;
esac

LGFILE=$DW_LOGINS/$_sa.0.$_dbtag.logon

if (( ! _ovrwrite )); then
   if [[ -f $LGFILE ]]
   then
     echo "${0##*/}:  ERROR, Logon File $LGFILE already exists.  If you want to overwrite, use option -o Y"
     echo "   Logon File $LGFILE not changed"
     exit 4
    fi
fi

function parseTDLogonString
{
        LOGINFO=$1

        typeset -LR TPASS TUSER

        TPASS=${LOGINFO##*,*([  ])}
        TPASS=${TPASS%%*([      ])\;}
        TUSER=${LOGINFO%%*([    ]),*}
        TUSER=${TUSER##*([      ])}
        TUSER=${TUSER##*+([     ])}
        TUSER=${TUSER##*/}
        export TD_USERNAME=${TUSER%%*([         ])}
        export TD_PASSWORD=${TPASS%%*([         ])}

}

#get list of potential dirs..
case $_dbtag in
    td)  LOGININFO=$(DWDB $_sa)
         parseTDLogonString $LOGININFO
         _usr=$TD_USERNAME
         _pswd=$TD_PASSWORD;;
    ora) if [[ -z $_name ]]; then
           echo "${0##*/}:  ERROR, Database type of Oracle passed, but no name tag (-n) populated with tnsname"
           exit 4
         else
           #parse the oracle logons file
           set +e
           grep "^$_name\>" $DW_LOGINS/ora_logins.dat | read _t_name _usr _pswd
           grep_rcode=$?
           if [ $grep_rcode != 0 ]; then
              echo "${0##*/}:  ERROR, Passed name ( -n $_name ) does not exist in $DW_LOGINS/ora_logins.dat"
              exit 4;
           fi
         fi
esac
           
echo $_usr $_pswd > $LGFILE

echo "${0##*/}:  INFO, Logon File $LGFILE created."

exit 0

