#!/usr/bin/ksh -eu

###################################################################################################################
#
# Title:        DW_INFRA  Make Subject Area Logon
# File Name:    dw_infra.make_sa_logon.ksh
# Description:  Script to populate username and password for SAs by Logon File ID and DB type
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
#
#  Required option tags:
#  s - The Subject Area 
#  i - Logon File ID (0 is default for a Subject Area processing, other IDs are must be specified in ETL_ID level
#      configuration files.
#  t - Database type  - currently supports Teradata. MySql and oracle will be included here for future support
#      but is not presently used in the envs.
#  u - TD User Name that is to be used for the passed Subject Area and Logon File ID.
#  p - TD Password that is to be used for the passed Subject Area and Logon File ID.
#  
#
#  Optional option tags:
#  o - Tag set to overwrite an existing logon file.  If included and set to Y or y, then the process will overwrite
#      a file that already exists for the SA/Logon File ID. Otherwise, the process will fail and indicate the file
#      was not written and provide the option tag info to be included, should they want to overwrite the file.
#
#  Sample usage:
# $DW_MASTER_BIN/infra/dw_infra.make_sa_logon.ksh -s dw_infra_sample -i 1 -t td -u dwi_user -p dwi_coolpswd -o Y
#
###################################################################################################################
 
_type=
_ovrwrite=0

while getopts "s:i#t:u:p:o?" opt
do
case $opt in
   s)   _sa="$OPTARG";;
   i)   _id="$OPTARG";;
   t)   _type="$OPTARG";;
   u)   _usr="$OPTARG";;
   p)   _pswd="$OPTARG";;
   o)   _ovrwrite=1;;
   \?)  print >&2 "${0##*/}:  ERROR, Usage: $0 -s subject_area -i logon_file_id -u td_user_name -p td_password [ -o overwrite_file ]"
   exit 1;;
esac
done
shift $(($OPTIND - 1))

case $_type in
   T|t|TD|td|TERADATA|teradata) _dbtag="td" ;;
   O|o|ORA|ora|ORACLE|oracle) _dbtag="ora" ;;
   MYSQL|mysql) _dbtag="mysql" ;;
   *) echo "${0##*/}:  ERROR, Invalid Database Type. Acceptable values are:  T|t|TD|td|TERADATA|teradata|O|o|ORA|ora|ORACLE|oracle|MYSQL|mysql"
      exit 4;;
esac

LGFILE=$DW_LOGINS/$_sa.$_id.$_dbtag.logon

if (( ! _ovrwrite )); then
   if [[ -f $LGFILE ]]
   then
     echo "${0##*/}:  ERROR, Logon File $LGFILE already exists.  If you want to overwrite, add option -o"
     echo "   Logon File $LGFILE not changed"
     exit 4
   fi
fi
echo $_usr $_pswd > $LGFILE

echo "${0##*/}:  INFO, Logon File $LGFILE created."

exit 0
