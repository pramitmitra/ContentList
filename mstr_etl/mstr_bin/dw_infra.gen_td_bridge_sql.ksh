#!/bin/ksh -eu
# Title:        Single TD Bridge Run
# File Name:    dw_infra.gen_td_bridge_sql.ksh
# Description:  This script use to generate TD-Bridge SQL
# Developer:    George Xiong
# Created on:
# Location:     $DW_MASTER_BIN
# Logic:
#
# Date         Ver#   Modified By(Name)            Change and Reason for Change
#---------    -----  ---------------------------  ------------------------------
# 2013-05-23  1.1    George Xiong                    Initial
# 2013-09-08  1.2    George Xiong                    add env for artemis
# 2013-10-04  1.3    Ryan Wong                       Redhat changes
# 2013-05-30  1.4    George Xiong                    Add cfg option to turn off braceexpand and glob - NO_BRACEEXPAND_NO_GLOB(default=1)
# 2015-07-07  1.5    Ryan Wong                       Add td6 and td7 to JOB_ENV check
# 2015-08-26  1.6    Ryan Wong                       Add mapping for hopbatch and hopbe to hopper
# 2016-04-21  1.7    Michael Weng                    Check on hd* instead of specific hd1/hd2/hd3
####################################################################################################


ETL_ID=$1
JOB_ENV=$2


TABLE_ID=${ETL_ID##*.}

. /dw/etl/mstr_cfg/etlenv.setup
. $DW_MASTER_CFG/dw_etl_common_defs.cfg

# Option to turn off braceexpand and turn off glob
dwi_assignTagValue -p NO_BRACEEXPAND_NO_GLOB -t NO_BRACEEXPAND_NO_GLOB -f $ETL_CFG_FILE -s N -d 1

if [[ $NO_BRACEEXPAND_NO_GLOB -eq 1 ]]
  then
    set +o braceexpand
    set +o glob
fi


assignTagValue DATABASE_NAME DM_BRIDGE_TD_DATABASE  $ETL_CFG_FILE    > /dev/null 2>&1
assignTagValue TABLE_NAME DM_BRIDGE_TD_TABLE  $ETL_CFG_FILE      > /dev/null 2>&1

set +e

grep DW_BRIDGE_HD_DATAPATH $ETL_CFG_FILE|read A DATAPATH_CFG
eval print $DATAPATH_CFG |read DATAPATH_CFG

set -e

DATAPATH=${DATAPATH:-$DATAPATH_CFG}


assignTagValue SOCK_TIMEOUT DW_BRIDGE_SOCK_TIMEOUT  $ETL_CFG_FILE "W" 120  > /dev/null 2>&1

assignTagValue ARVO_SCHEMA_NAME DW_BRIDGE_ARVO_SCHEMANAME  $ETL_CFG_FILE   "W" > /dev/null 2>&1
assignTagValue ARVO_SCHEMA_NAMESPACE DW_BRIDGE_ARVO_SCHEMA_NAMESPACE  $ETL_CFG_FILE "W"    > /dev/null 2>&1

assignTagValue DELIMITERCHARACTER DW_BRIDGE_DELIMITERCHARACTER  $ETL_CFG_FILE "W"  > /dev/null 2>&1
assignTagValue DELIMITERCHARACTERCODE DW_BRIDGE_DELIMITERCHARACTERCODE  $ETL_CFG_FILE "W"  > /dev/null 2>&1

assignTagValue EXPORT_DESTINATION DW_BRIDGE_EXPORT_DESTINATION  $ETL_CFG_FILE  "W"  > /dev/null 2>&1

assignTagValue NULLCHARACTER DW_BRIDGE_NULLCHARACTER  $ETL_CFG_FILE "W"  > /dev/null 2>&1
assignTagValue NULLCHARACTERCODE DW_BRIDGE_NULLCHARACTERCODE  $ETL_CFG_FILE "W"  > /dev/null 2>&1

assignTagValue SEQUENCE_FILE_COMPRESSION DW_BRIDGE_SEQUENCE_FILE_COMPRESSION  $ETL_CFG_FILE "W"  > /dev/null 2>&1

assignTagValue COMPRESSION_CODEC DW_BRIDGE_COMPRESSION_CODEC  $ETL_CFG_FILE "W"  > /dev/null 2>&1

assignTagValue FAIL_ON_NO_IMPORT_FILE DW_BRIDGE_FAIL_ON_NO_IMPORT_FILE  $ETL_CFG_FILE "W" "true" > /dev/null 2>&1



SELECTSQL=""


if [[ $JOB_ENV = @(td1||td2||td3||td4||td5||td6||td7) ]]
then
  TERADATA_SYSTEM=$(JOB_ENV_UPPER=$(print $JOB_ENV | tr "[:lower:]" "[:upper:]"); eval print \$DW_${JOB_ENV_UPPER}_DB)
  assignTagValue DM_BRIDGE_HADOOP_SYSTEM DM_BRIDGE_HADOOP_SYSTEM $ETL_CFG_FILE "W"
  HADOOP_SYSTEM=$(JOB_ENV_UPPER=$(print $DM_BRIDGE_HADOOP_SYSTEM | tr "[:lower:]" "[:upper:]"); eval print \$DW_${JOB_ENV_UPPER}_DB)
  EXPORT_IMPORT_TYPE=IMPORT
  DATAPATH=${DATAPATH}*

  LOG_FILE=$DW_SA_TMP/${ETL_ID}.td_bridge.${HADOOP_SYSTEM}_to_${TERADATA_SYSTEM}.dynamic.sql.tmp
  TD_BRIDGE_RUNTIME_SQL=$DW_SQL/${ETL_ID}.td_bridge.${HADOOP_SYSTEM}_to_${TERADATA_SYSTEM}.dynamic.sql

  IMPORTSQL_FILE=""

  if [[ -f ${DW_SQL}/${ETL_ID}.td_bridge.import.sql ]] && [ -s ${DW_SQL}/${ETL_ID}.td_bridge.import.sql ]
  then

	   print "cat <<EOF" > $DW_SA_TMP/$TABLE_ID.td_bridge.$ETL_ID.td_bridge.import.sql.tmp
	   cat  ${DW_SQL}/${ETL_ID}.td_bridge.import.sql >> $DW_SA_TMP/$TABLE_ID.td_bridge.$ETL_ID.td_bridge.import.sql.tmp
	   print "\nEOF" >> $DW_SA_TMP/$TABLE_ID.td_bridge.$ETL_ID.td_bridge.import.sql.tmp
	   chmod +x $DW_SA_TMP/$TABLE_ID.td_bridge.$ETL_ID.td_bridge.import.sql.tmp

	   set +u
		. $DW_SA_TMP/$TABLE_ID.td_bridge.$ETL_ID.td_bridge.import.sql.tmp	 > $DW_SA_TMP/$TABLE_ID.td_bridge.$ETL_ID.td_bridge.import.sql.tmp.2
	   set -u

	   mv $DW_SA_TMP/$TABLE_ID.td_bridge.$ETL_ID.td_bridge.import.sql.tmp.2 $DW_SA_TMP/$TABLE_ID.td_bridge.$ETL_ID.td_bridge.import.sql.tmp

	   IMPORTSQL_FILE=$DW_SA_TMP/$TABLE_ID.td_bridge.$ETL_ID.td_bridge.import.sql.tmp
  fi


elif [[ $JOB_ENV = hd* ]]
then
  HADOOP_SYSTEM=$(JOB_ENV_UPPER=$(print $JOB_ENV | tr "[:lower:]" "[:upper:]"); eval print \$DW_${JOB_ENV_UPPER}_DB)
  assignTagValue DM_BRIDGE_TD_SYSTEM DM_BRIDGE_TD_SYSTEM $ETL_CFG_FILE "W"
  TERADATA_SYSTEM=$(JOB_ENV_UPPER=$(print $DM_BRIDGE_TD_SYSTEM | tr "[:lower:]" "[:upper:]"); eval print \$DW_${JOB_ENV_UPPER}_DB)
  EXPORT_IMPORT_TYPE=EXPORT

  LOG_FILE=$DW_SA_TMP/${ETL_ID}.td_bridge.${TERADATA_SYSTEM}_to_${HADOOP_SYSTEM}.dynamic.sql.tmp
  TD_BRIDGE_RUNTIME_SQL=$DW_SQL/${ETL_ID}.td_bridge.${TERADATA_SYSTEM}_to_${HADOOP_SYSTEM}.dynamic.sql

  if [[ -f ${DW_SQL}/${ETL_ID}.td_bridge.export.sel.sql ]] && [ -s ${DW_SQL}/${ETL_ID}.td_bridge.export.sel.sql ]
  then
	   print "cat <<EOF" > $DW_SA_TMP/$TABLE_ID.td_bridge.$ETL_ID.td_bridge.export.sel.sql.tmp
	   cat  ${DW_SQL}/${ETL_ID}.td_bridge.export.sel.sql >> $DW_SA_TMP/$TABLE_ID.td_bridge.$ETL_ID.td_bridge.export.sel.sql.tmp
	   print "\nEOF" >> $DW_SA_TMP/$TABLE_ID.td_bridge.$ETL_ID.td_bridge.export.sel.sql.tmp
	   chmod +x $DW_SA_TMP/$TABLE_ID.td_bridge.$ETL_ID.td_bridge.export.sel.sql.tmp

	   set +u
		. $DW_SA_TMP/$TABLE_ID.td_bridge.$ETL_ID.td_bridge.export.sel.sql.tmp	 > $DW_SA_TMP/$TABLE_ID.td_bridge.$ETL_ID.td_bridge.export.sel.sql.tmp.2
	   set -u

	   mv $DW_SA_TMP/$TABLE_ID.td_bridge.$ETL_ID.td_bridge.export.sel.sql.tmp.2 $DW_SA_TMP/$TABLE_ID.td_bridge.$ETL_ID.td_bridge.export.sel.sql.tmp

	   SELECTSQL=`cat  $DW_SA_TMP/$TABLE_ID.td_bridge.$ETL_ID.td_bridge.export.sel.sql.tmp`
  fi

else
  print "ony support JOB_ENV:  	td1||td2||td3||td5||td6||td7||hd*"
  exit 4
fi

EXPORT_IMPORT_TYPE_LOWER=$(print $EXPORT_IMPORT_TYPE | tr "[:upper:]" "[:lower:]")


if [[ $TERADATA_SYSTEM = @(ace||vivaldibe) ]]
then
  TERADATA_SYSTEM=vivaldi
fi

if [[ $TERADATA_SYSTEM = "mozartbe" ]]
then
  TERADATA_SYSTEM=mozart
fi

if [[ $TERADATA_SYSTEM = "davincibe" ]]
then
  TERADATA_SYSTEM=davinci
fi


if [[ $TERADATA_SYSTEM = @(hopbatch||hopbe) ]]
then
  TERADATA_SYSTEM=hopper
fi


set -A BRIDGE_MAP_VALUES  EXPORT_IMPORT_TYPE  LOG_FILE TERADATA_SYSTEM TD_USERNAME TD_PASSWORD DATABASE_NAME TABLE_NAME  HADOOP_SYSTEM DATAPATH DELIMITERCHARACTER DELIMITERCHARACTERCODE NULLCHARACTER NULLCHARACTERCODE EXPORT_DESTINATION SEQUENCE_FILE_COMPRESSION COMPRESSION_CODEC SOCK_TIMEOUT ARVO_SCHEMA_NAME ARVO_SCHEMA_NAMESPACE FAIL_ON_NO_IMPORT_FILE
set -A BRIDGE_NORMAL_VALUES  ot  l s u p d t  hs dp dc dx nc nx ed z zc st an as "fi"



BRIDGE_ARG=""
args_idx=0

while [[ $args_idx -lt ${#BRIDGE_MAP_VALUES[*]} ]]
do
    ARG_NAME=${BRIDGE_MAP_VALUES[$args_idx]}

    eval "ARG_VALUE=\${${ARG_NAME}:-}"

    if [[ "X${ARG_VALUE:-}" != "X" ]]
    then
 #   	print $ARG_NAME    $ARG_VALUE  ${BRIDGE_NORMAL_VALUES[$args_idx]}
	BRIDGE_ARG=" $BRIDGE_ARG -${BRIDGE_NORMAL_VALUES[$args_idx]}   ${ARG_VALUE} "
    elif [[ $ARG_NAME = @(TERADATA_SYSTEM||TERADATA_PASSWORD||DATABASE_NAME||TABLE_NAME||HADOOP_SYSTEM||DATAPATH||EXPORT_IMPORT_TYPE) ]]
    then
    	  print "${0##*/}:  FATAL ERROR, $ARG_NAME not defined" >&2
  	  exit 4

    fi
    ((args_idx=args_idx+1))
done


#--------------------------------------
# launch TD Bridge
#--------------------------------------
print "Running java -jar $DW_MASTER_EXE/sqlBridge.jar to generate the SQL for TD-Bridge"
set +e

  if [[ $EXPORT_IMPORT_TYPE = "IMPORT" ]]
  then
  	set +e
	  	eval  java -jar $DW_MASTER_EXE/sqlBridge.jar   "$BRIDGE_ARG"
	  	RCODE=$?
  	set -e
  	if [ $RCODE != 0 ]
	then
	  print "${0##*/}:  FATAL_ERROR, see log file $LOG_FILE" >&2
	  exit 4
	else
	  if [ X$IMPORTSQL_FILE = "X" ]
	  then
		  print "\n" >> $LOG_FILE
		  mv $LOG_FILE  $TD_BRIDGE_RUNTIME_SQL

	  else

		egrep -i "^ +set +query_band|^set +query_band" $LOG_FILE  | egrep -i "for +session;$|for +session +;$"|read QueryBandLine

		print $QueryBandLine > $TD_BRIDGE_RUNTIME_SQL
		print "\n" >> $TD_BRIDGE_RUNTIME_SQL
		cat $IMPORTSQL_FILE >> $TD_BRIDGE_RUNTIME_SQL
		print "\n" >> $TD_BRIDGE_RUNTIME_SQL
		rm -f $LOG_FILE

	  fi
	  print "SQL for TD-Bridge generated successfully"
	fi


  else	#[[ $EXPORT_IMPORT_TYPE = "EXPORT" ]]
  	set +e


	if [[ X$SELECTSQL = "X" ]]
	then
		eval  java -jar $DW_MASTER_EXE/sqlBridge.jar   "$BRIDGE_ARG"
		RCODE=$?
	else
		eval  java -jar $DW_MASTER_EXE/sqlBridge.jar    "$BRIDGE_ARG" " -sq \"${SELECTSQL}\""
		RCODE=$?
       fi

  	set -e

  	if [ $RCODE != 0 ]
	then
	  print "${0##*/}:  FATAL_ERROR, see log file $LOG_FILE" >&2
	  exit 4
	else
	  print "\n" >> $LOG_FILE
	  mv $LOG_FILE  $TD_BRIDGE_RUNTIME_SQL
	  print "SQL for TD-Bridge generated successfully"
	fi
  fi

if [[ $NO_BRACEEXPAND_NO_GLOB -eq 1 ]]
  then
    set -o braceexpand
    set -o glob
fi

#eval  java -jar $DW_MASTER_EXE/sqlBridge.jar   "$BRIDGE_ARG" " -sq \"${SELECTSQL}\""
#print  java -jar $DW_MASTER_EXE/sqlBridge.jar   "$BRIDGE_ARG" " -sq \"${SELECTSQL}\""
