#!/bin/ksh -eu
# Title:        Single oraoci Extract
# File Name:    single_oraoci_extract.ksh
# Description:  Handle submiting a oracle oci extract job
# Developer:    George Xiong
# Created on:
# Location:     $DW_EXE
# Logic:
#
#
# Called by:    UC4/Unix
#
# Date         Ver#   Modified By(Name)            Change and Reason for Change
#---------    -----  ---------------------------  ------------------------------
# 2013-01-04   1.0    George Xiong                 Initial version
# 2013-07-22   1.1    Jacky Shen                   Add UTF8 condition
# 2013-07-22   1.1    George Xiong                 Add CLOB Length condition
# 2013-10-04   1.2    Ryan Wong                    Redhat changes
# 2013-11-05   1.3    Ryan Wong                    Syncing grep changes on Redhat development
# 2015-08-25   1.4    John Hackley                 ETL password encryption changes
#############################################################################################################

ETL_ID=$1
FILE_ID=$2
DBC_FILE=$3
TABLE_NAME=$4
DATA_FILENAME=$5
shift 5

while [ $# -gt 0 ]
do
  DWI_KWD="${1}"
  shift
  case $DWI_KWD in
    -UOW_FROM )
      export UOW_FROM="${1}"
      shift
      ;;
    -UOW_TO )
      export UOW_TO="${1}"
      shift
      ;;
    -PARAM1 )
      export PARAM1="${1}"
      shift
      ;;
    -PARAM2 )
      export PARAM2="${1}"
      shift
      ;;
    -PARAM3 )
      export PARAM3="${1}"
      shift
      ;;
    -PARAM4 )
      export PARAM4="${1}"
      shift
      ;;
    * )
      print "FATAL ERROR:  Unexpected command line argument"
      print "Usage: single_oraoci_extract.ksh <ETL_ID> <FILE_ID> <DBC_FILE> <TABLE_NAME> <DATA_FILENAME> -UOW_FROM <UOW_FROM> -UOW_TO <UOW_TO> -PARAM1 <PARAM1> -PARAM2 <PARAM2> -PARAM3 <PARAM3> -PARAM4 <PARAM4>"
      exit 4
  esac
done

. /dw/etl/mstr_cfg/etlenv.setup
. $DW_MASTER_LIB/dw_etl_common_functions.lib

EXTRACT_LOG_FILE=$DW_SA_LOG/$TABLE_ID.extract.$FILE_ID.oracle_oci$UOW_APPEND.$CURR_DATETIME.log
RECORD_COUNT_FILE=$DW_SA_TMP/$TABLE_ID.ex.$FILE_ID.record_count.dat





# For extract: Calculate FROM_EXTRACT_VALUE and TO_EXTRACT_VALUE
if [[ $LAST_EXTRACT_TYPE == "V" ]]
then
  assignTagValue TO_EXTRACT_VALUE_FUNCTION TO_EXTRACT_VALUE_FUNCTION $ETL_CFG_FILE
  LAST_EXTRACT_VALUE_FILE=$DW_SA_DAT/$TABLE_ID.$FILE_ID.last_extract_value.dat
  export FROM_EXTRACT_VALUE=$(<$LAST_EXTRACT_VALUE_FILE)
  export TO_EXTRACT_VALUE=$($TO_EXTRACT_VALUE_FUNCTION)
elif [[ $LAST_EXTRACT_TYPE == "U" ]]
then
  assignTagValue UOW_FROM_REFORMAT_CODE UOW_FROM_REFORMAT_CODE $ETL_CFG_FILE W 0
  assignTagValue UOW_TO_REFORMAT_CODE UOW_TO_REFORMAT_CODE $ETL_CFG_FILE W 0
  export FROM_EXTRACT_VALUE=$($DW_MASTER_BIN/dw_infra.reformat_timestamp.ksh $UOW_FROM $UOW_FROM_REFORMAT_CODE)
  export TO_EXTRACT_VALUE=$($DW_MASTER_BIN/dw_infra.reformat_timestamp.ksh $UOW_TO $UOW_TO_REFORMAT_CODE)
fi



SQL_FILENAME=${ETL_ID}.sel.sql

print "cat <<EOF" > $DW_SA_TMP/$TABLE_ID.${FILE_ID}.ex.$SQL_FILENAME.tmp
cat $DW_SQL/${SQL_FILENAME}|tr ';' ' '  >> $DW_SA_TMP/$TABLE_ID.${FILE_ID}.ex.$SQL_FILENAME.tmp
print " " >> $DW_SA_TMP/$TABLE_ID.${FILE_ID}.ex.$SQL_FILENAME.tmp
print "EOF" >> $DW_SA_TMP/$TABLE_ID.${FILE_ID}.ex.$SQL_FILENAME.tmp

chmod +x  $DW_SA_TMP/$TABLE_ID.${FILE_ID}.ex.$SQL_FILENAME.tmp
set +u
. $DW_SA_TMP/$TABLE_ID.${FILE_ID}.ex.$SQL_FILENAME.tmp > $DW_SA_TMP/$TABLE_ID.${FILE_ID}.ex.$SQL_FILENAME.tmp.2
set -u






file_idx=0
instance_idx=0

for FILE_ID_LIST  in `cat  $DW_SA_TMP/$TABLE_ID.*.dbc.*.lis${GROUP_APPEND} |sort|awk '{print $1}'`
do
 if [ ${FILE_ID_LIST} = ${FILE_ID} ]
 then
    instance_idx=${file_idx} 
    break
 fi
 ((file_idx+=1))
done
 




assignTagValue ORAOCI_EXTRACT_NHOSTS MULTI_HOST  $ETL_CFG_FILE W 0
if [ $ORAOCI_EXTRACT_NHOSTS = 0 ]
  then
  HOSTS_LIST_FILE=$DW_CFG/$ETL_ID.host.lis
  if [ ! -f $HOSTS_LIST_FILE ]
  then
    print "${0##*/}:  FATAL ERROR: ORAOCI_EXTRACT_NHOSTS is zero, and $HOSTS_LIST_FILE does not exist" >&2
    exit 4
  fi
elif [[ $ORAOCI_EXTRACT_NHOSTS = 1 ]]
then
	ORAOCI_NODE=${servername}
elif [[ $ORAOCI_EXTRACT_NHOSTS = @(2||4||6||8||16||32) ]]
then
  HOSTS_LIST_FILE=$DW_MASTER_CFG/${servername%%.*}.${ORAOCI_EXTRACT_NHOSTS}ways.host.lis
else
  print "${0##*/}:  FATAL ERROR: ORAOCI_EXTRACT_NHOSTS not valid value $ORAOCI_EXTRACT_NHOSTS" >&2
  exit 4
fi



set -A ORAOCI_HOSTS
if [ $ORAOCI_EXTRACT_NHOSTS = 1 ]
then
  ORAOCI_HOSTS[0]=$ORAOCI_NODE
else
  ORAOCI_IDX=0
  while read ORAOCI_NODE junk
  do
    ORAOCI_HOSTS[$ORAOCI_IDX]=$ORAOCI_NODE
    ((ORAOCI_IDX+=1))
  done < $HOSTS_LIST_FILE
fi

ORAOCI_HOST_CNT=${#ORAOCI_HOSTS[*]}

host_idx=$(( $instance_idx % $ORAOCI_HOST_CNT ))

host_name=${ORAOCI_HOSTS[${host_idx}]}



if [ ${host_name%%.*} = ${servername%%.*} ]
 then
 	mv  $DW_SA_TMP/$TABLE_ID.${FILE_ID}.ex.$SQL_FILENAME.tmp.2 $DW_SA_TMP/$TABLE_ID.${FILE_ID}.ex.$SQL_FILENAME.tmp		
 else
 	set +e
	ssh -nq $host_name "mkdir -p $DW_SA_LOG;mkdir -p $IN_DIR" > /dev/null	
	ssh -nq $host_name "mkdir -p $DW_SA_TMP;" > /dev/null
	set -e
	
	scp $DW_SA_TMP/$TABLE_ID.${FILE_ID}.ex.$SQL_FILENAME.tmp.2  ${host_name}:$DW_SA_TMP/$TABLE_ID.${FILE_ID}.ex.$SQL_FILENAME.tmp 
	rm  $DW_SA_TMP/$TABLE_ID.${FILE_ID}.ex.$SQL_FILENAME.tmp.2
fi  




  
 

if [ -f ${DW_DBC}/${DBC_FILE}	]
then
 grep -w "^db_name:" ${DW_DBC}/${DBC_FILE} | read A TNS_NAME_TMP
 TNS_NAME=${TNS_NAME_TMP#@}
 
 IS_UTF8=0
 set +e
 grep -w "^db_character_set:" ${DW_DBC}/${DBC_FILE} | read B charset_tmp
 rc_charset=$?
 set -e

 if [[ $rc_charset -eq 0 ]]
 then
   if [[ $charset_tmp == "utf8" ]]
   then
     IS_UTF8=1
   fi
  fi

else
	print "${0##*/}:  ERROR, Failure determining dbms value from ${DW_DBC}/${DBC_FILE}"
	exit 4
fi

# Retrieve and decrypt password for $TNS_NAME
DWI_fetch_pw $ETL_ID oracle $TNS_NAME
DWIrc=$?

if [[ -n $ORA_PASSWORD ]]
then
	export $ORA_USERNAME $ORA_PASSWORD
else
	print "Unable to retrieve Oracle password, exiting; ETL_ID=$ETL_ID; TNS_NAME=$TNS_NAME"
	exit $DWIrc
fi	 


long=5000
 

if [[ $LAST_EXTRACT_TYPE == "U" ]]
then
	DATA_FILENAME_TMP=${IN_DIR}/${DATA_FILENAME}
else 
	DATA_FILENAME_TMP=${IN_DIR}/${DATA_FILENAME}.${BATCH_SEQ_NUM}
fi


assignTagValue FIELD_HEX_DELIMITER ORAOCI_FIELD_HEX_DELIMITER  $ETL_CFG_FILE W  "0x7f"
assignTagValue ROW_HEX_DELIMITER ORAOCI_ROW_HEX_DELIMITER  $ETL_CFG_FILE W  "0x0a"
assignTagValue LOB_LENGTH ORAOCI_LOB_LENGTH  $ETL_CFG_FILE W  "1"
assignTagValue REMOVE_DELIMITER_IN_CHAR ORAOCI_REMOVE_DELIMITER_IN_CHAR  $ETL_CFG_FILE W  "0"


print ${host_name%%.*}
print ${servername%%.*}

 if [ ${host_name%%.*} = ${servername%%.*} ]
  then
    
    print "Local launch eval oraexp2.bin logins=\"${ORA_USERNAME}/******@${TNS_NAME}\" sql=$DW_SA_TMP/$TABLE_ID.${FILE_ID}.ex.$SQL_FILENAME.tmp fdel=${FIELD_HEX_DELIMITER}  rdel=${ROW_HEX_DELIMITER} file=${DATA_FILENAME_TMP} array=50 removedelimiter=${REMOVE_DELIMITER_IN_CHAR} writelen=${LOB_LENGTH} " | tee $EXTRACT_LOG_FILE
	

    set +e
    
    if [[ $IS_UTF8 -eq 1 ]]
    then
      export NLS_LANG=AMERICAN_AMERICA.UTF8
    fi

    eval $DW_MASTER_BIN/oraexp2.bin sql=$DW_SA_TMP/$TABLE_ID.${FILE_ID}.ex.$SQL_FILENAME.tmp fdel=${FIELD_HEX_DELIMITER}  rdel=${ROW_HEX_DELIMITER} file=${DATA_FILENAME_TMP} array=50 removedelimiter=${REMOVE_DELIMITER_IN_CHAR} writelen=${LOB_LENGTH}  logins=\"${ORA_USERNAME}/${ORA_PASSWORD}@${TNS_NAME}\"  >> $EXTRACT_LOG_FILE 2>&1 &
    wait $!
    rcode=$?
    set -e

  else 
        
    print "Remote launch ssh -nq $host_name  $DW_MASTER_BIN/oraexp2.bin logins=\"${ORA_USERNAME}/******@${TNS_NAME}\" sql=$DW_SA_TMP/$TABLE_ID.${FILE_ID}.ex.$SQL_FILENAME.tmp fdel=${FIELD_HEX_DELIMITER}  rdel=${ROW_HEX_DELIMITER} file=${DATA_FILENAME_TMP} array=50 removedelimiter=${REMOVE_DELIMITER_IN_CHAR} writelen=${LOB_LENGTH}" |tee  $EXTRACT_LOG_FILE

    set +e
    
    if [[ $IS_UTF8 -eq 1 ]]
    then
        ssh -nq $host_name ". $HOME/.profile;export TNS_ADMIN=$TNS_ADMIN;export NLS_LANG=AMERICAN_AMERICA.UTF8;$DW_MASTER_BIN/oraexp2.bin  sql=$DW_SA_TMP/$TABLE_ID.${FILE_ID}.ex.$SQL_FILENAME.tmp fdel=${FIELD_HEX_DELIMITER}  rdel=${ROW_HEX_DELIMITER} file=${DATA_FILENAME_TMP} array=50 removedelimiter=${REMOVE_DELIMITER_IN_CHAR} writelen=${LOB_LENGTH} logins=${ORA_USERNAME}/${ORA_PASSWORD}@${TNS_NAME} " >> $EXTRACT_LOG_FILE 2>&1 &
        
        wait $!
        rcode=$?
    else 
 
    	ssh -nq $host_name ". $HOME/.profile;export TNS_ADMIN=$TNS_ADMIN;$DW_MASTER_BIN/oraexp2.bin  sql=$DW_SA_TMP/$TABLE_ID.${FILE_ID}.ex.$SQL_FILENAME.tmp fdel=${FIELD_HEX_DELIMITER}  rdel=${ROW_HEX_DELIMITER} file=${DATA_FILENAME_TMP} array=50 removedelimiter=${REMOVE_DELIMITER_IN_CHAR} writelen=${LOB_LENGTH} logins=${ORA_USERNAME}/${ORA_PASSWORD}@${TNS_NAME} " >> $EXTRACT_LOG_FILE 2>&1 &           
    
    	wait $!
    	rcode=$?
    fi

    set -e
fi



if [ ${rcode} != 0 ]
  then
    print "${0##*/}:  FATAL ERROR, see log file $EXTRACT_LOG_FILE  " >&2
    exit 4
fi

 
# Scrape  log for total record count

RECORD_COUNT=$(egrep  "Totally [[0-9]+ rows exported"  $EXTRACT_LOG_FILE|awk  '{print $4}')


if [[ "X$RECORD_COUNT" != "X" ]]
then
  print $RECORD_COUNT > $RECORD_COUNT_FILE
else
  print "${0##*/}:  FATAL ERROR, Problem scraping record count from $EXTRACT_LOG_FILE" >&2
  exit 4
fi

if [[ $LAST_EXTRACT_TYPE = "V" ]]
then
  print $TO_EXTRACT_VALUE > $DW_SA_DAT/$TABLE_ID.$FILE_ID.last_extract_value.dat
fi

exit 0
