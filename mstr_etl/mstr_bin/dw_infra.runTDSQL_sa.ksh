#!/bin/ksh -eu
#
#############################################################################################################
#
# Title:        Run Teradata SQL
# File Name:    dw_infra.runTDSQL.ksh
# Description:  Basic SQL Execution script
# Developer:    Kevin Oaks
# Created on:   2010-10-14
# Location:     $DW_EXE
#
# Date         Ver#   Modified By(Name)            Change and Reason for Change
#---------    -----  ---------------------------  ------------------------------
# 2010-10-15   1.0    Kevin Oaks                   Copied and modified from target_table_load.ksh
# 2010-11-01   1.1    Kevin Oaks                   Cleaned up, renamed, added Query Banding Logic
# 2011-09-09   1.2    Kevin Oaks                   Added Optional UOW logic
# 2011-11-01   1.3    Kevin Oaks                   Added override for TD Login File
# 2011-11-15   1.4    Kevin Oaks                   Moved OPTARGS processing before setup
#                                                  to allow TD Login override via etlenv.setup
# 2013-06-18   1.5    George Xiong		   fix the issue, when queryband tag value contain "/", cause the job fail
# 2013-10-04   1.6    Ryan Wong                    Redhat changes
# 2016-09-16   1.7    Ryan Wong                    Adding Queryband name-value-pairs UC4_JOB_BATCH_MODE and UC4_JOB_PRIORITY
#
##############################################################################################################

function usage {
   print "Usage: $0 <ETL_ID> <JOB_ENV> <SQL_FILENAME> -l <TD_LOGON_FILE_OVERRIDE> [ -f <UOW_FROM> -t <UOW_TO> ]
NOTE: UOW_FROM amd UOW_TO are optional but must be used in tandem if either is present."
}

# Check Args for validity/help
if [[ ( $# -gt 0 && $1 == -help ) || $# -lt 3 ]]
then
   usage
   exit 1
fi

ETL_ID=$1
JOB_ENV=$2
SQL_FILENAME=$3

shift 3

# Initialize UOW related variables
UOW_FROM_FLAG=0
UOW_TO_FLAG=0
UOW_FROM=""
UOW_TO=""

# getopts loop for processing optional args including UOW
while getopts "f:t:l:" opt
do
   case $opt in
      f ) if [ $UOW_FROM_FLAG -ne 0 ]
          then
             print "Fatal Error: -f flag specified more than once" >&2
             usage
             exit 4
          fi
          print "Setting UOW_FROM_FLAG == 1"
          UOW_FROM_FLAG=1
          print "Setting UOW_FROM == $OPTARG"
          UOW_FROM=${OPTARG};;
      t ) if [ $UOW_TO_FLAG -ne 0 ]
          then
             print "FATAL ERROR: -t flag specified more than once" >&2
             usage
             exit 4
          fi
          print "Setting UOW_TO_FLAG == 1"
          UOW_TO_FLAG=1
          print "Setting UOW_TO == $OPTARG"
          UOW_TO=$OPTARG;;
      l ) print "Setting TD_LOGON_FILE ID == $OPTARG"
          export TD_LOGON_FILE_OVERRIDE=${OPTARG};;
      \? ) usage
           exit 4;;
   esac
done
shift $((OPTIND - 1))

# Host Setup/Definitions/Functions
. /dw/etl/mstr_cfg/etlenv.setup
. $DW_MASTER_CFG/dw_etl_common_defs.cfg
. $DW_MASTER_LIB/dw_etl_common_functions.lib

SQL_FILE_BASENAME=${SQL_FILENAME##*/}
export SQL_FILE_BASENAME=${SQL_FILE_BASENAME%.*}

# Calculate UOW values
UOW_APPEND=""
UOW_PARAM_LIST=""
UOW_PARAM_LIST_AB=""
if [[ $UOW_FROM_FLAG -eq 1 && $UOW_TO_FLAG -eq 1 ]]
then
   UOW_APPEND=.$UOW_TO
   UOW_PARAM_LIST="-f $UOW_FROM -t $UOW_TO"
   UOW_PARAM_LIST_AB="-UOW_FROM $UOW_FROM -UOW_TO $UOW_TO"
   is_valid_ts $UOW_FROM
   is_valid_ts $UOW_TO
elif [[ ($UOW_FROM_FLAG -eq 1 && $UOW_TO_FLAG -eq 0) || ($UOW_TO_FLAG -eq 1 && $UOW_FROM_FLAG -eq 0) ]]
then
   print "Missing required UOW (FROM or TO) parameter."
   usage
   exit 1
fi 

export DB_NAME=$(JOB_ENV_UPPER=$(print $JOB_ENV | tr "[:lower:]" "[:upper:]"); eval print \$DW_${JOB_ENV_UPPER}_DB)
export FILE_DATETIME=${CURR_DATETIME:-$(date "+%Y%m%d-%H%M%S")}

print "cat <<EOF" > $DW_SA_TMP/$TABLE_ID.bt.$SQL_FILENAME.tmp

# Adding QUERY_BAND UC4 PART

export UC4_JOB_NAME=${UC4_JOB_NAME:-"NA"}
export UC4_PRNT_CNTR_NAME=${UC4_PRNT_CNTR_NAME:-"NA"}
export UC4_TOP_LVL_CNTR_NAME=${UC4_TOP_LVL_CNTR_NAME:-"NA"};
export UC4_JOB_RUN_ID=${UC4_JOB_RUN_ID:-"NA"}
export UC4_JOB_BATCH_MODE=${UC4_JOB_BATCH_MODE:-"NA"}
export UC4_JOB_PRIORITY=${UC4_JOB_PRIORITY:-"NA"}
export QB_STR_UC4="UC4_JOB_NAME=${UC4_JOB_NAME};UC4_PRNT_CNTR_NAME=${UC4_PRNT_CNTR_NAME};UC4_TOP_LVL_CNTR_NAME=${UC4_TOP_LVL_CNTR_NAME};UC4_JOB_RUN_ID=${UC4_JOB_RUN_ID};UC4_JOB_BATCH_MODE=${UC4_JOB_BATCH_MODE};UC4_JOB_PRIORITY=${UC4_JOB_PRIORITY}"


# Adding QUERY_BAND RM PART

export QB_STR_RM=` head  -1 $DW_SA_SQL/$SQL_FILENAME| egrep -i "^/\* +RM_REL_DATE=[0-9]+;RM_REL_ID=[0-9|a-z|A-Z|\_]+ +\*/$"|awk -F\* '{print $2}' `
RM_REL_DATE=`print $QB_STR_RM|awk -F\; '{print  $1}'| awk -F\= '{print  $2}'`
export RM_REL_DATE=${RM_REL_DATE:-"196912310000"}
RM_REL_ID=`print $QB_STR_RM|awk -F\; '{print  $2}'| awk -F\= '{print  $2}'`
export RM_REL_ID=${RM_REL_ID:-"NA"}


# Adding QUERY_BAND from infra level

set +e
egrep -i "^ +set +query_band|^set +query_band" $DW_SA_SQL/$SQL_FILENAME | egrep -i "for +session;$|for +session +;$"
rcode=$?
set -e

# If the SQL script doesn't have QUERY_BAND setting
if [ $rcode = 1 ]
  then
  	
    QB_TMP="SA=$SUBJECT_AREA;TBID=$TABLE_ID;SCRIPTNAME=$SQL_FILENAME;RM_REL_DATE=$RM_REL_DATE;RM_REL_ID=$RM_REL_ID;$QB_STR_UC4;"	
    print "SET QUERY_BAND for $SQL_FILENAME"
  #  print "SET QUERY_BAND = '$QB_TMP' FOR SESSION;" >> $DW_SA_TMP/$TABLE_ID.bt.$SQL_FILENAME.tmp
    cat $DW_SA_SQL/$SQL_FILENAME >> $DW_SA_TMP/$TABLE_ID.bt.$SQL_FILENAME.tmp
    print "\nEOF" >> $DW_SA_TMP/$TABLE_ID.bt.$SQL_FILENAME.tmp

# If the SQL script already has QUERY_BAND setting
elif [ $rcode = 0 ]
  then
    print "QUERY_BAND is already set by user. Append Infra Standard Tag if any not included"
    QB_STR=`egrep -i "^ +set +query_band|^set +query_band" $DW_SA_SQL/$SQL_FILENAME | egrep -i "for +session;$|for +session +;$" | head -1 | awk -F\' '{print $2}'`
    QB_TMP=$QB_STR
    STD_QB="SA TBID SCRIPTNAME RM_REL_DATE RM_REL_ID UC4_JOB_NAME UC4_PRNT_CNTR_NAME UC4_TOP_LVL_CNTR_NAME UC4_JOB_RUN_ID"

    for QB_STR_CHK in `print $STD_QB`
      do
        case $QB_STR_CHK in
                              SA)   QB_STR_CHK_VLU=$SUBJECT_AREA;;		
                            TBID)   QB_STR_CHK_VLU=$TABLE_ID;;
                      SCRIPTNAME)   QB_STR_CHK_VLU=$SQL_FILENAME;;		
	             RM_REL_DATE)   QB_STR_CHK_VLU=$RM_REL_DATE;;		
	               RM_REL_ID)   QB_STR_CHK_VLU=$RM_REL_ID;;	      
	            UC4_JOB_NAME)   QB_STR_CHK_VLU=$UC4_JOB_NAME;;		
	      UC4_PRNT_CNTR_NAME)   QB_STR_CHK_VLU=$UC4_PRNT_CNTR_NAME;;	
	   UC4_TOP_LVL_CNTR_NAME)   QB_STR_CHK_VLU=$UC4_TOP_LVL_CNTR_NAME;;	
		  UC4_JOB_RUN_ID)   QB_STR_CHK_VLU=$UC4_JOB_RUN_ID;;	                         
        esac
        set +e
        print $QB_STR | grep -iw "${QB_STR_CHK}"
        rcode=$?
        set -e
        if [ $rcode = 1 ]
          then
            QB_TMP="$QB_TMP$QB_STR_CHK=$QB_STR_CHK_VLU;"
        fi
    done

 #   print "SET QUERY_BAND = '$QB_TMP' FOR SESSION;" >> $DW_SA_TMP/$TABLE_ID.bt.$SQL_FILENAME.tmp
    cat $DW_SA_SQL/$SQL_FILENAME > $DW_SA_TMP/$TABLE_ID.bt.$SQL_FILENAME.qbtmp
    print "\nEOF" >> $DW_SA_TMP/$TABLE_ID.bt.$SQL_FILENAME.qbtmp

    egrep -in "^ +set +query_band|^set +query_band" $DW_SA_TMP/$TABLE_ID.bt.$SQL_FILENAME.qbtmp | egrep -i "for +session;$|for +session +;$"|awk -F\: '{print $1}'|read QueryBandLine
    eval " sed ${QueryBandLine}d $DW_SA_TMP/$TABLE_ID.bt.$SQL_FILENAME.qbtmp >> $DW_SA_TMP/$TABLE_ID.bt.$SQL_FILENAME.tmp "

    rm -f $DW_SA_TMP/$TABLE_ID.bt.$SQL_FILENAME.qbtmp
fi

print "SET QUERY_BAND = '$QB_TMP' FOR SESSION;" > $DW_SA_TMP/$TABLE_ID.bt.$SQL_FILENAME.tmp.2

chmod +x $DW_SA_TMP/$TABLE_ID.bt.$SQL_FILENAME.tmp
set +u
. $DW_SA_TMP/$TABLE_ID.bt.$SQL_FILENAME.tmp >> $DW_SA_TMP/$TABLE_ID.bt.$SQL_FILENAME.tmp.2
set -u
mv $DW_SA_TMP/$TABLE_ID.bt.$SQL_FILENAME.tmp.2 $DW_SA_TMP/$TABLE_ID.bt.$SQL_FILENAME.tmp

#--- Add SQL file size validation ---
if [ ! -s $DW_SA_TMP/$TABLE_ID.bt.$SQL_FILENAME.tmp ]
then
   print " SQL File is empty : $DW_SA_TMP/$TABLE_ID.bt.$SQL_FILENAME.tmp "
   exit 1
fi

RUN_SQL_LOGFILE=$DW_SA_LOG/$TABLE_ID.bt.${SQL_FILE_BASENAME}${UOW_APPEND}.$FILE_DATETIME.log

set +e
bteq <<EOF > $RUN_SQL_LOGFILE
.SET ERROROUT STDOUT 
.set session transaction btet
.logon $DB_NAME/$TD_USERNAME,$TD_PASSWORD
select 'SESS', session;
.maxerror 1
.run file=$DW_SA_TMP/$TABLE_ID.bt.$SQL_FILENAME.tmp;
EOF
rcode=$?
set -e

if [ rcode -ne 0 ]
then
    print "Error executing SQL:"
    print
    cat $RUN_SQL_LOGFILE
    exit 1
fi

exit



