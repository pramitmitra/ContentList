#!/bin/ksh -eu
#------------------------------------------------------------------------------------------------
# Filename:     target_table_load.ksh
#
# Revision History:
#
# Name             Date            Description
# ---------------  --------------  ---------------------------------------------------
# ???              ??/??/????      Initial Creation
# Ryan Wong        10/04/2013      Redhat changes
#
#------------------------------------------------------------------------------------------------

#export AB_GRAPH_NAME;AB_GRAPH_NAME=target_table_load

# Host Setup Commands:
#. /export/home/abinitio/cfg/abinitio.setup

if [ $# -gt 0 -a X"$1" = X"-help" ]; then
print -r -- 'Usage: target_table_load.ksh <ETL_ID> <JOB_ENV> <SQL_FILENAME>'
exit 1
fi

ETL_ID=$1
JOB_ENV=$2
SQL_FILENAME=$3

# Command Line Processing

if [ X"${ETL_ID:-}" = X"" ]; then
   print -r -- 'Required parameter ETL_ID undefined'
   print -r -- 'Usage: target_table_load.ksh <ETL_ID> <JOB_ENV> <SQL_FILENAME>'
   exit 1
fi

if [ X"${JOB_ENV:-}" = X"" ]; then
   print -r -- 'Required parameter JOB_ENV undefined'
   print -r -- 'Usage: target_table_load.ksh <ETL_ID> <JOB_ENV> <SQL_FILENAME>'
   exit 1
fi

if [ X"${SQL_FILENAME:-}" = X"" ]; then
   print -r -- 'Required parameter SQL_FILENAME undefined'
   print -r -- 'Usage: target_table_load.ksh <ETL_ID> <JOB_ENV> <SQL_FILENAME>'
   exit 1
fi

#export ETL_CFG_FILE="$DW_CFG"'/'"$ETL_ID"'.cfg'
#export TABLE_ID="${ETL_ID##*.}"
#export AB_JOB=$(if [ $ETL_ENV ]
#then
#   print $AB_JOB.$TABLE_ID.${SQL_FILENAME%.sql}.$ETL_ENV.$JOB_ENV
#else
#   print $AB_JOB.$TABLE_ID.${SQL_FILENAME%.sql}.$JOB_ENV
#fi)
export SUBJECT_AREA="${ETL_ID%%.*}"
export DB_NAME=$(JOB_ENV_UPPER=$(print $JOB_ENV | tr "[:lower:]" "[:upper:]"); eval print \$DW_${JOB_ENV_UPPER}_DB)
#export AB_IDB_CONFIG='teradata_'"${DB_NAME}"'.dbc'
export DW_SA_LOG="$DW_LOG"'/'"$JOB_ENV"'/'"$SUBJECT_AREA"
export DW_SA_TMP="$DW_TMP"'/'"$JOB_ENV"'/'"$SUBJECT_AREA"
export FILE_DATETIME=$(date '+%Y%m%d-%H%M%S')

print "cat <<EOF" > $DW_SA_TMP/$TABLE_ID.bt.$SQL_FILENAME.tmp



# Adding QUERY_BAND UC4 PART

export UC4_JOB_NAME=${UC4_JOB_NAME:-"NA"}
export UC4_PRNT_CNTR_NAME=${UC4_PRNT_CNTR_NAME:-"NA"}
export UC4_TOP_LVL_CNTR_NAME=${UC4_TOP_LVL_CNTR_NAME:-"NA"};
export UC4_JOB_RUN_ID=${UC4_JOB_RUN_ID:-"NA"}
export QB_STR_UC4="UC4_JOB_NAME=${UC4_JOB_NAME};UC4_PRNT_CNTR_NAME=${UC4_PRNT_CNTR_NAME};UC4_TOP_LVL_CNTR_NAME=${UC4_TOP_LVL_CNTR_NAME};UC4_JOB_RUN_ID=${UC4_JOB_RUN_ID}"


# Adding QUERY_BAND RM PART

export QB_STR_RM=` head  -1 $DW_SQL/$SQL_FILENAME| egrep -i "^/\* +RM_REL_DATE=[0-9]+;RM_REL_ID=[0-9|a-z|A-Z|\_]+ +\*/$"|awk -F\* '{print $2}' `
RM_REL_DATE=`print $QB_STR_RM|awk -F\; '{print  $1}'| awk -F\= '{print  $2}'`
export RM_REL_DATE=${RM_REL_DATE:-"196912310000"}
RM_REL_ID=`print $QB_STR_RM|awk -F\; '{print  $2}'| awk -F\= '{print  $2}'`
export RM_REL_ID=${RM_REL_ID:-"NA"}


# Adding QUERY_BAND from infra level

set +e
egrep -i "^ +set +query_band|^set +query_band" $DW_SQL/$SQL_FILENAME | egrep -i "for +session;$|for +session +;$"
rcode=$?
set -e

# If the SQL script doesn't have QUERY_BAND setting
if [ $rcode = 1 ]
  then
  	
    QB_TMP="SA=$SUBJECT_AREA;TBID=$TABLE_ID;SCRIPTNAME=$SQL_FILENAME;RM_REL_DATE=$RM_REL_DATE;RM_REL_ID=$RM_REL_ID;$QB_STR_UC4;"	
    print "SET QUERY_BAND for $SQL_FILENAME"
    print "SET QUERY_BAND = '$QB_TMP' FOR SESSION;" >> $DW_SA_TMP/$TABLE_ID.bt.$SQL_FILENAME.tmp
    cat $DW_SQL/$SQL_FILENAME >> $DW_SA_TMP/$TABLE_ID.bt.$SQL_FILENAME.tmp
    print "\nEOF" >> $DW_SA_TMP/$TABLE_ID.bt.$SQL_FILENAME.tmp

# If the SQL script already has QUERY_BAND setting
elif [ $rcode = 0 ]
  then
    print "QUERY_BAND is already set by user. Append Infra Standard Tag if any not included"
    QB_STR=`egrep -i "^ +set +query_band|^set +query_band" $DW_SQL/$SQL_FILENAME | egrep -i "for +session;$|for +session +;$" | head -1 | awk -F\' '{print $2}'`
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
    
    
    print "SET QUERY_BAND = '$QB_TMP' FOR SESSION;" >> $DW_SA_TMP/$TABLE_ID.bt.$SQL_FILENAME.tmp
    cat $DW_SQL/$SQL_FILENAME > $DW_SA_TMP/$TABLE_ID.bt.$SQL_FILENAME.qbtmp
    print "\nEOF" >> $DW_SA_TMP/$TABLE_ID.bt.$SQL_FILENAME.qbtmp
    eval sed '/"$QB_STR"/d' $DW_SA_TMP/$TABLE_ID.bt.$SQL_FILENAME.qbtmp >> $DW_SA_TMP/$TABLE_ID.bt.$SQL_FILENAME.tmp
    rm -f $DW_SA_TMP/$TABLE_ID.bt.$SQL_FILENAME.qbtmp
fi

#cat $DW_SQL/$SQL_FILENAME >> $DW_SA_TMP/$TABLE_ID.bt.$SQL_FILENAME.tmp
#print "\nEOF" >> $DW_SA_TMP/$TABLE_ID.bt.$SQL_FILENAME.tmp

chmod +x $DW_SA_TMP/$TABLE_ID.bt.$SQL_FILENAME.tmp
set +u
. $DW_SA_TMP/$TABLE_ID.bt.$SQL_FILENAME.tmp > $DW_SA_TMP/$TABLE_ID.bt.$SQL_FILENAME.tmp.2
set -u
mv $DW_SA_TMP/$TABLE_ID.bt.$SQL_FILENAME.tmp.2 $DW_SA_TMP/$TABLE_ID.bt.$SQL_FILENAME.tmp

#--- Add SQL file size validation ---
if [ ! -s $DW_SA_TMP/$TABLE_ID.bt.$SQL_FILENAME.tmp ]
then
   print " SQL File is empty : $DW_SA_TMP/$TABLE_ID.bt.$SQL_FILENAME.tmp "
   exit 1
fi

RUN_SQL_LOGFILE="$DW_SA_LOG"'/'"$TABLE_ID"'.bt.'"${SQL_FILENAME%.sql}"'.'"$FILE_DATETIME"'.log'

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
