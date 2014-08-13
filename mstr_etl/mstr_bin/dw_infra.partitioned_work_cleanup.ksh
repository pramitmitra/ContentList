#!/bin/ksh -eu
#------------------------------------------------------------------------------------------------
# Filename:     dw_infra.partitioned_work_cleanup.ksh
# Description:  Deletes from partitioned Work table(s) any rows which are older than the
#               desired retention value (as specified in partitioned_work.lis file.
#
# Developer:    John Hackley
# Created on:   December 7, 2012
# Location:     $DW_MASTER_BIN/
#
# Execution:    $DW_MASTER_BIN/dw_infra.partitioned_work_cleanup.ksh <ETL_ID> <JOB_ENV>
#
# Revision History:
#
# Name             Date            Description
# ---------------  --------------  --------------------------------------------------------------
# John Hackley     Dec 7, 2012     Initial Creation
# Ryan Wong        Oct 4, 2013     Redhat changes
# Ryan Wong        Aug 11, 2014    Fix grep issue for Redhat for DB_TYPE
#------------------------------------------------------------------------------------------------

ETL_ID=$1
JOB_ENV=$2        # extract, td1, td2, td3, td4, etc... ( primary, secondary, all -- deprecated )

# Convert legacy dual values to current multi env values
case $JOB_ENV in
        all)   JOB_ENV="td1|td2";;
    primary)   JOB_ENV=td1;;
  secondary)   JOB_ENV=td2;;
esac

JOB_ENV_UPPER=$(print $JOB_ENV | tr "[:lower:]" "[:upper:]")
. $DW_MASTER_LIB/dw_etl_common_functions.lib

# determine which database we are using through the DBC file

CFG_DBC_PARAM=$(JOB_ENV_UPPER=$(print $JOB_ENV | tr [:lower:] [:upper:]); eval print ${JOB_ENV_UPPER}_DBC)
DEFAULT_DB_NAME=$(JOB_ENV_UPPER=$(print $JOB_ENV | tr [:lower:] [:upper:]); eval print teradata_\$DW_${JOB_ENV_UPPER}_DB)

set +e
DB_NAME=$(grep "^$CFG_DBC_PARAM\>" $DW_CFG/${ETL_ID}.cfg | read PARAM VALUE PARAM_COMMENT; eval print ${VALUE:-$DEFAULT_DB_NAME})
rcode=$?
set -e
if [ $rcode != 0 ]
then
   DB_NAME=$DEFAULT_DB_NAME
fi

set +e
DB_TYPE=$(grep "^dbms\>" $DW_DBC/${DB_NAME}.dbc | tr [:lower:] [:upper:] | read PARAM VALUE PARAM_COMMENT; print ${VALUE:-0})
rcode=$?
set -e

if [ $rcode != 0 ]
then
    print "${0##*/}:  ERROR, Failure determining dbms value from $DW_DBC/${DB_NAME}.dbc" >&2
    exit 4
fi

DB_NAME=$(JOB_ENV_UPPER=$(print $JOB_ENV | tr "[:lower:]" "[:upper:]"); eval print \$DW_${JOB_ENV_UPPER}_DB)

#------------------------------------------------------------------------------------------------
# exit unless loading Teradata
#------------------------------------------------------------------------------------------------

if [[ "$LOAD_PROCESS_TYPE" != "T" && ("$LOAD_PROCESS_TYPE" != "D" || "$DB_TYPE" != "TERADATA") ]]
then

  print "Not loading Teradata; skipping deletion from partitioned Work tables"
  exit 0

else

  #------------------------------------------------------------------------------------------------
  # exit if Disabled flag is set for this Teradata instance
  #------------------------------------------------------------------------------------------------

  DISABLE_PARTITION_TABLE_CLEANUP_UPPER=$(print $DISABLE_PARTITION_TABLE_CLEANUP | tr "[:lower:]" "[:upper:]")

  if [[ "$DISABLE_PARTITION_TABLE_CLEANUP_UPPER" == *"$JOB_ENV_UPPER"* ]]
  then

    print "Cleanup of partitioned Work tables disabled on $JOB_ENV; skipping deletion from partitioned Work tables"
    exit 0

  else
    #------------------------------------------------------------------------------------------------
    # exit if partitioned_work.lis file does not exist
    #------------------------------------------------------------------------------------------------

    if [ ! -f $DW_CFG/$ETL_ID.partitioned_work.lis ]
    then
      print "$ETL_ID.partitioned_work.lis not found; skipping deletion from partitioned Work tables"
      exit 0

    else

      assignTagValue STAGE_DB STAGE_DB $DW_CFG/$ETL_ID.cfg
      assignTagValue PARTITIONED_WORK_DB PARTITIONED_WORK_DB $DW_CFG/$ETL_ID.cfg

      #------------------------------------------------------------------------------------------------
      # create and execute BTEQ script containing DELETE statements to clean up partitioned work tables
      #------------------------------------------------------------------------------------------------

      export FILE_DATETIME=${CURR_DATETIME:-$(date "+%Y%m%d-%H%M%S")}
      BTEQ_LOGFILE=$DW_SA_LOG/$TABLE_ID.bt.partitioned_work_cleanup${UOW_APPEND}.$FILE_DATETIME.log

#     make sure bteq file exists and is empty
      if [ -f $DW_SA_TMP/$TABLE_ID.bt.partitioned_work_cleanup.tmp ]
      then
        print "" > $DW_SA_TMP/$TABLE_ID.bt.partitioned_work_cleanup.tmp
      else
        touch $DW_SA_TMP/$TABLE_ID.bt.partitioned_work_cleanup.tmp
      fi

      QB_TMP="SA=$SUBJECT_AREA;TBID=$TABLE_ID;SCRIPTNAME=dw_infra.partitioned_work_cleanup.ksh;RM_REL_DATE=196912310000;RM_REL_ID=NA;$QB_STR_UC4;"

#     save and change Field Separator such that "cut" will read entire record
      OLDIFS=$IFS
      IFS='
'

#     use "expand" in the next line instead of "cat".  for some reason, cat interprets a tab character as a delimiter, even though IFS is set to newline
      for LIS_RECORD in $(expand $DW_CFG/$ETL_ID.partitioned_work.lis | cut -f1)
      do

#       reset Field Separator to remove extra white space upon read
        IFS=$OLDIFS
        print $LIS_RECORD | read LIS_JOB_ENV LIS_PARTN_WORK_TABLE LIS_RETENTION

        if [ $LIS_RETENTION == "" ]
        then
#         Default value (PARTITION_TABLE_RETENTION) comes from $DW_MASTER_CFG/etlenv.setup
          LIS_RETENTION="$PARTITION_TABLE_RETENTION"
          print "No retention specified; defaulting to $PARTITION_TABLE_RETENTION days"
        fi

        LIS_JOB_ENV_UPPER=$(print $LIS_JOB_ENV | tr "[:lower:]" "[:upper:]")

#       check if JOB_ENV is in the pipe-delimited list of environments from the .lis file
        if [[ "$LIS_JOB_ENV_UPPER" == *"$JOB_ENV_UPPER"* ]]
        then
          print "DELETE FROM $PARTITIONED_WORK_DB.$LIS_PARTN_WORK_TABLE WHERE UOW_TO_DT < CAST ('"${UOW_TO_DATE}"' AS DATE FORMAT 'Y4MMDD') - $LIS_RETENTION;" >> $DW_SA_TMP/$TABLE_ID.bt.partitioned_work_cleanup.tmp
        fi

      done

#     if it's not empty, run bteq script to delete from partitioned work tables

      if [ -s $DW_SA_TMP/$TABLE_ID.bt.partitioned_work_cleanup.tmp ]
      then
        set +e
        bteq <<EOF > $BTEQ_LOGFILE
.SET ERROROUT STDOUT 
.set session transaction btet
.logon $DB_NAME/$TD_USERNAME,$TD_PASSWORD
select 'SESS', session;
SET QUERY_BAND = '$QB_TMP' UPDATE FOR SESSION;
.maxerror 1
.run file=$DW_SA_TMP/$TABLE_ID.bt.partitioned_work_cleanup.tmp;
.logoff
.exit
EOF
        rcode=$?
        set -e

        if [ rcode -ne 0 ]
        then
            print "Error executing SQL to delete from partitioned work tables"
            exit 1
        else
            print "Successfully deleted from partitioned work tables"
        fi
      else
            print "Did not find any partitioned work tables to delete from; exiting successfully"
      fi
    fi
  fi
fi

exit 0
