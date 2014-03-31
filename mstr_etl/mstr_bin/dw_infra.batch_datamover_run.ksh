#!/bin/ksh -eu
###################################################################################################################
#
# Title:        DW_INFRA Batch DataMove Run 
# File Name:    dw_infra.batch_datamove_run.ksh
# Description:  Run component for (Teradata) data replication/movement across platforms
#               Called by $DW_MASTER_EXE/dw_infra.batch_datamove_handler.ksh
# Developer:    Kevin Oaks
# Created on:   2010-10-11
# Location:     $DW_MASTER_EXE
#
# Date         Ver#   Modified By(Name)            Change and Reason for Change
#---------    -----  ---------------------------  ------------------------------
# 2010-12-10   1.0    Kevin Oaks                    Initial Prod Version
# 2011-11-15   1.1    Kevin Oaks                    Added TD Login Override for option -s
# 2011-12-21   1.2    Ryan Wong                     Change loader_cleanup to use dw_infra.loader_cleanup.ksh
#                                                   Add UOW date based dir
# 2013-04-19   1.3    Ryan Wong                     Adding UNIT_OF_WORK_FILE for cleanup
#                                                   Also updating UOW directory path to include UOW HH/MM/SS
# 2013-10-04   1.4    Ryan Wong                     Redhat changes
#
###################################################################################################################
###################################################################################################################
#### Notes on usage from $DW_MASTER_EXE/dw_infra.batch_datamove_handler.ksh
#### This handler encapsulates the modules for batch level data movement/replication.
#### Currently Teradata based, but functionality may be extended in future to accomodate
#### ingest/acquistition/hadoop/etc...
####
#### Module supports UOW functionality.
####
#### When running as source, data is extracted to file. When running as target, the data that was extracted
#### to file is then loaded to the target. There must be a job plan for the source as well as one for each 
#### target. This handler self identifies whether it is running as a source or a target based on the JOB_ENV
#### provided in conjunction with the DM_(SRC|TRGT)_ENV tags present in the $ETL_ID cfg file. 
####
#### The components available are:
#### 1: Execute SQL to stage data on the source system prior to extracting for
####    replication/transformation elsewhere. - Optional (s)
#### 2: Execute Stage to Base SQL on Source - Paypal scenario is use case. - Optional (r)
#### 3: Extract from source. - Required
#### 4: Load to Target. Can be loaded directly to final target or to stage prior to
####    transforming to target. If source is UTF8 data, then data should always be staged
####    on load. Load as ascii to stage, then use the common Teradata conversion UDF for
####    converting to unicode during Stage to Base on Target phase.
####    Current load modes are Truncate-Insert and Append. These should be considered when
####    deciding how/where to load, and special care used when Truncate-Insert is the mode.
####    Note that a source may have multiple targets but a job plan will need to be created
####    for each. - Required
#### 5: Execute Stage to Base SQL on Target. The same sql executed in item 2 will be
####    executed here by default. Will provide override for this - Optional (R)
####
#### Replication graph currently supports Teradata Source/Targets only.
#### Addititional functionality may be added later to support other platforms.
####
#### Valid options (source values are lowercase, target values are uppercase):
#### s - Extract to stage on source
#### r - Execute runSQL on source ( typically a load/transform to final table )
#### R - execute runSQL on target ( typically a load/transform to final table )
####
#### f <UOW_FROM> - a UOW_FROM value generated in UC4
#### t <UOW_TO> - a UOW_TO value generated in UC4
####
###################################################################################################################
###################################################################################################################
#
# Functions:

typeset -fu processCommand

function processCommand {

_process=$1
 shift 1
_processCommand=$@
_logFile=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.$ENV_TYPE.$_process${UOW_APPEND}.$CURR_DATETIME.log

  set +e
  eval $_processCommand > $_logFile 2>&1
  _pcrcode=$?
  set -e

  if [ $_pcrcode -ne 0 ]
  then
    print "${0##*/}:  FATAL ERROR running $_processCommand. See log file $_logFile" >&2
  fi

  print $_pcrcode
}

#
###################################################################################################################

. $DW_MASTER_LIB/dw_etl_common_functions.lib
. $DW_MASTER_LIB/dw_etl_common_abinitio_functions.lib


# Source the error message handling logic.  On failure, trap will send the contents of the PARENT_ERROR_FILE to the
# subject area designated email addresses.

. $DW_MASTER_LIB/message_handler

print "Error Message Handling invoked"

# Determine if we are extracting or loading
print "Determining Execution Mode"

dwi_assignTagValue -p DM_SRC_ENV -t DM_SRC_ENV -f $ETL_CFG_FILE
dwi_assignTagValue -p DM_TRGT_ENV -t DM_TRGT_ENV -f $ETL_CFG_FILE
print "DM_SRC_ENV == $DM_SRC_ENV"
print "DM_TRGT_ENV == $DM_TRGT_ENV"

EXEC_MODE=ndef
if [ $DM_SRC_ENV == $JOB_ENV ]
then
  EXEC_MODE=E
  ENV_TYPE=src
else
  # Fill target environment array, count elements and initialize loop index to 0
  set -A DM_TRGT_ENV_ARR `print "$DM_TRGT_ENV"| awk -F',' '{for(i=1; i<=NF; i++){printf("%s ", $i)}}'`
  integer DM_TRGT_ENV_ARR_ELEMS=${#DM_TRGT_ENV_ARR[*]}
  integer idx=0

  # Make sure we have at least one array element
  if ((DM_TRGT_ENV_ARR_ELEMS == 0))
  then
    print "${0##*/}:  FATAL ERROR, invalid value for parameter DM_TRGT_ENV: ($DM_TRGT_ENV)" >&2
    exit 4
  fi

  # Cycle through target list and see if we find a match. If so, set EXEC_MODE and ENV_TYPE appropriately
  while ((idx < DM_TRGT_ENV_ARR_ELEMS))
  do
    if [ ${DM_TRGT_ENV_ARR[idx]} == $JOB_ENV ]
    then
      EXEC_MODE=L
      ENV_TYPE=trgt
      break
    fi
    ((idx+=1))
  done
fi

if [ $EXEC_MODE != ndef ]
then
  print "JOB_ENV == $JOB_ENV"
  print "EXEC_MODE == $EXEC_MODE"
  print "ENV_TYPE == $ENV_TYPE"
else
  print "${0##*/}:  FATAL ERROR, invalid JOB_ENV: $JOB_ENV. Unable to determine if source or target." >&2
  exit 4
fi

# Define Constants

BATCH_SEQ_NUM_FILE=$DW_SA_DAT/$TABLE_ID.$JOB_TYPE.$ENV_TYPE.batch_seq_num.dat
UNIT_OF_WORK_FILE=$DW_SA_DAT/$TABLE_ID.$JOB_TYPE.$ENV_TYPE.uow.dat
COMP_FILE=$DW_SA_TMP/$TABLE_ID.$JOB_TYPE_ID.$ENV_TYPE.complete
DAT_VAR_LIST=$DW_SA_DAT/$TABLE_ID.$JOB_TYPE.$ENV_TYPE.variables.lis
CFG_VAR_LIST=$DW_CFG/$ETL_ID.$JOB_TYPE.variables.lis

##################################################################################
# Define SQL Files
# EXTRACT_TO_STAGE & EXTRACT_FROM_SOURCE are static 
# LOAD_TO_BASE can differ depending on ENV_TYPE, so must perform check
##################################################################################

EXTRACT_TO_STAGE_SQL_FILENAME=$ETL_ID.$JOB_TYPE.extract_to_stage.sql
EXTRACT_FROM_SOURCE_SQL_FILENAME=$ETL_ID.$JOB_TYPE.extract.sql
dwi_assignTagValue -p USE_ENV_TYPE_LTB_SQL -t DM_USE_ENV_TYPE_LTB_SQL -f $ETL_CFG_FILE -s n -d 0

if [ $USE_ENV_TYPE_LTB_SQL -eq 1 ]
then
  LOAD_TO_BASE_SQL_FILENAME=$ETL_ID.$JOB_TYPE.$ENV_TYPE.load_to_base.sql
else
  LOAD_TO_BASE_SQL_FILENAME=$ETL_ID.$JOB_TYPE.load_to_base.sql
fi

############################################################################################################
# Instantiate any job specific variables needed for this job
# Static variables used regardless of environment will exist in $DW_CFG
# Dynamic or environment specific variables will exist in $DW_SA_DAT
# Run environment specific last so that they can override static where necessary
# Given multiple target environments, some may use environment specific variabels,
# some may not. This tag will be a list of environments that do, so we can instantiate
# only when necessary. 
############################################################################################################

print "Checking for job specific variables"

dwi_assignTagValue -p USE_CFG_VAR_LIS -t DM_USE_CFG_VAR_LIS -f $ETL_CFG_FILE -s n -d 0
dwi_assignTagValue -p USE_DAT_VAR_ENV -t DM_USE_DAT_VAR_ENV -f $ETL_CFG_FILE -s n -d ""
USE_DAT_VAR_LIS=0

print "USE_CFG_VAR_LIS == $USE_CFG_VAR_LIS"
print "USE_DAT_VAR_ENV == $USE_DAT_VAR_ENV"

if [[ -n $USE_DAT_VAR_ENV ]]
then

  # Fill environment array, count elements and initialize loop index to 0
  set -A USE_DAT_VAR_ENV_ARR `print "$USE_DAT_VAR_ENV"| awk -F',' '{for(i=1; i<=NF; i++){printf("%s ", $i)}}'`
  integer USE_DAT_VAR_ENV_ARR_ELEMS=${#USE_DAT_VAR_ENV_ARR[*]}
  integer idx=0

  # Make sure we have at least one array element
  if ((USE_DAT_VAR_ENV_ARR_ELEMS == 0))
  then
    print "${0##*/}:  FATAL ERROR, invalid value for parameter USE_DAT_VAR_ENV: ($USE_DAT_VAR_ENV)" >&2
    exit 4
  fi

  # Cycle through env list and see if we find a match. If so, set USE_DAT_VAR_LIS to 1 and break out
  while ((idx < USE_DAT_VAR_ENV_ARR_ELEMS))
  do
    if [ ${USE_DAT_VAR_ENV_ARR[idx]} == $JOB_ENV ]
    then
      USE_DAT_VAR_LIS=1
      break
    fi
    ((idx+=1))
  done
fi
print "USE_DAT_VAR_LIS == $USE_DAT_VAR_LIS"

if [ $USE_CFG_VAR_LIS -eq 1 ]
then
  print "Using $CFG_VAR_LIST to instantiate job specific variables."
  cat $CFG_VAR_LIST
  . $CFG_VAR_LIST
fi

if [ USE_DAT_VAR_LIS -eq 1 ]
then
  print "Using $DAT_VAR_LIST to instantiate job specific variables. May override values in $CFG_VAR_LIST"
  cat $DAT_VAR_LIST
  . $DAT_VAR_LIST
fi

print "Determining IS_RESTART status"

if [ ! -f $COMP_FILE ]
then
 # COMP_FILE does not exist.  1st run for this processing period.
 IS_RESTART=N
 > $COMP_FILE
else
 IS_RESTART=Y
fi

print "IS_RESTART == $IS_RESTART"

# Get batch sequence number

PREV_BATCH_SEQ_NUM=$(<$BATCH_SEQ_NUM_FILE)
((BATCH_SEQ_NUM=PREV_BATCH_SEQ_NUM+1))

print "Batch Sequence Number acquired"

# Define touchfiles.

DW_WATCHFILE=$ETL_ID.$JOB_TYPE.$ENV_TYPE.$BATCH_SEQ_NUM.done

print "Touchfiles defined"

# Print standard environment variables
set +u
print_standard_env
set -u

# Run Cleanup

PROCESS=datamove_cleanup
grcode=`grepCompFile $PROCESS $COMP_FILE`

if [ $grcode -eq 1 ]
then
  PROCESS_COMMAND="$DW_MASTER_BIN/dw_infra.loader_cleanup.ksh $JOB_ENV $JOB_TYPE_ID"
  print "Executing phase $PROCESS"

  rcode=`processCommand $PROCESS $PROCESS_COMMAND`

  if [ $rcode != 0 ]
  then
    exit $rcode 
  else
    print "$PROCESS phase complete"
    print $PROCESS >> $COMP_FILE
  fi

else
  print "$PROCESS already complete"
fi

print "
###################################################################################################################
#
# Executing Datamove Process for ETL_ID: $ETL_ID
# Execution Mode == $EXEC_MODE (E == extract, L == load)
# Job Environment == $JOB_ENV
# Source Enviroment == $DM_SRC_ENV
# Target Environemt(s) == $DM_TRGT_ENV
# UOW_FROM == $UOW_FROM
# UOW_TO == $UOW_TO
# BATCH_SEQ_NUM == $BATCH_SEQ_NUM
# SRC_STAGE_DATA == $SRC_STAGE_DATA
# SRC_LOAD_TO_BASE == $SRC_LOAD_TO_BASE
# TRGT_LOAD_TO_BASE == $TRGT_LOAD_TO_BASE
# 
###################################################################################################################
"

###################################################################################################################
# Extract from External Source to Stage on Source system
# Uses dw_infra.runTDSQL.ksh
# Optional
# Done only when EXEC_MODE == E and SRC_STAGE_DATA = 1
###################################################################################################################

if [[ $EXEC_MODE == E && $SRC_STAGE_DATA -eq 1 ]]
then
  PROCESS=extract_to_src_stage
  grcode=`grepCompFile $PROCESS $COMP_FILE`

  if [ $grcode != 0 ]
  then

    # grep CFG File for TD_LOGON override
    dwi_assignTagValue -p DM_SSD_TD_LOGON_OVERRIDE_ID -t DM_SSD_TD_LOGON_OVERRIDE_ID -f $ETL_CFG_FILE -s N -d N
    if [[ $DM_SSD_TD_LOGON_OVERRIDE_ID == N ]]
    then
      SSD_TD_LOGON_OVERRIDE_STRING=""
    else
      SSD_TD_LOGON_OVERRIDE_STRING="-l $DM_SSD_TD_LOGON_OVERRIDE_ID"
    fi

    PROCESS_COMMAND="$DW_MASTER_EXE/dw_infra.runTDSQL.ksh $ETL_ID $JOB_ENV $EXTRACT_TO_STAGE_SQL_FILENAME $SSD_TD_LOGON_OVERRIDE_STRING $UOW_PARAM_LIST"
    print "Executing $PROCESS phase"

    rcode=`processCommand $PROCESS $PROCESS_COMMAND`

    if [ $rcode != 0 ]
    then
      exit $rcode
    else
      print "$PROCESS phase complete"
      print $PROCESS >> $COMP_FILE
    fi
  else
    print "$PROCESS already complete"
  fi
fi

###################################################################################################################
# Load to Base on Originating System
# Uses dw_infra.runTDSQL.ksh
# Optional
# Done only when EXEC_MODE == E and SRC_LOAD_TO_BASE = 1
###################################################################################################################

if [[ $EXEC_MODE == E && $SRC_LOAD_TO_BASE -eq 1 ]]
then
  PROCESS=load_to_base_src
  grcode=`grepCompFile $PROCESS $COMP_FILE`

  if [ $grcode != 0 ]
  then
    PROCESS_COMMAND="$DW_MASTER_EXE/dw_infra.runTDSQL.ksh $ETL_ID $JOB_ENV $LOAD_TO_BASE_SQL_FILENAME $UOW_PARAM_LIST"
    print "Executing $PROCESS phase"

    rcode=`processCommand $PROCESS $PROCESS_COMMAND`

    if [ $rcode != 0 ]
    then
      exit $rcode
    else
      print "$PROCESS phase complete"
      print $PROCESS >> $COMP_FILE
    fi
  else
    print "$PROCESS already complete"
  fi
fi

#####################################################################
# Extract from Source/Load to Target
# Utilizes Abinitio Graph
# Graph Extracts or Loads based on value of $EXEC_MODE
# Not Optional
#####################################################################


PROCESS=datamove
grcode=`grepCompFile $PROCESS $COMP_FILE`

if [ $grcode != 0 ]
then

  print "Executing $PROCESS phase"

  ### grep for replication specific tags
  dwi_assignTagValue -p DATA_DIR -t DM_DATA_DIR -f $ETL_CFG_FILE
  dwi_assignTagValue -p DB_INTERFACE -t DM_DB_INTERFACE -f $ETL_CFG_FILE
  dwi_assignTagValue -p TRGT_TABLE -t DM_TRGT_TABLE -f $ETL_CFG_FILE
  dwi_assignTagValue -p LOAD_TYPE -t DM_LOAD_TYPE -f $ETL_CFG_FILE
  dwi_assignTagValue -p IS_UTF8_EXTRACT -t DM_USE_UTF8_EXTRACT -f $ETL_CFG_FILE -s w -d 0

  if [[ -n $UOW_TO ]]
  then
    export UOW_DATE=$(print $UOW_TO | cut -c1-8)
    if [[ ${DATA_DIR} != ${DATA_DIR%mfs*} ]]
    then
      m_mkdirifnotexist $DATA_DIR/$JOB_ENV/$SUBJECT_AREA/$TABLE_ID/$UOW_DATE/$UOW_TO_HH/$UOW_TO_MI/$UOW_TO_SS
    else
      mkdirifnotexist $DATA_DIR/$JOB_ENV/$SUBJECT_AREA/$TABLE_ID/$UOW_DATE/$UOW_TO_HH/$UOW_TO_MI/$UOW_TO_SS
    fi
  fi

  # Define DML File
  if [[ $EXEC_MODE == "E" && IS_UTF8_EXTRACT -eq 1 ]]
  then
    DML_FILE=$DW_DML/$ETL_ID.datamove.extract_utf8.dml 
  else
    DML_FILE=$DW_DML/$ETL_ID.datamove.dml
  fi

  # Define DBC File
  if [[ $EXEC_MODE == "E" && IS_UTF8_EXTRACT -eq 1 ]]
  then
    APPEND_UTF8="_utf8"
  else
    APPEND_UTF8=""
  fi

  DBC_FILE=$(JOB_ENV_UPPER=$(print $JOB_ENV | tr '[:lower:]' '[:upper:]'); eval print teradata_\$DW_${JOB_ENV_UPPER}_DB\${APPEND_UTF8}.dbc)

  print "
########################################################################################
# DATA_DIR == $DATA_DIR
# DB_INTERFACE == $DB_INTERFACE
# IS_UTF8_EXTRACT == $IS_UTF8_EXTRACT
# TRGT_TABLE == $TRGT_TABLE
# LOAD_TYPE == $LOAD_TYPE
# DML_FILE == $DML_FILE
# DBC_FILE == $DBC_FILE
########################################################################################
"

  PROCESS_COMMAND="$DW_MASTER_EXE/dw_infra.batch_teradata_datamover.ksh -ETL_ID $ETL_ID -JOB_ENV $JOB_ENV -EXEC_MODE $EXEC_MODE -DATA_DIR $DATA_DIR -DB_INTERFACE $DB_INTERFACE -AB_IDB_SRC_DBC $DBC_FILE -AB_IDB_TRGT_DBC $DBC_FILE -TRGT_TABLE $TRGT_TABLE -ETL_CFG_FILE $ETL_CFG_FILE -DML_FILE $DML_FILE -LOAD_TYPE $LOAD_TYPE -BATCH_SEQ_NUM $BATCH_SEQ_NUM $UOW_PARAM_LIST_AB"

  rcode=`processCommand $PROCESS $PROCESS_COMMAND`

  if [ $rcode != 0 ]
  then
    exit $rcode
  else
    print "$PROCESS phase complete"
    print $PROCESS >> $COMP_FILE
  fi
else
  print "$PROCESS already complete"
fi

###################################################################################################################
# Load to Base on Target
# Uses dw_infra.runTDSQL.ksh
# Optional
# Done only when EXEC_MODE == L and TRGT_LOAD_TO_BASE = 1
###################################################################################################################

if [[ $EXEC_MODE == L && $TRGT_LOAD_TO_BASE -eq 1 ]]
then
  PROCESS=load_to_base_trgt
  grcode=`grepCompFile $PROCESS $COMP_FILE`

  if [ $grcode != 0 ]
  then
    PROCESS_COMMAND="$DW_MASTER_EXE/dw_infra.runTDSQL.ksh $ETL_ID $JOB_ENV $LOAD_TO_BASE_SQL_FILENAME $UOW_PARAM_LIST"
    print "Executing $PROCESS phase"

    rcode=`processCommand $PROCESS $PROCESS_COMMAND`

    if [ $rcode != 0 ]
    then
      exit $rcode
    else
      print "$PROCESS phase complete"
      print $PROCESS >> $COMP_FILE
    fi
  else
    print "$PROCESS already complete"
  fi
fi

# Advance State and clean up as needed

PROCESS=update_state
grcode=`grepCompFile $PROCESS $COMP_FILE`

if [ $grcode != 0 ]
then
  print "Executing $PROCESS phase"

  print "Updating $JOB_ENV $ENV_TYPE BATCH_SEQ_NUM to $BATCH_SEQ_NUM"
  print $BATCH_SEQ_NUM > $BATCH_SEQ_NUM_FILE
  if [[ "X$UOW_TO" != "X" ]]
  then
    print "Updating $JOB_ENV $ENV_TYPE UNIT_OF_WORK to $UOW_TO `date`"
    print $UOW_TO > $UNIT_OF_WORK_FILE
  fi

  print "$PROCESS phase complete"
  print $PROCESS >> $COMP_FILE
else
   print "$PROCESS already complete"
fi

# Touch Watchfile
PROCESS=touch_watch
grcode=`grepCompFile $PROCESS $COMP_FILE`

if [ $grcode != 0 ]
then
  PROCESS_COMMAND="$DW_MASTER_EXE/touchWatchFile.ksh $ETL_ID $JOB_TYPE $JOB_ENV $DW_WATCHFILE $UOW_PARAM_LIST"
  print "Executing $PROCESS phase"

  rcode=`processCommand $PROCESS $PROCESS_COMMAND`

  if [ $rcode != 0 ]
  then
    exit $rcode
  else
    print "$PROCESS phase complete"
    print $PROCESS >> $COMP_FILE
  fi
else
  print "$PROCESS already complete"
fi

print "Removing the complete file"
rm -f $COMP_FILE


print "
###################################################################################################################
#
# Completed Execution of Datamove Process for ETL_ID: $ETL_ID
# Execution Mode == $EXEC_MODE (E == extract, L == load)
# Job Environment == $JOB_ENV
# Source Enviroment == $DM_SRC_ENV
# Target Environemt(s) == $DM_TRGT_ENV
# UOW_FROM == $UOW_FROM
# UOW_TO == $UOW_TO
# BATCH_SEQ_NUM == $BATCH_SEQ_NUM
#
###################################################################################################################
"
tcode=0
exit
