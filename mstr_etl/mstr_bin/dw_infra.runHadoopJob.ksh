#!/bin/ksh -eu
#------------------------------------------------------------------------------------------------
# Filename:     dw_infra.runHadoopJob.ksh
#
# Revision History:
#
# Name             Date            Description
# ---------------  --------------  ---------------------------------------------------
# ???              ??/??/????      Initial Creation
# Ryan Wong        10/04/2013      Redhat changes
# Ryan Wong        11/21/2013      Update hd login method, consolidate to use dw_adm
# Ryan Wong        05/02/2014      Add cfg option to turn off braceexpand and glob - NO_BRACEEXPAND_NO_GLOB
#                                    Apply only to PARAM_LIST
# Ryan Wong        08/12/2014      Set NO_BRACEEXPAND_NO_GLOB default=1 for consistency
# Jiankang Liu     04/29/2015      Exit with the real job code to avoid override
# Jiankang Liu     06/11/2015      Fix the escape back slash bug of PARAM_LIST
# Michael Weng     04/21/2016      Rename and refactor to support JOB_SUB_ENV for variable hadoop jobs
# Michael Weng     09/09/2016      Enable use of batch account keytab
# Ryan Wong        09/16/2016      Adding Queryband name-value-pairs UC4_JOB_BATCH_MODE and UC4_JOB_PRIORITY
# Michael Weng     10/12/2016      Add hadoop authentication
# Pramit Mitra     04/17/2017      Commenting out proxy hive user configuration
#------------------------------------------------------------------------------------------------

ETL_ID=$1
JOB_ENV=$2
HADOOP_JAR=$3
shift 3

if ! [[ $JOB_ENV == hd* ]]
then
  print "INFRA_ERROR: invalid JOB_ENV: $JOB_ENV for running Hadoop Jobs."
  exit 4
fi

if [ $# -ge 1 ]
then
  PARAM_LIST=$*
fi

PARAM_LIST=${PARAM_LIST:-""}
PARAM_LIST=`eval echo $PARAM_LIST`

. $DW_MASTER_LIB/dw_etl_common_functions.lib

# Login into hadoop
. $DW_MASTER_CFG/hadoop.login


export UC4_JOB_NAME=${UC4_JOB_NAME:-"NA"}
export UC4_PRNT_CNTR_NAME=${UC4_PRNT_CNTR_NAME:-"NA"}
export UC4_TOP_LVL_CNTR_NAME=${UC4_TOP_LVL_CNTR_NAME:-"NA"};
export UC4_JOB_RUN_ID=${UC4_JOB_RUN_ID:-"NA"}
export UC4_JOB_BATCH_MODE=${UC4_JOB_BATCH_MODE:-"NA"}
export UC4_JOB_PRIORITY=${UC4_JOB_PRIORITY:-"NA"}

JAVA=$JAVA_HOME/bin/java
JAVA_CMD_OPT=`bash /dw/etl/mstr_lib/hadoop_ext/hadoop.setup`
RUN_SCRIPT=$HADOOP_JAR
RUN_CLASS=${MAIN_CLASS:-"NA"}
DATAPLATFORM_ETL_INFO="ETL_ID=${ETL_ID};UC4_JOB_NAME=${UC4_JOB_NAME};UC4_PRNT_CNTR_NAME=${UC4_PRNT_CNTR_NAME};UC4_TOP_LVL_CNTR_NAME=${UC4_TOP_LVL_CNTR_NAME};UC4_JOB_RUN_ID=${UC4_JOB_RUN_ID};UC4_JOB_BATCH_MODE=${UC4_JOB_BATCH_MODE};UC4_JOB_PRIORITY=${UC4_JOB_PRIORITY};UOW_FROM=${UOW_FROM};UOW_TO=${UOW_TO};RUN_SCRIPT=${RUN_SCRIPT};RUN_CLASS=${RUN_CLASS};"

# if there is too much parameters need to be passed to hadoop jar, using a parameters.lis
dwi_assignTagValue -p USE_JAR_PARAM_LIS -t USE_JAR_PARAM_LIS -f $ETL_CFG_FILE -s N -d 0

# Option to turn off braceexpand and turn off glob
dwi_assignTagValue -p NO_BRACEEXPAND_NO_GLOB -t NO_BRACEEXPAND_NO_GLOB -f $ETL_CFG_FILE -s N -d 1

if [[ $USE_JAR_PARAM_LIS -eq 1 ]]
then
  if [[ $NO_BRACEEXPAND_NO_GLOB -eq 1 ]]
  then
    set +o braceexpand
    set +o glob
  fi
  PARAM_LIS_TMP=`eval print -- $(<$DW_CFG/$ETL_ID.param.lis)`
  PARAM_LIST="$PARAM_LIST $PARAM_LIS_TMP"
  if [[ $NO_BRACEEXPAND_NO_GLOB -eq 1 ]]
  then
    set -o braceexpand
    set -o glob
  fi
fi

################################################################################
# Function to submit hive job. Parameter: hive, tez, beeline, ......
function run_hive_job
{
  HIVE_JOB=$1
  print "Submitting HIVE job to Cluster"

  if [[ -n $PARAM_LIST ]]
  then
    for param in $PARAM_LIST
    do
      if [ ${param%=*} = $param ]
      then
        print "${0##*/}: ERROR, parameter definition $param is not of form <PARAM_NAME=PARAM_VALUE>"
        exit 4
      else
        print "Exporting $param"
        export $param
      fi
    done
  fi

  # To be compatible with previous folder structure
  if [ ! -f "$DW_HQL/$HADOOP_JAR" ]; then
    DW_HQL=$DW_HOME/hql/
    if [ ! -f "$DW_HQL/$HADOOP_JAR" ]; then
      print "${0##*/}: ERROR, file $DW_HQL/$HADOOP_JAR cannot be found"
      exit 4
    fi
  fi

  print "cat <<EOF" > $DW_SA_TMP/$TABLE_ID.ht.$HADOOP_JAR.tmp
  cat $DW_HQL/$HADOOP_JAR >> $DW_SA_TMP/$TABLE_ID.ht.$HADOOP_JAR.tmp
  print "\nEOF" >> $DW_SA_TMP/$TABLE_ID.ht.$HADOOP_JAR.tmp
  chmod +x $DW_SA_TMP/$TABLE_ID.ht.$HADOOP_JAR.tmp
  set +u
  . $DW_SA_TMP/$TABLE_ID.ht.$HADOOP_JAR.tmp >> $DW_SA_TMP/$TABLE_ID.ht.$HADOOP_JAR.tmp.2
  set -u
  mv $DW_SA_TMP/$TABLE_ID.ht.$HADOOP_JAR.tmp.2 $DW_SA_TMP/$TABLE_ID.ht.$HADOOP_JAR.tmp

  # hive sql through hive cli
  if [[ $HIVE_JOB == hive ]]
  then
#    if ! [[ $(whoami) == @(sg_adm|dw_adm) ]]
#    then
      $HIVE_HOME/bin/hive --hiveconf mapred.job.queue.name=$HD_QUEUE \
                          --hiveconf dataplatform.etl.info="$DATAPLATFORM_ETL_INFO" \
                          -f $DW_SA_TMP/$TABLE_ID.ht.$HADOOP_JAR.tmp
#    else
#      CLASSPATH=`$HADOOP_HOME/bin/hadoop classpath`
#      CLASSPATH=${CLASSPATH}:$DW_MASTER_LIB/hadoop_ext/DataplatformETLHandlerUtil.jar
#      for jar_file in $HIVE_HOME/lib/*.jar
#      do
#        CLASSPATH=$CLASSPATH:$jar_file
#      done
#      CLASSPATH=$CLASSPATH:$HIVE_HOME/conf
#      HIVE_CLI_JAR=`ls $HIVE_HOME/lib/hive-cli-*.jar`

#      exec "$JAVA" -Dproc_jar $JAVA_CMD_OPT -classpath "$CLASSPATH" \
#                   DataplatformRunJar sg_adm ~/.keytabs/apd.sg_adm.keytab $HD_USERNAME \
#                   $HIVE_CLI_JAR org.apache.hadoop.hive.cli.CliDriver \
#                   --hiveconf mapred.job.queue.name=$HD_QUEUE \
#                   --hiveconf dataplatform.etl.info="$DATAPLATFORM_ETL_INFO" \
#                   -f $DW_SA_TMP/$TABLE_ID.ht.$HADOOP_JAR.tmp
#    fi
    retcode=$?

#  # hive sql through tez execution engine
#  elif [[ $HIVE_JOB == tez ]]
#  then
#    # TO TO BE ADDED
#    retcode=$?

#  # hive sql through beeline
#  elif [[ $HIVE_JOB == beeline ]]
#  then
#    $HIVE_HOME/bin/beeline -u "$HS2_DB_URL; \
#                               principal=$HS2_PRINCIPAL; \
#                               hive.server2.proxy.user=$HD_USERNAME?tez.queue.name=$HD_QUEUE;" \
#                           --hiveconf dataplatform.etl.info="$DATAPLATFORM_ETL_INFO" \
#                           -f $DW_SA_TMP/$TABLE_ID.ht.$HADOOP_JAR.tmp
#    retcode=$?

  else
    print "INFRA ERROR: $HIVE_JOB is not yet implemented."
    exit 4
  fi

  exit $retcode
}

################################################################################
# Function to submit user jar execution
function run_hadoop_jar
{
  dwi_assignTagValue -p MAPRED_OUTPUT_COMPRESS -t MAPRED_OUTPUT_COMPRESS -f $ETL_CFG_FILE -s N -d 0

  # To be compatible with previous folder structure
  if [ ! -f "$DW_JAR/$HADOOP_JAR" ]; then
    DW_JAR=$DW_HOME/jar/
    if [ ! -f "$DW_JAR/$HADOOP_JAR" ]; then
      print "${0##*/}: ERROR, file $DW_JAR/$HADOOP_JAR cannot be found"
      exit 4
    fi
  fi

  if [[ $MAPRED_OUTPUT_COMPRESS -eq 0 ]]
  then
    MAPRED_OUTPUT_COMPRESS_IND=false
  else
    MAPRED_OUTPUT_COMPRESS_IND=true
  fi

  if [[ $NO_BRACEEXPAND_NO_GLOB -eq 1 ]]
  then
    set +o braceexpand
    set +o glob
  fi

  CMD_STR="$HADOOP_HOME/bin/hadoop jar $DW_JAR/$HADOOP_JAR $MAIN_CLASS \
                              -Dmapred.job.queue.name=$HD_QUEUE -Dmapred.output.compress=$MAPRED_OUTPUT_COMPRESS_IND \
                              -Ddataplatform.etl.info=\"$DATAPLATFORM_ETL_INFO\" \
                              $PARAM_LIST"
  print $CMD_STR
  eval $CMD_STR
  retcode=$?

  if [[ $NO_BRACEEXPAND_NO_GLOB -eq 1 ]]
  then
    set -o braceexpand
    set -o glob
  fi

  exit $retcode
}

################################################################################
# Submit job into hadoop cluster
################################################################################
if [[ "X$JOB_SUB_ENV" == "X" ]]
then
  # legacy support: no specify on jar or hql
  print "INFRA WARNING: JOB_SUB_ENV is not set. Default to jar or hive."
  print "Deprecated soon. Please specify JOB_SUB_ENV from calling script: "
  print "     hive    - submit hive sql through hive cli"
#  print "     beeline - submit hive sql through hive beeline"
#  print "     tez     - submit hive sql through tez execution engine"
  print "     jar     - submit user jar execution"

  JOB_EXT=${HADOOP_JAR##*.}
  if [[ $JOB_EXT == "hql" ]]
  then
    run_hive_job hive
  else
    run_hadoop_jar
  fi

elif [[ $JOB_SUB_ENV == hive ]]
then
  # submit hive query through hive cli
  run_hive_job hive

#elif [[ $JOB_SUB_ENV == tez ]]
#then
#  # submit hive query through tez execution engine
#  run_hive_job tez
#
#elif [[ $JOB_SUB_ENV == beeline ]]
#then
#  # submit hive query through beeline
#  run_hive_job beeline

elif [[ $JOB_SUB_ENV == jar ]]
then
  # submit user jar for execution
  run_hadoop_jar

else
  print "INFRA_ERROR: unsupported JOB_SUB_ENV: $JOB_SUB_ENV for running Hadoop Jobs."
  exit 4
fi

################################################################################
