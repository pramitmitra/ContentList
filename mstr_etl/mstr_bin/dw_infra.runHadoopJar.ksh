#!/bin/ksh -eu
#------------------------------------------------------------------------------------------------
# Filename:     dw_infra.runHadoopJar.ksh
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
# Yiming Meng      11/26/2015      Bring Hive back
# Yiming Meng      12/23/2015      Enable Hive on Tez, Hive Vectorization and Hive CBO
#------------------------------------------------------------------------------------------------

ETL_ID=$1
JOB_ENV=$2
HADOOP_JAR=$3
shift 3

if [ $# -ge 1 ]
then
PARAM_LIST=$*
fi

PARAM_LIST=${PARAM_LIST:-""}
PARAM_LIST=`eval echo $PARAM_LIST`

. $DW_MASTER_LIB/dw_etl_common_functions.lib

# Check if HD_USERNAME has been configured
if [[ -z $HD_USERNAME ]]
  then
    print "INFRA_ERROR: can't not deterine batch account the connect hadoop cluster"
    exit 4
fi

export UC4_JOB_NAME=${UC4_JOB_NAME:-"NA"}
export UC4_PRNT_CNTR_NAME=${UC4_PRNT_CNTR_NAME:-"NA"}
export UC4_TOP_LVL_CNTR_NAME=${UC4_TOP_LVL_CNTR_NAME:-"NA"};
export UC4_JOB_RUN_ID=${UC4_JOB_RUN_ID:-"NA"}

JAVA=$JAVA_HOME/bin/java
JAVA_CMD_OPT=`bash /dw/etl/mstr_lib/hadoop_ext/hadoop.setup`
RUN_SCRIPT=$HADOOP_JAR
RUN_CLASS=${MAIN_CLASS:-"NA"}
DATAPLATFORM_ETL_INFO="ETL_ID=${ETL_ID};UC4_JOB_NAME=${UC4_JOB_NAME};UC4_PRNT_CNTR_NAME=${UC4_PRNT_CNTR_NAME};UC4_TOP_LVL_CNTR_NAME=${UC4_TOP_LVL_CNTR_NAME};UC4_JOB_RUN_ID=${UC4_JOB_RUN_ID};UOW_FROM=${UOW_FROM};UOW_TO=${UOW_TO};RUN_SCRIPT=${RUN_SCRIPT};RUN_CLASS=${RUN_CLASS};"

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

CURR_USER=`whoami`

JOB_EXT=${HADOOP_JAR##*.}
  
if [[ $JOB_EXT == "hql" ]]
then
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
  # TO be compatible with previous folder structure
  if [ ! -f "$DW_HQL/$HADOOP_JAR" ]; then
    DW_HQL=$DW_HOME/hql
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
  
  dwi_assignTagValue -p RUN_HIVE_ON_TEZ -t RUN_HIVE_ON_TEZ -f $ETL_CFG_FILE -s N -d 0
  dwi_assignTagValue -p ENABLE_HIVE_VECTORIZATION -t ENABLE_HIVE_VECTORIZATION -f $ETL_CFG_FILE -s N -d 0
  dwi_assignTagValue -p ENABLE_HIVE_CBO -t ENABLE_HIVE_CBO -f $ETL_CFG_FILE -s N -d 0
  HIVE_COMMON_CONFS=""
  if [[ $RUN_HIVE_ON_TEZ -eq 0 ]]; then
    print "Run Hive with MR"
    HIVE_COMMON_CONFS="--hiveconf mapred.job.queue.name=$HD_QUEUE"
  else
    print "Run Hive with Tez"
    HIVE_COMMON_CONFS="--hiveconf hive.execution.engine=tez --hiveconf tez.queue.name=$HD_QUEUE"
  fi

  if [[ $ENABLE_HIVE_VECTORIZATION -ne 0 ]]; then
    print "Enable Hive Vectorization"
    HIVE_COMMON_CONFS="$HIVE_COMMON_CONFS --hiveconf hive.vectorized.execution.enabled=true"
    HIVE_COMMON_CONFS="$HIVE_COMMON_CONFS --hiveconf hive.vectorized.execution.reduce.enabled=true"
  fi

  if [[ $ENABLE_HIVE_CBO -ne 0 ]]; then
    print "Enable Hive Cost-based Optimization"
    HIVE_COMMON_CONFS="$HIVE_COMMON_CONFS --hiveconf hive.cbo.enable=true"
    HIVE_COMMON_CONFS="$HIVE_COMMON_CONFS --hiveconf hive.compute.query.using.stats=true"
    HIVE_COMMON_CONFS="$HIVE_COMMON_CONFS --hiveconf hive.stats.fetch.column.stats=true"
    HIVE_COMMON_CONFS="$HIVE_COMMON_CONFS --hiveconf hive.stats.fetch.partition.stats=true"
  fi

  HIVE_COMMON_CONFS="$HIVE_COMMON_CONFS --hiveconf dataplatform.etl.info=\"$DATAPLATFORM_ETL_INFO\""
  
  if [[ $CURR_USER == $HD_USERNAME ]]
    then
      print "$HIVE_HOME/bin/hive $HIVE_COMMON_CONFS -f $DW_SA_TMP/$TABLE_ID.ht.$HADOOP_JAR.tmp"
      $HIVE_HOME/bin/hive $HIVE_COMMON_CONFS \
                            -f $DW_SA_TMP/$TABLE_ID.ht.$HADOOP_JAR.tmp
  else
    CLASSPATH=`$HADOOP_HOME/bin/hadoop classpath`
    CLASSPATH=${CLASSPATH}:$DW_MASTER_LIB/hadoop_ext/DataplatformETLHandlerUtil.jar
    for jar_file in $HIVE_HOME/lib/*.jar
      do
        CLASSPATH=$CLASSPATH:$jar_file
      done
    CLASSPATH=$CLASSPATH:$HIVE_HOME/conf
    HIVE_CLI_JAR=`ls $HIVE_HOME/lib/hive-cli-*.jar`
    
    if [[ $RUN_HIVE_ON_TEZ -ne 0 ]]; then
      for TEZ_JAR_FILE in $TEZ_HOME/*.jar
      do
        CLASSPATH=$CLASSPATH:$TEZ_JAR_FILE
      done
      for TEZ_JAR_FILE in $TEZ_HOME/lib/*.jar
      do
        CLASSPATH=$CLASSPATH:$TEZ_JAR_FILE
      done
      CLASSPATH=$CLASSPATH:$TEZ_CONF_DIR
    fi

    print "exec \"$JAVA\" -Dproc_jar $JAVA_CMD_OPT -classpath \"$CLASSPATH\" \
                 DataplatformRunJar sg_adm ~dw_adm/.keytabs/apd.sg_adm.keytab $HD_USERNAME \
                 $HIVE_CLI_JAR org.apache.hadoop.hive.cli.CliDriver \
                 $HIVE_COMMON_CONFS \
                 -f $DW_SA_TMP/$TABLE_ID.ht.$HADOOP_JAR.tmp"
    exec "$JAVA" -Dproc_jar $JAVA_CMD_OPT -classpath "$CLASSPATH" \
                 DataplatformRunJar sg_adm ~dw_adm/.keytabs/apd.sg_adm.keytab $HD_USERNAME \
                 $HIVE_CLI_JAR org.apache.hadoop.hive.cli.CliDriver \
                 $HIVE_COMMON_CONFS \
                 -f $DW_SA_TMP/$TABLE_ID.ht.$HADOOP_JAR.tmp
    retcode=$?
  fi
else
  dwi_assignTagValue -p MAPRED_OUTPUT_COMPRESS -t MAPRED_OUTPUT_COMPRESS -f $ETL_CFG_FILE -s N -d 0
  if [[ $MAPRED_OUTPUT_COMPRESS -eq 0 ]]
  then
     MAPRED_OUTPUT_COMPRESS_IND=false
  else
     MAPRED_OUTPUT_COMPRESS_IND=true
  fi 

  if [[ $CURR_USER == $HD_USERNAME ]]
  then
    if [[ $NO_BRACEEXPAND_NO_GLOB -eq 1 ]]
    then
      set +o braceexpand
      set +o glob
    fi
    $HADOOP_HOME/bin/hadoop jar $DW_JAR/$HADOOP_JAR $MAIN_CLASS \
                                -Dmapred.job.queue.name=$HD_QUEUE -Dmapred.output.compress=$MAPRED_OUTPUT_COMPRESS_IND \
                                -Ddataplatform.etl.info="$DATAPLATFORM_ETL_INFO" \
                                $PARAM_LIST
    retcode=$?
    if [[ $NO_BRACEEXPAND_NO_GLOB -eq 1 ]]
    then
      set -o braceexpand
      set -o glob
    fi

  else
  
    CLASSPATH=`$HADOOP_HOME/bin/hadoop classpath`
    CLASSPATH=${CLASSPATH}:$DW_MASTER_LIB/hadoop_ext/DataplatformETLHandlerUtil.jar

    if [[ $NO_BRACEEXPAND_NO_GLOB -eq 1 ]]
    then
      set +o braceexpand
      set +o glob
    fi
    # TO be compatible with previous folder structure
    if [ ! -f "$DW_JAR/$HADOOP_JAR" ]; then
      DW_JAR=$DW_HOME/jar
      if [ ! -f "$DW_JAR/$HADOOP_JAR" ]; then
        print "${0##*/}: ERROR, file $DW_JAR/$HADOOP_JAR cannot be found"
        exit 4
      fi
    fi

    CMD_STR="$JAVA -Dproc_jar $JAVA_CMD_OPT -classpath $CLASSPATH \
        DataplatformRunJar sg_adm ~dw_adm/.keytabs/apd.sg_adm.keytab $HD_USERNAME \
        $DW_JAR/$HADOOP_JAR $MAIN_CLASS \
        -Dmapred.job.queue.name=$HD_QUEUE \
        -Dmapred.output.compress=$MAPRED_OUTPUT_COMPRESS_IND \
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
  fi
  exit $retcode
fi

