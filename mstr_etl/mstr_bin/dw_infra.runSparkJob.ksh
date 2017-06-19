#!/bin/ksh -eu
#------------------------------------------------------------------------------------------------
# Filename:     dw_infra.runSparkJob.ksh
#
# Revision History:
#
# Name             Date            Description
# ---------------  --------------  ---------------------------------------------------
# ???              ??/??/????      Initial Creation
# Pramit Mitra     03/01/2017      Extending the framework for Spark-Submit.
# Pramit Mitra     04/03/2017      Change Input parameter numbers and derive SQLFile and CONFFile from ETL_ID 
# Pramit Mitra     04/07/2017      Set SPARK CONF File name based on Spark Handler type
# Pramit Mitra     04/10/2017      Added logic to regenrate .sql and .seq file
# Pramit Mitra     04/24/2017      Added logic to read files from seq files based on original order
# Ryan Wong        06/02/2017      Added cleansing logic for SEQ file
# Pramit Mitra     06/09/2017      Added logback.xml to overwrite log4j scope for Spark-Submit
# Ryan Wong        06/12/2017      ADPO-141, Added dynamic gen spark conf w/default conf, yarn.queue, and app.name
# Pramit Mitra     06/15/2017      Olympus-Sub cluster logic added based on ETL_ENV
#------------------------------------------------------------------------------------------------

ETL_ID=$1
JOB_ENV=$2
BASENAME=$3

if [ $# -ge 1 ]
then
   PARAM_LIST=$*
fi

PARAM_LIST=${PARAM_LIST:-""}
PARAM_LIST=`eval echo $PARAM_LIST`

. $DW_MASTER_LIB/dw_etl_common_functions.lib

# Login into hadoop
. $DW_MASTER_CFG/hadoop.login

export SA_DIR=`echo ${ETL_ID} | awk -F'.' '{ print $1; }'`
export SQL_PATH=${DW_SQL}/${SA_DIR}
export SEQ_FILE_NAME0=${ETL_ID}
print "Value of SEQ_FILE_NAME0 = "${ETL_ID}

## Setting SPARK CONF File name based on Spark Handler type
if [[ ${BASENAME} == target_table_merge_handler ]]
 then
  export SPARK_CONF_SUFF=ttm
  export SPARK_CONF=${ETL_ID}_${SPARK_CONF_SUFF}.cfg
  export SEQ_FILE_NAME=${SEQ_FILE_NAME0}_${SPARK_CONF_SUFF}.sql.seq
  print "Value of SEQ_FILE_NAME = "${SEQ_FILE_NAME}
elif [[ ${BASENAME} == single_table_transform_handler ]]
 then
  export SPARK_CONF_SUFF=stt
  export SPARK_CONF=${ETL_ID}_${SPARK_CONF_SUFF}.cfg
  export SEQ_FILE_NAME=${SEQ_FILE_NAME0}_${SPARK_CONF_SUFF}.sql.seq
  print "Value of SEQ_FILE_NAME = "${SEQ_FILE_NAME}
fi

print "Value of SEQ_FILE_NAME = " ${SPARK_CONF_SUFF}.sql.seq

export SPARK_SQL_SEQ=${ETL_ID}.sql.seq
export SPARK_SQL_LST=`cat ${SQL_PATH}/${SPARK_SQL_SEQ}`
eval JOB_SUB_ENV='spark'

set +eu
if [[ $JOB_ENV == sp* ]]
 then
      . $DW_MASTER_CFG/.olympus_env.sh
else
  print "INFRA_ERROR: invalid JOB_ENV: $JOB_ENV for running Hadoop Jobs."
  exit 4
fi

export UC4_JOB_NAME=${UC4_JOB_NAME:-"NA"}
export UC4_PRNT_CNTR_NAME=${UC4_PRNT_CNTR_NAME:-"NA"}
export UC4_TOP_LVL_CNTR_NAME=${UC4_TOP_LVL_CNTR_NAME:-"NA"};
export UC4_JOB_RUN_ID=${UC4_JOB_RUN_ID:-"NA"}
export UC4_JOB_BATCH_MODE=${UC4_JOB_BATCH_MODE:-"NA"}
export UC4_JOB_PRIORITY=${UC4_JOB_PRIORITY:-"NA"}

JAVA=$JAVA_HOME/bin/java
JAVA_CMD_OPT=`bash /dw/etl/mstr_lib/hadoop_ext/hadoop.setup`
#RUN_SCRIPT=$HADOOP_JAR
RUN_CLASS=${MAIN_CLASS:-"NA"}
#DATAPLATFORM_ETL_INFO="ETL_ID=${ETL_ID};UC4_JOB_NAME=${UC4_JOB_NAME};UC4_PRNT_CNTR_NAME=${UC4_PRNT_CNTR_NAME};UC4_TOP_LVL_CNTR_NAME=${UC4_TOP_LVL_CNTR_NAME};UC4_JOB_RUN_ID=${UC4_JOB_RUN_ID};UC4_JOB_BATCH_MODE=${UC4_JOB_BATCH_MODE};UC4_JOB_PRIORITY=${UC4_JOB_PRIORITY};UOW_FROM=${UOW_FROM};UOW_TO=${UOW_TO};RUN_SCRIPT=${RUN_SCRIPT};RUN_CLASS=${RUN_CLASS};"
DATAPLATFORM_ETL_INFO="ETL_ID=${ETL_ID};UC4_JOB_NAME=${UC4_JOB_NAME};UC4_PRNT_CNTR_NAME=${UC4_PRNT_CNTR_NAME};UC4_TOP_LVL_CNTR_NAME=${UC4_TOP_LVL_CNTR_NAME};UC4_JOB_RUN_ID=${UC4_JOB_RUN_ID};UC4_JOB_BATCH_MODE=${UC4_JOB_BATCH_MODE};UC4_JOB_PRIORITY=${UC4_JOB_PRIORITY};"


################################################################################
function run_spark_jar
{
print "Inside run_spark_jar function" 

set +e

print "Spark Submit Issued for :::::: ${ETL_ID}" > ${PARENT_LOG_FILE%.log}.spark_submit_statement.log
print "${SPARK_HOME}/bin/spark-submit --class com.ebay.dss.zeta.ZetaDriver --files "$DW_EXE/hmc/adpo_load_cfg/aes.properties,${SPARK_HOME}/conf/log4j.properties,${HIVE_HOME}/conf/hive-site.xml,${SPARK_SQL_LST_PATH}" --driver-class-path ${SPARK_HOME}/jars/datanucleus-rdbms-3.2.9.jar:${SPARK_HOME}/jars/datanucleus-api-jdo-3.2.6.jar:${SPARK_HOME}/jars/datanucleus-core-3.2.10.jar --properties-file ${SPARK_CONF_DYNAMIC} --conf spark.yarn.access.namenodes=hdfs://${SPARK_FS} ${DW_LIB}/zeta-driver-0.0.1-SNAPSHOT-jar-with-dependencies.jar  sql -s "${SPARK_SQL_LST1}"" >> ${PARENT_LOG_FILE%.log}.spark_submit_statement.log

export SPARK_SUBMIT_OPTS="-Dlogback.configurationFile=file://${SPARK_HOME}/conf/logback.xml"

${SPARK_HOME}/bin/spark-submit --class com.ebay.dss.zeta.ZetaDriver --files "$DW_EXE/hmc/adpo_load_cfg/aes.properties,${SPARK_HOME}/log4j.properties,${HIVE_HOME}/conf/hive-site.xml,${SPARK_SQL_LST_PATH}" --driver-class-path ${SPARK_HOME}/jars/datanucleus-rdbms-3.2.9.jar:${SPARK_HOME}/jars/datanucleus-api-jdo-3.2.6.jar:${SPARK_HOME}/jars/datanucleus-core-3.2.10.jar --properties-file ${SPARK_CONF_DYNAMIC} --conf spark.yarn.access.namenodes=hdfs://${SPARK_FS} ${DW_LIB}/zeta-driver-0.0.1-SNAPSHOT-jar-with-dependencies.jar  sql -s "${SPARK_SQL_LST1}"

rcode=$?

set -e

 if [ $rcode != 0 ]
    then
    print "Inside Error Handler"  
    #print "${0##*/}:  ERROR running $BASENAME, see log file $LOG_FILE" >&2
    #exit $rcode
    print "Value of Return Code ="$rcode 
    exit 4
 else
   print "Spark-SQL Submit process complete"
fi 

}

function groomSparkSQL
{

rm -r ${DW_TMP}/${JOB_ENV}/${SA_DIR}/${ETL_ID}*tmp*;rm -r ${DW_TMP}/${JOB_ENV}/${SA_DIR}/tmp*${ETL_ID}*;

# Cleanse SEQ File
SEQ_FILE_TMP=${DW_TMP}/${JOB_ENV}/${SA_DIR}/tmp_${SEQ_FILE_NAME}
SEQ_FILE_TMP2=${DW_TMP}/${JOB_ENV}/${SA_DIR}/tmp_${SEQ_FILE_NAME}2
cp ${DW_SQL}/${SEQ_FILE_NAME} $SEQ_FILE_TMP

# Trim front and trailing whitespace
sed -e 's/^[ \t]*//' $SEQ_FILE_TMP > $SEQ_FILE_TMP2
sed -e 's/[ \t]*$//' $SEQ_FILE_TMP2 > $SEQ_FILE_TMP
# Remove empty lines and any starting with hash
sed -e '/^$/d' $SEQ_FILE_TMP > $SEQ_FILE_TMP2
sed -e '/^#/d' $SEQ_FILE_TMP2 > $SEQ_FILE_TMP

   while read p; do
       echo $p
       print "cat <<EOF" > ${DW_TMP}/${JOB_ENV}/${SA_DIR}/tmp_${p}
       cat ${DW_SQL}/$p >> ${DW_TMP}/${JOB_ENV}/${SA_DIR}/tmp_$p
       print "\nEOF" >> ${DW_TMP}/${JOB_ENV}/${SA_DIR}/tmp_$p
       echo tmp_$p >> ${DW_TMP}/${JOB_ENV}/${SA_DIR}/${ETL_ID}_SQLFileList.lst.tmp1
       echo ${DW_TMP}/${JOB_ENV}/${SA_DIR}/tmp_$p >> ${DW_TMP}/${JOB_ENV}/${SA_DIR}/${ETL_ID}_SQLFileList_withPath.lst.tmp1
       chmod +x ${DW_TMP}/${JOB_ENV}/${SA_DIR}/tmp_$p
       set +u
       . ${DW_TMP}/${JOB_ENV}/${SA_DIR}/tmp_$p > ${DW_TMP}/${JOB_ENV}/${SA_DIR}/tmp1_$p
       awk 'NR > 1{print t} {t = $0}END{if (NF) print }' ${DW_TMP}/${JOB_ENV}/${SA_DIR}/tmp1_$p > ${DW_TMP}/${JOB_ENV}/${SA_DIR}/tmp2_$p
       set -u
       mv ${DW_TMP}/${JOB_ENV}/${SA_DIR}/tmp2_$p ${DW_TMP}/${JOB_ENV}/${SA_DIR}/tmp_$p
    done < ${SEQ_FILE_TMP}

## Logic to transpose rows into column ##
cat ${DW_TMP}/${JOB_ENV}/${SA_DIR}/${ETL_ID}_SQLFileList.lst.tmp1 | tr '\n' ',' > ${DW_TMP}/${JOB_ENV}/${SA_DIR}/${ETL_ID}_SQLFileList.lst.tmp2
cat ${DW_TMP}/${JOB_ENV}/${SA_DIR}/${ETL_ID}_SQLFileList_withPath.lst.tmp1 | tr '\n' ',' > ${DW_TMP}/${JOB_ENV}/${SA_DIR}/${ETL_ID}_SQLFileList_withPath.lst.tmp2

## Logic to remove additional comma from the end of files, from previous steps ##
sed 's/,$//' ${DW_TMP}/${JOB_ENV}/${SA_DIR}/${ETL_ID}_SQLFileList.lst.tmp2 > ${DW_TMP}/${JOB_ENV}/${SA_DIR}/${ETL_ID}_SQLFileList.lst.tmp
sed 's/,$//' ${DW_TMP}/${JOB_ENV}/${SA_DIR}/${ETL_ID}_SQLFileList_withPath.lst.tmp2 > ${DW_TMP}/${JOB_ENV}/${SA_DIR}/${ETL_ID}_SQLFileList_withPath.lst.tmp

export SPARK_SQL_LST1=`cat ${DW_TMP}/${JOB_ENV}/${SA_DIR}/${ETL_ID}_SQLFileList.lst.tmp`
export SPARK_SQL_LST_PATH=`cat ${DW_TMP}/${JOB_ENV}/${SA_DIR}/${ETL_ID}_SQLFileList_withPath.lst.tmp`

}

################################################################################
# Dynamic Config Precedence Rules https://jirap.corp.ebay.com/browse/ADPO-141
# (1) Properties with highest precedence in SPARK_CONF, ETL_ID_[stt|ttm].cfg
# (2) If properties not exists, generate:
#       spark.yarn.queue=HD_QUEUE
#       spark.app.name=ETL_ID.[stt|ttm]
# (3) If properties not exists, use defaults found in SPARK_CONF_DEFAULT
################################################################################
function groomSparkConf
{

export SPARK_CONF_DEFAULT=${DW_MASTER_CFG}/zeta_default.conf
export SPARK_CONF_DYNAMIC=${DW_TMP}/${JOB_ENV}/${SA_DIR}/tmp_${SPARK_CONF}

print "Dynamic Gen Spark Conf:  Start"

if [ ! -f $SPARK_CONF_DEFAULT ]
then
   print "INFRA_ERROR:  Dynamic Gen Spark Conf:  Default Spark Conf not found.  $SPARK_CONF_DEFAULT"
   exit 4
fi

# Initialize Dynamic Conf
if [[ -f ${DW_CFG}/${SPARK_CONF} ]]
then
  print "Dynamic Gen Spark Conf:  $SPARK_CONF found.  Copy to $SPARK_CONF_DYNAMIC"
  cp $DW_CFG/$SPARK_CONF $SPARK_CONF_DYNAMIC
else
  print "Dynamic Gen Spark Conf: $SPARK_CONF not found.  Clean out $SPARK_CONF_DYNAMIC"
  > $SPARK_CONF_DYNAMIC
fi

# If not exists, add property spark.yarn.queue
YARN_QUEUE_FOUND=0
while read sc; do
  if [[ ${sc%%=*} == "spark.yarn.queue" ]]
  then
     YARN_QUEUE_FOUND=1
     break
  fi
done < $SPARK_CONF_DYNAMIC

if [[ $YARN_QUEUE_FOUND -eq 0 ]]
then
   print "Dynamic Gen Spark Conf:  Yarn Queue NOT Found.  Setting Default:  spark.yarn.queue=$HD_QUEUE"
   print "spark.yarn.queue=${HD_QUEUE}" >> $SPARK_CONF_DYNAMIC
else
   print "Dynamic Gen Spark Conf:  Yarn Queue Found.  SKIPPING"
fi

# If not exists, add property spark.app.name
APP_NAME_FOUND=0
while read sc; do
  if [[ ${sc%%=*} == "spark.app.name" ]]
  then
     APP_NAME_FOUND=1
     break
  fi
done < $SPARK_CONF_DYNAMIC

if [[ $APP_NAME_FOUND -eq 0 ]]
then
   print "Dynamic Gen Spark Conf:  App Name NOT Found.  Setting Default:  spark.app.name=${ETL_ID}.${SPARK_CONF_SUFF}"
   print "spark.app.name=${ETL_ID}.${SPARK_CONF_SUFF}" >> $SPARK_CONF_DYNAMIC
else
   print "Dynamic Gen Spark Conf:  App Name Found.  SKIPPING"
fi

# If not exists, add default properties
print "Dynamic Gen Spark Conf:  Adding Default Properties"
while read scd; do
   SCD_NAME_FOUND=0
   while read sc; do
      if [[ ${scd%%=*} == ${sc%%=*} ]]
      then
        SCD_NAME_FOUND=1
        break
      fi
   done < $SPARK_CONF_DYNAMIC

   if [[ $SCD_NAME_FOUND -eq 0 ]]
   then
     print ${scd} >> $SPARK_CONF_DYNAMIC
   fi
done < $SPARK_CONF_DEFAULT

print "Dynamic Gen Spark Conf:  Copy Conf to Log ${PARENT_LOG_FILE%.log}.spark_conf_dynamic.log"
cat $SPARK_CONF_DYNAMIC > ${PARENT_LOG_FILE%.log}.spark_conf_dynamic.log

print "Dynamic Gen Spark Conf:  Success"

}

################################################################################
# Submit job into hadoop cluster
################################################################################
if [[ $JOB_SUB_ENV == 'spark' ]]
then
print "Call Spark Submit Module"
    groomSparkSQL
    groomSparkConf
    run_spark_jar
else
  print "INFRA_ERROR: unsupported JOB_SUB_ENV: $JOB_SUB_ENV for running Hadoop Jobs."
  exit 4
fi

print "End of Script"
################################################################################
