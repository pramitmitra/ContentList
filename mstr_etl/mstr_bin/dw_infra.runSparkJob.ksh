#!/bin/ksh -eu
#------------------------------------------------------------------------------------------------
# Filename:     dw_infra.runSparkJob.ksh
#
# Revision History:
#
# Name             Date            Description
# ---------------  --------------  ------------------------------------------------------------------------------------------------------------------------
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
# Ryan Wong        06/22/2017      ADPO-204, Add error checking for not exist SQL file
# Pramit Mitra     07/26/2017      Implemented SetFacl (File Mast update) logic for HDFS files on Ares/AresQA
# Pramit Mitra     08/25/2017      Added Avro Jar dependency into Spark-Submit
# Pramit Mitra     09/07/2017      Data Lineage Log generated per DSS request
# Pramit Mitra     09/20/2017      ADPO-976, Combine spark-defaults.conf from SPARK_HOME when running job on different clusters
# Pramit Mitra     10/11/2017      DINT-1018, Conditional Copy logic added to ensure no copy is attempted when user specify ALL spark properties
# Michael Weng     05/04/2018      DINT-1448 - ETL handler is looking for log4j.properties on wrong location
# Michale Weng     07/11/2018      UC4 variable binding
# Michael Weng     07/17/2018      Support different version of Zeta Driver
# Michael Weng     08/09/2018      Enable custom zeta_default.conf to be used
# Pramit Mitra     10/01/2018      DINT-1676, Temp SQL file cleanup process enhancement to support multiple ETL_ID concurrent execution with subset names
# Pramit Mitra     10/07/2018      Multiple Version Spark Execution Support
# Michael Weng     10/16/2018      Fix issue on multi-version Spark support
#-------------------------------------------------------------------------------------------------------------------------------------------------------------

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
export TABLE_ID=`echo ${ETL_ID} | awk -F'.' '{ print $2; }'`
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
eval JOB_SUB_ENV='spark'

export UC4_JOB_NAME=${UC4_JOB_NAME:-"NA"}
export UC4_PRNT_CNTR_NAME=${UC4_PRNT_CNTR_NAME:-"NA"}
export UC4_TOP_LVL_CNTR_NAME=${UC4_TOP_LVL_CNTR_NAME:-"NA"};
export UC4_JOB_RUN_ID=${UC4_JOB_RUN_ID:-"NA"}
export UC4_JOB_BATCH_MODE=${UC4_JOB_BATCH_MODE:-"NA"}
export UC4_JOB_PRIORITY=${UC4_JOB_PRIORITY:-"NA"}
export UC4_INFO_STR="{\"UC4_JOB_NAME\": \"${UC4_JOB_NAME}\",\"UC4_PRNT_CNTR_NAME\": \"${UC4_PRNT_CNTR_NAME}\",\"UC4_TOP_LVL_CNTR_NAME\": \"${UC4_TOP_LVL_CNTR_NAME}\",\"UC4_JOB_RUN_ID\": \"${UC4_JOB_RUN_ID}\",\"UC4_JOB_BATCH_MODE\": \"${UC4_JOB_BATCH_MODE}\",\"UC4_JOB_PRIORITY\": \"${UC4_JOB_PRIORITY}\"}"

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

if [[ -n ${spark_version:-""} ]] && [[ ${spark_version} == @(next|prev) ]] && [[ -f $DW_MASTER_CFG/.${HD_CLUSTER}_${spark_version}_env.sh ]]
then
  print "INFO: Using Spark version: $spark_version ..."
  set +eu
  . $DW_MASTER_CFG/.${HD_CLUSTER}_${spark_version}_env.sh
  export ZETA_DRIVER_JAR=${ZETA_DRIVER_JAR:-zeta-driver-${spark_version}.jar}
else
  print "INFO: Using default Spark version ..."
  export ZETA_DRIVER_JAR=${ZETA_DRIVER_JAR:-zeta-driver.jar}
fi

export DATANUCLEUS_API_JDO_JAR=$(ls $SPARK_HOME/jars/datanucleus-api-jdo-*.jar)
export DATANUCLEUS_CORE_JAR=$(ls $SPARK_HOME/jars/datanucleus-core-*.jar)
export DATANUCLEUS_RDBMS_JAR=$(ls $SPARK_HOME/jars/datanucleus-rdbms-*.jar)
export AVRO_JAR=${AVRO_JAR:-avro-1.8.2.jar}

print "Spark Submit Issued for :::::: ${ETL_ID}" > ${PARENT_LOG_FILE%.log}.spark_submit_statement.log
print "${SPARK_HOME}/bin/spark-submit --conf spark.uc4.info=\"${UC4_INFO_STR}\" --class com.ebay.dss.zeta.ZetaDriver --jars ${DW_LIB}/${AVRO_JAR} --files "$DW_EXE/hmc/adpo_load_cfg/aes.properties,${SPARK_HOME}/conf/log4j.properties,${HIVE_HOME}/conf/hive-site.xml,${SPARK_SQL_LST_PATH}" --conf spark.executor.extraClassPath=${AVRO_JAR} --driver-class-path ${AVRO_JAR}:${DW_LIB}/${ZETA_DRIVER_JAR}:${DATANUCLEUS_RDBMS_JAR}:${DATANUCLEUS_API_JDO_JAR}:${DATANUCLEUS_CORE_JAR} --properties-file ${SPARK_CONF_DYNAMIC} --conf spark.yarn.access.namenodes=${SPARK_FS} ${DW_LIB}/${ZETA_DRIVER_JAR}  sql -s "${SPARK_SQL_LST1}"" >> ${PARENT_LOG_FILE%.log}.spark_submit_statement.log

export SPARK_SUBMIT_OPTS="-Dlogback.configurationFile=file://${SPARK_HOME}/conf/logback.xml"

${SPARK_HOME}/bin/spark-submit --conf spark.uc4.info="${UC4_INFO_STR}" --class com.ebay.dss.zeta.ZetaDriver --jars ${DW_LIB}/${AVRO_JAR} --files "$DW_EXE/hmc/adpo_load_cfg/aes.properties,${SPARK_HOME}/conf/log4j.properties,${HIVE_HOME}/conf/hive-site.xml,${SPARK_SQL_LST_PATH}" --conf spark.executor.extraClassPath=${AVRO_JAR} --driver-class-path ${AVRO_JAR}:${DW_LIB}/${ZETA_DRIVER_JAR}:${DATANUCLEUS_RDBMS_JAR}:${DATANUCLEUS_API_JDO_JAR}:${DATANUCLEUS_CORE_JAR} --properties-file ${SPARK_CONF_DYNAMIC} --conf spark.yarn.access.namenodes=${SPARK_FS} ${DW_LIB}/${ZETA_DRIVER_JAR}  sql -s "${SPARK_SQL_LST1}"

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
#DINT-1676, Temp SQL file cleanup process enhancement to support multiple ETL_ID concurrent execution with subset names
#rm -r ${DW_TMP}/${JOB_ENV}/${SA_DIR}/${ETL_ID}*tmp*;rm -r ${DW_TMP}/${JOB_ENV}/${SA_DIR}/tmp*${ETL_ID}*;

rm -r ${DW_TMP}/${JOB_ENV}/${SA_DIR}/tmp_${ETL_ID}_stt.sql.seq*;
rm -r ${DW_TMP}/${JOB_ENV}/${SA_DIR}/${ETL_ID}_SQLFileList*;
rm -r ${DW_TMP}/${JOB_ENV}/${SA_DIR}/tmp_${ETL_ID}.spark_sql;
rm -r ${DW_TMP}/${JOB_ENV}/${SA_DIR}/tmp1_${ETL_ID}.spark_sql;

set -e
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
       if [ ! -f ${DW_SQL}/$p ]
       then
         print "INFRA_ERROR:  SQL file does not exist:  ${DW_SQL}/$p"
         exit 4
       fi
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

################################################################################
#  Data Lineage related addition log generation
###############################################################################
export HOSTNAME=`hostname`
print "#  Data Lineage related details for ETL_ID : ${ETL_ID}" > ${PARENT_LOG_FILE%.log}.data_lineage.log
echo "UC4_JOB_NAME : ${UC4_JOB_NAME}" >> ${PARENT_LOG_FILE%.log}.data_lineage.log
echo "HD_USERNAME : ${HD_USERNAME}" >> ${PARENT_LOG_FILE%.log}.data_lineage.log
echo "HD_QUEUE : ${HD_QUEUE}" >> ${PARENT_LOG_FILE%.log}.data_lineage.log
echo "HD_DOMAIN: ${HD_DOMAIN}" >> ${PARENT_LOG_FILE%.log}.data_lineage.log
echo "Batch account submitting this SQL : ${HD_USERNAME}" >> ${PARENT_LOG_FILE%.log}.data_lineage.log
echo "ETL Hostname : ${HOSTNAME}" >> ${PARENT_LOG_FILE%.log}.data_lineage.log
echo "Job Execution Time: ${CURR_DATETIME}" >> ${PARENT_LOG_FILE%.log}.data_lineage.log
print "\n\n" >> ${PARENT_LOG_FILE%.log}.data_lineage.log

for SQLSCRIPT in $(echo $SPARK_SQL_LST1 | sed "s/,/ /g")
  do
   echo "SQL File Content of :${SQLSCRIPT}" | sed "s/tmp_//" >> ${PARENT_LOG_FILE%.log}.data_lineage.log
   cat  ${DW_TMP}/${JOB_ENV}/${SA_DIR}/${SQLSCRIPT} >> ${PARENT_LOG_FILE%.log}.data_lineage.log
   print "\n " >> ${PARENT_LOG_FILE%.log}.data_lineage.log
done

}

################################################################################
# Dynamic Config Precedence Rules https://jirap.corp.ebay.com/browse/ADPO-141
# (1) Properties with highest precedence in SPARK_CONF, ETL_ID_[stt|ttm].cfg
# (2) If properties not exists, generate:
#       spark.yarn.queue=HD_QUEUE
#       spark.app.name=ETL_ID.[stt|ttm]
# (3) If properties not exists, use defaults found in SPARK_CONF_DEFAULT
################################################################################

################################################################################
#         JIRA # https://jirap.corp.ebay.com/browse/DINT-976
# Adding logic to Combine spark-defaults.conf from SPARK_HOME when running job on different clusters
# It's an additional step after "SPARK_CONF_DYNAMIC" file is generated by framework.
# The process will look for additional parameters from ${SPARK_HOME}/conf/spark-defaults.conf and append only
# additional values into SPARK_CONF_DYNAMIC file.
#################################################################################
function groomSparkConf
{
set -e
export SPARK_CONF_DEFAULT=${SPARK_CONF_DEFAULT:-${DW_MASTER_CFG}/zeta_default.conf}
export SPARK_CONF_DYNAMIC=${DW_TMP}/${JOB_ENV}/${SA_DIR}/tmp_${SPARK_CONF}

## Added as part of DINT-976 on 09/20/2017 by pmitra
export SPARK_CONF_CLUSTER=${SPARK_HOME}/conf/spark-defaults.conf
export tmp_read=${DW_TMP}/${JOB_ENV}/${SA_DIR}/tmp_read_${ETL_ID}
export TMP_SPARK_CONF_CLUSTER=${DW_TMP}/${JOB_ENV}/${SA_DIR}/TMP_SPARK_CONF_CLUSTER_${ETL_ID}
export TMP2_SPARK_CONF_CLUSTER=${DW_TMP}/${JOB_ENV}/${SA_DIR}/TMP2_SPARK_CONF_CLUSTER_${ETL_ID}
export TMP3_SPARK_CONF_CLUSTER=${DW_TMP}/${JOB_ENV}/${SA_DIR}/TMP3_SPARK_CONF_CLUSTER_${ETL_ID}
export TMP4_SPARK_CONF_CLUSTER=${DW_TMP}/${JOB_ENV}/${SA_DIR}/TMP4_SPARK_CONF_CLUSTER_${ETL_ID}

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

## Additional logic added by pmitra on 09/20/2017 as part of JIRA #DINT-976

# Remove empty lines and any starting with hash
sed -e '/^$/d' $SPARK_CONF_CLUSTER > $TMP_SPARK_CONF_CLUSTER
sed -e '/^#/d' $TMP_SPARK_CONF_CLUSTER > $TMP2_SPARK_CONF_CLUSTER

rm -f ${TMP3_SPARK_CONF_CLUSTER} ${TMP4_SPARK_CONF_CLUSTER}

 while read T2; do
    echo $T2 > ${tmp_read}
    read name value < ${tmp_read}
    echo $name"="$value>>${TMP3_SPARK_CONF_CLUSTER}
 done < ${TMP2_SPARK_CONF_CLUSTER}

echo "End of Spark Conf file modification Process"

print "Starting Dynamic Generation of conf : Adding Cluster Default Properties"


while read T3; do
  T3_NAME_FOUND=0
    while read scd1; do
      if [[ ${T3%%=*} == ${scd1%%=*} ]]
      then
      T3_NAME_FOUND=1
      break
      fi
    done < $SPARK_CONF_DYNAMIC

    if [[ $T3_NAME_FOUND -eq 0 ]]
      then
      print ${T3} >> ${TMP4_SPARK_CONF_CLUSTER}
    fi
done < $TMP3_SPARK_CONF_CLUSTER

# DINT-1018 : Adding conditional copy logic. Ignore file copy is user has already specified all properties at ETL_ID level
if [[ -f ${TMP4_SPARK_CONF_CLUSTER} ]]
then
  print "Appending the content generated from cluster specific spark-default.conf into user specific config"
  cat ${TMP4_SPARK_CONF_CLUSTER} >> $SPARK_CONF_DYNAMIC
else
  print "User has specified all parameters, present on cluster specific config file : Nothing to copy"
fi


print "Dynamic Gen Spark Conf:  Copy Conf to Log ${PARENT_LOG_FILE%.log}.spark_conf_dynamic.log"
cat $SPARK_CONF_DYNAMIC > ${PARENT_LOG_FILE%.log}.spark_conf_dynamic.log

print "Dynamic Gen Spark Conf:  Success"

}

function hdfsFileMaskUpdate
{
  set +e
  print "Inside hdfsFileMaskUpdate function"
  export SA_DIR_HDFS=`echo ${SA_DIR} | awk -F'_' '{ print $2; }'`
  #export HADOOP_PROXY_USER=${HD_USERNAME}
  export HDFS_PATH_TO=/sys/edw/gdw_tables/${SA_DIR_HDFS}/${SA_DIR}/snapshot/${PARTITION_NAME}=${UOW_TO_DATE}
  export HDFS_PATH_FROM=/sys/edw/gdw_tables/${SA_DIR_HDFS}/${SA_DIR}/snapshot/${PARTITION_NAME}=${UOW_FROM_DATE}

  ${HADOOP_HOME2}/bin/hadoop fs -ls hdfs://${HDFS_NN}/${HDFS_PATH_FROM}
  val_from=$?
  echo "Return code for UOW_FROM = $val_from" >> ${PARENT_LOG_FILE%.log}.hdfsFileMaskUpdate.log
  if [[ $val_from -eq 0 ]]
      then
      echo "HDFS Directory ${HDFS_PATH_FROM} is present" >> ${PARENT_LOG_FILE%.log}.hdfsFileMaskUpdate.log
      ${HADOOP_HOME2}/bin/hadoop fs -setfacl -R -m mask::rwx hdfs://${HDFS_NN}/${HDFS_PATH_FROM}
      echo "SelFacl applied successfully on ${HDFS_PATH_FROM}" >> ${PARENT_LOG_FILE%.log}.hdfsFileMaskUpdate.log
      export rcode=0
  else
      ${HADOOP_HOME2}/bin/hadoop fs -ls hdfs://${HDFS_NN}/${HDFS_PATH_TO}
      val_to=$?
      echo "Return code for UOW_TO = $val_to" >> ${PARENT_LOG_FILE%.log}.hdfsFileMaskUpdate.log
  if [[ $val_to -eq 0 ]]
      then
      echo "HDFS Directory ${HDFS_PATH_TO} is present" >> ${PARENT_LOG_FILE%.log}.hdfsFileMaskUpdate.log
      ${HADOOP_HOME2}/bin/hadoop fs -setfacl -R -m mask::rwx hdfs://${HDFS_NN}/${HDFS_PATH_TO}
      echo "SelFacl applied successfully on ${HDFS_PATH_TO}" >> ${PARENT_LOG_FILE%.log}.hdfsFileMaskUpdate.log
      export rcode=0
      echo "Value of rcode = $rcode" >>${PARENT_LOG_FILE%.log}.hdfsFileMaskUpdate.log
  else
     echo "HDFS Directory doen't exist" >> ${PARENT_LOG_FILE%.log}.hdfsFileMaskUpdate.log
     export rcode=1
     echo "Value of rcode = $rcode" >>${PARENT_LOG_FILE%.log}.hdfsFileMaskUpdate.log
  fi
fi
  set -e


if [ $rcode != 0 ]
   then
    print "Inside Error Handler"
    print "Value of Return Code ="$rcode
    exit 4
 else
   print "hdfsFileMaskUpdate process complete"
fi

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
#    hdfsFileMaskUpdate
else
  print "INFRA_ERROR: unsupported JOB_SUB_ENV: $JOB_SUB_ENV for running Hadoop Jobs."
  exit 4
fi

print "End of Script"
exit 0
################################################################################
