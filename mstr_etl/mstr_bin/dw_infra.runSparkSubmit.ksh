#!/bin/ksh -eu
# Title:        Run Spark Submit
# File Name:    dw_infra.runSparkSubmit.ksh
# Description:  Submit Spark Submit Command
# Developer:    Ryan Wong
# Created on:
# Location:     $DW_MASTER_BIN
# Logic:
#
#
# Called by:    UC4/Unix
#
# Date           Ver#   Modified By(Name)            Change and Reason for Change
#-----------    -----  ---------------------------  ---------------------------------------------------------------------------------------------------------------
# 2017-08-21       .1   Ryan Wong                    Initial
# 2017-09-21       .2   Pramit Mitra                 ADPO-976, Combine spark-defaults.conf from SPARK_HOME when running job on different clusters
# 2017-10-11       .3   Pramit Mitra                 DINT-1018, Conditional Copy logic added to ensure no copy is attempted when user specify ALL spark properties
# 2017-10-06       .4   Pramit Mitra                 Implemented SetFacl (File Mast update) logic for HDFS files on Storage cluster
# 2017-10-23       .5   Pramit Mitra                 SA_DIR evaluation logic implemented as prerequisite for SetFacl implementation
# 2018-04-10       .6   Pramit Mitra                 DINT-1356 - Introducing two new variables: 1. HDFS_BASE_PATH and 2. PARTITION_VALUE to construct Non-Standard HDFS 
#
#####################################################################################################################################################################

ETL_ID=$1
JOB_ENV=$2
SEQ_YN=$3

if [ $# -ne 3 ]
then
   print "Usage:  $0 <ETL_ID> <JOB_ENV> <SEQ_YN>\n  SEQ_YN: is 1 for seq file or 0 for single sql file\n"
   exit 2
fi

. $DW_MASTER_LIB/dw_etl_common_functions.lib

# Login into hadoop
. $DW_MASTER_CFG/hadoop.login

export SA_DIR=`echo ${ETL_ID} | awk -F'.' '{ print $1; }'`

JAVA=$JAVA_HOME/bin/java
JAVA_CMD_OPT=`bash /dw/etl/mstr_lib/hadoop_ext/hadoop.setup`
RUN_CLASS=${MAIN_CLASS:-"NA"}

# Adding SA_DIR evaluation logic from ETL_ID as this is required for setfacl implementation
export SA_DIR=`echo ${ETL_ID} | awk -F'.' '{ print $1; }'`

################################################################################
# Grooming SQL
################################################################################
set -e
SEQ_FILE_NAME=${ETL_ID}_${SPARK_CONF_SUFF}.sql.seq
print "Value of SEQ_FILE_NAME = "${SEQ_FILE_NAME}

# Cleanse SEQ File
SEQ_FILE_TMP=${DW_SA_TMP}/tmp_${SEQ_FILE_NAME}
SEQ_FILE_TMP2=${DW_SA_TMP}/tmp_${SEQ_FILE_NAME}2

if [[ $SEQ_YN == 0 ]]
then
  print $SQL_FILE > $SEQ_FILE_TMP
  print "Using SQL File:  $SQL_FILE"
else
  cp ${DW_SQL}/${SEQ_FILE_NAME} $SEQ_FILE_TMP
  print "Using SEQ File:  $SEQ_FILE_NAME"
fi

# Trim front and trailing whitespace
sed -e 's/^[ \t]*//' $SEQ_FILE_TMP > $SEQ_FILE_TMP2
sed -e 's/[ \t]*$//' $SEQ_FILE_TMP2 > $SEQ_FILE_TMP
# Remove empty lines and any starting with hash
sed -e '/^$/d' $SEQ_FILE_TMP > $SEQ_FILE_TMP2
sed -e '/^#/d' $SEQ_FILE_TMP2 > $SEQ_FILE_TMP

> ${DW_SA_TMP}/${ETL_ID}_SQLFileList.lst.tmp1
> ${DW_SA_TMP}/${ETL_ID}_SQLFileList_withPath.lst.tmp1
   while read p; do
       if [ ! -f ${DW_SQL}/$p ]
       then
         print "INFRA_ERROR:  SQL file does not exist:  ${DW_SQL}/$p"
         exit 4
       fi
       echo $p
       print "cat <<EOF" > ${DW_SA_TMP}/tmp_$p
       cat ${DW_SQL}/$p >> ${DW_SA_TMP}/tmp_$p
       print "\nEOF" >> ${DW_SA_TMP}/tmp_$p
       echo tmp_$p >> ${DW_SA_TMP}/${ETL_ID}_SQLFileList.lst.tmp1
       echo ${DW_SA_TMP}/tmp_$p >> ${DW_SA_TMP}/${ETL_ID}_SQLFileList_withPath.lst.tmp1
       chmod +x ${DW_SA_TMP}/tmp_$p
       set +u
       . ${DW_SA_TMP}/tmp_$p > ${DW_SA_TMP}/tmp1_$p
       awk 'NR > 1{print t} {t = $0}END{if (NF) print }' ${DW_SA_TMP}/tmp1_$p > ${DW_SA_TMP}/tmp2_$p
       set -u
       mv ${DW_SA_TMP}/tmp2_$p ${DW_SA_TMP}/tmp_$p
    done < ${SEQ_FILE_TMP}

## Logic to transpose rows into column ##
cat ${DW_SA_TMP}/${ETL_ID}_SQLFileList.lst.tmp1 | tr '\n' ',' > ${DW_SA_TMP}/${ETL_ID}_SQLFileList.lst.tmp2
cat ${DW_SA_TMP}/${ETL_ID}_SQLFileList_withPath.lst.tmp1 | tr '\n' ',' > ${DW_SA_TMP}/${ETL_ID}_SQLFileList_withPath.lst.tmp2

## Logic to remove additional comma from the end of files, from previous steps ##
sed 's/,$//' ${DW_SA_TMP}/${ETL_ID}_SQLFileList.lst.tmp2 > ${DW_SA_TMP}/${ETL_ID}_SQLFileList.lst.tmp
sed 's/,$//' ${DW_SA_TMP}/${ETL_ID}_SQLFileList_withPath.lst.tmp2 > ${DW_SA_TMP}/${ETL_ID}_SQLFileList_withPath.lst.tmp

export SPARK_SQL_LST1=`cat ${DW_SA_TMP}/${ETL_ID}_SQLFileList.lst.tmp`
export SPARK_SQL_LST_PATH=`cat ${DW_SA_TMP}/${ETL_ID}_SQLFileList_withPath.lst.tmp`


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
export SPARK_CONF=${ETL_ID}_${SPARK_CONF_SUFF}.cfg
export SPARK_CONF_DEFAULT=${DW_MASTER_CFG}/zeta_default.conf
export SPARK_CONF_DYNAMIC=${DW_SA_TMP}/${SPARK_CONF}.tmp

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
#    echo $T2 >> ${TMP3_SPARK_CONF_CLUSTER}
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


################################################################################
# Submit job into hadoop cluster
################################################################################
print "Call Spark Submit Module"

print "Spark Submit Issued for :::::: ${ETL_ID}" > ${PARENT_LOG_FILE%.log}.spark_submit_statement.log
print "${SPARK_HOME}/bin/spark-submit --class com.ebay.dss.zeta.ZetaDriver --jars ${DW_LIB}/avro-1.8.2.jar --files "$DW_EXE/hmc/adpo_load_cfg/aes.properties,${SPARK_HOME}/conf/log4j.properties,${HIVE_HOME}/conf/hive-site.xml,${SPARK_SQL_LST_PATH}" --conf spark.executor.extraClassPath=avro-1.8.2.jar --driver-class-path avro-1.8.2.jar:${DW_LIB}/zeta-driver-0.0.1-SNAPSHOT-jar-with-dependencies.jar:${SPARK_HOME}/jars/datanucleus-rdbms-3.2.9.jar:${SPARK_HOME}/jars/datanucleus-api-jdo-3.2.6.jar:${SPARK_HOME}/jars/datanucleus-core-3.2.10.jar --properties-file ${SPARK_CONF_DYNAMIC} --conf spark.yarn.access.namenodes=${SPARK_FS} ${DW_LIB}/zeta-driver-0.0.1-SNAPSHOT-jar-with-dependencies.jar  sql -s "${SPARK_SQL_LST1}"" >> ${PARENT_LOG_FILE%.log}.spark_submit_statement.log

export SPARK_SUBMIT_OPTS="-Dlogback.configurationFile=file://${SPARK_HOME}/conf/logback.xml"
set +e
${SPARK_HOME}/bin/spark-submit --class com.ebay.dss.zeta.ZetaDriver --jars ${DW_LIB}/avro-1.8.2.jar --files "$DW_EXE/hmc/adpo_load_cfg/aes.properties,${SPARK_HOME}/log4j.properties,${HIVE_HOME}/conf/hive-site.xml,${SPARK_SQL_LST_PATH}" --conf spark.executor.extraClassPath=avro-1.8.2.jar --driver-class-path avro-1.8.2.jar:${DW_LIB}/zeta-driver-0.0.1-SNAPSHOT-jar-with-dependencies.jar:${SPARK_HOME}/jars/datanucleus-rdbms-3.2.9.jar:${SPARK_HOME}/jars/datanucleus-api-jdo-3.2.6.jar:${SPARK_HOME}/jars/datanucleus-core-3.2.10.jar --properties-file ${SPARK_CONF_DYNAMIC} --conf spark.yarn.access.namenodes=${SPARK_FS} ${DW_LIB}/zeta-driver-0.0.1-SNAPSHOT-jar-with-dependencies.jar  sql -s "${SPARK_SQL_LST1}"
rcode=$?
set -e

 if [ $rcode != 0 ]
    then
    print "Inside Error Handler"
    print "Value of Return Code ="$rcode
    exit 4
 else
   print "Spark-SQL Submit process complete"
fi

################################################################################
# umask / setfacl update logic on storage hadoop cluser
################################################################################

  set +e
  print "Inside hdfsFileMaskUpdate Block"
  #export SA_DIR_HDFS=`echo ${SA_DIR} | awk -F'_' '{ print $2; }'`
  export SA_DIR_HDFS=`echo ${SA_DIR#*_}`
  export HADOOP_PROXY_USER=${HD_USERNAME}

  #DINT-1356 - Introducing two new variables: 1. HDFS_BASE_PATH and 2. PARTITION_VALUE to construct Non-Standard HDFS directory structure. 3rd variable STM_MERGE_TABLE_ID has already been evaluated outer scope and defaulted as TABLE_ID if missing.
  assignTagValue HDFS_BASE_PATH HDFS_BASE_PATH $ETL_CFG_FILE W /sys/edw/gdw_tables
  assignTagValue PARTITION_VALUE PARTITION_VALUE $ETL_CFG_FILE W snapshot/dt

  ##Considering 8 character UOW values
  export UOW_TO_STM=`echo ${UOW_TO_DATE} | cut -c1-8`
  export UOW_FROM_STM=`echo ${UOW_FROM_DATE} | cut -c1-8`
  #export HDFS_PATH_TO=/sys/edw/gdw_tables/${SA_DIR_HDFS}/${TABLE_ID}/snapshot/${PARTITION_NAME}=${UOW_TO_STM}
  #export HDFS_PATH_FROM=/sys/edw/gdw_tables/${SA_DIR_HDFS}/${TABLE_ID}/snapshot/${PARTITION_NAME}=${UOW_FROM_STM}
  export HDFS_PATH_TO=${HDFS_BASE_PATH}/${SA_DIR_HDFS}/${TABLE_ID}/${PARTITION_VALUE}=${UOW_TO_STM}
  export HDFS_PATH_FROM=${HDFS_BASE_PATH}/${SA_DIR_HDFS}/${TABLE_ID}/${PARTITION_VALUE}=${UOW_FROM_STM}

  ${HADOOP_HOME2}/bin/hadoop fs -test -d ${HDFS_PATH_TO}
   val_to=$?
   echo "Return code for UOW_TO = $val_to" > ${PARENT_LOG_FILE%.log}.hdfsFileMaskUpdate.log
   echo "UOW_TO VALUE SET AS : ${HDFS_PATH_TO}" >> ${PARENT_LOG_FILE%.log}.hdfsFileMaskUpdate.log
  if [[ $val_to -eq 0 ]]
      then
      echo "HDFS Directory ${HDFS_PATH_TO} is present" >> ${PARENT_LOG_FILE%.log}.hdfsFileMaskUpdate.log
      ${HADOOP_HOME2}/bin/hadoop fs -setfacl -R -m mask::rwx ${HDFS_PATH_TO}
      echo "SelFacl applied successfully on ${HDFS_PATH_TO}" >> ${PARENT_LOG_FILE%.log}.hdfsFileMaskUpdate.log
      export rcode=0
  else
      ${HADOOP_HOME2}/bin/hadoop fs -test -d ${HDFS_PATH_FROM}
      val_from=$?
      echo "Return code for UOW_FROM = $val_from" >> ${PARENT_LOG_FILE%.log}.hdfsFileMaskUpdate.log
      echo "UOW_FROM VALUE SET AS : ${HDFS_PATH_FROM}" >> ${PARENT_LOG_FILE%.log}.hdfsFileMaskUpdate.log
  if [[ $val_from -eq 0 ]]
      then
      echo "HDFS Directory ${HDFS_PATH_FROM} is present" >> ${PARENT_LOG_FILE%.log}.hdfsFileMaskUpdate.log
      ${HADOOP_HOME2}/bin/hadoop fs -setfacl -R -m mask::rwx ${HDFS_PATH_FROM}
      echo "SelFacl applied successfully on ${HDFS_PATH_FROM}" >> ${PARENT_LOG_FILE%.log}.hdfsFileMaskUpdate.log
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
    print "hdfs fileMask Update can't be completed ... exiting process !!!!"
    print "Value of Return Code ="$rcode
    ## exit 4 is commented out to make hdfsFileMaskUpdate process Non-Fatal, as currently many ETL_ID(s) are not following directory structure standard. 
    # exit 4
 else
   print "hdfsFileMaskUpdate process complete"
fi

print "End of Spark Submit Script"
exit 0
