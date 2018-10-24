#############
# this section contains exports ONLY for Hercules-Sub
#############
export SPARK_FS=$(eval print \$${JOB_ENV_UPPER}_STORAGE_NN_URL)
export ORIGPATH=$PATH
export MANPATH=${MANPATH:-""}:/usr/share/man:/usr/local/man:/usr/local/pssh:/usr/local/pssh/man
export EDITOR=vi
export PS1="[\$(date +%H:%M)]:[\u@\h:\W]\$ "
export HADOOP_HOME=/apache/hadoop_client/herculesqa_prev/hadoop
#export HADOOP_HOME2=/apache/hadoop_client/${STORAGE_ENV}/hadoop
export HADOOP_HOME2=/apache/hadoop_client/herculesqa_prev/hadoop
export HIVE_HOME=/apache/hadoop_client/herculesqa_prev/hive
#export SPARK_HOME=/apache/hadoop_client/${COMPUTE_ENV}/spark
export SPARK_HOME=/apache/hadoop_client/herculesqa_prev/spark
export PARTITION_NAME=dt
export HADOOP_CONF_DIR=$HADOOP_HOME2/conf
export HIVE_CONF_DIR=$HIVE_HOME/conf
#export HADOOP_PID_DIR=$HADOOP_HOME/pids
#export HADOOP_LOG_DIR=$HADOOP_HOME/logs
#export JAVA_HOME=/usr/java/latest
# grab our hadoop exports
if [ -f "$HADOOP_CONF_DIR/hadoop-env.sh" ]; then
   . "$HADOOP_CONF_DIR/hadoop-env.sh"
fi
HIVE_ENV_FILE="$HIVE_CONF_DIR/hive-env.sh"
if [ -f "$HIVE_ENV_FILE" ]; then
    source "$HIVE_ENV_FILE"
fi
# do his last as hadoop-env.sh can change $JAVA_HOME
export PATH=$HADOOP_HOME/bin:$HBASE_HOME/bin:$HIVE_HOME/bin:$JAVA_HOME/bin:$PATH:/usr/local/sbin:/usr/local/bin:/usr/local/pssh/bin:$HOME/bin

# Move all NN URLs to etlenv.<env>.teradata_target_variables.lis
export HADOOP_NN_URL=$(eval print \$${JOB_ENV_UPPER}_NN_URL)
export HADOOP_CLI_HOST="hercules-lvs-cli-1.vip.ebay.com"

# For spark-submit log enhancements https://jirap.corp.ebay.com/browse/ADPO-138
export SPARK_DEFAULT_FS=$SPARK_FS
export HADOOP_HISTORY_LOG="https://hercules-sub-lvs-rm-2.vip.ebay.com:50070/cluster/apps/FINISHED"
export ADPO_DEBUG_WIKI_LOG="https://wiki.vip.corp.ebay.com/display/DataServicesandSolutions/ADPO+-+Debug+Steps+for+Spark+Job"
