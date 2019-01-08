#############
# this section contains exports ONLY for Apollo RNO
#############
export SPARK_FS=$(eval print \$${JOB_ENV_UPPER}_STORAGE_NN_URL)
export ORIGPATH=$PATH
export MANPATH=$MANPATH:/usr/share/man:/usr/local/man:/usr/local/pssh:/usr/local/pssh/man
export EDITOR=vi
export PS1="[\$(date +%H:%M)]:[\u@\h:\W]\$ "
export SPARK_HOME=/apache/hadoop_client/apollorno_prev/spark
export HBASE_HOME=/apache/hadoop_client/apollorno_prev/hbase
export HADOOP_HOME=/apache/hadoop_client/apollorno_prev/hadoop
export HADOOP_HOME2=$HADOOP_HOME
export HIVE_HOME=/apache/hadoop_client/apollorno_prev/hive
export HADOOP_CONF_DIR=$HADOOP_HOME/conf
export HIVE_CONF_DIR=$HIVE_HOME/conf
export HADOOP_PID_DIR=$HADOOP_HOME/pids
export HADOOP_LOG_DIR=$HADOOP_HOME/logs
#export JAVA_HOME=${JAVA_HOME:-/usr/java/latest}
export HISTSIZE=99999
export HISTFILESIZE=99999
export HISTTIMEFORMAT="%Y-%m-%d %T -> "
export HISTCONTROL=erasedups
export HISTIGNORE="&:ls:l:[bf]g:exit"
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
export HADOOP_CLI_HOST="apollodevour001-357981.stratus.rno.ebay.com"

# HiveServer2
export HS2_DB_URL="jdbc:hive2://rnohdc42en0005.rno.ebay.com:10000/"
export HS2_PRINCIPAL="hadoop/rnohdc42en0005.rno.ebay.com@PROD.EBAY.COM"

# For spark-submit log enhancements
export SPARK_DEFAULT_FS=$SPARK_FS
export HADOOP_HISTORY_LOG="https://apollo-rno-shs-1.vip.hadoop.ebay.com:8080"
export ADPO_DEBUG_WIKI_LOG="https://wiki.vip.corp.ebay.com/display/DataServicesandSolutions/ADPO+-+Debug+Steps+for+Spark+Job"
