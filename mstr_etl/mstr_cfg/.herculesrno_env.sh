#############
# this section contains exports ONLY for Olympus
#############
export SPARK_FS=$(eval print \$${JOB_ENV_UPPER}_STORAGE_NN_URL)
export ORIGPATH=$PATH
export MANPATH=${MANPATH:-""}:/usr/share/man:/usr/local/man:/usr/local/pssh:/usr/local/pssh/man
export EDITOR=vi
export PS1="[\$(date +%H:%M)]:[\u@\h:\W]\$ "
export HADOOP_HOME=/apache/hadoop_client/herculesrno/hadoop
export HADOOP_HOME2=/apache/hadoop_client/herculesrno/hadoop
export HIVE_HOME=/apache/hadoop_client/herculesrno/hive
export SPARK_HOME=/apache/hadoop_client/herculesrno/spark
export PARTITION_NAME=dt
export HADOOP_CONF_DIR=$HADOOP_HOME2/conf
export HIVE_CONF_DIR=$HIVE_HOME/conf
#export HADOOP_PID_DIR=$HADOOP_HOME/pids
#export HADOOP_LOG_DIR=$HADOOP_HOME/logs
#export TEZ_HOME=/apache/hadoop_client/herculesrno/tez
#export TEZ_CONF_DIR=$TEZ_HOME/conf
#export TEZ_JARS=$TEZ_HOME/*:$TEZ_HOME/lib/*
#export JAVA_HOME=/usr/java/latest
#export HISTSIZE=99999
#export HISTFILESIZE=99999
#export HISTTIMEFORMAT="%Y-%m-%d %T -> "
#export HISTCONTROL=erasedups
#export HISTIGNORE="&:ls:l:[bf]g:exit"
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
export HADOOP_CLI_HOST="hercules-cli001-537017.stratus.rno.ebay.com"

# For spark-submit log enhancements https://jirap.corp.ebay.com/browse/ADPO-138
export SPARK_DEFAULT_FS=$SPARK_FS
export HADOOP_HISTORY_LOG="https://hercules-rno-shs-1.vip.hadoop.ebay.com:8080"
export ADPO_DEBUG_WIKI_LOG="https://wiki.vip.corp.ebay.com/display/DataServicesandSolutions/ADPO+-+Debug+Steps+for+Spark+Job"
