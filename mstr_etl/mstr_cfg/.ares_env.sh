#############
# this section contains exports ONLY for ares
#############
export ORIGPATH=$PATH
export MANPATH=$MANPATH:/usr/share/man:/usr/local/man:/usr/local/pssh:/usr/local/pssh/man
export EDITOR=vi
export PS1="[\$(date +%H:%M)]:[\u@\h:\W]\$ "
export HBASE_HOME=/apache/hadoop_client/ares/hbase
export HADOOP_HOME=/apache/hadoop_client/ares/hadoop
export HIVE_HOME=/apache/hadoop_client/ares/hive
export HADOOP_CONF_DIR=$HADOOP_HOME/conf
export HIVE_CONF_DIR=$HIVE_HOME/conf
export HADOOP_PID_DIR=$HADOOP_HOME/pids
export HADOOP_LOG_DIR=$HADOOP_HOME/logs
export JAVA_HOME=/usr/java/latest
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
export HADOOP_NN_URL=$HD1_NN_URL
export HADOOP_CLI_HOST="ares-devour.vip.ebay.com"

# HiveServer2
export HS2_DB_URL="jdbc:hive2://lvsaishdc3hn0002.lvs.ebay.com:10000/"
export HS2_PRINCIPAL="hadoop/lvsaishdc3hn0002-be.lvs.ebay.com@APD.EBAY.COM"
