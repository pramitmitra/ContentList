#!/usr/bin/sh
#############
# this section contains exports ONLY for ares
#############
export ORIGPATH=$PATH
export MANPATH=$MANPATH:/usr/share/man:/usr/local/man:/usr/local/pssh:/usr/local/pssh/man
export EDITOR=vi
export PS1="[\$(date +%H:%M)]:[\u@\h:\W]\$ "
export HBASE_HOME=/apache/hadoop_client/ares/hbase
export HADOOP_HOME=/apache/hadoop_client/ares/hadoop
export HADOOP_CONF_DIR=$HADOOP_HOME/conf
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
# do his last as hadoop-env.sh can change $JAVA_HOME
export PATH=$HADOOP_HOME/bin:$HBASE_HOME/bin:$JAVA_HOME/bin:$PATH:/usr/local/sbin:/usr/local/bin:/usr/local/pssh/bin:$HOME/bin



#
# User specific aliases and functions
#
# ls
alias l='ls'
alias ls='ls -hF'
alias la='ls -Alh'              # show hidden files
alias li='ls -li'
alias lx='ls -lXBh'             # sort by extension
alias lk='ls -lSr'              # sort by size
alias lc='ls -lcrh'             # sort by change time
alias lu='ls -lurh'             # sort by access time
alias lr='ls -lRh'              # recursive ls
alias lt='ls -ltrh'             # sort by date
alias ll='ls -lsh'              #
alias lm='ls -alh |more'        # pipe through 'more'
alias lsh='ls -lt | head'
alias tree='tree -Csu'          # nice alternative to 'ls'
alias lsd='ls -lsd'
alias las='ls -lash'
# df
alias dfl='df -l'
alias dfh='df -h'
alias dfn='df -t nfs -h'
alias dfi='df -i'
# history
alias ht="history | tail"
# hadoop/hbase/etc
alias hsh="$HBASE_HOME/bin/hbase shell"
alias hfs="hadoop fs "

### FUNCTIONS

function ldl
{
    # Display directory permissions
    if test $1x = 'x'; then
        ls -l|grep ^d|more
    else
        ls -lsd $1 | more
    fi
}


function psg
{
    if [ -z "$1" ]; then
        ps auxw|more
        return 0
    fi
    ps auxw | grep $1 | grep -v grep|more
    return 0
}

function psgw
{
    if [ -z "$1" ]; then
        ps auxwwee|more
        return 0
    fi
    ps auxwwee | grep $1 | grep -v grep|more
    return 0
}

function psgmore
{
    if [ -z "$1" ]; then
        ps auxw|more
        return 0
    fi
    ps auxw | grep -v grep|more
    return 0
}

function findgrep
{ 
    ( /usr/bin/find . -exec grep "$1" {} /dev/null \; 2> /dev/null ) ;
}
