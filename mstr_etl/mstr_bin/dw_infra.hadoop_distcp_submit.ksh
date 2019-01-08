#!/bin/ksh -eu
# Title:        Hadoop Distcp Submit
# File Name:    dw_infra.hadoop_distcp_submit.ksh
# Description:  Submit a distcp
# Developer:    Ryan Wong
# Created on:   2018-11-29
# Location:     $DW_MASTER_BIN
# Logic:
#
#
# Called by:    UC4/Unix
#
# Date           Ver#   Modified By(Name)            Change and Reason for Change
#-----------    -----  ---------------------------  ---------------------------------------------------------
#
# 2018-11-30      1.0   Ryan Wong                    Initial
# 2018-12-05      1.1   Ryan Wong                    Adding options
# 2018-12-10      1.2   Ryan Wong                    Fixing hadoop home for target
# 2018-12-11      1.3   Ryan Wong                    Adding logic to make target temp dir before copy
###################################################################################################################

. /dw/etl/mstr_cfg/etlenv.setup


# Check Options
HD_QUEUE=${HD_QUEUE_OPTION:-$HD_QUEUE}
MAP_CNT=${MAPPER_CNT_OPTION:-100}
SUBMIT_HD_ENV=${SUBMIT_HD_ENV:-${TARGET_HD_ENV}}

SOURCE_HD_ENV_UPPER=$(print $SOURCE_HD_ENV | tr '[[:lower:]]' '[[:upper:]]')
TARGET_HD_ENV_UPPER=$(print $TARGET_HD_ENV | tr '[[:lower:]]' '[[:upper:]]')
SOURCE_HD_CLUSTER=$(eval print "\${DW_${SOURCE_HD_ENV_UPPER}_DB}")
TARGET_HD_CLUSTER=$(eval print "\${DW_${TARGET_HD_ENV_UPPER}_DB}")
SOURCE_HD_NN_URL=$(eval print "\${${SOURCE_HD_ENV_UPPER}_NN_URL}")
TARGET_HD_NN_URL=$(eval print "\${${TARGET_HD_ENV_UPPER}_NN_URL}")
SOURCE_HD_PATH=$SOURCE_HD_NN_URL/$SOURCE_HD_PATH
TARGET_HD_PATH=$TARGET_HD_NN_URL/$TARGET_HD_PATH
SOURCE_HD_NN=$(print $SOURCE_HD_NN_URL|sed 's#hdfs://##' |awk -F':' '{print $1}')
TARGET_HD_NN=$(print $TARGET_HD_NN_URL|sed 's#hdfs://##' |awk -F':' '{print $1}')

SUBMIT_HD_ENV_UPPER=$(print $SUBMIT_HD_ENV | tr '[[:lower:]]' '[[:upper:]]')
SUBMIT_HD_CLUSTER=$(eval print \$DW_${SUBMIT_HD_ENV_UPPER}_DB)

#temporary directory for data copy to ensure the data integrity
TARGET_HD_PATH_TMP=${TARGET_HD_PATH%/*}/_dint_temporary
TARGET_HD_PATH_TMP_COPY=${TARGET_HD_PATH_TMP}/${SOURCE_HD_PATH##*/}
print "TARGET_HD_PATH_TMP == $TARGET_HD_PATH_TMP"
print "TARGET_HD_PATH_TMP_COPY == $TARGET_HD_PATH_TMP_COPY"


########################################
# THIS NEEDS TO BE ENHANCED
# always add Artemis NN info
# no such info in hdfs-site.xml of Juno/Ares/Apollo, have to manually add it
########################################
if [[ ${JOB_ENV} != "hd3" ]];then
    SOURCE_HD_NN=$(print $SOURCE_HD_NN_URL|sed 's#hdfs://##' |awk -F':' '{print $1}')
    TARGET_HD_NN=$(print $TARGET_HD_NN_URL|sed 's#hdfs://##' |awk -F':' '{print $1}')
    export HD_NN_INFO="-Ddfs.nameservices=${SOURCE_HD_NN},${TARGET_HD_NN},artemis-lvs-nn-ha
     -Ddfs.ha.namenodes.artemis-lvs-nn-ha=nn1,nn2
     -Ddfs.namenode.rpc-address.artemis-lvs-nn-ha.nn1=artemis-nn.vip.ebay.com:8020
     -Ddfs.namenode.rpc-address.artemis-lvs-nn-ha.nn2=artemis-nn-2.vip.ebay.com:8020
     -Ddfs.client.failover.proxy.provider.artemis-lvs-nn-ha=org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider
     "
    print "HD_NN_INFO == $HD_NN_INFO"
fi

# Set HADOOP environment and login to target
. $DW_MASTER_CFG/.${TARGET_HD_CLUSTER}_env.sh
. $DW_MASTER_CFG/hadoop.login $TARGET_HD_ENV


print "Remove temporary dir on target host $TARGET_HD_PATH_TMP"
hadoop fs -rm -f -r -skipTrash $TARGET_HD_PATH_TMP


print "Checking if target already exists"
set +e
hadoop fs -ls $TARGET_HD_PATH
rcode=$?
set -e

if [[ $rcode == 0 ]]
then
    if [[ $TARGET_DELETE == 1 ]]
    then
        print "Removing Target"
        set +e
        hadoop fs $HD_NN_INFO -rm -r -skipTrash $TARGET_HD_PATH
        rcode=$?
        set -e

        if [[ $rcode != 0 ]]
        then
            print "ERROR: Failed to remove Target: $TARGET_HD_PATH"
            exit 5
        fi
    else
        print "ERROR: SHALL NOT COPY.  Option TARGET_DELETE == $TARGET_DELETE, and Target exists: $TARGET_HD_PATH"
        exit 4
    fi
else
    print "Target:  $TARGET_HD_PATH does not exist, processing copy"
fi

print "Make Temp Directory $TARGET_HD_PATH_TMP"
set +e
hadoop fs -mkdir -p $TARGET_HD_PATH_TMP
rcode=$?
set -e

if [[ $rcode != 0 ]]
then
    print "ERROR:  Failed to make Temp Directory"
    exit 5
fi

################################################################################
# Check if source or target is in Reno, and launch job appropriately
################################################################################
if [[ ( $SOURCE_HD_CLUSTER == *rno && $TARGET_HD_CLUSTER != *rno ) || ( $SOURCE_HD_CLUSTER != *rno && $TARGET_HD_CLUSTER == *rno ) ]]
then
    # Logic to determine phx or lvs optimus
    if [[ $SOURCE_HD_CLUSTER == apollo || $TARGET_HD_CLUSTER == apollo ]]
    then
        OPTIMUS=$DW_MASTER_BIN/optimus_submit_phx.py
    else
        OPTIMUS=$DW_MASTER_BIN/optimus_submit_lvs.py
    fi

    set +e
    /usr/bin/python $OPTIMUS -s $SOURCE_HD_PATH -d $TARGET_HD_PATH_TMP -q $HD_QUEUE
    rcode=$?
    set -e

    if [[ $rcode != 0 ]]
    then
        print "ERROR:  Failed to run optimus, please check log"
        exit 4
    fi
else

    # Set HADOOP environment and login to submit env
    . $DW_MASTER_CFG/.${SUBMIT_HD_CLUSTER}_env.sh
    . $DW_MASTER_CFG/hadoop.login $SUBMIT_HD_ENV

    set +e
    hadoop distcp $HD_NN_INFO -Dmapred.job.queue.name=$HD_QUEUE -Dmapreduce.map.memory.mb=2816 -Dmapreduce.map.java.opts=-Xmx2048m -Dhadoop.ssl.enabled=false -Ddfs.client.use.datanode.hostname=true -prbp -i -m $MAP_CNT $SOURCE_HD_PATH $TARGET_HD_PATH_TMP
    rcode=$?
    set -e

    if [[ $rcode != 0 ]]
    then
        print "ERROR:  Failed to run distp command, please check log"
        exit 4
    fi
fi


# Set HADOOP environment and login to target
. $DW_MASTER_CFG/.${TARGET_HD_CLUSTER}_env.sh
. $DW_MASTER_CFG/hadoop.login $TARGET_HD_ENV

print "Moving temporary to target directory"
print "Temp: $TARGET_HD_PATH_TMP_COPY"
print "Target: $TARGET_HD_PATH"
set +e
hadoop fs -mv $TARGET_HD_PATH_TMP_COPY $TARGET_HD_PATH
rcode=$?
set -e

if [[ $rcode != 0 ]]
then
    print "ERROR:  Failed on $HD_NN_INFO to move"
    exit 4
fi

print "Remove Temp Directory $TARGET_HD_PATH_TMP"
set +e
hadoop fs -rm -f -r -skipTrash $TARGET_HD_PATH_TMP
rcode=$?
set -e

if [[ $rcode != 0 ]]
then
    print "ERROR:  Failed to remove Temp Directory"
    exit 5
fi


exit 0
