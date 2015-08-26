#!/bin/ksh -eu
# This script copies td5 to td7 directory structure
# Must be run as root, or owner permissions will not be copied appropriately
#
# Date        Version  Modified By         Revision Notes
# ----------  -------  ------------------  ----------------------------------- #
# 2015-08-26  1.0       Ryan Wong           Initial Version
# ----- ---------------------------------------------------------------- ----- #
#

ETL_ENV=$1

if [[ "X$ETL_ENV" = "X" ]]
then
        print
        print "FATAL ERROR: Usage is $0 <ETL_ENV>, where ETL_ENV is infra|dev|qa|prod" >&2
        print
        return 3
fi

# Only root can start/stop/restart the service
uid=`id | cut -d\( -f1 | cut -d= -f2`
if [ $uid -ne 0 ]
then
        echo `date`: "$0: Fatal Error: Must be run as root."
        exit 1
fi

# Define the etl hierarchy root and etl environment base
HROOT=/dw/etl
BASE=$HROOT/home

# Export the etl environment
export ETL_ENV

# set ENV_TYPE based on ETL_ENV
export ENV_TYPE=${ETL_ENV##*_}

# $DW_HOME is set from the value in $HOME/.etlenv
if [ -d $BASE/$ETL_ENV ]
then
        DW_HOME=$BASE/$ETL_ENV
else
        print
        print "FATAL ERROR: Environment \"$ETL_ENV\" is not a valid environment" >&2
        print
        return 4
fi


# Define and export environment specific etl dirs
export DW_HOME
export DW_ARC=$DW_HOME/arc
export DW_DAT=$DW_HOME/dat
export DW_DBC=$DW_HOME/dbc
export DW_IN=$DW_HOME/in
export DW_LIB=$DW_HOME/lib
export DW_LOG=$DW_HOME/log
export DW_CMP=$DW_HOME/cmp
export DW_CMS=$DW_HOME/cms
export DW_LOGINS=$DW_HOME/.logins
export DW_MFS=$DW_HOME/mfs
export DW_MP=$DW_HOME/mp
export DW_OUT=$DW_HOME/out
export DW_SRC=$DW_HOME/src
export DW_TMP=$DW_HOME/tmp
export DW_WATCH=$DW_HOME/watch

export DW_IN02=$DW_MFS/fs02/in
export DW_IN04=$DW_MFS/fs04/in
export DW_IN08=$DW_MFS/fs08/in
export DW_IN12=$DW_MFS/fs12/in
export DW_IN16=$DW_MFS/fs16/in
export DW_IN20=$DW_MFS/fs20/in
export DW_IN24=$DW_MFS/fs24/in
export DW_TMP02=$DW_MFS/fs02/tmp
export DW_TMP04=$DW_MFS/fs04/tmp
export DW_TMP08=$DW_MFS/fs08/tmp
export DW_TMP12=$DW_MFS/fs12/tmp
export DW_TMP16=$DW_MFS/fs16/tmp
export DW_TMP20=$DW_MFS/fs20/tmp
export DW_TMP24=$DW_MFS/fs24/tmp
export DW_LAND=$DW_HOME/land


rsync -v -a -l -t -f"+ */" -f"- *" $DW_DAT/td5/ $DW_DAT/td7/
rsync -v -a -l -t -f"+ */" -f"- *" $DW_LOG/td5/ $DW_LOG/td7/
rsync -v -a -l -t -f"+ */" -f"- *" $DW_TMP/td5/ $DW_TMP/td7/
rsync -v -a -l -t -f"+ */" -f"- *" $DW_ARC/td5/ $DW_ARC/td7/

if [ ! -h $DW_IN/td7 ]; then cd $DW_IN; ln -s $DW_IN/extract td7; fi
if [ ! -h $DW_OUT/td7 ]; then cd $DW_OUT; ln -s $DW_OUT/extract td7; fi
if [ ! -h $DW_IN02/td7 ]; then cd $DW_IN02; ln -s $DW_IN02/extract td7; fi
if [ ! -h $DW_IN04/td7 ]; then cd $DW_IN04; ln -s $DW_IN04/extract td7; fi
if [ ! -h $DW_IN08/td7 ]; then cd $DW_IN08; ln -s $DW_IN08/extract td7; fi
if [ ! -h $DW_IN12/td7 ]; then cd $DW_IN12; ln -s $DW_IN12/extract td7; fi
if [ ! -h $DW_IN16/td7 ]; then cd $DW_IN16; ln -s $DW_IN16/extract td7; fi
if [ ! -h $DW_IN20/td7 ]; then cd $DW_IN20; ln -s $DW_IN20/extract td7; fi
if [ ! -h $DW_IN24/td7 ]; then cd $DW_IN24; ln -s $DW_IN24/extract td7; fi

rsync -v -a -l -t -f"+ */" -f"- *" $DW_TMP02/td5/ $DW_TMP02/td7/
rsync -v -a -l -t -f"+ */" -f"- *" $DW_TMP04/td5/ $DW_TMP04/td7/
rsync -v -a -l -t -f"+ */" -f"- *" $DW_TMP08/td5/ $DW_TMP08/td7/
rsync -v -a -l -t -f"+ */" -f"- *" $DW_TMP12/td5/ $DW_TMP12/td7/
rsync -v -a -l -t -f"+ */" -f"- *" $DW_TMP16/td5/ $DW_TMP16/td7/
rsync -v -a -l -t -f"+ */" -f"- *" $DW_TMP20/td5/ $DW_TMP20/td7/
rsync -v -a -l -t -f"+ */" -f"- *" $DW_TMP24/td5/ $DW_TMP24/td7/



if [ ! -d $DW_WATCH/td7 ]
then
  mkdir -p $DW_WATCH/td7
  chown dw_adm:dw_ops $DW_WATCH/td7
  chmod 775 $DW_WATCH/td7
  chmod g+s $DW_WATCH/td7
fi
