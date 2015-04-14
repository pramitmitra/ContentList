#!/bin/ksh -eu
#
# dw_infra.change_owner_subject_area_to_secure_user_remote.ksh
#
# Description:
# ----- ---------------------------------------------------------------- ----- #
# Converts a subject area belonging to dw_adm to a secure batch id.
#
#
# Date        Version  Modified By         Revision Notes
# ----------  -------  ------------------  ----------------------------------- #
# 2015-03-26  1.0       Ryan Wong           Initial Version
# 2015-04-06  1.1       Ryan Wong           Add chown for DW_WATCH files
# 2015-04-07  1.2       Ryan Wong           Separate local and remote w/diff scripts
################################################################################

SCRIPTNAME=$(basename $0)
CURR_DATETIME=${CURR_DATETIME:-$(date '+%Y%m%d-%H%M%S')}
LOGFILE=$PWD/$SCRIPTNAME.$CURR_DATETIME.log

exec 1>>$LOGFILE 2>&1
echo `date`: "Running script $0"
echo `date`: "Log File name: $LOGFILE"


# Only dw_infra can run this script
uid=`id | cut -d\( -f1 | cut -d= -f2`
if [ $uid -ne 4005 ]
then
        echo `date`: "$SCRIPTNAME: FATAL ERROR: Must be run as dw_infra."
        exit 4
fi

. /dw/etl/mstr_cfg/etlenv.setup
. /dw/etl/mstr_lib/dw_etl_common_functions.lib

ETL_ID=$1
SECURE_BATCH_ID=$2

if [[ "X${ETL_ID:-}" = "X" ]]
then
    echo `date`: "$SCRIPTNAME: FATAL ERROR: ETL_ID not defined" >&2
    exit 5
fi

if [[ "X${SECURE_BATCH_ID:-}" = "X" ]]
then
    echo `date`: "$SCRIPTNAME: FATAL ERROR: SECURE_BATCH_ID not defined" >&2
    exit 6
fi

while read JOB_ENV
do

    . /dw/etl/mstr_cfg/dw_etl_common_defs.cfg
    unset DW_SA_LOG
    unset DW_SA_OUT
    . /dw/etl/mstr_cfg/dw_etl_common_defs.cfg

    # Remote directories
    # Change ownership
    sudo chown --preserve-root -v -R $SECURE_BATCH_ID $DW_SA_DAT
    sudo chown --preserve-root -v dw_adm:dw_ops $DW_SA_DAT
    sudo chmod --preserve-root -v -R g+w $DW_SA_DAT
    sudo chmod --preserve-root -v -R g+s $DW_SA_DAT
    sudo chmod --preserve-root -v -R g+w $DW_SA_LOG
    sudo chmod --preserve-root -v -R g+s $DW_SA_LOG

    # Watch Files
    echo "Change watch files in directory $DW_WATCH/$JOB_ENV"
    for wfile in $(find $DW_WATCH/$JOB_ENV -name "$SUBJECT_AREA.*")
    do
        sudo chown --preserve-root -v $SECURE_BATCH_ID:dw_ops $wfile
    done

done < $DW_MASTER_CFG/dw_etl_job_env.lis

exit
