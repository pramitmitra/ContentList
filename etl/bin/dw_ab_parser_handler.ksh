#!/bin/ksh -eu
#======================================================================
# NAME  : dw_ab_parser_handler.ksh
# TYPE  : KornShell script
# USAGE : Script handles file management from the ETL_ID.sources.lis file,
#         and managest calling of PARSER to all files in the list.
#		    The PARSER program must take exactly 2 parameters, infile and outfile.
# OUTPUT: This script takes the current batch seq files to parse.
#
#
# EXAMPLE:
# dw_rtp.dw_rtp_slr_trking_parser_handler.ksh dw_rtp.dw_rtp_slr_trking dw_rtp.dw_rtp_slr_trking_parser.ksh
#
#
# Date          Ver#    Modified By           Comments
# ----------    -----   ------------          ------------------------
# 2006-02-24    1.0     Ryan Wong             Initial Version
# 2013-10-04    1.1     Ryan Wong             Redhat changes
#======================================================================

if [ $# != 2 ]
then
   print "Usage:  $0 <ETL_ID> <PARSER>"
   exit 4
fi

export ETL_ID=$1
export PARSER=$2
export JOB_ENV=extract
export JOB_TYPE=extract
export JOB_TYPE_ID=ex
export SUBJECT_AREA=${ETL_ID%%.*}
export TABLE_ID=${ETL_ID##*.}

. /dw/etl/mstr_cfg/etlenv.setup

export DW_SA_DAT=$DW_DAT/$JOB_ENV/$SUBJECT_AREA
export DW_SA_IN=$DW_IN/$JOB_ENV/$SUBJECT_AREA
export DW_SA_LOG=$DW_LOG/$JOB_ENV/$SUBJECT_AREA
export DW_SA_TMP=$DW_TMP/$JOB_ENV/$SUBJECT_AREA
BATCH_SEQ_NUM_FILE=$DW_SA_DAT/$TABLE_ID.$JOB_TYPE.batch_seq_num.dat
TABLE_LIS_FILE=$DW_CFG/$ETL_ID.sources.lis

# get BATCH_SEQ_NUM
PREV_BATCH_SEQ_NUM=$(<$BATCH_SEQ_NUM_FILE)
((BATCH_SEQ_NUM=PREV_BATCH_SEQ_NUM+1))

print "--------------------------------------------------------------------------------"
print -- "- Starting $0 `date`"
print "--------------------------------------------------------------------------------"

while read FILE_ID DBC_FILE PARALLEL_NUM TABLE_NAME DATA_FILENAME PARAM
do
	DATA_FILE=$(eval print $DW_SA_IN/$DATA_FILENAME)
	if [ ! -f $DATA_FILE.notparse.$BATCH_SEQ_NUM ]
	then
		mv $DATA_FILE.$BATCH_SEQ_NUM $DATA_FILE.notparse.$BATCH_SEQ_NUM
	fi

	
	if [ -f $DW_EXE/$SUBJECT_AREA/$PARSER ]
        then
           $DW_EXE/$SUBJECT_AREA/$PARSER $DATA_FILE.notparse.$BATCH_SEQ_NUM $DATA_FILE.$BATCH_SEQ_NUM
        else    
           $DW_EXE/$PARSER $DATA_FILE.notparse.$BATCH_SEQ_NUM $DATA_FILE.$BATCH_SEQ_NUM
        fi 
        
done < $TABLE_LIS_FILE

integer TOTAL_REC_CNT=0
while read FILE_ID DBC_FILE PARALLEL_NUM TABLE_NAME DATA_FILENAME PARAM
do
	DATA_FILE=$(eval print $DW_SA_IN/$DATA_FILENAME)
	wc -l $DATA_FILE.$BATCH_SEQ_NUM | read REC_CNT FILE_NAME_OUT
	((TOTAL_REC_CNT+=$REC_CNT))
done < $TABLE_LIS_FILE

if [ ! -f $DW_SA_IN/$TABLE_ID.record_count.dat.notparse.$BATCH_SEQ_NUM ]
then
	mv $DW_SA_IN/$TABLE_ID.record_count.dat.$BATCH_SEQ_NUM $DW_SA_IN/$TABLE_ID.record_count.dat.notparse.$BATCH_SEQ_NUM
fi

print $TOTAL_REC_CNT > $DW_SA_IN/$TABLE_ID.record_count.dat.$BATCH_SEQ_NUM

print "--------------------------------------------------------------------------------"
print -- "- Finshed $0 `date`"
print "--------------------------------------------------------------------------------"

