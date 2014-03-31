#!/bin/ksh -eu
#------------------------------------------------------------------------------------------------
# Filename:     single_table_extract_execsql.ksh
#
# Revision History:
#
# Name             Date            Description
# ---------------  --------------  ---------------------------------------------------
# ???              ??/??/????      Initial Creation
# Ryan Wong        10/04/2013      Redhat changes
#
#------------------------------------------------------------------------------------------------

ETL_ID=$1
JOB_ENV=$2
DATA_FILENAME=$3
FILE_ID=$4
TABLE_NAME=$5

if [ X"${ETL_ID:-}" = X"" ]; then
   print -r -- 'Required parameter ETL_ID undefined'
   print -r -- 'Usage: target_table_load.ksh <ETL_ID> <JOB_ENV> <DATA_FILENAME>'
   exit 1
fi

if [ X"${JOB_ENV:-}" = X"" ]; then
   print -r -- 'Required parameter JOB_ENV undefined'
   print -r -- 'Usage: target_table_load.ksh <ETL_ID>  <JOB_ENV> <DATA_FILENAME>'
   exit 1
fi


export SUBJECT_AREA;SUBJECT_AREA=${ETL_ID%%.*}

. /dw/etl/mstr_cfg/etlenv.setup

#----------------------------------------------------------------------------------------
#-- Assign Environmental variables
#---------------------------------------------------------------------------------------
CURR_DATETIME=$(date '+%Y%m%d-%H%M%S')
FILENAME="$DW_OUT/$JOB_ENV/$SUBJECT_AREA/$DATA_FILENAME.$FILE_ID.$TO_EXTRACT_SEQ.dat"
CFG_FILE="$DW_CFG/$ETL_ID.cfg"
EXTRACT_SQL=${ETL_ID}.sel.sql


print "cat <<EOF" > $DW_SA_TMP/$TABLE_ID.ex.$TABLE_NAME.$TO_EXTRACT_SEQ.tmp
cat $DW_SQL/$EXTRACT_SQL  >> $DW_SA_TMP/$TABLE_ID.ex.$TABLE_NAME.$TO_EXTRACT_SEQ.tmp
print "\nEOF" >> $DW_SA_TMP/$TABLE_ID.ex.$TABLE_NAME.$TO_EXTRACT_SEQ.tmp


set +u
. $DW_SA_TMP/$TABLE_ID.ex.$TABLE_NAME.$TO_EXTRACT_SEQ.tmp > $DW_SA_TMP/$TABLE_ID.ex.$TABLE_NAME.$TO_EXTRACT_SEQ.tmp.2
set -u
mv $DW_SA_TMP/$TABLE_ID.ex.$TABLE_NAME.$TO_EXTRACT_SEQ.tmp.2  $DW_SA_TMP/$TABLE_ID.ex.$TABLE_NAME.$TO_EXTRACT_SEQ.tmp

SQL_FILENAME=$DW_SA_TMP/$TABLE_ID.ex.$TABLE_NAME.$TO_EXTRACT_SEQ.tmp

export CURR_DATETIME=$(date '+%Y%m%d%H%M%S')
logon_file=$(<$DW_LOGINS/$SUBJECT_AREA)
#----------------------------------------------------------------------------------------
#-- Get the variables from Config file
#---------------------------------------------------------------------------------------

search_args() {
        val=$*
        search_args_idx=0
        while [[ $search_args_idx -lt ${#tpt_custom_args[@]} ]]
        do
                if [[ $val == ${tpt_custom_args[$search_args_idx]} ]]
                then
                        return $search_args_idx
                fi
                search_args_idx=$(( search_args_idx + 1 ))
        done
        return 255
}

set -A tpt_normal_args  rl i is ii d ht hf qs id lf c
set -A tpt_custom_args row_limit null_ind null_string_ind null_numeral_ind  delimiter header_title header_format quote_string int_date trip_space char_set

set -A tpt_arg_values
set -A tpt_arg_names

######################################################################################################
#
#       Read Parameters from Config File.
#
######################################################################################################

arg_count=${#tpt_normal_args[@]}
arg_idx=0
notnull_arg_idx=0
while [[ $arg_idx -lt $arg_count ]]
do 
	Param="${tpt_custom_args[$arg_idx]}"
        if [[ $Param != "" ]]
        then 
        grep -c "$Param" $DW_CFG/$ETL_ID.cfg | read count
        fi
        if [[ x"$count" != x"0" ]]
        then
                grep -s "^$Param\>"  $DW_CFG/$ETL_ID.cfg | read param Parameter COMMENT
                if [[ $Parameter != "" && $Parameter != "#" ]]
                then
                        tpt_arg_names[$notnull_arg_idx]=$Param
                        tpt_arg_values[$notnull_arg_idx]=$Parameter
                        notnull_arg_idx=$(( notnull_arg_idx + 1 ))
                fi
        fi
        arg_idx=$(( arg_idx + 1 ))
done

print "
###################################################################
#
#		Parameters for Extract
#
###################################################################
"
tpt_arg=""
tpt_args_idx=0

while [[ $tpt_args_idx -lt ${#tpt_arg_values[@]} ]]
do		
                search_args "${tpt_arg_names[$tpt_args_idx]}"
                tpt_arg_pos=$?
		tpt_arg="$tpt_arg -${tpt_normal_args[$tpt_arg_pos]} ${tpt_arg_values[$tpt_args_idx]}"
         	print ${tpt_arg_names[$tpt_args_idx]}"\t\t\t:" ${tpt_arg_values[$tpt_args_idx]} 
         	tpt_args_idx=$(( tpt_args_idx + 1 ))
done

print "
###################################################################
"

EXTRACTBINARY="-f $FILENAME -l $logon_file -s  $SQL_FILENAME $tpt_arg "

$DW_EXE/execsql $EXTRACTBINARY   
