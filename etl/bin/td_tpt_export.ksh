#!/bin/ksh -eu
#------------------------------------------------------------------------------------------------
# Title:        Teradata tpt extract Handler
# File Name:    td_tpt_extract.ksh
# Description:  This script is to run one instance of load at a specified host.
# Developer: rganesan
# Created on:
# Location:     $DW_BIN
# Logic:
#
# Date         Ver#   Modified By(Name)            Change and Reason for Change
#---------    -----  ---------------------------  ------------------------------
# 06-02-2000   1.2 	rganesan		    modification done for DW sync
# 10-04-2013   1.3    Ryan Wong                     Redhat changes
# 10-17-2013   1.4    Ryan Wong                     Changed tpt_load binary to point to DW_MASTER_EXE
#------------------------------------------------------------------------------------------------

set -A tpt_normal_args s u p d t wd mn po i n f l dl dc c th ts ns df le lf fp z qb pi md mr id v uow etl_id sf fes tes sq 
set -A tpt_custom_args teradata_host userid password database_name table_name working_database master_node port instance_nbr instance_cnt data_file  log_file hex_delimiter char_delimiter charset tenacity_hours tenacity_sleep sessions date_format logon_variable logon_file data_file_pattern compress_flag query_band print_interval modulo_divisor modulo_remainder in_directory verbosity uow  etl_id sql_file from_extract_seq to_extract_seq sq
set -A tpt_arg_values

#--------------------------------------
# functions
#--------------------------------------
search_args() {
        val=$*
        search_args_idx=0
        while [[ $search_args_idx -lt ${#tpt_normal_args[*]} ]]
        do
                tpt_default_arg=${tpt_normal_args[$search_args_idx]}
                tpt_custom_arg=${tpt_custom_args[$search_args_idx]}
                if [[ $val = "-${tpt_normal_args[$search_args_idx]}" || $val = "-${tpt_custom_args[$search_args_idx]}" ]]
                then
                        return $search_args_idx
                fi
                search_args_idx=$(( search_args_idx + 1 ))
        done
        return 255
}
#--------------------------------------
# parse arguments
#--------------------------------------
while [[ ${#} -gt 0 ]]
do

        #get option from command line
        option="$1"

        #test that option is valid
        search_args "$option"
        tpt_arg_idx=$?
        if [[ $tpt_arg_idx -eq 255 ]]
        then
                print "ERROR: Invalid argument: $option"
                exit 1
        fi

        #try to get value for previous option
        if [[ ${#} -gt 1 ]]
        then
                shift
                value="$1"
                tpt_arg_values[$tpt_arg_idx]="$value"
        else
                print "ERROR: No value for option: $option"
        fi

        #move on to next option/value set
        if [[ ${#} -gt 0 ]]
        then
                shift
        fi
done

search_args "-etl_id"
search_arg_idx=$?
ETL_ID=${tpt_arg_values[$search_arg_idx]}

export SUBJECT_AREA=${ETL_ID%%.*}
export TABLE_ID;TABLE_ID=${ETL_ID##*.}
. /dw/etl/mstr_cfg/etlenv.setup


DW_SA_TMP="$DW_TMP/extract/$SUBJECT_AREA/"

if [[ $data_directory = "" ]]
then DW_SA_OUT="$DW_OUT/extract/$SUBJECT_AREA/"
else 
DW_SA_OUT=$data_directory
fi



#--------------------------------------
# make log file unique if specified
#--------------------------------------

search_args "-instance_nbr"
search_arg_idx=$?
instance_nbr=${tpt_arg_values[$search_arg_idx]}


search_args "-log_file"
search_arg_idx=$?
log_file=${tpt_arg_values[$search_arg_idx]}
if [[ $log_file != "" ]]
then
        tpt_arg_values[$search_arg_idx]="${tpt_arg_values[$search_arg_idx]}.${instance_nbr}"
fi

search_args "-to_extract_seq"
search_arg_idx=$?
TO_EXTRACT_SEQ=${tpt_arg_values[$search_arg_idx]}


search_args "-data_file"
search_arg_idx=$?
data_file=${tpt_arg_values[$search_arg_idx]}


if [[ $data_file != "" ]]
then
        tpt_arg_values[$search_arg_idx]="${tpt_arg_values[$search_arg_idx]}.$TO_EXTRACT_SEQ.${instance_nbr}"
fi



search_args "-sq"
search_arg_idx=$?
sql=${tpt_arg_values[$search_arg_idx]}


#--------------------------------------
# build argument string
#--------------------------------------
#build argument string
tpt_arg=""
tpt_args_idx=0
while [[ $tpt_args_idx -lt ${#tpt_normal_args[*]} ]]
do
        tpt_default_arg=${tpt_normal_args[$tpt_args_idx]}
        tpt_arg_value=${tpt_arg_values[$tpt_args_idx]}
        #exclude custom args
        if [[ $tpt_arg_value != "" && $tpt_default_arg != "md" && $tpt_default_arg != "mr" && $tpt_default_arg != "fp"  && $tpt_default_arg != "id" && $tpt_default_arg != "lt"  && $tpt_default_arg != "li"  && $tpt_default_arg != "uow" && $tpt_default_arg != "fes" && $tpt_default_arg != "tes" && $tpt_default_arg != "etl_id"  && $tpt_default_arg != "sq" ]]
        then
                #TODO
                #tpt_arg="$tpt_arg -$tpt_default_arg \"$tpt_arg_value\""
                tpt_arg="$tpt_arg -$tpt_default_arg $tpt_arg_value"
        fi
        tpt_args_idx=$(( tpt_args_idx + 1 ))
done

#--------------------------------------
# launch tpt
#--------------------------------------
export LD_LIBRARY_PATH=$TPT_LD_LIBRARY_PATH
export NLSPATH=$TPT_NLSPATH
 
$DW_MASTER_EXE/tpt_load.64 -ot 2 $tpt_arg  -sq "$sql"

