#!/bin/ksh -eu
#------------------------------------------------------------------------------------------------
# Title:        Teradata tpt load Handler
# File Name:    td_load_api.ksh
# Description:  This script is to run one instance of load at a specified host.
# Developer:
# Created on:
# Location:     $DW_BIN
# Logic:
#
# Date         Ver#   Modified By(Name)            Change and Reason for Change
#---------    -----  ---------------------------  ------------------------------
#Mar 2010	1	Ram Ganesan			
#Nov 2010	2	Shilpa Dabke		 Replaced ls with find
# Please contact dl-ebay-apd-sojourner for any questions	
#2013-10-04     3       Ryan Wong                 Redhat changes
# 2013-10-17    4    Ryan Wong                    Changed tpt_load binary to point to DW_MASTER_EXE
#------------------------------------------------------------------------------------------------

set -A tpt_normal_args s u p d t wd mn po i n f fl l dl dc c th ts ns df le lf fp z qb pi md mr id v etl_id je
set -A tpt_custom_args teradata_host userid password database_name table_name working_database master_node port instance_nbr instance_cnt data_file data_list_file log_file hex_delimiter char_delimiter charset tenacity_hours tenacity_sleep sessions date_format logon_variable logon_file data_file_pattern compress_flag query_band print_interval modulo_divisor modulo_remainder in_directory verbosity etl_id job_env
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

#--------------------------------------
# extract select arguments
#--------------------------------------
search_args "-database_name"
search_arg_idx=$?
database_name=${tpt_arg_values[$search_arg_idx]}

search_args "-table_name"
search_arg_idx=$?
table_name=${tpt_arg_values[$search_arg_idx]}

search_args "-instance_nbr"
search_arg_idx=$?
instance_nbr=${tpt_arg_values[$search_arg_idx]}

search_args "-modulo_divisor"
search_arg_idx=$?
modulo_divisor=${tpt_arg_values[$search_arg_idx]}

search_args "-modulo_remainder"
search_arg_idx=$?
modulo_remainder=${tpt_arg_values[$search_arg_idx]}

search_args "-data_list_file"
search_arg_idx=$?
data_list_file=${tpt_arg_values[$search_arg_idx]}


search_args "-data_file_pattern"
search_arg_idx=$?
data_file_pattern=${tpt_arg_values[$search_arg_idx]}

search_args "-etl_id"
search_arg_idx=$?
ETL_ID=${tpt_arg_values[$search_arg_idx]}

search_args "-in_directory"
search_arg_idx=$?
in_directory=${tpt_arg_values[$search_arg_idx]}


search_args "-compress_flag"
search_arg_idx=$?
compress_flag=${tpt_arg_values[$search_arg_idx]}


search_args "-job_env"
search_arg_idx=$?
JOB_ENV=${tpt_arg_values[$search_arg_idx]}


if [[ $compress_flag = 1 ]]
then 
file_extn=".gz"
fi 
if [[ $compress_flag = 2 ]]
then 
file_extn=".bz2"
fi
if [[ $compress_flag = 0 ]]
then
file_extn=""
fi


export SUBJECT_AREA=${ETL_ID%%.*}
export TABLE_ID;TABLE_ID=${ETL_ID##*.}

. /dw/etl/mstr_cfg/etlenv.setup

DW_SA_TMP="$DW_TMP/$JOB_ENV/$SUBJECT_AREA/"

if [[ $in_directory = "" ]]
then DW_SA_IN="$DW_IN/extract/$SUBJECT_AREA/"
else 
DW_SA_IN=$in_directory
fi

#--------------------------------------
# make log file unique if specified
#--------------------------------------
search_args "-log_file"
search_arg_idx=$?
log_file=${tpt_arg_values[$search_arg_idx]}
if [[ $log_file != "" ]]
then
        tpt_arg_values[$search_arg_idx]="${tpt_arg_values[$search_arg_idx]}.${instance_nbr}"
fi

#--------------------------------------
# prepare file list if necessary
#--------------------------------------

#file containing list of data files
if [[ $data_list_file != "" ]]
then
        if [[ ! -r "$data_list_file" ]]
        then
                print "ERROR: Data file list does not exist: $data_list_file"
                exit 1
        fi
	        if [[ ! -w $DW_SA_TMP || ! -d $DW_SA_TMP ]]
        then
                print "ERROR: Unable to write to work directory $DW_SA_TMP"
                exit 1
        fi

        base_name=`basename $data_list_file`
        instance_data_list_file="$DW_SA_TMP$TABLE_ID.ld.${instance_nbr}"
        rm -f $instance_data_list_file
	data_file_entry_idx=0
        while read data_file_entry
        do

                modulo_result=$(( ( $data_file_entry_idx % $modulo_divisor ) + 1 ))
                if [[ $modulo_result -eq $modulo_remainder ]]
                then
                        print "$data_file_entry" >> $instance_data_list_file
                fi

                data_file_entry_idx=$(( data_file_entry_idx + 1 ))

        done < $data_list_file

        search_args "-data_list_file"
        search_arg_idx=$?
        tpt_arg_values[$search_arg_idx]=$instance_data_list_file

fi



#file pattern for data files
if [[ $data_file_pattern != "" ]]
then

patterns=`print $data_file_pattern | sed -e 's/,/ /g'`

        instance_data_list_file="$DW_SA_TMP$TABLE_ID.ld.${instance_nbr}"
        > $instance_data_list_file

data_file_dir=$in_directory

for pattern in $patterns 
do

        if [[ ! -d $data_file_dir ]]
        then
                print "ERROR: Directory from data file pattern does not exist: $data_file_dir"
                exit 1
        fi
	
        if [[ ! -w $DW_SA_TMP  || ! -d $DW_SA_TMP ]]
        then
                print "ERROR: Unable to write to work directory $DW_SA_TMP"
                exit 1
        fi


        data_file_entry_idx=0
#        for data_file_entry in `$KSH_PATH "ls $data_file_dir*$table_name*$pattern*$file_extn" 2>/dev/null`
	 for data_file_entry in `$KSH_PATH "find $data_file_dir -name "$table_name*$pattern*$file_extn"`
        do

                modulo_result=$(( ( $data_file_entry_idx % $modulo_divisor ) + 1 ))
                if [[ $modulo_result -eq $modulo_remainder ]]
                then
                        print "$data_file_entry" >> $instance_data_list_file
                fi

                data_file_entry_idx=$(( data_file_entry_idx + 1 ))

        done

        search_args "-data_list_file"
        search_arg_idx=$?
        tpt_arg_values[$search_arg_idx]=$instance_data_list_file
done
fi

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
        if [[ $tpt_arg_value != "" && $tpt_default_arg != "md" && $tpt_default_arg != "mr" && $tpt_default_arg != "fp"  && $tpt_default_arg != "id" && $tpt_default_arg != "lt"  && $tpt_default_arg != "li"  && $tpt_default_arg != "etl_id" && $tpt_default_arg != "je" ]]
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
$DW_MASTER_EXE/tpt_load.64  $tpt_arg  
rc=$?

