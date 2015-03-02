#!/bin/ksh -eu
#------------------------------------------------------------------------------------------------
# Title:        Teradata tpt load Handler
# File Name:    td_load_api_handler.ksh
# Description:  Handler submits multiple instances of load job on different hosts based on the instance_cnt.  
# Developer:
# Created on:
# Location:     $DW_BIN
# Logic:
#
# Date         Ver#   Modified By(Name)            Change and Reason for Change
#---------    -----  ---------------------------  ------------------------------
# ????-??-??  1.1    ???                          Initial Creation
# 2013-10-04  1.2    Ryan Wong                    Redhat changes
# 2013-10-08  1.3    Ryan Wong                    Netstat on Redhat
# 2013-10-17  1.4    Ryan Wong                    Changed tpt_load binary to point to DW_MASTER_EXE
# 2015-03-02  1.5    Jiankang Liu                 Fix the tpt_load job path bug
####################################################################################################

if [ $# -lt 2 ]
then
print "Error :- Usage: ksh $0 <ETL_ID> <JOB_ENV> [<next_uow=next_uow_val> <PARAM_NAME1=PARAM_VALUE1> <PARAM_NAME2=PARAM_VALUE2> ...]"
exit 1
fi 

ETL_ID=$1
JOB_ENV=$2

export SUBJECT_AREA=${ETL_ID%%.*}
export TABLE_ID;TABLE_ID=${ETL_ID##*.}
mpjret=$?
if [ 0 -ne $mpjret ] ; then
   print -- Error evaluating: 'parameter TABLE_ID', interpretation 'shell'
   exit $mpjret
fi

JOB_TYPE_ID="ld"
next_uow=""
CURR_DATETIME=$(date '+%Y%m%d-%H%M%S')


. /dw/etl/mstr_cfg/etlenv.setup


TPT_LOAD_JOB="$DW_BIN/td_tpt_load.ksh"
logon_file="$DW_LOGINS/$SUBJECT_AREA"
work_directory="$DW_TMP/$JOB_ENV/$SUBJECT_AREA"
log_file="$DW_LOG/$JOB_ENV/$SUBJECT_AREA/$TABLE_ID.ld.$(date '+%Y%m%d-%H%M%S').log"
DW_SA_TMP="$DW_TMP/$JOB_ENV/$SUBJECT_AREA/"
DW_SA_LOG="$DW_LOG/$JOB_ENV/$SUBJECT_AREA/"
SUBJECT_AREA_LAST_LOAD_FILE=$DW_DAT/$JOB_ENV/$SUBJECT_AREA/$TABLE_ID.last_load_value.dat
PARENT_ERROR_FILE=$DW_SA_LOG$TABLE_ID.$JOB_TYPE_ID.tpt_load_handler.$CURR_DATETIME.err

set +e
grep "^TPT_NODE_LIST\>" $DW_CFG/$ETL_ID.cfg | read PARAM TPT_NODE_LIST 
rcode=$?
set -e

if [ $rcode != 0 ]
then
   print "${0##*/}:  ERROR, failure determining value for TPT_NODE_LIST parameter from $DW_CFG/$ETL_ID.cfg"
   exit 4
fi

set -A hosts $TPT_NODE_LIST 
host_cnt=${#hosts[*]}


set -A tpt_normal_args s d t wd mn po n fl l dl dc c th ts ns df lf fp z qb pi id v lt li 
set -A tpt_custom_args teradata_host database_name table_name working_database master_node port instance_cnt data_list_file log_file hex_delimiter char_delimiter charset tenacity_hours tenacity_sleep sessions date_format logon_file data_file_pattern compress_flag query_band print_interval in_directory verbosity load_type load_interval
set -A tpt_arg_values
set -A tpt_arg_names

set +e
grep -s "^compress_flag\>" $DW_CFG/$ETL_ID.cfg | read PARAM compress_flag comment
rcode=$?
set -e

if [ $rcode != 0 ]
then
compress_flag=0
fi

set +e
grep -s "^DATA_RET_DAYS\>" $DW_CFG/$ETL_ID.cfg | read PARAM DATA_RET_DAYS comment
rcode=$?
set -e

if [ $rcode != 0 ]
then
DATA_RET_DAYS=0
fi

export load_interval=$(grep "^load_interval\>" $DW_CFG/$ETL_ID.cfg | read PARAM VALUE comment; eval print $VALUE)
export load_type=$(grep "^load_type\>" $DW_CFG/$ETL_ID.cfg | read PARAM VALUE comment; eval print $VALUE)
export data_list_file=$(grep "^data_list_file\>" $DW_CFG/$ETL_ID.cfg  | read PARAM VALUE COMMENT; eval print $VALUE)
grep "^instance_cnt\>" $DW_CFG/$ETL_ID.cfg | read PARAM instance_cnt comment
grep "^port\>" $DW_CFG/$ETL_ID.cfg | read PARAM port comment
grep "^master_node\>" $DW_CFG/$ETL_ID.cfg | read PARAM master_node comment

shift 2 

if [ $# -ge 1 ]
then
        for param in $*
        do
                if [ ${param%=*} = $param ]
                then
                        print "${0##*/}: ERROR, parameter definition $param is not of form <PARAM_NAME=PARAM_VALUE>"
                        exit 4
                else
                        export $param
                fi
        done
fi

condn_compress_sfx=""

if [ $compress_flag = 1 ]
then
condn_compress_sfx=".gz"
else
        if [ $compress_flag = 2 ]
        then condn_compress_sfx=".bz2"
        fi
fi

if [[ $next_uow == "" ]]
then 
 LAST_LOAD_PATTERN=$(<$SUBJECT_AREA_LAST_LOAD_FILE)
else
 LAST_LOAD_PATTERN="No_Arc"
fi

COMP_FILE=$DW_SA_TMP/$TABLE_ID.load.complete

if [ ! -f $COMP_FILE ]
then
        # COMP_FILE does not exist.  1st run for this processing period.
        FIRST_RUN=Y
else
        FIRST_RUN=N
fi

# Source the error message handling logic.  On failure, trap will send the contents of the PARENT_ERROR_FILE to the
# subject area designated email addresses.

. $DW_LIB/message_handler


print "

##########################################################################################################
#
# Beginning load for ETL_ID: $ETL_ID   `date`
#
##########################################################################################################
"

if [ $FIRST_RUN = Y ]
then
        # Need to run the clean up process since this is the first run for the current processing period.
if [[ $host_cnt -gt 1 ]] 
then
	COMMAND_SCRIPT="$DW_TMP/$JOB_ENV/$SUBJECT_AREA/$TABLE_ID.load.command.dat"
	> $COMMAND_SCRIPT
	host_idx=0
	while [[ $host_idx -lt $host_cnt ]];
	do
	host_name=${hosts[${host_idx}]}
	print "/usr/bin/ssh -q $host_name \"$DW_EXE/loader_cleanup_td.ksh $JOB_ENV $JOB_TYPE_ID $ETL_ID $DATA_RET_DAYS $condn_compress_sfx $LAST_LOAD_PATTERN\" 1>$DW_SA_TMP/$TABLE_ID.load.$host_name.output.tmp 2>&1 &">>$COMMAND_SCRIPT
	host_idx=$(( $host_idx + 1 ))
	done
	print "wait" >>$COMMAND_SCRIPT
	. $COMMAND_SCRIPT
	host_idx=0;
	while [[ $host_idx -lt $host_cnt ]];
	do
	host_name=${hosts[${host_idx}]}
	wc -l $DW_SA_TMP/$TABLE_ID.load.$host_name.output.tmp  | read err_count filename
	if [[ $err_count -gt 0 ]]
	then
	print "Error Executing Cleaner Process in $host_name" >&2
	cat $DW_SA_TMP/$TABLE_ID.load.$host_name.output.tmp  
	exit 4
	fi
	host_idx=$(( $host_idx + 1 ))
	print "Cleaner Process Completed Successfully in $host_name"
	done
else 
	$DW_EXE/loader_cleanup_td.ksh $JOB_ENV $JOB_TYPE_ID $ETL_ID $DATA_RET_DAYS $condn_compress_sfx $LAST_LOAD_PATTERN
	rc=$?
	if [[ $rc -gt 0 ]]
	then
	print " Error processing loader_cleanup_td.ksh "
	else
	print "loader_cleanup_td.ksh completed successfully"
	fi
fi
        > $COMP_FILE
else
        print "loader_cleanup.ksh process already complete"
fi


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
set +e
	grep "^[^#]"  $DW_CFG/$ETL_ID.cfg | grep -s "^$Param\>"  | read param Parameter COMMENT
set -e
                if [[ -n $Parameter ]]
                then
                        tpt_arg_names[$notnull_arg_idx]=$Param
                        tpt_arg_values[$notnull_arg_idx]=$Parameter
                        notnull_arg_idx=$(( notnull_arg_idx + 1 ))
                fi
        arg_idx=$(( arg_idx + 1 ))
done


print "
#################################################################################
#
#	load Processing for following Parameters
#
#################################################################################" > $log_file

tpt_arg=""
tpt_args_idx=0
while [[ $tpt_args_idx -lt ${#tpt_arg_names[*]} ]]
do
	 if [[ ${tpt_arg_names[$tpt_args_idx]}  != "load_type" && ${tpt_arg_names[$tpt_args_idx]} != "load_interval" && ${tpt_arg_names[$tpt_args_idx]} != "data_list_file"  ]]
 	then
 		tpt_arg="$tpt_arg -${tpt_arg_names[$tpt_args_idx]}  \"${tpt_arg_values[$tpt_args_idx]}\""
         print ${tpt_arg_names[$tpt_args_idx]}"\t\t\t:" ${tpt_arg_values[$tpt_args_idx]} >> $log_file
	 fi
	 tpt_args_idx=$(( tpt_args_idx + 1 ))
done


print "
#################################################################################" >> $log_file

#############################################################
#
#   Define File Pattern/File list 
#
#############################################################

if [[ $next_uow = "" ]]               # daily or hourly load
then
	LAST_LOAD_PATTERN=$(<$SUBJECT_AREA_LAST_LOAD_FILE)
        if [[ $load_type = "Hourly" || $load_type = "Daily" || $load_type = "Minutely" || $load_type = "Datetime" ]]
        then 
	LOAD_PATTERN=`$DW_EXE/add_duration $LAST_LOAD_PATTERN $load_interval $load_type` 

                    rc=$?
                    if [ $rc !=  0 ]
                    then
                    print "Incorrect prev_uow pattern or load_interval paramter for load type: $load_type" >&2
                    exit 1
                    fi

        else
		if [[ $load_type = "SEQ" ]] 
                then
        		LOAD_PATTERN=$(( LAST_LOAD_PATTERN + 1 ))
                            rc=$?
                                        if [ $rc !=  0 ]
                                        then
                                        print "Incorrect uow pattern for load_type=\"SEQ\"" >&2
                                        exit 1
                                        fi

        	else 
        	LOAD_PATTERN=$LAST_LOAD_PATTERN
                fi
        fi
	if [[ $data_list_file = "" ]]
	then
		data_file_pattern=$LOAD_PATTERN
	else 
		data_list_file=$data_list_file.$LOAD_PATTERN.list
	fi
else					#  load for the UC4 specified pattern 

	data_file_pattern=$next_uow
fi

print "
#########################################################################################################
#
#       Processing  load for the load Pattern  $data_file_pattern 
#
#########################################################################################################"

if [[ $next_uow = ""  && $data_list_file != ""  ]]
then
	tpt_arg="$tpt_arg -data_list_file \"$data_list_file\""
else
	tpt_arg="$tpt_arg -data_file_pattern \"$data_file_pattern\"" 
fi

#########################################################################################################
#
#       Launching the Instances in different Hosts based on the Instant_cnt and Number of Hosts avaiable.
#
#########################################################################################################


set -A host_instance_total #Total number of instances per host
set -A host_instance_cnt   #running number of instances per host


if [[ $instance_cnt = "" ]]
then
        print "ERROR: Instance count not specified"  >&2
exit 4
fi

#loop through each instance and use mod to determine host to calculate total instances per host

instance_idx=0

while [[ $instance_idx -lt $host_cnt ]];
do
host_instance_total[$instance_idx]=0
host_instance_cnt[$instance_idx]=0
instance_idx=$(( $instance_idx + 1 ))
done

instance_idx=0

while [[ $instance_idx -lt $instance_cnt ]]; 
do

        host_idx=$(( $instance_idx % $host_cnt ))
        host_instance_total[$host_idx]=$(( ${host_instance_total[$host_idx]} + 1 ))
        instance_idx=$(( $instance_idx + 1 ))
done

#Check whether the port number already in use to initiate the instances or fail the load


master=`print $master_node | awk 'BEGIN {FS="."} {print $1}'`

if [[ $master = "" ||  $port = "" ]]
then
print "Either master host or port number is not specified" >&2
exit 1
fi

set +e
x=`netstat  -t|awk '{print $4}'|grep ${master%%.*}|grep $port `
rcode=$?
set -e

if [ $rcode = 0 ]
then
  print "FATAL ERROR: Port number $port is already in use" >&2
  exit 4
fi


#Launch the instances at different hosts
instance_idx=0
while [[ $instance_idx -lt $instance_cnt ]]; do
        host_idx=$(( $instance_idx % $host_cnt ))
        host_name=${hosts[${host_idx}]}
        instance_nbr=$(( $instance_idx + 1))

        host_instance_cnt[$host_idx]=$(( ${host_instance_cnt[$host_idx]} + 1 ))

if [[ $host_cnt -gt 1 ]]
then
       print "Launching instance $instance_nbr on $host_name (${host_instance_cnt[$host_idx]}/${host_instance_total[$host_idx]})..." 
	$SSH_PATH -q $host_name ksh $TPT_LOAD_JOB $tpt_arg -etl_id $ETL_ID -job_env $JOB_ENV -log_file $log_file -i $instance_nbr -lf $logon_file  -modulo_divisor ${host_instance_total[$host_idx]} -modulo_remainder ${host_instance_cnt[$host_idx]}  </dev/null >> $log_file &  
       pid=$!
else 
	print "Launching instance $instance_nbr on $host_name (${host_instance_cnt[$host_idx]}/${host_instance_total[$host_idx]})...."	
	eval ksh $TPT_LOAD_JOB $tpt_arg -etl_id $ETL_ID -job_env $JOB_ENV -log_file $log_file -i $instance_nbr -lf $logon_file  -modulo_divisor ${host_instance_total[$host_idx]} -modulo_remainder ${host_instance_cnt[$host_idx]}  </dev/null >> $log_file &
        pid=$!
fi
        tpt_client_pids[$instance_idx]=$pid
        instance_idx=$(( $instance_idx + 1 ))
done


#########################################################################################################
#
#       loop through each instance, wait for it to finish, and capture return code
#
#########################################################################################################

max_rc=0
instance_idx=0
while [[ $instance_idx -lt $instance_cnt ]]; do

        instance_nbr=$(( $instance_idx + 1 ))
        instance_pid=${tpt_client_pids[$instance_idx]}

        #make sure pid is valid
        if [[ $instance_pid = "" ]]
        then
                #don't wait for invalid pid and
                print "No PID for instance $instance_nbr"
                max_rc=255
        else
                #wait for pid to finish
                print "Waiting for the instance $instance_nbr to complete"
                wait $instance_pid
               grep -c "ERR" $log_file   | read errcnt
	       if [[ $errcnt -eq 0 ]]
	       then
               		print "instance $instance_nbr is complete" 
               fi
        fi

        instance_idx=$(( $instance_idx + 1 ))
done

set +e 
grep "ERR" $log_file > $log_file.err
grep -c "ERR" $log_file   | read errcnt
set -e

if [[ $errcnt -gt 0 ]]
then
        print "Load failed . See the Error Log $log_file " >&2
        exit 1
else

print "Removing the complete file  `date`"
rm -f $COMP_FILE
if [[ $next_uow = "" ]]
then
print $LOAD_PATTERN > $SUBJECT_AREA_LAST_LOAD_FILE
next_uow=$LOAD_PATTERN
fi

print "
##########################################################################################################
#
# Load for ETL_ID: $ETL_ID for UOW: $next_uow is successfully Completed `date`
#
##########################################################################################################"

tcode=0
exit
fi 
