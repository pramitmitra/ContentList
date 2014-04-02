#!/bin/ksh -eu
# Title:        dw_infra.Synch_HADR_Node.ksh
# File Name:    dw_infra.synch_hadr_node.ksh
# Description:  Synch passed list of data and state files to target ETL system.  This script will be used by both real-time
#               and batch mode synching scripts.
#
#               If either file list is not passed (data or state), that processing will be skipped.
#               Data File List will be pushed via socparc file transfer (sft) in first phase.  It will be assumed to be in the 
#                 standard sft file transfer format 
#               State File List will be pushed via scp in second phase.  This list should be in the format of:
#                  Target_Server TARGET_FILE [SOURCE_FILE]
#                  If the source files is not populated, it will assume source and target is the same 
#                  ex:
#                  zaisetlcoreha01  -d $DW_SA_DAT/mydatafiles.lis -s $DW_SA_DAT/mystatefiles.lis -l $DW_SA_LOG/mylogfile -x $DW_SA_TMP/myexcfile.lis
#                  
# Developer:    Brian Wenner
# Created on:   
# Location:     $DW_MASTER_BIN  
# Logic:       
#
#
# Called by:    Appworx/Unix
#
# Date         Ver#   Modified By(Name)            Change and Reason for Change
#---------    -----  ---------------------------  ------------------------------
# 2010-12-21   1.0    Brian Wenner                  initial version - new submodule for HADR synching
# 2011-12-15   1.1    Ryan Wong                     Need to create remote directory, if not exists before scp
# 2012-08-08   1.2    Kevin Oaks                   Port to RedHat:
#                                                   - now using /bin/ksh rather than /usr/bin/ksh
#                                                   - Converted all echo statements to print

#Set Optional Variables to defaults
DATALIS=
STATELIS=
MKRMTDIR=
RMTSERVER=
PT=

while getopts "d:s:l:x:p:t:r:" opt
do
case $opt in
   d)   DATALIS="$OPTARG";;
   s)   STATELIS="$OPTARG";;
   l)   LFILE="$OPTARG";;
   x)   XFILE="$OPTARG";;
   p)   PT="$OPTARG";;
   t)   MKRMTDIR="$OPTARG";;
   r)   RMTSERVER="$OPTARG";;
   \?)  print >&2 "Usage: $0 -d data_file_list -s state_file_list -l log_file -x exc_file_list [-p sft_port]"
   exit 1;;
esac
done
shift $(($OPTIND - 1))

. /dw/etl/mstr_cfg/etlenv.setup

CMP=$SFT_COMPRESS
BW=$SFT_BW
VL=$SFT_VERBOSE_LEVEL
PI=$SFT_PRINT_INTERVAL
NW=$SFT_NWAYS

if [[ -z $PT ]]; then
  PT=$SFT_DFLT_PORT
fi

if [[ -n $DATALIS ]]; then
print "${0##*/}: INFO, 
##########################################################################################################
# Processing Data List file: $DATALIS `date`
##########################################################################################################
" 

  if [[ -f $DATALIS ]]; then
    #synch the files
    set +e
    $DW_MASTER_BIN/sg_file_xfr_client -d 2 -f $DATALIS -x $XFILE -l $LFILE -p $PT -c $CMP -b $BW -i $PI -v $VL -n $NW
    RCODE=$?
    set -e

    if [ $RCODE != 0 ]
    then
       print "${0##*/}:  ERROR, Process failed with return code ( $RCODE ) - view log at $LFILE" >&2
       exit 4
    elif [[ -f $XFILE ]]
    then
      print "${0##*/}:  ERROR, Exception file ( $XFILE ) exists after process ended.  Process did not complete successfully" >&2
      print "${0##*/}:  ERROR, View log at $LFILE" >&2
      exit 4
    fi
  else
    print "${0##*/}:  ERROR, Passed Data List File $DATALIS not found." >&2
    exit 4
   fi

print "${0##*/}:  INFO,
##########################################################################################################
# Finished Processing Data List file: $DATALIS `date`
##########################################################################################################
" 

else
print "${0##*/}:  INFO,
##########################################################################################################
# Skipped Processing Data List file ( no list passed ) `date`
##########################################################################################################
"  

fi

if [[ -n $MKRMTDIR ]]; then
print "${0##*/}:  INFO,
##########################################################################################################
# Making remote directory: $MKRMTDIR `date`
##########################################################################################################
"
#ssh to remote machine, run .etlenv, call mkdirifnotexists
ssh -n $RMTSERVER ". /dw/etl/mstr_cfg/etlenv.setup; mkdirifnotexist $MKRMTDIR"
fi



if [[ -n $STATELIS ]]; then
print "${0##*/}:  INFO,
##########################################################################################################
# Processing State List file: $STATELIS `date`
##########################################################################################################
"  

  if [[ -f $STATELIS ]]; then
    while read TGT_HOST_TMP TGT_FILE_TMP SRC_FILE_TMP
    do
        TGT_HOST=`print $(eval print $TGT_HOST_TMP)`
        TGT_FILE=`print $(eval print $TGT_FILE_TMP)`
        if [[ -n $SRC_FILE_TMP ]]; then
           SRC_FILE=`print $(eval print $SRC_FILE_TMP)`
        else
           SRC_FILE=$TGT_FILE
        fi
        TGT_FILE_DIR=${TGT_FILE%/*}
        set +e
        ssh -n $TGT_HOST "mkdir -p $TGT_FILE_DIR" > /dev/null
        set -e
        scp -B $SRC_FILE $TGT_HOST:$TGT_FILE  >&2
     done < $STATELIS
   fi

print "${0##*/}:  INFO,
##########################################################################################################
# Finished Processing State List file: $STATELIS `date`
##########################################################################################################
" 

else
print "${0##*/}:  INFO,
##########################################################################################################
# Skipped Processing State List file ( no list passed ) `date`
##########################################################################################################
" 
fi

exit 0
