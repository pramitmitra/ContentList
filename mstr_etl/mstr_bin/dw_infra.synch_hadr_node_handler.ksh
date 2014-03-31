#!/bin/ksh -eu
# Title:        DW Infra Synch_HADR_Node_Handler.ksh
# File Name:    dw_infra.synch_hadr_node_handler.ksh
# Description:  Standard ETL Handle for Synching files.
#               Depending on HADR_MODE.
#                 HADR_MODE[0] directs the synching to HA
#                 HADR_MODE[1] directs the synching to DR
#               These modes are set when $DW_MASTER_CFG/etlenv.setup is run.
#               Valid values for HADR_MODE: (A)ctive, (P)assive, (N)one 
#               Presently DR is never set to active, but this script will allow for that option.
#
#
#               IF: HADR_MODE[instance] is (A)ctive
#                  For passed DATALIS, sft process file list is create to push files via socparc file transfer (sft)
#                  from HADR_SRC[instance] to HADR_TRGT[instance] inline. (this is optional, if no passed DATALIS,
#                  the process assumes no data files should be synched.
#                  For passed STATELIS, scp process file list is create to push files via scp
#                  from HADR_SRC[instance] to HADR_TRGT[instance] inline. (this is optional, if no passed STATELIS,
#                  the process assumes no state files should be synched.
#                  If instance is 0, and HADR_MODE[1] is (P)assive, create the passive push lists for DATALIS and 
#                  STATELIS and push them to HADR_TRGT[0]
#               IF: HADR_MODE[instance] is (P)assive
#                  For passed DATALIS, sft process file list is created to push files via socparc file transfer (sft)
#                  from HADR_SRC[instance] to HADR_TRGT[instance] offline. Files are copied to a temporary directory
#                  and pushed from that location (this is optional, if no passed DATALIS, the process assumes 
#                  no data files should be synched.
#                  For passed STATELIS, scp process file list is created to push files via scp
#                  from HADR_SRC[instance] to HADR_TRGT[instance] offline. Files are copied to a temporary directory
#                  and pushed from that location (this is optional, if no passed STATELIS, the process assumes 
#                  no state files should be synched.
#               IF: HADR_MODE[instance] is (N)one
#                  HADR is not active for this instance, and no further processing of files for HADR purproses will occur.
#                  
#               Passed DATALIS and HA and DR versions made from it, are removed at the end
#               of the script. 
#
#               Parameters:
#                 d - Data File List (conditionally optional, a state list, a data file list, or both must be passed)
#                     This is a simple list of the data files to be synched.  It should be uniquely named
#                     such that instances of different runs are unique from each other. These files are pushed via sft.
#                     This filename (minus directory path) is used for later processing, so uniqueness
#                     should not rely on directory path.  ETL Infrastructure will use a name such as: 
#                        $DW_SA_TMP/$ETL_ID.$JOB_ENV.$JOB_TYPE_ID.synch_files.data.$BATCH_SEQ_NUM.dat
#                     If the param is not passed, the process will assume no data files are to be synched.
#                 s - State File List (conditionally optional, a state list, a data file list, or both must be passed) 
#                     This is a simple list of the state files to be synched.  It should be uniquely named
#                     such that instances of different runs are unique from each other. These files are assumed to be small 
#                     and are pushed via scp.  This filename (minus directory path) is used for later processing, so uniqueness
#                     should not rely on directory path.  ETL Infrastructure will use a name such as:
#                        $DW_SA_TMP/$ETL_ID.$JOB_ENV.$JOB_TYPE_ID.synch_files.state.$BATCH_SEQ_NUM.dat
#                     If the param is not passed, the process will assume no state files are to be synched.
#                 l - Log File (required)
#                     The top level log file for the synching process. It should be uniquely named such that
#                     instances of different runs are unique from each other. The ETL Infrastructure will use a name like:
#                        $DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.hadr.$CURR_DATETIME.log
#                 u - Unit of Work ID (required)
#                     Typically a date, datetime, or batch sequence number. This will be combined with Processing ID to ensure uniqueness 
#                 i - Processing ID for the process (required)
#                     This will be used for temporary file naming, as well as offline batch processing of files.
#                     This will be combined with Unit of Work ID to ensure uniqueness. The ETL infrastructure will be using:
#                        $ETL_ID.$JOB_ENV.$JOB_TYPE_ID
#                  
# Developer:    Brian Wenner
# Created on:   
# Location:     $DW_BIN  
# Logic:       
#
#
# Called by:    Appworx/Unix
#
# Date         Ver#   Modified By(Name)            Change and Reason for Change
#---------    -----  ---------------------------  ------------------------------
# 2009-10-21   1.0    Brian Wenner                  initial version
# 2010-05-21   1.1    Brian Wenner                  overhaul to handle various up and down states
# 2010-10-15   1.2    Brian Wenner                  allow for HA or DR to be intentionally absent (not simply inactive/down)
# 2013-02-21   1.3    Ryan Wong                     Wrong logfile printed out on error.  Changing from ILFILE to LOGFILE.
# 2013-10-04   1.4    Ryan Wong                     Redhat changes
#############################################################################################################

# take the passed file list, and build the SFT list file for pushing data files

while getopts "i:e:t:l:u:" opt
do
case $opt in
   i)   ETL_ID="$OPTARG";;
   e)   JOB_ENV="$OPTARG";;
   t)   JOB_TYPE_ID="$OPTARG";;
   l)   LFILE="$OPTARG";;
   u)   UOWID="$OPTARG";;
   \?)  print >&2 "${0##*/}:  ERROR, Usage: $0 -d data_file_list -s state_file_list -l log_file -u unit_of_work_id"
   exit 1;;
esac
done

if [[ ! -n $LFILE ]]; then
   print "${0##*/}:  ERROR,  Log File must be passed (option -l)" >&2
   exit 4
fi

if [[ ! -n $ETL_ID ]]; then
   print "${0##*/}:  ERROR,  ETL ID must be passed (option -i)" >&2
   exit 4
fi

if [[ ! -n $JOB_ENV ]]; then
   print "${0##*/}:  ERROR,  JOB_ENV must be passed (option -e)" >&2
   exit 4
fi

if [[ ! -n $JOB_TYPE_ID ]]; then
   print "${0##*/}:  ERROR,  JOB_TYPE_ID must be passed (option -t)" >&2
   exit 4
fi

if [[ ! -n $UOWID ]]; then
   print "${0##*/}:  ERROR,  Unit of Work must be passed (typically a batch_seq_num or a date/datetime)" >&2
   exit 4
fi

. /dw/etl/mstr_cfg/etlenv.setup
#. $DW_MASTER_CFG/dw_etl_common_defs.cfg

if (( HADR_ACTIVE != 1 )); then
   print "${0##*/}:  INFO, Global HA/DR processing is not enabled.  HADR_ACTIVE = $HADR_ACTIVE" 
   print "${0##*/}:  INFO, No processing will be done." 
   exit 0
fi

PCGID=$ETL_ID.$JOB_ENV.$JOB_TYPE_ID.$UOWID
MFSLIS=$DW_SA_TMP/$PCGID.synch_files.mfs.dat
DATALIS=$DW_SA_TMP/$PCGID.synch_files.data.dat
STATELIS=$DW_SA_TMP/$PCGID.synch_files.state.dat
#LFILE=$DW_SA_LOG/$PCGID.synch_files.log


if [[ ! -s $DATALIS && ! -s $STATELIS && ! -s $MFSLIS ]]; then
   print "${0##*/}:  ERROR, Neither a Data, State, nor mfs file list is populated for processing" >&2
   exit 4
fi

INST=0

MAXINST=1 #(later we might change this to read from a cfg file, in case have more more than 2 destinations)

#Step through each set synching instance, and proceed appropriately.

#Determine HADR mode.
# (A)ctive  - synch from HADR_SRC to HADR_TGT in real time
# (P)assive - set up synch from HADR_SRC to HADR_TGT for batch synching 
# (N)one    - no synching to be done.

while (( INST <= MAXINST ))
do
  if [ ${HADR_MODE[$INST]} == "N" ]; then
     print "${0##*/}:  INFO, HADR Processing from source ${HADR_SRC[$INST]} to target ${HADR_TRGT[$INST]} is turned off." 
  else
    # create the appropriate data and state files for this synch instance.
    TMP_ROOT_DIR[$INST]=$DW_IN/sft/$PCGID.$INST
    PINST=$((INST - 1))
    ACTIVE_DFILE[$INST]=$DW_SA_TMP/$PCGID.$INST.data.sft.lis 
    ACTIVE_SFILE[$INST]=$DW_SA_TMP/$PCGID.$INST.state.sft.lis
    ACTIVE_MSTR_FILE[$INST]=$DW_MASTER_DAT/sft/pending/$PCGID.$INST.master.dat
    PASSIVE_DFILE[$INST]=${TMP_ROOT_DIR[$INST]}/$PCGID.$INST.data.sft.lis 
    PASSIVE_SFILE[$INST]=${TMP_ROOT_DIR[$INST]}/$PCGID.$INST.state.sft.lis
    PASSIVE_MSTR_FILE[$INST]=${TMP_ROOT_DIR[$INST]}/$PCGID.$INST.master.dat
    XFILE[$INST]=$DW_SA_TMP/$PCGID.$INST.data.excpt.lis
    ILFILE[$INST]=$DW_SA_LOG/$PCGID.$INST.synch_hadr_node.sft.$CURR_DATETIME.log
    LOGFILE[$INST]=$DW_SA_LOG/$PCGID.$INST.synch_hadr_node.$CURR_DATETIME.log
    MKRMTTMPDIR[$INST]=0
    PASSSTATE[$INST]=0
    PASSDATA[$INST]=0

    if [[  ${HADR_MODE[$INST]} == "A" ]]; then
      DFILE[$INST]=${ACTIVE_DFILE[$INST]} 
      SFILE[$INST]=${ACTIVE_SFILE[$INST]} 
      MSTR_FILE[$INST]=${ACTIVE_MSTR_FILE[$INST]} 
    else
      DFILE[$INST]=${PASSIVE_DFILE[$INST]} 
      SFILE[$INST]=${PASSIVE_SFILE[$INST]} 
      MSTR_FILE[$INST]=${PASSIVE_MSTR_FILE[$INST]} 
      mkdirifnotexist ${TMP_ROOT_DIR[$INST]}
    fi

    > ${DFILE[$INST]}
    > ${SFILE[$INST]}

    if [ -s $MFSLIS ]; then
      print "${0##*/}:  INFO, Process the mfs files for this instance" 
      PASSSTATE[$INST]=1
      PASSDATA[$INST]=1
        
      while read MFSCTLFN
      do
        TMFSCTLFN=$MFSCTLFN.$PCGID.$INST.mfs.mfctl.tmp
        # create temp mfctl file, replacing source with target system.
        cat $MFSCTLFN | sed -e "s/${HADR_SRC[$INST]}/${HADR_TRGT[$INST]}/g" > $TMFSCTLFN
        
        
        #create sft file list, with all source files
        grep '^  \"file:' $MFSCTLFN | while read fline
        do
          fname1=${fline##*file://$SFT_SNODE}
          fname=${fname1%%\"*}
          if [[ ${HADR_MODE[$INST]} == "A" ]]; then
            print "${HADR_TRGT[$INST]}:$fname,$fname" >> ${DFILE[$INST]}
          else
            if [[ ${HADR_SRC[$INST]} == "$SFT_SNODE" ]]; then
              #I am the source of the data, copy the files to TMP_ROOT_DIR, and populate DFILE appropriately.
              FNDIR=${fname%/*}
              TMPFN=${TMP_ROOT_DIR[$INST]}/$fname
              mkdirifnotexist ${TMP_ROOT_DIR[$INST]}/$FNDIR
              print "${0##*/}:  INFO, HADR Mode is Passive: Copying file $FN to $TMPFN" 
              cp ${fname} $TMPFN
              print "${HADR_TRGT[$INST]}:$TMPFN,${fname}" >> ${DFILE[$INST]}
            else
              print "${HADR_TRGT[$INST]}:$fname,$fname" >> ${DFILE[$INST]}
            fi
          fi
        done

        #add the temp control file to the state file list
        if [[ ${HADR_MODE[$INST]} == "A" ]]; then
           print "${HADR_TRGT[$INST]} ${MFSCTLFN} ${TMFSCTLFN}" >> ${SFILE[$INST]}
        else
          if [[ ${HADR_SRC[$INST]} == "$SFT_SNODE" ]]; then
            #I am the source of the data, copy the target TMFSCTLFN to TMP_ROOT_DIR, and populate DFILE appropriately.
            FNDIR=${TMFSCTLFN%/*}
            TMPFN=${TMP_ROOT_DIR[$INST]}/$TMFSCTLFN
            mkdirifnotexist ${TMP_ROOT_DIR[$INST]}/$FNDIR

            print "${0##*/}:  INFO, HADR Mode is (P)assive: Copying temp mfs Control file $MFSCTLFN to $TMPFN" 
            cp -p ${TMFSCTLFN} $TMPFN
            print "${HADR_TRGT[$INST]} ${MFSCTLFN} ${TMPFN}" >> ${SFILE[$INST]}
          else
            #I am not the source, put this temp MFSCTLFN into ${SFILE[$PINST]} as well as ${SFILE[$INST]} appropriately
            FNDIR=${MFSCTLFN%/*}
            TMPFN=${TMP_ROOT_DIR[$INST]}/$MFSCTLFN
            mkdirifnotexist ${TMP_ROOT_DIR[$INST]}/$FNDIR
            cp -p ${TMFSCTLFN} $TMPFN
            print "${HADR_TRGT[$PINST]} ${TMFSCTLFN} ${TMPFN}" >> ${SFILE[$PINST]}
            print "${HADR_TRGT[$INST]} ${MFSCTLFN} ${TMFSCTLFN}" >> ${SFILE[$INST]}
            MKRMTTMPDIR[$PINST]=1
          fi
        fi
        
      done < $MFSLIS
    fi

    if [[ -s $DATALIS ]]; then

      PASSDATA[$INST]=1

      while read FN
      do
        if [[ -f $FN ]]; then
          if [[ ${HADR_MODE[$INST]} == "A" ]]; then
            print "${HADR_TRGT[$INST]}:${FN},${FN}" >> ${DFILE[$INST]}
          else
            if [[ ${HADR_SRC[$INST]} == "$SFT_SNODE" ]]; then
              #I am the source of the data, copy the files to TMP_ROOT_DIR, and populate DFILE appropriately.
              FNDIR=${FN%/*}
              TMPFN=${TMP_ROOT_DIR[$INST]}/$FN
              mkdirifnotexist ${TMP_ROOT_DIR[$INST]}/$FNDIR
              print "${0##*/}:  INFO, HADR Mode is Passive: Copying file $FN to $TMPFN" 
              cp ${FN} $TMPFN
              print "${HADR_TRGT[$INST]}:$TMPFN,${FN}" >> ${DFILE[$INST]}
            else
              print "${HADR_TRGT[$INST]}:${FN},${FN}" >> ${DFILE[$INST]}
            fi
          fi
      else
        print "${0##*/}:  ERROR, Data File does not exist on source: $FN" >&2
        exit 4
      fi
      done < $DATALIS
    fi

    if [[ -s $STATELIS ]]; then

      PASSSTATE[$INST]=1

      while read FN
      do
        if [[ -f $FN ]]; then
          if [[ ${HADR_MODE[$INST]} == "A" ]]; then
             print "${HADR_TRGT[$INST]} ${FN} ${FN}" >> ${SFILE[$INST]}
          else
            if [[ ${HADR_SRC[$INST]} == "$SFT_SNODE" ]]; then
            #I am the source of the data, copy the files to TMP_ROOT_DIR, and populate DFILE appropriately.
              FNDIR=${FN%/*}
              TMPFN=${TMP_ROOT_DIR[$INST]}/$FN
              mkdirifnotexist ${TMP_ROOT_DIR[$INST]}/$FNDIR

              print "${0##*/}:  INFO, HADR Mode is (P)assive: Copying file $FN to $TMPFN" 
              cp -p ${FN} $TMPFN
              print "${HADR_TRGT[$INST]} ${FN} ${TMPFN}" >> ${SFILE[$INST]}
            else
              print "${HADR_TRGT[$INST]} ${FN} ${FN}" >> ${SFILE[$INST]}
            fi
          fi
        else
          print "${0##*/}:  ERROR, State File does not exist on source: $FN" >&2
          exit 4
        fi
      done < $STATELIS
    fi

    

    if [[ ${HADR_MODE[$INST]} != "A" && ${HADR_SRC[$INST]} != "$SFT_SNODE" ]]; then
      #copy files into SFILE for Prev instance
      print "${HADR_TRGT[$PINST]} ${PASSIVE_DFILE[$INST]} ${PASSIVE_DFILE[$INST]}" >> ${SFILE[$PINST]}
      print "${HADR_TRGT[$PINST]} ${PASSIVE_SFILE[$INST]} ${PASSIVE_SFILE[$INST]}" >> ${SFILE[$PINST]}
      print "${HADR_TRGT[$PINST]} ${ACTIVE_MSTR_FILE[$INST]} ${PASSIVE_MSTR_FILE[$INST]}" >> ${SFILE[$PINST]}
      MKRMTTMPDIR[$PINST]=1
    fi
  fi
  (( INST += 1 ))
done

INST=0
while (( INST <= MAXINST ))
do
  if [[ ${HADR_MODE[$INST]} != "N" ]]
  then
  NINST=$((INST + 1))
  PINST=$((INST - 1))

    #make the run options - used for active or passive processing
    OPTIOND=
    OPTIONP=
    OPTIONS=
    OPTIONT=

    (( PASSDATA[$INST] )) && OPTIOND="-d ${DFILE[$INST]}"
    (( PASSDATA[$INST] )) && OPTIONP=" -x ${XFILE[$INST]} -l ${ILFILE[$INST]} -p ${HADR_SFT_PORT[$INST]}"
    (( PASSSTATE[$INST] )) && OPTIONS="-s ${SFILE[$INST]}"
    (( MKRMTTMPDIR[$INST] )) && OPTIONT="-t ${TMP_ROOT_DIR[$NINST]} -r ${HADR_TRGT[$INST]}"
    
    if [[ ${HADR_MODE[$INST]} != "A" ]]; then
    #if not active - do not save -x, -l or -p params
      OPTIONLIS[$INST]="$OPTIOND $OPTIONS $OPTIONT"
      print "${OPTIONLIS[$INST]}" > ${MSTR_FILE[$INST]}
    else
       OPTIONLIS[$INST]="$OPTIOND $OPTIONP $OPTIONS $OPTIONT"
    fi
  fi
    (( INST += 1 ))
done

INST=0
while (( INST <= MAXINST ))
do
  NINST=$((INST + 1))
  if [[ ${HADR_MODE[$INST]} != "A" ]]; then
    (( INST += 1 ))
   else 

      set +e
      $DW_MASTER_BIN/dw_infra.synch_hadr_node.ksh ${OPTIONLIS[$INST]} > ${LOGFILE[$INST]} 2>&1
      rcode=$?
      set -e

      if (( rcode != 0 )); then
        print "${0##*/}:  ERROR, see instance log file ${LOGFILE[$INST]}"  >&2
        exit 4
      fi
      rm -f ${DFILE[$INST]} ${SFILE[$INST]} 
      (( INST += 1 ))
  fi
done

#Cleanup tmp files
rm -f $DATALIS $MFSLIS $STATELIS

exit 0
