#!/bin/ksh -eu
#------------------------------------------------------------------------------------------------
# Filename:     dw_infra.loader_cleanup.ksh
# Description:  Removes temp and watch files and renames data and log files from previous
#               processing period to.r4a.  Check BATCH_SEQ_NUM for all load processes to
#               determine which data files can be renamed.  Use the minimum BATCH_SEQ_NUM
#               from the load processes as the max BATCH_SEQ_NUM that can be archived.  A
#               delete date is appended to the data file names based on the number of days
#               the data files need to be retained (retrieved from the config file).
#
# Developer:    Craig Werre
# Created on:   10/05/2005
# Location:     $DW_MASTER_BIN/
#
# Execution:    $DW_MASTER_BIN/loader_cleanup.ksh <JOB_ENV> <JOB_TYPE_ID>
#
# Parameters:   JOB_TYPE_ID = <ex|ld|bt|dm>
#
# Revision History:
#
# Name             Date            Description
# ---------------  --------------  --------------------------------------------------------------
# Craig Werre      10/05/2005      Initial Creation
# Craig Werre      10/26/2005      Added LOAD_JOB_ENV logic to determine which environments
#                                  are being loaded
# Stanley Zhang    10/10/2007      Add unload archive statement
# Orlando Jin      08/22/2008      Add TD unload archive/purge process
# Kevin Oaks       06/16/2010      Added support for Multi Target Environment
# Kevin Oaks       12/05/2011      Added support for datamover and modified 'extract'
#                                  path to bypass datamove files.
# Ryan Wong        12/20/2011      Modified version of loader_cleanup.ksh.  Since we have date
#                                  based log dir, no longer need to mark these r4a.
# Ryan Wong        12/22/2011      Added code to mark uow date base dir data files
# Ryan Wong        05/17/2012      Added cleanup for UOW and local file extract list and tar files
#                                  based upon modified time
# Ryan Wong        09/12/2012      Updating to cleanup Local File Ex sources.lis file
# Ryan Wong        11/11/2012      Added cleanup for extract type tpt
# Ryan Wong        02/05/2013      Check there is an existing IN_DIR/$TABLE_ID/UOW_TO directory
#                                  before attempting to delete.
# Ryan Wong        04/19/2013      Adding UOW cleanup using UNIT_OF_WORK_FILE
# Ryan Wong        05/23/2013      Temp patch.  Skip UOW cleanup for DATA_RET_DAYS gt 0
# Ryan Wong        05/31/2013      Fix UOW cleanup for TPT
# Ryan Wong        06/27/2013      Zero-byte BSN file causes error when reading -- Add check
# Ryan Wong        07/15/2013      Fix UOW cleanup folder for DATA_RET_DAYS gt 0, add TABLE_ID to r4a folder name
# Ryan Wong        08/01/2013      Fix R4A_FILE list for UOW to be less than (lt) the MIN_LOAD_UNIT_OF_WORK
# Ryan Wong        10/04/2013      Redhat changes
#------------------------------------------------------------------------------------------------

JOB_ENV=$1        # extract, td1, td2, td3, td4, etc... ( primary, secondary, all -- deprecated )
JOB_TYPE_ID=$2    # ex, ld, bt, dm

UOW_TO=${UOW_TO:-""}

if [ $JOB_ENV == extract ]
then

    #----------------------------------------------------------------------------------------------------------
    # determine the number of days that data files need to be retained after being loaded.
    #----------------------------------------------------------------------------------------------------------

    set +e
    grep "^DATA_RET_DAYS\>" $DW_CFG/$ETL_ID.cfg | read PARAM DATA_RET_DAYS COMMENT
    rcode=$?
    set -e

    #  default DATA_RET_DAYS to 0 if it doesn't exist in the cfg file
    if [ $rcode == 1 ]
    then
        DATA_RET_DAYS=0
    elif [ $rcode != 0 ]
    then
        print "${0##*/}:  ERROR, failure determining value for DATA_RET_DAYS parameter from $DW_CFG/$ETL_ID.cfg" >&2
        exit 4
    fi

    #----------------------------------------------------------------------------------------------------------
    # DEL_DATE represents the date the data file can be deleted.  It will be appended to the data file
    # name at the time it is marked as ready for archive.
    #----------------------------------------------------------------------------------------------------------
    DEL_DATE=$($DW_EXE/add_days ${CURR_DATETIME%-*} $DATA_RET_DAYS)

    set +e
    grep "^EXTRACT_PROCESS_TYPE\>" $DW_CFG/$ETL_ID.cfg | read PARAM EXTRACT_PROCESS_TYPE COMMENT
    rcode=$?
    set -e
    if [[ $rcode != 0 ]]
    then
      print "${0##*/}:  ERROR, failure determining value for EXTRACT_PROCESS_TYPE parameter from $DW_CFG/$ETL_ID.cfg" >&2
      exit 4
    fi

    #----------------------------------------------------------------------------------------------------------
    # tpt calls dw_infra.loader_cleanup_handler.ksh which takes min bsn as a parameter
    #----------------------------------------------------------------------------------------------------------
    if [ $EXTRACT_PROCESS_TYPE = "T" ]
    then
      if [[ "X$MIN_LOAD_BATCH_SEQ_NUM" != "X" || "X$MIN_LOAD_UNIT_OF_WORK" != "X" ]]
      then
        print "--------------------------------------------------------------------------------------"
        print "minimum LOAD_BATCH_SEQ_NUM = $MIN_LOAD_BATCH_SEQ_NUM"
        print "minimum LOAD_UNIT_OF_WORK = $MIN_LOAD_UNIT_OF_WORK"
        print "--------------------------------------------------------------------------------------"
      elif [[ "X$UOW_TO" = "X" ]]
      then
        print "${0##*/}:  ERROR, EXTRACT_PROCESS_TYPE is T and both UOW_TO and (MIN_LOAD_BATCH_SEQ_NUM or MIN_LOAD_UNIT_OF_WORK) is undefined" >&2
        exit 4
      fi
    else
      #----------------------------------------------------------------------------------------------------------
      # LOAD_JOB_ENV is a pipe delimited list of all targets in use. A single target is stand alone sans pipe.
      # primary, secondary, all are deprecated in favor of td1, td2, etc... all resolves to td1|td2 until
      # functionality is completely deprecated from the production environment. Determines which load
      # BATCH_SEQ_NUM file(s) to use to determine when data files have been loaded and can be archived.
      #----------------------------------------------------------------------------------------------------------
      set +e
      grep "^LOAD_JOB_ENV\>" $DW_CFG/$ETL_ID.cfg | read PARAM LOAD_JOB_ENV COMMENT
      rcode=$?
      set -e

      if [ $rcode != 0 ]
      then
        print "${0##*/}:  ERROR, failure determining value for LOAD_JOB_ENV parameter from $DW_CFG/$ETL_ID.cfg" >&2
        exit 4
      fi

      # Convert legacy dual values to current multi env values
      case $LOAD_JOB_ENV in
              all)   LOAD_JOB_ENV="td1|td2";;
          primary)   LOAD_JOB_ENV=td1;;
        secondary)   LOAD_JOB_ENV=td2;;
      esac

      # Fill job environment array, count elements and initialize loop index to 0
      set -A LOAD_JOB_ENV_ARR `print "$LOAD_JOB_ENV"| awk -F'|' '{for(i=1; i<=NF; i++){printf("%s ", $i)}}'`
      integer ARR_ELEMS=${#LOAD_JOB_ENV_ARR[*]}
      integer idx=0

      # Make sure we have at least one array element
      if ((ARR_ELEMS == 0))
      then
        print "${0##*/}:  FATAL ERROR, invalid value for parameter LOAD_JOB_ENV: ($LOAD_JOB_ENV)" >&2
        exit 4
      fi

      # find minimum load batch sequence number or uow to direct cleanup
      while ((idx < ARR_ELEMS))
      do
        bsn_file=$DW_DAT/${LOAD_JOB_ENV_ARR[idx]}/$SUBJECT_AREA/$TABLE_ID.load.batch_seq_num.dat
        if [ -f $bsn_file ]
        then
          set +e
          cat $bsn_file | read LOAD_BATCH_SEQ_NUM
          rcode=$?
          set -e
          if [ $rcode != 0 ]
          then
            print "${0##*/}:  FATAL ERROR, failure reading file $bsn_file" >&2
            exit 4
          fi
        else
          print "${0##*/}:  FATAL ERROR, BATCH SEQUENCE NUMBER FILE $bsn_file does not exist." >&2
          exit 4
        fi

        if ((idx == 0))
        then
          MIN_LOAD_BATCH_SEQ_NUM=$LOAD_BATCH_SEQ_NUM
        elif [ $LOAD_BATCH_SEQ_NUM -lt $MIN_LOAD_BATCH_SEQ_NUM ]
        then
          MIN_LOAD_BATCH_SEQ_NUM=$LOAD_BATCH_SEQ_NUM
        fi

        if [[ "X$UOW_TO" != "X" ]]
        then
          uow_file=$DW_DAT/${LOAD_JOB_ENV_ARR[idx]}/$SUBJECT_AREA/$TABLE_ID.load.uow.dat
          #
          # If uow_file does not exist, default = UOW_TO_DATE - 31
          #
          if [ -f $uow_file ]
          then
            set +e
            cat $uow_file | read LOAD_UNIT_OF_WORK
            rcode=$?
            set -e
            if [ $rcode != 0 ]
            then
              print "${0##*/}:  FATAL ERROR, failure reading file $uow_file" >&2
              exit 4
            fi
            LOAD_UNIT_OF_WORK=$(print $LOAD_UNIT_OF_WORK | cut -c1-8)
          else
            UOW_TO_DATE_TMP=$(print $UOW_TO | cut -c1-8)
            LOAD_UNIT_OF_WORK=$($DW_EXE/add_days $UOW_TO_DATE_TMP -31)
            print "${LOAD_UNIT_OF_WORK}000000" > $uow_file
          fi

          if ((idx == 0))
          then
            MIN_LOAD_UNIT_OF_WORK=$LOAD_UNIT_OF_WORK
          elif [ $LOAD_UNIT_OF_WORK -lt $MIN_LOAD_UNIT_OF_WORK ]
          then
            MIN_LOAD_UNIT_OF_WORK=$LOAD_UNIT_OF_WORK
          fi
        fi

        ((idx+=1))
      done

      print "--------------------------------------------------------------------------------------"
      print "LOAD JOB ENVIRONMENTS for this ETL_ID: ${LOAD_JOB_ENV_ARR[*]}"
      print "minimum LOAD_BATCH_SEQ_NUM = $MIN_LOAD_BATCH_SEQ_NUM"
      if [[ "X$UOW_TO" != "X" ]]
      then
        print "minimum LOAD_UNIT_OF_WORK = $MIN_LOAD_UNIT_OF_WORK"
      fi
      print "--------------------------------------------------------------------------------------"
    fi

    #----------------------------------------------------------------------------------------------------------
    # Moving files to r4a directory
    #----------------------------------------------------------------------------------------------------------
    R4A_FILE=$DW_SA_TMP/$TABLE_ID.$JOB_TYPE_ID.r4a.$DEL_DATE
    TMP_FIND_FILE=$DW_SA_TMP/$TABLE_ID.$JOB_TYPE_ID.find
    if [[ "X$UOW_TO" != "X" ]]
    then
      set +e
      find $DW_SA_IN/$TABLE_ID -name '20??????' -type d -prune > $TMP_FIND_FILE 2>/dev/null
      rcode=$?
      set -e

      if [ -s $TMP_FIND_FILE ]
      then
        FIRST_FILE=1
        for fn in $(<$TMP_FIND_FILE)
        do
          FILE_UNIT_OF_WORK=${fn##*/}
          if [ $FILE_UNIT_OF_WORK -lt $MIN_LOAD_UNIT_OF_WORK ]
          then
            if [ $FIRST_FILE = 1 ]
            then
              print $fn>$R4A_FILE
              FIRST_FILE=0
            else
              print $fn>>$R4A_FILE
            fi
          fi
        done
      fi
    else
      #----------------------------------------------------------------------------------------------------------
      # rename data files and record count files with a batch sequence number <= to the minimum
      # batch sequence number from any of the load processes for this TABLE_ID.
      #----------------------------------------------------------------------------------------------------------
      print "Moving data files loaded through batch sequence number $MIN_LOAD_BATCH_SEQ_NUM to r4a directory"

      # For compressed files, pull compression suffix from filename before determining batch_seq_num
      # Check for compressed files even when compression may not be active, and vise versa, in case recent change
      set +e
      grep "^CNDTL_COMPRESSION_SFX\>" $DW_CFG/$ETL_ID.cfg | read PARAM CNDTL_COMPRESSION_SFX COMMENT
      rcode=$?
      set -e
      if [ $rcode != 0 ]
      then
        CNDTL_COMPRESSION_SFX=".gz"
      fi

      # Added bz2 for tpt
      set +e
      grep "^TPT_EXTRACT_COMPRESS_FLAG\>" $DW_CFG/$ETL_ID.cfg | read PARAM TPT_EXTRACT_COMPRESS_FLAG COMMENT
      rcode=$?
      set -e
      if [[ $rcode = 0 && $TPT_EXTRACT_COMPRESS_FLAG = 2 ]]
      then
      CNDTL_COMPRESSION_SFX=".bz2"
      fi

      if [ -f $DW_SA_IN/$TABLE_ID.!(*.r4a|datamove.*) ]
      then
        FIRST_FILE=1
        for fn in $DW_SA_IN/$TABLE_ID.!(*.r4a|datamove.*)
        do
          if [ ${fn##*.} = ${CNDTL_COMPRESSION_SFX#.} ]
          then
             RM_EXTENSION=${fn%.*}
             FILE_BATCH_SEQ_NUM=${RM_EXTENSION##*.}
          else
             FILE_BATCH_SEQ_NUM=${fn##*.}
          fi

          if [ $FILE_BATCH_SEQ_NUM -le $MIN_LOAD_BATCH_SEQ_NUM ]
          then
            if [ $FIRST_FILE = 1 ]
            then
              print $fn>$R4A_FILE
              FIRST_FILE=0
            else
              print $fn>>$R4A_FILE
            fi
          fi
        done
      fi
    fi

    if [ -f $R4A_FILE ]
    then
      if [ $DATA_RET_DAYS -le 0 ]
      then
        # Add for r4r logic
        if [ ! -d $DW_SA_IN/tr4r_${TABLE_ID}_${CURR_DATETIME} ]
        then
          set +e
          mkdir $DW_SA_IN/tr4r_${TABLE_ID}_${CURR_DATETIME}
          rcode=?
          set -e
          if [[ $rcode > 0 && ! -d $DW_SA_IN/tr4r_${TABLE_ID}_${CURR_DATETIME} ]]
          then
            print "Failed creating $DW_SA_IN/tr4r_${TABLE_ID}_${CURR_DATETIME} directory"
            exit 4
          fi
        fi
                 
        xargs $DW_LIB/mass_mv $DW_SA_IN/tr4r_${TABLE_ID}_${CURR_DATETIME} < $R4A_FILE
        mv $DW_SA_IN/tr4r_${TABLE_ID}_${CURR_DATETIME} $DW_SA_IN/r4r_${TABLE_ID}_${CURR_DATETIME}
      elif [ $DATA_RET_DAYS -gt 0 ]
      then
        if [ ! -d $DW_SA_IN/r4a_${TABLE_ID}_${DEL_DATE} ]
          then
          # Trap error in case another ETL gets to the dir check while still creating dir
          # As long as directory exists after failure assume error was directory exists
          set +e
          mkdir $DW_SA_IN/r4a_${TABLE_ID}_${DEL_DATE}
          rcode=$?
          set -e
          if [[ $rcode > 0 && ! -d $DW_SA_IN/r4a_${TABLE_ID}_${DEL_DATE} ]]
          then
            print "Failed creating $DW_SA_IN/r4a_${TABLE_ID}_${DEL_DATE} directory"
            exit 4
          fi
        fi

        xargs $DW_LIB/mass_mv $DW_SA_IN/r4a_${TABLE_ID}_${DEL_DATE} < $R4A_FILE
      fi
    fi

    # multifile system data files
    if [ -f $DW_MFS/fs??/in/extract/$SUBJECT_AREA/$TABLE_ID.!(*.r4a|datamove.*) ]
    then
        for fn in $DW_MFS/fs??/in/extract/$SUBJECT_AREA/$TABLE_ID.!(*.r4a|datamove.*)
        do
            FILE_BATCH_SEQ_NUM=${fn##*.}

            if [ $FILE_BATCH_SEQ_NUM -le $MIN_LOAD_BATCH_SEQ_NUM ]
            then
                m_mv $fn $fn.$DEL_DATE.r4a
            fi
        done
    fi

    #----------------------------------------------------------------------------------------------------------
    # remove extract watch files with a batch sequence number <= to the minimum batch sequence number
    # from any of the load processes for this TABLE_ID.
    #----------------------------------------------------------------------------------------------------------
    print "Removing extract watch files for data loaded through batch sequence number $MIN_LOAD_BATCH_SEQ_NUM"
    if [ -f $DW_WATCH/extract/$ETL_ID.$JOB_TYPE.*.done ]
    then
        for fn in $DW_WATCH/extract/$ETL_ID.$JOB_TYPE.*.done
        do
            FILE_BATCH_SEQ_NUM=${fn%.done}
            FILE_BATCH_SEQ_NUM=${FILE_BATCH_SEQ_NUM##*.}

            if [ $FILE_BATCH_SEQ_NUM -le $MIN_LOAD_BATCH_SEQ_NUM ]
            then
                rm -f $fn
            fi
        done
    fi

    #----------------------------------------------------------------------------------------------------------
    # remove Batch Level sources.lis files for local_file_processing which have already been consumed
    #----------------------------------------------------------------------------------------------------------
    if [[ "X$UOW_TO" != "X" ]]
    then
        set +e
        find $DW_SA_DAT -name $ETL_ID.sources.lis*.20???????????? -prune -type f -mtime +31 -exec rm -rf {} \;
        set -e
    else
        for f in $DW_SA_DAT/$ETL_ID.sources.lis.*
        do
            if [ -f $f ]
            then
                BATCH_SEQ_NUM_EXTRACT=${f##*.}
                if [ $BATCH_SEQ_NUM_EXTRACT -le $MIN_LOAD_BATCH_SEQ_NUM ]
                then
                    rm -f $f
                fi
            fi
        done
    fi

    #----------------------------------------------------------------------------------------------------------
    # remove Batch Level tar files for local_file_processing which have already been consumed
    #----------------------------------------------------------------------------------------------------------

    set +e
    grep "^EXTRACT_PROCESS_TYPE\>" $DW_CFG/$ETL_ID.cfg 2>/dev/null | read PARAM EXTRACT_PROCESS_TYPE COMMENT
    set -e

    if [ $EXTRACT_PROCESS_TYPE == "L" ]
    then
        while read RECORD_ID LANDDIR FNL
        do
            set +e
            LANDDIR=$(eval print $LANDDIR)
            set -e

            if [[ "X$UOW_TO" != "X" ]]
            then
                set +e
                find $LANDDIR -name $TABLE_ID.*lfe.*.tar -prune -type f -mtime +31 -exec rm -rf {} \;
                set -e
            else
                for f in $LANDDIR/$TABLE_ID.*lfe.*.tar
                do
                    TAR_BATCH_SEQ_NUM=${f%.tar}
                    TAR_BATCH_SEQ_NUM=${TAR_BATCH_SEQ_NUM##*.}

                    if [ $TAR_BATCH_SEQ_NUM -le $MIN_LOAD_BATCH_SEQ_NUM ]
                    then
                        rm -f $f
                    fi
                done
            fi
        done < $DW_CFG/$ETL_ID.sources.lis
    fi
fi

##############################################################################################
# Section Added to Support Batch Data Mover
##############################################################################################
if [[ $JOB_TYPE_ID == dm ]]
then
   # Determine if extract or load
   set +e
   grep "^DM_SRC_ENV\>" $DW_CFG/$ETL_ID.cfg | read PARAM DM_SRC_ENV COMMENT
   rcode=$?
   set -e
   
   if [[ $rcode -eq 0 ]]
   then
      if [[ $JOB_ENV == $DM_SRC_ENV ]]
      then
         EXEC_MODE=E
      else
         EXEC_MODE=L
      fi
   else
      print "${0##*/}: FATAL ERROR, Unable to resolve parameter DM_SRC_ENV from $DW_CFG/$ETL_ID.cfg." >2
      exit 4
   fi

   if [[ $EXEC_MODE == E ]]
   then
      # Determine minimum load batch seq num
      set +e
      grep "^DM_TRGT_ENV\>" $DW_CFG/$ETL_ID.cfg | read PARAM DM_TRGT_ENV COMMENT                    
      rcode=$?
      set -e

      if [[ $rcode -eq 0 ]]
      then
         print "DM_TRGT_ENV == $DM_TRGT_ENV"
      else
         print "${0##*/}: FATAL ERROR, Unable to resolve parameter DM_TRGT_ENV from $DW_CFG/$ETL_ID.cfg." >2
         exit 4
      fi

      # Fill job environment array, count elements and initialize loop index to 0
      set -A DM_TRGT_ENV_ARR `print "$DM_TRGT_ENV"| awk -F',' '{for(i=1; i<=NF; i++){printf("%s ", $i)}}'`
      integer DM_TRGT_ENV_ARR_ELEMS=${#DM_TRGT_ENV_ARR[*]}
      integer idx=0
  
      # Make sure we have at least one array element
      if ((DM_TRGT_ENV_ARR_ELEMS == 0))
      then
         print "${0##*/}:  FATAL ERROR, invalid value for parameter DM_TRGT_ENV: ($DM_TRGT_ENV)" >&2
         exit 4
      fi

      # find minimum load batch sequence number to direct cleanup
      while ((idx < DM_TRGT_ENV_ARR_ELEMS))
      do
         if [[ "X$UOW_TO" != "X" ]]
         then
            uow_file=$DW_DAT/${DM_TRGT_ENV_ARR[idx]}/$SUBJECT_AREA/$TABLE_ID.datamove.trgt.uow.dat
            #
            # If uow_file does not exist, default = UOW_TO_DATE - 31
            #
            if [ -f $uow_file ]
            then
               set +e
               cat $uow_file | read DM_TRGT_UNIT_OF_WORK
               rcode=$?
               set -e
               if [ $rcode != 0 ]
               then
                 print "${0##*/}:  FATAL ERROR, failure reading file $uow_file" >&2
                 exit 4
               fi
               DM_TRGT_UNIT_OF_WORK=$(print $DM_TRGT_UNIT_OF_WORK | cut -c1-8)
            else
               UOW_TO_DATE_TMP=$(print $UOW_TO | cut -c1-8)
               DM_TRGT_UNIT_OF_WORK=$($DW_EXE/add_days $UOW_TO_DATE_TMP -31)
               print "${DM_TRGT_UNIT_OF_WORK}000000" > $uow_file
            fi

            if ((idx == 0))
            then
               MIN_DM_TRGT_UNIT_OF_WORK=$DM_TRGT_UNIT_OF_WORK
            elif [[ $DM_TRGT_UNIT_OF_WORK -lt $MIN_DM_TRGT_UNIT_OF_WORK ]]
            then
               MIN_DM_TRGT_UNIT_OF_WORK=$DM_TRGT_UNIT_OF_WORK
            fi
         else
            bsn_file=$DW_DAT/${DM_TRGT_ENV_ARR[idx]}/$SUBJECT_AREA/$TABLE_ID.datamove.trgt.batch_seq_num.dat
            if [ -f $bsn_file ]
            then
               set +e
               cat $bsn_file | read DM_TRGT_BATCH_SEQ_NUM
               rcode=$?
               set -e
               if [ $rcode != 0 ]
               then
                 print "${0##*/}:  FATAL ERROR, failure reading file $bsn_file" >&2
                 exit 4
               fi
            else
               print "${0##*/}:  FATAL ERROR, BATCH SEQUENCE NUMBER FILE $bsn_file does not exist." >&2
               exit 4
            fi

            if ((idx == 0))
            then
               MIN_DM_TRGT_BATCH_SEQ_NUM=$DM_TRGT_BATCH_SEQ_NUM
            elif [[ $DM_TRGT_BATCH_SEQ_NUM -lt $MIN_DM_TRGT_BATCH_SEQ_NUM ]]
            then
               MIN_DM_TRGT_BATCH_SEQ_NUM=$DM_TRGT_BATCH_SEQ_NUM
            fi
         fi

         ((idx+=1))
      done

      print "--------------------------------------------------------------------------------------"
      print "TARGET JOB ENVIRONMENTS for this ETL_ID: ${DM_TRGT_ENV_ARR[*]}"
      if [[ "X$UOW_TO" != "X" ]]
      then
         print "minimum MIN_DM_TRGT_UNIT_OF_WORK == $MIN_DM_TRGT_UNIT_OF_WORK"
         print "--------------------------------------------------------------------------------------"
         print "Deleting source files loaded through unit of work $MIN_DM_TRGT_UNIT_OF_WORK"
      else
         print "minimum DM_TRGT_BATCH_SEQ_NUM == $MIN_DM_TRGT_BATCH_SEQ_NUM"
         print "--------------------------------------------------------------------------------------"
         print "Deleting source files loaded through batch sequence number $MIN_DM_TRGT_BATCH_SEQ_NUM"
      fi

      set +e
      grep "^DM_DATA_DIR\>" $DW_CFG/$ETL_ID.cfg | read PARAM DM_DATA_DIR COMMENT
      rcode=$?
      set -e
      
      if [[ $rcode -eq 0 ]]
      then
         DM_DATA_DIR=$(eval print $DM_DATA_DIR)
         print "DM_DATA_DIR == $DM_DATA_DIR"
      else
         print "${0##*/}: FATAL ERROR, Unable to determine DM_DATA_DIR from $DW_CFG/$ETL_ID.cfg" >2
         exit 4
      fi
      
      # Determine if DM_DATA_DIR is a multifile system
      if [[ -f $DM_DATA_DIR/.mdir ]]
      then
         DM_DD_MFS=1
      else
         DM_DD_MFS=0
      fi

      if [[ "X$UOW_TO" != "X" ]]
      then
         # Handle UOW directory
         TMP_FIND_FILE=$DW_SA_TMP/$TABLE_ID.$JOB_TYPE_ID.find
         set +e
         find $DM_DATA_DIR/$JOB_ENV/$SUBJECT_AREA/$TABLE_ID -name '20??????' -type d -prune > $TMP_FIND_FILE 2>/dev/null
         rcode=$?
         set -e

         if [ -s $TMP_FIND_FILE ]
         then
            for fn in $(<$TMP_FIND_FILE)
            do
               FILE_UNIT_OF_WORK=${fn##*/}
               if [ $FILE_UNIT_OF_WORK -le $MIN_DM_TRGT_UNIT_OF_WORK ]
               then
                  if [[ $DM_DD_MFS -eq 0 ]]
                  then
                     print "Removing uow directory $fn"
                     rm -rf $fn
                  elif [[ $DM_DD_MFS -eq 1 ]]
                  then
                     print "Removing multi-file directory $fn"
                     m_rm -rf $fn
                  else
                     print "${0##*/}: FATAL ERROR, unable to determine file system type." >2
                  fi
               fi
            done
         fi
      else
         for fn in $DM_DATA_DIR/$JOB_ENV/$SUBJECT_AREA/$TABLE_ID.datamove.dat.!(*.r4a)
         do
            FILE_BATCH_SEQ_NUM=${fn##*.}
            if [ $FILE_BATCH_SEQ_NUM -le $MIN_DM_TRGT_BATCH_SEQ_NUM ]
            then
               if [[ $DM_DD_MFS -eq 0 ]]
               then
                  print "Removing file $fn"
                  rm -f $fn
               elif [[ $DM_DD_MFS -eq 1 ]]
               then
                  print "Removing multi-file $fn"
                  m_rm -f $fn
               else
                  print "${0##*/}: FATAL ERROR, unable to determine file system type." >2
               fi
            fi
         done 
      fi
   fi
fi

if [[ $JOB_TYPE_ID = unload || $JOB_TYPE_ID = ex ]]
then
  
  #----------------------------------------------------------------------------------------------------------
  # Determine the number of days that unload data files need to be retained
  #----------------------------------------------------------------------------------------------------------
  set +e
  grep "^UNLOAD_DATA_ARC_DAYS\>" $DW_CFG/$ETL_ID.cfg | read PARAM UNLOAD_DATA_ARC_DAYS COMMENT
  rcode=$?
  set -e
  
  #----------------------------------------------------------------------------------------------------------
  # Archive only if $UNLOAD_DATA_ARC_DAYS is assigned value in the cfg file
  #----------------------------------------------------------------------------------------------------------
  if [ $rcode = 0 ]
  then
    
    #----------------------------------------------------------------------------------------------------------
    # Assign DW_SA_OUT without exported by multi_unload_handler.ksh.
    # Recommend to move the DW_SA_OUT statement before quoting loader_cleanup.ksh by multi_unload_handler.ksh.
    #----------------------------------------------------------------------------------------------------------
    set +e
    if [ $JOB_TYPE_ID = unload ]
    then
      grep "^OUT_DIR\>" $DW_CFG/$ETL_ID.cfg | read PARAM OUT_DIR COMMENT
    elif [ $JOB_TYPE_ID = ex ]
    then
      grep "^IN_DIR\>" $DW_CFG/$ETL_ID.cfg | read PARAM OUT_DIR COMMENT
    fi
    rcode=$?
    set -e
    if [ $rcode != 0 ]
    then
      print "${0##*/}:  ERROR, failure determining value for OUT_DIR parameter from $DW_CFG/$ETL_ID.cfg" >&2
      exit 4
    fi
    DW_SA_OUT=`eval print $OUT_DIR`/$JOB_ENV/$SUBJECT_AREA

    ARC_DATE=$($DW_EXE/add_days ${CURR_DATETIME%-*} $UNLOAD_DATA_ARC_DAYS)

    #----------------------------------------------------------------------------------------------------------
    # Move the data files older than $UNLOAD_DATA_ARC_DAYS days
    #----------------------------------------------------------------------------------------------------------
    print "Removing unload files greater than $UNLOAD_DATA_ARC_DAYS days old"

    #----------------------------------------------------------------------------------------------------------
    # Archive the file name with TABLE_ID
    #----------------------------------------------------------------------------------------------------------
    R4A_FILE=$DW_SA_TMP/$TABLE_ID.$JOB_TYPE_ID.r4a.$ARC_DATE
    #find $DW_SA_OUT/ -type d ! -name "" -prune -o -type f -mtime +$UNLOAD_DATA_ARC_DAYS -name "*$TABLE_ID.*" -print > $R4A_FILE
    set +e
    find $DW_SA_OUT/ -type f -mtime +$UNLOAD_DATA_ARC_DAYS -name "*$TABLE_ID.*" -print > $R4A_FILE
    set -e

    #print "find $DW_SA_OUT/ -type d ! -name \"\" -prune -o -type f -mtime +$UNLOAD_DATA_ARC_DAYS -name \"*$TABLE_ID.*\" -print > $R4A_FILE"
    print "find $DW_SA_OUT/ -type f -mtime +$UNLOAD_DATA_ARC_DAYS -name \"*$TABLE_ID.*\" -print > $R4A_FILE"
    #----------------------------------------------------------------------------------------------------------
    # Archive the file name with UNLOAD_ARC_FILENAME from cfg file - optional
    #----------------------------------------------------------------------------------------------------------
    set +e
    grep "^UNLOAD_ARC_FILENAME\>" $DW_CFG/$ETL_ID.cfg | read PARAM UNLOAD_ARC_FILENAME COMMENT
    rcode=$?
    set -e
    if [ $rcode = 0 ]
    then
      R4A_FILE_OPT=$DW_SA_TMP/$TABLE_ID.$JOB_TYPE_ID.r4a.$ARC_DATE.opt
      R4A_FILE_TMP=$DW_SA_TMP/$TABLE_ID.$JOB_TYPE_ID.r4a.$ARC_DATE.tmp
      #find $DW_SA_OUT/ -type d ! -name "" -prune -o -type f -mtime +$UNLOAD_DATA_ARC_DAYS -name "*$UNLOAD_ARC_FILENAME.*" -print > $R4A_FILE_OPT
      set +e
      find $DW_SA_OUT/ -type f -mtime +$UNLOAD_DATA_ARC_DAYS -name "*$UNLOAD_ARC_FILENAME.*" -print > $R4A_FILE_OPT
      set -e
      # Merge $R4A_FILE_TMP to $R4A_FILE
        sort -d $R4A_FILE $R4A_FILE_OPT > $R4A_FILE_TMP
        mv -f $R4A_FILE_TMP $R4A_FILE
        rm -f $R4A_FILE_OPT
    fi
    cat $R4A_FILE

    if [ -f $R4A_FILE ]
    then

      while read FILE
      do
        ARC_FN=${FILE##*/}
        DEL_DIR=${FILE%/*}

        #---------------------------------------------------------------
        # Don't compress files that are already compressed
        #---------------------------------------------------------------
        if [[ ${ARC_FN##*.} = 'gz' || ${ARC_FN##*.} = 'Z' ]]
        then
          cp $FILE $DW_ARC/$JOB_ENV/$SUBJECT_AREA/${ARC_FN%.*}.$ARC_DATE.${ARC_FN##*.}
          rm -f $FILE
        else
          gzip -c $FILE > $DW_ARC/$JOB_ENV/$SUBJECT_AREA/$ARC_FN.$ARC_DATE.gz
          rm -f $FILE
        fi
      done < $R4A_FILE

      fi

  elif [ $rcode = 1 ]
  then
    print "${0##*/}:  Skip archive due to no UNLOAD_DATA_ARC_DAYS parameter from $DW_CFG/$ETL_ID.cfg" >&2

  elif [ $rcode != 0 ]
  then
    print "${0##*/}:  ERROR, failure determining value for UNLOAD_DATA_ARC_DAYS parameter from $DW_CFG/$ETL_ID.cfg" >&2
    exit 4
  fi



  #----------------------------------------------------------------------------------------------------------
  # Delete the file name older than UNLOAD_DATA_DEL_DAYS from cfg file under $DW_ARC
  #----------------------------------------------------------------------------------------------------------
  set +e
  grep "^UNLOAD_DATA_DEL_DAYS\>" $DW_CFG/$ETL_ID.cfg | read PARAM UNLOAD_DATA_DEL_DAYS COMMENT
  rcode=$?
  set -e

  if [ $rcode = 0 ]
  then
     set +e
    find $DW_ARC/$JOB_ENV/$SUBJECT_AREA -mtime +$UNLOAD_DATA_DEL_DAYS -name "*$TABLE_ID.*.gz" -print -o -name "*$TABLE_ID.*.Z" -print | xargs rm -f;
    set -e

    #----------------------------------------------------------------------------------------------------------
    # Archive the file name with UNLOAD_ARC_FILENAME from cfg file - optional
    #----------------------------------------------------------------------------------------------------------
    set +e
    grep "^UNLOAD_ARC_FILENAME\>" $DW_CFG/$ETL_ID.cfg | read PARAM UNLOAD_ARC_FILENAME COMMENT
    rcode=$?
    set -e
    if [ $rcode = 0 ]
    then
       set +e
      find $DW_ARC/$JOB_ENV/$SUBJECT_AREA -mtime +$UNLOAD_DATA_DEL_DAYS -name "*$UNLOAD_ARC_FILENAME.*.gz" -print -o -name "*$UNLOAD_ARC_FILENAME.*.Z" -print | xargs rm -f;
      set -e
    fi

  elif [ $rcode = 1 ]
  then
    print "${0##*/}:  Skip deletion due to no UNLOAD_DATA_DEL_DAYS parameter from $DW_CFG/$ETL_ID.cfg" >&2

  elif [ $rcode != 0 ]
  then
    print "${0##*/}:  ERROR, failure determining value for UNLOAD_DATA_DEL_DAYS parameter from $DW_CFG/$ETL_ID.cfg" >&2
    exit 4
  fi


fi


if [ $JOB_TYPE_ID != bt ]
then
    #------------------------------------------------------------------------
    #  Remove extract and load temp files.
    #------------------------------------------------------------------------
    print "Removing temp files"

    if [ -f $DW_SA_TMP/$TABLE_ID.$JOB_TYPE_ID.* ]
    then
        for fn in $DW_SA_TMP/$TABLE_ID.$JOB_TYPE_ID.*
        do
            rm -f $fn
        done
    fi

    if [ -f $DW_MFS/fs??/tmp/$JOB_ENV/$SUBJECT_AREA/$TABLE_ID.$JOB_TYPE_ID.* ]
    then
        for fn in $DW_MFS/fs??/tmp/$JOB_ENV/$SUBJECT_AREA/$TABLE_ID.$JOB_TYPE_ID.*
        do
            m_rm -f $fn
        done
    fi


    #------------------------------------------------------------------------
    #  Move extract and load log/err files to the archive directory.
    #------------------------------------------------------------------------
#    print "Marking log files from previous processing periods as .r4a"
#
#    if [ -f $DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.!(*.r4a|*$CURR_DATETIME.*) ]
#    then
#        for fn in $DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.!(*.r4a|*$CURR_DATETIME.*)
#        do
#            if [[ ${fn##*.} == err && ! -s $fn ]]
#            then
#                rm -f $fn     # remove empty error files
#            else
#                mv -f $fn $fn.r4a
#            fi
#        done
#    fi
#
#    if [[ $JOB_TYPE_ID == dm ]]
#    then
#       if [ -f $DW_SA_LOG/$TABLE_ID.bt.*.datamove.!(*.r4a|*$CURR_DATETIME.*) ]
#       then
#           for fn in $DW_SA_LOG/$TABLE_ID.bt.*.datamove.!(*.r4a|*$CURR_DATETIME.*)
#           do
#               mv $fn $fn.r4a
#           done
#       fi
#    fi

else
    #------------------------------------------------------------------------
    #  Remove bteq temp files.
    #------------------------------------------------------------------------
    print "Removing temp files"

    if [ -f $DW_SA_TMP/$TABLE_ID.$JOB_TYPE_ID.${SQL_FILE_BASENAME}.* ]
    then
        for fn in $DW_SA_TMP/$TABLE_ID.$JOB_TYPE_ID.${SQL_FILE_BASENAME}.*
        do
            rm -f $fn
        done
    fi

    #------------------------------------------------------------------------
    #  Move bteq log/err files to the archive directory.
    #------------------------------------------------------------------------
#    print "Marking log files from previous processing periods as .r4a"
#
#    if [ -f $DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.*${SQL_FILE_BASENAME}.!(*.r4a|*$CURR_DATETIME.*) ]
#    then
#        for fn in $DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.*${SQL_FILE_BASENAME}.!(*.r4a|*$CURR_DATETIME.*)
#        do
#            if [[ ${fn##*.} == err && ! -s $fn ]]
#            then
#                rm -f $fn     # remove empty error files
#            else
#                mv -f $fn $fn.r4a
#            fi
#        done
#    fi
fi

print "loader cleanup process complete"

exit
