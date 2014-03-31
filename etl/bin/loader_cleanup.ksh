#!/bin/ksh -eu

#------------------------------------------------------------------------------------------------
# Filename:     loader_cleanup.ksh
# Description:  Removes temp and watch files and renames data and log files from previous
#               processing period to.r4a.  Check BATCH_SEQ_NUM for all load processes to
#               determine which data files can be renamed.  Use the minimum BATCH_SEQ_NUM
#               from the load processes as the max BATCH_SEQ_NUM that can be archived.  A
#               delete date is appended to the data file names based on the number of days
#               the data files need to be retained (retrieved from the config file).
#
# Developer:    Craig Werre
# Created on:   10/05/2005
# Location:     $DW_EXE/
#
# Execution:    $DW_EXE/loader_cleanup.ksh <JOB_ENV> <JOB_TYPE_ID>
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
#
# Kevin Oaks       08/20/2012      Started Port to RedHat:
#                                  - now using /bin/ksh rather than /usr/bin/ksh
#                                  - converted all echo statements to print
#                                  - deprecated primary/secondary conversions
#------------------------------------------------------------------------------------------------

JOB_ENV=$1        # extract, td1, td2, td3, td4, etc... ( primary, secondary, all -- deprecated )
JOB_TYPE_ID=$2    # ex, ld, bt, dm

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


    #----------------------------------------------------------------------------------------------------------
    # LOAD_JOB_ENV is a pipe delimited list of all targets in use. A single target is stand alone sans pipe.
    # Determines which load BATCH_SEQ_NUM file(s) to use to determine when data files have been loaded and
    # can be archived.
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
#    case $LOAD_JOB_ENV in
#            all)   LOAD_JOB_ENV="td1|td2";;
#        primary)   LOAD_JOB_ENV=td1;;
#      secondary)   LOAD_JOB_ENV=td2;;
#    esac

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

    # find minimum load batch sequence number to direct cleanup
    while ((idx < ARR_ELEMS))
    do
        bsn_file=$DW_DAT/${LOAD_JOB_ENV_ARR[idx]}/$SUBJECT_AREA/$TABLE_ID.load.batch_seq_num.dat
        if [ -f $bsn_file ]
        then
            cat $bsn_file | read LOAD_BATCH_SEQ_NUM
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

        ((idx+=1))
    done

    print "--------------------------------------------------------------------------------------"
    print "LOAD JOB ENVIRONMENTS for this ETL_ID: ${LOAD_JOB_ENV_ARR[*]}"
    print "minimum LOAD_BATCH_SEQ_NUM = $MIN_LOAD_BATCH_SEQ_NUM"
    print "--------------------------------------------------------------------------------------"

    #----------------------------------------------------------------------------------------------------------
    # rename data files and record count files with a batch sequence number <= to the minimum
    # batch sequence number from any of the load processes for this TABLE_ID.
    #----------------------------------------------------------------------------------------------------------

    print "Moving data files loaded through batch sequence number $MIN_LOAD_BATCH_SEQ_NUM to r4a directory"

    if [ -f $DW_SA_IN/$TABLE_ID.!(*.r4a|datamove.*) ]
    then

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

        R4A_FILE=$DW_SA_TMP/$TABLE_ID.$JOB_TYPE_ID.r4a.$DEL_DATE
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
                       if [ ! -d $DW_SA_IN/r4a_$DEL_DATE ]
                then
                # Trap error in case another ETL gets to the dir check while still creating dir
                # As long as directory exists after failure assume error was directory exists
                set +e
                mkdir $DW_SA_IN/r4a_$DEL_DATE
                rcode=$?
                set -e
                if [[ $rcode > 0 && ! -d $DW_SA_IN/r4a_$DEL_DATE ]]
                then
                    print "Failed creating $DW_SA_IN/r4a_$DEL_DATE directory"
                    exit 4
                fi
            fi

            xargs $DW_LIB/mass_mv $DW_SA_IN/r4a_$DEL_DATE < $R4A_FILE
        fi
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

            for f in $LANDDIR/$TABLE_ID.*lfe.*.tar
            do
                TAR_BATCH_SEQ_NUM=${f%.tar}
                TAR_BATCH_SEQ_NUM=${TAR_BATCH_SEQ_NUM##*.}

                if [ $TAR_BATCH_SEQ_NUM -le $MIN_LOAD_BATCH_SEQ_NUM ]
                then
                    rm -f $f
                fi
            done
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
         bsn_file=$DW_DAT/${DM_TRGT_ENV_ARR[idx]}/$SUBJECT_AREA/$TABLE_ID.datamove.trgt.batch_seq_num.dat
         if [ -f $bsn_file ]
         then
            cat $bsn_file | read DM_TRGT_BATCH_SEQ_NUM
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

         ((idx+=1))
      done

      print "--------------------------------------------------------------------------------------"
      print "TARGET JOB ENVIRONMENTS for this ETL_ID: ${DM_TRGT_ENV_ARR[*]}"
      print "minimum DM_TRGT_BATCH_SEQ_NUM == $MIN_DM_TRGT_BATCH_SEQ_NUM"
      print "--------------------------------------------------------------------------------------"

      print "Deleting source files loaded through batch sequence number $MIN_DM_TRGT_BATCH_SEQ_NUM"

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
    print "Marking log files from previous processing periods as .r4a"

    if [ -f $DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.!(*.r4a|*$CURR_DATETIME.*) ]
    then
        for fn in $DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.!(*.r4a|*$CURR_DATETIME.*)
        do
            if [[ ${fn##*.} == err && ! -s $fn ]]
            then
                rm -f $fn     # remove empty error files
            else
                mv -f $fn $fn.r4a
            fi
        done
    fi

    if [[ $JOB_TYPE_ID == dm ]]
    then
       if [ -f $DW_SA_LOG/$TABLE_ID.bt.*.datamove.!(*.r4a|*$CURR_DATETIME.*) ]
       then
           for fn in $DW_SA_LOG/$TABLE_ID.bt.*.datamove.!(*.r4a|*$CURR_DATETIME.*)
           do
               mv $fn $fn.r4a
           done
       fi
    fi

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
    print "Marking log files from previous processing periods as .r4a"

    if [ -f $DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.*${SQL_FILE_BASENAME}.!(*.r4a|*$CURR_DATETIME.*) ]
    then
        for fn in $DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.*${SQL_FILE_BASENAME}.!(*.r4a|*$CURR_DATETIME.*)
        do
            if [[ ${fn##*.} == err && ! -s $fn ]]
            then
                rm -f $fn     # remove empty error files
            else
                mv -f $fn $fn.r4a
            fi
        done
    fi
fi

print "loader cleanup process complete"

exit
