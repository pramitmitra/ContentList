#!/bin/ksh -eu

#------------------------------------------------------------------------------------------------
# Filename:     loader_cleanup_sg.ksh
# Description:  Removes temp and watch files and renames data and log files from previous
#               processing period to.r4a.  Check BATCH_SEQ_NUM for all load processes to
#               determine which data files can be renamed.  Use the minimum BATCH_SEQ_NUM
#               from the load processes as the max BATCH_SEQ_NUM that can be archived.  A
#               delete date is appended to the data file names based on the number of days
#               the data files need to be retained (retrieved from the config file).
#
# Developer:    Orlando Jin
# Created on:   07/05/2010
# Location:     $DW_EXE/
#
# Execution:    $DW_EXE/loader_cleanup_sg.ksh <JOB_ENV> <JOB_TYPE_ID> <ETL_ID
#
# Parameters:   JOB_ENV = <extract|td1|td2|td3|td4>
#               JOB_TYPE_ID = <ex|ld|bt>
#               ETL_ID
#
# Revision History:
#
# Name             Date            Description
# ---------------  --------------  --------------------------------------------------------------
# Craig Werre      07/05/2010      Initial Creation
# Ryan Wong        10/04/2013      Redhat changes
#------------------------------------------------------------------------------------------------

JOB_ENV=$1      # extract, td1, td2, td3, td4,( primary, secondary, all -- deprecated )
JOB_TYPE_ID=$2  # ex, ld, bt
ETL_ID=$3
JOB_TYPE_TMP=${4:-""}
CURR_DATETIME_TMP=${5:-""}
. /dw/etl/mstr_cfg/etlenv.setup
. $DW_MASTER_CFG/dw_etl_common_defs.cfg
. $DW_MASTER_LIB/dw_etl_common_functions.lib

if [ ! -z $CURR_DATETIME_TMP ]
then
  CURR_DATETIME=$CURR_DATETIME_TMP
fi

if [ ! -z $JOB_TYPE_TMP ]
then
  JOB_TYPE=$JOB_TYPE_TMP
fi

LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.loader_cleanup.$CURR_DATETIME.log
if [ $JOB_ENV == "extract" ]
then

    #----------------------------------------------------------------------------------------------------------
    # determine the number of days that data files need to be retained after being loaded.
    #----------------------------------------------------------------------------------------------------------
    assignTagValue DATA_RET_DAYS DATA_RET_DAYS $ETL_CFG_FILE W 0 >>$LOG_FILE

    #----------------------------------------------------------------------------------------------------------
    # DEL_DATE represents the date the data file can be deleted.  It will be appended to the data file
    # name at the time it is marked as ready for archive.
    #----------------------------------------------------------------------------------------------------------
    DEL_DATE=$($DW_EXE/add_days ${CURR_DATETIME%-*} $DATA_RET_DAYS)

    #----------------------------------------------------------------------------------------------------------
    # LOAD_JOB_ENV is a pipe delimited list of all targets in use. A single target is stand alone sans pipe.
    # primary, secondary, all are deprecated in favor of td1, td2, etc... all resolves to td1|td2 until
    # functionality is completely deprecated from the production environment. Determines which load
    # BATCH_SEQ_NUM file(s) to use to determine when data files have been loaded and can be archived.
    #----------------------------------------------------------------------------------------------------------
    assignTagValue LOAD_JOB_ENV LOAD_JOB_ENV $ETL_CFG_FILE >>$LOG_FILE

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
        print "${0##*/}:  FATAL ERROR, invalid value for parameter LOAD_JOB_ENV: ($LOAD_JOB_ENV)"
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
            print "${0##*/}:  FATAL ERROR, BATCH SEQUENCE NUMBER FILE $bsn_file does not exist."
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

    print "--------------------------------------------------------------------------------------" >>  $LOG_FILE
    print "LOAD JOB ENVIRONMENTS for this ETL_ID: ${LOAD_JOB_ENV_ARR[*]}"                          >> $LOG_FILE
    print "minimum LOAD_BATCH_SEQ_NUM = $MIN_LOAD_BATCH_SEQ_NUM"                                   >> $LOG_FILE
    print "--------------------------------------------------------------------------------------" >> $LOG_FILE

    #----------------------------------------------------------------------------------------------------------
    # rename data files and record count files with a batch sequence number <= to the minimum
    # batch sequence number from any of the load processes for this TABLE_ID.
    #----------------------------------------------------------------------------------------------------------

    print "Moving data files loaded through batch sequence number $MIN_LOAD_BATCH_SEQ_NUM to r4a directory" >> $LOG_FILE

    if [ -f $DW_SA_IN/$TABLE_ID.!(*.r4a) ]
    then

        # For compressed files, pull compression suffix from filename before determining batch_seq_num
        # Check for compressed files even when compression may not be active, and vise versa, in case recent change
        assignTagValue CNDTL_COMPRESSION_SFX CNDTL_COMPRESSION_SFX $ETL_CFG_FILE W ".gz" >>$LOG_FILE

        R4A_FILE=$DW_SA_TMP/$TABLE_ID.$JOB_TYPE_ID.r4a.$DEL_DATE
        FIRST_FILE=1

        for fn in $DW_SA_IN/$TABLE_ID.!(*.r4a)
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
            mkdirifnotexist $DW_SA_IN/r4a_$DEL_DATE
            xargs $DW_LIB/mass_mv $DW_SA_IN/r4a_$DEL_DATE < $R4A_FILE
        fi
    fi

    if [ -f $DW_MFS/fs??/in/extract/$SUBJECT_AREA/$TABLE_ID.!(*.r4a) ]
    then
        for fn in $DW_MFS/fs??/in/extract/$SUBJECT_AREA/$TABLE_ID.!(*.r4a)
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
    print "Removing extract watch files for data loaded through batch sequence number $MIN_LOAD_BATCH_SEQ_NUM" >>$LOG_FILE
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

    assignTagValue EXTRACT_PROCESS_TYPE EXTRACT_PROCESS_TYPE $ETL_CFG_FILE >>$LOG_FILE

    if [ $EXTRACT_PROCESS_TYPE == "L" ]
    then
        while read RECORD_ID LANDDIR FNL
        do
            set +e
            LANDDIR=$(eval print $LANDDIR)
            set -e

            for f in $LANDDIR/$TABLE_ID.*lfe.*.tar
            do
                TAR_BATCH_SEQ_NUM=${f%.done}
                TAR_BATCH_SEQ_NUM=${FILE_BATCH_SEQ_NUM##*.}

                if [ $TAR_BATCH_SEQ_NUM -le $MIN_LOAD_BATCH_SEQ_NUM ]
                then
                    rm -f $f
                fi
            done
        done < $DW_CFG/$ETL_ID.sources.lis
    fi
fi

if [[ $JOB_TYPE_ID == "unload" || $JOB_TYPE_ID == "ex" ]]
then

  #----------------------------------------------------------------------------------------------------------
  # Determine the number of days that unload data files need to be retained
  #----------------------------------------------------------------------------------------------------------
  assignTagValue UNLOAD_DATA_ARC_DAYS UNLOAD_DATA_ARC_DAYS $ETL_CFG_FILE W 0 >>$LOG_FILE
  #----------------------------------------------------------------------------------------------------------
  # Archive only if $UNLOAD_DATA_ARC_DAYS is assigned value in the cfg file
  #----------------------------------------------------------------------------------------------------------
  if [ -n "$UNLOAD_DATA_ARC_DAYS" ]
  then

    #----------------------------------------------------------------------------------------------------------
    # Assign DW_SA_OUT without exported by multi_unload_handler.ksh.
    # Recommend to move the DW_SA_OUT statement before quoting loader_cleanup.ksh by multi_unload_handler.ksh.
    #----------------------------------------------------------------------------------------------------------
    set +e
    if [ $JOB_TYPE_ID == "unload" ]
    then
      assignTagValue OUT_DIR OUT_DIR $ETL_CFG_FILE >>$LOG_FILE
    elif [ $JOB_TYPE_ID == "ex" ]
    then
      assignTagValue OUT_DIR IN_DIR $ETL_CFG_FILE >>$LOG_FILE
    fi
    rcode=$?
    set -e

    DW_SA_OUT=`eval print $OUT_DIR`/$JOB_ENV/$SUBJECT_AREA

    ARC_DATE=$($DW_EXE/add_days ${CURR_DATETIME%-*} $UNLOAD_DATA_ARC_DAYS)

    #----------------------------------------------------------------------------------------------------------
    # Move the data files older than $UNLOAD_DATA_ARC_DAYS days
    #----------------------------------------------------------------------------------------------------------
    print "Removing unload files greater than $UNLOAD_DATA_ARC_DAYS days old" >> $LOG_FILE

    #----------------------------------------------------------------------------------------------------------
    # Archive the file name with TABLE_ID
    #----------------------------------------------------------------------------------------------------------
    R4A_FILE=$DW_SA_TMP/$TABLE_ID.$JOB_TYPE_ID.r4a.$ARC_DATE
    find $DW_SA_OUT/ -type f -mtime +$UNLOAD_DATA_ARC_DAYS -name "*$TABLE_ID.*" > $R4A_FILE

    print "find $DW_SA_OUT/ -type f -mtime +$UNLOAD_DATA_ARC_DAYS -name \"*$TABLE_ID.*\" -print > $R4A_FILE" >> $LOG_FILE
    #----------------------------------------------------------------------------------------------------------
    # Archive the file name with UNLOAD_ARC_FILENAME from cfg file - optional
    #----------------------------------------------------------------------------------------------------------
    assignTagValue UNLOAD_ARC_FILENAME UNLOAD_ARC_FILENAME $ETL_CFG_FILE W >> $LOG_FILE
    if [ ! -z $UNLOAD_ARC_FILENAME ]
    then
      R4A_FILE_OPT=$DW_SA_TMP/$TABLE_ID.$JOB_TYPE_ID.r4a.$ARC_DATE.opt
      R4A_FILE_TMP=$DW_SA_TMP/$TABLE_ID.$JOB_TYPE_ID.r4a.$ARC_DATE.tmp
      find $DW_SA_OUT/ -type f -mtime +$UNLOAD_DATA_ARC_DAYS -name "*$UNLOAD_ARC_FILENAME.*" -print > $R4A_FILE_OPT
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
  else
    print "${0##*/}:  Skip archive due to no UNLOAD_DATA_ARC_DAYS parameter from $DW_CFG/$ETL_ID.cfg" >> $LOG_FILE
  fi

  #----------------------------------------------------------------------------------------------------------
  # Delete the file name older than UNLOAD_DATA_DEL_DAYS from cfg file under $DW_ARC
  #----------------------------------------------------------------------------------------------------------
  assignTagValue UNLOAD_DATA_DEL_DAYS UNLOAD_DATA_DEL_DAYS $ETL_CFG_FILE W >> $LOG_FILE

  if [ -n $UNLOAD_DATA_DEL_DAYS  ]
  then
    find $DW_ARC/$JOB_ENV/$SUBJECT_AREA -mtime +$UNLOAD_DATA_DEL_DAYS -name "*$TABLE_ID.*.gz" -print -o -name "*$TABLE_ID.*.Z" -print | xargs rm -f;

    #----------------------------------------------------------------------------------------------------------
    # Archive the file name with UNLOAD_ARC_FILENAME from cfg file - optional
    #----------------------------------------------------------------------------------------------------------
    assignTagValue UNLOAD_ARC_FILENAME UNLOAD_ARC_FILENAME $ETL_CFG_FILE W >> $LOG_FILE
    if [ -z $UNLOAD_ARC_FILENAME ]
    then
      find $DW_ARC/$JOB_ENV/$SUBJECT_AREA -mtime +$UNLOAD_DATA_DEL_DAYS -name "*$UNLOAD_ARC_FILENAME.*.gz" -print -o -name "*$UNLOAD_ARC_FILENAME.*.Z" -print | xargs rm -f;
    fi

  else
    print "${0##*/}:  Skip deletion due to no UNLOAD_DATA_DEL_DAYS parameter from $DW_CFG/$ETL_ID.cfg" >> $LOG_FILE
  fi
fi

if [ $JOB_TYPE_ID != bt ]
then
  #------------------------------------------------------------------------
  #  Remove extract and load temp files.
  #------------------------------------------------------------------------
  print "Removing temp files" >> $LOG_FILE

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
  print "Marking log files from previous processing periods as .r4a" >> $LOG_FILE

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
else
  #------------------------------------------------------------------------
  #  Remove bteq temp files.
  #------------------------------------------------------------------------
  print "Removing temp files" >> $LOG_FILE

  if [ -f $DW_SA_TMP/$TABLE_ID.$JOB_TYPE_ID.${SQL_FILENAME%.sql}.* ]
  then
    for fn in $DW_SA_TMP/$TABLE_ID.$JOB_TYPE_ID.${SQL_FILENAME%.sql}.*
    do
      rm -f $fn
    done
  fi

  #------------------------------------------------------------------------
  #  Move bteq log/err files to the archive directory.
  #------------------------------------------------------------------------
  print "Marking log files from previous processing periods as .r4a" >> $LOG_FILE

  if [ -f $DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.*${SQL_FILENAME%.sql}.!(*.r4a|*$CURR_DATETIME.*) ]
  then
    for fn in $DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.*${SQL_FILENAME%.sql}.!(*.r4a|*$CURR_DATETIME.*)
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

print "loader cleanup process complete" >> $LOG_FILE

exit
