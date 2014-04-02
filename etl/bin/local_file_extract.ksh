#!/bin/ksh -eu
# Title:        Local File Extract
# File Name:    local_file_extract.ksh
# Description:  Handle copy local file from Land to Local
# Developer:    ???
# Created on:
# Location:     $DW_EXE
# Logic:
#
#
# Called by:    UC4/Unix
#
# Date         Ver#   Modified By(Name)            Change and Reason for Change
#---------    -----  ---------------------------  ------------------------------
# ????-??-??   1.0    ??                            Initial creation
# 2012-05-15   1.1    Ryan Wong                     Modified to add UOW
# 2012-09-12   1.2    Ryan Wong                     Removing BSN from UOW type processing
# 2013-06-17   1.3    Ryan Wong                     Bug Fix.  If a job is killed during process_tar_files,
#                                                     then TMP_SRC_LIS is blanked out on restart.
#                                                     Only blank out TMP_SRC_LIS during create_tar_files phase.
# 2013-10-04   1.4    Ryan Wong                     Redhat changes
# 2013-11-12   1.5    George Xiong                  Fix tar -I issue
#############################################################################################################

print "Checking configuration settings for compression and donefile processing"

# create the batch specific sources.lis file for files consumed by an ETL_ID
# add logic, so that if we have already tar'd files, we use the tar instead of trying
# to tar the files up again for each record_id.  Thus, if we failover, with this step already 
# complete, we do not need to start it again.  will need to use a temp tar name, so that if we fail while building
# we do not think we completed.
# consider adding gzip to tar, for easing transfer

BATCH_SEQ_NUM=$1
UOW_TO=${UOW_TO:-""}

. $DW_MASTER_LIB/dw_etl_common_functions.lib

FILE_ID=0

TMP_SRC_LIS=$DW_SA_TMP/$ETL_ID.sources.lis
MV_SRC_LIS=$DW_SA_TMP/$ETL_ID.mv.sources.lis
if [[ -n $UOW_TO ]]
then
  SRC_LIS=$DW_SA_DAT/$ETL_ID.sources.lis.$UOW_TO
else
  SRC_LIS=$DW_SA_DAT/$ETL_ID.sources.lis.$BATCH_SEQ_NUM
fi

touch $TMP_SRC_LIS

# check on file compression.  At this point, we only can accept files compressed by gzip.
assignTagValue IS_COMPRESS CNDTL_EXTRACT_COMPRESS $DW_CFG/$ETL_ID.cfg W 0


if [ $IS_COMPRESS != 1 ]
then
  COMPRESS_SFX=""
else
  COMPRESS_SFX=".gz"
fi
if [[ -n $UOW_TO ]]
then
  INFILE_SFX=$COMPRESS_SFX
else
  INFILE_SFX=.$BATCH_SEQ_NUM$COMPRESS_SFX
fi

# check  to see if done file processing is required
assignTagValue IS_DONEFILE CNDTL_DONEFILE $DW_CFG/$ETL_ID.cfg W 0

# If not defined, get IN_DIR from etl cfg
set +u
if [ "$IN_DIR" == "" ]
then
  assignTagValue IN_DIR IN_DIR $ETL_CFG_FILE W $DW_IN
  export IN_DIR=$IN_DIR/$JOB_ENV/$SUBJECT_AREA
fi
set -u

if [[ -n $UOW_TO ]]
then
  print "reading $DW_CFG/$ETL_ID.sources.lis to create UOW specific sources.lis"
else
  print "reading $DW_CFG/$ETL_ID.sources.lis to create BATCH_SEQ_NUM specific sources.lis"
fi

# create process id specific tmp files to ensure empty files exist in the case of no files found.

#> $DW_SA_TMP/$TABLE_ID.lclmv.$$.lis

if [[ -n $UOW_TO ]]
then
  LCLMVDIR=$IN_DIR/lclmv/$TABLE_ID.$UOW_TO
else
  LCLMVDIR=$IN_DIR/lclmv/$TABLE_ID.$BATCH_SEQ_NUM
fi
mkdirifnotexist $LCLMVDIR

PWD_VAR=$PWD
COMP_FILE=$DW_SA_TMP/$TABLE_ID.lfe.complete

mkfileifnotexist $COMP_FILE

#--------------------------------------------------
# start of create_tar_files
#--------------------------------------------------
PROCESS=create_tar_files
grcode=`grepCompFile $PROCESS $COMP_FILE`

if [ $grcode != 0 ]
then

  #read file names from sources.lis - each record could have wildcards, so assign file ids by file found, not by FN
  # also add the complete file logic here.

  # step through each record ID, create tar file and file list at this level.

  > $TMP_SRC_LIS

  while read RECORD_ID LANDDIR FNL
  do
    set +e
    LANDDIR=$(eval print $LANDDIR)
    set -f
    FNL=$(eval print $FNL)
    set +f

    if [[ -n $UOW_TO ]]
    then
      TARFILENAME=$TABLE_ID.$RECORD_ID.lfe.$UOW_TO.tar
      TEMPTARFILENAME=$TABLE_ID.$RECORD_ID.lfe.$UOW_TO.tar.tmp
    else
      TARFILENAME=$TABLE_ID.$RECORD_ID.lfe.$BATCH_SEQ_NUM.tar
      TEMPTARFILENAME=$TABLE_ID.$RECORD_ID.lfe.$BATCH_SEQ_NUM.tar.tmp
    fi
    RMDONEFILE=$DW_SA_TMP/$TABLE_ID.$RECORD_ID.lfe.rmdone.lis
    RCRDFILELIST=$LANDDIR/$TABLE_ID.$RECORD_ID.lfe.files.lis
    TMPRCRDFILELIST=$DW_SA_TMP/$TABLE_ID.$RECORD_ID.lfe.files.lis
    MATCHLCLFILE=$DW_SA_TMP/$TABLE_ID.$RECORD_ID.lfe.matchlocal.lis

    #look to see if TARFILE for $RECORD_ID already exists.  Using LANDDIR, so that
    #HA node/restartability functionality is preserved.

    if [ ! -f $LANDDIR/$TARFILENAME ]
    then
        #compile the file set for this record ID
      > $RMDONEFILE 
      > $TMPRCRDFILELIST

      #get the list of potential files 
      if [ ! -f $MATCHLCLFILE ]
      then
        if [ -f $LANDDIR/$FNL ]
        then
          set +e
          # Use for loop rather than ls to avoid file list too long error.
          for fn in $LANDDIR/$FNL; do print $fn; done > $MATCHLCLFILE 
          rcode=$?
          set -e

          if [ $rcode != 0 ]
          then
            print "${0##*/}:  ERROR, failure determining files for RECORD_ID $RECORD_ID in $DW_CFG/$ETL_ID.sources.lis" >&2
            exit 4
          fi
        else
          > $MATCHLCLFILE
        fi
      fi

      # confirm done processing and create move list
      if [ -s $MATCHLCLFILE ]
      then
        while read FN 
        do
          LANDFILE=${FN##*/}
          INFILE=$TABLE_ID.$FILE_ID.dat

          DONEFILEFOUND=0

          if [ $IS_DONEFILE = 1 ]
          then
            DONEFILE1=${LANDFILE%.*}.done
            DONEFILE2=$LANDFILE.done

            if [ -f $LANDDIR/$DONEFILE1 ]
            then 
              DONEFILE=$DONEFILE1
              DONEFILEFOUND=1
            elif [ -f $LANDDIR/$DONEFILE2 ]
            then
              DONEFILE=$DONEFILE2
              DONEFILEFOUND=1
            fi

            if [ $DONEFILEFOUND = 1 ]
            then 
              print "$FILE_ID $RECORD_ID $INFILE $LANDFILE" >> $TMP_SRC_LIS    
              ((FILE_ID+=1))
              print $LANDFILE >> $TMPRCRDFILELIST
              print $LANDDIR/$DONEFILE >> $RMDONEFILE
            else
              print "No Done File found for $LANDFILE"
            fi
          else
            print "$FILE_ID $RECORD_ID $INFILE $LANDFILE" >> $TMP_SRC_LIS
            ((FILE_ID+=1))
            print $LANDFILE >> $TMPRCRDFILELIST
          fi
        done < $MATCHLCLFILE

        # tar up the localfiles for record ID, cp to tmpdir on local machine, untar at local drive
        cd $LANDDIR
        tar -cvf $TEMPTARFILENAME    $(cat $TMPRCRDFILELIST)
        mv $TEMPTARFILENAME $TARFILENAME #rename at completion to avoid picking up partial file.
        mv $TMPRCRDFILELIST $RCRDFILELIST

      else
        #no files in set
        mv $TMPRCRDFILELIST $RCRDFILELIST
      fi  
    else
      print " Tar file $LANDDIR/$TARFILENAME already exists"
    fi
  done < $DW_CFG/$ETL_ID.sources.lis

  print "$PROCESS phase complete"
  print $PROCESS >> $COMP_FILE
else
  print "$PROCESS already complete"
fi
#--------------------------------------------------
# end of create_tar_files
#--------------------------------------------------


#--------------------------------------------------
# start of process_tar_files
#--------------------------------------------------
PROCESS=process_tar_files

grcode=`grepCompFile $PROCESS $COMP_FILE`

if [ $grcode != 0 ]
then

  #now that we have the tar files/list, remove original files, copy tar to lclmv dir, and unload it

  while read RECORD_ID LANDDIR FNL
  do
    set +e
    LANDDIR=$(eval print $LANDDIR)
    set -e

    if [[ -n $UOW_TO ]]
    then
      TARFILENAME=$TABLE_ID.$RECORD_ID.lfe.$UOW_TO.tar
    else
      TARFILENAME=$TABLE_ID.$RECORD_ID.lfe.$BATCH_SEQ_NUM.tar
    fi
    RMDONEFILE=$DW_SA_TMP/$TABLE_ID.$RECORD_ID.lfe.rmdone.lis
    RMORIGFILE=$DW_SA_TMP/$TABLE_ID.$RECORD_ID.lfe.rmorig.lis
    RCRDFILELIST=$LANDDIR/$TABLE_ID.$RECORD_ID.lfe.files.lis
    MATCHLCLFILE=$DW_SA_TMP/$TABLE_ID.$RECORD_ID.lfe.matchlocal.lis


    > $RMORIGFILE

    #Remove original files (now in tars) - use the same list that tar was built with
    #read through tar list, create current remove list based on what is found, then
    # remove them
    print "removing original files for TABLE_ID $TABLE_ID RECORD_ID $RECORD_ID"
    while read TARFNL
    do
      if [ -f $LANDDIR/$TARFNL ]
      then
         print "$LANDDIR/$TARFNL" >> $RMORIGFILE
      fi
    done < $RCRDFILELIST

    if [ -s $RMORIGFILE ]
    then
      set +e
      xargs rm < $RMORIGFILE
      rcode=$?
      set -e
 
      if [ $rcode != 0 ]
      then
        print "${0##*/}:  WARNING, failure deleting original files for TABLE_ID $TABLE_ID RECORD_ID $RECORD_ID" >&2
      fi
    fi

    if [ $IS_DONEFILE = 1 ]
    then
      print "removing done files for TABLE_ID $TABLE_ID RECORD_ID $RECORD_ID"
      set +e
      xargs rm < $RMDONEFILE
      rcode=$?
      set -e

      if [ $rcode != 0 ]
      then
         print "${0##*/}:  WARNING, failure deleting done files for TABLE_ID $TABLE_ID RECORD_ID $RECORD_ID" >&2
      fi
    fi

    # cp tar file to local device and untar them
    if [ -f $LANDDIR/$TARFILENAME ]
    then
      mkdirifnotexist $LCLMVDIR/$RECORD_ID

      cp $LANDDIR/$TARFILENAME $LCLMVDIR/$RECORD_ID/$TARFILENAME
      cd $LCLMVDIR/$RECORD_ID
  
      set +e  
      tar -xvf $TARFILENAME
      rcode=$?
      set -e

      if [ $rcode != 0 ]
      then
        print "${0##*/}:  ERROR, failure extracting files from tar: $IN_DIR/lclmv/$TARFILENAME" >&2
        exit 4
      else
        print "tar files extracted from $IN_DIR/lclmv/$TARFILENAME"
        cd $LANDDIR
      fi
    fi
  done < $DW_CFG/$ETL_ID.sources.lis

  rm -f $RCRDFILELIST

  print "$PROCESS phase complete"
  print $PROCESS >> $COMP_FILE
  cd $PWD_VAR
else
  print "$PROCESS already complete"
fi

#--------------------------------------------------
# end of process_tar_files
#--------------------------------------------------


#--------------------------------------------------
# start of local_mv_files
#--------------------------------------------------
PROCESS=local_mv_files

grcode=`grepCompFile $PROCESS $COMP_FILE`

if [ $grcode != 0 ]
then

  # $MV_SRC_LIS is only created during initial run since we know that is the full data set and
  # do not want the list to change on restart.

  if [ ! -f $MV_SRC_LIS ]
  then
    mv $TMP_SRC_LIS $MV_SRC_LIS
  fi

  if [[ -n $UOW_TO ]]
  then
    MVLIST=$DW_SA_TMP/$TABLE_ID.movelist.$UOW_TO.lis
  else
    MVLIST=$DW_SA_TMP/$TABLE_ID.movelist.$BATCH_SEQ_NUM.lis
  fi

  > $MVLIST

  while read FILE_ID RECORD_ID INFILE LANDFILE
  do
    print $LCLMVDIR/$RECORD_ID/$LANDFILE $IN_DIR/$INFILE$INFILE_SFX >> $MVLIST
  done < $MV_SRC_LIS

  set +e 
  $DW_EXE/movelist < $MVLIST
  rcode=$?
  set -e

  if [ $rcode != 0 ]
  then
    print "${0##*/}:  ERROR, failure moving files in $MVLIST" >&2
    exit 4
  else
    rm $MVLIST
  fi
  print "$PROCESS phase complete"
  print $PROCESS >> $COMP_FILE
else
  print "$PROCESS already complete"
fi
#--------------------------------------------------
# end of local_mv_files
#--------------------------------------------------


#--------------------------------------------------
# start of finalize_lfe
#--------------------------------------------------
PROCESS=finalize_lfe

grcode=`grepCompFile $PROCESS $COMP_FILE`

if [ $grcode != 0 ]
then

  # create the file id level record count files.

  # check if any records are in $MV_SRC_LIS - if none - create a dummy file, and put it in
  # add logic for compression

  print "moving and renaming files"
  wc -l $MV_SRC_LIS | read FILE_COUNT FN

  if [ $FILE_COUNT == 0 ]
  then

    if [[ -n $UOW_TO ]]
    then
      print "no files received for this batch $BATCH_SEQ_NUM, creating dummy files for load process"
      > $IN_DIR/$TABLE_ID.0.dat
      print "0 0 $TABLE_ID.0.dat $TABLE_ID.0.dat" > $MV_SRC_LIS
      print 0 > $IN_DIR/$TABLE_ID.record_count.dat
    else
      print "no files received for this batch $BATCH_SEQ_NUM, creating dummy files for load process"
      > $IN_DIR/$TABLE_ID.0.dat.$BATCH_SEQ_NUM
      print "0 0 $TABLE_ID.0.dat $TABLE_ID.0.dat" > $MV_SRC_LIS
      print 0 > $IN_DIR/$TABLE_ID.record_count.dat.$BATCH_SEQ_NUM
    fi

    if [ $IS_COMPRESS = 1 ]
    then
      if [[ -n $UOW_TO ]]
      then
        gzip $IN_DIR/$TABLE_ID.0.dat
      else
        gzip $IN_DIR/$TABLE_ID.0.dat.$BATCH_SEQ_NUM
      fi
    fi
  else
    integer FILESIZE=0
    integer TOTAL_SIZE=0
    while read TGT_FILE_ID RECORD_ID TGT_INFILE SRC_LANDFILE
    do
      print $IN_DIR/$TGT_INFILE$INFILE_SFX
    done < $MV_SRC_LIS | xargs ls -l | while read x1 x2 x3 x4 FILESIZE x6
    do
      TOTAL_SIZE=TOTAL_SIZE+FILESIZE
    done
    if [[ -n $UOW_TO ]]
    then
      print $((TOTAL_SIZE/100)) > $IN_DIR/$TABLE_ID.record_count.dat
    else
      print $((TOTAL_SIZE/100)) > $IN_DIR/$TABLE_ID.record_count.dat.$BATCH_SEQ_NUM
    fi
  fi

  # Move temporary source list file to DW_SA_DAT. This should only be last step
  # as single_table_extract_handler.ksh checks for the presence of this prior to
  # executing local_file_extract.ksh

  #mv $MV_SRC_LIS $SRC_LIS
  cut -f 1,3- -d " " $MV_SRC_LIS > $SRC_LIS
  rm -f $MV_SRC_LIS

  rm -f $DW_SA_TMP/$TABLE_ID.*.lfe.*.lis

  print "$PROCESS phase complete"
  print $PROCESS >> $COMP_FILE
else
  print "$PROCESS already complete"
fi
#--------------------------------------------------
# end of finalize_lfe
#--------------------------------------------------

print "Remove the temp local move directory"
rmdirtreeifexist $LCLMVDIR #note that this is not removing the LANDING dir TARFILE 

print "Removing the local_file_extract complete file  `date`"
rm -f $COMP_FILE

print "local file extract processing complete"

exit 0
