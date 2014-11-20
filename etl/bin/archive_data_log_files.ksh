#!/bin/ksh -eu

#------------------------------------------------------------------------------------------------
# Filename:     archive_data_log_files.ksh
# Description:  Remove data and log files from the archive directories at the appropriate time
#               for each.  Data files with a delete date equal to or less than today will be
#               deleted.  Log files greater than 31 days old will be deleted.  Then, it will
#               move all data and log files marked with .r4a to their respective archive
#               directories.  It should run once a day at a slower time on the etl server
#               running the Next Gen loaders.
#               
# Developer:    Craig Werre
# Created on:   10/05/2005
# Location:     $DW_EXE/
#
# Execution:    $DW_EXE/shell_handler.ksh dw_infra.archive primary $DW_EXE/archive_data_log_files.ksh
#
# Parameters:   none
#
# Revision History:
#
# Name             Date            Description
# ---------------  --------------  ---------------------------------------------------
# Craig Werre      10/05/2005      Initial Creation
# Orlando Jin      08/21/2008      Adding over 30 days old extract touch-file purging
# Jacky Shen       08/30/2010      /usr/ucb/ps -auxwwwl
# Jacky Shen       01/18/2011      Create arc fold if not exist
# Ryan Wong        01/03/2012      Add remove for date based log dirs
# Ryan Wong        04/03/2012      Fix command for finding date based log dirs
# Ryan Wong        04/04/2012      Do not error if find command fails to find files
# Ryan Wong        05/24/2012      Reduce extract touchfile retention to 20 days
# Ryan Wong        07/15/2013      Add del/arch for new naming of r4a_${TABLE_ID}_${DEL_DATE}
# Ryan Wong        08/02/2013      Update UOW archive.  Cannot use zip since it has a 2GB size limit.
#                                    Instead, move directory to DW_ARC into a directory w/ext .dir
# Ryan Wong        10/04/2013      Redhat changes
# Jiankang Liu     11/03/2014      To adapt archive jobs to new ETL servers Tempo, make such changes:
#										1. Add archive type parameter, local type runs on each nodes of Tempo cluster, shared type runs on random node. 
#										2. Split code blocks into different functions base on functionality and directory. 
#										3. Group the functions with $DW_LOG, $DW_WATCH into deal_shared_files function, while the functions with $DW_ARC 
#										$DW_IN, $DW_MFS into deal_local_files function. 
#										4. Call deal_shared_files if archive type is shared, else call deal_local_files. 
#------------------------------------------------------------------------------------------------
typeset -fu usage

function usage {
   	print "Usage:  $0 <ARCH_TYPE>
	ARCH_TYPE =   <local|shared>"
}

if [ $# -ne 1 ]
then
   usage 
   exit 4
fi

ARCH_TYPE=$1

if [ $ARCH_TYPE = shared ]
then
	export ARCH_TYPE="arch_shared"
else
	export ARCH_TYPE="arch_local"
fi

print "####################################################################################"
print "#"
print "# Beginning archive process for data and log files  `date`"
print "#"
print "####################################################################################"
print ""

SHELL_EXE_NAME=${0##*/}
DW_SA_LOG=$DW_LOG/td1/dw_infra
DW_SA_ARC=$DW_ARC/td1/dw_infra

#--------------------------------------------------------------------------------------
# Determine if there is already an achive process running
#--------------------------------------------------------------------------------------
while [ $(/bin/ps -awwwl -u $DWI_WHOAMI| grep "archive_data_log_files.ksh" | grep -v "shell_handler.ksh" | grep -v "grep archive_data_log_files.ksh"| wc -l) -ge 2 ]
  do
    if [ $(/bin/ps -awwwl -u $DWI_WHOAMI| grep "archive_data_log_files.ksh" | grep -v "shell_handler.ksh" | grep -v "grep archive_data_log_files.ksh"| wc -l) -gt 2 ]
    then
      print "There is already 2 achive process running. Exit"
      exit 0
    else
      print "There is already an achive process running. Sleeping for 30 seconds"
      sleep 30
      continue
    fi
  done

#------------------------------------------------------------------------
# determine which subject areas exist in each dual active environment
#------------------------------------------------------------------------
function return_subject_area_dirs {
	upper_dir=$1

	for dirs in $(cd $upper_dir; ls -1Fd * | grep '/$' | cut -f1 -d/)
	do
		print $dirs
	done
}

function mkdirifnotexist {
############################################################################
# function to make a directory if it does not already exist.
# single parameter $_dir is the directory to be made.  Function will check
# to see if directory already exists, and if it does not, will attempt to
# create it.  If it already exists, function will print that out.
# If creation fails, function will return error, and message the failure,
# else it will message success, and return 0
#
# Directory already existing is considered successful completion.
#
# Example: mkdirifnotexist $DW_SA_IN/mynewdir/
#
############################################################################
_dir=$1

  if [ ! -d $_dir ]
  then
    set +e
    mkdir -p $_dir
    mkdir_rcode=$?
    set -e

    if [ $mkdir_rcode != 0 ]
    then
      print "${0##*/}:  FATAL ERROR, Unable to make directory $_dir." >&2
      return 4
    else
      print "Successfuly made directory $_dir"
    fi
#  else
#    print "directory $_dir already exists"
  fi
  return 0
}


function deal_dwsalog_selflogfile {
#------------------------------------------------------------------------
# this section is for the log and error file created by this process.
# if previous error file is not empty, rename it to *.r4a.
# otherwise remove it - excludes the current error file.
#------------------------------------------------------------------------
print "[deal_dwsalog_selflogfile] mark error files as r4a and rm empty error files excludes today"
if [ -f $DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.${SHELL_EXE_NAME%.ksh}.!($CURR_DATETIME).* ]
then
	for FILE in $DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.${SHELL_EXE_NAME%.ksh}.!($CURR_DATETIME).[le][or][gr]
	do
		if [ -s $FILE ]
		then
			mv -f $FILE $FILE.r4a
		else
			rm -f $FILE
		fi
	done
fi
}


function rm_dwlog_olderthan31_dir {
#------------------------------------------------------------------------
#  Remove date based log dirs greater than 31 days old.
#------------------------------------------------------------------------
print "[rm_dwlog_olderthan31_dir] Removing date based log dirs greater than 31 days old  `date`"

set +e
find $DW_LOG/extract/*/*/ -name '20??????' -prune -type d -mtime +31 -exec rm -rf {} \;
find $DW_LOG/td?/*/*/ -name '20??????' -prune -type d -mtime +31 -exec rm -rf {} \;
set -e
}


function rm_dwarc_olderthantoday_datgzfile {
#------------------------------------------------------------------------
#  Remove data files with a delete date of today or earlier.
#------------------------------------------------------------------------
print "[rm_dwarc_olderthantoday_datgzfile] Removing data files with a delete date of today or earlier  `date`"

if [ -f $DW_ARC/extract/*/*.dat.*.*.gz ]
then
	for FILE in $DW_ARC/extract/*/*.dat.*.*.gz
	do
		DEL_DATE=${FILE%.gz}
		DEL_DATE=${DEL_DATE##*.}

		if [ $DEL_DATE -le ${CURR_DATETIME%-*} ]
		then
			rm -f $FILE
		fi
	done
fi
}


function rm_dwarc_olderthantoday_zipfile {
#------------------------------------------------------------------------
#  Remove zip archives with a delete date of today or earlier.
#------------------------------------------------------------------------
print "[rm_dwarc_olderthantoday_zipfile] Removing zip archives with a delete date of today or earlier  `date`"

if [ -f $DW_ARC/extract/*/*.zip ]
then
	for FILE in $DW_ARC/extract/*/*.zip
	do
		DEL_DATE=${FILE%.zip}
		DEL_DATE=${DEL_DATE##*.}
		if [ $DEL_DATE -le ${CURR_DATETIME%-*} ]
		then
			rm -f $FILE
		fi
	done
fi
}


function rm_dwarc_olderthantoday_dir {
#------------------------------------------------------------------------
#  Remove dir archives with a delete date of today or earlier.
#------------------------------------------------------------------------
print "[rm_dwarc_olderthantoday_dir] Removing dir archives with a delete date of today or earlier  `date`"

if [ -d $DW_ARC/extract/*/*.dir ]
then
	for FILE in $DW_ARC/extract/*/*.dir
	do
		DEL_DATE=${FILE%.dir}
		DEL_DATE=${DEL_DATE##*.}
		if [ $DEL_DATE -le ${CURR_DATETIME%-*} ]
		then
			rm -rf $FILE
		fi
	done
fi	
}


#------------------------------------------------------------------------
#  CHANGED for mass file functionality
#------------------------------------------------------------------------
function rm_dwin_r4amassfile {
print "[rm_dwin_r4amassfile] remove mass files with r4a marked"
if [ -d $DW_IN/extract/*/r4a_???????? ]
then
	for DIR in $(ls -d $DW_IN/extract/*/r4a_????????)
	do
		DEL_DATE=${DIR##*/r4a_}

		if [ $DEL_DATE -lt ${CURR_DATETIME%-*} ]
		then
			rm -rf $DIR
#		elif [ $DEL_DATE -eq ${CURR_DATETIME%-*} ]
#		then
#			rm -f $DIR/*
		fi
	done
fi

if [ -d $DW_IN/extract/*/r4a_*_???????? ]
then
	for DIR in $(ls -d $DW_IN/extract/*/r4a_*_????????)
	do
		DEL_DATE=${DIR##*_}

		if [ $DEL_DATE -lt ${CURR_DATETIME%-*} ]
		then
			rm -rf $DIR
		fi
	done
fi

if [ -d $DW_IN/extract/*/r4r_* ]
then
	for DIR in $(ls -d $DW_IN/extract/*/r4r_*)
	do
			rm -rf $DIR
	done
fi
}

function rm_dwmfs_r4amassfile {
print "[rm_dwmfs_r4amassfile] remove mass files with r4a marked"
if [ -f $DW_MFS/fs??/arc/extract/*/*.gz ]
then
	for FILE in $DW_MFS/fs??/arc/extract/*/*.gz
	do
		DEL_DATE=${FILE%.gz}
		DEL_DATE=${DEL_DATE##*.}

		if [ $DEL_DATE -le ${CURR_DATETIME%-*} ]
		then
			m_rm -f $FILE
		fi
	done
fi

if [ -f $DW_MFS/fs??/in/extract/*/*.r4a ]
then
	for FILE in $DW_MFS/fs??/in/extract/*/*.r4a
	do
		DEL_DATE=${FILE%.r4a*}
		DEL_DATE=${DEL_DATE##*.}

		if [ $DEL_DATE -le ${CURR_DATETIME%-*} ]
		then
			m_rm -f $FILE
		fi
	done
fi
}


function rm_dwarc_olderthan31_file {
#------------------------------------------------------------------------
#  Remove files greater than 31 days old.
#------------------------------------------------------------------------
print "[rm_dwarc_olderthan31_file] Removing log/err files greater than 31 days old  `date`"

find $DW_ARC/*/*/ -type f -mtime +31 -exec rm -f {} \;	
}



#------------------------------------------------------------------------
#  Get all data files marked .r4a (ready for archive).
#  Move these files to the archive directory and compress them.
#------------------------------------------------------------------------
function archive_dwin_r4afile {
print "[archive_dwin_r4afile] Moving and compressing data files marked .r4a in $DW_IN/extract   `date`"

return_subject_area_dirs $DW_IN/extract | while read SA_DIR
do
	if [ -f $DW_IN/extract/$SA_DIR/r4a_????????/* ]
	then
		print "	subject area directory:  $SA_DIR  `date`"
		mkdirifnotexist $DW_ARC/extract/$SA_DIR

		for FILE in $DW_IN/extract/$SA_DIR/r4a_????????/*
		do
			ARC_FN=${FILE##*/}
			DEL_DIR=${FILE%/*}
			DEL_DATE=${DEL_DIR##*/r4a_}

			#---------------------------------------------------------------
			# Don't compress files that are already compressed
			#_______________________________________________________________

			if [[ ${ARC_FN##*.} = 'gz' || ${ARC_FN##*.} = 'Z' ]]
			then
				mv $FILE $DW_ARC/extract/$SA_DIR/${ARC_FN%.*}.$DEL_DATE.${ARC_FN##*.}
			else
				gzip -c $FILE > $DW_ARC/extract/$SA_DIR/$ARC_FN.$DEL_DATE.gz
				rm -f $FILE
			fi
		done
	fi

	if [ -f $DW_IN/extract/$SA_DIR/r4a_*_????????/* ] || [ -d $DW_IN/extract/$SA_DIR/r4a_*_????????/* ]
	then
		print "  subject area directory:  $SA_DIR  `date`"
		mkdirifnotexist $DW_ARC/extract/$SA_DIR

		for FILE in $DW_IN/extract/$SA_DIR/r4a_*_????????/*
		do
			ARC_FN=${FILE##*/}
			DEL_DIR=${FILE%/*}
			DEL_DATE=${DEL_DIR##*_}

			#---------------------------------------------------------------
			# Don't compress files that are already compressed
			#_______________________________________________________________

			if [ -d $FILE ]
			then
				TABLE_ID_TMP=${DEL_DIR##*/r4a_}
				SA_TABLE_ID=${TABLE_ID_TMP%_*}
				mkdirifnotexist $DW_ARC/extract/$SA_DIR/${SA_TABLE_ID}_${ARC_FN}.${DEL_DATE}.dir
				mv $FILE $DW_ARC/extract/$SA_DIR/${SA_TABLE_ID}_${ARC_FN}.${DEL_DATE}.dir
			elif [[ ${ARC_FN##*.} = 'gz' || ${ARC_FN##*.} = 'Z' ]]
			then
				mv $FILE $DW_ARC/extract/$SA_DIR/${ARC_FN%.*}.$DEL_DATE.${ARC_FN##*.}
			else
				gzip -c $FILE > $DW_ARC/extract/$SA_DIR/$ARC_FN.$DEL_DATE.gz
				rm -f $FILE
			fi
		done
	fi
done
}

function archive_dwmfs_r4afile {
print "[archive_dwmfs_r4afile] archive r4a files under $DW_MFS"
print "Moving and compressing data files marked .r4a in $DW_MFS/fs04/in/extract   `date`"

return_subject_area_dirs $DW_MFS/fs04/in/extract | while read SA_DIR
do
	if [ -f $DW_MFS/fs04/in/extract/$SA_DIR/*.r4a ]
	then
		print "	subject area directory:  $SA_DIR  `date`"
        mkdirifnotexist $DW_MFS/fs04/arc/extract/$SA_DIR

		for FILE in $DW_MFS/fs04/in/extract/$SA_DIR/*.r4a
		do
			ARC_FN=${FILE##*/}
			ARC_FN=${ARC_FN%.r4a}

			m_mv $FILE $DW_MFS/fs04/arc/extract/$SA_DIR/$ARC_FN

			for F in $(m_expand -native $DW_MFS/fs04/arc/extract/$SA_DIR/$ARC_FN)
			do
				gzip -f $F &
			done

			wait

			# rename control file and individual file names inside of control file to .gz
			sed -e "s/$ARC_FN/$ARC_FN.gz/" $DW_MFS/fs04/arc/extract/$SA_DIR/$ARC_FN > $DW_MFS/fs04/arc/extract/$SA_DIR/$ARC_FN.gz
			rm -f $DW_MFS/fs04/arc/extract/$SA_DIR/$ARC_FN
		done
	fi
done

print "Moving and compressing data files marked .r4a in $DW_MFS/fs08/in/extract   `date`"

return_subject_area_dirs $DW_MFS/fs08/in/extract | while read SA_DIR
do
	if [ -f $DW_MFS/fs08/in/extract/$SA_DIR/*.r4a ]
	then
		print "	subject area directory:  $SA_DIR  `date`"
        mkdirifnotexist $DW_MFS/fs08/arc/extract/$SA_DIR

		for FILE in $DW_MFS/fs08/in/extract/$SA_DIR/*.r4a
		do
			ARC_FN=${FILE##*/}
			ARC_FN=${ARC_FN%.r4a}

			m_mv $FILE $DW_MFS/fs08/arc/extract/$SA_DIR/$ARC_FN

			for F in $(m_expand -native $DW_MFS/fs08/arc/extract/$SA_DIR/$ARC_FN)
			do
				gzip -f $F &
			done

			wait

			# rename control file and individual file names inside of control file to .gz
			sed -e "s/$ARC_FN/$ARC_FN.gz/" $DW_MFS/fs08/arc/extract/$SA_DIR/$ARC_FN > $DW_MFS/fs08/arc/extract/$SA_DIR/$ARC_FN.gz
			rm -f $DW_MFS/fs08/arc/extract/$SA_DIR/$ARC_FN
		done
	fi
done

print "Moving and compressing data files marked .r4a in $DW_MFS/fs16/in/extract   `date`"

return_subject_area_dirs $DW_MFS/fs16/in/extract | while read SA_DIR
do
	if [ -f $DW_MFS/fs16/in/extract/$SA_DIR/*.r4a ]
	then
		print "	subject area directory:  $SA_DIR  `date`"
        mkdirifnotexist $DW_MFS/fs16/arc/extract/$SA_DIR

		for FILE in $DW_MFS/fs16/in/extract/$SA_DIR/*.r4a
		do
			ARC_FN=${FILE##*/}
			ARC_FN=${ARC_FN%.r4a}

			m_mv $FILE $DW_MFS/fs16/arc/extract/$SA_DIR/$ARC_FN

			for F in $(m_expand -native $DW_MFS/fs16/arc/extract/$SA_DIR/$ARC_FN)
			do
				gzip -f $F &
			done

			wait

			# rename control file and individual file names inside of control file to .gz
			sed -e "s/$ARC_FN/$ARC_FN.gz/" $DW_MFS/fs16/arc/extract/$SA_DIR/$ARC_FN > $DW_MFS/fs16/arc/extract/$SA_DIR/$ARC_FN.gz
			rm -f $DW_MFS/fs16/arc/extract/$SA_DIR/$ARC_FN
		done
	fi
done
}

#------------------------------------------------------------------------
#  Get all log/err files marked .r4a (ready for archive).
#  Move these files to the archive directory.
#------------------------------------------------------------------------
# print "Moving log/err files marked .r4a in $DW_LOG/extract  `date`"
# 
# return_subject_area_dirs $DW_LOG/extract | while read SA_DIR
# do
# 	if [ -f $DW_LOG/extract/$SA_DIR/*.r4a ]
# 	then
# 		print "	subject area directory:  $SA_DIR  `date`"
# 		for FILE in $DW_LOG/extract/$SA_DIR/*.r4a
# 		do
# 			mv -f $FILE $DW_ARC/extract/$SA_DIR
# 		done
# 
# 		for FILE in $DW_ARC/extract/$SA_DIR/*.r4a
# 		do
# 			mv -f $FILE ${FILE%.r4a}
# 		done
# 	fi
# 
# done
# 
# print "Moving log/err files marked .r4a in $DW_LOG/primary  `date`"
# 
# return_subject_area_dirs $DW_LOG/primary | while read SA_DIR
# do
# 	if [ -f $DW_LOG/primary/$SA_DIR/*.r4a ]
# 	then
# 		print "	subject area directory:  $SA_DIR  `date`"
# 		for FILE in $DW_LOG/primary/$SA_DIR/*.r4a
# 		do
# 			mv -f $FILE $DW_ARC/primary/$SA_DIR
# 		done
#  
# 		for FILE in $DW_ARC/primary/$SA_DIR/*.r4a
# 		do
# 			mv -f $FILE ${FILE%.r4a}
# 		done
# 	fi
# done
# 
# print "Moving log/err files marked .r4a in $DW_LOG/secondary  `date`"
#  
# return_subject_area_dirs $DW_LOG/secondary | while read SA_DIR
# do
# 	if [ -f $DW_LOG/secondary/$SA_DIR/*.r4a ]
# 	then
# 		print "	subject area directory:  $SA_DIR  `date`"
# 		for FILE in $DW_LOG/secondary/$SA_DIR/*.r4a
# 		do
# 			mv -f $FILE $DW_ARC/secondary/$SA_DIR
# 		done
#  
# 		for FILE in $DW_ARC/secondary/$SA_DIR/*.r4a
# 		do
# 			mv -f $FILE ${FILE%.r4a}
# 		done
# 	fi
# done

function archive_dwlog_r4afiles {
#------------------------------------------------------------------------
#  Get all log/err files in sa folder of dw_log marked .r4a (ready for archive).
#  Move these files to the archive directory.
#------------------------------------------------------------------------
print "[archive_dwlog_r4afiles] mv all .r4a files in $DW_LOG to $DW_ARC"
while read ETL_JOB_ENV
  do
    print "Moving log/err files marked .r4a in $DW_LOG/$ETL_JOB_ENV  `date`"
    return_subject_area_dirs $DW_LOG/$ETL_JOB_ENV | while read SA_DIR
      do
	      if [ -f $DW_LOG/$ETL_JOB_ENV/$SA_DIR/*.r4a ]
	        then
		        print "	subject area directory:  $SA_DIR  `date`"
                mkdirifnotexist $DW_ARC/$ETL_JOB_ENV/$SA_DIR
		        for FILE in $DW_LOG/$ETL_JOB_ENV/$SA_DIR/*.r4a
		          do
			          mv -f $FILE $DW_ARC/$ETL_JOB_ENV/$SA_DIR
		        done
 
          for FILE in $DW_ARC/$ETL_JOB_ENV/$SA_DIR/*.r4a
  	        do
  		        mv -f $FILE ${FILE%.r4a}
  	      done
	      fi
    done
done < $DW_MASTER_CFG/dw_etl_job_env.lis
}


function rm_dwwatch_olderthan30_file {
#------------------------------------------------------------------------
#  Remove extract touch files greater than 30 days old.
#------------------------------------------------------------------------
print "[rm_dwwatch_olderthan30_file] Removing extract touch files greater than 30 days old  `date`"

set +e
find $DW_WATCH/extract/ -type f -mtime +30 -exec rm -f {} \;
set -e	
}

function deal_shared_files {
#------------------------------------------------------------------------
#  deal the files under local directory
#  in Tempo servers, such directories are mounted on shared storage
#  $DW_LOG and $DW_WATCH
#------------------------------------------------------------------------
print "Deal with the files under $DW_LOG and $DW_WATCH"
deal_dwsalog_selflogfile
rm_dwlog_olderthan31_dir
archive_dwlog_r4afiles
rm_dwwatch_olderthan30_file
}

function deal_local_files {
#------------------------------------------------------------------------
#  deal the files under local directory
#  in Tempo servers, such directories are mounted on shared storage
#  $DW_ARC, $DW_IN, $DW_MFS
#------------------------------------------------------------------------
print "Deal with the files under $DW_ARC, $DW_IN, $DW_MFS"
rm_dwarc_olderthantoday_datgzfile
rm_dwarc_olderthantoday_zipfile
rm_dwarc_olderthantoday_dir
rm_dwin_r4amassfile
rm_dwmfs_r4amassfile
rm_dwarc_olderthan31_file
archive_dwin_r4afile
archive_dwmfs_r4afile
}

if [ $ARCH_TYPE = "arch_shared" ]
then
	deal_shared_files
else
	deal_local_files
fi

print ""
print "####################################################################################"
print "#"
print "# Archive of data and log files complete  `date`"
print "#"
print "####################################################################################"

tcode=0

exit
