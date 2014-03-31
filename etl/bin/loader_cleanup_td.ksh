#!/bin/ksh 
#------------------------------------------------------------------------------------------------
# Filename:     loader_cleanup.ksh
#
# Revision History:
#
# Name             Date            Description
# ---------------  --------------  ---------------------------------------------------
# ???              ??/??/????      Initial Creation
# Ryan Wong        10/04/2013      Redhat changes
#
#------------------------------------------------------------------------------------------------

JOB_ENV=$1
JOB_TYPE_ID=$2
ETL_ID=$3
DATA_RET_DAYS=$4 
CNDTL_COMPRESSION_SFX=$5

export SUBJECT_AREA=${ETL_ID%%.*}
export TABLE_ID;TABLE_ID=${ETL_ID##*.}

mpjret=$?
if [ 0 -ne $mpjret ] ; then
	   print -- Error evaluating: 'parameter TABLE_ID', interpretation 'shell'
	      exit $mpjret
fi

CURR_DATETIME=$(date '+%Y%m%d-%H%M%S')

. /dw/etl/mstr_cfg/etlenv.setup

DW_SA_TMP="$DW_TMP/$JOB_ENV/$SUBJECT_AREA"
DW_SA_LOG="$DW_LOG/$JOB_ENV/$SUBJECT_AREA"
DW_SA_DAT="$DW_DAT/$JOB_ENV/$SUBJECT_AREA"
DW_SA_IN="$DW_IN/$JOB_ENV/$SUBJECT_AREA"
log_file=$DW_SA_LOG/$TABLE_ID.ld.loader_cleanup.$CURR_DATETIME.log


if [ $JOB_ENV = extract ]
then

#  default DATA_RET_DAYS to 0 if DATA_RET_DAYS is not provided.

        if [[ $DATA_RET_DAYS == "" ]]
        then
                DATA_RET_DAYS=0
        fi

        #----------------------------------------------------------------------------------------------------------
        # DEL_DATE represents the date the data file can be deleted.  It will be appended to the data file
        # name at the time it is marked as ready for archive.
        #----------------------------------------------------------------------------------------------------------
        DEL_DATE=$($DW_EXE/add_days ${CURR_DATETIME%-*} $DATA_RET_DAYS)

        #----------------------------------------------------------------------------------------------------------
        # LOAD_JOB_ENV is extract .  Determines which load pattern file(s) to
        # use to determine when data files have been loaded and can be archived.
        #----------------------------------------------------------------------------------------------------------

		   MIN_LOAD_PATTERN=$6
	
print "
#################################################################################################
#
#	Cleaner process for the load pattern $MIN_LOAD_PATTERN
#
###############################################################################################" > $log_file
        #----------------------------------------------------------------------------------------------------------
        # rename data files and record count files with a load pattern <= to the minimum
        # load pattern from any of the load processes (extract or acquisition) for this ETL_ID.
        #----------------------------------------------------------------------------------------------------------
	print "Moving Data Files to r4a directory" >> $log_file
        if [ -f $DW_SA_IN/$TABLE_ID.!(*.r4a) ]
        then

                # For compressed files, pull compression suffix from filename before determining batch_seq_num
                # Check for compressed files even when compression may not be active, and vise versa, in case recent change
                if [ $CNDTL_COMPRESSION_SFX != "" ]
                then
                        CNDTL_COMPRESSION_SFX=".gz"
                fi

                R4A_FILE=$DW_SA_TMP/$TABLE_ID.$JOB_TYPE_ID.r4a.$DEL_DATE
                FIRST_FILE=1
                for fn in $DW_SA_IN/$TABLE_ID.!(*.r4a)
                do
                        if [ ${fn##*.} = ${CNDTL_COMPRESSION_SFX#.} ]
                        then
                                RM_EXTENSION_1=${fn%.*}
				RM_EXTENSION=${RM_EXTENSION_1%.*}
                                FILE_LOAD_PATTERN=${RM_EXTENSION##*.}
                        else
                                FILE_LOAD_PATTERN=${fn##*.}
                        fi
                        if [[ $6 != "No_Arc" ]]
			then  
                       		 if [ $FILE_LOAD_PATTERN -le $MIN_LOAD_PATTERN ]
                       		 then
                               		 if [ $FIRST_FILE = 1 ]
                                	 then
                                        	 print $fn>$R4A_FILE
                                         	FIRST_FILE=0
                                	 else
                                        	print $fn>>$R4A_FILE
                                	 fi
                        	fi
			fi
                done

                if [ -f $R4A_FILE ]
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

print "Removing tmp files "   >> $log_file

if [ $JOB_TYPE_ID != tr ]
then
    #------------------------------------------------------------------------
    #  Remove extract and load temp files.
    #------------------------------------------------------------------------
    if [ -f $DW_SA_TMP/$TABLE_ID.$JOB_TYPE_ID.* ]
    then
        for fn in $DW_SA_TMP/$TABLE_ID.$JOB_TYPE_ID.*
        do
            rm -f $fn
        done
    fi

    #------------------------------------------------------------------------
    #  Move extract and load log/err files to the archive directory.
    #------------------------------------------------------------------------

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

fi

print "Loader Clean up completed successfully" >>$log_file
exit

