#!/bin/ksh -eu
#------------------------------------------------------------------------------------------------
# Title:        single table extract resubmit handler
# File Name:    single_table_extract_resubmit_handler.ksh
# Description:  This job runs while the single table handlers mult-process jobs are running, to allow 
#               for restarts keyed via an ETL_ID/DBC level file in the SA_TMP_DIR. 
# Developer:    Brian Wenner
# Created on:   2006-09-11
# Location:     $DW_EXE  
# Logic:       
#
#
# Called by:    single_table_extract_handler.ksh
#
# Date         Ver#   Modified By(Name)            Change and Reason for Change
#---------    -----  ---------------------------  ------------------------------
# 2006-09-11   1.0    Brian Wenner                  Added Heading Info
# 2007-05-31   1.1    Orlando Jin                   Add PPID to avoid unexpected process kill/resubmit
# 2007-10-24   1.2    Cesar Valenzuela              Changed CATY to DISTR_TABLE
# 2013-10-04   1.3    Ryan Wong                     Redhat changes
#------------------------------------------------------------------------------------------------

#assume EXTRACT_CONN_TYPE is exported from parent
set -A MPLIS_PID $1
set -A MPLIS_DBC_FILE $2
set -A MPLIS_PPID $3
integer mpidx=0
MPLIS=""

for MPPID in ${MPLIS_PID[*]}
do
   MPLIS="$MPLIS,$MPPID"
done
MPLIS=${MPLIS#,}


GREP_PATTERN=""
x=0
while [ $x -lt ${#MPLIS_PID[*]} ] 
do
  GREP_PATTERN="${GREP_PATTERN}|^ *${MPLIS_PID[$x]} *${MPLIS_PPID[$x]}\$"
  ((x+=1))
done
GREP_PATTERN=${GREP_PATTERN#'|'}
#print "$GREP_PATTERN"

TABLE_LIS_FILE=$DW_CFG/$ETL_ID.sources.lis
function _update_last_extract_value
{
if [ $EXTRACT_PROCESS_TYPE != 'D' ]
then
        exit    $_exit_status
fi
	if [ -f $MULTI_COMP_FILE ]
	then
		set +e
		cp $MULTI_COMP_FILE $MULTI_COMP_FILE.tmp
		rcode=$?
		set -e
		if [ $rcode != 0 ]
		then
			exit 
		fi
		print "Complete file found in the trap function"
		if [ $USE_DISTR_TABLE -eq 1 ]
		then
        while read FILE_ID DBC_FILE STBY_DBC_FILE
			  do
        # check to see if the $FILE_ID process has already been run (exists in the complete file).
        #  If so, dont replace dat files.
				  read SOURCE_NAME DATA_FILENAME PARAM_LIST < $TABLE_LIS_FILE 	
                                  eval SOURCE_NAME=$SOURCE_NAME
          set +e
          grep "^$FILE_ID $SOURCE_NAME" $MULTI_COMP_FILE.tmp >/dev/null
          rcode=$?
          set -e
        if [ $rcode = 1 ]
        then
            print "Last extract value file for FILE_ID: $FILE_ID is replaced" 
            set +e
            cp $DW_SA_TMP/$TABLE_ID.$FILE_ID.last_extract_value.dat.tmp $DW_SA_DAT/$TABLE_ID.$FILE_ID.last_extract_value.dat
            set -e
        fi
        done < $DW_CFG/$DISTR_TABLE_LIS_FILE.sources.lis
		else
			while read FILE_ID CONN_FILE PARALLE_NUM SOURCE_NAME REST
      do
        # check to see if the $FILE_ID process has already been run (exists in the complete file).
				#  If so, dont replace dat files.
       	set +e
        grep "^$FILE_ID $SOURCE_NAME" $MULTI_COMP_FILE.tmp >/dev/null
      	rcode=$?
       	set -e
        if [ $rcode = 1 ]
        then
					  print "Last extract value file for FILE_ID: $FILE_ID is replaced"
          	set +e
            cp $DW_SA_TMP/$TABLE_ID.$FILE_ID.last_extract_value.dat.tmp $DW_SA_DAT/$TABLE_ID.$FILE_ID.last_extract_value.dat
           	set -e
       	fi
     	done < $TABLE_LIS_FILE	
		fi
		set +e
        	rm  $MULTI_COMP_FILE.tmp
        	set -e
	else 
		print "Complete file not found trap"
	fi
}

trap '_exit_status=$?;_update_last_extract_value;exit $_exit_status' EXIT HUP QUIT TERM INT			
 
#get USE_DISTR_TABLE value
set +e
grep "^USE_DISTR_TABLE\>" $DW_CFG/$ETL_ID.cfg | read PARAM USE_DISTR_TABLE COMMENT
rcode=$?
set -e

if [ $rcode != 0 ]
then
   print "${0##*/}:  ERROR, failure determining value for USE_DISTR_TABLE parameter from $DW_CFG/$ETL_ID.cfg" >&2
   exit 4
fi
#get USE_DISTR_TABLE_STBY value
if [ $USE_DISTR_TABLE -eq 1 ]
then
   set +e
   grep "^USE_DISTR_TABLE_STBY\>" $DW_CFG/$ETL_ID.cfg | read PARAM USE_DISTR_TABLE_STBY COMMENT
   rcode=$?
   set -e

   if [ $rcode != 0 ]
   then
      print "${0##*/}:  ERROR, failure determining value for USE_DISTR_TABLE_STBY parameter from $DW_CFG/$ETL_ID.cfg" >&2
      exit 4
   fi
fi

#get DISTR_TABLE_LIS_FILE value
set +e
grep "^DISTR_TABLE_LIS_FILE\>" $DW_CFG/$ETL_ID.cfg | read PARAM DISTR_TABLE_LIS_FILE COMMENT
rcode=$?
set -e

if [ $rcode != 0 ]
then
   print "${0##*/}:  ERROR, failure determining value for DISTR_TABLE_LIS_FILE parameter from $DW_CFG/$ETL_ID.cfg" >&2
   exit 4
fi

set +e
grep "^EXTRACT_PROCESS_TYPE\>" $DW_CFG/$ETL_ID.cfg | read PARAM EXTRACT_PROCESS_TYPE COMMENT
rcode=$?
set -e

if [ $rcode != 0 ]
then
   print "${0##*/}:  ERROR, failure determining value for EXTRACT_PROCESS_TYPE parameter from $DW_CFG/$ETL_ID.cfg" >&2
   exit 4
fi

if [ $EXTRACT_PROCESS_TYPE != 'D' ]
then
   print "${0##*/}: ETL_ID: $ETL_ID is not a DBC extract, exiting restart monitoring."
   exit 0
fi
# create a copy of last extract values
# When extract is killed after the last extract value is updated this will prevent it
if [ $FIRST_RUN = Y ]
then
	if [ $USE_DISTR_TABLE -eq 1 ]
	then
		READ_DBC_FILE=$DW_CFG/$DISTR_TABLE_LIS_FILE.sources.lis
	else 
		READ_DBC_FILE=$DW_CFG/$ETL_ID.sources.lis
	fi

	while
	if [ $USE_DISTR_TABLE -eq 1 ]
	then
        	read FILE_ID DBC_FILE STBY_DBC_FILE
	else
        	read FILE_ID DBC_FILE PARALLEL_NUM TABLE_NAME DATA_FILENAME PARAM_LIST
	fi
	do
		set +e
		cp $DW_SA_DAT/$TABLE_ID.$FILE_ID.last_extract_value.dat $DW_SA_TMP/$TABLE_ID.$FILE_ID.last_extract_value.dat.tmp
		set -e
	done < $READ_DBC_FILE
fi
		
#end creating the last extract values

while [ $(ps -p$MPLIS -o'pid ppid' | egrep "$GREP_PATTERN" | wc -l) -ge 1 ] || [ -f $DW_SA_TMP/$ETL_ID.ex.*.restart ] 
#when this list gets to one, there are no more active subprocesses running.
# added functionality to check for the restart file
do

   #look for any restart jobs for this ETL ID.
   if [ -f $DW_SA_TMP/$ETL_ID.ex.*.restart ] 
   then
      mpidx=0
      #step through each job, and make sure its not running still, then start it again with info in 
      for FILE in $(ls $DW_SA_TMP/$ETL_ID.ex.*.restart)
      do
	print " Restart file found $FILE"
        RDBC_FILE=${FILE#$DW_SA_TMP/$ETL_ID.ex.}
        RDBC_FILE=${RDBC_FILE%%.restart}
	# renaming file with standard name so that clean up will pick it
	RENAME_FILE=${FILE#$DW_SA_TMP/$ETL_ID}  
        RENAME_FILE=${RENAME_FILE%%.restart}
        RENAME_FILE="$DW_SA_TMP/$TABLE_ID$RENAME_FILE.restarted"	
	CRNT_RST_FILE=$FILE
        read HOST_DISTR_TABLE_STBY < $FILE
         if [ $USE_DISTR_TABLE -eq 1 ]
         then
            DISTR_TABLE_FOUND=0
            while read FILE_ID DBC_FILE STBY_DBC_FILE
            do
	      #added this to help stby catys
		
	      if [ $USE_DISTR_TABLE_STBY -eq 0 ]
	      then
		 COMP_DBC_FILE=$DBC_FILE
	      else
		 COMP_DBC_FILE=$STBY_DBC_FILE
              fi

              if [ $COMP_DBC_FILE == $RDBC_FILE ]
              then
                 DISTR_TABLE_FOUND=1
                 break
              fi
            done < $DW_CFG/$DISTR_TABLE_LIS_FILE.sources.lis
            if [ DISTR_TABLE_FOUND -eq 1 ]
            then 
            #determine new DBC_FILE - in case its diff than standard run
               if [ $HOST_DISTR_TABLE_STBY -eq 0 ]
               then
                  eval RDBC_FILE=$DBC_FILE
               else
                  eval RDBC_FILE=$STBY_DBC_FILE
               fi

            #determine regular DBC_FILE - this will be used for all log files for consistency
               if [ $USE_DISTR_TABLE_STBY -eq 0 ]
               then
                  eval DBC_FILE=$DBC_FILE
               else
                  eval DBC_FILE=$STBY_DBC_FILE
               fi
            else
               print "${0##*/}:  ERROR, DBC_FILE: $RDBC_FILE for ETL_ID $ETL_ID could not be found" >&2  
               exit 4
            fi


         else #non-DISTR_TABLE MULTI-JOB, so no standby, and use TABLE_LIS_FILE      
            #get the dbc file
            HOST_FOUND=0
	    #this supports pointing to regular hosts when running against standbys
	      set +e
              print $RDBC_FILE|grep "stby.dbc" >/dev/null
              rcode=$?
              set -e
              if [ $rcode = 0 ]
              then
		IS_NONDISTR_TABLE_STBY=1
	      else
	        IS_NONDISTR_TABLE_STBY=0
              fi
            while read FILE_ID DBC_FILE THE_REST
            do
              if [ $DBC_FILE == $RDBC_FILE ]
              then
                 HOST_FOUND=1
                 break
              fi
            done < $TABLE_LIS_FILE
            if [ HOST_FOUND  -eq 1 ]
            then
		if [ $IS_NONDISTR_TABLE_STBY = 1 ]
		then
		   COMP_DBC_FILE="${DBC_FILE%*stby.dbc}.dbc"
	 	else
               	   eval COMP_DBC_FILE=$DBC_FILE
                fi

                if [ $HOST_DISTR_TABLE_STBY -eq 0 ]
                then
                   RDBC_FILE=$COMP_DBC_FILE
                else
                   RDBC_FILE="${COMP_DBC_FILE%*.dbc}stby.dbc"
                fi
            else
               print "${0##*/}:  ERROR, DBC_FILE: $RDBC_FILE for ETL_ID $ETL_ID could not be found" >&2
               exit 4
            fi

         fi
         #now that we have the DBC FILE, check to see if the process is still running, if so sleep
         while [ $mpidx -lt ${#MPLIS_PID[*]} ]
         do
            print "mpidx: $mpidx  Count MPLIS_PID: ${#MPLIS_PID[*]}"
            print "mplis index dbcFile: ${MPLIS_DBC_FILE[$mpidx]}   DBC_FILE: $DBC_FILE"
            if [ ${MPLIS_DBC_FILE[$mpidx]} == $DBC_FILE ]
            then
               #see if its running
               while [ $(ps -p${MPLIS_PID[$mpidx]} -o'pid ppid' | grep " ${MPLIS_PPID[$mpidx]}$" | wc -l) -ge 1 ]
               do 
		 sleep 30
                 continue
               done
               #process is dead, use this mpidx on restart to added subprocess
		print " process is killed  use this mpidx on restart to added subprocess"
		print " $MPLIS_PID[$mpidx] will be used "
               break
            fi
            ((mpidx+=1))
         done   

         #once the process is dead, resubmit using the .lis file record for that DBC_FILE
         
         if [ -f $DW_SA_TMP/$TABLE_ID.$DBC_FILE.*.lis ]
         then
	    FILE=$(ls $DW_SA_TMP/$TABLE_ID.$DBC_FILE.*.lis)
	    while read FILE_ID DBC_VAR REST
	    do
		print $FILE_ID $RDBC_FILE $REST >> $FILE.restart
	    done < $FILE 

            set +e 
            mv $FILE.restart $FILE
	    set -e

            LOG_FILE=$DW_SA_LOG/$TABLE_ID.$JOB_TYPE_ID.${DBC_FILE%.dbc}.run_multi_single_table_extract.$CURR_DATETIME.log
	    if [ -f $LOG_FILE ]
	    then
		mv $LOG_FILE $LOG_FILE.r4a
	    fi
            if [ -f ${PARENT_LOG_FILE%.log}.err ]
            then
               print "Removing previous error file: ${PARENT_LOG_FILE%.log}.err `date`"
               mv ${PARENT_LOG_FILE%.log}.err ${PARENT_LOG_FILE%.log}.err.r4a
            fi
	    # copying the last extract values for the files
		while read FILE_ID CONN_FILE SOURCE_NAME REST
		do
			print "Copying Dat file for the resubmit"
			# check to see if the $FILE_ID process has already been run (exists in the complete file).  If so, skip it.
        		set +e
        		grep "^$FILE_ID $SOURCE_NAME" $MULTI_COMP_FILE >/dev/null
        		rcode=$?
        		set -e
			print "$FILE_ID $SOURCE_NAME"
			if [ $rcode = 1 ]
			then
				set +e
				cp $DW_SA_TMP/$TABLE_ID.$FILE_ID.last_extract_value.dat.tmp $DW_SA_DAT/$TABLE_ID.$FILE_ID.last_extract_value.dat
				set -e
				# Added the following to handle rec files
				if [ -f ${PWD_VAR}/single_${EXTRACT_TYPE}_extract.${ETL_ID}.${FILE_ID}.${ETL_ENV}.extract.rec ]
				then
					print " rec file ${PWD_VAR}/single_${EXTRACT_TYPE}_extract.${ETL_ID}.${FILE_ID}.${ETL_ENV}.extract.rec exists removing .."
					m_rollback -d ${PWD_VAR}/single_${EXTRACT_TYPE}_extract.${ETL_ID}.${FILE_ID}.${ETL_ENV}.extract.rec
				fi
			fi
		done < $FILE

            print "Running run_multi_single_extract.ksh for $EXTRACT_TYPE $FILE  `date`"
            COMMAND="$DW_EXE/run_multi_single_extract.ksh $EXTRACT_TYPE $FILE $LOG_FILE > $LOG_FILE 2>&1"
         fi

         set +e
#         eval $COMMAND || print "${0##*/}: ERROR, failure processing for $FILE, see log file $LOG_FILE" >>$ERROR_FILE &
         eval $COMMAND &
         MPLIS_PID[$mpidx]=$!
         MPLIS_PPID[$mpidx]=$$
         set -e
         MPLIS="${MPLIS},${MPLIS_PID[$mpidx]}"

         # Rebuild the grep patten string
         GREP_PATTERN=""
         x=0
         while [ $x -lt ${#MPLIS_PID[*]} ] 
         do
           GREP_PATTERN="${GREP_PATTERN}|^ *${MPLIS_PID[$x]} *${MPLIS_PPID[$x]}\$"
           ((x+=1))
         done
         GREP_PATTERN=${GREP_PATTERN#'|'}

         set +e
         mv $CRNT_RST_FILE $RENAME_FILE
         set -e
     done
   fi
   sleep 30 
	print " no restart file found so looping back"
done
         
exit
