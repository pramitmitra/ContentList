#!/bin/ksh -eu
#------------------------------------------------------------------------------------------------
# Title:        Synch_mfs_HADR_Node.ksh
# File Name:    synch_mfs_hadr_node.ksh
# Description:  Synch a passed mfs file
#               Node synch process on HA. If we are running on HA already - it would
#               synch directly to DR.
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
# 2009-10-21   1.0    Brian Wenner                  initial version
# 2013-10-04   1.1    Ryan Wong                     Redhat changes
#------------------------------------------------------------------------------------------------

if [ $# != 2 ]
then
        print "Usage:  $0 <synch file >  <log file>"
        exit 4
fi

# take the passed file list, and build the SGFT list file for pushing data files

export MCPFILE=$1
LFILE=$2

. /dw/etl/mstr_cfg/etlenv.setup
. $DW_MASTER_LIB/dw_etl_common_functions.lib
 
_vl=${DW_FUNC_VERBOSITY:-0}
if [ _vl > 0 ]
then
   $VLT='-v'
fi

# what type of node am I, if primary, I synch to HA, then launch DR synch on HA node.
# if HA, then I synch to DR only
# if DR, then I have nothing to synch to


if [ "$SNODETYPE" == "PR" ]
then
  HADRTYPE="HA"
  TNODE=$HANODE
  TACTIVE=$HAACTIVE
  print "Running as Primary"
elif [ "$SNODETYPE" == "HA" ]
then
  HADRTYPE="DR"
  TNODE=$DRNODE
  TACTIVE=$DRACTIVE
  print "Running as HA"
elif [ "$SNODETYPE" == "DR" ]
then
  print "Am running on DR node - there is nothing to synch"
  exit 0
else
   print "Invalid Server Node Type is set, must be PR, HA, or DR."
   exit 4
fi

#decide if you want to 'save' synching files when individual server is not active.
if [ $TACTIVE != 1 ]
then
   print "$HADRTYPE processing is not active"
   exit 0
fi


if [[ -f $MCPFILE ]]
then
   m_cp $VLT mfile://$SGFT_SNODE/$MCPFILE mfile://${TNODE}/$MCPFILE
else
   print "File does not exist on source: $MCPFILE"
   exit 4
fi

  # if node is PR, create the HA-to-DR sft file and add it to the HA file transfer
  if [ "$SNODETYPE" == "PR" ]
  then

    DR_SFILE=$LISTFILE.DR.sft.lis
  
    if [[ -f $LISTFILE ]]
    then
      while read FN
      do
        if [[ -f $FN ]]
        then
          print "${DRNODE}:${FN},${FN}" >> $DR_SFILE
        else
          print "File does not exist on source: $FN"
         exit 4
        fi
      done < $LISTFILE
    fi
    # listfile created - push it to HA in $DW_MASTER_DAT/sft
    HA_SFILE=${DR_SFILE##*/}
    print "${TNODE}:/$DW_MASTER_DAT/sft/$HA_SFILE,$DR_SFILE" >> $SFILE
  fi
    
  CMP=$SGFT_COMPRESS
  BW=$SGFT_BW
  #ENC=$SGFT_ENCRYPT pending confirmation of flags/support
  VL=$SGFT_VERBOSE_LEVEL
  PI=$SGFT_PRINT_INTERVAL
  NW=$SGFT_NWAYS
  PT=$SGFT_PORT

  #synch the files
  set +e
  $DW_MASTER_BIN/sg_file_xfr_client -d 2 -f $SFILE -x $XFILE -l $LFILE -p $PT -c $CMP -b $BW -i $PI -v $VL -n $NW
  RCODE=$?
  set -e

  if [ $RCODE != 0 ]
  then
     print " process failed with return code ( $RCODE ) - view log at $LFILE"
     exit 4
  elif [[ -f $XFILE ]]
  then
      print "Exception file ( $XFILE ) exists after process ended.  Process did not complete successfully"
      print "View log at $LFILE"
      exit 4
  fi
else
 #no list file found, print failure and exit with failure.
 print "List file does not exist, Listfile passed: $LISTFILE"
 exit 4
fi

exit 0
