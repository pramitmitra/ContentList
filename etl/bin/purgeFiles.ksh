#!/bin/ksh
this=`basename $0`
# this file is best viewed with tabstop=2
#==========================================================================================
# Filename:    purgeFiles.sh
# Description: purge files matching a given pattern, from a given directory.You have the option
#              of either files that older than a given nr of days
#                        OR
#                        files that are larger than a given size 
#
# Developer:   Lucian
# Created on:  2004-11-13
#
# Revision History:
#
# Name             Date            Description
# ---------------  --------------  ---------------------------------------------------
# Lucian           11/13/2004      Initial Creation
# Ryan Wong        10/04/2013      Redhat changes
#
# Called By:    from Unix or Appworx (LIB_PURGEFILES)
USAGE_purgef () {
cat << EOF
#
# USAGE : $this help    <-- will give you a full description
#       : $this directory=<directory> pattern=\"<file pattern>\" 
#       :       keep=<keep_days> 
#       :    OR
#       :       size=<max_size_to_keep(in Kilobytes)> 
#       :
#       :       action=<remove | show>
#       :       [on_exception=<no_email|warning|warning,no_email>]"
#       :       [log_dir=<alternative_log_dir>]"
# WHERE : 
#       : <directory> = directory where the script starts looking for files that match the pattern and 
#       :               creation date(older than keep_days parameter). It looks into all subdirectories, but it 
#       :               does not follow symbolic links
#       : <file pattern> = the file pattern that it's going to look for e.g. "*imk*.dat.gz". Give as specific a pattern
#       :                  as possible. Avoid things like "*" of "*.dat".
#       :                  The pattern can be any valid shell pattern e.g. "*.[ch]" = all files suffixed in .c of .h
#       :                  "*crfe_[0-9][0-9]*.ctl" = all files with crfe followed by a 2 digit number, etc.
#       : IMPORTANT NOTE: if the pattern you pass resolves to files in the current dir (the dir where you are in
#       : when you invoke the tool, the eval at line 147 will fail. In that case, you would need to escape the *
#       : e.g. instead of "dd*imk*dat" you would need to pass "dd\\*imk\\*dat"
#       : <keep_days> = nr of days to keep. Files with a creation date before (today - keep_days) are going to be considered.
#       : E.g. if you pass keep_days parameter as 31, all files older than 31 will be considered for deletion.
#       : <max_size_to_keep> = Files with a size larger than this are going to be considered.
#       : E.g. if you pass size parameter as 500,000, all files larger than 500Mb will be considered for deletion (numbers approximate,
#       : for the exact numbers, use 1024 to multiply instead of 1000)
#       : action=show   = dry run. This will give you a list of files that would be considered.
#       : action=remove  = the files are going to be actually removed
#       : on_exception=<no_email|warning|warning,no_email> = the default behavior in case an exception occurs is :
#       :  error,email (raise an error, send email to DW_BACK_END)
#       :  You can bypass this, by invoking it with the 'on_exception'
#       :  parameter. The accepted options are :
#       : on_exception=no_email
#       : on_exception=warning
#       : on_exception=warning,no_email
#       : on_exception=warning,no_email
#       : log_dir=<alternate_log_dir> = redirect the log file to another directory (Default is : $DW_SA_LOG)
#       :             e.g. log_dir=$DW_HOME/tmp
# EXAMPLES 
#       : $this directory=$DW_OUT/mydir pattern="*imk*.dat.gz*" keep=31 action=show
#       : $this directory=$DW_OUT/mydir pattern="*imk*.dat.gz*" keep=31 action=show on_exception=warning,no_email
#       : $this directory=$DW_OUT/mydir pattern="*imk*.dat.gz*" keep=7 action=remove log_dir=$DW_HOME/log
#       : $this directory=$DW_OUT/mydir pattern="*imk*.dat.gz*" size=300000 action=remove 
#       :
# NOTES : [a] means this param is optional. <a|b|c> means a OR b OR c
#
EOF
}
#         
# Return : 101 - incorrect usage.Incorrect nr of params.
#          102 - incorrect usage. '=' not found in one of the params 
#          103 - can not write to the given log_dir
#          104 - parameter directory not specified
#          105 - You can not run this script while you are in the same directory as the one you want purge
#          106 - parameter  pattern not specified
#          107 - parameter keep or size are not specified
#          108 - only one of : keep or size must be specified
#          109 - parameter action not specified
#
VERSION="DW_ELF_3.0"
#==============================================================================
# Commented for NGC
#. /.dwProfile
#Added for NGC
. /export/home/abinitio/cfg/abinitio.setup

#------------------------------------------
#-- Functions
#------------------------------------------
HELP_exfexp () {
eq_line=0
cat $0|while read line
do
  no_eq=`print $line|grep "^#===="> /dev/null;print $?`
  if (( no_eq == 1 )); then 
    if (( eq_line == 0 )); then
      continue;
    else
      printf "$line\n"|egrep -ve "{|}|EOF"|sed -e "s/\$this/$this/g"
    fi
  else
    printf "$line\n"|egrep -ve "{|}|EOF"|sed -e "s/\$this/$this/g"
    (( eq_line = eq_line + 1 ))
  fi  
  if (( eq_line == 2 )); then
    break
  fi
done
}
        
processException () {
  if [[ -z "$on_exception" ]];then
# Commented for NGC
#       MsgHandler $this ERROR "$*" email |tee -a $log_file
#Added for NGC
        print "$this ERROR $*"

        elif [[ "$on_exception" == "no_email" ]];then
# Commented for NGC
#       MsgHandler $this ERROR "$*" |tee -a $log_file
#Added for NGC
        print "$this ERROR $*" 

        elif [[ "$on_exception" == "warning" ]];then
# Commented for NGC
#       MsgHandler $this WARNING "$*" email |tee -a $log_file
#Added for NGC
        print "$this WARNING $*"

        elif [[ "$on_exception" == "warning,no_email" ]];then
# Commented for NGC
#       MsgHandler $this WARNING "$*" |tee -a $log_file
#Added for NGC
        print "$this WARNING $*" 
        fi
}

#------------------------------------------
#-- Process input parameters - nr. of params
#------------------------------------------
if [[ "$1" == "help" || "$1" == "-help" ]]; then
        HELP_exfexp
        exit 0
fi

nr_parms=$#
if (( nr_parms < 4 )) ; then
                msg="Incorrect number of parameters. Command was : $this $*"
                processException $msg 
                USAGE_purgef
                exit 101
fi

now=`date '+20%y%m%d%H%M%S'`
file_prefix=${this%.sh}.$now.$$

#Commented for NGC
#log_file=$DW_LOG/$file_prefix.log

#------------------------------------------
#-- Process input params - value and type of params
#------------------------------------------
params="$*"

i=0
for p in $params;do
        eq=`print $p|grep = >/dev/null 2>&1;print $?`
        if (( eq != 0 )); then
                msg="One of the parameters does not have '=', or there are extra spaces on the command line. Command was : $this $sql_script $*"
                processException $msg 
                USAGE_exfexp
                exit 102
        fi
        arr[$i]="$p"

        name=`print ${arr[$i]%%=*}`
        value=`print ${arr[$i]##*=}`

        eq_vars=`print ${arr[$i]}|egrep -e "directory=|pattern=|keep=|size=|action=|on_exception=|log_dir=" >/dev/null 2>&1;print $?`

        if (( eq_vars == 0 )); then
                eval `print $name=$value`
        fi
        
        (( i = i + 1 ))
done    

if [[ -n $log_dir ]]; then
        DW_LOG=$log_dir
        if [[ ! -w $log_dir ]]; then
                msg="Can not write to specified log_dir $log_dir"
                processException $msg 
                exit 103
        fi
fi

if [[ -z "$directory" ]]; then
        USAGE_purgef
        msg="Parameter directory not specified."
        processException $msg 
        exit 104
fi

if [[ -z "$pattern" ]]; then
                USAGE_purgef
                msg="Parameter pattern not specified."
                processException $msg 
                exit 106
fi

if [[ ! -z "$keep" && ! -z "$size" ]]; then
         msg="You must only pass one of the 2 params : 'keep' or 'size'"
         processException $msg 
         exit 107
fi

if [[ -z "$keep" ]]; then
         if [[ -z "$size" ]]; then
                 USAGE_purgef
                 msg="Parameter keep or size are not specified."
                 processException $msg 
                 exit 108
         else
                 # size is passed in Mb so :
                 # multiply by 1024 (to get bytes), and then divide by 512 (to get nr of blocks)
                 # so, 
                 (( nr_of_blocks = size * 2 ))
                 condition="-size +$nr_of_blocks"
         fi
else
        condition="-mtime +$keep"
        size=0
fi


if [[ -z "$action" ]]; then
                USAGE_purgef
                msg="Parameter action not specified."
                processException $msg 
                exit 109
fi

#-------------------------------------------------
#-- Purge files older than $keep  days 
#-------------------------------------------------
cnt=0
sz=0
total_sz=0
if (( size != 0 )) ; then
# Commented for NGC
#         MsgHandler $this INFO "Purging files in $directory larger than $size Kb" > $log_file 2>&1
# Added for NGC
          print "$this INFO Purging files in $directory larger than $size Kb"
 else
# Commented for NGC
#         MsgHandler $this INFO "Purging files in $directory older than $keep days" > $log_file 2>&1
# Added for NGC
	  print "$this INFO Purging files in $directory older than $keep days" 
fi

for fil in `find $directory -name "$pattern" -type f $condition -print  2>/dev/null`
do
        #-- If you can't find the file (-f) it means it's a piece of a file that has spaces in it, skip it.
        if [[ ! -f $fil ]]; then
# Commented for NGC
#               MsgHandler $this INFO "Can not find ... ${fil##*/} - probably file name contains spaces." >> $log_file 2>&1
# Added for NGC
                print "$this INFO Can not find ... ${fil##*/} - probably file name contains spaces." 
                continue
        fi
        fil_b=`print $fil|sed -e 's/$directory//g'`
        sz=`ls -l $fil|awk '{print $5}'`
        if [[ "$action" == "remove" ]]; then
# Commented for NGC
#                MsgHandler $this INFO "Removing ... $fil_b ($sz bytes)" >> $log_file 2>&1
# Added for NGC
		 print "$this INFO Removing ... $fil_b ($sz bytes)" 
        \rm -f $fil > /dev/null 2>&1
                ret=$?
                if (( ret != 0 )); then
# Commented for NGC
#                MsgHandler $this WARNING "Remove failed. Permissions ?" >> $log_file 2>&1
# Added for NGC
                 print "$this WARNING Remove failed. Permissions ?"
                else
                (( cnt = cnt + 1 ))
                fi
        else
# Commented for NGC
#                MsgHandler $this INFO "Would remove ... $fil_b ($sz bytes) " >> $log_file 2>&1
# Added for NGC
                print "$this INFO Would remove ... $fil_b ($sz bytes) " 
        (( cnt = cnt + 1 ))
        fi
        (( total_sz = total_sz + sz ))
done

if [[ "$action" == "remove" ]]; then
# Commented for NGC
#        MsgHandler $this INFO "$cnt files removed from $directory and its subdirectories (total $total_sz bytes) (symlinks were not followed)" >> $log_file 2>&1
# Added for NGC
         print "$this INFO $cnt files removed from $directory and its subdirectories (total $total_sz bytes) (symlinks were not followed)"
else
# Commented for NGC
#        MsgHandler "$this INFO $cnt files would be removed from $directory and its subdirectories (total $total_sz bytes) (symlinks are not followed)" 
# Added for NGC
        print "$this INFO $cnt files would be removed from $directory and its subdirectories (total $total_sz bytes) (symlinks are not followed)" 
fi

