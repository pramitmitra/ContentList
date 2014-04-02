#!/bin/ksh -eu

MOD_SEQ_NUM_FILE=$DW_SA_DAT/$TABLE_ID.$JOB_TYPE.mod_seq_num.dat

cat $DW_SA_DAT/$TABLE_ID.$JOB_TYPE.mod_seq_num.dat |read base seq rest 

print  " ${base} )= ${seq} "

exit
