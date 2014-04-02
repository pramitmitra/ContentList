#!/usr/bin/ksh -eu
SERVERLIST=${1:-tmp_serverlist}

while read SN 
do
echo "running $SN"
   ssh -n $SN '. /dw/etl/mstr_cfg/etlenv.setup; mkdirifnotexist /dw/etl/mstr_dat/sft/pending;mkdirifnotexist /dw/etl/mstr_dat/sft/excpt' 
done < $SERVERLIST

