SERVERLIST=${1:-ab_serverlist}

while read SN ABH
do
   LFILE=/dw/etl/mstr_bin/infra/log/$SN.check_key.$ABH.log
      ssh -n $SN 'export AB_HOME=/usr/local/abinitio-V2-15;PATH=$AB_HOME/bin:$PATH:;ab-key show' > $LFILE 2>&1
      ssh -n $SN 'export AB_HOME=/usr/local/abinitio;PATH=$AB_HOME/bin:$PATH:;ab-key show' > $LFILE 2>&1
done < ab_serverlist

