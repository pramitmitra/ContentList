while read SN
do
   ssh -n $SN 'export AB_HOME=/usr/local/abinitio;PATH=$AB_HOME/bin:$PATH:;ab-key -y show' > ./log/$SN.check_key.log
done < ab_serverlist.215

