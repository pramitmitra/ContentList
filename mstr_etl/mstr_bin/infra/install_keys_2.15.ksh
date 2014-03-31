while read SN
do
if [ ! -f ./log/$SN.add_key.log ] 
then
   ssh -n $SN 'export AB_HOME=/usr/local/abinitio;PATH=$AB_HOME/bin:$PATH:;ab-key -y show;ab-key -y add /dw/etl/mstr_bin/key/' > ./log/$SN.add_key.log
else
   echo $SN already processed
fi
done < ab_serverlist.215

