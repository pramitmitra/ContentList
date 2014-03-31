while read SN ABH
do
if [ ! -f ./log/$SN.add_key.216.log ] 
then
   ssh -n $SN 'export AB_HOME=/usr/local/$ABH;echo $ABH;PATH=$AB_HOME/bin:$PATH:;ab-key -y show;ab-key -y add /dw/etl/mstr_bin/key/' > ./log/$SN.add_key.216.log
else
   echo $SN already processed for 2.16
fi
done < ab_serverlist.216

