#!/usr/bin/ksh -eu

read ETL_ENV < $HOME/.etlenv

for DIR_NAME in `echo "td1 td2 td3 td4 td5 td6 hd1 hd2 hd3"`
do

  
  if [ -d /dw/etl/home/${ETL_ENV}/in/$DIR_NAME ]
  then
  	    	
	  set +e
           cd /dw/etl/home/${ETL_ENV}/in/;ls -l|grep ^d|grep -w $DIR_NAME|grep -iv old  > /dev/null 2>&1
	   rcode=$? 
	  set -e
	  
	  if [[ $rcode = 0 ]]
	  then 
 
	   mv /dw/etl/home/${ETL_ENV}/in/$DIR_NAME   /dw/etl/home/${ETL_ENV}/in/${DIR_NAME}_old  
           ln -s   /dw/etl/home/${ETL_ENV}/in/extract  /dw/etl/home/${ETL_ENV}/in/$DIR_NAME 
            rsync -a  /dw/etl/home/${ETL_ENV}/in/${DIR_NAME}_old/*    /dw/etl/home/${ETL_ENV}/in/extract/    
	  fi
  else
 
   ln -s   /dw/etl/home/${ETL_ENV}/in/extract  /dw/etl/home/${ETL_ENV}/in/$DIR_NAME
 
  fi
done
