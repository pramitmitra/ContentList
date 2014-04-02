export SUBJECT_AREA=$1

export ETL_ID=${SUBJECT_AREA}.push_sa_code

. /dw/etl/mstr_cfg/etlenv.setup

if [[ ! -d $DW_CFG || ! -d $DW_SQL ]]
then
   echo "please run dw_infra.create_subject_area_dirs.ksh create subject area dir first"
      
else  

mv $DW_HOME/cfg/$SUBJECT_AREA.*   $DW_HOME/cfg/$SUBJECT_AREA/
mv $DW_HOME/dml/$SUBJECT_AREA.*   $DW_HOME/dml/$SUBJECT_AREA/
mv $DW_HOME/xfr/$SUBJECT_AREA.*   $DW_HOME/xfr/$SUBJECT_AREA/
mv $DW_HOME/sql/$SUBJECT_AREA.*   $DW_HOME/sql/$SUBJECT_AREA/
mv $DW_HOME/exe/$SUBJECT_AREA.*   $DW_HOME/exe/$SUBJECT_AREA/

fi
