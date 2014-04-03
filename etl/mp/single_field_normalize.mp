@
{18|Type|XXGrepository|Primary_Name|XXGgraph|_ab_object_schema|0|_ab_semantic_schema|1|eme_flatten_format|1|schema_version|34|root_id|0|ent_count|112|rel_count|189|}
{2010001002|XXGdirectory|0|1|1|0|{@{}@}}
{2010600005|XXGgraph|1|0|1|0|{|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|TRGT_ETL_ID||3|2|REFO$||{0|}}
{30001002|XXparameter|SRC_BATCH_SEQ_NUM|$BATCH_SEQ_NUM|3|2|REFO$||{0|}}
{30001002|XXparameter|SRC_ETL_ID|$ETL_ID|3|2|REFO$||{0|}}
{30001002|XXparameter|SRC_JOB_ENV|$JOB_ENV|3|2|REFO$||{0|}}
{30001002|XXparameter|SRC_JOB_TYPE|$JOB_TYPE|3|2|REFO$||{0|}}
{30001002|XXparameter|SRC_JOB_TYPE_ID|$JOB_TYPE_ID|3|2|REFO$||{0|}}
{30001002|XXparameter|INPUT_DML_FILENAME|$SRC_ETL_ID.read.dml|3|2|REFO$||{0|}}
{30001002|XXparameter|OUTPUT_DML_FILENAME|$TRGT_ETL_ID.read.dml|3|2|REFO$||{0|}}
{30001002|XXparameter|AB_JOB|$AB_JOB.$SRC_ETL_ID.$TRGT_ETL_ID|3|2|RE$||{0|}}
{30001002|XXparameter|TRGT_ETL_CFG_FILE|$DW_CFG/$TRGT_ETL_ID.cfg|3|2|RE$||{0|}}
{30001002|XXparameter|TRGT_SUBJECT_AREA|$\{TRGT_ETL_ID%%.*\}|3|6|REk||{0|}}
{30001002|XXparameter|TRGT_TABLE_ID|$\{TRGT_ETL_ID##*.\}|3|6|REk||{0|}}
{30001002|XXparameter|SRC_ETL_CFG_FILE|$DW_CFG/$SRC_ETL_ID.cfg|3|2|RE$||{0|}}
{30001002|XXparameter|SRC_SUBJECT_AREA|$\{SRC_ETL_ID%%.*\}|3|6|REk||{0|}}
{30001002|XXparameter|SRC_TABLE_ID|$\{SRC_ETL_ID##*.\}|3|6|REk||{0|}}
{30001002|XXparameter|CURR_DATETIME|$\{CURR_DATETIME:-$(date '+%Y%m%d-%H%M%S')\}|3|6|REk||{0|}}
{30001002|XXparameter|CURR_DATE|$\{CURR_DATE:-$\{CURR_DATETIME%-*\}\}|3|6|REk||{0|}}
{30001002|XXparameter|IS_CATY|$(grep "^IS_CATY\\>" $SRC_ETL_CFG_FILE \| read PARAM VALUE COMMENT; print $VALUE)|3|6|REk||{0|}}
{30001002|XXparameter|IN_DIR|$(
grep "^IN_DIR\\>" $SRC_ETL_CFG_FILE \| read PARAM VALUE COMMENT; 
if [[ "X$UOW_TO" != "X" ]]
then
   eval print $VALUE/$SRC_JOB_ENV/$SRC_SUBJECT_AREA/$SRC_TABLE_ID/$UOW_TO_DATE/$UOW_TO_HH/$UOW_TO_MI/$UOW_TO_SS
else
   eval print $VALUE/$SRC_JOB_ENV/$SRC_SUBJECT_AREA
fi
)|3|6|REk||{0|}}
{30001002|XXparameter|TRGT_IN_DIR|$(
grep "^IN_DIR\\>" $TRGT_ETL_CFG_FILE \| read PARAM VALUE COMMENT; 
if [[ "X$UOW_TO" != "X" ]]
then
   eval print $VALUE/$SRC_JOB_ENV/$TRGT_SUBJECT_AREA/$TRGT_TABLE_ID/$UOW_TO_DATE/$UOW_TO_HH/$UOW_TO_MI/$UOW_TO_SS
   if [[ ! -d $VALUE/$SRC_JOB_ENV/$TRGT_SUBJECT_AREA/$TRGT_TABLE_ID/$UOW_TO_DATE/$UOW_TO_HH/$UOW_TO_MI/$UOW_TO_SS ]]
   then
      mkdir -p $VALUE/$SRC_JOB_ENV/$TRGT_SUBJECT_AREA/$TRGT_TABLE_ID/$UOW_TO_DATE/$UOW_TO_HH/$UOW_TO_MI/$UOW_TO_SS
   fi
   
else
   eval print $VALUE/$SRC_JOB_ENV/$TRGT_SUBJECT_AREA
fi
)|3|6|REKk||{0|}}
{30001002|XXparameter|CNDTL_EXTRACT_COMPRESS_LEVEL|$(grep "^CNDTL_EXTRACT_COMPRESS_LEVEL\\>" $TRGT_ETL_CFG_FILE \| read PARAM VALUE COMMENT; print $\{VALUE:-""\})|3|6|REKk|File Compression Level|{0|}}
{30001002|XXparameter|CNDTL_EXTRACT_COMPRESS|$(grep "^CNDTL_EXTRACT_COMPRESS\\>" $TRGT_ETL_CFG_FILE \| read PARAM VALUE COMMENT;  print $\{VALUE:-0\})|3|6|REKk||{0|}}
{30001002|XXparameter|CNDTL_EXTRACT_NOT_COMPRESS|$(if [[ $CNDTL_EXTRACT_COMPRESS != 1 ]]
  then
     print 1
  else
     print 0
  fi)|3|6|REKk||{0|}}
{30001002|XXparameter|CNDTL_SRC_EXTRACT_COMPRESS|$(grep "^CNDTL_EXTRACT_COMPRESS\\>" $SRC_ETL_CFG_FILE \| read PARAM VALUE COMMENT;  print $\{VALUE:-0\})|3|6|REKk||{0|}}
{30001002|XXparameter|EXTRACT_PROCESS_TYPE|$(grep "^EXTRACT_PROCESS_TYPE\\>" $SRC_ETL_CFG_FILE \| read PARAM VALUE COMMENT; print $VALUE)|3|6|REKk||{0|}}
{30001002|XXparameter|INPUT_FILE|$( INPUT_FILE_TMP=""
   if [ $CNDTL_SRC_EXTRACT_COMPRESS != 1 ]
   then
      COMP_SFX=""
   else
       COMP_SFX=.gz
   fi

   if [ $EXTRACT_PROCESS_TYPE = 'L' ]
   then
      if [[ -n $UOW_TO ]]
      then
         SRC_DATA_LIS=$DW_SA_DAT/$ETL_ID.sources.lis.$UOW_TO
      else
         SRC_DATA_LIS=$DW_SA_DAT/$ETL_ID.sources.lis.$BATCH_SEQ_NUM
      fi

      while read FILE_ID DATA_FILENAME OLD_FILENAME DONE_FILENAME
      do
         if [[ -n $UOW_TO ]]
         then
            DATA_FILENAME_TMP=$DATA_FILENAME$COMP_SFX
         else
            DATA_FILENAME_TMP=$DATA_FILENAME.$SRC_BATCH_SEQ_NUM$COMP_SFX
         fi

         INPUT_FILE_TMP="$INPUT_FILE_TMP $(eval print $IN_DIR/$DATA_FILENAME_TMP)"
      done < $SRC_DATA_LIS
   elif [ $IS_CATY != 1 ]
   then   
      while read FILE_ID DBC_FILE PARALLEL_NUM TABLE_NAME DATA_FILENAME PARAM
      do
         if [[ -n $UOW_TO ]]
         then
            DATA_FILENAME_TMP=$DATA_FILENAME
         else
            DATA_FILENAME_TMP=$DATA_FILENAME.$SRC_BATCH_SEQ_NUM
         fi

         if [[ CNDTL_SRC_EXTRACT_COMPRESS = 1  && -f $(eval print $IN_DIR/$DATA_FILENAME_TMP.Z) ]]
         then
            COMP_SFX=.Z
         fi

         if [ -f $(eval print $\{IN_DIR\}/.$\{DATA_FILENAME_TMP\}$\{COMP_SFX\}.mfctl) ]
         then
            INPUT_FILE_TMP="$INPUT_FILE_TMP $(eval print $(m_expand $IN_DIR/$DATA_FILENAME_TMP$COMP_SFX))"
         else
            INPUT_FILE_TMP="$INPUT_FILE_TMP $(eval print $IN_DIR/$DATA_FILENAME_TMP$COMP_SFX)"
         fi
      done < $DW_CFG/$SRC_ETL_ID.sources.lis
   else
      read TABLE_NAME DATA_FILENAME PARAM < $DW_CFG/$SRC_ETL_ID.sources.lis

      while read FILE_ID DBC_FILE
      do
         if [[ -n $UOW_TO ]]
         then
            DATA_FILENAME_TMP=$DATA_FILENAME
         else
            DATA_FILENAME_TMP=$DATA_FILENAME.$SRC_BATCH_SEQ_NUM
         fi

         if [[ CNDTL_SRC_EXTRACT_COMPRESS = 1  && -f $(eval print $IN_DIR/$DATA_FILENAME.Z) ]]
         then
            COMP_SFX=.Z
         fi

         if [ -f $(eval print $\{IN_DIR\}/.$\{DATA_FILENAMETMP\}$\{COMP_SFX\}.mfctl) ]
         then
            INPUT_FILE_TMP="$INPUT_FILE_TMP $(eval print $(m_expand $IN_DIR/$DATA_FILENAME_TMP$COMP_SFX))"
         else
            INPUT_FILE_TMP="$INPUT_FILE_TMP $(eval print $IN_DIR/$DATA_FILENAME_TMP$COMP_SFX)"
         fi
      done < $DW_CFG/dw_caty.sources.lis
   fi
   print $INPUT_FILE_TMP
)|3|6|REk||{0|}}
{30001002|XXparameter|OUTPUT_FILE|$( INPUT_FILE_TMP=""
   for fn in $INPUT_FILE
   do
      if [[ $\{fn##*.\} = 'Z' \|\| $\{fn##*.\} = 'gz' ]]
      then
         fn=$\{fn%.*\}
      fi

      fn=$(print $fn \| sed s/$SRC_TABLE_ID/$TRGT_TABLE_ID/g)

      fn=$TRGT_IN_DIR/$\{fn##*/\}

      if [ $CNDTL_EXTRACT_COMPRESS = 1 ]
      then
         fn=$\{fn\}.gz
      fi

      INPUT_FILE_TMP="$INPUT_FILE_TMP $fn"
   done

   print $INPUT_FILE_TMP
)|3|6|REk||{0|}}
{30001002|XXparameter|INPUT_DML|$DW_DML/$INPUT_DML_FILENAME|3|2|RE$||{0|}}
{30001002|XXparameter|OUTPUT_DML|$DW_DML/$OUTPUT_DML_FILENAME|3|2|RE$||{0|}}
{30001002|XXparameter|REFORMAT_DML_FILE|$DW_DML/$TRGT_ETL_ID.reformat.write.dml|3|2|RE$||{0|}}
{30001002|XXparameter|REFORMAT_XFR_FILE|$( if [ -f $DW_XFR/$TRGT_ETL_ID.normalize.reformat.xfr ]
   then
      print $DW_XFR/$TRGT_ETL_ID.normalize.reformat.xfr
   else
      print $DW_XFR/$TRGT_ETL_ID.reformat.xfr
   fi
)|3|6|REk||{0|}}
{30001002|XXparameter|REDEFINE_FORMAT_DML_FILE|$DW_DML/$TRGT_ETL_ID.redefine_format.write.dml|3|2|RE$||{0|}}
{30001002|XXparameter|NORMALIZE_XFR_FILE|$DW_XFR/$TRGT_ETL_ID.normalize.xfr|3|2|RE$||{0|}}
{30001002|XXparameter|LOG_FILE_DIR|$DW_LOG/$SRC_JOB_ENV/$TRGT_SUBJECT_AREA/$TRGT_TABLE_ID/$CURR_DATE|3|6|REk||{0|}}
{30001002|XXparameter|LOG_FILE|$LOG_FILE_DIR/$TRGT_TABLE_ID.$SRC_JOB_TYPE_ID.normalize.$CURR_DATETIME.log|3|6|REk||{0|}}
{30001002|XXparameter|RECORD_COUNT_FILE|$(
if [[ ! -d $TRGT_IN_DIR ]]
then
   mkdir -p $TRGT_IN_DIR
fi

if [[ -n $UOW_TO ]]
then
   RECORD_COUNT_FILE_TMP=$TRGT_IN_DIR/$TRGT_TABLE_ID.record_count.dat
else
   RECORD_COUNT_FILE_TMP=$TRGT_IN_DIR/$TRGT_TABLE_ID.record_count.dat.$SRC_BATCH_SEQ_NUM
fi

print $RECORD_COUNT_FILE_TMP
)|3|6|REk||{0|}}
{30001002|XXparameter|WATCH_FILE|$(

WATCH_FILE=$TRGT_ETL_ID.$SRC_JOB_TYPE.$SRC_BATCH_SEQ_NUM.done

OLD_DW_SA_WATCHFILEDIR=$DW_WATCH/$SRC_JOB_ENV
OLD_DW_SA_WATCHFILE=$OLD_DW_SA_WATCHFILEDIR/$WATCH_FILE

if [[ -n $UOW_TO ]]
then
        UOW_WATCH_FILE=$WATCH_FILE
            UOW_WATCH_FILE_WO_DONE=$\{UOW_WATCH_FILE%.done\}
            if [[ $UOW_WATCH_FILE_WO_DONE != $WATCH_FILE ]]
            then
              BATCH_SEQ_NUM=$\{UOW_WATCH_FILE_WO_DONE##*.\}
              if [[ $BATCH_SEQ_NUM == +([0-9]) ]]
              then
                UOW_WATCH_FILE=$\{UOW_WATCH_FILE_WO_DONE%.*\}.done
              fi
            fi
        UOW_DATE=$\{UOW_DATE:-$(print $UOW_TO \| cut -c1-8)\}
        DW_SA_WATCHFILEDIR=$DW_WATCH/$SRC_JOB_ENV/$UOW_DATE
        DW_SA_WATCHFILE=$DW_SA_WATCHFILEDIR/$UOW_WATCH_FILE.$UOW_TO
   print $OLD_DW_SA_WATCHFILE $DW_SA_WATCHFILE
else
   print $OLD_DW_SA_WATCHFILE
fi
)|3|6|REk||{0|}}
{30001002|XXparameter|_parameters_of_Normalized_Output_File|@34|9|FHK|@{0|}}
{30001002|XXparameter|_parameters_of_Normalize_Normalize_on_Vectored_Sub_Records|@34|9|FHK|@{0|}}
{30001002|XXparameter|_parameters_of_Reformat_Determine_Number_of_Normalized_Values|@34|9|FHK|@{0|}}
{30001002|XXparameter|_parameters_of_Redefine_Format_Create_Vectored_Sub_Records|@34|9|FHK|@{0|}}
{30001002|XXparameter|_parameters_of_Input_File|@34|9|FHK|@{0|}}
{30001002|XXparameter|_parameters_of_Gather_Logs|@34|9|FHK|@{0|}}
{30001002|XXparameter|_parameters_of_Rollup|@34|9|FHK|@{0|}}
{30001002|XXparameter|_parameters_of_Output_log_file|@34|9|FHK|@{0|}}
{30001002|XXparameter|_parameters_of_Record_Count_File|@34|9|FHK|@{0|}}
{30001002|XXparameter|_parameters_of_Filter_record_count_rows|@34|9|FHK|@{0|}}
{30001002|XXparameter|_parameters_of_Redefine_Format_1|@34|9|FHK|@{0|}}
{30001002|XXparameter|_parameters_of_Touch_Watch_File|@34|9|FHK|@{0|}}
{30001002|XXparameter|_parameters_of_Deflate|@34|9|FHK|@{0|}}
{30001002|XXparameter|!ab_compatibility|3.1.4.4|3|9|P|@{0|}}
}}@0|@0|0|0|0|1035000|807000|0|single_field_normalize|Kevin Oaks||1|10|0||6||2049|{0|}0|0|{0|}{0|}{0|}{0|}1.0|1035294|807353|7|}}
{2010703001|XXGgraphinfo|2|0|3|0|{@{}@1|3.0.3.1|{3|1|1|1|}Job 'single_field_normalize' Execution Status Report

Compilation of graph "single_field_normalize" started at Friday, October 04, 2013 11:07:31
No compilation errors
||m_env -v
mkdir -pm 0755 $LOG_FILE_DIR
|. /export/home/abinitio/cfg/abinitio.setup








|@|31633919|}}
{2010705002|XXGrunsettings|3|0|5|0|{@{}@lvsdpelega003.lvs.ebay.com||||15|300|0|1|0|1|0|0|0|1|1|0|0|1|1|1|1|0|0|0|@17|}}
{2010242002|XXGface|4|0|7|0|{@{}@Arial|16711680|80|1|}}
{2010240003|XXGlegend|5|0|9|0|{@{}@This graph will take an input record and normalize it based on a single field.
It is designed to work as a post extract process of the single_table_extract_handler.ksh.
It will create a second file from the original extract, using a supplied $TARGET_ETL_ID.
To utilize this graph you must supply the following:

For execution:

TRGT_ETL_ID as a passed in parameter. This graph assumes that $TRGT_ETL_ID has
an associated .cfg file and all standard directories and sub-directories in place.
SRC_BATCH_SEQ_NUM - This is literally passed in as $BATCH_SEQ_NUM on the command line
when run as a post process of single_table_extract_handler.

All other positional parameters are assumed to be inherited from the single_table_extract_handler.ksh.
These are:

SRC_ETL_ID - defaults to$ETL_ID from single_table_extract_handler.ksh
SRC_JOB_ENV - defaults to $JOB_ENV from single_table_extract_handler.ksh
SRC_JOB_TYPE - defaults to $JOB_TYPE from single_table_extract_handler.ksh
SRC_JOB_TYPE_ID - defaults to $JOB_TYPE_ID from single_table_extract_handler.ksh

INPUT_DML_FILENAME - defaults to $SRC_ETL_ID.read.dml
OUTPUT_DML_FILENAME - defaults to $TRGT_ETL_ID.read.dml

* the preceding defaulted values can be overridden by passing them in on the command line.

For run time:

The following must exist in the appropriate directories:

INPUT_DML - a dml file of name $SRC_ETL_ID.read.dml
OUTPUT_DML - a dml file of name $TRGT_ETL_ID.read.dml
REFORMAT_DML_FILE - a dml file of name $DW_DML/$TRGT_ETL_ID.reformat.write.dml
REFORMAT_XFR_FILE - an .xfr file of name $DW_XFR/$TRGT_ETL_ID.normalize.reformat.xfr
                                                                           or  $DW_XFR/$TRGT_ETL_ID.reformat.xfr if first does not exist
REDEFINE_FORMAT_DML_FILE a dml file of name $DW_DML/$TRGT_ETL_ID.redefine_format.write.dml
NORMALIZE_XFR_FILE - an .xfr file of name $DW_XFR/$TRGT_ETL_ID.normalize.xfr|19858|152037|40000|172000|572000|504000|455|}}
{2010242002|XXGface|6|0|11|0|{@{}@Arial|16711680|80|1|}}
{2010210004|XXGflow|7|0|13|0|{@{}@384|.5|.5|{8|101000|94000|101000|94000|127000|94000|134000|94000|}499|2065|}}
{2010210004|XXGflow|8|0|15|0|{@{}@384|.5|.5|{12|750000|100000|750000|100000|763000|100000|763000|94000|777000|94000|784000|94000|}460|17|}}
{2010210004|XXGflow|9|0|17|0|{@{}@384|.5|.5|{8|110000|740000|138000|740000|139000|740000|159000|740000|}503|17|}}
{2010210004|XXGflow|10|0|19|0|{@{}@384|.5|.5|{12|679000|741000|699000|741000|703000|741000|703000|739000|708000|739000|740000|739000|}509|17|}}
{2010210004|XXGflow|11|0|21|0|{@{}@384|.5|.5|{12|328000|94000|328000|94000|341000|94000|341000|93000|354000|93000|361000|93000|}500|17|}}
{2010210004|XXGflow|12|0|23|0|{@{}@384|.5|.5|{12|539000|93000|539000|93000|553000|93000|553000|100000|568000|100000|575000|100000|}477|17|}}
{2010210004|XXGflow|13|0|25|0|{@{}@384|.5|.5|{12|662000|175000|662000|175000|662000|190000|665000|190000|665000|205000|665000|212000|}479|19|}}
{2010210004|XXGflow|14|0|27|0|{@{}@384|.5|.5|{8|315000|740000|335000|740000|338000|740000|358000|740000|}505|17|}}
{2010210004|XXGflow|15|0|29|0|{@{}@384|.5|.5|{12|503000|740000|523000|740000|529000|740000|529000|741000|535000|741000|555000|741000|}507|17|}}
{2010210004|XXGflow|16|0|31|0|{@{}@384|.5|.5|{12|912000|94000|912000|94000|926000|94000|926000|93000|940000|93000|947000|93000|}462|17|}}
{2010501005|XXGpvertex|17|0|33|0|{|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|condition|$CNDTL_EXTRACT_COMPRESS|3|6|k|@{0|}}
{30001002|XXparameter|compression|$CNDTL_EXTRACT_COMPRESS_LEVEL|3|6|k|@{0|}}
{30001002|XXparameter|!prototype_path|$AB_COMPONENTS/Translate/Compress/Deflate.mpc|3|2|Pf$|@{0|}}
{30001002|XXparameter|Layout||3|8|s=|@{0|}}
{30001002|XXparameter|in_metadata||3|8|s=|@{0|}}
}}@0|@774920|41646|795000|62000|107000|65000|461|Deflate|Ab Initio Software|Built-in 2.12.2.m1:|1|10|0||6||32769|1|{1|0|}}}
{2010203004|XXGoport|18|0|35|0|{@{}@901000|89000|11000|11000|out|0.0|@@@2320|0|}}
{2010202004|XXGiport|19|0|38|0|{@{}@785000|89000|11000|11000|in|0.0|@@@1808|0|}}
{2010501005|XXGpvertex|20|0|41|0|{Compresses data in a manner compatible with the zip and gzip deflate method. Use Inflate to uncompress the data.|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|mpname|broadcast|3|1|Hl||{0|}}
{30001002|XXparameter|image__|unitool|3|2|H$||{0|}}
{30001002|XXparameter|condition||3|2|F$||{0|}}
{30001002|XXparameter|conditionInputPort|in|3|2|F$||{0|}}
{30001002|XXparameter|conditionOutputPort|out|3|2|F$||{0|}}
{30001002|XXparameter|condition_interpretation|Replace with flow|15|1|Fl||{2|Replace with flow|Remove completely|}}
{30001002|XXparameter|condition_interpretation.display_name|condition-interpretation|3|9|P|@{0|}}
{30001002|XXparameter|compression|1|1|2|FK$|Compression level (0-9)|{0|}}
{30001002|XXparameter|HelpID|comp_deflate|3|2|FH$||{0|}}
{30001002|XXparameter|port_analysis|out=byte_copy(in)|3|2|H$||{0|}}
{30001002|XXparameter|continuous_analysis|broadcast, publisher|3|2|H$||{0|}}
{30001002|XXparameter|Layout|@9|2|RFs$||{0|}}
{30001002|XXparameter|in_metadata||7|1|RFsl||{0|}}
{30001002|XXparameter|out_metadata|void(1)|7|1|RFl||{0|}}
{30001002|XXparameter|doc_transform||8|2|FHs$|Document your transformation for dependency analysis|{0|}}
{30001002|XXparameter|doc_operation1|out::document(in)|3|2|RH$||{0|}}
{30001002|XXparameter|mpcmodtime|1299550340|1|1|Hl|The last modification time of this component's template|{0|}}
{30001002|XXparameter|_propagate_through||3|1|FHKl|@{0|}}
}}@0|@0|0|0|0|0|0|0|@@@1|10|-1|@6|@1|1|{1|0|}}}
{2010203004|XXGoport|21|0|43|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|out|0.0|@@@2320|0|}}
{2010202004|XXGiport|22|0|46|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|in|0.0|@@@1808|0|}}
{2010503005|XXGfvertex|23|0|49|0|{|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|Layout|$[[record kind 67 subkind 87 parts [vector _interp_("$OUTPUT_FILE", "dollar_substitution")]]]|3|9||@{0|}}
{30001002|XXparameter|!prototype_path|$AB_COMPONENTS/Datasets/Output_File.mdc|3|2|Pf$|@{0|}}
{30001002|XXparameter|write_metadata||3|8|s=|@{0|}}
}}@0|@933282|53723|958000|74000|40000|40000|463|Normalized Output File|Kevin Oaks||1|10|0||6||32769|-1|-1|}}
{2010202004|XXGiport|24|0|51|0|{@{}@948000|88000|11000|11000|write|0.0|@@@1776|0|}}
{2010503005|XXGfvertex|25|0|54|0|{Represents one file, many files, or a multifile as an output from your graph.|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|protection|0666|12|2|RF$||{0|}}
{30001002|XXparameter|mode|0x0062|1|2|FH$|modes of access|{0|}}
{30001002|XXparameter|condition||3|2|F$||{0|}}
{30001002|XXparameter|conditionInputPort||3|2|F$||{0|}}
{30001002|XXparameter|conditionOutputPort||3|2|F$||{0|}}
{30001002|XXparameter|condition_interpretation|Remove completely|15|1|Fl||{2|Replace with flow|Remove completely|}}
{30001002|XXparameter|condition_interpretation.display_name|condition-interpretation|3|9|P|@{0|}}
{30001002|XXparameter|key||19|2|RF$|Key specifier For Lookup File|{0|}}
{30001002|XXparameter|key.condition|param direct_addressed False mode lookup|3|15|P?|@{0|}}
{30001002|XXparameter|m_catalog_add_option_names|block_compressed keep_on_disk direct_addressed|3|2|H$||{0|}}
{30001002|XXparameter|eme_index_location||3|2|F$|Place in the EME to create a dataset corresponding to this index.|{0|}}
{30001002|XXparameter|eme_index_location.condition|mode lookup version 2.15.2.r23:|3|15|P?|@{0|}}
{30001002|XXparameter|index_url|@28|2|F$|The URL of the index for this lookup file (optional)|{0|}}
{30001002|XXparameter|index_url.condition|param direct_addressed False mode lookup version 2.15.2.r23:|3|15|P?|@{0|}}
{30001002|XXparameter|keep_on_disk|False|13|1|Fl|Keep on Disk|{0|}}
{30001002|XXparameter|keep_on_disk.condition|mode lookup version 2.15.2.r23:|3|15|P?|@{0|}}
{30001002|XXparameter|block_compressed|False|13|1|Fl|Block Compressed|{0|}}
{30001002|XXparameter|block_compressed.condition|mode lookup version 2.15.2.r23:|3|15|P?|@{0|}}
{30001002|XXparameter|direct_addressed|False|13|1|Fl|Set to true if the block compressed lookup is direct addressed|{0|}}
{30001002|XXparameter|direct_addressed.condition|param block_compressed True mode lookup version 2.15.5.r34:|3|15|P?|@{0|}}
{30001002|XXparameter|Layout|@28|2|RF$||{0|}}
{30001002|XXparameter|write_metadata||7|1|RFsl||{0|}}
{30001002|XXparameter|mpcmodtime|1299550332|1|1|Hl|The last modification time of this component's template|{0|}}
{30001002|XXparameter|eme_dataset_location||3|2|F$|Place in the EME to create a dataset corresponding to this file.|{0|}}
}}@0|@0|0|0|0|0|0|0|@@@1|10|-1|@6|@1|-1|-1|}}
{2010202004|XXGiport|26|0|56|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|write|0.0|@@@1776|0|}}
{2010501005|XXGpvertex|27|0|60|0|{|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|transform|$NORMALIZE_XFR_FILE|3|2|f$|@{0|}}
{30001002|XXparameter|out_metadata|$OUTPUT_DML|3|2|f$|@{0|}}
{30001002|XXparameter|error_metadata|string('\\n')|3|1|l|@{0|}}
{30001002|XXparameter|log_metadata|record string("\|") node, timestamp, component, subcomponent, event_type; string("\|\\n") event_text; end|3|1|l|@{0|}}
{30001002|XXparameter|!prototype_path|$AB_COMPONENTS/Transform/Normalize.mpc|3|2|Pf$|@{0|}}
{30001002|XXparameter|Layout||3|8|s=|@{0|}}
{30001002|XXparameter|in_metadata||3|8|s=|@{0|}}
{30001002|XXparameter|reject_metadata||3|8|s=|@{0|}}
}}@0|@565998|16389|586000|36000|154000|129000|459|Normalize - Normalize on Vectored Sub-Records|Ab Initio Software|Built-in 2.0:|1|10|0||6||32769|1|{1|0|}}}
{2010203004|XXGoport|28|0|62|0|{@{}@739000|95000|11000|11000|out|0.0|@@@2448|0|}}
{2010203004|XXGoport|29|0|65|0|{@{}@0|0|0|0|reject|0.0|@@@1168|0|}}
{2010203004|XXGoport|30|0|67|0|{@{}@0|0|0|0|error|0.0|@@@1176|0|}}
{2010203004|XXGoport|31|0|69|0|{@{}@657000|164000|11000|11000|log|0.0|@@@1176|0|}}
{2010202004|XXGiport|32|0|72|0|{@{}@576000|95000|11000|11000|in|0.0|@@@1808|0|}}
{2010501005|XXGpvertex|33|0|75|0|{Generates multiple output data records from each input data record.  Normalize can separate a data record with a vector field into several individual records, each containing one element of the vector.|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|mpname|normalize|3|1|Hl||{0|}}
{30001002|XXparameter|image__|/~ab_home/bin/transform|3|2|H$||{0|}}
{30001002|XXparameter|transform||8|1|RFOl|Required and optional DML functions|{0|}}
{30001002|XXparameter|error_group||3|2|F$|Optional group name identifying a Handle Errors component to which errors can be directed.|{0|}}
{30001002|XXparameter|error_group.condition|version 2.16:|3|15|P?|@{0|}}
{30001002|XXparameter|log_group||3|2|F$|Optional group name of a Handle Logs component to which log output can be directed.|{0|}}
{30001002|XXparameter|log_group.condition|version 2.16:|3|15|P?|@{0|}}
{30001002|XXparameter|reject_threshold|Abort on first reject|15|1|Fl|When to abort if input records are rejected|{3|Abort on first reject|Never abort|Use limit/ramp|}}
{30001002|XXparameter|reject_threshold.display_name|reject-threshold|3|9|P|@{0|}}
{30001002|XXparameter|limit|0|1|2|F$|Maximum rejected records before failure|{0|}}
{30001002|XXparameter|limit.condition|param reject-threshold Use*|3|15|P?|@{0|}}
{30001002|XXparameter|ramp|0.0|2|2|F$|Rate of rejected records|{0|}}
{30001002|XXparameter|ramp.condition|param reject-threshold Use*|3|15|P?|@{0|}}
{30001002|XXparameter|limit_keyword|value reject-threshold Never* 0 value reject-threshold Abort* 0 sameas limit default dollar_substitution 0|1|13|Kv|Maximum rejected records before failure|{0|}}
{30001002|XXparameter|ramp_keyword|value reject-threshold Never* 99.0 value reject-threshold Abort* 0.0 sameas ramp default dollar_substitution 0.0|2|13|Kv|Rate of rejected records|{0|}}
{30001002|XXparameter|ramp_keyword.condition|version :2.12.999.m999|3|15|P?|@{0|}}
{30001002|XXparameter|ramp_keyword2|value reject-threshold Never* -1 value reject-threshold Abort* 0.0 sameas ramp default dollar_substitution 0.0|2|13|Kv|Rate of rejected records|{0|}}
{30001002|XXparameter|ramp_keyword2.condition|version 2.13:|3|15|P?|@{0|}}
{30001002|XXparameter|keyword_map|limit_keyword limit ramp_keyword ramp ramp_keyword2 ramp|3|2|$||{0|}}
{30001002|XXparameter|keyword_map.display_name|keyword-map|3|9|P|@{0|}}
{30001002|XXparameter|logging|False|13|1|Fl|Log internal events|{0|}}
{30001002|XXparameter|log|log_concat|3|13|Kv|Special log parameter|{0|}}
{30001002|XXparameter|log.condition|param logging True|3|15|P?|@{0|}}
{30001002|XXparameter|log_input||15|1|Fl|Frequency of input records to log|{7| |1|10|100|1000|10000|100000|}}
{30001002|XXparameter|log_input.condition|param logging True|3|15|P?|@{0|}}
{30001002|XXparameter|log_output||15|1|Fl|Frequency of output records to log|{7| |1|10|100|1000|10000|100000|}}
{30001002|XXparameter|log_output.condition|param logging True|3|15|P?|@{0|}}
{30001002|XXparameter|log_reject||15|1|Fl|Frequency of reject records to log|{7| |1|10|100|1000|10000|100000|}}
{30001002|XXparameter|log_reject.condition|param logging True|3|15|P?|@{0|}}
{30001002|XXparameter|log_intermediate||15|1|Fl|Frequency of intermediate records to log|{7| |1|10|100|1000|10000|100000|}}
{30001002|XXparameter|log_intermediate.condition|param logging True|3|15|P?|@{0|}}
{30001002|XXparameter|num_variables|1|1|2|H$|Number of intermediate types in transform package|{0|}}
{30001002|XXparameter|variable1|optional temporary_type temp|3|2|H$|Temporary variable|{0|}}
{30001002|XXparameter|num_operations|11|1|2|H$|Number of operations in package|{0|}}
{30001002|XXparameter|operation1|optional integer(8) out::input_select(in)|3|2|H$|Select input record|{0|}}
{30001002|XXparameter|operation2|optional temp ? temp::initialize(in) : NULL|3|2|H$|Initialize temporary|{0|}}
{30001002|XXparameter|operation3|optional integer(8) out::length(in)|3|2|H$|Number of records to output.  Either this function or finished() must be defined.|{0|}}
{30001002|XXparameter|operation4|temp ? temp::normalize(temp, in, integer(8) index) : out::normalize(in, integer(8) index)|3|2|H$|Do computation|{0|}}
{30001002|XXparameter|operation5|optional temp ? integer(8) finished_out::finished(temp, in, integer(8) index) : integer(8) finished_out::finished(in, integer(8) index)|3|2|H$|Are we done with this input record?  Either this function or length() must be defined.|{0|}}
{30001002|XXparameter|operation6|optional temp ? out::finalize(temp, in) : NULL|3|2|H$|Create output record|{0|}}
{30001002|XXparameter|operation7|optional integer(8) out::output_select(out)|3|2|H$|Select output record|{0|}}
{30001002|XXparameter|operation1_analysis|integer(8) input_select_out::input_select(in)|3|2|H$||{0|}}
{30001002|XXparameter|operation3_analysis|integer(8) length_out::length(in)|3|2|H$|Number of records to output|{0|}}
{30001002|XXparameter|operation7_analysis|integer(8) output_select_out::output_select(out)|3|2|H$|Select output record|{0|}}
{30001002|XXparameter|condition||3|2|F$||{0|}}
{30001002|XXparameter|conditionInputPort|in|3|2|F$||{0|}}
{30001002|XXparameter|conditionOutputPort|out|3|2|F$||{0|}}
{30001002|XXparameter|condition_interpretation|Replace with flow|15|1|Fl||{2|Replace with flow|Remove completely|}}
{30001002|XXparameter|condition_interpretation.display_name|condition-interpretation|3|9|P|@{0|}}
{30001002|XXparameter|port_analysis|reject=in|3|2|H$||{0|}}
{30001002|XXparameter|continuous_analysis||3|2|H$||{0|}}
{30001002|XXparameter|filter_aggregate_analysis|out=filter_xfr(transform.input_select); out=filter_xfr(transform.output_select)|3|2|H$||{0|}}
{30001002|XXparameter|num_types|2|1|2|H$|Number of private data types in package|{0|}}
{30001002|XXparameter|type1|error_info_t error_info "record utf8 string(big endian integer(4)) component; big endian integer(4) port_index; utf8 string(big endian integer(4)) parameter; utf8 string(big endian integer(4)) message; record utf8 string(big endian integer(4)) code; big endian integer(4) parent_index; record utf8 string(big endian integer(4)) name; utf8 string(big endian integer(4)) value; end [big endian integer(4)] attributes; end [big endian integer(4)] details; end"|3|2|H$|Type of error_info|{0|}}
{30001002|XXparameter|type2|log_event_t log_event "record string('') event_type; string('') event_text; end"|3|2|H$|Type of error_info|{0|}}
{30001002|XXparameter|operation8|optional out::output_for_error(error_info, in)|3|2|H$|Function to create output in case of error.|{0|}}
{30001002|XXparameter|operation9|optional error::make_error(error_info, in)|3|2|H$|Function to create error record in case of error.|{0|}}
{30001002|XXparameter|operation10|optional log_event::log_error(error_info, in)|3|2|H$|Function to create log record in case of error.|{0|}}
{30001002|XXparameter|operation11|optional log_event::final_log_output()|3|2|H$|Function to create final log record.|{0|}}
{30001002|XXparameter|num_callbacks|4|1|2|H$|Number of callbacks in package|{0|}}
{30001002|XXparameter|_use_rich_error_format|True|13|1|Hl|False means use legacy string newline format|{0|}}
{30001002|XXparameter|_use_rich_log_format|True|13|1|Hl|False means use legacy native charset format|{0|}}
{30001002|XXparameter|Layout|@9|2|RFs$||{0|}}
{30001002|XXparameter|in_metadata||7|1|RFsl||{0|}}
{30001002|XXparameter|out_metadata||7|1|RFl||{0|}}
{30001002|XXparameter|reject_metadata||7|1|Fsl||{0|}}
{30001002|XXparameter|error_metadata||7|1|Fl||{0|}}
{30001002|XXparameter|log_metadata||7|1|RFl||{0|}}
{30001002|XXparameter|doc_transform||8|2|FHs$|Document your transformation for dependency analysis|{0|}}
{30001002|XXparameter|doc_operation1|out,reject,error,log::document(in)|3|2|RH$||{0|}}
{30001002|XXparameter|mpcmodtime|1299550340|1|1|Hl|The last modification time of this component's template|{0|}}
{30001002|XXparameter|_propagation|metadata type: reject = in
metadata type: error = remote("$AB_HOME/include/error-info.dml")
metadata type: log = remote("$AB_HOME/include/log-info.dml")|3|1|HKl|@{0|}}
{30001002|XXparameter|_propagate_through||3|1|FHKl|@{0|}}
}}@0|@0|0|0|0|0|0|0|@@@1|10|-1|@6|@1|1|{1|0|}}}
{2010203004|XXGoport|34|0|77|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|out|0.0|@@@2448|0|}}
{2010203004|XXGoport|35|0|80|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|F=||{0|}}
}}@0|0|0|0|reject|0.0|@@@1168|0|}}
{2010203004|XXGoport|36|0|83|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|F=||{0|}}
}}@0|0|0|0|error|0.0|@@@1176|0|}}
{2010203004|XXGoport|37|0|86|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|log|0.0|@@@1176|0|}}
{2010202004|XXGiport|38|0|89|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|in|0.0|@@@1808|0|}}
{2010501005|XXGpvertex|39|0|93|0|{|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|out_metadata|$REDEFINE_FORMAT_DML_FILE|3|2|f$|@{0|}}
{30001002|XXparameter|!prototype_path|$AB_COMPONENTS/Miscellaneous/Redefine_Format.mpc|3|2|Pf$|@{0|}}
{30001002|XXparameter|Layout||3|8|s=|@{0|}}
{30001002|XXparameter|in_metadata||3|8|s=|@{0|}}
}}@0|@352392|11124|372000|31000|157000|125000|478|Redefine Format - Create Vectored Sub-Records|Ab Initio Software|Built-in|1|10|0||6||32769|1|{1|0|}}}
{2010203004|XXGoport|40|0|95|0|{@{}@528000|88000|11000|11000|out|0.0|@@@2448|0|}}
{2010202004|XXGiport|41|0|98|0|{@{}@362000|88000|11000|11000|in|0.0|@@@1936|0|}}
{2010501005|XXGpvertex|42|0|101|0|{Copies data records from its input to its output without changing the values.  Use Redefine Format to change a record format or rename fields.|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|mpname|copy|3|1|Hl||{0|}}
{30001002|XXparameter|image__|unitool|3|2|H$||{0|}}
{30001002|XXparameter|condition||3|2|F$||{0|}}
{30001002|XXparameter|conditionInputPort|in|3|2|F$||{0|}}
{30001002|XXparameter|conditionOutputPort|out|3|2|F$||{0|}}
{30001002|XXparameter|condition_interpretation|Replace with flow|15|1|Fl||{2|Replace with flow|Remove completely|}}
{30001002|XXparameter|condition_interpretation.display_name|condition-interpretation|3|9|P|@{0|}}
{30001002|XXparameter|port_analysis|out=byte_copy(in)|3|2|H$||{0|}}
{30001002|XXparameter|continuous_analysis||3|2|H$||{0|}}
{30001002|XXparameter|Layout|@9|2|RFs$||{0|}}
{30001002|XXparameter|in_metadata||7|1|RFsl||{0|}}
{30001002|XXparameter|out_metadata||7|1|RFl||{0|}}
{30001002|XXparameter|doc_transform||8|2|FHs$|Document your transformation for dependency analysis|{0|}}
{30001002|XXparameter|doc_operation1|out::document(in)|3|2|RH$||{0|}}
{30001002|XXparameter|mpcmodtime|1299550339|1|1|Hl|The last modification time of this component's template|{0|}}
{30001002|XXparameter|_propagate_through||3|1|FHKl|@{0|}}
}}@0|@0|0|0|0|0|0|0|@@@1|10|-1|@6|@1|1|{1|0|}}}
{2010203004|XXGoport|43|0|103|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|out|0.0|@@@2448|0|}}
{2010202004|XXGiport|44|0|106|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|in|0.0|@@@1936|0|}}
{2010501005|XXGpvertex|45|0|110|0|{|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|transform0|$REFORMAT_XFR_FILE|3|2|f$|@{0|}}
{30001002|XXparameter|out0_metadata|$REFORMAT_DML_FILE|3|2|f$|@{0|}}
{30001002|XXparameter|error0_metadata|string('\\n')|3|1|l|@{0|}}
{30001002|XXparameter|log_metadata|record string("\|") node, timestamp, component, subcomponent, event_type; string("\|\\n") event_text; end|3|1|l|@{0|}}
{30001002|XXparameter|!prototype_path|$AB_COMPONENTS/Transform/Reformat.mpc|3|2|Pf$|@{0|}}
{30001002|XXparameter|Layout||3|8|s=|@{0|}}
{30001002|XXparameter|in_metadata||3|8|s=|@{0|}}
{30001002|XXparameter|reject0_metadata||3|8|s=|@{0|}}
}}@0|@125070|12106|145000|32000|173000|125000|501|Reformat - Determine Number of Normalized Values|Ab Initio Software|Built-in|1|10|0||6||32769|1|{1|0|}}}
{2010203004|XXGoport|46|0|112|0|{@{}@317000|89000|11000|11000|out0|0.0|count|out|0|2448|0|}}
{2010203004|XXGoport|47|0|115|0|{@{}@0|0|0|0|reject0|0.0|count|reject|0|1168|0|}}
{2010203004|XXGoport|48|0|117|0|{@{}@0|0|0|0|error0|0.0|count|error|0|1176|0|}}
{2010203004|XXGoport|49|0|119|0|{@{}@0|0|0|0|log|0.0|@@@1176|0|}}
{2010202004|XXGiport|50|0|121|0|{@{}@135000|89000|11000|11000|in|0.0|@@@1808|0|}}
{2010501005|XXGpvertex|51|0|124|0|{Changes the record format of your data by dropping fields or by using DML expressions to add fields, combine fields, or modify the data.|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|mpname|reformat-transform|3|1|Hl||{0|}}
{30001002|XXparameter|image__|unitool|3|2|H$||{0|}}
{30001002|XXparameter|count|1|1|1|Fl|Number of reformat transforms|{0|}}
{30001002|XXparameter|count.is_index|true|3|9|P|@{0|}}
{30001002|XXparameter|transform0||8|1|FOl|Reformat transform|{0|}}
{30001002|XXparameter|transform0.index|count|3|9|P|@{0|}}
{30001002|XXparameter|transform0.index_value|0|3|9|P|@{0|}}
{30001002|XXparameter|transform0.index_name|transform|3|9|P|@{0|}}
{30001002|XXparameter|select||20|2|FK$|Filter expression before reformatting|{0|}}
{30001002|XXparameter|error_group||3|2|F$|Optional group name identifying a Handle Errors component to which errors can be directed.|{0|}}
{30001002|XXparameter|error_group.condition|version 2.16:|3|15|P?|@{0|}}
{30001002|XXparameter|log_group||3|2|F$|Optional group name of a Handle Logs component to which log output can be directed.|{0|}}
{30001002|XXparameter|log_group.condition|version 2.16:|3|15|P?|@{0|}}
{30001002|XXparameter|reject_threshold|Abort on first reject|15|1|Fl|When to abort if input records are rejected|{3|Abort on first reject|Never abort|Use limit/ramp|}}
{30001002|XXparameter|reject_threshold.display_name|reject-threshold|3|9|P|@{0|}}
{30001002|XXparameter|limit|0|1|2|F$|Maximum rejected records before failure|{0|}}
{30001002|XXparameter|limit.condition|param reject-threshold Use*|3|15|P?|@{0|}}
{30001002|XXparameter|ramp|0.0|2|2|F$|Rate of rejected records|{0|}}
{30001002|XXparameter|ramp.condition|param reject-threshold Use*|3|15|P?|@{0|}}
{30001002|XXparameter|limit_keyword|value reject-threshold Never* 0 value reject-threshold Abort* 0 sameas limit default dollar_substitution 0|1|13|Kv|Maximum rejected records before failure|{0|}}
{30001002|XXparameter|ramp_keyword|value reject-threshold Never* 99.0 value reject-threshold Abort* 0.0 sameas ramp default dollar_substitution 0.0|2|13|Kv|Rate of rejected records|{0|}}
{30001002|XXparameter|ramp_keyword.condition|version :2.12.999.m999|3|15|P?|@{0|}}
{30001002|XXparameter|ramp_keyword2|value reject-threshold Never* -1 value reject-threshold Abort* 0.0 sameas ramp default dollar_substitution 0.0|2|13|Kv|Rate of rejected records|{0|}}
{30001002|XXparameter|ramp_keyword2.condition|version 2.13:|3|15|P?|@{0|}}
{30001002|XXparameter|keyword_map|limit_keyword limit ramp_keyword ramp ramp_keyword2 ramp|3|2|$||{0|}}
{30001002|XXparameter|keyword_map.display_name|keyword-map|3|9|P|@{0|}}
{30001002|XXparameter|output_index||8|1|FKl|Output index computation|{0|}}
{30001002|XXparameter|output_index.display_name|output-index|3|9|P|@{0|}}
{30001002|XXparameter|output_index.keyword|output-index|3|9|P|@{0|}}
{30001002|XXparameter|output_index_port|discriminator|3|2|FHK$|Output index port for debugger|{0|}}
{30001002|XXparameter|output_index_port.display_name|output-index_port|3|9|P|@{0|}}
{30001002|XXparameter|output_index_port.keyword|output-index_port|3|9|P|@{0|}}
{30001002|XXparameter|output_index_signature|int out::output_index(in)|3|2|H$|Function returning index of output port|{0|}}
{30001002|XXparameter|output_index_signature.display_name|output-index_signature|3|9|P|@{0|}}
{30001002|XXparameter|output_indexes||8|1|FKl|Output index vector computation|{0|}}
{30001002|XXparameter|output_indexes.display_name|output-indexes|3|9|P|@{0|}}
{30001002|XXparameter|output_indexes.keyword|output-indexes|3|9|P|@{0|}}
{30001002|XXparameter|output_indexes_port|discriminator|3|2|FHK$|Output index port for debugger|{0|}}
{30001002|XXparameter|output_indexes_port.display_name|output-indexes_port|3|9|P|@{0|}}
{30001002|XXparameter|output_indexes_port.keyword|output-indexes_port|3|9|P|@{0|}}
{30001002|XXparameter|output_indexes_signature|int[int] out::output_indexes(in)|3|2|H$|Function returning vector of indexes of output ports|{0|}}
{30001002|XXparameter|output_indexes_signature.display_name|output-indexes_signature|3|9|P|@{0|}}
{30001002|XXparameter|logging|False|13|1|Fl|Log internal events|{0|}}
{30001002|XXparameter|log|log_concat|3|13|Kv|Special log parameter|{0|}}
{30001002|XXparameter|log.condition|param logging True|3|15|P?|@{0|}}
{30001002|XXparameter|log_input||15|1|Fl|Frequency of input records to log|{7| |1|10|100|1000|10000|100000|}}
{30001002|XXparameter|log_input.condition|param logging True|3|15|P?|@{0|}}
{30001002|XXparameter|log_output||15|1|Fl|Frequency of output records to log|{7| |1|10|100|1000|10000|100000|}}
{30001002|XXparameter|log_output.condition|param logging True|3|15|P?|@{0|}}
{30001002|XXparameter|log_reject||15|1|Fl|Frequency of reject records to log|{7| |1|10|100|1000|10000|100000|}}
{30001002|XXparameter|log_reject.condition|param logging True|3|15|P?|@{0|}}
{30001002|XXparameter|num_operations|5|1|2|H$|Number of operations in package|{0|}}
{30001002|XXparameter|operation1|out::reformat(in)|3|2|H$|Reformat operation|{0|}}
{30001002|XXparameter|condition||3|2|F$||{0|}}
{30001002|XXparameter|conditionInputPort|in|3|2|F$||{0|}}
{30001002|XXparameter|conditionOutputPort|out*|3|2|F$||{0|}}
{30001002|XXparameter|condition_interpretation|Replace with flow|15|1|Fl||{2|Replace with flow|Remove completely|}}
{30001002|XXparameter|condition_interpretation.display_name|condition-interpretation|3|9|P|@{0|}}
{30001002|XXparameter|port_analysis|reject*=in;out*=in|3|2|H$||{0|}}
{30001002|XXparameter|continuous_analysis||3|2|H$||{0|}}
{30001002|XXparameter|filter_aggregate_analysis|out=filter(select)|3|2|H$||{0|}}
{30001002|XXparameter|num_types|2|1|2|H$|Number of private data types in package|{0|}}
{30001002|XXparameter|type1|error_info_t error_info "record utf8 string(big endian integer(4)) component; big endian integer(4) port_index; utf8 string(big endian integer(4)) parameter; utf8 string(big endian integer(4)) message; record utf8 string(big endian integer(4)) code; big endian integer(4) parent_index; record utf8 string(big endian integer(4)) name; utf8 string(big endian integer(4)) value; end [big endian integer(4)] attributes; end [big endian integer(4)] details; end"|3|2|H$|Type of error_info|{0|}}
{30001002|XXparameter|type2|log_event_t log_event "record string('') event_type; string('') event_text; end"|3|2|H$|Type of error_info|{0|}}
{30001002|XXparameter|operation2|optional out::output_for_error(error_info, in)|3|2|H$|Function to create output in case of error.|{0|}}
{30001002|XXparameter|operation3|optional error::make_error(error_info, in)|3|2|H$|Function to create error record in case of error.|{0|}}
{30001002|XXparameter|operation4|optional log_event::log_error(error_info, in)|3|2|H$|Function to create log record in case of error.|{0|}}
{30001002|XXparameter|operation5|optional log_event::final_log_output()|3|2|H$|Function to create final log record.|{0|}}
{30001002|XXparameter|num_callbacks|4|1|2|H$|Number of callbacks in package|{0|}}
{30001002|XXparameter|_use_rich_error_format|True|13|1|Hl|False means use legacy string newline format|{0|}}
{30001002|XXparameter|_use_rich_log_format|True|13|1|Hl|False means use legacy native charset format|{0|}}
{30001002|XXparameter|Layout|@9|2|RFs$||{0|}}
{30001002|XXparameter|main_mp_port|count out|3|1|l||{0|}}
{30001002|XXparameter|in_metadata||7|1|RFsl||{0|}}
{30001002|XXparameter|out0_metadata||7|1|RFl||{0|}}
{30001002|XXparameter|reject0_metadata||7|1|Fsl||{0|}}
{30001002|XXparameter|error0_metadata||7|1|Fl||{0|}}
{30001002|XXparameter|log_metadata||7|1|Fl||{0|}}
{30001002|XXparameter|doc_transform||8|2|FHs$|Document your transformation for dependency analysis|{0|}}
{30001002|XXparameter|doc_operation1|out[*],reject[*],error[*],log::document(in)|3|2|RH$||{0|}}
{30001002|XXparameter|mpcmodtime|1299550340|1|1|Hl|The last modification time of this component's template|{0|}}
{30001002|XXparameter|_propagation|metadata type: error = remote("$AB_HOME/include/error-info.dml")
metadata type: reject = in
metadata type: log = remote("$AB_HOME/include/log-info.dml")|3|1|HKl|@{0|}}
{30001002|XXparameter|_propagate_through||3|1|FHKl|@{0|}}
}}@0|@0|0|0|0|0|0|0|@@@1|10|-1|@6|@1|1|{1|0|}}}
{2010203004|XXGoport|52|0|126|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
{30001002|XXparameter|transform0||8|8|FO=|Reformat transform|{0|}}
{30001002|XXparameter|transform0.index|count|3|9|P|@{0|}}
{30001002|XXparameter|transform0.index_value|0|3|9|P|@{0|}}
{30001002|XXparameter|transform0.index_name|transform|3|9|P|@{0|}}
}}@0|0|0|0|out0|0.0|count|out|0|2448|0|}}
{2010203004|XXGoport|53|0|130|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|F=||{0|}}
}}@0|0|0|0|reject0|0.0|count|reject|0|1168|0|}}
{2010203004|XXGoport|54|0|133|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|F=||{0|}}
}}@0|0|0|0|error0|0.0|count|error|0|1176|0|}}
{2010203004|XXGoport|55|0|136|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|F=||{0|}}
}}@0|0|0|0|log|0.0|@@@1176|0|}}
{2010202004|XXGiport|56|0|139|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|in|0.0|@@@1808|0|}}
{2010503005|XXGfvertex|57|0|143|0|{|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|Layout|$[[record kind 67 subkind 87 parts [vector _interp_("$INPUT_FILE", "dollar_substitution")]]]|3|9||@{0|}}
{30001002|XXparameter|read_metadata|$INPUT_DML|3|2|f$|@{0|}}
{30001002|XXparameter|!prototype_path|$AB_COMPONENTS/Datasets/Input_File.mdc|3|2|Pf$|@{0|}}
}}@0|@26349|54954|51000|75000|40000|40000|457|Input File|Kevin Oaks||1|10|0||6||32769|-1|-1|}}
{2010203004|XXGoport|58|0|145|0|{@{}@90000|89000|11000|11000|read|0.0|@@@2160|0|}}
{2010503005|XXGfvertex|59|0|148|0|{Represents one file, many files, or a multifile as an input to your graph.|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|protection|0666|12|2|RF$||{0|}}
{30001002|XXparameter|mode|0x0001|1|2|FH$|modes of access|{0|}}
{30001002|XXparameter|condition||3|2|F$||{0|}}
{30001002|XXparameter|conditionInputPort||3|2|F$||{0|}}
{30001002|XXparameter|conditionOutputPort||3|2|F$||{0|}}
{30001002|XXparameter|condition_interpretation|Remove completely|15|1|Fl||{2|Replace with flow|Remove completely|}}
{30001002|XXparameter|condition_interpretation.display_name|condition-interpretation|3|9|P|@{0|}}
{30001002|XXparameter|key||19|2|RF$|Key specifier For Lookup File|{0|}}
{30001002|XXparameter|key.condition|param direct_addressed False mode lookup|3|15|P?|@{0|}}
{30001002|XXparameter|m_catalog_add_option_names|block_compressed keep_on_disk direct_addressed|3|2|H$||{0|}}
{30001002|XXparameter|eme_index_location||3|2|F$|Place in the EME to create a dataset corresponding to this index.|{0|}}
{30001002|XXparameter|eme_index_location.condition|mode lookup version 2.15.2.r23:|3|15|P?|@{0|}}
{30001002|XXparameter|index_url|@28|2|F$|The URL of the index for this lookup file (optional)|{0|}}
{30001002|XXparameter|index_url.condition|param direct_addressed False mode lookup version 2.15.2.r23:|3|15|P?|@{0|}}
{30001002|XXparameter|keep_on_disk|False|13|1|Fl|Keep on Disk|{0|}}
{30001002|XXparameter|keep_on_disk.condition|mode lookup version 2.15.2.r23:|3|15|P?|@{0|}}
{30001002|XXparameter|block_compressed|False|13|1|Fl|Block Compressed|{0|}}
{30001002|XXparameter|block_compressed.condition|mode lookup version 2.15.2.r23:|3|15|P?|@{0|}}
{30001002|XXparameter|direct_addressed|False|13|1|Fl|Set to true if the block compressed lookup is direct addressed|{0|}}
{30001002|XXparameter|direct_addressed.condition|param block_compressed True mode lookup version 2.15.5.r34:|3|15|P?|@{0|}}
{30001002|XXparameter|Layout|@28|2|RF$||{0|}}
{30001002|XXparameter|read_metadata||7|1|RFl||{0|}}
{30001002|XXparameter|mpcmodtime|1299550332|1|1|Hl|The last modification time of this component's template|{0|}}
{30001002|XXparameter|eme_dataset_location||3|2|F$|Place in the EME to create a dataset corresponding to this file.|{0|}}
}}@0|@0|0|0|0|0|0|0|@@@1|10|-1|@6|@1|-1|-1|}}
{2010203004|XXGoport|60|0|150|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|read|0.0|@@@2160|0|}}
{2010501005|XXGpvertex|61|0|155|0|{|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|select_expr|string_index(line, "bytes) written") > 0|3|2|$|@{0|}}
{30001002|XXparameter|error_metadata|string('
')|3|1|l|@{0|}}
{30001002|XXparameter|log_metadata|record string("\|") node, timestamp, component, subcomponent, event_type; string("\|\\n") event_text; end|3|1|l|@{0|}}
{30001002|XXparameter|!prototype_path|$AB_COMPONENTS/Transform/Filter_by_Expression.mpc|3|2|Pf$|@{0|}}
{30001002|XXparameter|in_metadata||3|8|s=|@{0|}}
{30001002|XXparameter|out_metadata||3|8|s=|@{0|}}
{30001002|XXparameter|deselect_metadata||3|8|s=|@{0|}}
{30001002|XXparameter|reject_metadata||3|8|s=|@{0|}}
{30001002|XXparameter|Layout||3|8|s=|@{0|}}
}}@1|@150122|667751|170000|688000|135000|106000|504|Filter record count rows|Ab Initio Software|Built-in|1|10|0||6||32769|1|{1|0|}}}
{2010203004|XXGoport|62|0|157|0|{@{}@304000|735000|11000|11000|out|0.0|@@@2448|0|}}
{2010203004|XXGoport|63|0|160|0|{@{}@0|0|0|0|deselect|0.0|@@@2192|0|}}
{2010203004|XXGoport|64|0|162|0|{@{}@0|0|0|0|reject|0.0|@@@1168|0|}}
{2010203004|XXGoport|65|0|164|0|{@{}@0|0|0|0|error|0.0|@@@1176|0|}}
{2010203004|XXGoport|66|0|166|0|{@{}@0|0|0|0|log|0.0|@@@1176|0|}}
{2010202004|XXGiport|67|0|168|0|{@{}@160000|735000|11000|11000|in|0.0|@@@1808|0|}}
{2010501005|XXGpvertex|68|0|171|0|{Filters data records according to the specified DML expression.|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|mpname|select-transform|3|1|Hl||{0|}}
{30001002|XXparameter|image__|unitool|3|2|H$||{0|}}
{30001002|XXparameter|select_expr||20|2|RFO$|Filter expression|{0|}}
{30001002|XXparameter|select_expr.condition|param use_package False|3|15|P?|@{0|}}
{30001002|XXparameter|no_select_expr|-use_package|3|2|RO$|Replaces select_expr as the positional argument when use_package is true|{0|}}
{30001002|XXparameter|no_select_expr.condition|param use_package True|3|15|P?|@{0|}}
{30001002|XXparameter|use_package|False|13|1|Fl|If true, use the select() function defined in the package|{0|}}
{30001002|XXparameter|use_package.condition|version 2.16:|3|15|P?|@{0|}}
{30001002|XXparameter|package||8|1|FKl|Package containing transforms and data types|{0|}}
{30001002|XXparameter|package.condition|version 2.16:|3|15|P?|@{0|}}
{30001002|XXparameter|error_group||3|2|F$|Optional group name identifying a Handle Errors component to which errors can be directed.|{0|}}
{30001002|XXparameter|error_group.condition|version 2.16:|3|15|P?|@{0|}}
{30001002|XXparameter|log_group||3|2|F$|Optional group name of a Handle Logs component to which log output can be directed.|{0|}}
{30001002|XXparameter|log_group.condition|version 2.16:|3|15|P?|@{0|}}
{30001002|XXparameter|num_operations|5|1|2|H$|Number of operations in package|{0|}}
{30001002|XXparameter|operation1|optional integer(8) select_out::select(in)|3|2|H$|Use this instead of the select_expr parameter when use_package is true.|{0|}}
{30001002|XXparameter|operation1.condition|version 2.16: param use_package True|3|15|P?|@{0|}}
{30001002|XXparameter|operation1_analysis|skip_missing integer(8) select_out::select(in)|3|2|H$||{0|}}
{30001002|XXparameter|operation1_analysis.condition|version 2.16: param use_package True|3|15|P?|@{0|}}
{30001002|XXparameter|reject_threshold|Abort on first reject|15|1|Fl|When to abort if input records are rejected|{3|Abort on first reject|Never abort|Use limit/ramp|}}
{30001002|XXparameter|reject_threshold.display_name|reject-threshold|3|9|P|@{0|}}
{30001002|XXparameter|limit|0|1|2|F$|Maximum rejected records before failure|{0|}}
{30001002|XXparameter|limit.condition|param reject-threshold Use*|3|15|P?|@{0|}}
{30001002|XXparameter|ramp|0.0|2|2|F$|Rate of rejected records|{0|}}
{30001002|XXparameter|ramp.condition|param reject-threshold Use*|3|15|P?|@{0|}}
{30001002|XXparameter|limit_keyword|value reject-threshold Never* 0 value reject-threshold Abort* 0 sameas limit default dollar_substitution 0|1|13|Kv|Maximum rejected records before failure|{0|}}
{30001002|XXparameter|ramp_keyword|value reject-threshold Never* 99.0 value reject-threshold Abort* 0.0 sameas ramp default dollar_substitution 0.0|2|13|Kv|Rate of rejected records|{0|}}
{30001002|XXparameter|ramp_keyword.condition|version :2.12.999.m999|3|15|P?|@{0|}}
{30001002|XXparameter|ramp_keyword2|value reject-threshold Never* -1 value reject-threshold Abort* 0.0 sameas ramp default dollar_substitution 0.0|2|13|Kv|Rate of rejected records|{0|}}
{30001002|XXparameter|ramp_keyword2.condition|version 2.13:|3|15|P?|@{0|}}
{30001002|XXparameter|keyword_map|limit_keyword limit ramp_keyword ramp ramp_keyword2 ramp|3|2|$||{0|}}
{30001002|XXparameter|keyword_map.display_name|keyword-map|3|9|P|@{0|}}
{30001002|XXparameter|logging|False|13|1|Fl|Log internal events|{0|}}
{30001002|XXparameter|log|log_concat|3|13|Kv|Special log parameter|{0|}}
{30001002|XXparameter|log.condition|param logging True|3|15|P?|@{0|}}
{30001002|XXparameter|log_input||15|1|Fl|Frequency of input records to log|{7| |1|10|100|1000|10000|100000|}}
{30001002|XXparameter|log_input.condition|param logging True|3|15|P?|@{0|}}
{30001002|XXparameter|log_output||15|1|Fl|Frequency of output records to log|{7| |1|10|100|1000|10000|100000|}}
{30001002|XXparameter|log_output.condition|param logging True|3|15|P?|@{0|}}
{30001002|XXparameter|log_reject||15|1|Fl|Frequency of reject records to log|{7| |1|10|100|1000|10000|100000|}}
{30001002|XXparameter|log_reject.condition|param logging True|3|15|P?|@{0|}}
{30001002|XXparameter|condition||3|2|F$||{0|}}
{30001002|XXparameter|conditionInputPort|in|3|2|F$||{0|}}
{30001002|XXparameter|conditionOutputPort|out|3|2|F$||{0|}}
{30001002|XXparameter|condition_interpretation|Replace with flow|15|1|Fl||{2|Replace with flow|Remove completely|}}
{30001002|XXparameter|condition_interpretation.display_name|condition-interpretation|3|9|P|@{0|}}
{30001002|XXparameter|port_analysis|reject=in; out=in; deselect=in|3|2|H$||{0|}}
{30001002|XXparameter|continuous_analysis||3|2|H$||{0|}}
{30001002|XXparameter|filter_aggregate_analysis|out=filter(select_expr); deselect=filter(select_expr); out=filter_xfr(package.select_out); deselect=filter_xfr(package.select_out);|3|2|H$||{0|}}
{30001002|XXparameter|num_types|2|1|2|H$|Number of private data types in package|{0|}}
{30001002|XXparameter|type1|error_info_t error_info "record utf8 string(big endian integer(4)) component; big endian integer(4) port_index; utf8 string(big endian integer(4)) parameter; utf8 string(big endian integer(4)) message; record utf8 string(big endian integer(4)) code; big endian integer(4) parent_index; record utf8 string(big endian integer(4)) name; utf8 string(big endian integer(4)) value; end [big endian integer(4)] attributes; end [big endian integer(4)] details; end"|3|2|H$|Type of error_info|{0|}}
{30001002|XXparameter|type2|log_event_t log_event "record string('') event_type; string('') event_text; end"|3|2|H$|Type of error_info|{0|}}
{30001002|XXparameter|operation2|optional out::output_for_error(error_info, in)|3|2|H$|Function to create output in case of error.|{0|}}
{30001002|XXparameter|operation3|optional error::make_error(error_info, in)|3|2|H$|Function to create error record in case of error.|{0|}}
{30001002|XXparameter|operation4|optional log_event::log_error(error_info, in)|3|2|H$|Function to create log record in case of error.|{0|}}
{30001002|XXparameter|operation5|optional log_event::final_log_output()|3|2|H$|Function to create final log record.|{0|}}
{30001002|XXparameter|num_callbacks|4|1|2|H$|Number of callbacks in package|{0|}}
{30001002|XXparameter|_use_rich_error_format|True|13|1|Hl|False means use legacy string newline format|{0|}}
{30001002|XXparameter|_use_rich_log_format|True|13|1|Hl|False means use legacy native charset format|{0|}}
{30001002|XXparameter|in_metadata||7|1|RFsl||{0|}}
{30001002|XXparameter|out_metadata||7|1|RFsl||{0|}}
{30001002|XXparameter|deselect_metadata||7|1|Fsl||{0|}}
{30001002|XXparameter|reject_metadata||7|1|Fsl||{0|}}
{30001002|XXparameter|error_metadata||7|1|Fl||{0|}}
{30001002|XXparameter|log_metadata||7|1|Fl||{0|}}
{30001002|XXparameter|doc_transform||8|2|FHs$|Document your transformation for dependency analysis|{0|}}
{30001002|XXparameter|doc_operation1|out,deselect,reject,error,log::document(in)|3|2|RH$||{0|}}
{30001002|XXparameter|mpcmodtime|1299550340|1|1|Hl|The last modification time of this component's template|{0|}}
{30001002|XXparameter|Layout|@9|2|RFs$||{0|}}
{30001002|XXparameter|_propagation|metadata type: deselect = out
metadata type: out = deselect
metadata type: reject = in
metadata type: error = remote("$AB_HOME/include/error-info.dml")
metadata type: log = remote("$AB_HOME/include/log-info.dml")|3|1|HKl|@{0|}}
{30001002|XXparameter|_propagate_through|metadata type: out = in
metadata type: in = out
metadata type: deselect = in
metadata type: in = deselect|3|1|FHKl|@{0|}}
}}@0|@0|0|0|0|0|0|0|@@@1|10|-1|@6|@1|1|{1|0|}}}
{2010203004|XXGoport|69|0|173|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|out|0.0|@@@2448|0|}}
{2010203004|XXGoport|70|0|176|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|F=||{0|}}
}}@0|0|0|0|deselect|0.0|@@@2192|0|}}
{2010203004|XXGoport|71|0|179|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|F=||{0|}}
}}@0|0|0|0|reject|0.0|@@@1168|0|}}
{2010203004|XXGoport|72|0|182|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|F=||{0|}}
}}@0|0|0|0|error|0.0|@@@1176|0|}}
{2010203004|XXGoport|73|0|185|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|F=||{0|}}
}}@0|0|0|0|log|0.0|@@@1176|0|}}
{2010202004|XXGiport|74|0|188|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|in|0.0|@@@1808|0|}}
{2010503005|XXGfvertex|75|0|191|0|{|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|Layout|$[[record kind 85 subkind 0 parts [vector _interp_("file:$RECORD_COUNT_FILE", "dollar_substitution")]]]|3|9||@{0|}}
{30001002|XXparameter|!prototype_path|$AB_COMPONENTS/Datasets/Output_File.mdc|3|2|Pf$|@{0|}}
{30001002|XXparameter|write_metadata||3|8|s=|@{0|}}
}}@1|@725854|699563|751000|720000|40000|40000|510|Record Count File|eBay||1|10|0||6||32769|-1|-1|}}
{2010202004|XXGiport|76|0|193|0|{@{}@741000|734000|11000|11000|write|0.0|@@@1776|0|}}
{2010503005|XXGfvertex|77|0|196|0|{Represents one file, many files, or a multifile as an output from your graph.|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|protection|0666|12|2|RF$||{0|}}
{30001002|XXparameter|mode|0x0062|1|2|FH$|modes of access|{0|}}
{30001002|XXparameter|condition||3|2|F$||{0|}}
{30001002|XXparameter|conditionInputPort||3|2|F$||{0|}}
{30001002|XXparameter|conditionOutputPort||3|2|F$||{0|}}
{30001002|XXparameter|condition_interpretation|Remove completely|15|1|Fl||{2|Replace with flow|Remove completely|}}
{30001002|XXparameter|condition_interpretation.display_name|condition-interpretation|3|9|P|@{0|}}
{30001002|XXparameter|key||19|2|RF$|Key specifier For Lookup File|{0|}}
{30001002|XXparameter|key.condition|param direct_addressed False mode lookup|3|15|P?|@{0|}}
{30001002|XXparameter|m_catalog_add_option_names|block_compressed keep_on_disk direct_addressed|3|2|H$||{0|}}
{30001002|XXparameter|eme_index_location||3|2|F$|Place in the EME to create a dataset corresponding to this index.|{0|}}
{30001002|XXparameter|eme_index_location.condition|mode lookup version 2.15.2.r23:|3|15|P?|@{0|}}
{30001002|XXparameter|index_url|@28|2|F$|The URL of the index for this lookup file (optional)|{0|}}
{30001002|XXparameter|index_url.condition|param direct_addressed False mode lookup version 2.15.2.r23:|3|15|P?|@{0|}}
{30001002|XXparameter|keep_on_disk|False|13|1|Fl|Keep on Disk|{0|}}
{30001002|XXparameter|keep_on_disk.condition|mode lookup version 2.15.2.r23:|3|15|P?|@{0|}}
{30001002|XXparameter|block_compressed|False|13|1|Fl|Block Compressed|{0|}}
{30001002|XXparameter|block_compressed.condition|mode lookup version 2.15.2.r23:|3|15|P?|@{0|}}
{30001002|XXparameter|direct_addressed|False|13|1|Fl|Set to true if the block compressed lookup is direct addressed|{0|}}
{30001002|XXparameter|direct_addressed.condition|param block_compressed True mode lookup version 2.15.5.r34:|3|15|P?|@{0|}}
{30001002|XXparameter|Layout|@28|2|RF$||{0|}}
{30001002|XXparameter|write_metadata||7|1|RFsl||{0|}}
{30001002|XXparameter|mpcmodtime|1299550332|1|1|Hl|The last modification time of this component's template|{0|}}
{30001002|XXparameter|eme_dataset_location||3|2|F$|Place in the EME to create a dataset corresponding to this file.|{0|}}
}}@0|@0|0|0|0|0|0|0|@@@1|10|-1|@6|@1|-1|-1|}}
{2010202004|XXGiport|78|0|198|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|write|0.0|@@@1776|0|}}
{2010501005|XXGpvertex|79|0|201|0|{|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|key|\{\}|3|2|$|@{0|}}
{30001002|XXparameter|transform|out::rollup(in) =
begin
  out.rec_cnt :: sum(in.rec_cnt);
end;|3|1|l|@{0|}}
{30001002|XXparameter|out_metadata|record
  decimal("\\n") rec_cnt = 0;
end;|3|1|l|@{0|}}
{30001002|XXparameter|error_metadata|string('\\n')|3|1|l|@{0|}}
{30001002|XXparameter|log_metadata|record string("\|") node, timestamp, component, subcomponent, event_type; string("\|\\n") event_text; end|3|1|l|@{0|}}
{30001002|XXparameter|!prototype_path|$AB_COMPONENTS/Transform/Rollup.mpc|3|2|Pf$|@{0|}}
{30001002|XXparameter|Layout||3|8|s=|@{0|}}
{30001002|XXparameter|in_metadata||3|8|s=|@{0|}}
{30001002|XXparameter|reject_metadata||3|8|s=|@{0|}}
}}@1|@546029|688918|566000|709000|103000|65000|508|Rollup|Ab Initio Software|Built-in 2.0:|1|10|0||6||32769|1|{1|0|}}}
{2010203004|XXGoport|80|0|203|0|{@{}@668000|736000|11000|11000|out|0.0|@@@2448|0|}}
{2010203004|XXGoport|81|0|206|0|{@{}@0|0|0|0|reject|0.0|@@@1168|0|}}
{2010203004|XXGoport|82|0|208|0|{@{}@0|0|0|0|error|0.0|@@@1176|0|}}
{2010203004|XXGoport|83|0|210|0|{@{}@0|0|0|0|log|0.0|@@@1176|0|}}
{2010202004|XXGiport|84|0|212|0|{@{}@556000|736000|11000|11000|in|0.0|@@@1808|0|}}
{2010501005|XXGpvertex|85|0|215|0|{Generates data records that summarize groups of data records.  Rollup in memory maximizes performance by keeping intermediate results in main memory.  Rollup sorted requires grouped input.|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|mpname|?rollup|3|1|Hl||{0|}}
{30001002|XXparameter|image__|/~ab_home/bin/transform|3|2|H$||{0|}}
{30001002|XXparameter|sorted_input|True|13|1|Fl|Ordering of input data|{0|}}
{30001002|XXparameter|sorted_input.display_name|sorted-input|3|9|P|@{0|}}
{30001002|XXparameter|sorted_input.update_clauses|retain_old_default|3|9|P|@{0|}}
{30001002|XXparameter|sorted_input_display_values|In-memory:Inputneednotbesorted Inputmustbesortedorgrouped|3|2|H$||{0|}}
{30001002|XXparameter|sorted_input_display_values.display_name|sorted-input-display-values|3|9|P|@{0|}}
{30001002|XXparameter|key_method|value sorted-input False True default constant True|13|13|Fv|How groups are specified|{0|}}
{30001002|XXparameter|key_method.display_name|key-method|3|9|P|@{0|}}
{30001002|XXparameter|key_method.condition|param sorted-input True|3|15|P?|@{0|}}
{30001002|XXparameter|key_method_display_values|Usekey_changefunction Usekeyspecifier|3|2|H$||{0|}}
{30001002|XXparameter|key_method_display_values.display_name|key-method-display-values|3|9|P|@{0|}}
{30001002|XXparameter|key||19|2|RF$|Key to define rollup groups|{0|}}
{30001002|XXparameter|key.condition|param key-method True|3|15|P?|@{0|}}
{30001002|XXparameter|positional_key|value key-method False "" sameas key|19|13|Ov|Key to define rollup groups|{0|}}
{30001002|XXparameter|transform||8|1|RFOl|Required and optional DML functions|{0|}}
{30001002|XXparameter|max_core|67108864|1|2|FK$|Maximum memory usage (before spilling to disk) in bytes|{0|}}
{30001002|XXparameter|max_core.display_name|max-core|3|9|P|@{0|}}
{30001002|XXparameter|max_core.keyword|max-core|3|9|P|@{0|}}
{30001002|XXparameter|max_core.condition|param sorted-input False|3|15|P?|@{0|}}
{30001002|XXparameter|max_core.update_clauses|retain_old_default|3|9|P|@{0|}}
{30001002|XXparameter|error_group||3|2|F$|Optional group name identifying a Handle Errors component to which errors can be directed.|{0|}}
{30001002|XXparameter|error_group.condition|version 2.16:|3|15|P?|@{0|}}
{30001002|XXparameter|log_group||3|2|F$|Optional group name of a Handle Logs component to which log output can be directed.|{0|}}
{30001002|XXparameter|log_group.condition|version 2.16:|3|15|P?|@{0|}}
{30001002|XXparameter|reject_threshold|Abort on first reject|15|1|Fl|When to abort if input records are rejected|{3|Abort on first reject|Never abort|Use limit/ramp|}}
{30001002|XXparameter|reject_threshold.display_name|reject-threshold|3|9|P|@{0|}}
{30001002|XXparameter|limit|0|1|2|F$|Maximum rejected records before failure|{0|}}
{30001002|XXparameter|limit.condition|param reject-threshold Use*|3|15|P?|@{0|}}
{30001002|XXparameter|ramp|0.0|2|2|F$|Rate of rejected records|{0|}}
{30001002|XXparameter|ramp.condition|param reject-threshold Use*|3|15|P?|@{0|}}
{30001002|XXparameter|limit_keyword|value reject-threshold Never* 0 value reject-threshold Abort* 0 sameas limit default dollar_substitution 0|1|13|Kv|Maximum rejected records before failure|{0|}}
{30001002|XXparameter|ramp_keyword|value reject-threshold Never* 99.0 value reject-threshold Abort* 0.0 sameas ramp default dollar_substitution 0.0|2|13|Kv|Rate of rejected records|{0|}}
{30001002|XXparameter|ramp_keyword.condition|version :2.12.999.m999|3|15|P?|@{0|}}
{30001002|XXparameter|ramp_keyword2|value reject-threshold Never* -1 value reject-threshold Abort* 0.0 sameas ramp default dollar_substitution 0.0|2|13|Kv|Rate of rejected records|{0|}}
{30001002|XXparameter|ramp_keyword2.condition|version 2.13:|3|15|P?|@{0|}}
{30001002|XXparameter|keyword_map|limit_keyword limit ramp_keyword ramp ramp_keyword2 ramp|3|2|$||{0|}}
{30001002|XXparameter|keyword_map.display_name|keyword-map|3|9|P|@{0|}}
{30001002|XXparameter|check_sort|True|13|1|FKl|Abort on first record that is out of sorted order|{0|}}
{30001002|XXparameter|check_sort.display_name|check-sort|3|9|P|@{0|}}
{30001002|XXparameter|check_sort.keyword|check-sort|3|9|P|@{0|}}
{30001002|XXparameter|check_sort.condition|param key-method True param sorted-input True version 2.11.1:|3|15|P?|@{0|}}
{30001002|XXparameter|check_sort.update_clauses|update_value False|3|9|P|@{0|}}
{30001002|XXparameter|grouped_input|False|13|1|Fl|Input data should be processed in groups|{0|}}
{30001002|XXparameter|grouped_input.display_name|grouped-input|3|9|P|@{0|}}
{30001002|XXparameter|grouped_input.condition|param sorted-input False version 2.14.102:|3|15|P?|@{0|}}
{30001002|XXparameter|grouped_input_display_values|Dataisnotgroupedbyamajorkey Dataisgroupedbyamajorkey|3|2|H$||{0|}}
{30001002|XXparameter|grouped_input_display_values.display_name|grouped-input-display-values|3|9|P|@{0|}}
{30001002|XXparameter|major_key||19|2|FK$|Major key into which data is grouped|{0|}}
{30001002|XXparameter|major_key.display_name|major-key|3|9|P|@{0|}}
{30001002|XXparameter|major_key.keyword|major-key|3|9|P|@{0|}}
{30001002|XXparameter|major_key.condition|param grouped-input True|3|15|P?|@{0|}}
{30001002|XXparameter|logging|False|13|1|Fl|Log internal events|{0|}}
{30001002|XXparameter|log|log_concat|3|13|Kv|Special log parameter|{0|}}
{30001002|XXparameter|log.condition|param logging True|3|15|P?|@{0|}}
{30001002|XXparameter|log_input||15|1|Fl|Frequency of input records to log|{7| |1|10|100|1000|10000|100000|}}
{30001002|XXparameter|log_input.condition|param logging True|3|15|P?|@{0|}}
{30001002|XXparameter|log_output||15|1|Fl|Frequency of output records to log|{7| |1|10|100|1000|10000|100000|}}
{30001002|XXparameter|log_output.condition|param logging True|3|15|P?|@{0|}}
{30001002|XXparameter|log_reject||15|1|Fl|Frequency of reject records to log|{7| |1|10|100|1000|10000|100000|}}
{30001002|XXparameter|log_reject.condition|param logging True|3|15|P?|@{0|}}
{30001002|XXparameter|log_intermediate||15|1|Fl|Frequency of intermediate records to log|{7| |1|10|100|1000|10000|100000|}}
{30001002|XXparameter|log_intermediate.condition|param logging True|3|15|P?|@{0|}}
{30001002|XXparameter|num_operations|10|1|2|H$|Number of operations in package|{0|}}
{30001002|XXparameter|num_variables|1|1|2|H$|Number of public variables in rollup|{0|}}
{30001002|XXparameter|variable1|optional temporary_type temp|3|2|H$|Temporary variable|{0|}}
{30001002|XXparameter|operation1|optional int out::input_select(in)|3|2|H$|Select input record|{0|}}
{30001002|XXparameter|operation2|optional temp ? temp::initialize(in) : NULL|3|2|H$|Initialize temporary|{0|}}
{30001002|XXparameter|operation3|temp ? temp::rollup(temp, in) : out::rollup(in)|3|2|H$|Do computation|{0|}}
{30001002|XXparameter|operation4|optional temp ? out::finalize(temp, in) : NULL|3|2|H$|Create output record|{0|}}
{30001002|XXparameter|operation5|optional int out::output_select(out)|3|2|H$|Select output record|{0|}}
{30001002|XXparameter|operation6|optional integer(8) out::key_change(in in1, in in2)|3|2|H$|Determine if key changed|{0|}}
{30001002|XXparameter|operation1_analysis|int input_select_out::input_select(in)|3|2|H$|Select input record|{0|}}
{30001002|XXparameter|operation5_analysis|int output_select_out::output_select(out)|3|2|H$|Select output record|{0|}}
{30001002|XXparameter|operation6_analysis|integer(8) key_change_out::key_change(in in, in in)|3|2|H$|Determine if key changed|{0|}}
{30001002|XXparameter|_simple_transform_operation|rollup|3|2|H$|What operation should transform editor open with|{0|}}
{30001002|XXparameter|_simple_transform_operation.condition|version 2.11.7.m1:|3|15|P?|@{0|}}
{30001002|XXparameter|sorted_mpname|rollup|3|2|H$||{0|}}
{30001002|XXparameter|sorted_mpname.display_name|sorted-mpname|3|9|P|@{0|}}
{30001002|XXparameter|memory_mpname|hash-rollup|3|2|H$||{0|}}
{30001002|XXparameter|memory_mpname.display_name|memory-mpname|3|9|P|@{0|}}
{30001002|XXparameter|condition||3|2|F$||{0|}}
{30001002|XXparameter|conditionInputPort|in|3|2|F$||{0|}}
{30001002|XXparameter|conditionOutputPort|out|3|2|F$||{0|}}
{30001002|XXparameter|condition_interpretation|Replace with flow|15|1|Fl||{2|Replace with flow|Remove completely|}}
{30001002|XXparameter|condition_interpretation.display_name|condition-interpretation|3|9|P|@{0|}}
{30001002|XXparameter|port_analysis|reject=in|3|2|H$||{0|}}
{30001002|XXparameter|continuous_analysis||3|2|H$||{0|}}
{30001002|XXparameter|filter_aggregate_analysis|out=filter_xfr(transform.input_select); out=filter_xfr(transform.output_select); out=aggregate(key)|3|2|H$||{0|}}
{30001002|XXparameter|num_types|2|1|2|H$|Number of private data types in package|{0|}}
{30001002|XXparameter|type1|error_info_t error_info "record utf8 string(big endian integer(4)) component; big endian integer(4) port_index; utf8 string(big endian integer(4)) parameter; utf8 string(big endian integer(4)) message; record utf8 string(big endian integer(4)) code; big endian integer(4) parent_index; record utf8 string(big endian integer(4)) name; utf8 string(big endian integer(4)) value; end [big endian integer(4)] attributes; end [big endian integer(4)] details; end"|3|2|H$|Type of error_info|{0|}}
{30001002|XXparameter|type2|log_event_t log_event "record string('') event_type; string('') event_text; end"|3|2|H$|Type of error_info|{0|}}
{30001002|XXparameter|operation7|optional out::output_for_error(error_info, in)|3|2|H$|Function to create output in case of error.|{0|}}
{30001002|XXparameter|operation8|optional error::make_error(error_info, in)|3|2|H$|Function to create error record in case of error.|{0|}}
{30001002|XXparameter|operation9|optional log_event::log_error(error_info, in)|3|2|H$|Function to create log record in case of error.|{0|}}
{30001002|XXparameter|operation10|optional log_event::final_log_output()|3|2|H$|Function to create final log record.|{0|}}
{30001002|XXparameter|num_callbacks|4|1|2|H$|Number of callbacks in package|{0|}}
{30001002|XXparameter|_use_rich_error_format|True|13|1|Hl|False means use legacy string newline format|{0|}}
{30001002|XXparameter|_use_rich_log_format|True|13|1|Hl|False means use legacy native charset format|{0|}}
{30001002|XXparameter|Layout|@9|2|RFs$||{0|}}
{30001002|XXparameter|in_metadata||7|1|RFsl||{0|}}
{30001002|XXparameter|out_metadata||7|1|RFl||{0|}}
{30001002|XXparameter|reject_metadata||7|1|Fsl||{0|}}
{30001002|XXparameter|error_metadata||7|1|Fl||{0|}}
{30001002|XXparameter|log_metadata||7|1|Fl||{0|}}
{30001002|XXparameter|doc_transform||8|2|FHs$|Document your transformation for dependency analysis|{0|}}
{30001002|XXparameter|doc_operation1|out,reject,error,log::document(in)|3|2|RH$||{0|}}
{30001002|XXparameter|mpcmodtime|1299550340|1|1|Hl|The last modification time of this component's template|{0|}}
{30001002|XXparameter|_propagation|metadata type: reject = in
metadata type: error = remote("$AB_HOME/include/error-info.dml")
metadata type: log = remote("$AB_HOME/include/log-info.dml")|3|1|HKl|@{0|}}
{30001002|XXparameter|_propagate_through||3|1|FHKl|@{0|}}
}}@0|@0|0|0|0|0|0|0|@@@1|10|-1|@6|@1|1|{1|0|}}}
{2010203004|XXGoport|86|0|217|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|out|0.0|@@@2448|0|}}
{2010203004|XXGoport|87|0|220|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|F=||{0|}}
}}@0|0|0|0|reject|0.0|@@@1168|0|}}
{2010203004|XXGoport|88|0|223|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|F=||{0|}}
}}@0|0|0|0|error|0.0|@@@1176|0|}}
{2010203004|XXGoport|89|0|226|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|F=||{0|}}
}}@0|0|0|0|log|0.0|@@@1176|0|}}
{2010202004|XXGiport|90|0|229|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|in|0.0|@@@1808|0|}}
{2010501005|XXGpvertex|91|0|233|0|{|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|out_metadata|record
  decimal(" records") rec_cnt = 0;
  string("\\n") junk = NULL;
end;|3|1|l|@{0|}}
{30001002|XXparameter|!prototype_path|$AB_COMPONENTS/Miscellaneous/Redefine_Format.mpc|3|2|Pf$|@{0|}}
{30001002|XXparameter|in_metadata||3|8|s=|@{0|}}
{30001002|XXparameter|Layout||3|8|s=|@{0|}}
}}@1|@349223|687013|369000|707000|124000|68000|506|Redefine Format-1|Ab Initio Software|Built-in|1|10|0||6||32769|1|{1|0|}}}
{2010203004|XXGoport|92|0|235|0|{@{}@492000|735000|11000|11000|out|0.0|@@@2448|0|}}
{2010202004|XXGiport|93|0|238|0|{@{}@359000|735000|11000|11000|in|0.0|@@@1936|0|}}
{2010501005|XXGpvertex|94|0|241|0|{Copies data records from its input to its output without changing the values.  Use Redefine Format to change a record format or rename fields.|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|mpname|copy|3|1|Hl||{0|}}
{30001002|XXparameter|image__|unitool|3|2|H$||{0|}}
{30001002|XXparameter|condition||3|2|F$||{0|}}
{30001002|XXparameter|conditionInputPort|in|3|2|F$||{0|}}
{30001002|XXparameter|conditionOutputPort|out|3|2|F$||{0|}}
{30001002|XXparameter|condition_interpretation|Replace with flow|15|1|Fl||{2|Replace with flow|Remove completely|}}
{30001002|XXparameter|condition_interpretation.display_name|condition-interpretation|3|9|P|@{0|}}
{30001002|XXparameter|port_analysis|out=byte_copy(in)|3|2|H$||{0|}}
{30001002|XXparameter|continuous_analysis||3|2|H$||{0|}}
{30001002|XXparameter|in_metadata||7|1|RFsl||{0|}}
{30001002|XXparameter|out_metadata||7|1|RFl||{0|}}
{30001002|XXparameter|doc_transform||8|2|FHs$|Document your transformation for dependency analysis|{0|}}
{30001002|XXparameter|doc_operation1|out::document(in)|3|2|RH$||{0|}}
{30001002|XXparameter|mpcmodtime|1299550339|1|1|Hl|The last modification time of this component's template|{0|}}
{30001002|XXparameter|Layout|@9|2|RFs$||{0|}}
{30001002|XXparameter|_propagate_through||3|1|FHKl|@{0|}}
}}@0|@0|0|0|0|0|0|0|@@@1|10|-1|@6|@1|1|{1|0|}}}
{2010203004|XXGoport|95|0|243|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|out|0.0|@@@2448|0|}}
{2010202004|XXGiport|96|0|246|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|in|0.0|@@@1936|0|}}
{2010503005|XXGfvertex|97|0|250|0|{|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|Layout|$[[record kind 85 subkind 0 parts [vector _interp_("file:$LOG_FILE", "dollar_substitution")]]]|3|9||@{0|}}
{30001002|XXparameter|read_metadata|record
  string("\\n") line;
end;|3|1|l|@{0|}}
{30001002|XXparameter|!prototype_path|$AB_COMPONENTS/Datasets/Input_File.mdc|3|2|Pf$|@{0|}}
}}@1|@35188|700746|60000|721000|40000|40000|502|Output log file|eBay||1|10|0||6||32769|-1|-1|}}
{2010203004|XXGoport|98|0|252|0|{@{}@99000|735000|11000|11000|read|0.0|@@@2160|0|}}
{2010503005|XXGfvertex|99|0|255|0|{Represents one file, many files, or a multifile as an input to your graph.|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|protection|0666|12|2|RF$||{0|}}
{30001002|XXparameter|mode|0x0001|1|2|FH$|modes of access|{0|}}
{30001002|XXparameter|condition||3|2|F$||{0|}}
{30001002|XXparameter|conditionInputPort||3|2|F$||{0|}}
{30001002|XXparameter|conditionOutputPort||3|2|F$||{0|}}
{30001002|XXparameter|condition_interpretation|Remove completely|15|1|Fl||{2|Replace with flow|Remove completely|}}
{30001002|XXparameter|condition_interpretation.display_name|condition-interpretation|3|9|P|@{0|}}
{30001002|XXparameter|key||19|2|RF$|Key specifier For Lookup File|{0|}}
{30001002|XXparameter|key.condition|param direct_addressed False mode lookup|3|15|P?|@{0|}}
{30001002|XXparameter|m_catalog_add_option_names|block_compressed keep_on_disk direct_addressed|3|2|H$||{0|}}
{30001002|XXparameter|eme_index_location||3|2|F$|Place in the EME to create a dataset corresponding to this index.|{0|}}
{30001002|XXparameter|eme_index_location.condition|mode lookup version 2.15.2.r23:|3|15|P?|@{0|}}
{30001002|XXparameter|index_url|@28|2|F$|The URL of the index for this lookup file (optional)|{0|}}
{30001002|XXparameter|index_url.condition|param direct_addressed False mode lookup version 2.15.2.r23:|3|15|P?|@{0|}}
{30001002|XXparameter|keep_on_disk|False|13|1|Fl|Keep on Disk|{0|}}
{30001002|XXparameter|keep_on_disk.condition|mode lookup version 2.15.2.r23:|3|15|P?|@{0|}}
{30001002|XXparameter|block_compressed|False|13|1|Fl|Block Compressed|{0|}}
{30001002|XXparameter|block_compressed.condition|mode lookup version 2.15.2.r23:|3|15|P?|@{0|}}
{30001002|XXparameter|direct_addressed|False|13|1|Fl|Set to true if the block compressed lookup is direct addressed|{0|}}
{30001002|XXparameter|direct_addressed.condition|param block_compressed True mode lookup version 2.15.5.r34:|3|15|P?|@{0|}}
{30001002|XXparameter|Layout|@28|2|RF$||{0|}}
{30001002|XXparameter|read_metadata||7|1|RFl||{0|}}
{30001002|XXparameter|mpcmodtime|1299550332|1|1|Hl|The last modification time of this component's template|{0|}}
{30001002|XXparameter|eme_dataset_location||3|2|F$|Place in the EME to create a dataset corresponding to this file.|{0|}}
}}@0|@0|0|0|0|0|0|0|@@@1|10|-1|@6|@1|-1|-1|}}
{2010203004|XXGoport|100|0|257|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|read|0.0|@@@2160|0|}}
{2010501005|XXGpvertex|101|0|265|0|{|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|LogFile|$[[record kind 85 subkind 0 parts [vector _interp_("file:$LOG_FILE", "dollar_substitution")]]]|3|9||@{0|}}
{30001002|XXparameter|!prototype_path|$AB_COMPONENTS/Miscellaneous/Gather_Logs.mpc|3|2|Pf$|@{0|}}
{30001002|XXparameter|in_metadata||3|8|s=|@{0|}}
}}@0|@596427|203207|616000|223000|99000|78000|480|Gather Logs|Ab Initio Software|Built-in|1|10|0||6||32769|1|{1|0|}}}
{2010202004|XXGiport|102|0|267|0|{@{}@660000|213000|11000|11000|in|0.0|@@@786|0|}}
{2010501005|XXGpvertex|103|0|270|0|{Collects the output from log ports of components for analysis of a graph after execution.|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|mpname|logger|3|1|Hl||{0|}}
{30001002|XXparameter|image__|unitool|3|2|H$||{0|}}
{30001002|XXparameter|LogFile|@28|2|RFO$|Filename to log to|{0|}}
{30001002|XXparameter|StartText|Start|3|2|RFO$|Text to include in start record|{0|}}
{30001002|XXparameter|EndText|End|3|2|RFO$|Text to include in end record|{0|}}
{30001002|XXparameter|condition||3|2|F$||{0|}}
{30001002|XXparameter|conditionInputPort||3|2|F$||{0|}}
{30001002|XXparameter|conditionOutputPort||3|2|F$||{0|}}
{30001002|XXparameter|condition_interpretation|Remove completely|15|1|Fl||{2|Replace with flow|Remove completely|}}
{30001002|XXparameter|condition_interpretation.display_name|condition-interpretation|3|9|P|@{0|}}
{30001002|XXparameter|Layout|$[[record kind 85 subkind 0 parts [vector _interp_("file:.", "dollar_substitution")]]]|9|9|F|logger should run serially by default|{0|}}
{30001002|XXparameter|continuous_analysis|publisher, fan_in_ok|3|2|H$||{0|}}
{30001002|XXparameter|in_metadata||7|1|RFsl||{0|}}
{30001002|XXparameter|doc_transform||8|2|FHs$|Document your transformation for dependency analysis|{0|}}
{30001002|XXparameter|doc_operation1|::document(in)|3|2|RH$||{0|}}
{30001002|XXparameter|mpcmodtime|1299550339|1|1|Hl|The last modification time of this component's template|{0|}}
}}@0|@0|0|0|0|0|0|0|@@@1|10|-1|@6|@1|1|{1|0|}}}
{2010202004|XXGiport|104|0|272|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|in|0.0|@@@786|0|}}
{2010501005|XXGpvertex|105|0|285|0|{|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|commandline|touch $WATCH_FILE|3|2|$|@{0|}}
{30001002|XXparameter|Layout|$[[record kind 85 subkind 0 parts [vector _interp_("file:.", "dollar_substitution")]]]|3|9||@{0|}}
{30001002|XXparameter|!prototype_path|$AB_COMPONENTS/Miscellaneous/Run_Program.mpc|3|2|Pf$|@{0|}}
}}@2|@843169|676255|863000|696000|123000|87000|511|Touch Watch File|Ab Initio Software|Built-in|1|10|0||6||32769|1|{1|0|}}}
{2010203004|XXGoport|106|0|287|0|{@{}@985000|734000|11000|11000|out|0.0|@@@2260|0|}}
{2010202004|XXGiport|107|0|289|0|{@{}@853000|734000|11000|11000|in|0.0|@@@1732|0|}}
{2010501005|XXGpvertex|108|0|291|0|{Executes a standard UNIX or Windows program.|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|mpname|filter|3|1|Hl||{0|}}
{30001002|XXparameter|image__|unitool|3|2|H$||{0|}}
{30001002|XXparameter|commandline||16|2|RFO$|Command line of UNIX or Windows program to execute|{0|}}
{30001002|XXparameter|NoHidePorts|in out|3|2|H$||{0|}}
{30001002|XXparameter|condition||3|2|F$||{0|}}
{30001002|XXparameter|conditionInputPort|in|3|2|F$||{0|}}
{30001002|XXparameter|conditionOutputPort|out|3|2|F$||{0|}}
{30001002|XXparameter|condition_interpretation|Replace with flow|15|1|Fl||{2|Replace with flow|Remove completely|}}
{30001002|XXparameter|condition_interpretation.display_name|condition-interpretation|3|9|P|@{0|}}
{30001002|XXparameter|port_analysis|out=all_to_all(in)|3|2|H$||{0|}}
{30001002|XXparameter|Layout|@9|2|RF$||{0|}}
{30001002|XXparameter|in_metadata||7|1|Fsl||{0|}}
{30001002|XXparameter|out_metadata||7|1|Fsl||{0|}}
{30001002|XXparameter|doc_transform||8|2|FHs$|Document your transformation for dependency analysis|{0|}}
{30001002|XXparameter|doc_operation1|out::document(in)|3|2|RH$||{0|}}
{30001002|XXparameter|mpcmodtime|1299550339|1|1|Hl|The last modification time of this component's template|{0|}}
{30001002|XXparameter|_propagate_through||3|1|FHKl|@{0|}}
}}@0|@0|0|0|0|0|0|0|@@@1|10|-1|@6|@1|1|{1|0|}}}
{2010203004|XXGoport|109|0|293|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|F=||{0|}}
}}@0|0|0|0|out|0.0|@@@2260|0|}}
{2010202004|XXGiport|110|0|296|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|F=||{0|}}
}}@0|0|0|0|in|0.0|@@@1732|0|}}
{2010110002|XXGconstant|111|0|299|0|{@{}@{1|1012494|}}}
{2010002001|XXGdirectory_object_object|0|1|2|0|{XXGgraph|}0|1|}
{2010704001|XXGgraph_graphinfo_graphinfo|1|0|2|0|{}1|2|}
{2010706001|XXGgraphinfo_runsettings_runsettings|2|0|4|0|{}2|3|}
{2010109001|XXGobject_property_value|3|0|6|0|{0|legend_face|92|}2|4|}
{2010605001|XXGgraph_legend_legend|4|0|8|0|{0|}1|5|}
{2010241001|XXGlegend_face_face|5|0|10|0|{}5|6|}
{2010604001|XXGgraph_flow_flow|6|0|12|0|{Flow_1|}1|7|}
{2010604001|XXGgraph_flow_flow|7|0|14|0|{Flow_2|}1|8|}
{2010604001|XXGgraph_flow_flow|8|0|16|0|{Flow_20|}1|9|}
{2010604001|XXGgraph_flow_flow|9|0|18|0|{Flow_23|}1|10|}
{2010604001|XXGgraph_flow_flow|10|0|20|0|{Flow_3|}1|11|}
{2010604001|XXGgraph_flow_flow|11|0|22|0|{Flow_4|}1|12|}
{2010604001|XXGgraph_flow_flow|12|0|24|0|{Flow_5|}1|13|}
{2010604001|XXGgraph_flow_flow|13|0|26|0|{Flow_6|}1|14|}
{2010604001|XXGgraph_flow_flow|14|0|28|0|{Flow_7|}1|15|}
{2010604001|XXGgraph_flow_flow|15|0|30|0|{Flow_8|}1|16|}
{2010601001|XXGgraph_vertex_vertex|16|0|32|0|{Deflate|}1|17|}
{2010212001|XXGvertex_oport_oport|17|0|34|0|{0|out|}17|18|}
{2010213001|XXGoport_dst_flow|18|0|36|0|{0|}18|16|}
{2010211001|XXGvertex_iport_iport|19|0|37|0|{0|in|}17|19|}
{2010214001|XXGiport_src_flow|20|0|39|0|{0|}19|8|}
{2010007001|XXGobject_proto_object|21|0|40|0|{}17|20|}
{2010212001|XXGvertex_oport_oport|22|0|42|0|{0|out|}20|21|}
{2010006001|XXGobject_psameas_object|23|0|44|0|{metadata|out_metadata|1|0|}21|20|}
{2010211001|XXGvertex_iport_iport|24|0|45|0|{0|in|}20|22|}
{2010006001|XXGobject_psameas_object|25|0|47|0|{metadata|in_metadata|1|0|}22|20|}
{2010006001|XXGobject_psameas_object|26|0|48|0|{Layout|Layout|0|0|}17|23|}
{2010211001|XXGvertex_iport_iport|27|0|50|0|{0|write|}23|24|}
{2010214001|XXGiport_src_flow|28|0|52|0|{0|}24|16|}
{2010007001|XXGobject_proto_object|29|0|53|0|{}23|25|}
{2010211001|XXGvertex_iport_iport|30|0|55|0|{0|write|}25|26|}
{2010006001|XXGobject_psameas_object|31|0|57|0|{metadata|write_metadata|1|0|}26|25|}
{2010006001|XXGobject_psameas_object|32|0|58|0|{write_metadata|out_metadata|0|0|}23|17|}
{2010006001|XXGobject_psameas_object|33|0|59|0|{in_metadata|out_metadata|0|0|}17|27|}
{2010212001|XXGvertex_oport_oport|34|0|61|0|{0|out|}27|28|}
{2010213001|XXGoport_dst_flow|35|0|63|0|{0|}28|8|}
{2010212001|XXGvertex_oport_oport|36|0|64|0|{1|reject|}27|29|}
{2010212001|XXGvertex_oport_oport|37|0|66|0|{2|error|}27|30|}
{2010212001|XXGvertex_oport_oport|38|0|68|0|{3|log|}27|31|}
{2010213001|XXGoport_dst_flow|39|0|70|0|{0|}31|13|}
{2010211001|XXGvertex_iport_iport|40|0|71|0|{0|in|}27|32|}
{2010214001|XXGiport_src_flow|41|0|73|0|{0|}32|12|}
{2010007001|XXGobject_proto_object|42|0|74|0|{}27|33|}
{2010212001|XXGvertex_oport_oport|43|0|76|0|{0|out|}33|34|}
{2010006001|XXGobject_psameas_object|44|0|78|0|{metadata|out_metadata|1|0|}34|33|}
{2010212001|XXGvertex_oport_oport|45|0|79|0|{1|reject|}33|35|}
{2010006001|XXGobject_psameas_object|46|0|81|0|{metadata|reject_metadata|1|0|}35|33|}
{2010212001|XXGvertex_oport_oport|47|0|82|0|{2|error|}33|36|}
{2010006001|XXGobject_psameas_object|48|0|84|0|{metadata|error_metadata|1|0|}36|33|}
{2010212001|XXGvertex_oport_oport|49|0|85|0|{3|log|}33|37|}
{2010006001|XXGobject_psameas_object|50|0|87|0|{metadata|log_metadata|1|0|}37|33|}
{2010211001|XXGvertex_iport_iport|51|0|88|0|{0|in|}33|38|}
{2010006001|XXGobject_psameas_object|52|0|90|0|{metadata|in_metadata|1|0|}38|33|}
{2010006001|XXGobject_psameas_object|53|0|91|0|{Layout|Layout|0|0|}27|23|}
{2010006001|XXGobject_psameas_object|54|0|92|0|{in_metadata|out_metadata|0|0|}27|39|}
{2010212001|XXGvertex_oport_oport|55|0|94|0|{0|out|}39|40|}
{2010213001|XXGoport_dst_flow|56|0|96|0|{0|}40|12|}
{2010211001|XXGvertex_iport_iport|57|0|97|0|{0|in|}39|41|}
{2010214001|XXGiport_src_flow|58|0|99|0|{0|}41|11|}
{2010007001|XXGobject_proto_object|59|0|100|0|{}39|42|}
{2010212001|XXGvertex_oport_oport|60|0|102|0|{0|out|}42|43|}
{2010006001|XXGobject_psameas_object|61|0|104|0|{metadata|out_metadata|1|0|}43|42|}
{2010211001|XXGvertex_iport_iport|62|0|105|0|{0|in|}42|44|}
{2010006001|XXGobject_psameas_object|63|0|107|0|{metadata|in_metadata|1|0|}44|42|}
{2010006001|XXGobject_psameas_object|64|0|108|0|{Layout|Layout|0|0|}39|23|}
{2010006001|XXGobject_psameas_object|65|0|109|0|{in_metadata|out0_metadata|0|0|}39|45|}
{2010212001|XXGvertex_oport_oport|66|0|111|0|{0|out0|}45|46|}
{2010213001|XXGoport_dst_flow|67|0|113|0|{0|}46|11|}
{2010212001|XXGvertex_oport_oport|68|0|114|0|{1|reject0|}45|47|}
{2010212001|XXGvertex_oport_oport|69|0|116|0|{2|error0|}45|48|}
{2010212001|XXGvertex_oport_oport|70|0|118|0|{3|log|}45|49|}
{2010211001|XXGvertex_iport_iport|71|0|120|0|{0|in|}45|50|}
{2010214001|XXGiport_src_flow|72|0|122|0|{0|}50|7|}
{2010007001|XXGobject_proto_object|73|0|123|0|{}45|51|}
{2010212001|XXGvertex_oport_oport|74|0|125|0|{0|out0|}51|52|}
{2010006001|XXGobject_psameas_object|75|0|127|0|{metadata|out0_metadata|1|0|}52|51|}
{2010006001|XXGobject_psameas_object|76|0|128|0|{transform0|transform0|1|0|}52|51|}
{2010212001|XXGvertex_oport_oport|77|0|129|0|{1|reject0|}51|53|}
{2010006001|XXGobject_psameas_object|78|0|131|0|{metadata|reject0_metadata|1|0|}53|51|}
{2010212001|XXGvertex_oport_oport|79|0|132|0|{2|error0|}51|54|}
{2010006001|XXGobject_psameas_object|80|0|134|0|{metadata|error0_metadata|1|0|}54|51|}
{2010212001|XXGvertex_oport_oport|81|0|135|0|{3|log|}51|55|}
{2010006001|XXGobject_psameas_object|82|0|137|0|{metadata|log_metadata|1|0|}55|51|}
{2010211001|XXGvertex_iport_iport|83|0|138|0|{0|in|}51|56|}
{2010006001|XXGobject_psameas_object|84|0|140|0|{metadata|in_metadata|1|0|}56|51|}
{2010006001|XXGobject_psameas_object|85|0|141|0|{Layout|Layout|0|0|}45|23|}
{2010006001|XXGobject_psameas_object|86|0|142|0|{in_metadata|read_metadata|0|0|}45|57|}
{2010212001|XXGvertex_oport_oport|87|0|144|0|{0|read|}57|58|}
{2010213001|XXGoport_dst_flow|88|0|146|0|{0|}58|7|}
{2010007001|XXGobject_proto_object|89|0|147|0|{}57|59|}
{2010212001|XXGvertex_oport_oport|90|0|149|0|{0|read|}59|60|}
{2010006001|XXGobject_psameas_object|91|0|151|0|{metadata|read_metadata|1|0|}60|59|}
{2010006001|XXGobject_psameas_object|92|0|152|0|{reject0_metadata|read_metadata|0|0|}45|57|}
{2010006001|XXGobject_psameas_object|93|0|153|0|{reject_metadata|out_metadata|0|0|}27|39|}
{2010601001|XXGgraph_vertex_vertex|94|0|154|0|{Filter_record_count_rows|}1|61|}
{2010212001|XXGvertex_oport_oport|95|0|156|0|{0|out|}61|62|}
{2010213001|XXGoport_dst_flow|96|0|158|0|{0|}62|14|}
{2010212001|XXGvertex_oport_oport|97|0|159|0|{1|deselect|}61|63|}
{2010212001|XXGvertex_oport_oport|98|0|161|0|{2|reject|}61|64|}
{2010212001|XXGvertex_oport_oport|99|0|163|0|{3|error|}61|65|}
{2010212001|XXGvertex_oport_oport|100|0|165|0|{4|log|}61|66|}
{2010211001|XXGvertex_iport_iport|101|0|167|0|{0|in|}61|67|}
{2010214001|XXGiport_src_flow|102|0|169|0|{0|}67|9|}
{2010007001|XXGobject_proto_object|103|0|170|0|{}61|68|}
{2010212001|XXGvertex_oport_oport|104|0|172|0|{0|out|}68|69|}
{2010006001|XXGobject_psameas_object|105|0|174|0|{metadata|out_metadata|1|0|}69|68|}
{2010212001|XXGvertex_oport_oport|106|0|175|0|{1|deselect|}68|70|}
{2010006001|XXGobject_psameas_object|107|0|177|0|{metadata|deselect_metadata|1|0|}70|68|}
{2010212001|XXGvertex_oport_oport|108|0|178|0|{2|reject|}68|71|}
{2010006001|XXGobject_psameas_object|109|0|180|0|{metadata|reject_metadata|1|0|}71|68|}
{2010212001|XXGvertex_oport_oport|110|0|181|0|{3|error|}68|72|}
{2010006001|XXGobject_psameas_object|111|0|183|0|{metadata|error_metadata|1|0|}72|68|}
{2010212001|XXGvertex_oport_oport|112|0|184|0|{4|log|}68|73|}
{2010006001|XXGobject_psameas_object|113|0|186|0|{metadata|log_metadata|1|0|}73|68|}
{2010211001|XXGvertex_iport_iport|114|0|187|0|{0|in|}68|74|}
{2010006001|XXGobject_psameas_object|115|0|189|0|{metadata|in_metadata|1|0|}74|68|}
{2010006001|XXGobject_psameas_object|116|0|190|0|{Layout|Layout|0|0|}61|75|}
{2010211001|XXGvertex_iport_iport|117|0|192|0|{0|write|}75|76|}
{2010214001|XXGiport_src_flow|118|0|194|0|{0|}76|10|}
{2010007001|XXGobject_proto_object|119|0|195|0|{}75|77|}
{2010211001|XXGvertex_iport_iport|120|0|197|0|{0|write|}77|78|}
{2010006001|XXGobject_psameas_object|121|0|199|0|{metadata|write_metadata|1|0|}78|77|}
{2010006001|XXGobject_psameas_object|122|0|200|0|{write_metadata|out_metadata|0|0|}75|79|}
{2010212001|XXGvertex_oport_oport|123|0|202|0|{0|out|}79|80|}
{2010213001|XXGoport_dst_flow|124|0|204|0|{0|}80|10|}
{2010212001|XXGvertex_oport_oport|125|0|205|0|{1|reject|}79|81|}
{2010212001|XXGvertex_oport_oport|126|0|207|0|{2|error|}79|82|}
{2010212001|XXGvertex_oport_oport|127|0|209|0|{3|log|}79|83|}
{2010211001|XXGvertex_iport_iport|128|0|211|0|{0|in|}79|84|}
{2010214001|XXGiport_src_flow|129|0|213|0|{0|}84|15|}
{2010007001|XXGobject_proto_object|130|0|214|0|{}79|85|}
{2010212001|XXGvertex_oport_oport|131|0|216|0|{0|out|}85|86|}
{2010006001|XXGobject_psameas_object|132|0|218|0|{metadata|out_metadata|1|0|}86|85|}
{2010212001|XXGvertex_oport_oport|133|0|219|0|{1|reject|}85|87|}
{2010006001|XXGobject_psameas_object|134|0|221|0|{metadata|reject_metadata|1|0|}87|85|}
{2010212001|XXGvertex_oport_oport|135|0|222|0|{2|error|}85|88|}
{2010006001|XXGobject_psameas_object|136|0|224|0|{metadata|error_metadata|1|0|}88|85|}
{2010212001|XXGvertex_oport_oport|137|0|225|0|{3|log|}85|89|}
{2010006001|XXGobject_psameas_object|138|0|227|0|{metadata|log_metadata|1|0|}89|85|}
{2010211001|XXGvertex_iport_iport|139|0|228|0|{0|in|}85|90|}
{2010006001|XXGobject_psameas_object|140|0|230|0|{metadata|in_metadata|1|0|}90|85|}
{2010006001|XXGobject_psameas_object|141|0|231|0|{Layout|Layout|0|0|}79|75|}
{2010006001|XXGobject_psameas_object|142|0|232|0|{in_metadata|out_metadata|0|0|}79|91|}
{2010212001|XXGvertex_oport_oport|143|0|234|0|{0|out|}91|92|}
{2010213001|XXGoport_dst_flow|144|0|236|0|{0|}92|15|}
{2010211001|XXGvertex_iport_iport|145|0|237|0|{0|in|}91|93|}
{2010214001|XXGiport_src_flow|146|0|239|0|{0|}93|14|}
{2010007001|XXGobject_proto_object|147|0|240|0|{}91|94|}
{2010212001|XXGvertex_oport_oport|148|0|242|0|{0|out|}94|95|}
{2010006001|XXGobject_psameas_object|149|0|244|0|{metadata|out_metadata|1|0|}95|94|}
{2010211001|XXGvertex_iport_iport|150|0|245|0|{0|in|}94|96|}
{2010006001|XXGobject_psameas_object|151|0|247|0|{metadata|in_metadata|1|0|}96|94|}
{2010006001|XXGobject_psameas_object|152|0|248|0|{Layout|Layout|0|0|}91|75|}
{2010006001|XXGobject_psameas_object|153|0|249|0|{in_metadata|read_metadata|0|0|}91|97|}
{2010212001|XXGvertex_oport_oport|154|0|251|0|{0|read|}97|98|}
{2010213001|XXGoport_dst_flow|155|0|253|0|{0|}98|9|}
{2010007001|XXGobject_proto_object|156|0|254|0|{}97|99|}
{2010212001|XXGvertex_oport_oport|157|0|256|0|{0|read|}99|100|}
{2010006001|XXGobject_psameas_object|158|0|258|0|{metadata|read_metadata|1|0|}100|99|}
{2010006001|XXGobject_psameas_object|159|0|259|0|{reject_metadata|out_metadata|0|0|}79|91|}
{2010006001|XXGobject_psameas_object|160|0|260|0|{deselect_metadata|read_metadata|0|0|}61|97|}
{2010006001|XXGobject_psameas_object|161|0|261|0|{in_metadata|read_metadata|0|0|}61|97|}
{2010006001|XXGobject_psameas_object|162|0|262|0|{out_metadata|read_metadata|0|0|}61|97|}
{2010006001|XXGobject_psameas_object|163|0|263|0|{reject_metadata|read_metadata|0|0|}61|97|}
{2010601001|XXGgraph_vertex_vertex|164|0|264|0|{Gather_Logs|}1|101|}
{2010211001|XXGvertex_iport_iport|165|0|266|0|{0|in|}101|102|}
{2010214001|XXGiport_src_flow|166|0|268|0|{0|}102|13|}
{2010007001|XXGobject_proto_object|167|0|269|0|{}101|103|}
{2010211001|XXGvertex_iport_iport|168|0|271|0|{0|in|}103|104|}
{2010006001|XXGobject_psameas_object|169|0|273|0|{metadata|in_metadata|1|0|}104|103|}
{2010006001|XXGobject_psameas_object|170|0|274|0|{in_metadata|log_metadata|0|0|}101|27|}
{2010601001|XXGgraph_vertex_vertex|171|0|275|0|{Input_File|}1|57|}
{2010601001|XXGgraph_vertex_vertex|172|0|276|0|{Normalize_Normalize_on_Vectored_Sub_Records|}1|27|}
{2010601001|XXGgraph_vertex_vertex|173|0|277|0|{Normalized_Output_File|}1|23|}
{2010601001|XXGgraph_vertex_vertex|174|0|278|0|{Output_log_file|}1|97|}
{2010601001|XXGgraph_vertex_vertex|175|0|279|0|{Record_Count_File|}1|75|}
{2010601001|XXGgraph_vertex_vertex|176|0|280|0|{Redefine_Format_1|}1|91|}
{2010601001|XXGgraph_vertex_vertex|177|0|281|0|{Redefine_Format_Create_Vectored_Sub_Records|}1|39|}
{2010601001|XXGgraph_vertex_vertex|178|0|282|0|{Reformat_Determine_Number_of_Normalized_Values|}1|45|}
{2010601001|XXGgraph_vertex_vertex|179|0|283|0|{Rollup|}1|79|}
{2010601001|XXGgraph_vertex_vertex|180|0|284|0|{Touch_Watch_File|}1|105|}
{2010212001|XXGvertex_oport_oport|181|0|286|0|{0|out|}105|106|}
{2010211001|XXGvertex_iport_iport|182|0|288|0|{0|in|}105|107|}
{2010007001|XXGobject_proto_object|183|0|290|0|{}105|108|}
{2010212001|XXGvertex_oport_oport|184|0|292|0|{0|out|}108|109|}
{2010006001|XXGobject_psameas_object|185|0|294|0|{metadata|out_metadata|1|0|}109|108|}
{2010211001|XXGvertex_iport_iport|186|0|295|0|{0|in|}108|110|}
{2010006001|XXGobject_psameas_object|187|0|297|0|{metadata|in_metadata|1|0|}110|108|}
{2010109001|XXGobject_property_value|188|0|298|0|{0|TrackingThumbprint|72|}1|111|}
