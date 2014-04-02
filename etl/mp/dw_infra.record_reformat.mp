@
{18|Type|XXGrepository|Primary_Name|XXGgraph|_ab_object_schema|0|_ab_semantic_schema|1|eme_flatten_format|1|schema_version|34|root_id|0|ent_count|98|rel_count|161|}
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
{30001002|XXparameter|IN_DIR|$(grep "^IN_DIR\\>" $SRC_ETL_CFG_FILE \| read PARAM VALUE COMMENT; eval print $VALUE/$SRC_JOB_ENV/$SRC_SUBJECT_AREA)|3|6|REk||{0|}}
{30001002|XXparameter|TRGT_IN_DIR|$(grep "^IN_DIR\\>" $TRGT_ETL_CFG_FILE \| read PARAM VALUE COMMENT; eval print $VALUE/$SRC_JOB_ENV/$TRGT_SUBJECT_AREA)|3|6|REKk||{0|}}
{30001002|XXparameter|CNDTL_EXTRACT_COMPRESS_LEVEL|$(grep "^CNDTL_EXTRACT_COMPRESS_LEVEL\\>" $TRGT_ETL_CFG_FILE \| read PARAM VALUE COMMENT; print $\{VALUE:-""\})|3|6|REKk|File Compression Level|{0|}}
{30001002|XXparameter|CNDTL_EXTRACT_COMPRESS|$(grep "^CNDTL_EXTRACT_COMPRESS\\>" $TRGT_ETL_CFG_FILE \| read PARAM VALUE COMMENT;  print $\{VALUE:-0\})|3|6|REKk||{0|}}
{30001002|XXparameter|CNDTL_EXTRACT_NOT_COMPRESS|$(if [[ $CNDTL_EXTRACT_COMPRESS != 1 ]]
  then
     print 1
  else
     print 0
  fi)|3|6|REKk||{0|}}
{30001002|XXparameter|CNDTL_SRC_EXTRACT_COMPRESS|$(grep "^CNDTL_EXTRACT_COMPRESS\\>" $SRC_ETL_CFG_FILE \| read PARAM VALUE COMMENT;  print $\{VALUE:-0\})|3|6|REKk||{0|}}
{30001002|XXparameter|INPUT_FILE|$( INPUT_FILE_TMP=""
   if [ $CNDTL_SRC_EXTRACT_COMPRESS != 1 ]
   then
      COMP_SFX=""
   else
       COMP_SFX=.gz
   fi

   if [ $EXTRACT_PROCESS_TYPE = 'L' ]
   then
      while read FILE_ID DATA_FILENAME OLD_FILENAME DONE_FILENAME
      do
         INPUT_FILE_TMP="$INPUT_FILE_TMP $(eval print $IN_DIR/$DATA_FILENAME.$SRC_BATCH_SEQ_NUM$COMP_SFX)"
      done < $DW_DAT/extract/$SRC_SUBJECT_AREA/$SRC_ETL_ID.sources.lis.$SRC_BATCH_SEQ_NUM
   elif [ $IS_CATY != 1 ]
   then   
      while read FILE_ID DBC_FILE PARALLEL_NUM TABLE_NAME DATA_FILENAME PARAM
      do
         if [[ CNDTL_SRC_EXTRACT_COMPRESS = 1  && -f $(eval print $IN_DIR/$DATA_FILENAME.$SRC_BATCH_SEQ_NUM.Z) ]]
         then
            COMP_SFX=.Z
         fi

         if [ -f $(eval print $\{IN_DIR\}/.$\{DATA_FILENAME\}.$\{SRC_BATCH_SEQ_NUM\}$\{COMP_SFX\}.mfctl) ]
         then
            INPUT_FILE_TMP="$INPUT_FILE_TMP $(eval print $(m_expand $IN_DIR/$DATA_FILENAME.$SRC_BATCH_SEQ_NUM$COMP_SFX))"
         else
            INPUT_FILE_TMP="$INPUT_FILE_TMP $(eval print $IN_DIR/$DATA_FILENAME.$SRC_BATCH_SEQ_NUM$COMP_SFX)"
         fi
      done < $DW_CFG/$SRC_ETL_ID.sources.lis
   else
      read TABLE_NAME DATA_FILENAME PARAM < $DW_CFG/$SRC_ETL_ID.sources.lis

      while read FILE_ID DBC_FILE
      do
         if [[ CNDTL_SRC_EXTRACT_COMPRESS = 1  && -f $(eval print $IN_DIR/$DATA_FILENAME.$SRC_BATCH_SEQ_NUM.Z) ]]
         then
            COMP_SFX=.Z
         fi

         if [ -f $(eval print $\{IN_DIR\}/.$\{DATA_FILENAME\}.$\{SRC_BATCH_SEQ_NUM\}$\{COMP_SFX\}.mfctl) ]
         then
            INPUT_FILE_TMP="$INPUT_FILE_TMP $(eval print $(m_expand $IN_DIR/$DATA_FILENAME.$SRC_BATCH_SEQ_NUM$COMP_SFX))"
         else
            INPUT_FILE_TMP="$INPUT_FILE_TMP $(eval print $IN_DIR/$DATA_FILENAME.$SRC_BATCH_SEQ_NUM$COMP_SFX)"
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
{30001002|XXparameter|REFORMAT_XFR_FILE|$DW_XFR/$TRGT_ETL_ID.reformat.xfr|3|2|RE$||{0|}}
{30001002|XXparameter|LOG_FILE_DIR|$DW_LOG/$SRC_JOB_ENV/$TRGT_SUBJECT_AREA/$TRGT_TABLE_ID/$CURR_DATE|3|6|REk||{0|}}
{30001002|XXparameter|LOG_FILE|$LOG_FILE_DIR/$TRGT_TABLE_ID.$SRC_JOB_TYPE_ID.normalize.$CURR_DATETIME.log|3|6|REk||{0|}}
{30001002|XXparameter|RECORD_COUNT_FILE|$TRGT_IN_DIR/$TRGT_TABLE_ID.record_count.dat.$SRC_BATCH_SEQ_NUM|3|2|RE$||{0|}}
{30001002|XXparameter|WATCH_FILE|$DW_WATCH/$SRC_JOB_ENV/$TRGT_ETL_ID.$SRC_JOB_TYPE.$SRC_BATCH_SEQ_NUM.done|3|2|RE$||{0|}}
{30001002|XXparameter|_parameters_of_Rollup|@34|9|FHK|@{0|}}
{30001002|XXparameter|_parameters_of_Output_log_file|@34|9|FHK|@{0|}}
{30001002|XXparameter|_parameters_of_Record_Count_File|@34|9|FHK|@{0|}}
{30001002|XXparameter|_parameters_of_Filter_record_count_rows|@34|9|FHK|@{0|}}
{30001002|XXparameter|_parameters_of_Redefine_Format_1|@34|9|FHK|@{0|}}
{30001002|XXparameter|_parameters_of_Touch_Watch_File|@34|9|FHK|@{0|}}
{30001002|XXparameter|_parameters_of_Input_File_1|@34|9|FHK|@{0|}}
{30001002|XXparameter|_parameters_of_Reformat_data_based_on_DW_XFR_ETL_ID_reformat_xfr|@34|9|FHK|@{0|}}
{30001002|XXparameter|_parameters_of_Deflate_1|@34|9|FHK|@{0|}}
{30001002|XXparameter|_parameters_of_Normalized_Output_File_1|@34|9|FHK|@{0|}}
{30001002|XXparameter|_parameters_of_Gather_Logs_1|@34|9|FHK|@{0|}}
{30001002|XXparameter|_parameters_of_Make_Log_Dir|@34|9|FHK|@{0|}}
{30001002|XXparameter|!ab_compatibility|3.1.4.4|3|9|P|@{0|}}
}}@0|@0|0|0|0|1280000|803000|0|dw_infra.record_reformat|Kevin Oaks||1|10|0||6||1|{0|}0|0|{0|}{0|}{0|}{0|}1.0|1280303|803030|7|}}
{2010703001|XXGgraphinfo|2|0|3|0|{@{}@1|3.0.3.1|{4|1|1|1|1|}Job 'dw_infra.record_reformat' Execution Status Report

Compilation of graph "dw_infra.record_reformat" started at Friday, October 04, 2013 10:59:32
No compilation errors
|||. /export/home/abinitio/cfg/abinitio.setup








|@|31633919|}}
{2010705002|XXGrunsettings|3|0|5|0|{@{}@lvsdpelega003.lvs.ebay.com||||1|60|0|1|0|0|0|0|0|1|1|0|0|1|1|1|1|0|0|0|@17|}}
{2010242002|XXGface|4|0|7|0|{@{}@Arial|16711680|80|1|}}
{2010240003|XXGlegend|5|0|9|0|{@{}@This graph will take an input record and reformat it based on record level.
It is designed to work as a post extract process of the single_table_extract_handler.ksh.
Also it could be executed independently by calling from $DW_MASTER_BIN/dw_infra.normalize_reformat_handler.ksh.
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
##SRC_JOB_TYPE - defaults to $JOB_TYPE from single_table_extract_handler.ksh
##SRC_JOB_TYPE_ID - defaults to $JOB_TYPE_ID from single_table_extract_handler.ksh

INPUT_DML_FILENAME - defaults to $SRC_ETL_ID.read.dml
OUTPUT_DML_FILENAME - defaults to $TRGT_ETL_ID.read.dml

* the preceding defaulted values can be overridden by passing them in on the command line.

For run time:

The following must exist in the appropriate directories:

INPUT_DML - a dml file of name $SRC_ETL_ID.read.dml
OUTPUT_DML - a dml file of name $TRGT_ETL_ID.read.dml
##REFORMAT_DML_FILE - a dml file of name $DW_DML/$TRGT_ETL_ID.reformat.write.dml
REFORMAT_XFR_FILE - an .xfr file of name $DW_XFR/$TRGT_ETL_ID.reformat.xfr
                                                                    
##REDEFINE_FORMAT_DML_FILE a dml file of name $DW_DML/$TRGT_ETL_ID.redefine_format.write.dml
##NORMALIZE_XFR_FILE - an .xfr file of name $DW_XFR/$TRGT_ETL_ID.normalize.xfr|18980|152279|39000|172000|641000|518000|595|}}
{2010242002|XXGface|6|0|11|0|{@{}@Arial|16711680|80|1|}}
{2010210004|XXGflow|7|0|13|0|{@{}@384|.5|.5|{12|647000|85000|667000|85000|696000|85000|696000|82000|725000|82000|745000|82000|}600|17|}}
{2010210004|XXGflow|8|0|15|0|{@{}@384|.5|.5|{12|888000|82000|908000|82000|1031000|82000|1031000|98000|1155000|98000|1195000|98000|}578|17|}}
{2010210004|XXGflow|9|0|17|0|{@{}@384|.5|.5|{12|524000|151000|524000|171000|524000|173000|885000|173000|885000|175000|885000|195000|}601|19|}}
{2010210004|XXGflow|10|0|19|0|{@{}@384|.5|.5|{8|109000|740000|137000|740000|138000|740000|158000|740000|}571|17|}}
{2010210004|XXGflow|11|0|21|0|{@{}@384|.5|.5|{12|678000|741000|698000|741000|702000|741000|702000|739000|707000|739000|739000|739000|}583|17|}}
{2010210004|XXGflow|12|0|23|0|{@{}@384|.5|.5|{8|314000|740000|334000|740000|337000|740000|357000|740000|}572|17|}}
{2010210004|XXGflow|13|0|25|0|{@{}@384|.5|.5|{12|502000|740000|522000|740000|528000|740000|528000|741000|534000|741000|554000|741000|}582|17|}}
{2010210004|XXGflow|14|0|27|0|{@{}@384|.5|.5|{8|279000|85000|305000|85000|382000|85000|402000|85000|}599|17|}}
{2010501005|XXGpvertex|15|0|29|0|{|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|condition|$CNDTL_EXTRACT_COMPRESS|3|6|k|@{0|}}
{30001002|XXparameter|compression|$CNDTL_EXTRACT_COMPRESS_LEVEL|3|6|k|@{0|}}
{30001002|XXparameter|!prototype_path|$AB_COMPONENTS/Translate/Compress/Deflate.mpc|3|2|Pf$|@{0|}}
{30001002|XXparameter|Layout||3|8|s=|@{0|}}
{30001002|XXparameter|in_metadata||3|8|s=|@{0|}}
}}@1|@736190|30426|756000|50000|122000|65000|584|Deflate-1|Ab Initio Software|Built-in 2.12.2.m1:|1|10|0||6||32769|1|{1|0|}}}
{2010203004|XXGoport|16|0|31|0|{@{}@877000|77000|11000|11000|out|0.0|@@@2320|0|}}
{2010202004|XXGiport|17|0|34|0|{@{}@746000|77000|11000|11000|in|0.0|@@@1808|0|}}
{2010501005|XXGpvertex|18|0|37|0|{Compresses data in a manner compatible with the zip and gzip deflate method. Use Inflate to uncompress the data.|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|mpname|broadcast|3|1|Hl||{0|}}
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
{2010203004|XXGoport|19|0|39|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|out|0.0|@@@2320|0|}}
{2010202004|XXGiport|20|0|42|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|in|0.0|@@@1808|0|}}
{2010503005|XXGfvertex|21|0|45|0|{|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|Layout|$[[record kind 67 subkind 87 parts [vector _interp_("$OUTPUT_FILE", "dollar_substitution")]]]|3|9||@{0|}}
{30001002|XXparameter|!prototype_path|$AB_COMPONENTS/Datasets/Output_File.mdc|3|2|Pf$|@{0|}}
{30001002|XXparameter|write_metadata||3|8|s=|@{0|}}
}}@1|@1181357|58924|1206000|79000|40000|40000|588|Normalized Output File-1|Kevin Oaks||1|10|0||6||32769|-1|-1|}}
{2010202004|XXGiport|22|0|47|0|{@{}@1196000|93000|11000|11000|write|0.0|@@@1776|0|}}
{2010503005|XXGfvertex|23|0|50|0|{Represents one file, many files, or a multifile as an output from your graph.|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|protection|0666|12|2|RF$||{0|}}
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
{2010202004|XXGiport|24|0|52|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|write|0.0|@@@1776|0|}}
{2010501005|XXGpvertex|25|0|56|0|{|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|transform0|$REFORMAT_XFR_FILE|3|2|f$|@{0|}}
{30001002|XXparameter|out0_metadata|$OUTPUT_DML|3|2|f$|@{0|}}
{30001002|XXparameter|error0_metadata|string('\\n')|3|1|l|@{0|}}
{30001002|XXparameter|log_metadata|record string("\|") node, timestamp, component, subcomponent, event_type; string("\|\\n") event_text; end|3|1|l|@{0|}}
{30001002|XXparameter|!prototype_path|$AB_COMPONENTS/Transform/Reformat.mpc|3|2|Pf$|@{0|}}
{30001002|XXparameter|Layout||3|8|s=|@{0|}}
{30001002|XXparameter|in_metadata||3|8|s=|@{0|}}
{30001002|XXparameter|reject0_metadata||3|8|s=|@{0|}}
}}@1|@392551|10706|413000|31000|224000|110000|602|Reformat data based on $DW_XFR/$ETL_ID.reformat.xfr|Ab Initio Software|Built-in|1|10|0||6||32769|1|{1|0|}}}
{2010203004|XXGoport|26|0|58|0|{@{}@636000|80000|11000|11000|out0|0.0|count|out|0|2448|0|}}
{2010203004|XXGoport|27|0|61|0|{@{}@0|0|0|0|reject0|0.0|count|reject|0|1168|0|}}
{2010203004|XXGoport|28|0|63|0|{@{}@0|0|0|0|error0|0.0|count|error|0|1176|0|}}
{2010203004|XXGoport|29|0|65|0|{@{}@519000|140000|11000|11000|log|0.0|@@@1176|0|}}
{2010202004|XXGiport|30|0|68|0|{@{}@403000|80000|11000|11000|in|0.0|@@@1808|0|}}
{2010501005|XXGpvertex|31|0|71|0|{Changes the record format of your data by dropping fields or by using DML expressions to add fields, combine fields, or modify the data.|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|mpname|reformat-transform|3|1|Hl||{0|}}
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
{30001002|XXparameter|log_metadata||7|1|RFl||{0|}}
{30001002|XXparameter|doc_transform||8|2|FHs$|Document your transformation for dependency analysis|{0|}}
{30001002|XXparameter|doc_operation1|out[*],reject[*],error[*],log::document(in)|3|2|RH$||{0|}}
{30001002|XXparameter|mpcmodtime|1299550340|1|1|Hl|The last modification time of this component's template|{0|}}
{30001002|XXparameter|_propagation|metadata type: error = remote("$AB_HOME/include/error-info.dml")
metadata type: reject = in
metadata type: log = remote("$AB_HOME/include/log-info.dml")|3|1|HKl|@{0|}}
{30001002|XXparameter|_propagate_through||3|1|FHKl|@{0|}}
}}@0|@0|0|0|0|0|0|0|@@@1|10|-1|@6|@1|1|{1|0|}}}
{2010203004|XXGoport|32|0|73|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
{30001002|XXparameter|transform0||8|8|FO=|Reformat transform|{0|}}
{30001002|XXparameter|transform0.index|count|3|9|P|@{0|}}
{30001002|XXparameter|transform0.index_value|0|3|9|P|@{0|}}
{30001002|XXparameter|transform0.index_name|transform|3|9|P|@{0|}}
}}@0|0|0|0|out0|0.0|count|out|0|2448|0|}}
{2010203004|XXGoport|33|0|77|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|F=||{0|}}
}}@0|0|0|0|reject0|0.0|count|reject|0|1168|0|}}
{2010203004|XXGoport|34|0|80|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|F=||{0|}}
}}@0|0|0|0|error0|0.0|count|error|0|1176|0|}}
{2010203004|XXGoport|35|0|83|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|log|0.0|@@@1176|0|}}
{2010202004|XXGiport|36|0|86|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|in|0.0|@@@1808|0|}}
{2010503005|XXGfvertex|37|0|90|0|{|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|Layout|$[[record kind 67 subkind 87 parts [vector _interp_("$INPUT_FILE", "dollar_substitution")]]]|3|9||@{0|}}
{30001002|XXparameter|read_metadata|$INPUT_DML|3|2|f$|@{0|}}
{30001002|XXparameter|!prototype_path|$AB_COMPONENTS/Datasets/Input_File.mdc|3|2|Pf$|@{0|}}
}}@1|@204249|45766|229000|66000|40000|40000|598|Input File-1|Kevin Oaks||1|10|0||6||32769|-1|-1|}}
{2010203004|XXGoport|38|0|92|0|{@{}@268000|80000|11000|11000|read|0.0|@@@2160|0|}}
{2010503005|XXGfvertex|39|0|95|0|{Represents one file, many files, or a multifile as an input to your graph.|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|protection|0666|12|2|RF$||{0|}}
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
{2010203004|XXGoport|40|0|97|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|read|0.0|@@@2160|0|}}
{2010501005|XXGpvertex|41|0|101|0|{|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|select_expr|string_index(line, "bytes) written") > 0|3|2|$|@{0|}}
{30001002|XXparameter|error_metadata|string('
')|3|1|l|@{0|}}
{30001002|XXparameter|log_metadata|record string("\|") node, timestamp, component, subcomponent, event_type; string("\|\\n") event_text; end|3|1|l|@{0|}}
{30001002|XXparameter|!prototype_path|$AB_COMPONENTS/Transform/Filter_by_Expression.mpc|3|2|Pf$|@{0|}}
{30001002|XXparameter|Layout||3|8|s=|@{0|}}
{30001002|XXparameter|in_metadata||3|8|s=|@{0|}}
{30001002|XXparameter|out_metadata||3|8|s=|@{0|}}
{30001002|XXparameter|deselect_metadata||3|8|s=|@{0|}}
{30001002|XXparameter|reject_metadata||3|8|s=|@{0|}}
}}@2|@149244|667993|169000|688000|135000|106000|585|Filter record count rows|Ab Initio Software|Built-in|1|10|0||6||32769|1|{1|0|}}}
{2010203004|XXGoport|42|0|103|0|{@{}@303000|735000|11000|11000|out|0.0|@@@2448|0|}}
{2010203004|XXGoport|43|0|106|0|{@{}@0|0|0|0|deselect|0.0|@@@2192|0|}}
{2010203004|XXGoport|44|0|108|0|{@{}@0|0|0|0|reject|0.0|@@@1168|0|}}
{2010203004|XXGoport|45|0|110|0|{@{}@0|0|0|0|error|0.0|@@@1176|0|}}
{2010203004|XXGoport|46|0|112|0|{@{}@0|0|0|0|log|0.0|@@@1176|0|}}
{2010202004|XXGiport|47|0|114|0|{@{}@159000|735000|11000|11000|in|0.0|@@@1808|0|}}
{2010501005|XXGpvertex|48|0|117|0|{Filters data records according to the specified DML expression.|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|mpname|select-transform|3|1|Hl||{0|}}
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
{30001002|XXparameter|Layout|@9|2|RFs$||{0|}}
{30001002|XXparameter|in_metadata||7|1|RFsl||{0|}}
{30001002|XXparameter|out_metadata||7|1|RFsl||{0|}}
{30001002|XXparameter|deselect_metadata||7|1|Fsl||{0|}}
{30001002|XXparameter|reject_metadata||7|1|Fsl||{0|}}
{30001002|XXparameter|error_metadata||7|1|Fl||{0|}}
{30001002|XXparameter|log_metadata||7|1|Fl||{0|}}
{30001002|XXparameter|doc_transform||8|2|FHs$|Document your transformation for dependency analysis|{0|}}
{30001002|XXparameter|doc_operation1|out,deselect,reject,error,log::document(in)|3|2|RH$||{0|}}
{30001002|XXparameter|mpcmodtime|1299550340|1|1|Hl|The last modification time of this component's template|{0|}}
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
{2010203004|XXGoport|49|0|119|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|out|0.0|@@@2448|0|}}
{2010203004|XXGoport|50|0|122|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|F=||{0|}}
}}@0|0|0|0|deselect|0.0|@@@2192|0|}}
{2010203004|XXGoport|51|0|125|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|F=||{0|}}
}}@0|0|0|0|reject|0.0|@@@1168|0|}}
{2010203004|XXGoport|52|0|128|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|F=||{0|}}
}}@0|0|0|0|error|0.0|@@@1176|0|}}
{2010203004|XXGoport|53|0|131|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|F=||{0|}}
}}@0|0|0|0|log|0.0|@@@1176|0|}}
{2010202004|XXGiport|54|0|134|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|in|0.0|@@@1808|0|}}
{2010503005|XXGfvertex|55|0|137|0|{|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|Layout|$[[record kind 85 subkind 0 parts [vector _interp_("file:$RECORD_COUNT_FILE", "dollar_substitution")]]]|3|9||@{0|}}
{30001002|XXparameter|!prototype_path|$AB_COMPONENTS/Datasets/Output_File.mdc|3|2|Pf$|@{0|}}
{30001002|XXparameter|write_metadata||3|8|s=|@{0|}}
}}@2|@724976|699805|750000|720000|40000|40000|590|Record Count File|eBay||1|10|0||6||32769|-1|-1|}}
{2010202004|XXGiport|56|0|139|0|{@{}@740000|734000|11000|11000|write|0.0|@@@1776|0|}}
{2010503005|XXGfvertex|57|0|142|0|{Represents one file, many files, or a multifile as an output from your graph.|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|protection|0666|12|2|RF$||{0|}}
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
{2010202004|XXGiport|58|0|144|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|write|0.0|@@@1776|0|}}
{2010501005|XXGpvertex|59|0|147|0|{|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|key|\{\}|3|2|$|@{0|}}
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
}}@2|@545151|689160|565000|709000|103000|65000|593|Rollup|Ab Initio Software|Built-in 2.0:|1|10|0||6||32769|1|{1|0|}}}
{2010203004|XXGoport|60|0|149|0|{@{}@667000|736000|11000|11000|out|0.0|@@@2448|0|}}
{2010203004|XXGoport|61|0|152|0|{@{}@0|0|0|0|reject|0.0|@@@1168|0|}}
{2010203004|XXGoport|62|0|154|0|{@{}@0|0|0|0|error|0.0|@@@1176|0|}}
{2010203004|XXGoport|63|0|156|0|{@{}@0|0|0|0|log|0.0|@@@1176|0|}}
{2010202004|XXGiport|64|0|158|0|{@{}@555000|736000|11000|11000|in|0.0|@@@1808|0|}}
{2010501005|XXGpvertex|65|0|161|0|{Generates data records that summarize groups of data records.  Rollup in memory maximizes performance by keeping intermediate results in main memory.  Rollup sorted requires grouped input.|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|mpname|?rollup|3|1|Hl||{0|}}
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
{2010203004|XXGoport|66|0|163|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|out|0.0|@@@2448|0|}}
{2010203004|XXGoport|67|0|166|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|F=||{0|}}
}}@0|0|0|0|reject|0.0|@@@1168|0|}}
{2010203004|XXGoport|68|0|169|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|F=||{0|}}
}}@0|0|0|0|error|0.0|@@@1176|0|}}
{2010203004|XXGoport|69|0|172|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|F=||{0|}}
}}@0|0|0|0|log|0.0|@@@1176|0|}}
{2010202004|XXGiport|70|0|175|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|in|0.0|@@@1808|0|}}
{2010501005|XXGpvertex|71|0|179|0|{|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|out_metadata|record
  decimal(" records") rec_cnt = 0;
  string("\\n") junk = NULL;
end;|3|1|l|@{0|}}
{30001002|XXparameter|!prototype_path|$AB_COMPONENTS/Miscellaneous/Redefine_Format.mpc|3|2|Pf$|@{0|}}
{30001002|XXparameter|Layout||3|8|s=|@{0|}}
{30001002|XXparameter|in_metadata||3|8|s=|@{0|}}
}}@2|@348345|687255|368000|707000|124000|68000|591|Redefine Format-1|Ab Initio Software|Built-in|1|10|0||6||32769|1|{1|0|}}}
{2010203004|XXGoport|72|0|181|0|{@{}@491000|735000|11000|11000|out|0.0|@@@2448|0|}}
{2010202004|XXGiport|73|0|184|0|{@{}@358000|735000|11000|11000|in|0.0|@@@1936|0|}}
{2010501005|XXGpvertex|74|0|187|0|{Copies data records from its input to its output without changing the values.  Use Redefine Format to change a record format or rename fields.|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|mpname|copy|3|1|Hl||{0|}}
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
{2010203004|XXGoport|75|0|189|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|out|0.0|@@@2448|0|}}
{2010202004|XXGiport|76|0|192|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|in|0.0|@@@1936|0|}}
{2010503005|XXGfvertex|77|0|196|0|{|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|Layout|$[[record kind 85 subkind 0 parts [vector _interp_("file:$LOG_FILE", "dollar_substitution")]]]|3|9||@{0|}}
{30001002|XXparameter|read_metadata|record
  string("\\n") line;
end;|3|1|l|@{0|}}
{30001002|XXparameter|!prototype_path|$AB_COMPONENTS/Datasets/Input_File.mdc|3|2|Pf$|@{0|}}
}}@2|@34310|700988|59000|721000|40000|40000|589|Output log file|eBay||1|10|0||6||32769|-1|-1|}}
{2010203004|XXGoport|78|0|198|0|{@{}@98000|735000|11000|11000|read|0.0|@@@2160|0|}}
{2010503005|XXGfvertex|79|0|201|0|{Represents one file, many files, or a multifile as an input to your graph.|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|protection|0666|12|2|RF$||{0|}}
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
{2010203004|XXGoport|80|0|203|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|read|0.0|@@@2160|0|}}
{2010501005|XXGpvertex|81|0|211|0|{|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|LogFile|$[[record kind 85 subkind 0 parts [vector _interp_("file:$LOG_FILE", "dollar_substitution")]]]|3|9||@{0|}}
{30001002|XXparameter|!prototype_path|$AB_COMPONENTS/Miscellaneous/Gather_Logs.mpc|3|2|Pf$|@{0|}}
{30001002|XXparameter|in_metadata||3|8|s=|@{0|}}
}}@1|@816281|186389|836000|206000|99000|78000|597|Gather Logs-1|Ab Initio Software|Built-in|1|10|0||6||32769|1|{1|0|}}}
{2010202004|XXGiport|82|0|213|0|{@{}@880000|196000|11000|11000|in|0.0|@@@786|0|}}
{2010501005|XXGpvertex|83|0|216|0|{Collects the output from log ports of components for analysis of a graph after execution.|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|mpname|logger|3|1|Hl||{0|}}
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
{2010202004|XXGiport|84|0|218|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|in|0.0|@@@786|0|}}
{2010501005|XXGpvertex|85|0|223|0|{|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|commandline|if [[ ! -d $LOG_FILE_DIR ]]
then
   set +e
   mkdir -pm 0775 $LOG_FILE_DIR
   set -e
fi|3|2|$|@{0|}}
{30001002|XXparameter|Layout|$[[record kind 85 subkind 0 parts [vector _interp_("1", "dollar_substitution")]]]|3|9||@{0|}}
{30001002|XXparameter|!prototype_path|$AB_COMPONENTS/Miscellaneous/Run_Program.mpc|3|2|Pf$|@{0|}}
}}@0|@20652|31530|41000|52000|114000|87000|603|Make Log Dir|Ab Initio Software|Built-in|1|10|0||6||32769|1|{1|0|}}}
{2010203004|XXGoport|86|0|225|0|{@{}@154000|90000|11000|11000|out|0.0|@@@2260|0|}}
{2010202004|XXGiport|87|0|227|0|{@{}@31000|90000|11000|11000|in|0.0|@@@1732|0|}}
{2010501005|XXGpvertex|88|0|229|0|{Executes a standard UNIX or Windows program.|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|mpname|filter|3|1|Hl||{0|}}
{30001002|XXparameter|image__|unitool|3|2|H$||{0|}}
{30001002|XXparameter|commandline||16|2|RFO$|Command line of UNIX or Windows program to execute|{0|}}
{30001002|XXparameter|NoHidePorts|in out|3|2|H$||{0|}}
{30001002|XXparameter|condition||3|2|F$||{0|}}
{30001002|XXparameter|conditionInputPort|in|3|2|F$||{0|}}
{30001002|XXparameter|conditionOutputPort|out|3|2|F$||{0|}}
{30001002|XXparameter|condition_interpretation|Replace with flow|15|1|Fl||{2|Replace with flow|Remove completely|}}
{30001002|XXparameter|condition_interpretation.display_name|condition-interpretation|3|9|P|@{0|}}
{30001002|XXparameter|port_analysis|out=all_to_all(in)|3|2|H$||{0|}}
{30001002|XXparameter|in_metadata||7|1|Fsl||{0|}}
{30001002|XXparameter|out_metadata||7|1|Fsl||{0|}}
{30001002|XXparameter|doc_transform||8|2|FHs$|Document your transformation for dependency analysis|{0|}}
{30001002|XXparameter|doc_operation1|out::document(in)|3|2|RH$||{0|}}
{30001002|XXparameter|mpcmodtime|1299550339|1|1|Hl|The last modification time of this component's template|{0|}}
{30001002|XXparameter|Layout|@9|2|RF$||{0|}}
{30001002|XXparameter|_propagate_through||3|1|FHKl|@{0|}}
}}@0|@0|0|0|0|0|0|0|@@@1|10|-1|@6|@1|1|{1|0|}}}
{2010203004|XXGoport|89|0|231|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|F=||{0|}}
}}@0|0|0|0|out|0.0|@@@2260|0|}}
{2010202004|XXGiport|90|0|234|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|F=||{0|}}
}}@0|0|0|0|in|0.0|@@@1732|0|}}
{2010501005|XXGpvertex|91|0|243|0|{|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|commandline|touch $WATCH_FILE|3|2|$|@{0|}}
{30001002|XXparameter|Layout|$[[record kind 85 subkind 0 parts [vector _interp_("file:.", "dollar_substitution")]]]|3|9||@{0|}}
{30001002|XXparameter|!prototype_path|$AB_COMPONENTS/Miscellaneous/Run_Program.mpc|3|2|Pf$|@{0|}}
}}@3|@842291|676497|862000|696000|123000|87000|594|Touch Watch File|Ab Initio Software|Built-in|1|10|0||6||32769|1|{1|0|}}}
{2010203004|XXGoport|92|0|245|0|{@{}@984000|734000|11000|11000|out|0.0|@@@2260|0|}}
{2010202004|XXGiport|93|0|247|0|{@{}@852000|734000|11000|11000|in|0.0|@@@1732|0|}}
{2010501005|XXGpvertex|94|0|249|0|{Executes a standard UNIX or Windows program.|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|mpname|filter|3|1|Hl||{0|}}
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
{2010203004|XXGoport|95|0|251|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|F=||{0|}}
}}@0|0|0|0|out|0.0|@@@2260|0|}}
{2010202004|XXGiport|96|0|254|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|F=||{0|}}
}}@0|0|0|0|in|0.0|@@@1732|0|}}
{2010110002|XXGconstant|97|0|257|0|{@{}@{1|978304|}}}
{2010002001|XXGdirectory_object_object|0|1|2|0|{XXGgraph|}0|1|}
{2010704001|XXGgraph_graphinfo_graphinfo|1|0|2|0|{}1|2|}
{2010706001|XXGgraphinfo_runsettings_runsettings|2|0|4|0|{}2|3|}
{2010109001|XXGobject_property_value|3|0|6|0|{0|legend_face|92|}2|4|}
{2010605001|XXGgraph_legend_legend|4|0|8|0|{0|}1|5|}
{2010241001|XXGlegend_face_face|5|0|10|0|{}5|6|}
{2010604001|XXGgraph_flow_flow|6|0|12|0|{Flow_10|}1|7|}
{2010604001|XXGgraph_flow_flow|7|0|14|0|{Flow_11|}1|8|}
{2010604001|XXGgraph_flow_flow|8|0|16|0|{Flow_12|}1|9|}
{2010604001|XXGgraph_flow_flow|9|0|18|0|{Flow_20|}1|10|}
{2010604001|XXGgraph_flow_flow|10|0|20|0|{Flow_23|}1|11|}
{2010604001|XXGgraph_flow_flow|11|0|22|0|{Flow_6|}1|12|}
{2010604001|XXGgraph_flow_flow|12|0|24|0|{Flow_7|}1|13|}
{2010604001|XXGgraph_flow_flow|13|0|26|0|{Flow_9|}1|14|}
{2010601001|XXGgraph_vertex_vertex|14|0|28|0|{Deflate_1|}1|15|}
{2010212001|XXGvertex_oport_oport|15|0|30|0|{0|out|}15|16|}
{2010213001|XXGoport_dst_flow|16|0|32|0|{0|}16|8|}
{2010211001|XXGvertex_iport_iport|17|0|33|0|{0|in|}15|17|}
{2010214001|XXGiport_src_flow|18|0|35|0|{0|}17|7|}
{2010007001|XXGobject_proto_object|19|0|36|0|{}15|18|}
{2010212001|XXGvertex_oport_oport|20|0|38|0|{0|out|}18|19|}
{2010006001|XXGobject_psameas_object|21|0|40|0|{metadata|out_metadata|1|0|}19|18|}
{2010211001|XXGvertex_iport_iport|22|0|41|0|{0|in|}18|20|}
{2010006001|XXGobject_psameas_object|23|0|43|0|{metadata|in_metadata|1|0|}20|18|}
{2010006001|XXGobject_psameas_object|24|0|44|0|{Layout|Layout|0|0|}15|21|}
{2010211001|XXGvertex_iport_iport|25|0|46|0|{0|write|}21|22|}
{2010214001|XXGiport_src_flow|26|0|48|0|{0|}22|8|}
{2010007001|XXGobject_proto_object|27|0|49|0|{}21|23|}
{2010211001|XXGvertex_iport_iport|28|0|51|0|{0|write|}23|24|}
{2010006001|XXGobject_psameas_object|29|0|53|0|{metadata|write_metadata|1|0|}24|23|}
{2010006001|XXGobject_psameas_object|30|0|54|0|{write_metadata|out_metadata|0|0|}21|15|}
{2010006001|XXGobject_psameas_object|31|0|55|0|{in_metadata|out0_metadata|0|0|}15|25|}
{2010212001|XXGvertex_oport_oport|32|0|57|0|{0|out0|}25|26|}
{2010213001|XXGoport_dst_flow|33|0|59|0|{0|}26|7|}
{2010212001|XXGvertex_oport_oport|34|0|60|0|{1|reject0|}25|27|}
{2010212001|XXGvertex_oport_oport|35|0|62|0|{2|error0|}25|28|}
{2010212001|XXGvertex_oport_oport|36|0|64|0|{3|log|}25|29|}
{2010213001|XXGoport_dst_flow|37|0|66|0|{0|}29|9|}
{2010211001|XXGvertex_iport_iport|38|0|67|0|{0|in|}25|30|}
{2010214001|XXGiport_src_flow|39|0|69|0|{0|}30|14|}
{2010007001|XXGobject_proto_object|40|0|70|0|{}25|31|}
{2010212001|XXGvertex_oport_oport|41|0|72|0|{0|out0|}31|32|}
{2010006001|XXGobject_psameas_object|42|0|74|0|{metadata|out0_metadata|1|0|}32|31|}
{2010006001|XXGobject_psameas_object|43|0|75|0|{transform0|transform0|1|0|}32|31|}
{2010212001|XXGvertex_oport_oport|44|0|76|0|{1|reject0|}31|33|}
{2010006001|XXGobject_psameas_object|45|0|78|0|{metadata|reject0_metadata|1|0|}33|31|}
{2010212001|XXGvertex_oport_oport|46|0|79|0|{2|error0|}31|34|}
{2010006001|XXGobject_psameas_object|47|0|81|0|{metadata|error0_metadata|1|0|}34|31|}
{2010212001|XXGvertex_oport_oport|48|0|82|0|{3|log|}31|35|}
{2010006001|XXGobject_psameas_object|49|0|84|0|{metadata|log_metadata|1|0|}35|31|}
{2010211001|XXGvertex_iport_iport|50|0|85|0|{0|in|}31|36|}
{2010006001|XXGobject_psameas_object|51|0|87|0|{metadata|in_metadata|1|0|}36|31|}
{2010006001|XXGobject_psameas_object|52|0|88|0|{Layout|Layout|0|0|}25|21|}
{2010006001|XXGobject_psameas_object|53|0|89|0|{in_metadata|read_metadata|0|0|}25|37|}
{2010212001|XXGvertex_oport_oport|54|0|91|0|{0|read|}37|38|}
{2010213001|XXGoport_dst_flow|55|0|93|0|{0|}38|14|}
{2010007001|XXGobject_proto_object|56|0|94|0|{}37|39|}
{2010212001|XXGvertex_oport_oport|57|0|96|0|{0|read|}39|40|}
{2010006001|XXGobject_psameas_object|58|0|98|0|{metadata|read_metadata|1|0|}40|39|}
{2010006001|XXGobject_psameas_object|59|0|99|0|{reject0_metadata|read_metadata|0|0|}25|37|}
{2010601001|XXGgraph_vertex_vertex|60|0|100|0|{Filter_record_count_rows|}1|41|}
{2010212001|XXGvertex_oport_oport|61|0|102|0|{0|out|}41|42|}
{2010213001|XXGoport_dst_flow|62|0|104|0|{0|}42|12|}
{2010212001|XXGvertex_oport_oport|63|0|105|0|{1|deselect|}41|43|}
{2010212001|XXGvertex_oport_oport|64|0|107|0|{2|reject|}41|44|}
{2010212001|XXGvertex_oport_oport|65|0|109|0|{3|error|}41|45|}
{2010212001|XXGvertex_oport_oport|66|0|111|0|{4|log|}41|46|}
{2010211001|XXGvertex_iport_iport|67|0|113|0|{0|in|}41|47|}
{2010214001|XXGiport_src_flow|68|0|115|0|{0|}47|10|}
{2010007001|XXGobject_proto_object|69|0|116|0|{}41|48|}
{2010212001|XXGvertex_oport_oport|70|0|118|0|{0|out|}48|49|}
{2010006001|XXGobject_psameas_object|71|0|120|0|{metadata|out_metadata|1|0|}49|48|}
{2010212001|XXGvertex_oport_oport|72|0|121|0|{1|deselect|}48|50|}
{2010006001|XXGobject_psameas_object|73|0|123|0|{metadata|deselect_metadata|1|0|}50|48|}
{2010212001|XXGvertex_oport_oport|74|0|124|0|{2|reject|}48|51|}
{2010006001|XXGobject_psameas_object|75|0|126|0|{metadata|reject_metadata|1|0|}51|48|}
{2010212001|XXGvertex_oport_oport|76|0|127|0|{3|error|}48|52|}
{2010006001|XXGobject_psameas_object|77|0|129|0|{metadata|error_metadata|1|0|}52|48|}
{2010212001|XXGvertex_oport_oport|78|0|130|0|{4|log|}48|53|}
{2010006001|XXGobject_psameas_object|79|0|132|0|{metadata|log_metadata|1|0|}53|48|}
{2010211001|XXGvertex_iport_iport|80|0|133|0|{0|in|}48|54|}
{2010006001|XXGobject_psameas_object|81|0|135|0|{metadata|in_metadata|1|0|}54|48|}
{2010006001|XXGobject_psameas_object|82|0|136|0|{Layout|Layout|0|0|}41|55|}
{2010211001|XXGvertex_iport_iport|83|0|138|0|{0|write|}55|56|}
{2010214001|XXGiport_src_flow|84|0|140|0|{0|}56|11|}
{2010007001|XXGobject_proto_object|85|0|141|0|{}55|57|}
{2010211001|XXGvertex_iport_iport|86|0|143|0|{0|write|}57|58|}
{2010006001|XXGobject_psameas_object|87|0|145|0|{metadata|write_metadata|1|0|}58|57|}
{2010006001|XXGobject_psameas_object|88|0|146|0|{write_metadata|out_metadata|0|0|}55|59|}
{2010212001|XXGvertex_oport_oport|89|0|148|0|{0|out|}59|60|}
{2010213001|XXGoport_dst_flow|90|0|150|0|{0|}60|11|}
{2010212001|XXGvertex_oport_oport|91|0|151|0|{1|reject|}59|61|}
{2010212001|XXGvertex_oport_oport|92|0|153|0|{2|error|}59|62|}
{2010212001|XXGvertex_oport_oport|93|0|155|0|{3|log|}59|63|}
{2010211001|XXGvertex_iport_iport|94|0|157|0|{0|in|}59|64|}
{2010214001|XXGiport_src_flow|95|0|159|0|{0|}64|13|}
{2010007001|XXGobject_proto_object|96|0|160|0|{}59|65|}
{2010212001|XXGvertex_oport_oport|97|0|162|0|{0|out|}65|66|}
{2010006001|XXGobject_psameas_object|98|0|164|0|{metadata|out_metadata|1|0|}66|65|}
{2010212001|XXGvertex_oport_oport|99|0|165|0|{1|reject|}65|67|}
{2010006001|XXGobject_psameas_object|100|0|167|0|{metadata|reject_metadata|1|0|}67|65|}
{2010212001|XXGvertex_oport_oport|101|0|168|0|{2|error|}65|68|}
{2010006001|XXGobject_psameas_object|102|0|170|0|{metadata|error_metadata|1|0|}68|65|}
{2010212001|XXGvertex_oport_oport|103|0|171|0|{3|log|}65|69|}
{2010006001|XXGobject_psameas_object|104|0|173|0|{metadata|log_metadata|1|0|}69|65|}
{2010211001|XXGvertex_iport_iport|105|0|174|0|{0|in|}65|70|}
{2010006001|XXGobject_psameas_object|106|0|176|0|{metadata|in_metadata|1|0|}70|65|}
{2010006001|XXGobject_psameas_object|107|0|177|0|{Layout|Layout|0|0|}59|55|}
{2010006001|XXGobject_psameas_object|108|0|178|0|{in_metadata|out_metadata|0|0|}59|71|}
{2010212001|XXGvertex_oport_oport|109|0|180|0|{0|out|}71|72|}
{2010213001|XXGoport_dst_flow|110|0|182|0|{0|}72|13|}
{2010211001|XXGvertex_iport_iport|111|0|183|0|{0|in|}71|73|}
{2010214001|XXGiport_src_flow|112|0|185|0|{0|}73|12|}
{2010007001|XXGobject_proto_object|113|0|186|0|{}71|74|}
{2010212001|XXGvertex_oport_oport|114|0|188|0|{0|out|}74|75|}
{2010006001|XXGobject_psameas_object|115|0|190|0|{metadata|out_metadata|1|0|}75|74|}
{2010211001|XXGvertex_iport_iport|116|0|191|0|{0|in|}74|76|}
{2010006001|XXGobject_psameas_object|117|0|193|0|{metadata|in_metadata|1|0|}76|74|}
{2010006001|XXGobject_psameas_object|118|0|194|0|{Layout|Layout|0|0|}71|55|}
{2010006001|XXGobject_psameas_object|119|0|195|0|{in_metadata|read_metadata|0|0|}71|77|}
{2010212001|XXGvertex_oport_oport|120|0|197|0|{0|read|}77|78|}
{2010213001|XXGoport_dst_flow|121|0|199|0|{0|}78|10|}
{2010007001|XXGobject_proto_object|122|0|200|0|{}77|79|}
{2010212001|XXGvertex_oport_oport|123|0|202|0|{0|read|}79|80|}
{2010006001|XXGobject_psameas_object|124|0|204|0|{metadata|read_metadata|1|0|}80|79|}
{2010006001|XXGobject_psameas_object|125|0|205|0|{reject_metadata|out_metadata|0|0|}59|71|}
{2010006001|XXGobject_psameas_object|126|0|206|0|{deselect_metadata|read_metadata|0|0|}41|77|}
{2010006001|XXGobject_psameas_object|127|0|207|0|{in_metadata|read_metadata|0|0|}41|77|}
{2010006001|XXGobject_psameas_object|128|0|208|0|{out_metadata|read_metadata|0|0|}41|77|}
{2010006001|XXGobject_psameas_object|129|0|209|0|{reject_metadata|read_metadata|0|0|}41|77|}
{2010601001|XXGgraph_vertex_vertex|130|0|210|0|{Gather_Logs_1|}1|81|}
{2010211001|XXGvertex_iport_iport|131|0|212|0|{0|in|}81|82|}
{2010214001|XXGiport_src_flow|132|0|214|0|{0|}82|9|}
{2010007001|XXGobject_proto_object|133|0|215|0|{}81|83|}
{2010211001|XXGvertex_iport_iport|134|0|217|0|{0|in|}83|84|}
{2010006001|XXGobject_psameas_object|135|0|219|0|{metadata|in_metadata|1|0|}84|83|}
{2010006001|XXGobject_psameas_object|136|0|220|0|{in_metadata|log_metadata|0|0|}81|25|}
{2010601001|XXGgraph_vertex_vertex|137|0|221|0|{Input_File_1|}1|37|}
{2010601001|XXGgraph_vertex_vertex|138|0|222|0|{Make_Log_Dir|}1|85|}
{2010212001|XXGvertex_oport_oport|139|0|224|0|{0|out|}85|86|}
{2010211001|XXGvertex_iport_iport|140|0|226|0|{0|in|}85|87|}
{2010007001|XXGobject_proto_object|141|0|228|0|{}85|88|}
{2010212001|XXGvertex_oport_oport|142|0|230|0|{0|out|}88|89|}
{2010006001|XXGobject_psameas_object|143|0|232|0|{metadata|out_metadata|1|0|}89|88|}
{2010211001|XXGvertex_iport_iport|144|0|233|0|{0|in|}88|90|}
{2010006001|XXGobject_psameas_object|145|0|235|0|{metadata|in_metadata|1|0|}90|88|}
{2010601001|XXGgraph_vertex_vertex|146|0|236|0|{Normalized_Output_File_1|}1|21|}
{2010601001|XXGgraph_vertex_vertex|147|0|237|0|{Output_log_file|}1|77|}
{2010601001|XXGgraph_vertex_vertex|148|0|238|0|{Record_Count_File|}1|55|}
{2010601001|XXGgraph_vertex_vertex|149|0|239|0|{Redefine_Format_1|}1|71|}
{2010601001|XXGgraph_vertex_vertex|150|0|240|0|{Reformat_data_based_on_DW_XFR_ETL_ID_reformat_xfr|}1|25|}
{2010601001|XXGgraph_vertex_vertex|151|0|241|0|{Rollup|}1|59|}
{2010601001|XXGgraph_vertex_vertex|152|0|242|0|{Touch_Watch_File|}1|91|}
{2010212001|XXGvertex_oport_oport|153|0|244|0|{0|out|}91|92|}
{2010211001|XXGvertex_iport_iport|154|0|246|0|{0|in|}91|93|}
{2010007001|XXGobject_proto_object|155|0|248|0|{}91|94|}
{2010212001|XXGvertex_oport_oport|156|0|250|0|{0|out|}94|95|}
{2010006001|XXGobject_psameas_object|157|0|252|0|{metadata|out_metadata|1|0|}95|94|}
{2010211001|XXGvertex_iport_iport|158|0|253|0|{0|in|}94|96|}
{2010006001|XXGobject_psameas_object|159|0|255|0|{metadata|in_metadata|1|0|}96|94|}
{2010109001|XXGobject_property_value|160|0|256|0|{0|TrackingThumbprint|72|}1|97|}
