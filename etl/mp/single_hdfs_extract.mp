@
{18|Type|XXGrepository|Primary_Name|XXGgraph|_ab_object_schema|0|_ab_semantic_schema|1|eme_flatten_format|1|schema_version|34|root_id|0|ent_count|173|rel_count|304|}
{2010001002|XXGdirectory|0|1|1|0|{@{}@}}
{2010600005|XXGgraph|1|0|1|0|{|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|ETL_ID||3|2|REFO$|subject_area.table_id|{0|}}
{30001002|XXparameter|FILE_ID|1|3|2|REFO$|file id retrieved from the sources list file|{0|}}
{30001002|XXparameter|JOB_CONFIG||3|2|REFO$|database configuration file retrieved from the sources list file|{0|}}
{30001002|XXparameter|SOURCE_NAME_NOT_USED||3|2|REFO$|Not used. Making positional arg consistent across all extraction.|{0|}}
{30001002|XXparameter|DATA_FILENAME_TMP|$ETL_ID.$FILE_ID.dat|3|2|REFO$|data filename retrieved from the sources list file|{0|}}
{30001002|XXparameter|DML_FILENAME|$ETL_ID.read.dml|3|2|EFK$|optional dml file name - used to override the default dml file|{0|}}
{30001002|XXparameter|UOW_FROM||3|2|EFK$|Unit of Work|{0|}}
{30001002|XXparameter|UOW_TO||3|2|EFK$|Next Unit of Work|{0|}}
{30001002|XXparameter|INPUT_FILE|$(eval print $SOURCE_NAME_NOT_USED)|3|6|REk||{0|}}
{30001002|XXparameter|JOB_ENV|extract|3|2|RE$||{0|}}
{30001002|XXparameter|UOW_DATE|$(print $UOW_TO \| cut -c1-8)|3|2|RE$||{0|}}
{30001002|XXparameter|ETL_CFG_FILE|$DW_CFG/$ETL_ID.cfg|3|2|RE$||{0|}}
{30001002|XXparameter|SUBJECT_AREA|$\{ETL_ID%%.*\}|3|6|REk||{0|}}
{30001002|XXparameter|TABLE_ID|$\{ETL_ID##*.\}|3|6|REk||{0|}}
{30001002|XXparameter|AB_JOB|$(if [ $ETL_ENV ]
then
   print $AB_JOB.$ETL_ID.$FILE_ID.$ETL_ENV.$JOB_ENV
else
   print $AB_JOB.$ETL_ID.$FILE_ID.$JOB_ENV
fi)|3|6|REk||{0|}}
{30001002|XXparameter|DW_SA_ARC|$DW_ARC/$JOB_ENV/$SUBJECT_AREA|3|2|RE$||{0|}}
{30001002|XXparameter|DW_SA_DAT|$DW_DAT/$JOB_ENV/$SUBJECT_AREA|3|2|RE$||{0|}}
{30001002|XXparameter|DW_SA_LOG|$\{DW_SA_LOG:-$DW_LOG/$JOB_ENV/$SUBJECT_AREA\}|3|6|REk||{0|}}
{30001002|XXparameter|DW_SA_TMP|$DW_TMP/$JOB_ENV/$SUBJECT_AREA|3|2|RE$||{0|}}
{30001002|XXparameter|FILE_DATETIME|$\{CURR_DATETIME:-$(date '+%Y%m%d-%H%M%S')\}|3|6|REk||{0|}}
{30001002|XXparameter|BATCH_SEQ_NUM|$(( $(<$DW_SA_DAT/$TABLE_ID.extract.batch_seq_num.dat) + 1))|3|6|REk||{0|}}
{30001002|XXparameter|CNDTL_EXTRACT_COMPRESS_LEVEL|$(grep "^CNDTL_EXTRACT_COMPRESS_LEVEL\\>" $ETL_CFG_FILE \| read PARAM VALUE COMMENT; print $\{VALUE:-""\})|1|6|Kk|fILE COMPRESSION LEVEL (0-9)|{0|}}
{30001002|XXparameter|CNDTL_EXTRACT_COMPRESS|$(grep "^CNDTL_EXTRACT_COMPRESS\\>" $ETL_CFG_FILE \| read PARAM VALUE COMMENT;  print $\{VALUE:-0\})|13|6|Kk||{0|}}
{30001002|XXparameter|DATA_FILENAME|$(if [[ -n $UOW_TO ]]
  then
     if [[ $DATA_FILENAME_TMP = "N" ]]
     then
        print $(grep "^DATA_FILENAME\\>" $ETL_CFG_FILE \| read PARAM VALUE COMMENT; eval print $VALUE)
     else
        eval print $DATA_FILENAME_TMP
     fi
  else
     if [[ $DATA_FILENAME_TMP = "N" ]]
     then
        print $(grep "^DATA_FILENAME\\>" $ETL_CFG_FILE \| read PARAM VALUE COMMENT; eval print $VALUE.$BATCH_SEQ_NUM)
     else
        eval print $DATA_FILENAME_TMP.$BATCH_SEQ_NUM
     fi
  fi
)|3|6|REk||{0|}}
{30001002|XXparameter|IN_DIR|$\{IN_DIR:-$(grep "^IN_DIR\\>" $ETL_CFG_FILE \| read PARAM VALUE COMMENT; eval print $VALUE/$JOB_ENV/$SUBJECT_AREA)\}|3|6|REk||{0|}}
{30001002|XXparameter|WRK_DIR|$(grep "^WRK_DIR\\>" $ETL_CFG_FILE \| read PARAM VALUE COMMENT; eval print $VALUE/$SUBJECT_AREA)|3|6|REk||{0|}}
{30001002|XXparameter|LAST_EXTRACT_TYPE|$(grep "^LAST_EXTRACT_TYPE\\>" $ETL_CFG_FILE \| read PARAM VALUE COMMENT; print $VALUE)|3|6|REk||{0|}}
{30001002|XXparameter|CNDTL_LAST_EXTRACT_ROLLUP|$(if [[ $LAST_EXTRACT_TYPE = "R" ]]; then print 1; else print 0; fi)|3|6|REk||{0|}}
{30001002|XXparameter|CNDTL_LAST_EXTRACT_VARIABLE|$(if [[ $LAST_EXTRACT_TYPE = "V" ]]; then print 1; else print 0; fi)|3|6|REk||{0|}}
{30001002|XXparameter|LAST_EXTRACT_VALUE_FILE|$(if [[ $LAST_EXTRACT_TYPE != \@("N"\|"U") ]]
  then
     print $DW_SA_DAT/$TABLE_ID.$FILE_ID.last_extract_value.dat
  else
     print a
  fi
)|3|6|REk||{0|}}
{30001002|XXparameter|UOW_FROM_REFORMAT_CODE|$(if [[ $LAST_EXTRACT_TYPE == "U" ]]
  then
   grep "^UOW_FROM_REFORMAT_CODE\\>" $ETL_CFG_FILE \| read PARAM VALUE COMMENT; eval print $\{VALUE:-0\}
  fi
)|3|6|Ek||{0|}}
{30001002|XXparameter|UOW_TO_REFORMAT_CODE|$(if [[ $LAST_EXTRACT_TYPE == "U" ]]
  then
   grep "^UOW_TO_REFORMAT_CODE\\>" $ETL_CFG_FILE \| read PARAM VALUE COMMENT; eval print $\{VALUE:-0\}
  fi
)|3|6|Ek||{0|}}
{30001002|XXparameter|FROM_EXTRACT_VALUE|$(if [[ $LAST_EXTRACT_TYPE == \@("V"\|"R") ]]
  then
     print $(<$LAST_EXTRACT_VALUE_FILE)
  elif [[ $LAST_EXTRACT_TYPE == "U" ]]
  then
     print $(eval $DW_MASTER_EXE/dw_infra.reformat_timestamp.ksh $UOW_FROM $UOW_FROM_REFORMAT_CODE)
  fi
)|3|6|REk||{0|}}
{30001002|XXparameter|TO_EXTRACT_VALUE_FUNCTION|$(if [[ $LAST_EXTRACT_TYPE = "V" ]]
  then
     print $(grep "^TO_EXTRACT_VALUE_FUNCTION\\>" $ETL_CFG_FILE \| read PARAM VALUE COMMENT; eval print $VALUE)
  fi
)|3|6|REk||{0|}}
{30001002|XXparameter|TO_EXTRACT_VALUE|$(if [[ $LAST_EXTRACT_TYPE == "V" ]]
  then
     print $($TO_EXTRACT_VALUE_FUNCTION)
  elif [[ $LAST_EXTRACT_TYPE == "U" ]]
  then
     print $(eval $DW_MASTER_EXE/dw_infra.reformat_timestamp.ksh $UOW_TO $UOW_TO_REFORMAT_CODE)
  fi
)|3|6|REk||{0|}}
{30001002|XXparameter|INPUT_DML|$DW_DML/$DML_FILENAME|3|2|RE$||{0|}}
{30001002|XXparameter|UOW_APPEND|$(if [[ $LAST_EXTRACT_TYPE == "U" ]]
  then
    print ".$UOW_TO"
  else
    print ""
  fi
 )|3|6|REk||{0|}}
{30001002|XXparameter|OUTPUT_FILE|$(if [ $CNDTL_EXTRACT_COMPRESS != 1 ]
  then
    eval print $IN_DIR/$DATA_FILENAME
  else
    eval print $IN_DIR/$DATA_FILENAME.gz
  fi)|3|6|REk||{0|}}
{30001002|XXparameter|ROLLUP_FIELD|$(grep "^ROLLUP_FIELD\\>" $ETL_CFG_FILE \| read PARAM VALUE COMMENT; print $VALUE)|3|6|REk||{0|}}
{30001002|XXparameter|RECORD_COUNT_FILE|$DW_SA_TMP/$TABLE_ID.ex.$FILE_ID.record_count.dat|3|2|RE$||{0|}}
{30001002|XXparameter|CNDTL_EXTRACT_REFORMAT|$(grep "^CNDTL_EXTRACT_REFORMAT\\>" $ETL_CFG_FILE \| read PARAM VALUE COMMENT; print $\{VALUE:-0\})|3|6|REk||{0|}}
{30001002|XXparameter|CNDTL_EXTRACT_PARTITION|$(grep "^CNDTL_EXTRACT_PARTITION\\>" $ETL_CFG_FILE \| read PARAM VALUE COMMENT; print $\{VALUE:-0\})|3|6|REk||{0|}}
{30001002|XXparameter|CNDTL_EXTRACT_PARTITION_LAYOUT|$(if [[ $CNDTL_EXTRACT_PARTITION -eq 1 ]]
  then
        grep "^CNDTL_EXTRACT_PARTITION_LAYOUT\\>" $ETL_CFG_FILE \| read PARAM VALUE COMMENT; eval print $\{VALUE:-$OUTPUT_FILE\}
  else
        print $OUTPUT_FILE
  fi
)|3|6|REk||{0|}}
{30001002|XXparameter|OUTPUT_DML|$(if (($CNDTL_EXTRACT_REFORMAT))
then
  print $DW_DML/$ETL_ID.extract_write.dml
else
  print $INPUT_DML
fi)|3|6|REKk||{0|}}
{30001002|XXparameter|REFORMAT_TRANS_FILE|$DW_XFR/$ETL_ID.extract_reformat.xfr|3|2|REK$||{0|}}
{30001002|XXparameter|PERF_PARALLEL_READ|$(grep "^HADOOP_PARALLEL_READ\\>" $ETL_CFG_FILE \| read PARAM VALUE COMMENT; print $\{VALUE:-0\})|3|6|Ek||{0|}}
{30001002|XXparameter|XFORM_REJECT_FILE|$DW_SA_TMP/$TABLE_ID.hdfs.ex.reformat.rej|3|2|K$||{0|}}
{30001002|XXparameter|XFORM_ERROR_FILE|$DW_SA_TMP/$TABLE_ID.hdfs.ex.reformat.err|3|2|K$||{0|}}
{30001002|XXparameter|_parameters_of_Replicate|@34|9|FHK|@{0|}}
{30001002|XXparameter|_parameters_of_Output_File|@34|9|FHK|@{0|}}
{30001002|XXparameter|_parameters_of_Last_Extract_Value_File_1|@34|9|FHK|@{0|}}
{30001002|XXparameter|_parameters_of_Last_Extract_Value_File|@34|9|FHK|@{0|}}
{30001002|XXparameter|_parameters_of_Send_From_Extract_Value_to_Rollup|@34|9|FHK|@{0|}}
{30001002|XXparameter|_parameters_of_Get_Max_Value_for_Extract_Field|@34|9|FHK|@{0|}}
{30001002|XXparameter|_parameters_of_Send_Primer_Row_to_Reformat|@34|9|FHK|@{0|}}
{30001002|XXparameter|_parameters_of_Update_Last_Extract_Value_File|@34|9|FHK|@{0|}}
{30001002|XXparameter|_parameters_of_Deflate|@34|9|FHK|@{0|}}
{30001002|XXparameter|_parameters_of_Conditional_Extract_Reformat|@34|9|FHK|@{0|}}
{30001002|XXparameter|_parameters_of_Conditional_Partition_by_Round_robin|@34|9|FHK|@{0|}}
{30001002|XXparameter|_parameters_of_Retain_Flow_to_Flow|@34|9|FHK|@{0|}}
{30001002|XXparameter|_parameters_of_Hadoop_Input_Dataset|@34|9|FHK|@{0|}}
{30001002|XXparameter|_parameters_of_Compute_Checksum_of_Input_Flow|@34|9|FHK|@{0|}}
{30001002|XXparameter|_parameters_of_Record_Count_File|@34|9|FHK|@{0|}}
{30001002|XXparameter|_parameters_of_Capture_out_record_count_from_Checksum|@34|9|FHK|@{0|}}
{30001002|XXparameter|_parameters_of_Parallel_Read_Hadoop|@34|9|FHK|@{0|}}
{30001002|XXparameter|_parameters_of_XForm_Error_File|@34|9|FHK|@{0|}}
{30001002|XXparameter|_parameters_of_XForm_Reject_File|@34|9|FHK|@{0|}}
{30001002|XXparameter|!ab_compatibility|3.1.4.4|3|9|P|@{0|}}
}}@0|@0|0|0|0|1591000|687000|0|single_hdfs_extract|Jacky Shen|Created 2/27/2012 7:44:50 PM|1|10|0||6||2049|{0|}0|0|{0|}{0|}{0|}{0|}1.0|1591026|687179|3|}}
{2010703001|XXGgraphinfo|2|0|3|0|{@{}@1|3.0.3.1|{2|1|1|}Job 'single_hdfs_extract' Execution Status Report

Compilation of graph "single_hdfs_extract" started at Friday, October 04, 2013 11:12:15
No compilation errors
||m_env -v


if [ -z $LAST_EXTRACT_TYPE ]
then
   print "$0: Error: LAST_EXTRACT_TYPE variable not set"
   exit 4
fi

if [[ $LAST_EXTRACT_TYPE != "N" && -z $FROM_EXTRACT_VALUE ]]
then
   print "$0: Error: FROM_EXTRACT_VALUE variable not set"
   exit 4
fi

if [[ $LAST_EXTRACT_TYPE = "V" && -z $TO_EXTRACT_VALUE ]]
then
   print "$0: Error: TO_EXTRACT_VALUE variable not set"
   exit 4
fi

if [[ $LAST_EXTRACT_TYPE = "R" && -z $ROLLUP_FIELD ]]
then
   print "$0: Error: ROLLUP_FIELD variable not set"
   exit 4
fi

if [ -z $BATCH_SEQ_NUM ]
then
   print "$0: Error: BATCH_SEQ_NUM variable not set"
   exit 4
fi



||@|31633919|}}
{2010705002|XXGrunsettings|3|0|5|0|{@{}@lvsdpelega003.lvs.ebay.com||||15|300|0|1|1|1|0|0|0|1|1|0|0|1|1|1|1|0|0|0|@17|}}
{2010242002|XXGface|4|0|7|0|{@{}@Arial|0|100|0|}}
{2010240003|XXGlegend|5|0|9|0|{@{}@Phase 1:  This is a conditional phase that runs
when the $TO_EXTRACT_VALUE variable can be
derived and used in the sql to extract the data.
The $TO_EXTRACT_VALUE value is written to the
$LAST_EXTRACT_VALUE_FILE for use as the
$FROM_EXTRACT_VALUE in the next run of the loader
system.|765416|555836|785000|576000|357000|105000|1126|}}
{2010242002|XXGface|6|0|11|0|{@{}@Courier New|16711680|90|0|}}
{2010240003|XXGlegend|7|0|13|0|{@{}@Phase 0:  Extracts data from Hadoop HDFS.

The source can be a file or a directory.
The file/dir name can contain shell command/env variable.
It can be dynamically generated from sources.lis during run time.

After extract, the component "checksum" will generate the checksum info
from the Flow and a reformat compoent followed to capture out record count.|915867|298842|936000|319000|525000|120000|1127|}}
{2010242002|XXGface|8|0|15|0|{@{}@Courier New|16711680|90|0|}}
{2010210004|XXGflow|9|0|17|0|{@{}@384|.5|.5|{12|282000|89000|302000|89000|322000|89000|322000|167000|342000|167000|362000|167000|}1155|17|}}
{2010210004|XXGflow|10|0|19|0|{@{}@384|.5|.5|{12|1097000|132000|1097000|152000|1081000|152000|1081000|137000|1065000|137000|1065000|157000|}1151|19|}}
{2010210004|XXGflow|11|0|21|0|{@{}@384|.5|.5|{12|799000|85000|819000|85000|832000|85000|832000|119000|845000|119000|865000|119000|}1121|20|}}
{2010210004|XXGflow|12|0|23|0|{@{}@384|.5|.5|{12|464000|624000|484000|624000|535000|624000|535000|598000|586000|598000|623000|598000|}1018|17|}}
{2010210004|XXGflow|13|0|25|0|{@{}@384|.5|.5|{12|514000|167000|534000|167000|570000|167000|570000|85000|606000|85000|626000|85000|}1131|17|}}
{2010210004|XXGflow|14|0|27|0|{@{}@384|.5|.5|{12|1172000|132000|1172000|152000|1184000|152000|1184000|138000|1196000|138000|1196000|158000|}1153|19|}}
{2010210004|XXGflow|15|0|29|0|{@{}@384|.5|.5|{12|1220000|76000|1240000|76000|1252000|76000|1252000|120000|1264000|120000|1284000|120000|}1144|17|}}
{2010210004|XXGflow|16|0|31|0|{@{}@384|.5|.5|{8|1412000|120000|1432000|120000|1444000|120000|1466000|120000|}1108|17|}}
{2010210004|XXGflow|17|0|33|0|{@{}@384|.5|.5|{12|1013000|114000|1033000|114000|1033000|95000|1029000|95000|1029000|76000|1049000|76000|}1143|17|}}
{2010210004|XXGflow|18|0|35|0|{@{}@384|.5|.5|{12|164000|247000|184000|247000|263000|247000|263000|177000|342000|177000|362000|177000|}1157|17|}}
{2010210004|XXGflow|19|0|37|0|{@{}@384|.5|1.0|{12|441000|280000|461000|280000|593000|280000|593000|297000|593000|297000|623000|297000|}1111|17|}}
{2010210004|XXGflow|20|0|39|0|{@{}@384|.5|.5|{12|173000|622000|193000|622000|229000|622000|229000|624000|266000|624000|286000|624000|}1017|17|}}
{2010210004|XXGflow|21|0|41|0|{@{}@384|.5|1.0|{12|514000|177000|534000|177000|603000|177000|603000|287000|603000|287000|623000|287000|}1132|531|}}
{2010210004|XXGflow|22|0|43|0|{@{}@384|.5|.5|{12|788000|292000|788000|292000|807000|292000|807000|294000|826000|294000|833000|294000|}1146|17|}}
{2010210004|XXGflow|23|0|45|0|{@{}@384|.5|.5|{12|1013000|124000|1033000|124000|1033000|220000|919000|220000|919000|316000|939000|316000|}1140|19|}}
{2010210004|XXGflow|24|0|47|0|{@{}@384|.5|.5|{12|1100000|316000|1120000|316000|1181000|316000|1181000|270000|1243000|270000|1263000|270000|}1141|17|}}
{2010210004|XXGflow|25|0|49|0|{@{}@384|.5|.5|{12|1457000|270000|1477000|270000|1478000|270000|1478000|253000|1479000|253000|1511000|253000|}1138|17|}}
{2010501005|XXGpvertex|26|0|51|0|{|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|error0_metadata|string('\\n')|3|1|l|@{0|}}
{30001002|XXparameter|log_metadata|record string("\|") node, timestamp, component, subcomponent, event_type; string("\|\\n") event_text; end|3|1|l|@{0|}}
{30001002|XXparameter|!prototype_path|$AB_COMPONENTS/Transform/Reformat.mpc|3|2|Pf$|@{0|}}
{30001002|XXparameter|Layout||3|8|s=|@{0|}}
{30001002|XXparameter|in_metadata||3|8|s=|@{0|}}
{30001002|XXparameter|out0_metadata||3|8|s=|@{0|}}
{30001002|XXparameter|reject0_metadata||3|8|s=|@{0|}}
}}@0|@1253846|197802|1274000|218000|173000|106000|1137|Capture out record count from Checksum|Ab Initio Software|Built-in|1|10|0||6||32769|1|{1|0|}}}
{2010203004|XXGoport|27|0|53|0|{@{}@1446000|265000|11000|11000|out0|0.0|count|out|0|2448|0|}}
{2010203004|XXGoport|28|0|56|0|{@{}@0|0|0|0|reject0|0.0|count|reject|0|1168|0|}}
{2010203004|XXGoport|29|0|58|0|{@{}@0|0|0|0|error0|0.0|count|error|0|1176|0|}}
{2010203004|XXGoport|30|0|60|0|{@{}@0|0|0|0|log|0.0|@@@1176|0|}}
{2010202004|XXGiport|31|0|62|0|{@{}@1264000|265000|11000|11000|in|0.0|@@@1808|0|}}
{2010501005|XXGpvertex|32|0|65|0|{Changes the record format of your data by dropping fields or by using DML expressions to add fields, combine fields, or modify the data.|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|mpname|reformat-transform|3|1|Hl||{0|}}
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
{30001002|XXparameter|out0_metadata||7|1|RFsl||{0|}}
{30001002|XXparameter|reject0_metadata||7|1|Fsl||{0|}}
{30001002|XXparameter|error0_metadata||7|1|Fl||{0|}}
{30001002|XXparameter|log_metadata||7|1|Fl||{0|}}
{30001002|XXparameter|doc_transform||8|2|FHs$|Document your transformation for dependency analysis|{0|}}
{30001002|XXparameter|doc_operation1|out[*],reject[*],error[*],log::document(in)|3|2|RH$||{0|}}
{30001002|XXparameter|mpcmodtime|1325635143|1|1|Hl|The last modification time of this component's template|{0|}}
{30001002|XXparameter|_propagation|metadata type: error = "string('\\\\n')"
metadata type: reject = in
metadata type: log = "record string(\\"\|\\") node, timestamp, component, subcomponent, event_type; string(\\"\|\\\\n\\") event_text; end"|3|1|HKl|@{0|}}
{30001002|XXparameter|_propagate_through||3|1|FHKl|@{0|}}
}}@0|@0|0|0|0|0|0|0|@@@1|10|-1|@6|@1|1|{1|0|}}}
{2010203004|XXGoport|33|0|67|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
{30001002|XXparameter|transform0||8|8|FO=|Reformat transform|{0|}}
{30001002|XXparameter|transform0.index|count|3|9|P|@{0|}}
{30001002|XXparameter|transform0.index_value|0|3|9|P|@{0|}}
{30001002|XXparameter|transform0.index_name|transform|3|9|P|@{0|}}
}}@0|0|0|0|out0|0.0|count|out|0|2448|0|}}
{2010203004|XXGoport|34|0|71|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|F=||{0|}}
}}@0|0|0|0|reject0|0.0|count|reject|0|1168|0|}}
{2010203004|XXGoport|35|0|74|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|F=||{0|}}
}}@0|0|0|0|error0|0.0|count|error|0|1176|0|}}
{2010203004|XXGoport|36|0|77|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|F=||{0|}}
}}@0|0|0|0|log|0.0|@@@1176|0|}}
{2010202004|XXGiport|37|0|80|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|in|0.0|@@@1808|0|}}
{2010503005|XXGfvertex|38|0|83|0|{|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|Layout|$[[record kind 85 subkind 0 parts [vector _interp_("file:$RECORD_COUNT_FILE", "dollar_substitution")]]]|3|9||@{0|}}
{30001002|XXparameter|write_metadata|record decimal("\\n") record_count; end|3|1|l|@{0|}}
{30001002|XXparameter|!prototype_path|$AB_COMPONENTS/Datasets/Output_File.mdc|3|2|Pf$|@{0|}}
}}@0|@1497193|213728|1522000|234000|40000|40000|1139|Record Count File|Craig Werre||1|10|0||6||33025|-1|-1|}}
{2010202004|XXGiport|39|0|85|0|{@{}@1512000|248000|11000|11000|write|0.0|@@@1776|0|}}
{2010503005|XXGfvertex|40|0|88|0|{Represents one file, many files, or a multifile as an output from your graph.|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|protection|0666|12|2|RF$||{0|}}
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
{30001002|XXparameter|write_metadata||7|1|RFl||{0|}}
{30001002|XXparameter|mpcmodtime|1299550332|1|1|Hl|The last modification time of this component's template|{0|}}
{30001002|XXparameter|eme_dataset_location||3|2|F$|Place in the EME to create a dataset corresponding to this file.|{0|}}
}}@0|@0|0|0|0|0|0|0|@@@1|10|-1|@6|@1|-1|-1|}}
{2010202004|XXGiport|41|0|90|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|write|0.0|@@@1776|0|}}
{2010501005|XXGpvertex|42|0|93|0|{|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|out_metadata|record big endian real(8) sum_of_crc; big endian real(8) sum_of_lengths; unsigned big endian integer(4) xor_of_crc; unsigned big endian integer(4) record_count; end|3|1|l|@{0|}}
{30001002|XXparameter|!prototype_path|$AB_COMPONENTS/Validate/Compute_Checksum.mpc|3|2|Pf$|@{0|}}
{30001002|XXparameter|Layout||3|8|s=|@{0|}}
{30001002|XXparameter|in_metadata||3|8|s=|@{0|}}
}}@0|@930153|243528|950000|264000|140000|106000|1142|Compute Checksum of Input Flow|Ab Initio Software|Built-in 2.0:|1|10|0||6||32769|1|{1|0|}}}
{2010203004|XXGoport|43|0|95|0|{@{}@1089000|311000|11000|11000|out|0.0|@@@2448|0|}}
{2010202004|XXGiport|44|0|98|0|{@{}@940000|311000|11000|11000|in|0.0|@@@1808|0|}}
{2010501005|XXGpvertex|45|0|101|0|{Calculates a checksum for data records.|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|mpname|compute-checksum|3|1|Hl||{0|}}
{30001002|XXparameter|image__|unitool|3|2|H$||{0|}}
{30001002|XXparameter|condition||3|2|F$||{0|}}
{30001002|XXparameter|conditionInputPort|in|3|2|F$||{0|}}
{30001002|XXparameter|conditionOutputPort|out|3|2|F$||{0|}}
{30001002|XXparameter|condition_interpretation|Replace with flow|15|1|Fl||{2|Replace with flow|Remove completely|}}
{30001002|XXparameter|condition_interpretation.display_name|condition-interpretation|3|9|P|@{0|}}
{30001002|XXparameter|port_analysis|out=all_to_all(in)|3|2|H$||{0|}}
{30001002|XXparameter|continuous_analysis||3|2|H$||{0|}}
{30001002|XXparameter|Layout|@9|2|RFs$||{0|}}
{30001002|XXparameter|in_metadata||7|1|RFsl||{0|}}
{30001002|XXparameter|out_metadata||7|1|RFl||{0|}}
{30001002|XXparameter|doc_transform||8|2|FHs$|Document your transformation for dependency analysis|{0|}}
{30001002|XXparameter|doc_operation1|out::document(in)|3|2|RH$||{0|}}
{30001002|XXparameter|mpcmodtime|1325635143|1|1|Hl|The last modification time of this component's template|{0|}}
{30001002|XXparameter|_propagation|metadata type: out = "record big endian real(8) sum_of_crc; big endian real(8) sum_of_lengths; unsigned big endian integer(4) xor_of_crc; unsigned big endian integer(4) record_count; end"|3|1|HKl|@{0|}}
{30001002|XXparameter|_propagate_through||3|1|FHKl|@{0|}}
}}@0|@0|0|0|0|0|0|0|@@@1|10|-1|@6|@1|1|{1|0|}}}
{2010203004|XXGoport|46|0|103|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|out|0.0|@@@2448|0|}}
{2010202004|XXGiport|47|0|106|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|in|0.0|@@@1808|0|}}
{2010501005|XXGpvertex|48|0|110|0|{Conditional component - Changes the record format of your data by dropping fields or by using DML expressions to add fields, combine fields, or modify the data.|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|transform0|$REFORMAT_TRANS_FILE|3|2|f$|@{0|}}
{30001002|XXparameter|condition|$CNDTL_EXTRACT_REFORMAT|3|2|$|@{0|}}
{30001002|XXparameter|in_metadata|$INPUT_DML|3|2|f$|@{0|}}
{30001002|XXparameter|out0_metadata|$OUTPUT_DML|3|2|f$|@{0|}}
{30001002|XXparameter|error0_metadata|string('\\n')|3|1|l|@{0|}}
{30001002|XXparameter|log_metadata|record
 void(integer(4)) record_cnt = invocation_number();
end|3|1|l|@{0|}}
{30001002|XXparameter|!prototype_path|$AB_COMPONENTS/Transform/Reformat.mpc|3|2|Pf$|@{0|}}
{30001002|XXparameter|Layout||3|8|s=|@{0|}}
{30001002|XXparameter|reject0_metadata||3|8|s=|@{0|}}
}}@0|@1039615|11100|1060000|31000|150000|91000|1145|Conditional Extract Reformat|Ab Initio Software|Built-in|1|10|0||6||32769|1|{1|0|}}}
{2010203004|XXGoport|49|0|112|0|{@{}@1209000|71000|11000|11000|out0|0.0|count|out|0|2448|0|}}
{2010203004|XXGoport|50|0|115|0|{@{}@1092000|121000|11000|11000|reject0|0.0|count|reject|0|1168|0|}}
{2010203004|XXGoport|51|0|118|0|{@{}@1167000|121000|11000|11000|error0|0.0|count|error|0|1176|0|}}
{2010203004|XXGoport|52|0|121|0|{@{}@0|0|0|0|log|0.0|@@@1176|0|}}
{2010202004|XXGiport|53|0|123|0|{@{}@1050000|71000|11000|11000|in|0.0|@@@1808|0|}}
{2010501005|XXGpvertex|54|0|126|0|{Changes the record format of your data by dropping fields or by using DML expressions to add fields, combine fields, or modify the data.|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|mpname|reformat-transform|3|1|Hl||{0|}}
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
{30001002|XXparameter|in_metadata||7|1|RFl||{0|}}
{30001002|XXparameter|out0_metadata||7|1|RFl||{0|}}
{30001002|XXparameter|reject0_metadata||7|1|RFsl||{0|}}
{30001002|XXparameter|error0_metadata||7|1|RFl||{0|}}
{30001002|XXparameter|log_metadata||7|1|Fl||{0|}}
{30001002|XXparameter|doc_transform||8|2|FHs$|Document your transformation for dependency analysis|{0|}}
{30001002|XXparameter|doc_operation1|out[*],reject[*],error[*],log::document(in)|3|2|RH$||{0|}}
{30001002|XXparameter|mpcmodtime|1299550340|1|1|Hl|The last modification time of this component's template|{0|}}
{30001002|XXparameter|_propagation|metadata type: error = remote("$AB_HOME/include/error-info.dml")
metadata type: reject = in
metadata type: log = remote("$AB_HOME/include/log-info.dml")|3|1|HKl|@{0|}}
{30001002|XXparameter|_propagate_through||3|1|FHKl|@{0|}}
}}@0|@0|0|0|0|0|0|0|@@@1|10|-1|@6|@1|1|{1|0|}}}
{2010203004|XXGoport|55|0|128|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
{30001002|XXparameter|transform0||8|8|FO=|Reformat transform|{0|}}
{30001002|XXparameter|transform0.index|count|3|9|P|@{0|}}
{30001002|XXparameter|transform0.index_value|0|3|9|P|@{0|}}
{30001002|XXparameter|transform0.index_name|transform|3|9|P|@{0|}}
}}@0|0|0|0|out0|0.0|count|out|0|2448|0|}}
{2010203004|XXGoport|56|0|132|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|reject0|0.0|count|reject|0|1168|0|}}
{2010203004|XXGoport|57|0|135|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|error0|0.0|count|error|0|1176|0|}}
{2010203004|XXGoport|58|0|138|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|F=||{0|}}
}}@0|0|0|0|log|0.0|@@@1176|0|}}
{2010202004|XXGiport|59|0|141|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|in|0.0|@@@1808|0|}}
{2010503005|XXGfvertex|60|0|144|0|{|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|Layout|$[[record kind 85 subkind 0 parts [vector _interp_("$OUTPUT_FILE", "dollar_substitution")]]]|3|9||@{0|}}
{30001002|XXparameter|!prototype_path|$AB_COMPONENTS/Datasets/Output_File.mdc|3|2|Pf$|@{0|}}
{30001002|XXparameter|write_metadata||3|8|s=|@{0|}}
}}@0|@1452290|81437|1477000|101000|40000|40000|990|Output File|Craig Werre||1|10|-1||6||33025|-1|-1|}}
{2010202004|XXGiport|61|0|146|0|{@{}@1467000|115000|11000|11000|write|0.0|@@@1776|0|}}
{2010503005|XXGfvertex|62|0|149|0|{Represents one file, many files, or a multifile as an output from your graph.|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|protection|0666|12|2|RF$||{0|}}
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
{2010202004|XXGiport|63|0|151|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|write|0.0|@@@1776|0|}}
{2010501005|XXGpvertex|64|0|154|0|{This is a conditional replicate function that is always set to the value of the preceding conditional partition. Provides workaround for partition component that doesn't retain flow-to-flow when its condition is false.|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|condition|$CNDTL_EXTRACT_PARTITION|3|2|$|@{0|}}
{30001002|XXparameter|!prototype_path|$AB_COMPONENTS/Miscellaneous/Replicate.mpc|3|2|Pf$|@{0|}}
{30001002|XXparameter|Layout||3|8|s=|@{0|}}
{30001002|XXparameter|in_metadata||3|8|s=|@{0|}}
{30001002|XXparameter|out_metadata||3|8|s=|@{0|}}
}}@0|@856265|56108|876000|76000|127000|87000|1068|Retain Flow to Flow|Ab Initio Software|Built-in|1|10|0||6||32769|1|{1|0|}}}
{2010203004|XXGoport|65|0|156|0|{@{}@1002000|109000|11000|21000|out|0.0|@@@2068|0|}}
{2010202004|XXGiport|66|0|160|0|{@{}@866000|114000|11000|11000|in|0.0|@@@1812|0|}}
{2010501005|XXGpvertex|67|0|163|0|{Arbitrarily combines all the data records it receives into a single flow and writes a copy of that flow to each of its output flows.|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|mpname|broadcast|3|1|Hl||{0|}}
{30001002|XXparameter|image__|unitool|3|2|H$||{0|}}
{30001002|XXparameter|NoHidePorts|in out|3|2|H$||{0|}}
{30001002|XXparameter|condition||3|2|F$||{0|}}
{30001002|XXparameter|conditionInputPort|in|3|2|F$||{0|}}
{30001002|XXparameter|conditionOutputPort|out|3|2|F$||{0|}}
{30001002|XXparameter|condition_interpretation|Replace with flow|15|1|Fl||{2|Replace with flow|Remove completely|}}
{30001002|XXparameter|condition_interpretation.display_name|condition-interpretation|3|9|P|@{0|}}
{30001002|XXparameter|port_analysis|out=exact(in)|3|2|H$||{0|}}
{30001002|XXparameter|continuous_analysis|broadcast, publisher|3|2|H$||{0|}}
{30001002|XXparameter|Layout|@9|2|RFs$||{0|}}
{30001002|XXparameter|in_metadata||7|1|RFsl||{0|}}
{30001002|XXparameter|out_metadata||7|1|RFsl||{0|}}
{30001002|XXparameter|doc_transform||8|2|FHs$|Document your transformation for dependency analysis|{0|}}
{30001002|XXparameter|doc_operation1|out::document(in)|3|2|RH$||{0|}}
{30001002|XXparameter|mpcmodtime|1299550339|1|1|Hl|The last modification time of this component's template|{0|}}
{30001002|XXparameter|_propagate_through|metadata type: out = in
metadata type: in = out|3|1|FHKl|@{0|}}
}}@0|@0|0|0|0|0|0|0|@@@1|10|-1|@6|@1|1|{1|0|}}}
{2010203004|XXGoport|68|0|165|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|out|0.0|@@@2068|0|}}
{2010202004|XXGiport|69|0|168|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|in|0.0|@@@1812|0|}}
{2010501005|XXGpvertex|70|0|179|0|{|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|condition|$CNDTL_EXTRACT_PARTITION|3|2|$|@{0|}}
{30001002|XXparameter|Layout|$[[record kind 85 subkind 0 parts [vector _interp_("$CNDTL_EXTRACT_PARTITION_LAYOUT", "dollar_substitution")]]]|3|9||@{0|}}
{30001002|XXparameter|!prototype_path|$AB_COMPONENTS/Partitioning/Partition_by_Round-robin.mpc|3|2|Pf$|@{0|}}
{30001002|XXparameter|in_metadata||3|8|s=|@{0|}}
{30001002|XXparameter|out_metadata||3|8|s=|@{0|}}
}}@0|@617313|12649|637000|33000|152000|106000|1122|Conditional Partition by Round-robin|Ab Initio Software|Built-in|1|10|0||6||32769|1|{1|0|}}}
{2010203004|XXGoport|71|0|181|0|{@{}@788000|80000|11000|11000|out|0.0|@@@2323|0|}}
{2010202004|XXGiport|72|0|184|0|{@{}@627000|80000|11000|11000|in|0.0|@@@1808|0|}}
{2010501005|XXGpvertex|73|0|187|0|{Distributes data records evenly to each output flow in round-robin fashion.

Use the Interleave component to reverse the effects of Partition by Round-robin.|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|mpname|roundrobin-partition|3|1|Hl||{0|}}
{30001002|XXparameter|image__|unitool|3|2|H$||{0|}}
{30001002|XXparameter|block_size|1|1|2|RFO$|Number of records before switching to the next port|{0|}}
{30001002|XXparameter|condition||3|2|F$||{0|}}
{30001002|XXparameter|conditionInputPort|in|3|2|F$||{0|}}
{30001002|XXparameter|conditionOutputPort|out|3|2|F$||{0|}}
{30001002|XXparameter|condition_interpretation|Replace with flow|15|1|Fl||{2|Replace with flow|Remove completely|}}
{30001002|XXparameter|condition_interpretation.display_name|condition-interpretation|3|9|P|@{0|}}
{30001002|XXparameter|port_analysis|out=in|3|2|H$||{0|}}
{30001002|XXparameter|continuous_analysis||3|2|H$||{0|}}
{30001002|XXparameter|Layout|@9|2|RF$||{0|}}
{30001002|XXparameter|in_metadata||7|1|RFsl||{0|}}
{30001002|XXparameter|out_metadata||7|1|RFsl||{0|}}
{30001002|XXparameter|doc_transform||8|2|FHs$|Document your transformation for dependency analysis|{0|}}
{30001002|XXparameter|doc_operation1|out::document(in)|3|2|RH$||{0|}}
{30001002|XXparameter|mpcmodtime|1299550340|1|1|Hl|The last modification time of this component's template|{0|}}
{30001002|XXparameter|_propagate_through|metadata type: out = in
metadata type: in = out|3|1|FHKl|@{0|}}
}}@0|@0|0|0|0|0|0|0|@@@1|10|-1|@6|@1|1|{1|0|}}}
{2010203004|XXGoport|74|0|189|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|out|0.0|@@@2323|0|}}
{2010202004|XXGiport|75|0|192|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|in|0.0|@@@1808|0|}}
{2010501005|XXGpvertex|76|0|195|0|{|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|input_files|$INPUT_FILE|3|2|$|@{0|}}
{30001002|XXparameter|job_config|$DW_DBC/$JOB_CONFIG|3|2|$|@{0|}}
{30001002|XXparameter|out_metadata|$INPUT_DML|3|2|f$|@{0|}}
{30001002|XXparameter|condition|$( if [[ $PERF_PARALLEL_READ = 1 ]]; then print "0"; else print "1"; fi)|3|6|k|@{0|}}
{30001002|XXparameter|!prototype_path|$AB_COMPONENTS/Datasets/Hadoop/Read_Hadoop_Files.mpc|3|2|Pf$|@{0|}}
{30001002|XXparameter|Layout||3|8|s=|@{0|}}
}}@0|@13608|183859|34000|204000|120000|87000|1158|Hadoop Input Dataset|Ab Initio Software|Built-in 3.1.0:|1|10|0||6||32769|1|{1|0|}}}
{2010203004|XXGoport|77|0|197|0|{@{}@153000|242000|11000|11000|out|0.0|@@@2512|0|}}
{2010501005|XXGpvertex|78|0|200|0|{Reads files from HDFS (Hadoop Distributed File System).|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|mpname||3|1|Hl||{0|}}
{30001002|XXparameter|image__|/~ab_home/bin/read-hadoop-files.ksh|3|2|H$||{0|}}
{30001002|XXparameter|input_files||3|2|RFO$|A space separated list of absolute paths of files and directories in an HDFS file system (e.g., /user/jones/input.dat). For each directory in the file list, all non-directory files in that directory are implicitly included in the list of files.|{0|}}
{30001002|XXparameter|job_config||3|2|FO$|An optional XML-formatted configuration file containing additional properties for Hadoop.|{0|}}
{30001002|XXparameter|HADOOP_HOME|$HADOOP_HOME|3|2|FK$|Home directory for Hadoop installation (e.g., /usr/local/hadoop)|{0|}}
{30001002|XXparameter|dataset_analysis|eme_dataset(file)=out,r|3|2|H$||{0|}}
{30001002|XXparameter|Layout|@9|2|RFs$||{0|}}
{30001002|XXparameter|out_metadata||7|1|RFl||{0|}}
{30001002|XXparameter|doc_transform||8|2|FHs$|Document your transformation for dependency analysis|{0|}}
{30001002|XXparameter|doc_operation1|out::document()|3|2|RH$||{0|}}
{30001002|XXparameter|mpcmodtime|1325635136|1|1|Hl|The last modification time of this component's template|{0|}}
{30001002|XXparameter|condition||3|2|F$||{0|}}
{30001002|XXparameter|condition_interpretation||15|1|Fl||{2|Replace with flow|Remove completely|}}
{30001002|XXparameter|condition_interpretation.display_name|condition-interpretation|3|9|P|@{0|}}
{30001002|XXparameter|conditionInputPort||3|2|F$||{0|}}
{30001002|XXparameter|conditionOutputPort||3|2|F$||{0|}}
{30001002|XXparameter|eme_dataset||3|2|F$|Location in which to create a dataset corresponding to this component.|{0|}}
}}@0|@0|0|0|0|0|0|0|@@@1|10|-1|@6|@1|1|{1|0|}}}
{2010203004|XXGoport|79|0|202|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|out|0.0|@@@2512|0|}}
{2010501005|XXGpvertex|80|0|207|0|{|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|condition|$CNDTL_EXTRACT_COMPRESS|3|6|k|@{0|}}
{30001002|XXparameter|compression|$CNDTL_EXTRACT_COMPRESS_LEVEL|3|6|k|@{0|}}
{30001002|XXparameter|!prototype_path|$AB_COMPONENTS/Translate/Compress/Deflate.mpc|3|2|Pf$|@{0|}}
{30001002|XXparameter|Layout||3|8|s=|@{0|}}
{30001002|XXparameter|in_metadata||3|8|s=|@{0|}}
{30001002|XXparameter|out_metadata||3|8|=|@{0|}}
}}@0|@1275471|67884|1295000|88000|107000|65000|1109|Deflate|Ab Initio Software|Built-in 2.12.2.m1:|1|10|0||6||32769|1|{1|0|}}}
{2010203004|XXGoport|81|0|209|0|{@{}@1401000|115000|11000|11000|out|0.0|@@@2320|0|}}
{2010202004|XXGiport|82|0|212|0|{@{}@1285000|115000|11000|11000|in|0.0|@@@1808|0|}}
{2010501005|XXGpvertex|83|0|215|0|{Compresses data in a manner compatible with the zip and gzip deflate method. Use Inflate to uncompress the data.|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|mpname|broadcast|3|1|Hl||{0|}}
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
{2010203004|XXGoport|84|0|217|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|out|0.0|@@@2320|0|}}
{2010202004|XXGiport|85|0|220|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|in|0.0|@@@1808|0|}}
{2010501005|XXGpvertex|86|0|226|0|{|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|sorted_input|False|3|1|l|@{0|}}
{30001002|XXparameter|key|\{\}|3|2|$|@{0|}}
{30001002|XXparameter|transform|out::rollup(in) =
begin
  out.last_extract_value :: (string("\\n")) first_defined(max(in.$\{ROLLUP_FIELD\}), NULL);                  
end;|3|3|c|@{0|}}
{30001002|XXparameter|condition|$CNDTL_LAST_EXTRACT_ROLLUP|3|2|$|@{0|}}
{30001002|XXparameter|condition_interpretation|Remove completely|3|1|l|@{0|}}
{30001002|XXparameter|error_metadata|string('\\n')|3|1|l|@{0|}}
{30001002|XXparameter|log_metadata|record string("\|") node, timestamp, component, subcomponent, event_type; string("\|\\n") event_text; end|3|1|l|@{0|}}
{30001002|XXparameter|!prototype_path|$AB_COMPONENTS/Transform/Rollup.mpc|3|2|Pf$|@{0|}}
{30001002|XXparameter|Layout||3|8|s=|@{0|}}
{30001002|XXparameter|in_metadata||3|8|s=|@{0|}}
{30001002|XXparameter|out_metadata||3|8|s=|@{0|}}
{30001002|XXparameter|reject_metadata||3|8|s=|@{0|}}
}}@0|@613965|219698|634000|240000|144000|106000|1113|Get Max Value for Extract Field|Ab Initio Software|Built-in 2.0:|1|10|0||6||32769|1|{1|0|}}}
{2010203004|XXGoport|87|0|228|0|{@{}@777000|287000|11000|11000|out|0.0|@@@2448|0|}}
{2010203004|XXGoport|88|0|231|0|{@{}@0|0|0|0|reject|0.0|@@@1168|0|}}
{2010203004|XXGoport|89|0|233|0|{@{}@0|0|0|0|error|0.0|@@@1176|0|}}
{2010203004|XXGoport|90|0|235|0|{@{}@0|0|0|0|log|0.0|@@@1176|0|}}
{2010202004|XXGiport|91|0|237|0|{@{}@624000|282000|11000|21000|in|0.0|@@@1808|0|}}
{2010501005|XXGpvertex|92|0|241|0|{Generates data records that summarize groups of data records.  Rollup in memory maximizes performance by keeping intermediate results in main memory.  Rollup sorted requires grouped input.|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|mpname|?rollup|3|1|Hl||{0|}}
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
{30001002|XXparameter|out_metadata||7|1|RFsl||{0|}}
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
{2010203004|XXGoport|93|0|243|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|out|0.0|@@@2448|0|}}
{2010203004|XXGoport|94|0|246|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|F=||{0|}}
}}@0|0|0|0|reject|0.0|@@@1168|0|}}
{2010203004|XXGoport|95|0|249|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|F=||{0|}}
}}@0|0|0|0|error|0.0|@@@1176|0|}}
{2010203004|XXGoport|96|0|252|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|F=||{0|}}
}}@0|0|0|0|log|0.0|@@@1176|0|}}
{2010202004|XXGiport|97|0|255|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|in|0.0|@@@1808|0|}}
{2010503005|XXGfvertex|98|0|258|0|{|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|condition|$CNDTL_LAST_EXTRACT_ROLLUP|3|2|$|@{0|}}
{30001002|XXparameter|Layout|$[[record kind 85 subkind 0 parts [vector _interp_("file:$LAST_EXTRACT_VALUE_FILE", "dollar_substitution")]]]|3|9||@{0|}}
{30001002|XXparameter|write_metadata|record
  string("\\n") last_extract_value;
end;|3|1|l|@{0|}}
{30001002|XXparameter|!prototype_path|$AB_COMPONENTS/Datasets/Output_File.mdc|3|2|Pf$|@{0|}}
}}@0|@819460|255266|844000|275000|40000|40000|1147|Last Extract Value File-1|Craig Werre||1|10|0||6||32769|-1|-1|}}
{2010202004|XXGiport|99|0|260|0|{@{}@834000|289000|11000|11000|write|0.0|@@@1776|0|}}
{2010503005|XXGfvertex|100|0|263|0|{Represents one file, many files, or a multifile as an output from your graph.|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|protection|0666|12|2|RF$||{0|}}
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
{30001002|XXparameter|write_metadata||7|1|RFl||{0|}}
{30001002|XXparameter|mpcmodtime|1299550332|1|1|Hl|The last modification time of this component's template|{0|}}
{30001002|XXparameter|eme_dataset_location||3|2|F$|Place in the EME to create a dataset corresponding to this file.|{0|}}
}}@0|@0|0|0|0|0|0|0|@@@1|10|-1|@6|@1|-1|-1|}}
{2010202004|XXGiport|101|0|265|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|write|0.0|@@@1776|0|}}
{2010503005|XXGfvertex|102|0|272|0|{|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|condition|$CNDTL_LAST_EXTRACT_VARIABLE|3|2|$|@{0|}}
{30001002|XXparameter|Layout|$[[record kind 85 subkind 0 parts [vector _interp_("file:$LAST_EXTRACT_VALUE_FILE", "dollar_substitution")]]]|3|9||@{0|}}
{30001002|XXparameter|write_metadata|record
  string("\\n") last_extract_value;
end;|3|1|l|@{0|}}
{30001002|XXparameter|!prototype_path|$AB_COMPONENTS/Datasets/Output_File.mdc|3|2|Pf$|@{0|}}
}}@1|@608928|558573|634000|579000|40000|40000|942|Last Extract Value File|Craig Werre||1|10|0||6||33025|-1|-1|}}
{2010202004|XXGiport|103|0|274|0|{@{}@624000|593000|11000|11000|write|0.0|@@@1776|0|}}
{2010503005|XXGfvertex|104|0|277|0|{Represents one file, many files, or a multifile as an output from your graph.|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|protection|0666|12|2|RF$||{0|}}
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
{30001002|XXparameter|write_metadata||7|1|RFl||{0|}}
{30001002|XXparameter|mpcmodtime|1299550332|1|1|Hl|The last modification time of this component's template|{0|}}
{30001002|XXparameter|eme_dataset_location||3|2|F$|Place in the EME to create a dataset corresponding to this file.|{0|}}
}}@0|@0|0|0|0|0|0|0|@@@1|10|-1|@6|@1|-1|-1|}}
{2010202004|XXGiport|105|0|279|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|write|0.0|@@@1776|0|}}
{2010600005|XXGgraph|106|0|284|0|{Subgraph created from graph "single_hdfs_extract"|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|_parameters_of_Reformat|@34|9|FHK|@{0|}}
{30001002|XXparameter|condition|$PERF_PARALLEL_READ|3|2|F$||{0|}}
{30001002|XXparameter|_parameters_of_Redefine_Format|@34|9|FHK|@{0|}}
{30001002|XXparameter|_parameters_of_Hadoop_Parallel_Read|@34|9|FHK|@{0|}}
}}@0|@11225|11224|31000|31000|613000|217000|1156|Parallel_Read_Hadoop|Jacky Shen||1|10|0||6||32769|{0|}0|0|{0|}{0|}{0|}{0|}.3931484520435333|613000|217000|3|}}
{2010210004|XXGflow|107|0|286|0|{@{}@384|.5|.5|{12|189000|93000|189000|93000|198000|93000|198000|82000|208000|82000|215000|82000|}1128|17|}}
{2010210004|XXGflow|108|0|288|0|{@{}@384|.5|.5|{12|369000|82000|389000|82000|389000|131000|364000|131000|364000|181000|384000|181000|}1126|17|}}
{2010501005|XXGpvertex|109|0|290|0|{|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|job_config|$DW_DBC/$JOB_CONFIG|3|2|$|@{0|}}
{30001002|XXparameter|job_input|$INPUT_FILE|3|2|$|@{0|}}
{30001002|XXparameter|job_options|-text|3|2|$|@{0|}}
{30001002|XXparameter|JAVA_HOME|$JAVA_HOME|3|2|$|@{0|}}
{30001002|XXparameter|HADOOP_HOME|$HADOOP_HOME|3|2|$|@{0|}}
{30001002|XXparameter|compressed|True|3|1|l|@{0|}}
{30001002|XXparameter|out_metadata|include "~ab_home/Projects/root/components/Internet/TCP/tcpheader.dml";

type key_value_pair = record
  void(big endian integer(4)) key;
  void(big endian integer(4)) value;
end;

metadata type = record
  tcpheader header;
  key_value_pair request_body;
end;|3|1|l|@{0|}}
{30001002|XXparameter|reject_metadata|void(integer(4));|3|1|l|@{0|}}
{30001002|XXparameter|error_metadata|string('\\n')|3|1|l|@{0|}}
{30001002|XXparameter|log_metadata|record string("\|") node, timestamp, component, subcomponent, event_type; string("\|\\n") event_text; end|3|1|l|@{0|}}
{30001002|XXparameter|!prototype_path|$AB_COMPONENTS/Datasets/Hadoop/Hadoop_Parallel_Read.mpc|3|2|Pf$|@{0|}}
{30001002|XXparameter|Layout||3|8|s=|@{0|}}
}}@0|@40000|30000|60000|50000|119000|87000|1129|Hadoop Parallel Read|Ab Initio Software|Built-in 3.1.0:|1|10|0||6||32769|1|{1|0|}}}
{2010203004|XXGoport|110|0|292|0|{@{}@178000|88000|11000|11000|out|0.0|@@@2448|0|}}
{2010203004|XXGoport|111|0|295|0|{@{}@0|0|0|0|reject|0.0|@@@1168|0|}}
{2010203004|XXGoport|112|0|297|0|{@{}@0|0|0|0|error|0.0|@@@1176|0|}}
{2010203004|XXGoport|113|0|299|0|{@{}@0|0|0|0|log|0.0|@@@1176|0|}}
{2010501005|XXGpvertex|114|0|301|0|{Extracts data originating from mappers in Hadoop.|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|mpname|\@mp hadoop-read|3|1|Hl||{0|}}
{30001002|XXparameter|image__||3|2|H$||{0|}}
{30001002|XXparameter|job_config||3|2|FO$|An optional XML-formatted configuration file containing additional properties for Hadoop.|{0|}}
{30001002|XXparameter|job_jar|/~ab_home/lib/java/ab_hadoop.jar|3|2|FO$|JAR file containing class files needed for Hadoop job.|{0|}}
{30001002|XXparameter|job_input||3|2|RFK$|HDFS input files and directories (space-separated) for Hadoop job.|{0|}}
{30001002|XXparameter|job_host||3|2|FK$|Name of host from which the Hadoop job is launched. If empty, the default is the run host.|{0|}}
{30001002|XXparameter|job_class|com.abinitio.hadoop.job.ExtractMapper|3|2|FK$|Name of class that defines main program for Hadoop job.|{0|}}
{30001002|XXparameter|job_options||3|2|FK$|Additional command line options to supply to the main program specified by 'job_class'|{0|}}
{30001002|XXparameter|job_log||3|2|FK$|Log file for Hadoop job output.|{0|}}
{30001002|XXparameter|JAVA_HOME||3|2|FK$|Home directory for Java installation|{0|}}
{30001002|XXparameter|HADOOP_HOME||3|2|FK$|Home directory for Hadoop installation|{0|}}
{30001002|XXparameter|compressed|False|13|1|FKl|If True, Hadoop job's output is compressed (gzipped) before sending, and then decompressed by the component|{0|}}
{30001002|XXparameter|poll_delay|0.5|2|2|FK$|If no record has been received, the wait time (in seconds) before polling for another record|{0|}}
{30001002|XXparameter|record_size_limit|1048576|1|2|FK$|Maximum allowable record size|{0|}}
{30001002|XXparameter|record_time_limit|60.0|2|2|FK$|Timeout (in seconds) for receiving a complete record|{0|}}
{30001002|XXparameter|reject_threshold|Abort on first reject|15|1|Fl|When to abort if input records are rejected|{3|Abort on first reject|Never abort|Use limit/ramp|}}
{30001002|XXparameter|reject_threshold.display_name|reject-threshold|3|9|P|@{0|}}
{30001002|XXparameter|limit|0|1|2|F$|Maximum rejected records before failure|{0|}}
{30001002|XXparameter|limit.condition|param reject-threshold Use*|3|15|P?|@{0|}}
{30001002|XXparameter|ramp|0.0|2|2|F$|Rate of rejected records|{0|}}
{30001002|XXparameter|ramp.condition|param reject-threshold Use*|3|15|P?|@{0|}}
{30001002|XXparameter|limit_keyword|value reject-threshold Never* 0 value reject-threshold Abort* 0 sameas limit default dollar_substitution 0|1|13|Kv|Maximum rejected records before failure|{0|}}
{30001002|XXparameter|ramp_keyword|value reject-threshold Never* -1 value reject-threshold Abort* 0.0 sameas ramp default dollar_substitution 0.0|2|13|Kv|Rate of rejected records|{0|}}
{30001002|XXparameter|lay|$layout|14|1|Kl|layout of this node|{0|}}
{30001002|XXparameter|keyword_map|limit_keyword limit ramp_keyword ramp lay layout|3|2|$||{0|}}
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
{30001002|XXparameter|error_group||3|2|F$|Optional group name identifying a Handle Errors component to which errors can be directed.|{0|}}
{30001002|XXparameter|log_group||3|2|F$|Optional group name of a Handle Logs component to which log output can be directed.|{0|}}
{30001002|XXparameter|_use_rich_error_format|True|13|1|Hl|False means use legacy string newline format|{0|}}
{30001002|XXparameter|_use_rich_log_format|True|13|1|Hl|False means use legacy native charset format|{0|}}
{30001002|XXparameter|dataset_analysis|eme_dataset(file)=out,r|3|2|H$||{0|}}
{30001002|XXparameter|Layout|@9|2|RFs$||{0|}}
{30001002|XXparameter|out_metadata||7|1|RFl||{0|}}
{30001002|XXparameter|reject_metadata||7|1|Fl||{0|}}
{30001002|XXparameter|error_metadata||7|1|Fl||{0|}}
{30001002|XXparameter|log_metadata||7|1|Fl||{0|}}
{30001002|XXparameter|doc_transform||8|2|FHs$|Document your transformation for dependency analysis|{0|}}
{30001002|XXparameter|doc_operation1|out,reject,error,log::document()|3|2|RH$||{0|}}
{30001002|XXparameter|mpcmodtime|1325635136|1|1|Hl|The last modification time of this component's template|{0|}}
{30001002|XXparameter|condition||3|2|F$||{0|}}
{30001002|XXparameter|condition_interpretation||15|1|Fl||{2|Replace with flow|Remove completely|}}
{30001002|XXparameter|condition_interpretation.display_name|condition-interpretation|3|9|P|@{0|}}
{30001002|XXparameter|conditionInputPort||3|2|F$||{0|}}
{30001002|XXparameter|conditionOutputPort||3|2|F$||{0|}}
{30001002|XXparameter|_propagation|metadata type: out = "include \\"~ab_home/Projects/root/components/Internet/TCP/tcpheader.dml\\";

type key_value_pair = record
  void(big endian integer(4)) key;
  void(big endian integer(4)) value;
end;

metadata type = record
  tcpheader header;
  key_value_pair request_body;
end;"
metadata type: reject = "void(integer(4));"
metadata type: error = "string('\\\\n')"
metadata type: log = "record string(\\"\|\\") node, timestamp, component, subcomponent, event_type; string(\\"\|\\\\n\\") event_text; end"|3|1|HKl|@{0|}}
{30001002|XXparameter|eme_dataset||3|2|F$|Location in which to create a dataset corresponding to this component.|{0|}}
}}@0|@0|0|0|0|0|0|0|@@@1|10|-1|@6|@1|1|{1|0|}}}
{2010203004|XXGoport|115|0|303|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|out|0.0|@@@2448|0|}}
{2010203004|XXGoport|116|0|306|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|F=||{0|}}
}}@0|0|0|0|reject|0.0|@@@1168|0|}}
{2010203004|XXGoport|117|0|309|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|F=||{0|}}
}}@0|0|0|0|error|0.0|@@@1176|0|}}
{2010203004|XXGoport|118|0|312|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|F=||{0|}}
}}@0|0|0|0|log|0.0|@@@1176|0|}}
{2010501005|XXGpvertex|119|0|316|0|{|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|out_metadata|$INPUT_DML|3|2|f$|@{0|}}
{30001002|XXparameter|!prototype_path|$AB_COMPONENTS/Miscellaneous/Redefine_Format.mpc|3|2|Pf$|@{0|}}
{30001002|XXparameter|Layout||3|8|s=|@{0|}}
{30001002|XXparameter|in_metadata||3|8|s=|@{0|}}
}}@0|@375000|128000|395000|148000|121000|68000|1124|Redefine Format|Ab Initio Software|Built-in|1|10|0||6||32769|1|{1|0|}}}
{2010203004|XXGoport|120|0|318|0|{@{}@515000|176000|11000|11000|out|0.0|@@@2448|0|}}
{2010202004|XXGiport|121|0|320|0|{@{}@385000|176000|11000|11000|in|0.0|@@@1936|0|}}
{2010501005|XXGpvertex|122|0|323|0|{Copies data records from its input to its output without changing the values.  Use Redefine Format to change a record format or rename fields.|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|mpname|copy|3|1|Hl||{0|}}
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
{30001002|XXparameter|mpcmodtime|1325635141|1|1|Hl|The last modification time of this component's template|{0|}}
{30001002|XXparameter|_propagate_through||3|1|FHKl|@{0|}}
}}@0|@0|0|0|0|0|0|0|@@@1|10|-1|@6|@1|1|{1|0|}}}
{2010203004|XXGoport|123|0|325|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|out|0.0|@@@2448|0|}}
{2010202004|XXGiport|124|0|328|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|in|0.0|@@@1936|0|}}
{2010501005|XXGpvertex|125|0|332|0|{|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|transform0|out :: reformat(in) =
begin
  out.value :: reinterpret_as(string(big endian integer(4)),in.request_body.value);
end;|3|1|l|@{0|}}
{30001002|XXparameter|out0_metadata|record
  string("\\n") value;
end;|3|1|l|@{0|}}
{30001002|XXparameter|error0_metadata|string('\\n')|3|1|l|@{0|}}
{30001002|XXparameter|log_metadata|record string("\|") node, timestamp, component, subcomponent, event_type; string("\|\\n") event_text; end|3|1|l|@{0|}}
{30001002|XXparameter|!prototype_path|$AB_COMPONENTS/Transform/Reformat.mpc|3|2|Pf$|@{0|}}
{30001002|XXparameter|Layout||3|8|s=|@{0|}}
{30001002|XXparameter|in_metadata||3|8|s=|@{0|}}
{30001002|XXparameter|reject0_metadata||3|8|s=|@{0|}}
}}@0|@205674|30000|226000|50000|133000|65000|1127|Reformat|Ab Initio Software|Built-in|1|10|0||6||32769|1|{1|0|}}}
{2010203004|XXGoport|126|0|334|0|{@{}@358000|77000|11000|11000|out0|0.0|count|out|0|2448|0|}}
{2010203004|XXGoport|127|0|337|0|{@{}@0|0|0|0|reject0|0.0|count|reject|0|1168|0|}}
{2010203004|XXGoport|128|0|339|0|{@{}@0|0|0|0|error0|0.0|count|error|0|1176|0|}}
{2010203004|XXGoport|129|0|341|0|{@{}@0|0|0|0|log|0.0|@@@1176|0|}}
{2010202004|XXGiport|130|0|343|0|{@{}@216000|77000|11000|11000|in|0.0|@@@1808|0|}}
{2010501005|XXGpvertex|131|0|346|0|{Changes the record format of your data by dropping fields or by using DML expressions to add fields, combine fields, or modify the data.|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|mpname|reformat-transform|3|1|Hl||{0|}}
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
{30001002|XXparameter|mpcmodtime|1325635143|1|1|Hl|The last modification time of this component's template|{0|}}
{30001002|XXparameter|_propagation|metadata type: error = "string('\\\\n')"
metadata type: reject = in
metadata type: log = "record string(\\"\|\\") node, timestamp, component, subcomponent, event_type; string(\\"\|\\\\n\\") event_text; end"|3|1|HKl|@{0|}}
{30001002|XXparameter|_propagate_through||3|1|FHKl|@{0|}}
}}@0|@0|0|0|0|0|0|0|@@@1|10|-1|@6|@1|1|{1|0|}}}
{2010203004|XXGoport|132|0|348|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
{30001002|XXparameter|transform0||8|8|FO=|Reformat transform|{0|}}
{30001002|XXparameter|transform0.index|count|3|9|P|@{0|}}
{30001002|XXparameter|transform0.index_value|0|3|9|P|@{0|}}
{30001002|XXparameter|transform0.index_name|transform|3|9|P|@{0|}}
}}@0|0|0|0|out0|0.0|count|out|0|2448|0|}}
{2010203004|XXGoport|133|0|352|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|F=||{0|}}
}}@0|0|0|0|reject0|0.0|count|reject|0|1168|0|}}
{2010203004|XXGoport|134|0|355|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|F=||{0|}}
}}@0|0|0|0|error0|0.0|count|error|0|1176|0|}}
{2010203004|XXGoport|135|0|358|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|F=||{0|}}
}}@0|0|0|0|log|0.0|@@@1176|0|}}
{2010202004|XXGiport|136|0|361|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|in|0.0|@@@1808|0|}}
{2010203004|XXGoport|137|0|368|0|{@{}@271000|84000|11000|11000|out0|.6912442396313364|@@@14736|0|}}
{2010501005|XXGpvertex|138|0|373|0|{|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|condition|$CNDTL_LAST_EXTRACT_ROLLUP|3|2|$|@{0|}}
{30001002|XXparameter|!prototype_path|$AB_COMPONENTS/Miscellaneous/Replicate.mpc|3|2|Pf$|@{0|}}
{30001002|XXparameter|Layout||3|8|s=|@{0|}}
{30001002|XXparameter|in_metadata||3|8|s=|@{0|}}
{30001002|XXparameter|out_metadata||3|8|s=|@{0|}}
}}@0|@352695|119922|373000|140000|131000|65000|1133|Replicate|Ab Initio Software|Built-in|1|10|0||6||32769|1|{1|0|}}}
{2010203004|XXGoport|139|0|375|0|{@{}@503000|162000|11000|21000|out|0.0|@@@2068|0|}}
{2010202004|XXGiport|140|0|379|0|{@{}@363000|162000|11000|21000|in|0.0|@@@1812|0|}}
{2010501005|XXGpvertex|141|0|383|0|{Arbitrarily combines all the data records it receives into a single flow and writes a copy of that flow to each of its output flows.|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|mpname|broadcast|3|1|Hl||{0|}}
{30001002|XXparameter|image__|unitool|3|2|H$||{0|}}
{30001002|XXparameter|NoHidePorts|in out|3|2|H$||{0|}}
{30001002|XXparameter|condition||3|2|F$||{0|}}
{30001002|XXparameter|conditionInputPort|in|3|2|F$||{0|}}
{30001002|XXparameter|conditionOutputPort|out|3|2|F$||{0|}}
{30001002|XXparameter|condition_interpretation|Replace with flow|15|1|Fl||{2|Replace with flow|Remove completely|}}
{30001002|XXparameter|condition_interpretation.display_name|condition-interpretation|3|9|P|@{0|}}
{30001002|XXparameter|port_analysis|out=exact(in)|3|2|H$||{0|}}
{30001002|XXparameter|continuous_analysis|broadcast, publisher|3|2|H$||{0|}}
{30001002|XXparameter|Layout|@9|2|RFs$||{0|}}
{30001002|XXparameter|in_metadata||7|1|RFsl||{0|}}
{30001002|XXparameter|out_metadata||7|1|RFsl||{0|}}
{30001002|XXparameter|doc_transform||8|2|FHs$|Document your transformation for dependency analysis|{0|}}
{30001002|XXparameter|doc_operation1|out::document(in)|3|2|RH$||{0|}}
{30001002|XXparameter|mpcmodtime|1299550339|1|1|Hl|The last modification time of this component's template|{0|}}
{30001002|XXparameter|_propagate_through|metadata type: out = in
metadata type: in = out|3|1|FHKl|@{0|}}
}}@0|@0|0|0|0|0|0|0|@@@1|10|-1|@6|@1|1|{1|0|}}}
{2010203004|XXGoport|142|0|385|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|out|0.0|@@@2068|0|}}
{2010202004|XXGiport|143|0|388|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|in|0.0|@@@1812|0|}}
{2010501005|XXGpvertex|144|0|395|0|{|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|num_records|1|3|2|$|@{0|}}
{30001002|XXparameter|command_line|-expression $ROLLUP_FIELD '$FROM_EXTRACT_VALUE'|3|2|$|@{0|}}
{30001002|XXparameter|condition|$CNDTL_LAST_EXTRACT_ROLLUP|3|2|$|@{0|}}
{30001002|XXparameter|!prototype_path|$AB_COMPONENTS/Validate/Generate_Records.mpc|3|2|Pf$|@{0|}}
{30001002|XXparameter|Layout||3|8|s=|@{0|}}
{30001002|XXparameter|out_metadata||3|8|s=|@{0|}}
}}@0|@262431|207684|282000|228000|149000|106000|1064|Send From Extract Value to Rollup|Ab Initio Software|Built-in 2.0:|1|10|0||6||32769|1|{1|0|}}}
{2010203004|XXGoport|145|0|397|0|{@{}@430000|275000|11000|11000|out|0.0|@@@2448|0|}}
{2010501005|XXGpvertex|146|0|400|0|{Generates a specified number of data records with fields of specified lengths and types.|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|mpname|generate|3|1|Hl||{0|}}
{30001002|XXparameter|image__|/~ab_home/bin/unitool|3|2|H$||{0|}}
{30001002|XXparameter|num_records||1|2|RFO$|Number of records to generate|{0|}}
{30001002|XXparameter|command_line||16|2|FO$|Field-specific options|{0|}}
{30001002|XXparameter|do_timed_checkpoints|False|13|1|FKl|Generate checkpoints at timed intervals|{0|}}
{30001002|XXparameter|do_timed_checkpoints.display_name|do-timed-checkpoints|3|9|P|@{0|}}
{30001002|XXparameter|do_timed_checkpoints.keyword|do-timed-checkpoints|3|9|P|@{0|}}
{30001002|XXparameter|condition||3|2|F$||{0|}}
{30001002|XXparameter|conditionInputPort||3|2|F$||{0|}}
{30001002|XXparameter|conditionOutputPort||3|2|F$||{0|}}
{30001002|XXparameter|condition_interpretation|Remove completely|15|1|Fl||{2|Replace with flow|Remove completely|}}
{30001002|XXparameter|condition_interpretation.display_name|condition-interpretation|3|9|P|@{0|}}
{30001002|XXparameter|continuous_analysis||3|2|H$||{0|}}
{30001002|XXparameter|Layout|@9|2|RFs$||{0|}}
{30001002|XXparameter|out_metadata||7|1|RFsl||{0|}}
{30001002|XXparameter|doc_transform||8|2|FHs$|Document your transformation for dependency analysis|{0|}}
{30001002|XXparameter|doc_operation1|out::document()|3|2|RH$||{0|}}
{30001002|XXparameter|mpcmodtime|1299550340|1|1|Hl|The last modification time of this component's template|{0|}}
}}@0|@0|0|0|0|0|0|0|@@@1|10|-1|@6|@1|1|{1|0|}}}
{2010203004|XXGoport|147|0|402|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|out|0.0|@@@2448|0|}}
{2010501005|XXGpvertex|148|0|407|0|{|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|num_records|1|3|2|$|@{0|}}
{30001002|XXparameter|condition|$CNDTL_LAST_EXTRACT_VARIABLE|3|2|$|@{0|}}
{30001002|XXparameter|out_metadata|record
  string(1) a;
end;|3|1|l|@{0|}}
{30001002|XXparameter|!prototype_path|$AB_COMPONENTS/Validate/Generate_Records.mpc|3|2|Pf$|@{0|}}
{30001002|XXparameter|Layout||3|8|s=|@{0|}}
}}@1|@11343|550488|31000|570000|132000|106000|938|Send Primer Row to Reformat|Ab Initio Software|Built-in 2.0:|1|10|0||6||32769|1|{1|0|}}}
{2010203004|XXGoport|149|0|409|0|{@{}@162000|617000|11000|11000|out|0.0|@@@2448|0|}}
{2010501005|XXGpvertex|150|0|412|0|{Generates a specified number of data records with fields of specified lengths and types.|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|mpname|generate|3|1|Hl||{0|}}
{30001002|XXparameter|image__|/~ab_home/bin/unitool|3|2|H$||{0|}}
{30001002|XXparameter|num_records||1|2|RFO$|Number of records to generate|{0|}}
{30001002|XXparameter|command_line||16|2|FO$|Field-specific options|{0|}}
{30001002|XXparameter|do_timed_checkpoints|False|13|1|FKl|Generate checkpoints at timed intervals|{0|}}
{30001002|XXparameter|do_timed_checkpoints.display_name|do-timed-checkpoints|3|9|P|@{0|}}
{30001002|XXparameter|do_timed_checkpoints.keyword|do-timed-checkpoints|3|9|P|@{0|}}
{30001002|XXparameter|condition||3|2|F$||{0|}}
{30001002|XXparameter|conditionInputPort||3|2|F$||{0|}}
{30001002|XXparameter|conditionOutputPort||3|2|F$||{0|}}
{30001002|XXparameter|condition_interpretation|Remove completely|15|1|Fl||{2|Replace with flow|Remove completely|}}
{30001002|XXparameter|condition_interpretation.display_name|condition-interpretation|3|9|P|@{0|}}
{30001002|XXparameter|continuous_analysis||3|2|H$||{0|}}
{30001002|XXparameter|Layout|@9|2|RFs$||{0|}}
{30001002|XXparameter|out_metadata||7|1|RFl||{0|}}
{30001002|XXparameter|doc_transform||8|2|FHs$|Document your transformation for dependency analysis|{0|}}
{30001002|XXparameter|doc_operation1|out::document()|3|2|RH$||{0|}}
{30001002|XXparameter|mpcmodtime|1299550340|1|1|Hl|The last modification time of this component's template|{0|}}
}}@0|@0|0|0|0|0|0|0|@@@1|10|-1|@6|@1|1|{1|0|}}}
{2010203004|XXGoport|151|0|414|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|out|0.0|@@@2448|0|}}
{2010501005|XXGpvertex|152|0|417|0|{|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|transform0|out::reformat(in) =
begin
  out.last_extract_value :: $TO_EXTRACT_VALUE;
end;|3|1|l|@{0|}}
{30001002|XXparameter|condition|$CNDTL_LAST_EXTRACT_VARIABLE|3|2|$|@{0|}}
{30001002|XXparameter|condition_interpretation|Remove completely|3|1|l|@{0|}}
{30001002|XXparameter|Layout|$[[record kind 85 subkind 0 parts [vector _interp_("file:$LAST_EXTRACT_VALUE_FILE", "dollar_substitution")]]]|3|9||@{0|}}
{30001002|XXparameter|error0_metadata|string('\\n')|3|1|l|@{0|}}
{30001002|XXparameter|log_metadata|record string("\|") node, timestamp, component, subcomponent, event_type; string("\|\\n") event_text; end|3|1|l|@{0|}}
{30001002|XXparameter|!prototype_path|$AB_COMPONENTS/Transform/Reformat.mpc|3|2|Pf$|@{0|}}
{30001002|XXparameter|in_metadata||3|8|s=|@{0|}}
{30001002|XXparameter|out0_metadata||3|8|s=|@{0|}}
{30001002|XXparameter|reject0_metadata||3|8|s=|@{0|}}
}}@1|@276800|552142|297000|572000|157000|106000|1019|Update Last Extract Value File|Ab Initio Software|Built-in|1|10|0||6||32769|1|{1|0|}}}
{2010203004|XXGoport|153|0|419|0|{@{}@453000|619000|11000|11000|out0|0.0|count|out|0|2448|0|}}
{2010203004|XXGoport|154|0|422|0|{@{}@0|0|0|0|reject0|0.0|count|reject|0|1168|0|}}
{2010203004|XXGoport|155|0|424|0|{@{}@0|0|0|0|error0|0.0|count|error|0|1176|0|}}
{2010203004|XXGoport|156|0|426|0|{@{}@0|0|0|0|log|0.0|@@@1176|0|}}
{2010202004|XXGiport|157|0|428|0|{@{}@287000|619000|11000|11000|in|0.0|@@@1808|0|}}
{2010501005|XXGpvertex|158|0|431|0|{Changes the record format of your data by dropping fields or by using DML expressions to add fields, combine fields, or modify the data.|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|mpname|reformat-transform|3|1|Hl||{0|}}
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
{30001002|XXparameter|Layout|@9|2|RF$||{0|}}
{30001002|XXparameter|main_mp_port|count out|3|1|l||{0|}}
{30001002|XXparameter|in_metadata||7|1|RFsl||{0|}}
{30001002|XXparameter|out0_metadata||7|1|RFsl||{0|}}
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
{2010203004|XXGoport|159|0|433|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
{30001002|XXparameter|transform0||8|8|FO=|Reformat transform|{0|}}
{30001002|XXparameter|transform0.index|count|3|9|P|@{0|}}
{30001002|XXparameter|transform0.index_value|0|3|9|P|@{0|}}
{30001002|XXparameter|transform0.index_name|transform|3|9|P|@{0|}}
}}@0|0|0|0|out0|0.0|count|out|0|2448|0|}}
{2010203004|XXGoport|160|0|437|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|F=||{0|}}
}}@0|0|0|0|reject0|0.0|count|reject|0|1168|0|}}
{2010203004|XXGoport|161|0|440|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|F=||{0|}}
}}@0|0|0|0|error0|0.0|count|error|0|1176|0|}}
{2010203004|XXGoport|162|0|443|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|F=||{0|}}
}}@0|0|0|0|log|0.0|@@@1176|0|}}
{2010202004|XXGiport|163|0|446|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|in|0.0|@@@1808|0|}}
{2010501005|XXGpvertex|164|0|453|0|{|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|LogFile|$[[record kind 85 subkind 0 parts [vector _interp_("$XFORM_ERROR_FILE", "dollar_substitution")]]]|3|9||@{0|}}
{30001002|XXparameter|condition|$CNDTL_EXTRACT_REFORMAT|3|2|$|@{0|}}
{30001002|XXparameter|!prototype_path|$AB_COMPONENTS/Miscellaneous/Gather_Logs.mpc|3|2|Pf$|@{0|}}
{30001002|XXparameter|in_metadata||3|8|s=|@{0|}}
}}@0|@1127000|149000|1147000|169000|99000|97000|1154|XForm Error File|Ab Initio Software|Built-in|1|10|0||6||32769|1|{1|0|}}}
{2010202004|XXGiport|165|0|455|0|{@{}@1191000|159000|11000|11000|in|0.0|@@@786|0|}}
{2010501005|XXGpvertex|166|0|458|0|{Collects the output from log ports of components for analysis of a graph after execution.|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|mpname|logger|3|1|Hl||{0|}}
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
{30001002|XXparameter|mpcmodtime|1325635141|1|1|Hl|The last modification time of this component's template|{0|}}
}}@0|@0|0|0|0|0|0|0|@@@1|10|-1|@6|@1|1|{1|0|}}}
{2010202004|XXGiport|167|0|460|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|in|0.0|@@@786|0|}}
{2010501005|XXGpvertex|168|0|464|0|{|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|LogFile|$[[record kind 85 subkind 0 parts [vector _interp_("$XFORM_REJECT_FILE", "dollar_substitution")]]]|3|9||@{0|}}
{30001002|XXparameter|condition|$CNDTL_EXTRACT_REFORMAT|3|2|$|@{0|}}
{30001002|XXparameter|!prototype_path|$AB_COMPONENTS/Miscellaneous/Gather_Logs.mpc|3|2|Pf$|@{0|}}
{30001002|XXparameter|in_metadata||3|8|s=|@{0|}}
}}@0|@995000|148000|1015000|168000|101000|97000|1152|XForm Reject File|Ab Initio Software|Built-in|1|10|0||6||32769|1|{1|0|}}}
{2010202004|XXGiport|169|0|466|0|{@{}@1060000|158000|11000|11000|in|0.0|@@@786|0|}}
{2010501005|XXGpvertex|170|0|469|0|{Collects the output from log ports of components for analysis of a graph after execution.|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|mpname|logger|3|1|Hl||{0|}}
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
{30001002|XXparameter|mpcmodtime|1325635141|1|1|Hl|The last modification time of this component's template|{0|}}
}}@0|@0|0|0|0|0|0|0|@@@1|10|-1|@6|@1|1|{1|0|}}}
{2010202004|XXGiport|171|0|471|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|in|0.0|@@@786|0|}}
{2010110002|XXGconstant|172|0|475|0|{@{}@{1|1039820|}}}
{2010002001|XXGdirectory_object_object|0|1|2|0|{XXGgraph|}0|1|}
{2010704001|XXGgraph_graphinfo_graphinfo|1|0|2|0|{}1|2|}
{2010706001|XXGgraphinfo_runsettings_runsettings|2|0|4|0|{}2|3|}
{2010109001|XXGobject_property_value|3|0|6|0|{0|legend_face|92|}2|4|}
{2010605001|XXGgraph_legend_legend|4|0|8|0|{0|}1|5|}
{2010241001|XXGlegend_face_face|5|0|10|0|{}5|6|}
{2010605001|XXGgraph_legend_legend|6|0|12|0|{1|}1|7|}
{2010241001|XXGlegend_face_face|7|0|14|0|{}7|8|}
{2010604001|XXGgraph_flow_flow|8|0|16|0|{Flow_1|}1|9|}
{2010604001|XXGgraph_flow_flow|9|0|18|0|{Flow_10|}1|10|}
{2010604001|XXGgraph_flow_flow|10|0|20|0|{Flow_11|}1|11|}
{2010604001|XXGgraph_flow_flow|11|0|22|0|{Flow_12|}1|12|}
{2010604001|XXGgraph_flow_flow|12|0|24|0|{Flow_16|}1|13|}
{2010604001|XXGgraph_flow_flow|13|0|26|0|{Flow_2|}1|14|}
{2010604001|XXGgraph_flow_flow|14|0|28|0|{Flow_21|}1|15|}
{2010604001|XXGgraph_flow_flow|15|0|30|0|{Flow_22|}1|16|}
{2010604001|XXGgraph_flow_flow|16|0|32|0|{Flow_23|}1|17|}
{2010604001|XXGgraph_flow_flow|17|0|34|0|{Flow_24|}1|18|}
{2010604001|XXGgraph_flow_flow|18|0|36|0|{Flow_3|}1|19|}
{2010604001|XXGgraph_flow_flow|19|0|38|0|{Flow_4|}1|20|}
{2010604001|XXGgraph_flow_flow|20|0|40|0|{Flow_5|}1|21|}
{2010604001|XXGgraph_flow_flow|21|0|42|0|{Flow_6|}1|22|}
{2010604001|XXGgraph_flow_flow|22|0|44|0|{Flow_7|}1|23|}
{2010604001|XXGgraph_flow_flow|23|0|46|0|{Flow_8|}1|24|}
{2010604001|XXGgraph_flow_flow|24|0|48|0|{Flow_9|}1|25|}
{2010601001|XXGgraph_vertex_vertex|25|0|50|0|{Capture_out_record_count_from_Checksum|}1|26|}
{2010212001|XXGvertex_oport_oport|26|0|52|0|{0|out0|}26|27|}
{2010213001|XXGoport_dst_flow|27|0|54|0|{0|}27|25|}
{2010212001|XXGvertex_oport_oport|28|0|55|0|{1|reject0|}26|28|}
{2010212001|XXGvertex_oport_oport|29|0|57|0|{2|error0|}26|29|}
{2010212001|XXGvertex_oport_oport|30|0|59|0|{3|log|}26|30|}
{2010211001|XXGvertex_iport_iport|31|0|61|0|{0|in|}26|31|}
{2010214001|XXGiport_src_flow|32|0|63|0|{0|}31|24|}
{2010007001|XXGobject_proto_object|33|0|64|0|{}26|32|}
{2010212001|XXGvertex_oport_oport|34|0|66|0|{0|out0|}32|33|}
{2010006001|XXGobject_psameas_object|35|0|68|0|{metadata|out0_metadata|1|0|}33|32|}
{2010006001|XXGobject_psameas_object|36|0|69|0|{transform0|transform0|1|0|}33|32|}
{2010212001|XXGvertex_oport_oport|37|0|70|0|{1|reject0|}32|34|}
{2010006001|XXGobject_psameas_object|38|0|72|0|{metadata|reject0_metadata|1|0|}34|32|}
{2010212001|XXGvertex_oport_oport|39|0|73|0|{2|error0|}32|35|}
{2010006001|XXGobject_psameas_object|40|0|75|0|{metadata|error0_metadata|1|0|}35|32|}
{2010212001|XXGvertex_oport_oport|41|0|76|0|{3|log|}32|36|}
{2010006001|XXGobject_psameas_object|42|0|78|0|{metadata|log_metadata|1|0|}36|32|}
{2010211001|XXGvertex_iport_iport|43|0|79|0|{0|in|}32|37|}
{2010006001|XXGobject_psameas_object|44|0|81|0|{metadata|in_metadata|1|0|}37|32|}
{2010006001|XXGobject_psameas_object|45|0|82|0|{Layout|Layout|0|0|}26|38|}
{2010211001|XXGvertex_iport_iport|46|0|84|0|{0|write|}38|39|}
{2010214001|XXGiport_src_flow|47|0|86|0|{0|}39|25|}
{2010007001|XXGobject_proto_object|48|0|87|0|{}38|40|}
{2010211001|XXGvertex_iport_iport|49|0|89|0|{0|write|}40|41|}
{2010006001|XXGobject_psameas_object|50|0|91|0|{metadata|write_metadata|1|0|}41|40|}
{2010006001|XXGobject_psameas_object|51|0|92|0|{in_metadata|out_metadata|0|0|}26|42|}
{2010212001|XXGvertex_oport_oport|52|0|94|0|{0|out|}42|43|}
{2010213001|XXGoport_dst_flow|53|0|96|0|{0|}43|24|}
{2010211001|XXGvertex_iport_iport|54|0|97|0|{0|in|}42|44|}
{2010214001|XXGiport_src_flow|55|0|99|0|{0|}44|23|}
{2010007001|XXGobject_proto_object|56|0|100|0|{}42|45|}
{2010212001|XXGvertex_oport_oport|57|0|102|0|{0|out|}45|46|}
{2010006001|XXGobject_psameas_object|58|0|104|0|{metadata|out_metadata|1|0|}46|45|}
{2010211001|XXGvertex_iport_iport|59|0|105|0|{0|in|}45|47|}
{2010006001|XXGobject_psameas_object|60|0|107|0|{metadata|in_metadata|1|0|}47|45|}
{2010006001|XXGobject_psameas_object|61|0|108|0|{Layout|Layout|0|0|}42|38|}
{2010006001|XXGobject_psameas_object|62|0|109|0|{in_metadata|in_metadata|0|0|}42|48|}
{2010212001|XXGvertex_oport_oport|63|0|111|0|{0|out0|}48|49|}
{2010213001|XXGoport_dst_flow|64|0|113|0|{0|}49|15|}
{2010212001|XXGvertex_oport_oport|65|0|114|0|{1|reject0|}48|50|}
{2010213001|XXGoport_dst_flow|66|0|116|0|{0|}50|10|}
{2010212001|XXGvertex_oport_oport|67|0|117|0|{2|error0|}48|51|}
{2010213001|XXGoport_dst_flow|68|0|119|0|{0|}51|14|}
{2010212001|XXGvertex_oport_oport|69|0|120|0|{3|log|}48|52|}
{2010211001|XXGvertex_iport_iport|70|0|122|0|{0|in|}48|53|}
{2010214001|XXGiport_src_flow|71|0|124|0|{0|}53|17|}
{2010007001|XXGobject_proto_object|72|0|125|0|{}48|54|}
{2010212001|XXGvertex_oport_oport|73|0|127|0|{0|out0|}54|55|}
{2010006001|XXGobject_psameas_object|74|0|129|0|{metadata|out0_metadata|1|0|}55|54|}
{2010006001|XXGobject_psameas_object|75|0|130|0|{transform0|transform0|1|0|}55|54|}
{2010212001|XXGvertex_oport_oport|76|0|131|0|{1|reject0|}54|56|}
{2010006001|XXGobject_psameas_object|77|0|133|0|{metadata|reject0_metadata|1|0|}56|54|}
{2010212001|XXGvertex_oport_oport|78|0|134|0|{2|error0|}54|57|}
{2010006001|XXGobject_psameas_object|79|0|136|0|{metadata|error0_metadata|1|0|}57|54|}
{2010212001|XXGvertex_oport_oport|80|0|137|0|{3|log|}54|58|}
{2010006001|XXGobject_psameas_object|81|0|139|0|{metadata|log_metadata|1|0|}58|54|}
{2010211001|XXGvertex_iport_iport|82|0|140|0|{0|in|}54|59|}
{2010006001|XXGobject_psameas_object|83|0|142|0|{metadata|in_metadata|1|0|}59|54|}
{2010006001|XXGobject_psameas_object|84|0|143|0|{Layout|Layout|0|0|}48|60|}
{2010211001|XXGvertex_iport_iport|85|0|145|0|{0|write|}60|61|}
{2010214001|XXGiport_src_flow|86|0|147|0|{0|}61|16|}
{2010007001|XXGobject_proto_object|87|0|148|0|{}60|62|}
{2010211001|XXGvertex_iport_iport|88|0|150|0|{0|write|}62|63|}
{2010006001|XXGobject_psameas_object|89|0|152|0|{metadata|write_metadata|1|0|}63|62|}
{2010006001|XXGobject_psameas_object|90|0|153|0|{write_metadata|out_metadata|0|0|}60|64|}
{2010212001|XXGvertex_oport_oport|91|0|155|0|{0|out|}64|65|}
{2010213001|XXGoport_dst_flow|92|0|157|0|{0|}65|17|}
{2010213001|XXGoport_dst_flow|93|0|158|0|{1|}65|23|}
{2010211001|XXGvertex_iport_iport|94|0|159|0|{0|in|}64|66|}
{2010214001|XXGiport_src_flow|95|0|161|0|{0|}66|11|}
{2010007001|XXGobject_proto_object|96|0|162|0|{}64|67|}
{2010212001|XXGvertex_oport_oport|97|0|164|0|{0|out|}67|68|}
{2010006001|XXGobject_psameas_object|98|0|166|0|{metadata|out_metadata|1|0|}68|67|}
{2010211001|XXGvertex_iport_iport|99|0|167|0|{0|in|}67|69|}
{2010006001|XXGobject_psameas_object|100|0|169|0|{metadata|in_metadata|1|0|}69|67|}
{2010006001|XXGobject_psameas_object|101|0|170|0|{Layout|Layout|0|0|}64|60|}
{2010006001|XXGobject_psameas_object|102|0|171|0|{in_metadata|in_metadata|0|0|}64|48|}
{2010006001|XXGobject_psameas_object|103|0|172|0|{out_metadata|in_metadata|0|0|}64|48|}
{2010006001|XXGobject_psameas_object|104|0|173|0|{reject0_metadata|in_metadata|0|0|}48|48|}
{2010006001|XXGobject_psameas_object|105|0|174|0|{out0_metadata|write_metadata|0|0|}26|38|}
{2010006001|XXGobject_psameas_object|106|0|175|0|{reject0_metadata|out_metadata|0|0|}26|42|}
{2010601001|XXGgraph_vertex_vertex|107|0|176|0|{Compute_Checksum_of_Input_Flow|}1|42|}
{2010601001|XXGgraph_vertex_vertex|108|0|177|0|{Conditional_Extract_Reformat|}1|48|}
{2010601001|XXGgraph_vertex_vertex|109|0|178|0|{Conditional_Partition_by_Round_robin|}1|70|}
{2010212001|XXGvertex_oport_oport|110|0|180|0|{0|out|}70|71|}
{2010213001|XXGoport_dst_flow|111|0|182|0|{0|}71|11|}
{2010211001|XXGvertex_iport_iport|112|0|183|0|{0|in|}70|72|}
{2010214001|XXGiport_src_flow|113|0|185|0|{0|}72|13|}
{2010007001|XXGobject_proto_object|114|0|186|0|{}70|73|}
{2010212001|XXGvertex_oport_oport|115|0|188|0|{0|out|}73|74|}
{2010006001|XXGobject_psameas_object|116|0|190|0|{metadata|out_metadata|1|0|}74|73|}
{2010211001|XXGvertex_iport_iport|117|0|191|0|{0|in|}73|75|}
{2010006001|XXGobject_psameas_object|118|0|193|0|{metadata|in_metadata|1|0|}75|73|}
{2010006001|XXGobject_psameas_object|119|0|194|0|{in_metadata|out_metadata|0|0|}70|76|}
{2010212001|XXGvertex_oport_oport|120|0|196|0|{0|out|}76|77|}
{2010213001|XXGoport_dst_flow|121|0|198|0|{0|}77|18|}
{2010007001|XXGobject_proto_object|122|0|199|0|{}76|78|}
{2010212001|XXGvertex_oport_oport|123|0|201|0|{0|out|}78|79|}
{2010006001|XXGobject_psameas_object|124|0|203|0|{metadata|out_metadata|1|0|}79|78|}
{2010006001|XXGobject_psameas_object|125|0|204|0|{Layout|Layout|0|0|}76|70|}
{2010006001|XXGobject_psameas_object|126|0|205|0|{out_metadata|out_metadata|0|0|}70|76|}
{2010601001|XXGgraph_vertex_vertex|127|0|206|0|{Deflate|}1|80|}
{2010212001|XXGvertex_oport_oport|128|0|208|0|{0|out|}80|81|}
{2010213001|XXGoport_dst_flow|129|0|210|0|{0|}81|16|}
{2010211001|XXGvertex_iport_iport|130|0|211|0|{0|in|}80|82|}
{2010214001|XXGiport_src_flow|131|0|213|0|{0|}82|15|}
{2010007001|XXGobject_proto_object|132|0|214|0|{}80|83|}
{2010212001|XXGvertex_oport_oport|133|0|216|0|{0|out|}83|84|}
{2010006001|XXGobject_psameas_object|134|0|218|0|{metadata|out_metadata|1|0|}84|83|}
{2010211001|XXGvertex_iport_iport|135|0|219|0|{0|in|}83|85|}
{2010006001|XXGobject_psameas_object|136|0|221|0|{metadata|in_metadata|1|0|}85|83|}
{2010006001|XXGobject_psameas_object|137|0|222|0|{Layout|Layout|0|0|}80|60|}
{2010006001|XXGobject_psameas_object|138|0|223|0|{in_metadata|out0_metadata|0|0|}80|48|}
{2010006001|XXGobject_psameas_object|139|0|224|0|{out_metadata|out_metadata|0|0|}80|64|}
{2010601001|XXGgraph_vertex_vertex|140|0|225|0|{Get_Max_Value_for_Extract_Field|}1|86|}
{2010212001|XXGvertex_oport_oport|141|0|227|0|{0|out|}86|87|}
{2010213001|XXGoport_dst_flow|142|0|229|0|{0|}87|22|}
{2010212001|XXGvertex_oport_oport|143|0|230|0|{1|reject|}86|88|}
{2010212001|XXGvertex_oport_oport|144|0|232|0|{2|error|}86|89|}
{2010212001|XXGvertex_oport_oport|145|0|234|0|{3|log|}86|90|}
{2010211001|XXGvertex_iport_iport|146|0|236|0|{0|in|}86|91|}
{2010214001|XXGiport_src_flow|147|0|238|0|{0|}91|21|}
{2010214001|XXGiport_src_flow|148|0|239|0|{1|}91|19|}
{2010007001|XXGobject_proto_object|149|0|240|0|{}86|92|}
{2010212001|XXGvertex_oport_oport|150|0|242|0|{0|out|}92|93|}
{2010006001|XXGobject_psameas_object|151|0|244|0|{metadata|out_metadata|1|0|}93|92|}
{2010212001|XXGvertex_oport_oport|152|0|245|0|{1|reject|}92|94|}
{2010006001|XXGobject_psameas_object|153|0|247|0|{metadata|reject_metadata|1|0|}94|92|}
{2010212001|XXGvertex_oport_oport|154|0|248|0|{2|error|}92|95|}
{2010006001|XXGobject_psameas_object|155|0|250|0|{metadata|error_metadata|1|0|}95|92|}
{2010212001|XXGvertex_oport_oport|156|0|251|0|{3|log|}92|96|}
{2010006001|XXGobject_psameas_object|157|0|253|0|{metadata|log_metadata|1|0|}96|92|}
{2010211001|XXGvertex_iport_iport|158|0|254|0|{0|in|}92|97|}
{2010006001|XXGobject_psameas_object|159|0|256|0|{metadata|in_metadata|1|0|}97|92|}
{2010006001|XXGobject_psameas_object|160|0|257|0|{Layout|Layout|0|0|}86|98|}
{2010211001|XXGvertex_iport_iport|161|0|259|0|{0|write|}98|99|}
{2010214001|XXGiport_src_flow|162|0|261|0|{0|}99|22|}
{2010007001|XXGobject_proto_object|163|0|262|0|{}98|100|}
{2010211001|XXGvertex_iport_iport|164|0|264|0|{0|write|}100|101|}
{2010006001|XXGobject_psameas_object|165|0|266|0|{metadata|write_metadata|1|0|}101|100|}
{2010006001|XXGobject_psameas_object|166|0|267|0|{in_metadata|out_metadata|0|0|}86|76|}
{2010006001|XXGobject_psameas_object|167|0|268|0|{out_metadata|write_metadata|0|0|}86|98|}
{2010006001|XXGobject_psameas_object|168|0|269|0|{reject_metadata|out_metadata|0|0|}86|76|}
{2010601001|XXGgraph_vertex_vertex|169|0|270|0|{Hadoop_Input_Dataset|}1|76|}
{2010601001|XXGgraph_vertex_vertex|170|0|271|0|{Last_Extract_Value_File|}1|102|}
{2010211001|XXGvertex_iport_iport|171|0|273|0|{0|write|}102|103|}
{2010214001|XXGiport_src_flow|172|0|275|0|{0|}103|12|}
{2010007001|XXGobject_proto_object|173|0|276|0|{}102|104|}
{2010211001|XXGvertex_iport_iport|174|0|278|0|{0|write|}104|105|}
{2010006001|XXGobject_psameas_object|175|0|280|0|{metadata|write_metadata|1|0|}105|104|}
{2010601001|XXGgraph_vertex_vertex|176|0|281|0|{Last_Extract_Value_File_1|}1|98|}
{2010601001|XXGgraph_vertex_vertex|177|0|282|0|{Output_File|}1|60|}
{2010601001|XXGgraph_vertex_vertex|178|0|283|0|{Parallel_Read_Hadoop|}1|106|}
{2010604001|XXGgraph_flow_flow|179|0|285|0|{Flow_1|}106|107|}
{2010604001|XXGgraph_flow_flow|180|0|287|0|{Flow_2|}106|108|}
{2010601001|XXGgraph_vertex_vertex|181|0|289|0|{Hadoop_Parallel_Read|}106|109|}
{2010212001|XXGvertex_oport_oport|182|0|291|0|{0|out|}109|110|}
{2010213001|XXGoport_dst_flow|183|0|293|0|{0|}110|107|}
{2010212001|XXGvertex_oport_oport|184|0|294|0|{1|reject|}109|111|}
{2010212001|XXGvertex_oport_oport|185|0|296|0|{2|error|}109|112|}
{2010212001|XXGvertex_oport_oport|186|0|298|0|{3|log|}109|113|}
{2010007001|XXGobject_proto_object|187|0|300|0|{}109|114|}
{2010212001|XXGvertex_oport_oport|188|0|302|0|{0|out|}114|115|}
{2010006001|XXGobject_psameas_object|189|0|304|0|{metadata|out_metadata|1|0|}115|114|}
{2010212001|XXGvertex_oport_oport|190|0|305|0|{1|reject|}114|116|}
{2010006001|XXGobject_psameas_object|191|0|307|0|{metadata|reject_metadata|1|0|}116|114|}
{2010212001|XXGvertex_oport_oport|192|0|308|0|{2|error|}114|117|}
{2010006001|XXGobject_psameas_object|193|0|310|0|{metadata|error_metadata|1|0|}117|114|}
{2010212001|XXGvertex_oport_oport|194|0|311|0|{3|log|}114|118|}
{2010006001|XXGobject_psameas_object|195|0|313|0|{metadata|log_metadata|1|0|}118|114|}
{2010006001|XXGobject_psameas_object|196|0|314|0|{Layout|Layout|0|0|}109|70|}
{2010601001|XXGgraph_vertex_vertex|197|0|315|0|{Redefine_Format|}106|119|}
{2010212001|XXGvertex_oport_oport|198|0|317|0|{0|out|}119|120|}
{2010211001|XXGvertex_iport_iport|199|0|319|0|{0|in|}119|121|}
{2010214001|XXGiport_src_flow|200|0|321|0|{0|}121|108|}
{2010007001|XXGobject_proto_object|201|0|322|0|{}119|122|}
{2010212001|XXGvertex_oport_oport|202|0|324|0|{0|out|}122|123|}
{2010006001|XXGobject_psameas_object|203|0|326|0|{metadata|out_metadata|1|0|}123|122|}
{2010211001|XXGvertex_iport_iport|204|0|327|0|{0|in|}122|124|}
{2010006001|XXGobject_psameas_object|205|0|329|0|{metadata|in_metadata|1|0|}124|122|}
{2010006001|XXGobject_psameas_object|206|0|330|0|{Layout|Layout|0|0|}119|70|}
{2010006001|XXGobject_psameas_object|207|0|331|0|{in_metadata|out0_metadata|0|0|}119|125|}
{2010212001|XXGvertex_oport_oport|208|0|333|0|{0|out0|}125|126|}
{2010213001|XXGoport_dst_flow|209|0|335|0|{0|}126|108|}
{2010212001|XXGvertex_oport_oport|210|0|336|0|{1|reject0|}125|127|}
{2010212001|XXGvertex_oport_oport|211|0|338|0|{2|error0|}125|128|}
{2010212001|XXGvertex_oport_oport|212|0|340|0|{3|log|}125|129|}
{2010211001|XXGvertex_iport_iport|213|0|342|0|{0|in|}125|130|}
{2010214001|XXGiport_src_flow|214|0|344|0|{0|}130|107|}
{2010007001|XXGobject_proto_object|215|0|345|0|{}125|131|}
{2010212001|XXGvertex_oport_oport|216|0|347|0|{0|out0|}131|132|}
{2010006001|XXGobject_psameas_object|217|0|349|0|{metadata|out0_metadata|1|0|}132|131|}
{2010006001|XXGobject_psameas_object|218|0|350|0|{transform0|transform0|1|0|}132|131|}
{2010212001|XXGvertex_oport_oport|219|0|351|0|{1|reject0|}131|133|}
{2010006001|XXGobject_psameas_object|220|0|353|0|{metadata|reject0_metadata|1|0|}133|131|}
{2010212001|XXGvertex_oport_oport|221|0|354|0|{2|error0|}131|134|}
{2010006001|XXGobject_psameas_object|222|0|356|0|{metadata|error0_metadata|1|0|}134|131|}
{2010212001|XXGvertex_oport_oport|223|0|357|0|{3|log|}131|135|}
{2010006001|XXGobject_psameas_object|224|0|359|0|{metadata|log_metadata|1|0|}135|131|}
{2010211001|XXGvertex_iport_iport|225|0|360|0|{0|in|}131|136|}
{2010006001|XXGobject_psameas_object|226|0|362|0|{metadata|in_metadata|1|0|}136|131|}
{2010006001|XXGobject_psameas_object|227|0|363|0|{Layout|Layout|0|0|}125|70|}
{2010006001|XXGobject_psameas_object|228|0|364|0|{in_metadata|out_metadata|0|0|}125|109|}
{2010006001|XXGobject_psameas_object|229|0|365|0|{reject0_metadata|out_metadata|0|0|}125|109|}
{2010601001|XXGgraph_vertex_vertex|230|0|366|0|{Reformat|}106|125|}
{2010212001|XXGvertex_oport_oport|231|0|367|0|{0|out0|}106|137|}
{2010216002|XXGoport_binding_oport|232|0|369|0|{.5|.5|{12|525000|181000|545000|181000|578000|181000|578000|170000|612000|170000|632000|170000|}0|}137|120|}
{2010213001|XXGoport_dst_flow|233|0|370|0|{0|}137|9|}
{2010601001|XXGgraph_vertex_vertex|234|0|371|0|{Record_Count_File|}1|38|}
{2010601001|XXGgraph_vertex_vertex|235|0|372|0|{Replicate|}1|138|}
{2010212001|XXGvertex_oport_oport|236|0|374|0|{0|out|}138|139|}
{2010213001|XXGoport_dst_flow|237|0|376|0|{0|}139|13|}
{2010213001|XXGoport_dst_flow|238|0|377|0|{1|}139|21|}
{2010211001|XXGvertex_iport_iport|239|0|378|0|{0|in|}138|140|}
{2010214001|XXGiport_src_flow|240|0|380|0|{0|}140|9|}
{2010214001|XXGiport_src_flow|241|0|381|0|{1|}140|18|}
{2010007001|XXGobject_proto_object|242|0|382|0|{}138|141|}
{2010212001|XXGvertex_oport_oport|243|0|384|0|{0|out|}141|142|}
{2010006001|XXGobject_psameas_object|244|0|386|0|{metadata|out_metadata|1|0|}142|141|}
{2010211001|XXGvertex_iport_iport|245|0|387|0|{0|in|}141|143|}
{2010006001|XXGobject_psameas_object|246|0|389|0|{metadata|in_metadata|1|0|}143|141|}
{2010006001|XXGobject_psameas_object|247|0|390|0|{Layout|Layout|0|0|}138|70|}
{2010006001|XXGobject_psameas_object|248|0|391|0|{in_metadata|out_metadata|0|0|}138|76|}
{2010006001|XXGobject_psameas_object|249|0|392|0|{out_metadata|out_metadata|0|0|}138|76|}
{2010601001|XXGgraph_vertex_vertex|250|0|393|0|{Retain_Flow_to_Flow|}1|64|}
{2010601001|XXGgraph_vertex_vertex|251|0|394|0|{Send_From_Extract_Value_to_Rollup|}1|144|}
{2010212001|XXGvertex_oport_oport|252|0|396|0|{0|out|}144|145|}
{2010213001|XXGoport_dst_flow|253|0|398|0|{0|}145|19|}
{2010007001|XXGobject_proto_object|254|0|399|0|{}144|146|}
{2010212001|XXGvertex_oport_oport|255|0|401|0|{0|out|}146|147|}
{2010006001|XXGobject_psameas_object|256|0|403|0|{metadata|out_metadata|1|0|}147|146|}
{2010006001|XXGobject_psameas_object|257|0|404|0|{Layout|Layout|0|0|}144|98|}
{2010006001|XXGobject_psameas_object|258|0|405|0|{out_metadata|out_metadata|0|0|}144|76|}
{2010601001|XXGgraph_vertex_vertex|259|0|406|0|{Send_Primer_Row_to_Reformat|}1|148|}
{2010212001|XXGvertex_oport_oport|260|0|408|0|{0|out|}148|149|}
{2010213001|XXGoport_dst_flow|261|0|410|0|{0|}149|20|}
{2010007001|XXGobject_proto_object|262|0|411|0|{}148|150|}
{2010212001|XXGvertex_oport_oport|263|0|413|0|{0|out|}150|151|}
{2010006001|XXGobject_psameas_object|264|0|415|0|{metadata|out_metadata|1|0|}151|150|}
{2010006001|XXGobject_psameas_object|265|0|416|0|{Layout|Layout|0|0|}148|152|}
{2010212001|XXGvertex_oport_oport|266|0|418|0|{0|out0|}152|153|}
{2010213001|XXGoport_dst_flow|267|0|420|0|{0|}153|12|}
{2010212001|XXGvertex_oport_oport|268|0|421|0|{1|reject0|}152|154|}
{2010212001|XXGvertex_oport_oport|269|0|423|0|{2|error0|}152|155|}
{2010212001|XXGvertex_oport_oport|270|0|425|0|{3|log|}152|156|}
{2010211001|XXGvertex_iport_iport|271|0|427|0|{0|in|}152|157|}
{2010214001|XXGiport_src_flow|272|0|429|0|{0|}157|20|}
{2010007001|XXGobject_proto_object|273|0|430|0|{}152|158|}
{2010212001|XXGvertex_oport_oport|274|0|432|0|{0|out0|}158|159|}
{2010006001|XXGobject_psameas_object|275|0|434|0|{metadata|out0_metadata|1|0|}159|158|}
{2010006001|XXGobject_psameas_object|276|0|435|0|{transform0|transform0|1|0|}159|158|}
{2010212001|XXGvertex_oport_oport|277|0|436|0|{1|reject0|}158|160|}
{2010006001|XXGobject_psameas_object|278|0|438|0|{metadata|reject0_metadata|1|0|}160|158|}
{2010212001|XXGvertex_oport_oport|279|0|439|0|{2|error0|}158|161|}
{2010006001|XXGobject_psameas_object|280|0|441|0|{metadata|error0_metadata|1|0|}161|158|}
{2010212001|XXGvertex_oport_oport|281|0|442|0|{3|log|}158|162|}
{2010006001|XXGobject_psameas_object|282|0|444|0|{metadata|log_metadata|1|0|}162|158|}
{2010211001|XXGvertex_iport_iport|283|0|445|0|{0|in|}158|163|}
{2010006001|XXGobject_psameas_object|284|0|447|0|{metadata|in_metadata|1|0|}163|158|}
{2010006001|XXGobject_psameas_object|285|0|448|0|{in_metadata|out_metadata|0|0|}152|148|}
{2010006001|XXGobject_psameas_object|286|0|449|0|{out0_metadata|write_metadata|0|0|}152|102|}
{2010006001|XXGobject_psameas_object|287|0|450|0|{reject0_metadata|out_metadata|0|0|}152|148|}
{2010601001|XXGgraph_vertex_vertex|288|0|451|0|{Update_Last_Extract_Value_File|}1|152|}
{2010601001|XXGgraph_vertex_vertex|289|0|452|0|{XForm_Error_File|}1|164|}
{2010211001|XXGvertex_iport_iport|290|0|454|0|{0|in|}164|165|}
{2010214001|XXGiport_src_flow|291|0|456|0|{0|}165|14|}
{2010007001|XXGobject_proto_object|292|0|457|0|{}164|166|}
{2010211001|XXGvertex_iport_iport|293|0|459|0|{0|in|}166|167|}
{2010006001|XXGobject_psameas_object|294|0|461|0|{metadata|in_metadata|1|0|}167|166|}
{2010006001|XXGobject_psameas_object|295|0|462|0|{in_metadata|error0_metadata|0|0|}164|48|}
{2010601001|XXGgraph_vertex_vertex|296|0|463|0|{XForm_Reject_File|}1|168|}
{2010211001|XXGvertex_iport_iport|297|0|465|0|{0|in|}168|169|}
{2010214001|XXGiport_src_flow|298|0|467|0|{0|}169|10|}
{2010007001|XXGobject_proto_object|299|0|468|0|{}168|170|}
{2010211001|XXGvertex_iport_iport|300|0|470|0|{0|in|}170|171|}
{2010006001|XXGobject_psameas_object|301|0|472|0|{metadata|in_metadata|1|0|}171|170|}
{2010006001|XXGobject_psameas_object|302|0|473|0|{in_metadata|in_metadata|0|0|}168|48|}
{2010109001|XXGobject_property_value|303|0|474|0|{0|TrackingThumbprint|72|}1|172|}
