@
{18|Type|XXGrepository|Primary_Name|XXGgraph|_ab_object_schema|0|_ab_semantic_schema|1|eme_flatten_format|1|schema_version|34|root_id|0|ent_count|150|rel_count|254|}
{2010001002|XXGdirectory|0|1|1|0|{@{}@}}
{2010600005|XXGgraph|1|0|1|0|{|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|ETL_ID||3|2|REFO$|subject_area.table_id|{0|}}
{30001002|XXparameter|JOB_ENV||3|2|REFO$||{0|}}
{30001002|XXparameter|FILE_ID||3|2|REFO$|file id retrieved from the sources list file|{0|}}
{30001002|XXparameter|DATA_FILENAME_TMP||3|2|REFO$|data filename retrieved from the sources list file|{0|}}
{30001002|XXparameter|DML_FILENAME|$ETL_ID.read_unload.dml|3|2|EFK$|optional dml file name - used to override the default dml file|{0|}}
{30001002|XXparameter|PARAM1||3|6|EFKk|shell interpretation|{0|}}
{30001002|XXparameter|PARAM2||3|6|EFKk|shell interpretation|{0|}}
{30001002|XXparameter|PARAM3||3|6|EFKk|shell interpretation|{0|}}
{30001002|XXparameter|PARAM4||3|6|EFKk|shell interpretation|{0|}}
{30001002|XXparameter|ETL_CFG_FILE|$DW_CFG/$ETL_ID.cfg|3|2|RE$||{0|}}
{30001002|XXparameter|SUBJECT_AREA|$\{ETL_ID%%.*\}|3|6|REk||{0|}}
{30001002|XXparameter|TABLE_ID|$\{ETL_ID##*.\}|3|6|REk||{0|}}
{30001002|XXparameter|DB_NAME|$(JOB_ENV_UPPER=$(print $JOB_ENV \| tr [:lower:] [:upper:])
  eval print \\$DW_$\{JOB_ENV_UPPER\}_DB)|3|6|REk||{0|}}
{30001002|XXparameter|UNLOAD_UTF8|$(grep "^UNLOAD_UTF8\\>" $ETL_CFG_FILE \| read PARAM VALUE COMMENT; eval print $VALUE)|3|6|REk||{0|}}
{30001002|XXparameter|AB_IDB_CONFIG|$(if [[ $UNLOAD_UTF8 = 1 ]]
  then
     print teradata_$\{DB_NAME\}_utf8.dbc
  else
     print teradata_$\{DB_NAME\}.dbc
  fi
)|3|6|REk|database configuration file retrieved from the sources list file|{0|}}
{30001002|XXparameter|AB_JOB|$(if [ $ETL_ENV ]
then
   print $AB_JOB.$ETL_ID.$FILE_ID.$ETL_ENV.$JOB_ENV
else
   print $AB_JOB.$ETL_ID.$FILE_ID.$JOB_ENV
fi)|3|6|REk||{0|}}
{30001002|XXparameter|DW_SQL|$DW_SQL|3|2|REK$||{0|}}
{30001002|XXparameter|DW_SA_ARC|$DW_ARC/$JOB_ENV/$SUBJECT_AREA|3|2|RE$||{0|}}
{30001002|XXparameter|DW_SA_DAT|$DW_DAT/$JOB_ENV/$SUBJECT_AREA|3|2|RE$||{0|}}
{30001002|XXparameter|DW_SA_LOG|$DW_LOG/$JOB_ENV/$SUBJECT_AREA|3|2|RE$||{0|}}
{30001002|XXparameter|DW_SA_TMP|$DW_TMP/$JOB_ENV/$SUBJECT_AREA|3|2|RE$||{0|}}
{30001002|XXparameter|FILE_DATETIME|$(date '+%Y%m%d-%H%M%S')|3|6|REk||{0|}}
{30001002|XXparameter|CNDTL_UNLOAD_COMPRESS|$(grep "^CNDTL_UNLOAD_COMPRESS\\>" $ETL_CFG_FILE \| read PARAM VALUE COMMENT; print $VALUE)|3|6|REk||{0|}}
{30001002|XXparameter|BATCH_SEQ_NUM|$(( $(<$DW_SA_DAT/$TABLE_ID.unload.batch_seq_num.dat) + 1))|3|6|REk||{0|}}
{30001002|XXparameter|DATA_FILENAME|$(if [[ $DATA_FILENAME_TMP = "N" ]]
  then
     print $(grep "^DATA_FILENAME\\>" $ETL_CFG_FILE \| read PARAM VALUE COMMENT; eval print $VALUE.$BATCH_SEQ_NUM)
  else 
     if [[ $CNDTL_UNLOAD_COMPRESS = 1 ]]
     then 
         eval print $DATA_FILENAME_TMP.gz.$BATCH_SEQ_NUM;
     else
         eval print $DATA_FILENAME_TMP.$BATCH_SEQ_NUM;
     fi
  fi
)|3|6|REk||{0|}}
{30001002|XXparameter|OUT_DIR|$(grep "^OUT_DIR\\>" $ETL_CFG_FILE \| read PARAM VALUE COMMENT; eval print $VALUE/$JOB_ENV/$SUBJECT_AREA)|3|6|REk||{0|}}
{30001002|XXparameter|WRK_DIR|$(grep "^WRK_DIR\\>" $ETL_CFG_FILE \| read PARAM VALUE COMMENT; eval print $VALUE/$SUBJECT_AREA)|3|6|REk||{0|}}
{30001002|XXparameter|LAST_EXTRACT_TYPE|$(grep "^UNLOAD_LAST_EXTRACT_TYPE\\>" $ETL_CFG_FILE \| read PARAM VALUE COMMENT; print $VALUE)|3|6|REk||{0|}}
{30001002|XXparameter|CNDTL_LAST_EXTRACT_ROLLUP|$(if [[ $LAST_EXTRACT_TYPE = "R" ]]; then print 1; else print 0; fi)|3|6|REk||{0|}}
{30001002|XXparameter|CNDTL_LAST_EXTRACT_VARIABLE|$(if [[ $LAST_EXTRACT_TYPE = "V" ]]; then print 1; else print 0; fi)|3|6|REk||{0|}}
{30001002|XXparameter|LAST_EXTRACT_VALUE_FILE|$(if [[ $LAST_EXTRACT_TYPE != "N" ]]
  then
     print $DW_SA_DAT/$TABLE_ID.$FILE_ID.last_unload_value.dat
  else
     print a
  fi
)|3|6|REk||{0|}}
{30001002|XXparameter|FROM_EXTRACT_VALUE|$(if [[ $LAST_EXTRACT_TYPE != "N" ]]
  then
     print $(<$LAST_EXTRACT_VALUE_FILE)
  fi
)|3|6|REk||{0|}}
{30001002|XXparameter|TO_EXTRACT_VALUE_FUNCTION|$(if [[ $LAST_EXTRACT_TYPE = "V" ]]
  then
     print $(grep "^UNLOAD_TO_EXTRACT_VALUE_FUNCTION\\>" $ETL_CFG_FILE \| read PARAM VALUE COMMENT; eval print $VALUE)
  fi
)|3|6|REk||{0|}}
{30001002|XXparameter|TO_EXTRACT_VALUE|$(if [[ $LAST_EXTRACT_TYPE = "V" ]]
  then
     print $($TO_EXTRACT_VALUE_FUNCTION)
  fi
)|3|6|REk||{0|}}
{30001002|XXparameter|CREATE_TMP_SQL_FILE|$(set -e
  print "cat <<EOF" > $DW_SA_TMP/$TABLE_ID.unload.$FILE_ID.unload.sql.tmp
  cat $DW_SQL/$ETL_ID.unload.sql >> $DW_SA_TMP/$TABLE_ID.unload.$FILE_ID.unload.sql.tmp
  print -- "--END-OF-SQL" >> $DW_SA_TMP/$TABLE_ID.unload.$FILE_ID.unload.sql.tmp
  print "\\nEOF" >> $DW_SA_TMP/$TABLE_ID.unload.$FILE_ID.unload.sql.tmp
  set +e)|3|6|REk||{0|}}
{30001002|XXparameter|INPUT_TABLE_SEL1|$(. $DW_SA_TMP/$TABLE_ID.unload.$FILE_ID.unload.sql.tmp)|3|6|REk||{0|}}
{30001002|XXparameter|CHECK_SQL_FILE|$(
set +e
print $INPUT_TABLE_SEL1 \| grep "END-OF-SQL"
err=$?
set -e

if [ $err -eq 1 ]
then
        print "SQL File is not integrated : $DW_SA_TMP/$TABLE_ID.unload.$FILE_ID.unload.sql.tmp" >&2
        exit 1
fi
)|3|6|REKk||{0|}}
{30001002|XXparameter|INPUT_TABLE_SEL|$\{INPUT_TABLE_SEL1%%--END-OF-SQL*\}|3|3|REKc||{0|}}
{30001002|XXparameter|INPUT_DML|$DW_DML/$DML_FILENAME|3|2|RE$||{0|}}
{30001002|XXparameter|EXTRACT_LOG_FILE|$DW_SA_LOG/$TABLE_ID.unload.$FILE_ID.input_table_extract.$FILE_DATETIME.log|3|2|RE$||{0|}}
{30001002|XXparameter|OUTPUT_FILE|$(eval print $OUT_DIR/$DATA_FILENAME)|3|6|REk||{0|}}
{30001002|XXparameter|ROLLUP_FIELD|$(grep "^UNLOAD_ROLLUP_FIELD\\>" $ETL_CFG_FILE \| read PARAM VALUE COMMENT; print $VALUE)|3|6|REk||{0|}}
{30001002|XXparameter|FEXP_SESSION_MAX_FIRST|$(grep "^FEXP_SESSION_MAX_FIRST\\>" $ETL_CFG_FILE \| read PARAM VALUE COMMENT; eval print $VALUE)|3|6|REk||{0|}}
{30001002|XXparameter|FEXP_SESSION_MIN_SECOND|$(grep "^FEXP_SESSION_MIN_SECOND\\>" $ETL_CFG_FILE \| read PARAM VALUE COMMENT; eval print $VALUE)|3|6|REk||{0|}}
{30001002|XXparameter|FEXP_SLEEP|$(grep "^FEXP_SLEEP\\>" $ETL_CFG_FILE \| read PARAM VALUE COMMENT; eval print $VALUE)|3|6|REk||{0|}}
{30001002|XXparameter|FEXP_TENACITY|$(grep "^FEXP_TENACITY\\>" $ETL_CFG_FILE \| read PARAM VALUE COMMENT; eval print $VALUE)|3|6|REk||{0|}}
{30001002|XXparameter|TABLE_ID21|$\{TABLE_ID%$\{TABLE_ID#?????????????????????\}\}|3|6|REKk||{0|}}
{30001002|XXparameter|FEXP_LOGTAB|$\{TABLE_ID21:-$TABLE_ID\}_$$_L|3|6|REKk||{0|}}
{30001002|XXparameter|FEXP_LOGFILE|$DW_SA_LOG/$TABLE_ID.unload.$FILE_ID.fast_export.$FILE_DATETIME.log|3|6|REk||{0|}}
{30001002|XXparameter|REFORMAT_INPUT_DML|$(if [[ -f $DW_DML/$ETL_ID.write_unload.dml ]]
then
        print $DW_DML/$ETL_ID.write_unload.dml
else
        print $INPUT_DML 
fi )|3|6|REk||{0|}}
{30001002|XXparameter|CNDTL_UNLOAD_REFORMAT|$(grep "^CNDTL_UNLOAD_REFORMAT\\>" $ETL_CFG_FILE \| read PARAM VALUE COMMENT; print $VALUE)|3|6|REk||{0|}}
{30001002|XXparameter|REFORMAT_TRANS_FILE|$DW_XFR/$ETL_ID.reformat_unload.xfr|3|2|RE$||{0|}}
{30001002|XXparameter|RECORD_COUNT_FILE|$DW_SA_TMP/$TABLE_ID.unload.$FILE_ID.record_count.dat|3|2|RE$||{0|}}
{30001002|XXparameter|_parameters_of_Gather_Extract_Logs|@34|9|FHK|@{0|}}
{30001002|XXparameter|_parameters_of_Replicate|@34|9|FHK|@{0|}}
{30001002|XXparameter|_parameters_of_Last_Extract_Value_File_1|@34|9|FHK|@{0|}}
{30001002|XXparameter|_parameters_of_Extract_Log_File|@34|9|FHK|@{0|}}
{30001002|XXparameter|_parameters_of_Rollup_Record_Counts|@34|9|FHK|@{0|}}
{30001002|XXparameter|_parameters_of_Record_Count_File|@34|9|FHK|@{0|}}
{30001002|XXparameter|_parameters_of_Redefine_Format|@34|9|FHK|@{0|}}
{30001002|XXparameter|_parameters_of_Last_Extract_Value_File|@34|9|FHK|@{0|}}
{30001002|XXparameter|_parameters_of_Filter_Record_Count_Rows|@34|9|FHK|@{0|}}
{30001002|XXparameter|_parameters_of_Send_From_Extract_Value_to_Rollup|@34|9|FHK|@{0|}}
{30001002|XXparameter|_parameters_of_Get_Max_Value_for_Extract_Field|@34|9|FHK|@{0|}}
{30001002|XXparameter|_parameters_of_Send_Primer_Row_to_Reformat|@34|9|FHK|@{0|}}
{30001002|XXparameter|_parameters_of_Update_Last_Extract_Value_File|@34|9|FHK|@{0|}}
{30001002|XXparameter|_parameters_of_Input_Table|@34|9|FHK|@{0|}}
{30001002|XXparameter|_parameters_of_Output_File|@34|9|FHK|@{0|}}
{30001002|XXparameter|_parameters_of_Deflate|@34|9|FHK|@{0|}}
{30001002|XXparameter|_parameters_of_Reformat|@34|9|FHK|@{0|}}
{30001002|XXparameter|_parameters_of_chmod_FEXP_LOGFILE|@34|9|FHK|@{0|}}
{30001002|XXparameter|!ab_compatibility|3.1.4.4|3|9|P|@{0|}}
}}@0|@0|0|0|0|1039000|930000|0|single_td_unload|Craig Werre||1|10|-1||6||2049|{0|}0|0|{0|}{0|}{0|}{0|}1.0|1039706|930882|7|}}
{2010703001|XXGgraphinfo|2|0|3|0|{@{}@1|3.0.3.1|{3|1|1|1|}Job 'single_td_unload' Execution Status Report

Compilation of graph "single_td_unload" started at Wednesday, November 22, 2006 15:18:27
No compilation errors
||if [ -z $LAST_EXTRACT_TYPE ]
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

if [ -z $FEXP_SESSION_MAX_FIRST ]
then
   print "$0: Error: FEXP_SESSION_MAX_FIRST variable not set"
   exit 4
fi

if [ -z $FEXP_SESSION_MIN_SECOND ]
then
   print "$0: Error: FEXP_SESSION_MIN_SECOND variable not set"
   exit 4
fi

if [ -z $FEXP_SLEEP ]
then
   print "$0: Error: FEXP_SLEEP variable not set"
   exit 4
fi

if [ -z $FEXP_TENACITY ]
then
   print "$0: Error: FEXP_TENACITY variable not set"
   exit 4
fi






















































































































||@|31633919|}}
{2010705002|XXGrunsettings|3|0|5|0|{@{}@lvsdpelega003.lvs.ebay.com||||1|300|0|1|1|0|0|0|0|1|1|0|0|1|1|1|1|0|0|0|@16|}}
{2010242002|XXGface|4|0|7|0|{@{}@Arial|0|100|0|}}
{2010240003|XXGlegend|5|0|9|0|{@{}@Phase 2:  This is a conditional phase that runs
when the $TO_EXTRACT_VALUE variable can be
derived and used in the sql to extract the data.
The $TO_EXTRACT_VALUE value is written to the
$LAST_EXTRACT_VALUE_FILE for use as the
$FROM_EXTRACT_VALUE in the next run of the loader
system.|669094|787681|689000|808000|336000|105000|29|}}
{2010242002|XXGface|6|0|11|0|{@{}@Courier New|16711680|90|0|}}
{2010240003|XXGlegend|7|0|13|0|{@{}@Phase 1:  The log file from the Input Table
component is parsed to get the number of
records unloaded.  This is used for determining
which Teradata utility to use to load the
data in the subsequent load graph.|667983|484637|688000|505000|322000|75000|30|}}
{2010242002|XXGface|8|0|15|0|{@{}@Courier New|16711680|90|0|}}
{2010240003|XXGlegend|9|0|17|0|{@{}@Phase 0:  Extracts data from the teradata
database using the sql contained in a file
$DW_SQL/$ETL_ID.unload.sql.  Replicate, Send
Primer Row, Get Max Value, and Last Extract
Value File-1 are conditional components based
on the need to rollup the extract field to get
the max value to use as the $FROM_EXTRACT_VALUE
in the next run of this graph.  The Send Primer
Row component passes the $FROM_EXTRACT_VALUE in
case no rows are unloaded from the source.|671737|208258|692000|228000|322000|150000|32|}}
{2010242002|XXGface|10|0|19|0|{@{}@Courier New|16711680|90|0|}}
{2010210004|XXGflow|11|0|21|0|{@{}@384|.5|.5|{12|84000|169000|84000|189000|84000|192000|85000|192000|85000|195000|85000|215000|}34|19|}}
{2010210004|XXGflow|12|0|23|0|{@{}@384|.5|.5|{12|319000|705000|339000|705000|442000|705000|442000|704000|546000|704000|578000|704000|}16|17|}}
{2010210004|XXGflow|13|0|25|0|{@{}@384|.5|.5|{8|318000|124000|348000|124000|371000|124000|391000|124000|}36|17|}}
{2010210004|XXGflow|14|0|27|0|{@{}@384|.5|.5|{8|426000|864000|446000|864000|547000|864000|584000|864000|}21|17|}}
{2010210004|XXGflow|15|0|29|0|{@{}@384|.5|.5|{8|545000|124000|565000|124000|595000|124000|615000|124000|}38|17|}}
{2010210004|XXGflow|16|0|31|0|{@{}@384|.5|.5|{8|743000|124000|763000|124000|778000|124000|800000|124000|}39|17|}}
{2010210004|XXGflow|17|0|33|0|{@{}@384|.5|.4000000059604645|{12|120000|128000|140000|128000|142000|128000|142000|129000|146000|129000|166000|129000|}33|17|}}
{2010210004|XXGflow|18|0|35|0|{@{}@384|.5|1.0|{12|322000|274000|342000|274000|353000|274000|353000|290000|353000|290000|383000|290000|}6|17|}}
{2010210004|XXGflow|19|0|37|0|{@{}@384|.5|.5|{12|173000|863000|193000|863000|210000|863000|210000|864000|228000|864000|248000|864000|}24|17|}}
{2010210004|XXGflow|20|0|39|0|{@{}@384|.5|1.0|{12|318000|134000|338000|134000|363000|134000|363000|280000|363000|280000|383000|280000|}5|531|}}
{2010210004|XXGflow|21|0|41|0|{@{}@384|.5|.5|{12|548000|285000|548000|285000|566000|285000|566000|286000|584000|286000|591000|286000|}8|17|}}
{2010210004|XXGflow|22|0|43|0|{@{}@384|.5|.5|{8|111000|554000|141000|554000|150000|554000|170000|554000|}26|17|}}
{2010210004|XXGflow|23|0|45|0|{@{}@384|.5|.5|{8|328000|554000|348000|554000|367000|554000|387000|554000|}27|17|}}
{2010210004|XXGflow|24|0|47|0|{@{}@384|.5|.5|{12|529000|554000|549000|554000|549000|629000|155000|629000|155000|705000|175000|705000|}18|17|}}
{2010501005|XXGpvertex|25|0|49|0|{|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|condition|$CNDTL_UNLOAD_COMPRESS|3|2|$|@{0|}}
{30001002|XXparameter|!prototype_path|C:\\Program Files\\Ab Initio\\Ab Initio GDE\\Components\\Compress\\Deflate.mpc|3|2|Pw$|@{0|}}
{30001002|XXparameter|Layout||3|8|s=|@{0|}}
{30001002|XXparameter|in_metadata||3|8|s=|@{0|}}
}}@0|@606316|71579|626000|92000|107000|65000|40|Deflate|Ab Initio Software|Built-in 2.12.2.m1:|1|10|-1||6||36865|1|{1|0|}}}
{2010203004|XXGoport|26|0|51|0|{@{}@732000|119000|11000|11000|out|0.0|@@@2320|0|}}
{2010202004|XXGiport|27|0|54|0|{@{}@616000|119000|11000|11000|in|0.0|@@@1808|0|}}
{2010501005|XXGpvertex|28|0|57|0|{Compresses data in a manner compatible with the zip and gzip deflate method. Use Inflate to uncompress the data.|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|mpname|broadcast|3|1|Hl|The name used on the mp command line for this component|{0|}}
{30001002|XXparameter|image__|unitool|3|2|H$|The image used if this component was a custom component|{0|}}
{30001002|XXparameter|condition||3|2|F$||{0|}}
{30001002|XXparameter|conditionInputPort|in|3|2|F$||{0|}}
{30001002|XXparameter|conditionOutputPort|out|3|2|F$||{0|}}
{30001002|XXparameter|condition_interpretation|Replace with flow|15|1|Fl||{2|Replace with flow|Remove completely|}}
{30001002|XXparameter|condition_interpretation.display_name|condition-interpretation|3|9|P|@{0|}}
{30001002|XXparameter|compression|1|1|2|FK$|Compression level (0-9)|{0|}}
{30001002|XXparameter|Layout|@9|2|RFs$||{0|}}
{30001002|XXparameter|HelpID|comp_deflate|3|2|F$||{0|}}
{30001002|XXparameter|in_metadata||7|1|RFsl||{0|}}
{30001002|XXparameter|out_metadata|void(1)|7|1|RFl||{0|}}
{30001002|XXparameter|doc_transform||8|2|FHs$|Document your transformation for dependency analysis|{0|}}
{30001002|XXparameter|doc_operation1|out::document(in)|3|2|RH$|The custom transformation|{0|}}
{30001002|XXparameter|mpcmodtime|1089906074|1|1|Hl|The last modification time of this component's template|{0|}}
{30001002|XXparameter|port_analysis|out=in|3|2|H$||{0|}}
{30001002|XXparameter|_propagate_through||3|1|FHKl|@{0|}}
}}@0|@0|0|0|0|0|0|0|@@@1|10|-1|@6|@1|1|{1|0|}}}
{2010203004|XXGoport|29|0|59|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|out|0.0|@@@2320|0|}}
{2010202004|XXGiport|30|0|62|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|in|0.0|@@@1808|0|}}
{2010503005|XXGfvertex|31|0|65|0|{|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|Layout|$[[record kind 85 subkind 0 parts [vector _interp_("$OUTPUT_FILE", "dollar_substitution")]]]|3|9||@{0|}}
{30001002|XXparameter|!prototype_path|C:\\Program Files\\Ab Initio\\Ab Initio GDE\\Components\\Datasets\\Output_File.mdc|3|2|Pw$|@{0|}}
{30001002|XXparameter|write_metadata||3|8|s=|@{0|}}
}}@0|@785622|84635|811000|105000|40000|40000|2|Output File|Craig Werre||1|10|-1||6||32769|-1|-1|}}
{2010202004|XXGiport|32|0|67|0|{@{}@801000|119000|11000|11000|write|0.0|@@@1776|0|}}
{2010503005|XXGfvertex|33|0|70|0|{Represents one file, many files, or a multifile as an output from your graph.  |{30100001|XXparameter_set|@@@@{{30001002|XXparameter|protection|0666|12|2|RF$||{0|}}
{30001002|XXparameter|mode|0x0062|1|2|FH$|modes of access|{0|}}
{30001002|XXparameter|condition||3|2|F$||{0|}}
{30001002|XXparameter|conditionInputPort||3|2|F$||{0|}}
{30001002|XXparameter|conditionOutputPort||3|2|F$||{0|}}
{30001002|XXparameter|condition_interpretation|Remove completely|15|1|Fl||{2|Replace with flow|Remove completely|}}
{30001002|XXparameter|condition_interpretation.display_name|condition-interpretation|3|9|P|@{0|}}
{30001002|XXparameter|key||19|2|RF$|Key specifier For Lookup File|{0|}}
{30001002|XXparameter|key.condition|mode lookup|3|15|P?|@{0|}}
{30001002|XXparameter|Layout|@28|2|RF$||{0|}}
{30001002|XXparameter|write_metadata||7|1|RFsl||{0|}}
{30001002|XXparameter|mpcmodtime|0|1|1|Hl|The last modification time of this component's template|{0|}}
{30001002|XXparameter|eme_dataset_location||3|2|F$|Place in the EME to create a dataset corresponding to this file.|{0|}}
}}@0|@0|0|0|0|0|0|0|@@@1|10|-1|@6|@1|-1|-1|}}
{2010202004|XXGiport|34|0|72|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|write|0.0|@@@1776|0|}}
{2010501005|XXGpvertex|35|0|76|0|{|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|transform0|$REFORMAT_TRANS_FILE|3|2|f$|@{0|}}
{30001002|XXparameter|condition|$CNDTL_UNLOAD_REFORMAT|3|2|$|@{0|}}
{30001002|XXparameter|in_metadata|$INPUT_DML|3|2|f$|@{0|}}
{30001002|XXparameter|out0_metadata|$REFORMAT_INPUT_DML|3|2|f$|@{0|}}
{30001002|XXparameter|error0_metadata|string('\\n')|3|1|l|@{0|}}
{30001002|XXparameter|log_metadata|record string("\|") node, timestamp, component, subcomponent, event_type; string("\|\\n") event_text; end|3|1|l|@{0|}}
{30001002|XXparameter|!prototype_path|C:\\Program Files\\Ab Initio\\Ab Initio GDE\\Components\\Transform\\Reformat.mpc|3|2|Pw$|@{0|}}
{30001002|XXparameter|_propagate_through|metadata type: in = out
metadata type: out = in|3|9||@{0|}}
{30001002|XXparameter|Layout||3|8|s=|@{0|}}
{30001002|XXparameter|reject0_metadata||3|8|s=|@{0|}}
}}@0|@382105|71579|402000|92000|133000|65000|37|Reformat|Ab Initio Software|Built-in|1|10|-1||6||36865|1|{1|0|}}}
{2010203004|XXGoport|36|0|78|0|{@{}@534000|119000|11000|11000|out0|0.0|count|out|0|2448|0|}}
{2010203004|XXGoport|37|0|81|0|{@{}@0|0|0|0|reject0|0.0|count|reject|0|1168|0|}}
{2010203004|XXGoport|38|0|83|0|{@{}@0|0|0|0|error0|0.0|count|error|0|1176|0|}}
{2010203004|XXGoport|39|0|85|0|{@{}@0|0|0|0|log|0.0|@@@1176|0|}}
{2010202004|XXGiport|40|0|87|0|{@{}@392000|119000|11000|11000|in|0.0|@@@1808|0|}}
{2010501005|XXGpvertex|41|0|90|0|{Changes the record format of your data by dropping fields or by using DML expressions to add fields, combine fields, or modify the data.|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|mpname|reformat-transform|3|1|Hl|The name used on the mp command line for this component|{0|}}
{30001002|XXparameter|image__|unitool|3|2|H$|The image used if this component was a custom component|{0|}}
{30001002|XXparameter|count|1|1|1|Fl|Number of reformat transforms|{0|}}
{30001002|XXparameter|count.is_index|true|3|9|P|@{0|}}
{30001002|XXparameter|transform0||8|1|FOl|Reformat transform|{0|}}
{30001002|XXparameter|transform0.index|count|3|9|P|@{0|}}
{30001002|XXparameter|transform0.index_value|0|3|9|P|@{0|}}
{30001002|XXparameter|transform0.index_name|transform|3|9|P|@{0|}}
{30001002|XXparameter|select||20|2|FK$|Filter expression before reformatting|{0|}}
{30001002|XXparameter|reject_threshold|Abort on first reject|15|1|Fl|When to abort if input records are rejected|{3|Abort on first reject|Never abort|Use limit/ramp|}}
{30001002|XXparameter|reject_threshold.display_name|reject-threshold|3|9|P|@{0|}}
{30001002|XXparameter|limit|0|1|2|F$|Maximum rejected records before failure|{0|}}
{30001002|XXparameter|limit.condition|param reject-threshold Use*|3|15|P?|@{0|}}
{30001002|XXparameter|ramp|0.0|2|2|F$|Rate of rejected records|{0|}}
{30001002|XXparameter|ramp.condition|param reject-threshold Use*|3|15|P?|@{0|}}
{30001002|XXparameter|limit_keyword|value reject-threshold Never* 0 value reject-threshold Abort* 0 sameas limit|1|13|Kv|Maximum rejected records before failure|{0|}}
{30001002|XXparameter|ramp_keyword|value reject-threshold Never* 99.0 value reject-threshold Abort* 0.0 sameas ramp|2|13|Kv|Rate of rejected records|{0|}}
{30001002|XXparameter|ramp_keyword.condition|version :2.12.2.m9|3|15|P?|@{0|}}
{30001002|XXparameter|ramp_keyword2|value reject-threshold Never* -1 value reject-threshold Abort* 0.0 sameas ramp|2|13|Kv|Rate of rejected records|{0|}}
{30001002|XXparameter|ramp_keyword2.condition|version 2.13:|3|15|P?|@{0|}}
{30001002|XXparameter|keyword_map|limit_keyword limit ramp_keyword ramp ramp_keyword2 ramp|3|2|$||{0|}}
{30001002|XXparameter|keyword_map.display_name|keyword-map|3|9|P|@{0|}}
{30001002|XXparameter|output_index||8|1|FKl|Output index computation|{0|}}
{30001002|XXparameter|output_index.display_name|output-index|3|9|P|@{0|}}
{30001002|XXparameter|output_index.keyword|output-index|3|9|P|@{0|}}
{30001002|XXparameter|output_index_port|discriminator|3|2|FHK$|Output index port for debugger|{0|}}
{30001002|XXparameter|output_index_port.display_name|output-index_port|3|9|P|@{0|}}
{30001002|XXparameter|output_index_port.keyword|output-index_port|3|9|P|@{0|}}
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
{30001002|XXparameter|conditionOutputPort|out*|3|2|F$||{0|}}
{30001002|XXparameter|condition_interpretation|Replace with flow|15|1|Fl||{2|Replace with flow|Remove completely|}}
{30001002|XXparameter|condition_interpretation.display_name|condition-interpretation|3|9|P|@{0|}}
{30001002|XXparameter|Layout|@9|2|RFs$||{0|}}
{30001002|XXparameter|in_metadata||7|1|RFl||{0|}}
{30001002|XXparameter|out0_metadata||7|1|RFl||{0|}}
{30001002|XXparameter|reject0_metadata||7|1|Fsl||{0|}}
{30001002|XXparameter|error0_metadata||7|1|Fl||{0|}}
{30001002|XXparameter|log_metadata||7|1|Fl||{0|}}
{30001002|XXparameter|doc_transform||8|2|FHs$|Document your transformation for dependency analysis|{0|}}
{30001002|XXparameter|doc_operation1|out[*],reject[*],error[*],log::document(in)|3|2|RH$|The custom transformation|{0|}}
{30001002|XXparameter|mpcmodtime|1099730098|1|1|Hl|The last modification time of this component's template|{0|}}
{30001002|XXparameter|output_index_signature|int out::output_index(in)|3|2|H$|Function returning index of output port|{0|}}
{30001002|XXparameter|output_index_signature.display_name|output-index_signature|3|9|P|@{0|}}
{30001002|XXparameter|output_indexes_signature|int[int] out::output_indexes(in)|3|2|H$|Function returning vector of indexes of output ports|{0|}}
{30001002|XXparameter|output_indexes_signature.display_name|output-indexes_signature|3|9|P|@{0|}}
{30001002|XXparameter|num_operations|1|1|2|H$|Number of operations per reformat|{0|}}
{30001002|XXparameter|operation1|out::reformat(in)|3|2|H$|Reformat operation|{0|}}
{30001002|XXparameter|port_analysis|reject*=in|3|2|H$||{0|}}
{30001002|XXparameter|main_mp_port|count out|3|1|l||{0|}}
{30001002|XXparameter|_propagation|metadata type: error = "string('\\\\n')"
metadata type: reject = in
metadata type: log = "record string(\\"\|\\") node, timestamp, component, subcomponent, event_type; string(\\"\|\\\\n\\") event_text; end"|3|1|HKl|@{0|}}
{30001002|XXparameter|_propagate_through||3|1|FHKl|@{0|}}
}}@0|@0|0|0|0|0|0|0|@@@1|10|-1|@6|@1|1|{1|0|}}}
{2010203004|XXGoport|42|0|92|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
{30001002|XXparameter|transform0||8|8|FO=|Reformat transform|{0|}}
{30001002|XXparameter|transform0.index|count|3|9|P|@{0|}}
{30001002|XXparameter|transform0.index_value|0|3|9|P|@{0|}}
{30001002|XXparameter|transform0.index_name|transform|3|9|P|@{0|}}
}}@0|0|0|0|out0|0.0|count|out|0|2448|0|}}
{2010203004|XXGoport|43|0|96|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|F=||{0|}}
}}@0|0|0|0|reject0|0.0|count|reject|0|1168|0|}}
{2010203004|XXGoport|44|0|99|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|F=||{0|}}
}}@0|0|0|0|error0|0.0|count|error|0|1176|0|}}
{2010203004|XXGoport|45|0|102|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|F=||{0|}}
}}@0|0|0|0|log|0.0|@@@1176|0|}}
{2010202004|XXGiport|46|0|105|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|in|0.0|@@@1808|0|}}
{2010503005|XXGfvertex|47|0|110|0|{|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|Layout|$[[record kind 85 subkind 0 parts [vector _interp_("file:$EXTRACT_LOG_FILE", "dollar_substitution")]]]|3|9||@{0|}}
{30001002|XXparameter|read_metadata|record
  string("\\n") line;
end;|3|1|l|@{0|}}
{30001002|XXparameter|!prototype_path|C:\\Program Files\\Ab Initio\\Ab Initio GDE\\Components\\Datasets\\Input_File.mdc|3|2|Pw$|@{0|}}
}}@1|@36031|514644|61000|535000|40000|40000|23|Extract Log File|Craig Werre||1|10|-1||6||32769|-1|-1|}}
{2010203004|XXGoport|48|0|112|0|{@{}@100000|549000|11000|11000|read|0.0|@@@2160|0|}}
{2010503005|XXGfvertex|49|0|115|0|{Represents one file, many files, or a multifile as an input to your graph.|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|protection|0666|12|2|RF$||{0|}}
{30001002|XXparameter|mode|0x0001|1|2|FH$|modes of access|{0|}}
{30001002|XXparameter|Layout|@28|2|RF$||{0|}}
{30001002|XXparameter|read_metadata||7|1|RFl||{0|}}
{30001002|XXparameter|mpcmodtime|1002750282|1|1|Hl|The last modification time of this component's template|{0|}}
{30001002|XXparameter|eme_dataset_location||3|2|F$|Place in the EME to create a dataset corresponding to this file.|{0|}}
}}@0|@0|0|0|0|0|0|0|@@@1|10|-1|@6|@1|-1|-1|}}
{2010203004|XXGoport|50|0|117|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|read|0.0|@@@2160|0|}}
{2010501005|XXGpvertex|51|0|120|0|{|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|select_expr|string_index(line, "UTY8722") > 0|3|2|$|@{0|}}
{30001002|XXparameter|error_metadata|string('
')|3|1|l|@{0|}}
{30001002|XXparameter|log_metadata|record string("\|") node, timestamp, component, subcomponent, event_type; string("\|\\n") event_text; end|3|1|l|@{0|}}
{30001002|XXparameter|!prototype_path|C:\\Program Files\\Ab Initio\\Ab Initio GDE\\Components\\Transform\\Filter by Expression.mpc|3|2|Pw$|@{0|}}
{30001002|XXparameter|Layout||3|8|s=|@{0|}}
{30001002|XXparameter|in_metadata||3|8|s=|@{0|}}
{30001002|XXparameter|out_metadata||3|8|s=|@{0|}}
{30001002|XXparameter|deselect_metadata||3|8|s=|@{0|}}
{30001002|XXparameter|reject_metadata||3|8|s=|@{0|}}
}}@1|@161000|481655|181000|502000|137000|106000|28|Filter Record Count Rows|Ab Initio Software|Built-in|1|10|-1||6||36865|1|{1|0|}}}
{2010203004|XXGoport|52|0|122|0|{@{}@317000|549000|11000|11000|out|0.0|@@@2448|0|}}
{2010203004|XXGoport|53|0|125|0|{@{}@0|0|0|0|deselect|0.0|@@@2192|0|}}
{2010203004|XXGoport|54|0|127|0|{@{}@0|0|0|0|reject|0.0|@@@1168|0|}}
{2010203004|XXGoport|55|0|129|0|{@{}@0|0|0|0|error|0.0|@@@1176|0|}}
{2010203004|XXGoport|56|0|131|0|{@{}@0|0|0|0|log|0.0|@@@1176|0|}}
{2010202004|XXGiport|57|0|133|0|{@{}@171000|549000|11000|11000|in|0.0|@@@1808|0|}}
{2010501005|XXGpvertex|58|0|136|0|{Filters data records according to a specified DML expression.|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|mpname|select-transform|3|1|Hl|The name used on the mp command line for this component|{0|}}
{30001002|XXparameter|image__|unitool|3|2|H$|The image used if this component was a custom component|{0|}}
{30001002|XXparameter|select_expr||20|2|RFO$|Filter expression|{0|}}
{30001002|XXparameter|reject_threshold|Abort on first reject|15|1|Fl|When to abort if input records are rejected|{3|Abort on first reject|Never abort|Use limit/ramp|}}
{30001002|XXparameter|reject_threshold.display_name|reject-threshold|3|9|P|@{0|}}
{30001002|XXparameter|limit|0|1|2|F$|Maximum rejected records before failure|{0|}}
{30001002|XXparameter|limit.condition|param reject-threshold Use*|3|15|P?|@{0|}}
{30001002|XXparameter|ramp|0.0|2|2|F$|Rate of rejected records|{0|}}
{30001002|XXparameter|ramp.condition|param reject-threshold Use*|3|15|P?|@{0|}}
{30001002|XXparameter|limit_keyword|value reject-threshold Never* 0 value reject-threshold Abort* 0 sameas limit|1|13|Kv|Maximum rejected records before failure|{0|}}
{30001002|XXparameter|ramp_keyword|value reject-threshold Never* 99.0 value reject-threshold Abort* 0.0 sameas ramp|2|13|Kv|Rate of rejected records|{0|}}
{30001002|XXparameter|ramp_keyword.condition|version :2.12.2.m9|3|15|P?|@{0|}}
{30001002|XXparameter|ramp_keyword2|value reject-threshold Never* -1 value reject-threshold Abort* 0.0 sameas ramp|2|13|Kv|Rate of rejected records|{0|}}
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
{30001002|XXparameter|Layout|@9|2|RFs$||{0|}}
{30001002|XXparameter|in_metadata||7|1|RFsl||{0|}}
{30001002|XXparameter|out_metadata||7|1|RFsl||{0|}}
{30001002|XXparameter|deselect_metadata||7|1|Fsl||{0|}}
{30001002|XXparameter|reject_metadata||7|1|Fsl||{0|}}
{30001002|XXparameter|error_metadata||7|1|Fl||{0|}}
{30001002|XXparameter|log_metadata||7|1|Fl||{0|}}
{30001002|XXparameter|doc_transform||8|2|FHs$|Document your transformation for dependency analysis|{0|}}
{30001002|XXparameter|doc_operation1|out,deselect,reject,error,log::document(in)|3|2|RH$|The custom transformation|{0|}}
{30001002|XXparameter|mpcmodtime|1100123984|1|1|Hl|The last modification time of this component's template|{0|}}
{30001002|XXparameter|port_analysis|reject=in; out=in; deselect=in|3|2|H$||{0|}}
{30001002|XXparameter|_propagation|metadata type: deselect = out
metadata type: out = deselect
metadata type: reject = in
metadata type: reject = out
metadata type: reject = deselect
metadata type: error = "string('
')"
metadata type: log = "record string(\\"\|\\") node, timestamp, component, subcomponent, event_type; string(\\"\|\\\\n\\") event_text; end"|3|1|HKl|@{0|}}
{30001002|XXparameter|_propagate_through|metadata type: out = in
metadata type: in = out
metadata type: deselect = in
metadata type: in = deselect|3|1|FHKl|@{0|}}
}}@0|@0|0|0|0|0|0|0|@@@1|10|-1|@6|@1|1|{1|0|}}}
{2010203004|XXGoport|59|0|138|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|out|0.0|@@@2448|0|}}
{2010203004|XXGoport|60|0|141|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|F=||{0|}}
}}@0|0|0|0|deselect|0.0|@@@2192|0|}}
{2010203004|XXGoport|61|0|144|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|F=||{0|}}
}}@0|0|0|0|reject|0.0|@@@1168|0|}}
{2010203004|XXGoport|62|0|147|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|F=||{0|}}
}}@0|0|0|0|error|0.0|@@@1176|0|}}
{2010203004|XXGoport|63|0|150|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|F=||{0|}}
}}@0|0|0|0|log|0.0|@@@1176|0|}}
{2010202004|XXGiport|64|0|153|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|in|0.0|@@@1808|0|}}
{2010503005|XXGfvertex|65|0|156|0|{|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|Layout|$[[record kind 85 subkind 0 parts [vector _interp_("file:$RECORD_COUNT_FILE", "dollar_substitution")]]]|3|9||@{0|}}
{30001002|XXparameter|!prototype_path|C:\\Program Files\\Ab Initio\\Ab Initio GDE\\Components\\Datasets\\Output_File.mdc|3|2|Pw$|@{0|}}
{30001002|XXparameter|write_metadata||3|8|s=|@{0|}}
}}@1|@564302|665096|589000|685000|40000|40000|17|Record Count File|Craig Werre||1|10|-1||6||32769|-1|-1|}}
{2010202004|XXGiport|66|0|158|0|{@{}@579000|699000|11000|11000|write|0.0|@@@1776|0|}}
{2010503005|XXGfvertex|67|0|161|0|{Represents one file, many files, or a multifile as an output from your graph.  |{30100001|XXparameter_set|@@@@{{30001002|XXparameter|protection|0666|12|2|RF$||{0|}}
{30001002|XXparameter|mode|0x0062|1|2|FH$|modes of access|{0|}}
{30001002|XXparameter|condition||3|2|F$||{0|}}
{30001002|XXparameter|conditionInputPort||3|2|F$||{0|}}
{30001002|XXparameter|conditionOutputPort||3|2|F$||{0|}}
{30001002|XXparameter|condition_interpretation|Remove completely|15|1|Fl||{2|Replace with flow|Remove completely|}}
{30001002|XXparameter|condition_interpretation.display_name|condition-interpretation|3|9|P|@{0|}}
{30001002|XXparameter|key||19|2|RF$|Key specifier For Lookup File|{0|}}
{30001002|XXparameter|key.condition|mode lookup|3|15|P?|@{0|}}
{30001002|XXparameter|Layout|@28|2|RF$||{0|}}
{30001002|XXparameter|write_metadata||7|1|RFsl||{0|}}
{30001002|XXparameter|mpcmodtime|1002323448|1|1|Hl|The last modification time of this component's template|{0|}}
{30001002|XXparameter|eme_dataset_location||3|2|F$|Place in the EME to create a dataset corresponding to this file.|{0|}}
}}@0|@0|0|0|0|0|0|0|@@@1|10|-1|@6|@1|-1|-1|}}
{2010202004|XXGiport|68|0|163|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|write|0.0|@@@1776|0|}}
{2010501005|XXGpvertex|69|0|166|0|{|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|sorted_input|False|3|1|l|@{0|}}
{30001002|XXparameter|key|\{\}|3|2|$|@{0|}}
{30001002|XXparameter|transform|out::rollup(in) =
begin
  out.record_count :: sum(in.record_count);
end;|3|1|l|@{0|}}
{30001002|XXparameter|out_metadata|record
  decimal("\\n", 0) record_count;
end;|3|1|l|@{0|}}
{30001002|XXparameter|error_metadata|string('\\n')|3|1|l|@{0|}}
{30001002|XXparameter|log_metadata|record string("\|") node, timestamp, component, subcomponent, event_type; string("\|\\n") event_text; end|3|1|l|@{0|}}
{30001002|XXparameter|!prototype_path|C:\\Program Files\\Ab Initio\\Ab Initio GDE\\Components\\Transform\\Rollup.mpc|3|2|Pw$|@{0|}}
{30001002|XXparameter|Layout||3|8|s=|@{0|}}
{30001002|XXparameter|in_metadata||3|8|s=|@{0|}}
{30001002|XXparameter|reject_metadata||3|8|s=|@{0|}}
}}@1|@165699|642393|186000|662000|123000|87000|15|Rollup Record Counts|Ab Initio Software|Built-in 2.0:|1|10|-1||6||36865|1|{1|0|}}}
{2010203004|XXGoport|70|0|168|0|{@{}@308000|700000|11000|11000|out|0.0|@@@2448|0|}}
{2010203004|XXGoport|71|0|171|0|{@{}@0|0|0|0|reject|0.0|@@@1168|0|}}
{2010203004|XXGoport|72|0|173|0|{@{}@0|0|0|0|error|0.0|@@@1176|0|}}
{2010203004|XXGoport|73|0|175|0|{@{}@0|0|0|0|log|0.0|@@@1176|0|}}
{2010202004|XXGiport|74|0|177|0|{@{}@176000|700000|11000|11000|in|0.0|@@@1808|0|}}
{2010501005|XXGpvertex|75|0|180|0|{Generates data records that summarize groups of data records.  Rollup in Memory maximizes performance by keeping intermediate results in main memory.|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|mpname|?rollup|3|1|Hl|The name used on the mp command line for this component|{0|}}
{30001002|XXparameter|image__|/~ab_home/bin/transform|3|2|H$|The image used if this component was a custom component|{0|}}
{30001002|XXparameter|sorted_input|True|13|1|Fl|Ordering of input data|{0|}}
{30001002|XXparameter|sorted_input.display_name|sorted-input|3|9|P|@{0|}}
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
{30001002|XXparameter|transform||8|1|RFOl|Filename containing rollup functions|{0|}}
{30001002|XXparameter|max_core|67108864|1|2|FK$|Maximum memory usage in bytes|{0|}}
{30001002|XXparameter|max_core.display_name|max-core|3|9|P|@{0|}}
{30001002|XXparameter|max_core.keyword|max-core|3|9|P|@{0|}}
{30001002|XXparameter|max_core.condition|param sorted-input False|3|15|P?|@{0|}}
{30001002|XXparameter|max_core.update_clauses|retain_old_default|3|9|P|@{0|}}
{30001002|XXparameter|reject_threshold|Abort on first reject|15|1|Fl|When to abort if input records are rejected|{3|Abort on first reject|Never abort|Use limit/ramp|}}
{30001002|XXparameter|reject_threshold.display_name|reject-threshold|3|9|P|@{0|}}
{30001002|XXparameter|limit|0|1|2|F$|Maximum rejected records before failure|{0|}}
{30001002|XXparameter|limit.condition|param reject-threshold Use*|3|15|P?|@{0|}}
{30001002|XXparameter|ramp|0.0|2|2|F$|Rate of rejected records|{0|}}
{30001002|XXparameter|ramp.condition|param reject-threshold Use*|3|15|P?|@{0|}}
{30001002|XXparameter|limit_keyword|value reject-threshold Never* 0 value reject-threshold Abort* 0 sameas limit|1|13|Kv|Maximum rejected records before failure|{0|}}
{30001002|XXparameter|ramp_keyword|value reject-threshold Never* 99.0 value reject-threshold Abort* 0.0 sameas ramp|2|13|Kv|Rate of rejected records|{0|}}
{30001002|XXparameter|ramp_keyword.condition|version :2.12.2.m9|3|15|P?|@{0|}}
{30001002|XXparameter|ramp_keyword2|value reject-threshold Never* -1 value reject-threshold Abort* 0.0 sameas ramp|2|13|Kv|Rate of rejected records|{0|}}
{30001002|XXparameter|ramp_keyword2.condition|version 2.13:|3|15|P?|@{0|}}
{30001002|XXparameter|keyword_map|limit_keyword limit ramp_keyword ramp ramp_keyword2 ramp|3|2|$||{0|}}
{30001002|XXparameter|keyword_map.display_name|keyword-map|3|9|P|@{0|}}
{30001002|XXparameter|check_sort|True|13|1|FKl|Abort on first record that is out of sorted order|{0|}}
{30001002|XXparameter|check_sort.display_name|check-sort|3|9|P|@{0|}}
{30001002|XXparameter|check_sort.keyword|check-sort|3|9|P|@{0|}}
{30001002|XXparameter|check_sort.condition|param key-method True param sorted-input True version 2.11.1:|3|15|P?|@{0|}}
{30001002|XXparameter|check_sort.update_clauses|update_value False|3|9|P|@{0|}}
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
{30001002|XXparameter|Layout|@9|2|RFs$||{0|}}
{30001002|XXparameter|in_metadata||7|1|RFsl||{0|}}
{30001002|XXparameter|out_metadata||7|1|RFl||{0|}}
{30001002|XXparameter|reject_metadata||7|1|Fsl||{0|}}
{30001002|XXparameter|error_metadata||7|1|Fl||{0|}}
{30001002|XXparameter|log_metadata||7|1|Fl||{0|}}
{30001002|XXparameter|doc_transform||8|2|FHs$|Document your transformation for dependency analysis|{0|}}
{30001002|XXparameter|doc_operation1|out,reject,error,log::document(in)|3|2|RH$|The custom transformation|{0|}}
{30001002|XXparameter|mpcmodtime|1100123984|1|1|Hl|The last modification time of this component's template|{0|}}
{30001002|XXparameter|num_operations|6|1|2|H$|Number of steps in rollup|{0|}}
{30001002|XXparameter|num_variables|1|1|2|H$|Number of public variables in rollup|{0|}}
{30001002|XXparameter|variable1|optional temporary_type temp|3|2|H$|Temporary variable|{0|}}
{30001002|XXparameter|operation1|optional int out::input_select(in)|3|2|H$|Select input record|{0|}}
{30001002|XXparameter|operation2|optional temp ? temp::initialize(in) : NULL|3|2|H$|Initialize temporary|{0|}}
{30001002|XXparameter|operation3|temp ? temp::rollup(temp, in) : out::rollup(in)|3|2|H$|Do computation|{0|}}
{30001002|XXparameter|operation4|optional temp ? out::finalize(temp, in) : NULL|3|2|H$|Create output record|{0|}}
{30001002|XXparameter|operation5|optional int out::output_select(out)|3|2|H$|Select output record|{0|}}
{30001002|XXparameter|operation6|optional integer(8) out::key_change(in in1, in in2)|3|2|H$|Determine if key changed|{0|}}
{30001002|XXparameter|operation1_analysis|int input_select_out::input_select(in)|3|2|H$||{0|}}
{30001002|XXparameter|operation5_analysis|int output_select_out::output_select(out)|3|2|H$|Select output record|{0|}}
{30001002|XXparameter|operation6_analysis|integer(8) key_change_out::key_change(in in, in in)|3|2|H$|Determine if key changed|{0|}}
{30001002|XXparameter|port_analysis|reject=in|3|2|H$||{0|}}
{30001002|XXparameter|_propagation|metadata type: reject = in
metadata type: error = "string('\\\\n')"
metadata type: log = "record string(\\"\|\\") node, timestamp, component, subcomponent, event_type; string(\\"\|\\\\n\\") event_text; end"|3|1|HKl|@{0|}}
{30001002|XXparameter|_propagate_through||3|1|FHKl|@{0|}}
}}@0|@0|0|0|0|0|0|0|@@@1|10|-1|@6|@1|1|{1|0|}}}
{2010203004|XXGoport|76|0|182|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|out|0.0|@@@2448|0|}}
{2010203004|XXGoport|77|0|185|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|F=||{0|}}
}}@0|0|0|0|reject|0.0|@@@1168|0|}}
{2010203004|XXGoport|78|0|188|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|F=||{0|}}
}}@0|0|0|0|error|0.0|@@@1176|0|}}
{2010203004|XXGoport|79|0|191|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|F=||{0|}}
}}@0|0|0|0|log|0.0|@@@1176|0|}}
{2010202004|XXGiport|80|0|194|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|in|0.0|@@@1808|0|}}
{2010501005|XXGpvertex|81|0|198|0|{|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|out_metadata|record
  string("UTY8722 ") junk;
  decimal(" total") record_count;
  string("\\n") newline = "\\n";
end;|3|1|l|@{0|}}
{30001002|XXparameter|!prototype_path|C:\\Program Files\\Ab Initio\\Ab Initio GDE\\Components\\Miscellaneous\\Redefine Format.mpc|3|2|Pw$|@{0|}}
{30001002|XXparameter|Layout||3|8|s=|@{0|}}
{30001002|XXparameter|in_metadata||3|8|s=|@{0|}}
}}@1|@378000|500849|398000|521000|121000|68000|19|Redefine Format|Ab Initio Software|Built-in|1|10|-1||6||36865|1|{1|0|}}}
{2010203004|XXGoport|82|0|200|0|{@{}@518000|549000|11000|11000|out|0.0|@@@2448|0|}}
{2010202004|XXGiport|83|0|203|0|{@{}@388000|549000|11000|11000|in|0.0|@@@1936|0|}}
{2010501005|XXGpvertex|84|0|206|0|{Copies data records from its input to its output without changing the values.  Use Redefine Format to change a record format or rename fields.|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|mpname|copy|3|1|Hl|The name used on the mp command line for this component|{0|}}
{30001002|XXparameter|image__|unitool|3|2|H$|The image used if this component was a custom component|{0|}}
{30001002|XXparameter|condition||3|2|F$||{0|}}
{30001002|XXparameter|conditionInputPort|in|3|2|F$||{0|}}
{30001002|XXparameter|conditionOutputPort|out|3|2|F$||{0|}}
{30001002|XXparameter|condition_interpretation|Replace with flow|15|1|Fl||{2|Replace with flow|Remove completely|}}
{30001002|XXparameter|condition_interpretation.display_name|condition-interpretation|3|9|P|@{0|}}
{30001002|XXparameter|Layout|@9|2|RFs$||{0|}}
{30001002|XXparameter|in_metadata||7|1|RFsl||{0|}}
{30001002|XXparameter|out_metadata||7|1|RFl||{0|}}
{30001002|XXparameter|doc_transform||8|2|FHs$|Document your transformation for dependency analysis|{0|}}
{30001002|XXparameter|doc_operation1|out::document(in)|3|2|RH$|The custom transformation|{0|}}
{30001002|XXparameter|mpcmodtime|971909678|1|1|Hl|The last modification time of this component's template|{0|}}
{30001002|XXparameter|port_analysis|out=all_to_all(in)|3|2|H$||{0|}}
{30001002|XXparameter|_propagate_through||3|1|FHKl|@{0|}}
}}@0|@0|0|0|0|0|0|0|@@@1|10|-1|@6|@1|1|{1|0|}}}
{2010203004|XXGoport|85|0|208|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|out|0.0|@@@2448|0|}}
{2010202004|XXGiport|86|0|211|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|in|0.0|@@@1936|0|}}
{2010501005|XXGpvertex|87|0|221|0|{|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|LogFile|$[[record kind 85 subkind 0 parts [vector _interp_("$EXTRACT_LOG_FILE", "dollar_substitution")]]]|3|9||@{0|}}
{30001002|XXparameter|!prototype_path|C:\\Program Files\\Ab Initio\\Ab Initio GDE\\Components\\Miscellaneous\\Gather Logs.mpc|3|2|Pw$|@{0|}}
{30001002|XXparameter|in_metadata||3|8|s=|@{0|}}
}}@0|@12675|205982|33000|226000|105000|97000|1|Gather Extract Logs|Ab Initio Software|Built-in|1|10|-1||6||34817|1|{1|0|}}}
{2010202004|XXGiport|88|0|223|0|{@{}@80000|216000|11000|11000|in|0.0|@@@786|0|}}
{2010501005|XXGpvertex|89|0|226|0|{Collects the output from log ports of components for analysis of a graph after execution.|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|mpname|logger|3|1|Hl|The name used on the mp command line for this component|{0|}}
{30001002|XXparameter|image__|unitool|3|2|H$|The image used if this component was a custom component|{0|}}
{30001002|XXparameter|LogFile|@28|2|RFO$|Filename to log to|{0|}}
{30001002|XXparameter|StartText|Start|3|2|RFO$|Text to include in start record|{0|}}
{30001002|XXparameter|EndText|End|3|2|RFO$|Text to include in end record|{0|}}
{30001002|XXparameter|condition||3|2|F$||{0|}}
{30001002|XXparameter|conditionInputPort||3|2|F$||{0|}}
{30001002|XXparameter|conditionOutputPort||3|2|F$||{0|}}
{30001002|XXparameter|condition_interpretation|Remove completely|15|1|Fl||{2|Replace with flow|Remove completely|}}
{30001002|XXparameter|condition_interpretation.display_name|condition-interpretation|3|9|P|@{0|}}
{30001002|XXparameter|Layout|$[[record kind 85 subkind 0 parts [vector _interp_("file:.", "dollar_substitution")]]]|9|9|F|logger should run serially by default|{0|}}
{30001002|XXparameter|in_metadata||7|1|RFsl||{0|}}
{30001002|XXparameter|doc_transform||8|2|FHs$|Document your transformation for dependency analysis|{0|}}
{30001002|XXparameter|doc_operation1|::document(in)|3|2|RH$|The custom transformation|{0|}}
{30001002|XXparameter|mpcmodtime|973625466|1|1|Hl|The last modification time of this component's template|{0|}}
}}@0|@0|0|0|0|0|0|0|@@@1|10|-1|@6|@1|1|{1|0|}}}
{2010202004|XXGiport|90|0|228|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|in|0.0|@@@786|0|}}
{2010505005|XXGtvertex|91|0|231|0|{|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|config_file|$AB_IDB_CONFIG|3|2|f$|@{0|}}
{30001002|XXparameter|table_spec_kind|select|3|1|l|@{0|}}
{30001002|XXparameter|table_spec|$\{INPUT_TABLE_SEL\}|3|3|c|@{0|}}
{30001002|XXparameter|field_type_preference|variable|3|1|l|@{0|}}
{30001002|XXparameter|column_delimiter|\||3|1|l|@{0|}}
{30001002|XXparameter|sleep|$FEXP_SLEEP|3|2|$|@{0|}}
{30001002|XXparameter|tenacity|$FEXP_TENACITY|3|2|$|@{0|}}
{30001002|XXparameter|sessions|$FEXP_SESSION_MAX_FIRST $FEXP_SESSION_MIN_SECOND|3|2|$|@{0|}}
{30001002|XXparameter|logtab_name|$FEXP_LOGTAB|3|2|$|@{0|}}
{30001002|XXparameter|fexp_logfile|$FEXP_LOGFILE|3|2|f$|@{0|}}
{30001002|XXparameter|oracle_max_kb|$ORA_MAX_KB|3|2|$|@{0|}}
{30001002|XXparameter|dbms|teradata|3|2|$|@{0|}}
{30001002|XXparameter|read_metadata|$INPUT_DML|3|2|f$|@{0|}}
{30001002|XXparameter|log_metadata|record string("\|") node, timestamp, component, subcomponent, event_type; string("\|\\n") event_text; end|3|1|l|@{0|}}
{30001002|XXparameter|!prototype_path|C:\\Program Files\\Ab Initio\\Ab Initio GDE\\Components\\Datasets\\Input_Table.mdc|3|2|Pw$|@{0|}}
{30001002|XXparameter|Layout||3|8|s=|@{0|}}
}}@0|@40000|78500|60000|99000|50000|60000|35|Input Table|Craig Werre|Built-in 2.2:|1|10|-1||6||4097|}}
{2010203004|XXGoport|92|0|233|0|{@{}@109000|123000|11000|11000|read|0.0|@@@2448|0|}}
{2010203004|XXGoport|93|0|236|0|{@{}@79000|158000|11000|11000|log|0.0|@@@1176|0|}}
{2010505005|XXGtvertex|94|0|239|0|{Unloads data records from a database.|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|mode|0x0001|1|2|FH$|modes of access|{0|}}
{30001002|XXparameter|HelpID|HICOMP_INPUT_TABLE|3|2|H$||{0|}}
{30001002|XXparameter|config_file||22|2|RFf$||{0|}}
{30001002|XXparameter|table_spec_kind|table|15|1|RFl||{3|table|insert|select|}}
{30001002|XXparameter|table_spec||24|3|RFc||{0|}}
{30001002|XXparameter|ora_interface|api|15|1|Fl|Unload data via api or utility|{1|api|}}
{30001002|XXparameter|ora_interface.condition|param_exact dbms oracle mode input|3|15|P?|@{0|}}
{30001002|XXparameter|ter_interface_old|Fast Export|15|1|Fl|Unload data via api or utility|{3|api|Fast Export|WB_export|}}
{30001002|XXparameter|ter_interface_old.condition|param_exact dbms teradata mode input version :2.11.8.m99|3|15|P?|@{0|}}
{30001002|XXparameter|ter_interface_new|FastExport|15|1|Fl|Unload data via api or utility|{3|api|FastExport|WB_export|}}
{30001002|XXparameter|ter_interface_new.condition|param_exact dbms teradata mode input version 2.12:|3|15|P?|@{0|}}
{30001002|XXparameter|ter_interface|sameas ter_interface_new sameas ter_interface_old|3|13|v||{0|}}
{30001002|XXparameter|ter_interface.condition|param_exact dbms teradata mode input|3|15|P?|@{0|}}
{30001002|XXparameter|db2eee_interface|api|15|1|Fl|Unload data via api or utility|{1|api|}}
{30001002|XXparameter|db2eee_interface.condition|param_exact dbms db2eee mode input|3|15|P?|@{0|}}
{30001002|XXparameter|db2ee_interface|api|15|1|Fl|Unload data via api or utility|{1|api|}}
{30001002|XXparameter|db2ee_interface.condition|param_exact dbms db2ee mode input|3|15|P?|@{0|}}
{30001002|XXparameter|db2uss_interface|api|15|1|Fl|Unload data via api or utility|{1|api|}}
{30001002|XXparameter|db2uss_interface.condition|param_exact dbms db2uss mode input|3|15|P?|@{0|}}
{30001002|XXparameter|mssql_interface|api|15|1|Fl|Unload data via api or utility|{1|api|}}
{30001002|XXparameter|mssql_interface.condition|param_exact dbms mssql mode input|3|15|P?|@{0|}}
{30001002|XXparameter|nucleus_interface|api|15|1|Fl|Unload data via api or utility|{1|api|}}
{30001002|XXparameter|nucleus_interface.condition|param_exact dbms nucleus mode input|3|15|P?|@{0|}}
{30001002|XXparameter|odbc_interface|api|15|1|Fl|Unload data via api or utility|{1|api|}}
{30001002|XXparameter|odbc_interface.condition|param_exact dbms odbc mode input|3|15|P?|@{0|}}
{30001002|XXparameter|oledb_interface|api|15|1|Fl|Unload data via api or utility|{1|api|}}
{30001002|XXparameter|oledb_interface.condition|param_exact dbms oledb mode input|3|15|P?|@{0|}}
{30001002|XXparameter|sybase_interface|api|15|1|Fl|Unload data via api or utility|{1|api|}}
{30001002|XXparameter|sybase_interface.condition|param_exact dbms sybase mode input|3|15|P?|@{0|}}
{30001002|XXparameter|sybaseiq_interface|api|15|1|Fl|Unload data via api or utility|{1|api|}}
{30001002|XXparameter|sybaseiq_interface.condition|param_exact dbms sybaseiq mode input|3|15|P?|@{0|}}
{30001002|XXparameter|informix_interface|api|15|1|Fl|Unload data via api or utility|{4|api|dbaccess_serial|dbaccess_xps|pload|}}
{30001002|XXparameter|informix_interface.condition|param_exact dbms informix mode input|3|15|P?|@{0|}}
{30001002|XXparameter|ims_interface|api|15|1|Fl|Unload data via api or utility|{1|api|}}
{30001002|XXparameter|ims_interface.condition|param_exact dbms ims mode input|3|15|P?|@{0|}}
{30001002|XXparameter|redbrick_interface|utility|15|1|Fl|Unload data via api or utility|{2|api|utility|}}
{30001002|XXparameter|redbrick_interface.condition|param_exact dbms redbrick mode input|3|15|P?|@{0|}}
{30001002|XXparameter|adabas_interface|api|15|1|Fl|Unload data via api or utility|{1|api|}}
{30001002|XXparameter|adabas_interface.condition|param_exact dbms adabas mode input|3|15|P?|@{0|}}
{30001002|XXparameter|nza_interface|api|15|1|Fl|Unload data via api or utility|{1|api|}}
{30001002|XXparameter|nza_interface.condition|param_exact dbms netezza mode input|3|15|P?|@{0|}}
{30001002|XXparameter|db2_400_interface|api|15|1|Fl|Unload data via api or utility|{1|api|}}
{30001002|XXparameter|db2_400_interface.condition|param_exact dbms db2_400 mode input|3|15|P?|@{0|}}
{30001002|XXparameter|odbc_400_interface|api|15|1|Fl|Unload data via api or utility|{1|api|}}
{30001002|XXparameter|odbc_400_interface.condition|param_exact dbms odbc_400 mode input|3|15|P?|@{0|}}
{30001002|XXparameter|ora__interface|utility|15|1|Fl|Load data using api or utility|{2|api|utility|}}
{30001002|XXparameter|ora__interface.condition|param_exact dbms oracle mode output|3|15|P?|@{0|}}
{30001002|XXparameter|ter__interface|Multiload|15|1|Fl|Load data using api or utility|{7|api|Multiload|Fastload|Tpump|WB_load|WB_update|WB_stream|}}
{30001002|XXparameter|ter__interface.condition|param_exact dbms teradata mode output|3|15|P?|@{0|}}
{30001002|XXparameter|db2eee__interface|utility|15|1|Fl|Load data using api or utility|{2|api|utility|}}
{30001002|XXparameter|db2eee__interface.condition|param_exact dbms db2eee mode output|3|15|P?|@{0|}}
{30001002|XXparameter|db2ee__interface|utility|15|1|Fl|Load data using api or utility|{2|api|utility|}}
{30001002|XXparameter|db2ee__interface.condition|param_exact dbms db2ee mode output|3|15|P?|@{0|}}
{30001002|XXparameter|db2uss__interface|api|15|1|Fl|Load data using api or utility|{1|api|}}
{30001002|XXparameter|db2uss__interface.condition|param_exact dbms db2uss mode output|3|15|P?|@{0|}}
{30001002|XXparameter|mssql__interface|utility|15|1|Fl|Load data using api or utility|{2|api|utility|}}
{30001002|XXparameter|mssql__interface.condition|param_exact dbms mssql mode output|3|15|P?|@{0|}}
{30001002|XXparameter|nucleus__interface|utility|15|1|Fl|Load data using api or utility|{2|api|utility|}}
{30001002|XXparameter|nucleus__interface.condition|param_exact dbms nucleus mode output|3|15|P?|@{0|}}
{30001002|XXparameter|odbc__interface|api|15|1|Fl|Load data using api or utility|{1|api|}}
{30001002|XXparameter|odbc__interface.condition|param_exact dbms odbc mode output|3|15|P?|@{0|}}
{30001002|XXparameter|oledb__interface|api|15|1|Fl|Load data using api or utility|{1|api|}}
{30001002|XXparameter|oledb__interface.condition|param_exact dbms oledb mode output|3|15|P?|@{0|}}
{30001002|XXparameter|sybase__interface|utility|15|1|Fl|Load data using api or utility|{2|api|utility|}}
{30001002|XXparameter|sybase__interface.condition|param_exact dbms sybase mode output|3|15|P?|@{0|}}
{30001002|XXparameter|sybaseiq__interface|utility|15|1|Fl|Load data using api or utility|{2|api|utility|}}
{30001002|XXparameter|sybaseiq__interface.condition|param_exact dbms sybaseiq mode output|3|15|P?|@{0|}}
{30001002|XXparameter|informix__interface|api|15|1|Fl|Load data using api or utility|{5|api|dbload|dbaccess_xps|pload|dbaccess_serial|}}
{30001002|XXparameter|informix__interface.condition|param_exact dbms informix mode output|3|15|P?|@{0|}}
{30001002|XXparameter|redbrick__interface|utility|15|1|Fl|Load data using api or utility|{2|api|utility|}}
{30001002|XXparameter|redbrick__interface.condition|param_exact dbms redbrick mode output|3|15|P?|@{0|}}
{30001002|XXparameter|nza__interface|utility|15|1|Fl|Load data using api or utility|{2|api|utility|}}
{30001002|XXparameter|nza__interface.condition|param_exact dbms netezza mode output|3|15|P?|@{0|}}
{30001002|XXparameter|db2_400__interface|api|15|1|Fl|Load data using api or utility|{2|api|utility|}}
{30001002|XXparameter|db2_400__interface.condition|param_exact dbms db2_400 mode output|3|15|P?|@{0|}}
{30001002|XXparameter|odbc_400__interface|api|15|1|Fl|Load data using api or utility|{2|api|utility|}}
{30001002|XXparameter|odbc_400__interface.condition|param_exact dbms odbc_400 mode output|3|15|P?|@{0|}}
{30001002|XXparameter|_ab_param_must_exist|1|3|2|H$||{0|}}
{30001002|XXparameter|not_ims|value dbms ims False value dbms * True|13|13|v||{0|}}
{30001002|XXparameter|not_redbrick|value dbms redbrick False value dbms * True|13|13|v||{0|}}
{30001002|XXparameter|ora_or_redbrick|value dbms redbrick True value dbms oracle True value dbms * False|13|13|v||{0|}}
{30001002|XXparameter|api_or_redbrick|value dbms redbrick True value interface api True value dbms * False|13|13|v||{0|}}
{30001002|XXparameter|supports_num_error|value interface_out api False value dbms netezza True value dbms redbrick True value dbms oracle True value dbms db2eee True value dbms db2ee True value dbms mssql True value dbms sybase True value dbms sybaseiq True value dbms db2_400 True value dbms odbc_400 True value informix__interface dbload True value informix__interface pload True value dbms * False|13|13|v||{0|}}
{30001002|XXparameter|supports_rows_per_commit|value interface_out api False value dbms redbrick True value dbms oracle True value dbms db2eee True value dbms db2ee True value dbms mssql True value dbms sybase True value dbms sybaseiq True value informix__interface dbload True value dbms * False|13|13|v||{0|}}
{30001002|XXparameter|db2eee_or_db2ee|value dbms db2eee True value dbms db2ee True value dbms * False|13|13|v||{0|}}
{30001002|XXparameter|interface_in|sameas ora_interface sameas ter_interface sameas db2eee_interface sameas db2ee_interface sameas db2uss_interface sameas db2_400_interface sameas mssql_interface sameas nucleus_interface sameas odbc_interface sameas odbc_400_interface sameas oledb_interface sameas sybase_interface sameas sybaseiq_interface sameas informix_interface sameas ims_interface sameas redbrick_interface sameas nza_interface|3|13|v||{0|}}
{30001002|XXparameter|interface_in.condition|mode input|3|15|P?|@{0|}}
{30001002|XXparameter|interface_out|sameas ora__interface sameas ter__interface sameas db2eee__interface sameas db2ee__interface sameas db2uss__interface sameas db2_400__interface sameas mssql__interface sameas nucleus__interface sameas odbc__interface sameas odbc_400__interface sameas oledb__interface sameas sybase__interface sameas sybaseiq__interface sameas informix__interface sameas redbrick__interface sameas nza__interface|3|13|v||{0|}}
{30001002|XXparameter|interface_out.condition|mode output|3|15|P?|@{0|}}
{30001002|XXparameter|interface|sameas interface_in sameas interface_out|3|13|Kv||{0|}}
{30001002|XXparameter|fixed_size_dml|False|13|1|RFOl|Generate fixed size DML when possible|{0|}}
{30001002|XXparameter|fixed_size_dml.condition|param_exact not_ims True version :2.12.2.m4|3|15|P?|@{0|}}
{30001002|XXparameter|field_type_preference|delimited|15|1|RFKl|Format of generated dml|{3|delimited|variable|fixed|}}
{30001002|XXparameter|field_type_preference.condition|param_exact not_ims True version 2.12.2.m5:|3|15|P?|@{0|}}
{30001002|XXparameter|column_delimiter||3|2|FK$|Default generated column delimiter|{0|}}
{30001002|XXparameter|column_delimiter.condition|param_exact not_ims True|3|15|P?|@{0|}}
{30001002|XXparameter|max_rows||1|2|FK$|Maximum rows to unload|{0|}}
{30001002|XXparameter|max_rows.condition|mode input param_exact not_ims True|3|15|P?|@{0|}}
{30001002|XXparameter|ablocal_expr||3|2|FK$|DML expression for ABLOCAL() in select|{0|}}
{30001002|XXparameter|ablocal_expr.condition|mode input param_exact not_ims True param_exact api_or_redbrick True|3|15|P?|@{0|}}
{30001002|XXparameter|remove_delimiters|False|13|1|RFOl|Remove delimiters found in data|{0|}}
{30001002|XXparameter|remove_delimiters.condition|param_exact interface api mode input version 2.7: param_exact not_ims True|3|15|P?|@{0|}}
{30001002|XXparameter|delimiter_replacement||3|2|FK$|Replacement for field delimiters found in data|{0|}}
{30001002|XXparameter|delimiter_replacement.condition|param_exact interface api mode input version 2.7: param_exact not_ims True|3|15|P?|@{0|}}
{30001002|XXparameter|num_errors|50|1|2|FK$|Maximum number of errors allowed|{0|}}
{30001002|XXparameter|num_errors.condition|mode output param_exact supports_num_error True|3|15|P?|@{0|}}
{30001002|XXparameter|commitNumber||1|2|FK$|Number of records per commit checkpoint|{0|}}
{30001002|XXparameter|commitNumber.condition|mode output param_exact interface api|3|15|P?|@{0|}}
{30001002|XXparameter|commitTable||24|3|FKc|Database table to store commit progress|{0|}}
{30001002|XXparameter|commitTable.condition|mode output param_exact interface api|3|15|P?|@{0|}}
{30001002|XXparameter|jobid||25|3|FKc|Unique job identifier|{0|}}
{30001002|XXparameter|jobid.condition|mode output param_exact interface api|3|15|P?|@{0|}}
{30001002|XXparameter|rows_per_commit||1|2|FK$|Rows per commit|{0|}}
{30001002|XXparameter|rows_per_commit.condition|mode output param_exact supports_rows_per_commit True|3|15|P?|@{0|}}
{30001002|XXparameter|ora_syb_nza_util_out|value ora__interface utility True value ora__interface api False value sybase__interface utility True value sybase__interface api False value nza__interface utility True value nza__interface api False|13|13|v||{0|}}
{30001002|XXparameter|file_type_modifier||25|3|FKc|Enter desired file type modifiers.|{0|}}
{30001002|XXparameter|file_type_modifier.condition|mode output param_exact interface utility param_exact db2eee_or_db2ee True|3|15|P?|@{0|}}
{30001002|XXparameter|row_count|0|1|2|FK$|Refer to DB2 UDB documentation for description.|{0|}}
{30001002|XXparameter|row_count.condition|mode output param_exact interface utility param_exact db2eee_or_db2ee True|3|15|P?|@{0|}}
{30001002|XXparameter|save_count|0|1|2|FK$|Refer to DB2 UDB documentation for description.|{0|}}
{30001002|XXparameter|save_count.condition|mode output param_exact interface utility param_exact db2eee_or_db2ee True|3|15|P?|@{0|}}
{30001002|XXparameter|data_buffer_size|0|1|2|FK$|Refer to DB2 UDB documentation for description.|{0|}}
{30001002|XXparameter|data_buffer_size.condition|mode output param_exact interface utility param_exact db2eee_or_db2ee True|3|15|P?|@{0|}}
{30001002|XXparameter|sort_buffer_size|0|1|2|FK$|Refer to DB2 UDB documentation for description.|{0|}}
{30001002|XXparameter|sort_buffer_size.condition|mode output param_exact interface utility param_exact db2eee_or_db2ee True|3|15|P?|@{0|}}
{30001002|XXparameter|hold_quiesce|False|13|1|FKl|Set to True to leave the utility in quiesced exclusive state.|{0|}}
{30001002|XXparameter|hold_quiesce.condition|mode output param_exact interface utility param_exact db2eee_or_db2ee True|3|15|P?|@{0|}}
{30001002|XXparameter|cpu_parallelism|1|1|2|FK$|Refer to DB2 UDB documentation for description.|{0|}}
{30001002|XXparameter|cpu_parallelism.condition|mode output param_exact interface utility param_exact db2eee_or_db2ee True|3|15|P?|@{0|}}
{30001002|XXparameter|disk_parallelism|0|1|2|FK$|Refer to DB2 UDB documentation for description.|{0|}}
{30001002|XXparameter|disk_parallelism.condition|mode output param_exact interface utility param_exact db2eee_or_db2ee True|3|15|P?|@{0|}}
{30001002|XXparameter|recoverable|False|13|1|FKl|Refer to DB2 UDB documentation for description.|{0|}}
{30001002|XXparameter|recoverable.condition|mode output param_exact interface utility param_exact db2eee_or_db2ee True|3|15|P?|@{0|}}
{30001002|XXparameter|indexing_mode|autoselect|15|1|FKl|Refer to DB2 UDB documentation for description.|{4|autoselect|rebuild|incremental|deferred|}}
{30001002|XXparameter|indexing_mode.condition|mode output param_exact interface utility param_exact db2eee_or_db2ee True|3|15|P?|@{0|}}
{30001002|XXparameter|access_level|allow_no_access|15|1|FKl|Refer to DB2 UDB documentation for description.|{2|allow_no_access|allow_read_access|}}
{30001002|XXparameter|access_level.condition|mode output param_exact interface utility param_exact db2eee_or_db2ee True|3|15|P?|@{0|}}
{30001002|XXparameter|lock_with_force|False|13|1|FKl|Refer to DB2 UDB documentation for description.|{0|}}
{30001002|XXparameter|lock_with_force.condition|mode output param_exact interface utility param_exact db2eee_or_db2ee True|3|15|P?|@{0|}}
{30001002|XXparameter|check_pending|deferred|15|1|FKl|Refer to DB2 UDB documentation for description.|{2|immediate|deferred|}}
{30001002|XXparameter|check_pending.condition|mode output param_exact interface utility param_exact db2eee_or_db2ee True|3|15|P?|@{0|}}
{30001002|XXparameter|statsopt|none|15|1|FKl|Refer to DB2 UDB documentation for description.|{9|none|index|table|both|exttable_only|exttable_index|extindex_only|extindex_table|all|}}
{30001002|XXparameter|statsopt.condition|mode output param_exact interface utility param_exact db2eee_or_db2ee True|3|15|P?|@{0|}}
{30001002|XXparameter|use_exception_table|False|13|1|FKl|Set to True to use exception table.|{0|}}
{30001002|XXparameter|use_exception_table.condition|mode output param_exact interface utility param_exact db2eee_or_db2ee True|3|15|P?|@{0|}}
{30001002|XXparameter|exception_table||25|3|FKc|Use only if you want to explicity specify the name for the exception table.|{0|}}
{30001002|XXparameter|exception_table.condition|mode output param_exact interface utility param_exact db2eee_or_db2ee True|3|15|P?|@{0|}}
{30001002|XXparameter|inf_dummy|a|3|2|$||{0|}}
{30001002|XXparameter|inf_util_in|value informix_interface api False value informix_interface * True|13|13|v||{0|}}
{30001002|XXparameter|inf_util_in.condition|param_exact dbms informix mode input|3|15|P?|@{0|}}
{30001002|XXparameter|inf_util_out|value informix__interface api False value informix__interface * True|13|13|v||{0|}}
{30001002|XXparameter|inf_util_out.condition|param_exact dbms informix mode output|3|15|P?|@{0|}}
{30001002|XXparameter|inf_util|value inf_util_in True True value inf_util_out True True value inf_dummy * False|13|13|v||{0|}}
{30001002|XXparameter|inf_util.condition|param_exact dbms informix|3|15|P?|@{0|}}
{30001002|XXparameter|inf_use_rej|value informix__interface dbaccess_xps True value informix__interface dbload True value informix__interface pload True value inf_dummy * False|13|13|v||{0|}}
{30001002|XXparameter|inf_use_rej.condition|param_exact inf_util_out True|3|15|P?|@{0|}}
{30001002|XXparameter|idelim|\||3|2|FK$|One-character delimiter for Informix load utility|{0|}}
{30001002|XXparameter|idelim.condition|param_exact inf_util_out True|3|15|P?|@{0|}}
{30001002|XXparameter|pdqpriority||3|2|FK$|DEFAULT, LOW, HIGH, ...|{0|}}
{30001002|XXparameter|pdqpriority.condition|param_exact inf_util True|3|15|P?|@{0|}}
{30001002|XXparameter|jobname||3|2|RFK$|Jobname for High-performance Load/Unload|{0|}}
{30001002|XXparameter|jobname.condition|param_exact interface pload|3|15|P?|@{0|}}
{30001002|XXparameter|pload_job_method|new|15|1|RFKl||{3|new|overwrite|reuse|}}
{30001002|XXparameter|pload_job_method.condition|param_exact interface pload|3|15|P?|@{0|}}
{30001002|XXparameter|isolation||3|2|FK$|'set isolation' choices: DIRTY READ, COMMITTED READ, ...|{0|}}
{30001002|XXparameter|isolation.condition|param_exact inf_util_in True|3|15|P?|@{0|}}
{30001002|XXparameter|ansi_isolation|default|15|1|FKl|'set transaction' isolation choices|{5|default|READ COMMITTED|REPEATABLE READ|SERIALIZABLE|READ UNCOMMITTED|}}
{30001002|XXparameter|ansi_isolation.condition|param_exact inf_util_in True|3|15|P?|@{0|}}
{30001002|XXparameter|force_raw|False|13|1|FKl|If true, temporarily alters table log mode to RAW|{0|}}
{30001002|XXparameter|force_raw.condition|param_exact inf_util_out True param_exact informix__interface dbaccess_xps|3|15|P?|@{0|}}
{30001002|XXparameter|rejectfile||3|2|RFK$|Rejectfile specification for Informix load utility|{0|}}
{30001002|XXparameter|rejectfile.condition|param_exact inf_use_rej True|3|15|P?|@{0|}}
{30001002|XXparameter|dummy1|a|3|2|$||{0|}}
{30001002|XXparameter|ter_util_in|value ter_interface api False value ter_interface * True|13|13|v||{0|}}
{30001002|XXparameter|ter_util_in.condition|param_exact dbms teradata mode input|3|15|P?|@{0|}}
{30001002|XXparameter|ter_util_out|value ter__interface api False value ter__interface * True|13|13|v||{0|}}
{30001002|XXparameter|ter_util_out.condition|param_exact dbms teradata mode output|3|15|P?|@{0|}}
{30001002|XXparameter|ter_util|value ter_util_in True True value ter_util_out True True value dummy1 * False|13|13|v||{0|}}
{30001002|XXparameter|ter_util.condition|param_exact dbms teradata|3|15|P?|@{0|}}
{30001002|XXparameter|ter_wb_in|value ter_interface WB_export True value ter_interface * False|13|13|v||{0|}}
{30001002|XXparameter|ter_wb_in.condition|param_exact dbms teradata mode input|3|15|P?|@{0|}}
{30001002|XXparameter|ter_wb_out|value ter__interface WB_* True value ter__interface * False|13|13|v||{0|}}
{30001002|XXparameter|ter_wb_out.condition|param_exact dbms teradata mode output|3|15|P?|@{0|}}
{30001002|XXparameter|ter_wb|value ter_wb_in True True value ter_wb_out True True value dummy1 * False|13|13|v||{0|}}
{30001002|XXparameter|ter_wb.condition|param_exact dbms teradata|3|15|P?|@{0|}}
{30001002|XXparameter|ter_m_or_t|value ter__interface Multiload True value ter__interface Tpump True value ter__interface WB_update True value ter__interface WB_stream True value ter__interface * False|13|13|v||{0|}}
{30001002|XXparameter|ter_m_or_t.condition|param_exact dbms teradata mode output|3|15|P?|@{0|}}
{30001002|XXparameter|ter_is_tpump|value ter__interface Tpump True value ter__interface WB_stream True value ter__interface * False|13|13|v||{0|}}
{30001002|XXparameter|ter_is_tpump.condition|param_exact dbms teradata mode output|3|15|P?|@{0|}}
{30001002|XXparameter|ter_show_brief|value ter_m_or_t True True value ter_util_in True True value dummy1 * False|13|13|v||{0|}}
{30001002|XXparameter|ter_show_brief.condition|param_exact dbms teradata|3|15|P?|@{0|}}
{30001002|XXparameter|brief|True|13|1|FKl|Ask utility to generate brief log info if TRUE|{0|}}
{30001002|XXparameter|brief.condition|param_exact ter_show_brief True|3|15|P?|@{0|}}
{30001002|XXparameter|sleep|6|1|2|FK$|Minutes to wait before retrying logon|{0|}}
{30001002|XXparameter|sleep.condition|param_exact ter_util True|3|15|P?|@{0|}}
{30001002|XXparameter|tenacity|0|1|2|FK$|Maximum hours to attempt logon before multiload gives up|{0|}}
{30001002|XXparameter|tenacity.condition|param_exact ter_util True|3|15|P?|@{0|}}
{30001002|XXparameter|sessions|* *|3|2|FK$|Number of  sessions|{0|}}
{30001002|XXparameter|sessions.condition|param_exact ter_util True|3|15|P?|@{0|}}
{30001002|XXparameter|instances|1|1|2|FK$|Number of Warehouse Builder Operator instances|{0|}}
{30001002|XXparameter|instances.condition|param_exact ter_wb True|3|15|P?|@{0|}}
{30001002|XXparameter|logtab_name||3|2|FK$|User-specified logtable name (optional)|{0|}}
{30001002|XXparameter|logtab_name.condition|param_exact ter_util True|3|15|P?|@{0|}}
{30001002|XXparameter|fexp_logfile||18|2|FKf$|Name of file to store Export log information (optional)|{0|}}
{30001002|XXparameter|fexp_logfile.condition|param_exact ter_util_in True|3|15|P?|@{0|}}
{30001002|XXparameter|vartext|False|13|1|FKl|Send data to Teradata load utility in VARTEXT mode|{0|}}
{30001002|XXparameter|vartext.condition|param_exact ter_util_out True|3|15|P?|@{0|}}
{30001002|XXparameter|delim|\||3|2|FK$|One-character delimiter for Teradata load utility|{0|}}
{30001002|XXparameter|delim.condition|param_exact vartext True|3|15|P?|@{0|}}
{30001002|XXparameter|np_axsmod_dir||18|2|RFKf$|Name of directory for Named Pipe AXSMOD storage (required)|{0|}}
{30001002|XXparameter|np_axsmod_dir.condition|param_exact ter_util_out True|3|15|P?|@{0|}}
{30001002|XXparameter|checkpoint||1|2|FK$|Checkpoint rate to be used by Teradata utility|{0|}}
{30001002|XXparameter|checkpoint.condition|param_exact ter_util_out True|3|15|P?|@{0|}}
{30001002|XXparameter|logfile||18|2|FKf$|Name of file to store load utility log information (optional)|{0|}}
{30001002|XXparameter|logfile.condition|param_exact ter_util_out True|3|15|P?|@{0|}}
{30001002|XXparameter|worktab_name||3|2|FK$|User-specified worktable name (optional)|{0|}}
{30001002|XXparameter|worktab_name.condition|param_exact ter_util_out True param_exact ter_is_tpump False|3|15|P?|@{0|}}
{30001002|XXparameter|errtab_name||3|2|FK$|User-specified errtable name (optional) [will have 1,2.. appended]|{0|}}
{30001002|XXparameter|errtab_name.condition|param_exact ter_util_out True|3|15|P?|@{0|}}
{30001002|XXparameter|macrodb||3|2|FK$|Database to use for Tpump macro creation and usage|{0|}}
{30001002|XXparameter|macrodb.condition|param_exact ter_is_tpump True|3|15|P?|@{0|}}
{30001002|XXparameter|errlimit||1|2|FK$|number errors allowed|{0|}}
{30001002|XXparameter|errlimit.condition|param_exact ter_util_out True|3|15|P?|@{0|}}
{30001002|XXparameter|errpercent||1|2|FK$|% errors allowed|{0|}}
{30001002|XXparameter|errpercent.condition|param_exact ter_m_or_t True|3|15|P?|@{0|}}
{30001002|XXparameter|ignore_dupl_rows|False|13|1|FKl|If true, ignore duplicate rows on insert|{0|}}
{30001002|XXparameter|ignore_dupl_rows.condition|param_exact ter_m_or_t True|3|15|P?|@{0|}}
{30001002|XXparameter|serialize|False|13|1|FKl|Guarantee serializability of operations on the same key if true|{0|}}
{30001002|XXparameter|serialize.condition|param_exact ter_is_tpump True|3|15|P?|@{0|}}
{30001002|XXparameter|pack|1|1|2|FK$|# of statements to pack into a Tpump request|{0|}}
{30001002|XXparameter|pack.condition|param_exact ter_is_tpump True|3|15|P?|@{0|}}
{30001002|XXparameter|rate||1|2|FK$|# of statements per minute to send to Teradata database|{0|}}
{30001002|XXparameter|rate.condition|param_exact ter_is_tpump True|3|15|P?|@{0|}}
{30001002|XXparameter|ora_util_out|value ora__interface utility True value ora__interface api False|13|13|v||{0|}}
{30001002|XXparameter|ora_util_out.condition|param_exact dbms oracle mode output|3|15|P?|@{0|}}
{30001002|XXparameter|oracle_max_kb||1|2|FK$|max kilobytes for each parallel unload select|{0|}}
{30001002|XXparameter|oracle_max_kb.condition|param_exact dbms oracle mode input version 2.10.12.m12:|3|15|P?|@{0|}}
{30001002|XXparameter|direct|True|13|1|RFOl|Use SQL*Loader Direct Mode|{0|}}
{30001002|XXparameter|direct.condition|param_exact ora_util_out True|3|15|P?|@{0|}}
{30001002|XXparameter|num_discards||1|2|FK$|# discards allowed|{0|}}
{30001002|XXparameter|num_discards.condition|param_exact ora_util_out True|3|15|P?|@{0|}}
{30001002|XXparameter|bindsize||1|2|FK$|SQL*Loader BINDSIZE setting|{0|}}
{30001002|XXparameter|bindsize.condition|param_exact ora_util_out True|3|15|P?|@{0|}}
{30001002|XXparameter|control_file||17|2|FKf$|Optional database load utility control file|{0|}}
{30001002|XXparameter|control_file.condition|param_exact ora_syb_nza_util_out True version 2.7:|3|15|P?|@{0|}}
{30001002|XXparameter|partition_name||3|2|FK$|Database partition name|{0|}}
{30001002|XXparameter|partition_name.condition|param_exact ora_util_out True|3|15|P?|@{0|}}
{30001002|XXparameter|native_options||25|3|FKc|Optional commands passed directly to the database load utility.|{0|}}
{30001002|XXparameter|native_options.condition|param_exact ora_syb_nza_util_out True|3|15|P?|@{0|}}
{30001002|XXparameter|syb_util_out|value sybase__interface utility True value sybase__interface api False|13|13|v||{0|}}
{30001002|XXparameter|syb_util_out.condition|param_exact dbms sybase mode output|3|15|P?|@{0|}}
{30001002|XXparameter|redbrick_util_out|value redbrick__interface utility True value redbrick__interface api False|13|13|v||{0|}}
{30001002|XXparameter|redbrick_util_out.condition|param_exact dbms redbrick mode output|3|15|P?|@{0|}}
{30001002|XXparameter|time_per_commit||1|2|FK$|# minutes per commit|{0|}}
{30001002|XXparameter|time_per_commit.condition|param_exact redbrick_util_out True|3|15|P?|@{0|}}
{30001002|XXparameter|tmu_control_file||17|2|FKf$|Optional TMU control file|{0|}}
{30001002|XXparameter|tmu_control_file.condition|param_exact redbrick_util_out True|3|15|P?|@{0|}}
{30001002|XXparameter|versioning||3|2|FK$|Settings for SET VERSIONING|{0|}}
{30001002|XXparameter|versioning.condition|param_exact redbrick_util_out True|3|15|P?|@{0|}}
{30001002|XXparameter|dateformat||3|2|FK$|Settings for SET DATEFORMAT|{0|}}
{30001002|XXparameter|dateformat.condition|param_exact redbrick_util_out True|3|15|P?|@{0|}}
{30001002|XXparameter|native_set_options||3|2|FK$|Settings before LOAD DATA|{0|}}
{30001002|XXparameter|native_set_options.condition|param_exact redbrick_util_out True|3|15|P?|@{0|}}
{30001002|XXparameter|native_load_options||3|2|FK$|Settings after LOAD DATA|{0|}}
{30001002|XXparameter|native_load_options.condition|param_exact redbrick_util_out True|3|15|P?|@{0|}}
{30001002|XXparameter|locale||3|2|FK$||{0|}}
{30001002|XXparameter|locale.condition|param_exact redbrick_util_out True|3|15|P?|@{0|}}
{30001002|XXparameter|ims_api_in|value ims_interface api True value ims_interface utility False|13|13|v||{0|}}
{30001002|XXparameter|ims_api_in.condition|param_exact dbms ims mode input|3|15|P?|@{0|}}
{30001002|XXparameter|batch_mode|DLI|15|1|RFKl|Run standalone batch or in a batch message processing region|{2|DLI|BMP|}}
{30001002|XXparameter|batch_mode.condition|param_exact ims_api_in True|3|15|P?|@{0|}}
{30001002|XXparameter|parent_segment_data|key_only|15|1|FKl|Unload just the keys from parents, or all the data|{2|key_only|full_segment|}}
{30001002|XXparameter|parent_segment_data.condition|param_exact ims_api_in True|3|15|P?|@{0|}}
{30001002|XXparameter|generate_unique_key|False|13|1|FKl|Generate a unique for segments with no unique key|{0|}}
{30001002|XXparameter|generate_unique_key.condition|param_exact ims_api_in True|3|15|P?|@{0|}}
{30001002|XXparameter|_ab_ims_path_command|m_db ims_path|3|2|$|Command to generate list of segments|{0|}}
{30001002|XXparameter|_ab_ims_path_command_params|config_file positional table_spec keyword|3|2|$|List of parameters for path command|{0|}}
{30001002|XXparameter|ims_path|generated _ab_ims_path_command page_change|3|13|FHv|List of segments from root to segment|{0|}}
{30001002|XXparameter|ims_path.is_index|true|3|9|P|@{0|}}
{30001002|XXparameter|ims_path.condition|param_exact dbms ims mode input|3|15|P?|@{0|}}
{30001002|XXparameter|ssa_||3|2|FK$|SSA qualifier for this segment|{0|}}
{30001002|XXparameter|ssa_.index|ims_path|3|9|P|@{0|}}
{30001002|XXparameter|ssa_.index_name|ssa_|3|9|P|@{0|}}
{30001002|XXparameter|ssa_.condition|param_exact dbms ims mode input|3|15|P?|@{0|}}
{30001002|XXparameter|ims_parameters||3|2|FK$|Other ims parameters|{0|}}
{30001002|XXparameter|ims_parameters.condition|param_exact ims_api_in True|3|15|P?|@{0|}}
{30001002|XXparameter|alternate_psbname||3|2|FK$|Alternate PSB name to use when starting DFSRRC00|{0|}}
{30001002|XXparameter|alternate_psbname.condition|param_exact ims_api_in True|3|15|P?|@{0|}}
{30001002|XXparameter|file_number||1|2|FK$|Set to valid file number to bypass PREDICT for file number lookup.|{0|}}
{30001002|XXparameter|file_number.condition|mode input param_exact dbms adabas|3|15|P?|@{0|}}
{30001002|XXparameter|nza_util_out|value nza__interface utility True value nza__interface api False|13|13|v||{0|}}
{30001002|XXparameter|nza_util_out.condition|param_exact dbms netezza mode output|3|15|P?|@{0|}}
{30001002|XXparameter|local_log_path||3|2|FK$|local path to log directory mapped from NPS server|{0|}}
{30001002|XXparameter|local_log_path.condition|param_exact nza_util_out True|3|15|P?|@{0|}}
{30001002|XXparameter|server_log_path||3|2|FK$|path on NPS server to log directory|{0|}}
{30001002|XXparameter|server_log_path.condition|param_exact nza_util_out True|3|15|P?|@{0|}}
{30001002|XXparameter|as400_util_out|value db2_400__interface utility True value db2_400__interface api False value odbc_400__interface utility True value odbc_400__interface api False|13|13|v||{0|}}
{30001002|XXparameter|hostname||3|2|FK$|Name of AS/400 system|{0|}}
{30001002|XXparameter|hostname.condition|param_exact as400_util_out True|3|15|P?|@{0|}}
{30001002|XXparameter|rebuild_loader|False|13|1|FKl|If True, rebuild the RPG table loader program on the AS/400.|{0|}}
{30001002|XXparameter|rebuild_loader.condition|param_exact as400_util_out True|3|15|P?|@{0|}}
{30001002|XXparameter|ifs_data_path||3|2|FK$|Location, on the AS/400, where data will be landed|{0|}}
{30001002|XXparameter|ifs_data_path.condition|param_exact as400_util_out True|3|15|P?|@{0|}}
{30001002|XXparameter|ifs_dml_path||3|2|FK$|Location, on the AS/400, where dml files will be landed|{0|}}
{30001002|XXparameter|ifs_dml_path.condition|param_exact as400_util_out True|3|15|P?|@{0|}}
{30001002|XXparameter|gendml_param_names|column_delimiter fixed_size_dml field_type_preference ablocal_expr parent_segment_data generate_unique_key|3|2|H$||{0|}}
{30001002|XXparameter|m_db_layout_param_names|ablocal_expr|3|2|H$||{0|}}
{30001002|XXparameter|m_db_dump_param_names|remove_delimiters delimiter_replacement ablocal_expr|3|2|H$||{0|}}
{30001002|XXparameter|dbms||3|2|RF$||{0|}}
{30001002|XXparameter|override_mpname|value ter_util_in True db-ter-fast-export value inf_util_in True db-inf-unload-data value dbms "" itable value dbms ims itable-ims value dbms * itable|3|13|v||{0|}}
{30001002|XXparameter|override_mpname.display_name|override-mpname|3|9|P|@{0|}}
{30001002|XXparameter|override_mpname.condition|mode input|3|15|P?|@{0|}}
{30001002|XXparameter|_ab_insert_statement_supported|value dbms ims False value dbms db2eee False value interface_out api True value dbms teradata True value dbms informix True value dbms * False|13|13|v||{0|}}
{30001002|XXparameter|_ab_select_statement_supported|value dbms "" True value dbms ims False value dbms * True|13|13|v||{0|}}
{30001002|XXparameter|condition||3|2|F$||{0|}}
{30001002|XXparameter|conditionInputPort||3|2|F$||{0|}}
{30001002|XXparameter|conditionOutputPort||3|2|F$||{0|}}
{30001002|XXparameter|condition_interpretation|Remove completely|15|1|Fl||{2|Replace with flow|Remove completely|}}
{30001002|XXparameter|condition_interpretation.display_name|condition-interpretation|3|9|P|@{0|}}
{30001002|XXparameter|read_metadata||7|1|RFl||{0|}}
{30001002|XXparameter|log_metadata||7|1|RFl||{0|}}
{30001002|XXparameter|mpcmodtime|1100942986|1|1|Hl|The last modification time of this component's template|{0|}}
{30001002|XXparameter|Layout|@9|2|RFs$||{0|}}
{30001002|XXparameter|_propagation|metadata type: log = "record string(\\"\|\\") node, timestamp, component, subcomponent, event_type; string(\\"\|\\\\n\\") event_text; end"|3|1|HKl|@{0|}}
{30001002|XXparameter|eme_dataset_location||3|2|F$|Place in the EME to create a dataset corresponding to this file.|{0|}}
}}@0|@0|0|0|0|0|0|0|@@@1|10|-1|@6|@1|}}
{2010203004|XXGoport|95|0|241|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|read|0.0|@@@2448|0|}}
{2010203004|XXGoport|96|0|244|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|log|0.0|@@@1176|0|}}
{2010501005|XXGpvertex|97|0|248|0|{|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|sorted_input|False|3|1|l|@{0|}}
{30001002|XXparameter|key|\{\}|3|2|$|@{0|}}
{30001002|XXparameter|transform|out::rollup(in) =
begin
  out.last_extract_value :: (string("\\n")) max(in.$\{ROLLUP_FIELD\});                 
end;|3|3|c|@{0|}}
{30001002|XXparameter|condition|$CNDTL_LAST_EXTRACT_ROLLUP|3|2|$|@{0|}}
{30001002|XXparameter|condition_interpretation|Remove completely|3|1|l|@{0|}}
{30001002|XXparameter|error_metadata|string('\\n')|3|1|l|@{0|}}
{30001002|XXparameter|log_metadata|record string("\|") node, timestamp, component, subcomponent, event_type; string("\|\\n") event_text; end|3|1|l|@{0|}}
{30001002|XXparameter|!prototype_path|C:\\Program Files\\Ab Initio\\Ab Initio GDE\\Components\\Transform\\Rollup.mpc|3|2|Pw$|@{0|}}
{30001002|XXparameter|Layout||3|8|s=|@{0|}}
{30001002|XXparameter|in_metadata||3|8|s=|@{0|}}
{30001002|XXparameter|out_metadata||3|8|s=|@{0|}}
{30001002|XXparameter|reject_metadata||3|8|s=|@{0|}}
}}@0|@373628|213447|394000|233000|144000|106000|7|Get Max Value for Extract Field|Ab Initio Software|Built-in 2.0:|1|10|-1||6||36865|1|{1|0|}}}
{2010203004|XXGoport|98|0|250|0|{@{}@537000|280000|11000|11000|out|0.0|@@@2448|0|}}
{2010203004|XXGoport|99|0|253|0|{@{}@0|0|0|0|reject|0.0|@@@1168|0|}}
{2010203004|XXGoport|100|0|255|0|{@{}@0|0|0|0|error|0.0|@@@1176|0|}}
{2010203004|XXGoport|101|0|257|0|{@{}@0|0|0|0|log|0.0|@@@1176|0|}}
{2010202004|XXGiport|102|0|259|0|{@{}@384000|275000|11000|21000|in|0.0|@@@1808|0|}}
{2010501005|XXGpvertex|103|0|263|0|{Generates data records that summarize groups of data records.  Rollup in Memory maximizes performance by keeping intermediate results in main memory.|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|mpname|?rollup|3|1|Hl|The name used on the mp command line for this component|{0|}}
{30001002|XXparameter|image__|/~ab_home/bin/transform|3|2|H$|The image used if this component was a custom component|{0|}}
{30001002|XXparameter|sorted_input|True|13|1|Fl|Ordering of input data|{0|}}
{30001002|XXparameter|sorted_input.display_name|sorted-input|3|9|P|@{0|}}
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
{30001002|XXparameter|transform||8|1|RFOl|Filename containing rollup functions|{0|}}
{30001002|XXparameter|max_core|67108864|1|2|FK$|Maximum memory usage in bytes|{0|}}
{30001002|XXparameter|max_core.display_name|max-core|3|9|P|@{0|}}
{30001002|XXparameter|max_core.keyword|max-core|3|9|P|@{0|}}
{30001002|XXparameter|max_core.condition|param sorted-input False|3|15|P?|@{0|}}
{30001002|XXparameter|max_core.update_clauses|retain_old_default|3|9|P|@{0|}}
{30001002|XXparameter|reject_threshold|Abort on first reject|15|1|Fl|When to abort if input records are rejected|{3|Abort on first reject|Never abort|Use limit/ramp|}}
{30001002|XXparameter|reject_threshold.display_name|reject-threshold|3|9|P|@{0|}}
{30001002|XXparameter|limit|0|1|2|F$|Maximum rejected records before failure|{0|}}
{30001002|XXparameter|limit.condition|param reject-threshold Use*|3|15|P?|@{0|}}
{30001002|XXparameter|ramp|0.0|2|2|F$|Rate of rejected records|{0|}}
{30001002|XXparameter|ramp.condition|param reject-threshold Use*|3|15|P?|@{0|}}
{30001002|XXparameter|limit_keyword|value reject-threshold Never* 0 value reject-threshold Abort* 0 sameas limit|1|13|Kv|Maximum rejected records before failure|{0|}}
{30001002|XXparameter|ramp_keyword|value reject-threshold Never* 99.0 value reject-threshold Abort* 0.0 sameas ramp|2|13|Kv|Rate of rejected records|{0|}}
{30001002|XXparameter|ramp_keyword.condition|version :2.12.2.m9|3|15|P?|@{0|}}
{30001002|XXparameter|ramp_keyword2|value reject-threshold Never* -1 value reject-threshold Abort* 0.0 sameas ramp|2|13|Kv|Rate of rejected records|{0|}}
{30001002|XXparameter|ramp_keyword2.condition|version 2.13:|3|15|P?|@{0|}}
{30001002|XXparameter|keyword_map|limit_keyword limit ramp_keyword ramp ramp_keyword2 ramp|3|2|$||{0|}}
{30001002|XXparameter|keyword_map.display_name|keyword-map|3|9|P|@{0|}}
{30001002|XXparameter|check_sort|True|13|1|FKl|Abort on first record that is out of sorted order|{0|}}
{30001002|XXparameter|check_sort.display_name|check-sort|3|9|P|@{0|}}
{30001002|XXparameter|check_sort.keyword|check-sort|3|9|P|@{0|}}
{30001002|XXparameter|check_sort.condition|param key-method True param sorted-input True version 2.11.1:|3|15|P?|@{0|}}
{30001002|XXparameter|check_sort.update_clauses|update_value False|3|9|P|@{0|}}
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
{30001002|XXparameter|Layout|@9|2|RFs$||{0|}}
{30001002|XXparameter|in_metadata||7|1|RFsl||{0|}}
{30001002|XXparameter|out_metadata||7|1|RFsl||{0|}}
{30001002|XXparameter|reject_metadata||7|1|Fsl||{0|}}
{30001002|XXparameter|error_metadata||7|1|Fl||{0|}}
{30001002|XXparameter|log_metadata||7|1|Fl||{0|}}
{30001002|XXparameter|doc_transform||8|2|FHs$|Document your transformation for dependency analysis|{0|}}
{30001002|XXparameter|doc_operation1|out,reject,error,log::document(in)|3|2|RH$|The custom transformation|{0|}}
{30001002|XXparameter|mpcmodtime|0|1|1|Hl|The last modification time of this component's template|{0|}}
{30001002|XXparameter|num_operations|6|1|2|H$|Number of steps in rollup|{0|}}
{30001002|XXparameter|num_variables|1|1|2|H$|Number of public variables in rollup|{0|}}
{30001002|XXparameter|variable1|optional temporary_type temp|3|2|H$|Temporary variable|{0|}}
{30001002|XXparameter|operation1|optional int out::input_select(in)|3|2|H$|Select input record|{0|}}
{30001002|XXparameter|operation2|optional temp ? temp::initialize(in) : NULL|3|2|H$|Initialize temporary|{0|}}
{30001002|XXparameter|operation3|temp ? temp::rollup(temp, in) : out::rollup(in)|3|2|H$|Do computation|{0|}}
{30001002|XXparameter|operation4|optional temp ? out::finalize(temp, in) : NULL|3|2|H$|Create output record|{0|}}
{30001002|XXparameter|operation5|optional int out::output_select(out)|3|2|H$|Select output record|{0|}}
{30001002|XXparameter|operation6|optional integer(8) out::key_change(in in1, in in2)|3|2|H$|Determine if key changed|{0|}}
{30001002|XXparameter|operation1_analysis|int input_select_out::input_select(in)|3|2|H$||{0|}}
{30001002|XXparameter|operation5_analysis|int output_select_out::output_select(out)|3|2|H$|Select output record|{0|}}
{30001002|XXparameter|operation6_analysis|integer(8) key_change_out::key_change(in in, in in)|3|2|H$|Determine if key changed|{0|}}
{30001002|XXparameter|port_analysis|reject=in|3|2|H$||{0|}}
{30001002|XXparameter|_propagation|metadata type: reject = in
metadata type: error = "string('\\\\n')"
metadata type: log = "record string(\\"\|\\") node, timestamp, component, subcomponent, event_type; string(\\"\|\\\\n\\") event_text; end"|3|1|HKl|@{0|}}
{30001002|XXparameter|_propagate_through||3|1|FHKl|@{0|}}
}}@0|@0|0|0|0|0|0|0|@@@1|10|-1|@6|@1|1|{1|0|}}}
{2010203004|XXGoport|104|0|265|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|out|0.0|@@@2448|0|}}
{2010203004|XXGoport|105|0|268|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|F=||{0|}}
}}@0|0|0|0|reject|0.0|@@@1168|0|}}
{2010203004|XXGoport|106|0|271|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|F=||{0|}}
}}@0|0|0|0|error|0.0|@@@1176|0|}}
{2010203004|XXGoport|107|0|274|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|F=||{0|}}
}}@0|0|0|0|log|0.0|@@@1176|0|}}
{2010202004|XXGiport|108|0|277|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|in|0.0|@@@1808|0|}}
{2010503005|XXGfvertex|109|0|280|0|{|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|condition|$CNDTL_LAST_EXTRACT_ROLLUP|3|2|$|@{0|}}
{30001002|XXparameter|Layout|$[[record kind 85 subkind 0 parts [vector _interp_("file:$LAST_EXTRACT_VALUE_FILE", "dollar_substitution")]]]|3|9||@{0|}}
{30001002|XXparameter|write_metadata|record
  string("\\n") last_extract_value;
end;|3|1|l|@{0|}}
{30001002|XXparameter|!prototype_path|C:\\Program Files\\Ab Initio\\Ab Initio GDE\\Components\\Datasets\\Output_File.mdc|3|2|Pw$|@{0|}}
}}@0|@577174|246560|602000|267000|40000|40000|9|Last Extract Value File-1|Craig Werre||1|10|-1||6||32769|-1|-1|}}
{2010202004|XXGiport|110|0|282|0|{@{}@592000|281000|11000|11000|write|0.0|@@@1776|0|}}
{2010503005|XXGfvertex|111|0|285|0|{Represents one file, many files, or a multifile as an output from your graph.  |{30100001|XXparameter_set|@@@@{{30001002|XXparameter|protection|0666|12|2|RF$||{0|}}
{30001002|XXparameter|mode|0x0062|1|2|FH$|modes of access|{0|}}
{30001002|XXparameter|condition||3|2|F$||{0|}}
{30001002|XXparameter|conditionInputPort||3|2|F$||{0|}}
{30001002|XXparameter|conditionOutputPort||3|2|F$||{0|}}
{30001002|XXparameter|condition_interpretation|Remove completely|15|1|Fl||{2|Replace with flow|Remove completely|}}
{30001002|XXparameter|condition_interpretation.display_name|condition-interpretation|3|9|P|@{0|}}
{30001002|XXparameter|key||19|2|RF$|Key specifier For Lookup File|{0|}}
{30001002|XXparameter|key.condition|mode lookup|3|15|P?|@{0|}}
{30001002|XXparameter|Layout|@28|2|RF$||{0|}}
{30001002|XXparameter|write_metadata||7|1|RFl||{0|}}
{30001002|XXparameter|mpcmodtime|0|1|1|Hl|The last modification time of this component's template|{0|}}
{30001002|XXparameter|eme_dataset_location||3|2|F$|Place in the EME to create a dataset corresponding to this file.|{0|}}
}}@0|@0|0|0|0|0|0|0|@@@1|10|-1|@6|@1|-1|-1|}}
{2010202004|XXGiport|112|0|287|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|write|0.0|@@@1776|0|}}
{2010503005|XXGfvertex|113|0|294|0|{|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|condition|$CNDTL_LAST_EXTRACT_VARIABLE|3|2|$|@{0|}}
{30001002|XXparameter|Layout|$[[record kind 85 subkind 0 parts [vector _interp_("file:$LAST_EXTRACT_VALUE_FILE", "dollar_substitution")]]]|3|9||@{0|}}
{30001002|XXparameter|write_metadata|record
  string("\\n") last_extract_value;
end;|3|1|l|@{0|}}
{30001002|XXparameter|!prototype_path|C:\\Program Files\\Ab Initio\\Ab Initio GDE\\Components\\Datasets\\Output_File.mdc|3|2|Pw$|@{0|}}
}}@2|@570190|824820|595000|845000|40000|40000|22|Last Extract Value File|Craig Werre||1|10|-1||6||32769|-1|-1|}}
{2010202004|XXGiport|114|0|296|0|{@{}@585000|859000|11000|11000|write|0.0|@@@1776|0|}}
{2010503005|XXGfvertex|115|0|299|0|{Represents one file, many files, or a multifile as an output from your graph.  |{30100001|XXparameter_set|@@@@{{30001002|XXparameter|protection|0666|12|2|RF$||{0|}}
{30001002|XXparameter|mode|0x0062|1|2|FH$|modes of access|{0|}}
{30001002|XXparameter|condition||3|2|F$||{0|}}
{30001002|XXparameter|conditionInputPort||3|2|F$||{0|}}
{30001002|XXparameter|conditionOutputPort||3|2|F$||{0|}}
{30001002|XXparameter|condition_interpretation|Remove completely|15|1|Fl||{2|Replace with flow|Remove completely|}}
{30001002|XXparameter|condition_interpretation.display_name|condition-interpretation|3|9|P|@{0|}}
{30001002|XXparameter|key||19|2|RF$|Key specifier For Lookup File|{0|}}
{30001002|XXparameter|key.condition|mode lookup|3|15|P?|@{0|}}
{30001002|XXparameter|Layout|@28|2|RF$||{0|}}
{30001002|XXparameter|write_metadata||7|1|RFl||{0|}}
{30001002|XXparameter|mpcmodtime|1002323448|1|1|Hl|The last modification time of this component's template|{0|}}
{30001002|XXparameter|eme_dataset_location||3|2|F$|Place in the EME to create a dataset corresponding to this file.|{0|}}
}}@0|@0|0|0|0|0|0|0|@@@1|10|-1|@6|@1|-1|-1|}}
{2010202004|XXGiport|116|0|301|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|write|0.0|@@@1776|0|}}
{2010501005|XXGpvertex|117|0|309|0|{|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|condition|$CNDTL_LAST_EXTRACT_ROLLUP|3|2|$|@{0|}}
{30001002|XXparameter|!prototype_path|C:\\Program Files\\Ab Initio\\Ab Initio GDE\\Components\\Miscellaneous\\Replicate.mpc|3|2|Pw$|@{0|}}
{30001002|XXparameter|Layout||3|8|s=|@{0|}}
{30001002|XXparameter|in_metadata||3|8|s=|@{0|}}
{30001002|XXparameter|out_metadata||3|8|s=|@{0|}}
}}@0|@157000|76622|177000|97000|131000|65000|4|Replicate|Ab Initio Software|Built-in|1|10|-1||6||36865|1|{1|0|}}}
{2010203004|XXGoport|118|0|311|0|{@{}@307000|119000|11000|21000|out|0.0|@@@2068|0|}}
{2010202004|XXGiport|119|0|315|0|{@{}@167000|124000|11000|11000|in|0.0|@@@1812|0|}}
{2010501005|XXGpvertex|120|0|318|0|{Arbitrarily combines all the data records it receives into a single flow and writes a copy of that flow to each of its output flows.|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|mpname|broadcast|3|1|Hl|The name used on the mp command line for this component|{0|}}
{30001002|XXparameter|image__|unitool|3|2|H$|The image used if this component was a custom component|{0|}}
{30001002|XXparameter|NoHidePorts|in out|3|2|H$||{0|}}
{30001002|XXparameter|condition||3|2|F$||{0|}}
{30001002|XXparameter|conditionInputPort|in|3|2|F$||{0|}}
{30001002|XXparameter|conditionOutputPort|out|3|2|F$||{0|}}
{30001002|XXparameter|condition_interpretation|Replace with flow|15|1|Fl||{2|Replace with flow|Remove completely|}}
{30001002|XXparameter|condition_interpretation.display_name|condition-interpretation|3|9|P|@{0|}}
{30001002|XXparameter|Layout|@9|2|RFs$||{0|}}
{30001002|XXparameter|in_metadata||7|1|RFsl||{0|}}
{30001002|XXparameter|out_metadata||7|1|RFsl||{0|}}
{30001002|XXparameter|doc_transform||8|2|FHs$|Document your transformation for dependency analysis|{0|}}
{30001002|XXparameter|doc_operation1|out::document(in)|3|2|RH$|The custom transformation|{0|}}
{30001002|XXparameter|mpcmodtime|0|1|1|Hl|The last modification time of this component's template|{0|}}
{30001002|XXparameter|port_analysis|out=in|3|2|H$||{0|}}
{30001002|XXparameter|_propagate_through|metadata type: out = in
metadata type: in = out|3|1|FHKl|@{0|}}
}}@0|@0|0|0|0|0|0|0|@@@1|10|-1|@6|@1|1|{1|0|}}}
{2010203004|XXGoport|121|0|320|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|out|0.0|@@@2068|0|}}
{2010202004|XXGiport|122|0|323|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|in|0.0|@@@1812|0|}}
{2010501005|XXGpvertex|123|0|330|0|{|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|num_records|1|3|2|$|@{0|}}
{30001002|XXparameter|command_line|-expression $ROLLUP_FIELD '$FROM_EXTRACT_VALUE'|3|2|$|@{0|}}
{30001002|XXparameter|condition|$CNDTL_LAST_EXTRACT_ROLLUP|3|2|$|@{0|}}
{30001002|XXparameter|!prototype_path|C:\\Program Files\\Ab Initio\\Ab Initio GDE\\Components\\Validate\\Generate Records.mpc|3|2|Pw$|@{0|}}
{30001002|XXparameter|Layout||3|8|s=|@{0|}}
{30001002|XXparameter|out_metadata||3|8|s=|@{0|}}
}}@0|@142621|202275|163000|222000|149000|106000|3|Send From Extract Value to Rollup|Ab Initio Software|Built-in 2.0:|1|10|-1||6||36865|1|{1|0|}}}
{2010203004|XXGoport|124|0|332|0|{@{}@311000|269000|11000|11000|out|0.0|@@@2448|0|}}
{2010501005|XXGpvertex|125|0|335|0|{Generates a specified number of data records with fields of specified lengths and types.|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|mpname|generate|3|1|Hl|The name used on the mp command line for this component|{0|}}
{30001002|XXparameter|image__|unitool|3|2|H$|The image used if this component was a custom component|{0|}}
{30001002|XXparameter|num_records||1|2|RFO$|Number of records to generate|{0|}}
{30001002|XXparameter|command_line||16|2|FO$|Field-specific options|{0|}}
{30001002|XXparameter|condition||3|2|F$||{0|}}
{30001002|XXparameter|conditionInputPort||3|2|F$||{0|}}
{30001002|XXparameter|conditionOutputPort||3|2|F$||{0|}}
{30001002|XXparameter|condition_interpretation|Remove completely|15|1|Fl||{2|Replace with flow|Remove completely|}}
{30001002|XXparameter|condition_interpretation.display_name|condition-interpretation|3|9|P|@{0|}}
{30001002|XXparameter|Layout|@9|2|RFs$||{0|}}
{30001002|XXparameter|out_metadata||7|1|RFsl||{0|}}
{30001002|XXparameter|doc_transform||8|2|FHs$|Document your transformation for dependency analysis|{0|}}
{30001002|XXparameter|doc_operation1|out::document()|3|2|RH$|The custom transformation|{0|}}
{30001002|XXparameter|mpcmodtime|971909688|1|1|Hl|The last modification time of this component's template|{0|}}
}}@0|@0|0|0|0|0|0|0|@@@1|10|-1|@6|@1|1|{1|0|}}}
{2010203004|XXGoport|126|0|337|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|out|0.0|@@@2448|0|}}
{2010501005|XXGpvertex|127|0|342|0|{|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|num_records|1|3|2|$|@{0|}}
{30001002|XXparameter|condition|$CNDTL_LAST_EXTRACT_VARIABLE|3|2|$|@{0|}}
{30001002|XXparameter|out_metadata|record
  string(1) a;
end;|3|1|l|@{0|}}
{30001002|XXparameter|!prototype_path|C:\\Program Files\\Ab Initio\\Ab Initio GDE\\Components\\Validate\\Generate Records.mpc|3|2|Pw$|@{0|}}
{30001002|XXparameter|Layout||3|8|s=|@{0|}}
}}@2|@10914|791371|31000|811000|132000|106000|25|Send Primer Row to Reformat|Ab Initio Software|Built-in 2.0:|1|10|-1||6||36865|1|{1|0|}}}
{2010203004|XXGoport|128|0|344|0|{@{}@162000|858000|11000|11000|out|0.0|@@@2448|0|}}
{2010501005|XXGpvertex|129|0|347|0|{Generates a specified number of data records with fields of specified lengths and types.|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|mpname|generate|3|1|Hl|The name used on the mp command line for this component|{0|}}
{30001002|XXparameter|image__|unitool|3|2|H$|The image used if this component was a custom component|{0|}}
{30001002|XXparameter|num_records||1|2|RFO$|Number of records to generate|{0|}}
{30001002|XXparameter|command_line||16|2|FO$|Field-specific options|{0|}}
{30001002|XXparameter|condition||3|2|F$||{0|}}
{30001002|XXparameter|conditionInputPort||3|2|F$||{0|}}
{30001002|XXparameter|conditionOutputPort||3|2|F$||{0|}}
{30001002|XXparameter|condition_interpretation|Remove completely|15|1|Fl||{2|Replace with flow|Remove completely|}}
{30001002|XXparameter|condition_interpretation.display_name|condition-interpretation|3|9|P|@{0|}}
{30001002|XXparameter|Layout|@9|2|RFs$||{0|}}
{30001002|XXparameter|out_metadata||7|1|RFl||{0|}}
{30001002|XXparameter|doc_transform||8|2|FHs$|Document your transformation for dependency analysis|{0|}}
{30001002|XXparameter|doc_operation1|out::document()|3|2|RH$|The custom transformation|{0|}}
{30001002|XXparameter|mpcmodtime|971909688|1|1|Hl|The last modification time of this component's template|{0|}}
}}@0|@0|0|0|0|0|0|0|@@@1|10|-1|@6|@1|1|{1|0|}}}
{2010203004|XXGoport|130|0|349|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|out|0.0|@@@2448|0|}}
{2010501005|XXGpvertex|131|0|352|0|{|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|transform0|out::reformat(in) =
begin
  out.last_extract_value :: $TO_EXTRACT_VALUE;
end;|3|1|l|@{0|}}
{30001002|XXparameter|condition|$CNDTL_LAST_EXTRACT_VARIABLE|3|2|$|@{0|}}
{30001002|XXparameter|condition_interpretation|Remove completely|3|1|l|@{0|}}
{30001002|XXparameter|Layout|$[[record kind 85 subkind 0 parts [vector _interp_("file:$LAST_EXTRACT_VALUE_FILE", "dollar_substitution")]]]|3|9||@{0|}}
{30001002|XXparameter|error0_metadata|string('\\n')|3|1|l|@{0|}}
{30001002|XXparameter|log_metadata|record string("\|") node, timestamp, component, subcomponent, event_type; string("\|\\n") event_text; end|3|1|l|@{0|}}
{30001002|XXparameter|!prototype_path|C:\\Program Files\\Ab Initio\\Ab Initio GDE\\Components\\Transform\\Reformat.mpc|3|2|Pw$|@{0|}}
{30001002|XXparameter|in_metadata||3|8|s=|@{0|}}
{30001002|XXparameter|out0_metadata||3|8|s=|@{0|}}
{30001002|XXparameter|reject0_metadata||3|8|s=|@{0|}}
}}@2|@239222|791840|259000|812000|157000|106000|20|Update Last Extract Value File|Ab Initio Software|Built-in|1|10|-1||6||36865|1|{1|0|}}}
{2010203004|XXGoport|132|0|354|0|{@{}@415000|859000|11000|11000|out0|0.0|count|out|0|2448|0|}}
{2010203004|XXGoport|133|0|357|0|{@{}@0|0|0|0|reject0|0.0|count|reject|0|1168|0|}}
{2010203004|XXGoport|134|0|359|0|{@{}@0|0|0|0|error0|0.0|count|error|0|1176|0|}}
{2010203004|XXGoport|135|0|361|0|{@{}@0|0|0|0|log|0.0|@@@1176|0|}}
{2010202004|XXGiport|136|0|363|0|{@{}@249000|859000|11000|11000|in|0.0|@@@1808|0|}}
{2010501005|XXGpvertex|137|0|366|0|{Changes the record format of your data by dropping fields or by using DML expressions to add fields, combine fields, or modify the data.|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|mpname|reformat-transform|3|1|Hl|The name used on the mp command line for this component|{0|}}
{30001002|XXparameter|image__|unitool|3|2|H$|The image used if this component was a custom component|{0|}}
{30001002|XXparameter|count|1|1|1|Fl|Number of reformat transforms|{0|}}
{30001002|XXparameter|count.is_index|true|3|9|P|@{0|}}
{30001002|XXparameter|transform0||8|1|FOl|Reformat transform|{0|}}
{30001002|XXparameter|transform0.index|count|3|9|P|@{0|}}
{30001002|XXparameter|transform0.index_value|0|3|9|P|@{0|}}
{30001002|XXparameter|transform0.index_name|transform|3|9|P|@{0|}}
{30001002|XXparameter|select||20|2|FK$|Filter expression before reformatting|{0|}}
{30001002|XXparameter|reject_threshold|Abort on first reject|15|1|Fl|When to abort if input records are rejected|{3|Abort on first reject|Never abort|Use limit/ramp|}}
{30001002|XXparameter|reject_threshold.display_name|reject-threshold|3|9|P|@{0|}}
{30001002|XXparameter|limit|0|1|2|F$|Maximum rejected records before failure|{0|}}
{30001002|XXparameter|limit.condition|param reject-threshold Use*|3|15|P?|@{0|}}
{30001002|XXparameter|ramp|0.0|2|2|F$|Rate of rejected records|{0|}}
{30001002|XXparameter|ramp.condition|param reject-threshold Use*|3|15|P?|@{0|}}
{30001002|XXparameter|limit_keyword|value reject-threshold Never* 0 value reject-threshold Abort* 0 sameas limit|1|13|Kv|Maximum rejected records before failure|{0|}}
{30001002|XXparameter|ramp_keyword|value reject-threshold Never* 99.0 value reject-threshold Abort* 0.0 sameas ramp|2|13|Kv|Rate of rejected records|{0|}}
{30001002|XXparameter|ramp_keyword.condition|version :2.12.2.m9|3|15|P?|@{0|}}
{30001002|XXparameter|ramp_keyword2|value reject-threshold Never* -1 value reject-threshold Abort* 0.0 sameas ramp|2|13|Kv|Rate of rejected records|{0|}}
{30001002|XXparameter|ramp_keyword2.condition|version 2.13:|3|15|P?|@{0|}}
{30001002|XXparameter|keyword_map|limit_keyword limit ramp_keyword ramp ramp_keyword2 ramp|3|2|$||{0|}}
{30001002|XXparameter|keyword_map.display_name|keyword-map|3|9|P|@{0|}}
{30001002|XXparameter|output_index||8|1|FKl|Output index computation|{0|}}
{30001002|XXparameter|output_index.display_name|output-index|3|9|P|@{0|}}
{30001002|XXparameter|output_index.keyword|output-index|3|9|P|@{0|}}
{30001002|XXparameter|output_index_port|discriminator|3|2|FHK$|Output index port for debugger|{0|}}
{30001002|XXparameter|output_index_port.display_name|output-index_port|3|9|P|@{0|}}
{30001002|XXparameter|output_index_port.keyword|output-index_port|3|9|P|@{0|}}
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
{30001002|XXparameter|conditionOutputPort|out*|3|2|F$||{0|}}
{30001002|XXparameter|condition_interpretation|Replace with flow|15|1|Fl||{2|Replace with flow|Remove completely|}}
{30001002|XXparameter|condition_interpretation.display_name|condition-interpretation|3|9|P|@{0|}}
{30001002|XXparameter|Layout|@9|2|RF$||{0|}}
{30001002|XXparameter|in_metadata||7|1|RFsl||{0|}}
{30001002|XXparameter|out0_metadata||7|1|RFsl||{0|}}
{30001002|XXparameter|reject0_metadata||7|1|Fsl||{0|}}
{30001002|XXparameter|error0_metadata||7|1|Fl||{0|}}
{30001002|XXparameter|log_metadata||7|1|Fl||{0|}}
{30001002|XXparameter|doc_transform||8|2|FHs$|Document your transformation for dependency analysis|{0|}}
{30001002|XXparameter|doc_operation1|out[*],reject[*],error[*],log::document(in)|3|2|RH$|The custom transformation|{0|}}
{30001002|XXparameter|mpcmodtime|1099733698|1|1|Hl|The last modification time of this component's template|{0|}}
{30001002|XXparameter|output_index_signature|int out::output_index(in)|3|2|H$|Function returning index of output port|{0|}}
{30001002|XXparameter|output_index_signature.display_name|output-index_signature|3|9|P|@{0|}}
{30001002|XXparameter|output_indexes_signature|int[int] out::output_indexes(in)|3|2|H$|Function returning vector of indexes of output ports|{0|}}
{30001002|XXparameter|output_indexes_signature.display_name|output-indexes_signature|3|9|P|@{0|}}
{30001002|XXparameter|num_operations|1|1|2|H$|Number of operations per reformat|{0|}}
{30001002|XXparameter|operation1|out::reformat(in)|3|2|H$|Reformat operation|{0|}}
{30001002|XXparameter|port_analysis|reject*=in|3|2|H$||{0|}}
{30001002|XXparameter|main_mp_port|count out|3|1|l||{0|}}
{30001002|XXparameter|_propagation|metadata type: error = "string('\\\\n')"
metadata type: reject = in
metadata type: log = "record string(\\"\|\\") node, timestamp, component, subcomponent, event_type; string(\\"\|\\\\n\\") event_text; end"|3|1|HKl|@{0|}}
{30001002|XXparameter|_propagate_through||3|1|FHKl|@{0|}}
}}@0|@0|0|0|0|0|0|0|@@@1|10|-1|@6|@1|1|{1|0|}}}
{2010203004|XXGoport|138|0|368|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
{30001002|XXparameter|transform0||8|8|FO=|Reformat transform|{0|}}
{30001002|XXparameter|transform0.index|count|3|9|P|@{0|}}
{30001002|XXparameter|transform0.index_value|0|3|9|P|@{0|}}
{30001002|XXparameter|transform0.index_name|transform|3|9|P|@{0|}}
}}@0|0|0|0|out0|0.0|count|out|0|2448|0|}}
{2010203004|XXGoport|139|0|372|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|F=||{0|}}
}}@0|0|0|0|reject0|0.0|count|reject|0|1168|0|}}
{2010203004|XXGoport|140|0|375|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|F=||{0|}}
}}@0|0|0|0|error0|0.0|count|error|0|1176|0|}}
{2010203004|XXGoport|141|0|378|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|F=||{0|}}
}}@0|0|0|0|log|0.0|@@@1176|0|}}
{2010202004|XXGiport|142|0|381|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|in|0.0|@@@1808|0|}}
{2010501005|XXGpvertex|143|0|388|0|{Executes a standard UNIX or Windows NT program.

Executes the command:

chmod 664 $FEXP_LOGFILE

once $FEXP_LOGFILE exists.  Sleeps for 10 seconds in between looking for this file.|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|commandline|$DW_EXE/chmod_td_unload_logfile.ksh|3|2|$|@{0|}}
{30001002|XXparameter|Layout|$[[record kind 85 subkind 0 parts [vector _interp_("$DW_TMP", "dollar_substitution")]]]|3|9||@{0|}}
{30001002|XXparameter|!prototype_path|C:\\Program Files\\Ab Initio\\Ab Initio GDE\\Components\\Miscellaneous\\Run Program.mpc|3|2|Pw$|@{0|}}
}}@0|@21739|346377|42000|366000|191000|68000|31|chmod $FEXP_LOGFILE|Ab Initio Software|Built-in|1|10|-1||6||36865|1|{1|0|}}}
{2010203004|XXGoport|144|0|390|0|{@{}@232000|394000|11000|11000|out|0.0|@@@2260|0|}}
{2010202004|XXGiport|145|0|392|0|{@{}@32000|394000|11000|11000|in|0.0|@@@1732|0|}}
{2010501005|XXGpvertex|146|0|394|0|{Executes a standard UNIX or Windows NT program.|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|mpname|filter|3|1|Hl|The name used on the mp command line for this component|{0|}}
{30001002|XXparameter|image__|unitool|3|2|H$|The image used if this component was a custom component|{0|}}
{30001002|XXparameter|commandline||16|2|RFO$|Unix or NT command line|{0|}}
{30001002|XXparameter|NoHidePorts|in out|3|2|H$||{0|}}
{30001002|XXparameter|condition||3|2|F$||{0|}}
{30001002|XXparameter|conditionInputPort|in|3|2|F$||{0|}}
{30001002|XXparameter|conditionOutputPort|out|3|2|F$||{0|}}
{30001002|XXparameter|condition_interpretation|Replace with flow|15|1|Fl||{2|Replace with flow|Remove completely|}}
{30001002|XXparameter|condition_interpretation.display_name|condition-interpretation|3|9|P|@{0|}}
{30001002|XXparameter|in_metadata||7|1|Fsl||{0|}}
{30001002|XXparameter|out_metadata||7|1|Fsl||{0|}}
{30001002|XXparameter|doc_transform||8|2|FHs$|Document your transformation for dependency analysis|{0|}}
{30001002|XXparameter|doc_operation1|out::document(in)|3|2|RH$|The custom transformation|{0|}}
{30001002|XXparameter|mpcmodtime|1003947270|1|1|Hl|The last modification time of this component's template|{0|}}
{30001002|XXparameter|Layout|@9|2|RF$||{0|}}
{30001002|XXparameter|port_analysis|out=in|3|2|H$||{0|}}
{30001002|XXparameter|_propagate_through||3|1|FHKl|@{0|}}
}}@0|@0|0|0|0|0|0|0|@@@1|10|-1|@6|@1|1|{1|0|}}}
{2010203004|XXGoport|147|0|396|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|F=||{0|}}
}}@0|0|0|0|out|0.0|@@@2260|0|}}
{2010202004|XXGiport|148|0|399|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|F=||{0|}}
}}@0|0|0|0|in|0.0|@@@1732|0|}}
{2010110002|XXGconstant|149|0|402|0|{@{}@{1|6912177|}}}
{2010002001|XXGdirectory_object_object|0|1|2|0|{XXGgraph|}0|1|}
{2010704001|XXGgraph_graphinfo_graphinfo|1|0|2|0|{}1|2|}
{2010706001|XXGgraphinfo_runsettings_runsettings|2|0|4|0|{}2|3|}
{2010109001|XXGobject_property_value|3|0|6|0|{0|legend_face|92|}2|4|}
{2010605001|XXGgraph_legend_legend|4|0|8|0|{0|}1|5|}
{2010241001|XXGlegend_face_face|5|0|10|0|{}5|6|}
{2010605001|XXGgraph_legend_legend|6|0|12|0|{1|}1|7|}
{2010241001|XXGlegend_face_face|7|0|14|0|{}7|8|}
{2010605001|XXGgraph_legend_legend|8|0|16|0|{2|}1|9|}
{2010241001|XXGlegend_face_face|9|0|18|0|{}9|10|}
{2010604001|XXGgraph_flow_flow|10|0|20|0|{Flow_1|}1|11|}
{2010604001|XXGgraph_flow_flow|11|0|22|0|{Flow_10|}1|12|}
{2010604001|XXGgraph_flow_flow|12|0|24|0|{Flow_11|}1|13|}
{2010604001|XXGgraph_flow_flow|13|0|26|0|{Flow_12|}1|14|}
{2010604001|XXGgraph_flow_flow|14|0|28|0|{Flow_13|}1|15|}
{2010604001|XXGgraph_flow_flow|15|0|30|0|{Flow_14|}1|16|}
{2010604001|XXGgraph_flow_flow|16|0|32|0|{Flow_2|}1|17|}
{2010604001|XXGgraph_flow_flow|17|0|34|0|{Flow_3|}1|18|}
{2010604001|XXGgraph_flow_flow|18|0|36|0|{Flow_4|}1|19|}
{2010604001|XXGgraph_flow_flow|19|0|38|0|{Flow_5|}1|20|}
{2010604001|XXGgraph_flow_flow|20|0|40|0|{Flow_6|}1|21|}
{2010604001|XXGgraph_flow_flow|21|0|42|0|{Flow_7|}1|22|}
{2010604001|XXGgraph_flow_flow|22|0|44|0|{Flow_8|}1|23|}
{2010604001|XXGgraph_flow_flow|23|0|46|0|{Flow_9|}1|24|}
{2010601001|XXGgraph_vertex_vertex|24|0|48|0|{Deflate|}1|25|}
{2010212001|XXGvertex_oport_oport|25|0|50|0|{0|out|}25|26|}
{2010213001|XXGoport_dst_flow|26|0|52|0|{0|}26|16|}
{2010211001|XXGvertex_iport_iport|27|0|53|0|{0|in|}25|27|}
{2010214001|XXGiport_src_flow|28|0|55|0|{0|}27|15|}
{2010007001|XXGobject_proto_object|29|0|56|0|{}25|28|}
{2010212001|XXGvertex_oport_oport|30|0|58|0|{0|out|}28|29|}
{2010006001|XXGobject_psameas_object|31|0|60|0|{metadata|out_metadata|1|0|}29|28|}
{2010211001|XXGvertex_iport_iport|32|0|61|0|{0|in|}28|30|}
{2010006001|XXGobject_psameas_object|33|0|63|0|{metadata|in_metadata|1|0|}30|28|}
{2010006001|XXGobject_psameas_object|34|0|64|0|{Layout|Layout|0|0|}25|31|}
{2010211001|XXGvertex_iport_iport|35|0|66|0|{0|write|}31|32|}
{2010214001|XXGiport_src_flow|36|0|68|0|{0|}32|16|}
{2010007001|XXGobject_proto_object|37|0|69|0|{}31|33|}
{2010211001|XXGvertex_iport_iport|38|0|71|0|{0|write|}33|34|}
{2010006001|XXGobject_psameas_object|39|0|73|0|{metadata|write_metadata|1|0|}34|33|}
{2010006001|XXGobject_psameas_object|40|0|74|0|{write_metadata|out_metadata|0|0|}31|25|}
{2010006001|XXGobject_psameas_object|41|0|75|0|{in_metadata|out0_metadata|0|0|}25|35|}
{2010212001|XXGvertex_oport_oport|42|0|77|0|{0|out0|}35|36|}
{2010213001|XXGoport_dst_flow|43|0|79|0|{0|}36|15|}
{2010212001|XXGvertex_oport_oport|44|0|80|0|{1|reject0|}35|37|}
{2010212001|XXGvertex_oport_oport|45|0|82|0|{2|error0|}35|38|}
{2010212001|XXGvertex_oport_oport|46|0|84|0|{3|log|}35|39|}
{2010211001|XXGvertex_iport_iport|47|0|86|0|{0|in|}35|40|}
{2010214001|XXGiport_src_flow|48|0|88|0|{0|}40|13|}
{2010007001|XXGobject_proto_object|49|0|89|0|{}35|41|}
{2010212001|XXGvertex_oport_oport|50|0|91|0|{0|out0|}41|42|}
{2010006001|XXGobject_psameas_object|51|0|93|0|{metadata|out0_metadata|1|0|}42|41|}
{2010006001|XXGobject_psameas_object|52|0|94|0|{transform0|transform0|1|0|}42|41|}
{2010212001|XXGvertex_oport_oport|53|0|95|0|{1|reject0|}41|43|}
{2010006001|XXGobject_psameas_object|54|0|97|0|{metadata|reject0_metadata|1|0|}43|41|}
{2010212001|XXGvertex_oport_oport|55|0|98|0|{2|error0|}41|44|}
{2010006001|XXGobject_psameas_object|56|0|100|0|{metadata|error0_metadata|1|0|}44|41|}
{2010212001|XXGvertex_oport_oport|57|0|101|0|{3|log|}41|45|}
{2010006001|XXGobject_psameas_object|58|0|103|0|{metadata|log_metadata|1|0|}45|41|}
{2010211001|XXGvertex_iport_iport|59|0|104|0|{0|in|}41|46|}
{2010006001|XXGobject_psameas_object|60|0|106|0|{metadata|in_metadata|1|0|}46|41|}
{2010006001|XXGobject_psameas_object|61|0|107|0|{Layout|Layout|0|0|}35|31|}
{2010006001|XXGobject_psameas_object|62|0|108|0|{reject0_metadata|in_metadata|0|0|}35|35|}
{2010601001|XXGgraph_vertex_vertex|63|0|109|0|{Extract_Log_File|}1|47|}
{2010212001|XXGvertex_oport_oport|64|0|111|0|{0|read|}47|48|}
{2010213001|XXGoport_dst_flow|65|0|113|0|{0|}48|22|}
{2010007001|XXGobject_proto_object|66|0|114|0|{}47|49|}
{2010212001|XXGvertex_oport_oport|67|0|116|0|{0|read|}49|50|}
{2010006001|XXGobject_psameas_object|68|0|118|0|{metadata|read_metadata|1|0|}50|49|}
{2010601001|XXGgraph_vertex_vertex|69|0|119|0|{Filter_Record_Count_Rows|}1|51|}
{2010212001|XXGvertex_oport_oport|70|0|121|0|{0|out|}51|52|}
{2010213001|XXGoport_dst_flow|71|0|123|0|{0|}52|23|}
{2010212001|XXGvertex_oport_oport|72|0|124|0|{1|deselect|}51|53|}
{2010212001|XXGvertex_oport_oport|73|0|126|0|{2|reject|}51|54|}
{2010212001|XXGvertex_oport_oport|74|0|128|0|{3|error|}51|55|}
{2010212001|XXGvertex_oport_oport|75|0|130|0|{4|log|}51|56|}
{2010211001|XXGvertex_iport_iport|76|0|132|0|{0|in|}51|57|}
{2010214001|XXGiport_src_flow|77|0|134|0|{0|}57|22|}
{2010007001|XXGobject_proto_object|78|0|135|0|{}51|58|}
{2010212001|XXGvertex_oport_oport|79|0|137|0|{0|out|}58|59|}
{2010006001|XXGobject_psameas_object|80|0|139|0|{metadata|out_metadata|1|0|}59|58|}
{2010212001|XXGvertex_oport_oport|81|0|140|0|{1|deselect|}58|60|}
{2010006001|XXGobject_psameas_object|82|0|142|0|{metadata|deselect_metadata|1|0|}60|58|}
{2010212001|XXGvertex_oport_oport|83|0|143|0|{2|reject|}58|61|}
{2010006001|XXGobject_psameas_object|84|0|145|0|{metadata|reject_metadata|1|0|}61|58|}
{2010212001|XXGvertex_oport_oport|85|0|146|0|{3|error|}58|62|}
{2010006001|XXGobject_psameas_object|86|0|148|0|{metadata|error_metadata|1|0|}62|58|}
{2010212001|XXGvertex_oport_oport|87|0|149|0|{4|log|}58|63|}
{2010006001|XXGobject_psameas_object|88|0|151|0|{metadata|log_metadata|1|0|}63|58|}
{2010211001|XXGvertex_iport_iport|89|0|152|0|{0|in|}58|64|}
{2010006001|XXGobject_psameas_object|90|0|154|0|{metadata|in_metadata|1|0|}64|58|}
{2010006001|XXGobject_psameas_object|91|0|155|0|{Layout|Layout|0|0|}51|65|}
{2010211001|XXGvertex_iport_iport|92|0|157|0|{0|write|}65|66|}
{2010214001|XXGiport_src_flow|93|0|159|0|{0|}66|12|}
{2010007001|XXGobject_proto_object|94|0|160|0|{}65|67|}
{2010211001|XXGvertex_iport_iport|95|0|162|0|{0|write|}67|68|}
{2010006001|XXGobject_psameas_object|96|0|164|0|{metadata|write_metadata|1|0|}68|67|}
{2010006001|XXGobject_psameas_object|97|0|165|0|{write_metadata|out_metadata|0|0|}65|69|}
{2010212001|XXGvertex_oport_oport|98|0|167|0|{0|out|}69|70|}
{2010213001|XXGoport_dst_flow|99|0|169|0|{0|}70|12|}
{2010212001|XXGvertex_oport_oport|100|0|170|0|{1|reject|}69|71|}
{2010212001|XXGvertex_oport_oport|101|0|172|0|{2|error|}69|72|}
{2010212001|XXGvertex_oport_oport|102|0|174|0|{3|log|}69|73|}
{2010211001|XXGvertex_iport_iport|103|0|176|0|{0|in|}69|74|}
{2010214001|XXGiport_src_flow|104|0|178|0|{0|}74|24|}
{2010007001|XXGobject_proto_object|105|0|179|0|{}69|75|}
{2010212001|XXGvertex_oport_oport|106|0|181|0|{0|out|}75|76|}
{2010006001|XXGobject_psameas_object|107|0|183|0|{metadata|out_metadata|1|0|}76|75|}
{2010212001|XXGvertex_oport_oport|108|0|184|0|{1|reject|}75|77|}
{2010006001|XXGobject_psameas_object|109|0|186|0|{metadata|reject_metadata|1|0|}77|75|}
{2010212001|XXGvertex_oport_oport|110|0|187|0|{2|error|}75|78|}
{2010006001|XXGobject_psameas_object|111|0|189|0|{metadata|error_metadata|1|0|}78|75|}
{2010212001|XXGvertex_oport_oport|112|0|190|0|{3|log|}75|79|}
{2010006001|XXGobject_psameas_object|113|0|192|0|{metadata|log_metadata|1|0|}79|75|}
{2010211001|XXGvertex_iport_iport|114|0|193|0|{0|in|}75|80|}
{2010006001|XXGobject_psameas_object|115|0|195|0|{metadata|in_metadata|1|0|}80|75|}
{2010006001|XXGobject_psameas_object|116|0|196|0|{Layout|Layout|0|0|}69|65|}
{2010006001|XXGobject_psameas_object|117|0|197|0|{in_metadata|out_metadata|0|0|}69|81|}
{2010212001|XXGvertex_oport_oport|118|0|199|0|{0|out|}81|82|}
{2010213001|XXGoport_dst_flow|119|0|201|0|{0|}82|24|}
{2010211001|XXGvertex_iport_iport|120|0|202|0|{0|in|}81|83|}
{2010214001|XXGiport_src_flow|121|0|204|0|{0|}83|23|}
{2010007001|XXGobject_proto_object|122|0|205|0|{}81|84|}
{2010212001|XXGvertex_oport_oport|123|0|207|0|{0|out|}84|85|}
{2010006001|XXGobject_psameas_object|124|0|209|0|{metadata|out_metadata|1|0|}85|84|}
{2010211001|XXGvertex_iport_iport|125|0|210|0|{0|in|}84|86|}
{2010006001|XXGobject_psameas_object|126|0|212|0|{metadata|in_metadata|1|0|}86|84|}
{2010006001|XXGobject_psameas_object|127|0|213|0|{Layout|Layout|0|0|}81|65|}
{2010006001|XXGobject_psameas_object|128|0|214|0|{in_metadata|read_metadata|0|0|}81|47|}
{2010006001|XXGobject_psameas_object|129|0|215|0|{reject_metadata|out_metadata|0|0|}69|81|}
{2010006001|XXGobject_psameas_object|130|0|216|0|{deselect_metadata|read_metadata|0|0|}51|47|}
{2010006001|XXGobject_psameas_object|131|0|217|0|{in_metadata|read_metadata|0|0|}51|47|}
{2010006001|XXGobject_psameas_object|132|0|218|0|{out_metadata|read_metadata|0|0|}51|47|}
{2010006001|XXGobject_psameas_object|133|0|219|0|{reject_metadata|read_metadata|0|0|}51|47|}
{2010601001|XXGgraph_vertex_vertex|134|0|220|0|{Gather_Extract_Logs|}1|87|}
{2010211001|XXGvertex_iport_iport|135|0|222|0|{0|in|}87|88|}
{2010214001|XXGiport_src_flow|136|0|224|0|{0|}88|11|}
{2010007001|XXGobject_proto_object|137|0|225|0|{}87|89|}
{2010211001|XXGvertex_iport_iport|138|0|227|0|{0|in|}89|90|}
{2010006001|XXGobject_psameas_object|139|0|229|0|{metadata|in_metadata|1|0|}90|89|}
{2010006001|XXGobject_psameas_object|140|0|230|0|{in_metadata|log_metadata|0|0|}87|91|}
{2010212001|XXGvertex_oport_oport|141|0|232|0|{0|read|}91|92|}
{2010213001|XXGoport_dst_flow|142|0|234|0|{0|}92|17|}
{2010212001|XXGvertex_oport_oport|143|0|235|0|{1|log|}91|93|}
{2010213001|XXGoport_dst_flow|144|0|237|0|{0|}93|11|}
{2010007001|XXGobject_proto_object|145|0|238|0|{}91|94|}
{2010212001|XXGvertex_oport_oport|146|0|240|0|{0|read|}94|95|}
{2010006001|XXGobject_psameas_object|147|0|242|0|{metadata|read_metadata|1|0|}95|94|}
{2010212001|XXGvertex_oport_oport|148|0|243|0|{1|log|}94|96|}
{2010006001|XXGobject_psameas_object|149|0|245|0|{metadata|log_metadata|1|0|}96|94|}
{2010006001|XXGobject_psameas_object|150|0|246|0|{Layout|Layout|0|0|}91|31|}
{2010601001|XXGgraph_vertex_vertex|151|0|247|0|{Get_Max_Value_for_Extract_Field|}1|97|}
{2010212001|XXGvertex_oport_oport|152|0|249|0|{0|out|}97|98|}
{2010213001|XXGoport_dst_flow|153|0|251|0|{0|}98|21|}
{2010212001|XXGvertex_oport_oport|154|0|252|0|{1|reject|}97|99|}
{2010212001|XXGvertex_oport_oport|155|0|254|0|{2|error|}97|100|}
{2010212001|XXGvertex_oport_oport|156|0|256|0|{3|log|}97|101|}
{2010211001|XXGvertex_iport_iport|157|0|258|0|{0|in|}97|102|}
{2010214001|XXGiport_src_flow|158|0|260|0|{0|}102|20|}
{2010214001|XXGiport_src_flow|159|0|261|0|{1|}102|18|}
{2010007001|XXGobject_proto_object|160|0|262|0|{}97|103|}
{2010212001|XXGvertex_oport_oport|161|0|264|0|{0|out|}103|104|}
{2010006001|XXGobject_psameas_object|162|0|266|0|{metadata|out_metadata|1|0|}104|103|}
{2010212001|XXGvertex_oport_oport|163|0|267|0|{1|reject|}103|105|}
{2010006001|XXGobject_psameas_object|164|0|269|0|{metadata|reject_metadata|1|0|}105|103|}
{2010212001|XXGvertex_oport_oport|165|0|270|0|{2|error|}103|106|}
{2010006001|XXGobject_psameas_object|166|0|272|0|{metadata|error_metadata|1|0|}106|103|}
{2010212001|XXGvertex_oport_oport|167|0|273|0|{3|log|}103|107|}
{2010006001|XXGobject_psameas_object|168|0|275|0|{metadata|log_metadata|1|0|}107|103|}
{2010211001|XXGvertex_iport_iport|169|0|276|0|{0|in|}103|108|}
{2010006001|XXGobject_psameas_object|170|0|278|0|{metadata|in_metadata|1|0|}108|103|}
{2010006001|XXGobject_psameas_object|171|0|279|0|{Layout|Layout|0|0|}97|109|}
{2010211001|XXGvertex_iport_iport|172|0|281|0|{0|write|}109|110|}
{2010214001|XXGiport_src_flow|173|0|283|0|{0|}110|21|}
{2010007001|XXGobject_proto_object|174|0|284|0|{}109|111|}
{2010211001|XXGvertex_iport_iport|175|0|286|0|{0|write|}111|112|}
{2010006001|XXGobject_psameas_object|176|0|288|0|{metadata|write_metadata|1|0|}112|111|}
{2010006001|XXGobject_psameas_object|177|0|289|0|{in_metadata|read_metadata|0|0|}97|91|}
{2010006001|XXGobject_psameas_object|178|0|290|0|{out_metadata|write_metadata|0|0|}97|109|}
{2010006001|XXGobject_psameas_object|179|0|291|0|{reject_metadata|read_metadata|0|0|}97|91|}
{2010601001|XXGgraph_vertex_vertex|180|0|292|0|{Input_Table|}1|91|}
{2010601001|XXGgraph_vertex_vertex|181|0|293|0|{Last_Extract_Value_File|}1|113|}
{2010211001|XXGvertex_iport_iport|182|0|295|0|{0|write|}113|114|}
{2010214001|XXGiport_src_flow|183|0|297|0|{0|}114|14|}
{2010007001|XXGobject_proto_object|184|0|298|0|{}113|115|}
{2010211001|XXGvertex_iport_iport|185|0|300|0|{0|write|}115|116|}
{2010006001|XXGobject_psameas_object|186|0|302|0|{metadata|write_metadata|1|0|}116|115|}
{2010601001|XXGgraph_vertex_vertex|187|0|303|0|{Last_Extract_Value_File_1|}1|109|}
{2010601001|XXGgraph_vertex_vertex|188|0|304|0|{Output_File|}1|31|}
{2010601001|XXGgraph_vertex_vertex|189|0|305|0|{Record_Count_File|}1|65|}
{2010601001|XXGgraph_vertex_vertex|190|0|306|0|{Redefine_Format|}1|81|}
{2010601001|XXGgraph_vertex_vertex|191|0|307|0|{Reformat|}1|35|}
{2010601001|XXGgraph_vertex_vertex|192|0|308|0|{Replicate|}1|117|}
{2010212001|XXGvertex_oport_oport|193|0|310|0|{0|out|}117|118|}
{2010213001|XXGoport_dst_flow|194|0|312|0|{0|}118|13|}
{2010213001|XXGoport_dst_flow|195|0|313|0|{1|}118|20|}
{2010211001|XXGvertex_iport_iport|196|0|314|0|{0|in|}117|119|}
{2010214001|XXGiport_src_flow|197|0|316|0|{0|}119|17|}
{2010007001|XXGobject_proto_object|198|0|317|0|{}117|120|}
{2010212001|XXGvertex_oport_oport|199|0|319|0|{0|out|}120|121|}
{2010006001|XXGobject_psameas_object|200|0|321|0|{metadata|out_metadata|1|0|}121|120|}
{2010211001|XXGvertex_iport_iport|201|0|322|0|{0|in|}120|122|}
{2010006001|XXGobject_psameas_object|202|0|324|0|{metadata|in_metadata|1|0|}122|120|}
{2010006001|XXGobject_psameas_object|203|0|325|0|{Layout|Layout|0|0|}117|31|}
{2010006001|XXGobject_psameas_object|204|0|326|0|{in_metadata|read_metadata|0|0|}117|91|}
{2010006001|XXGobject_psameas_object|205|0|327|0|{out_metadata|read_metadata|0|0|}117|91|}
{2010601001|XXGgraph_vertex_vertex|206|0|328|0|{Rollup_Record_Counts|}1|69|}
{2010601001|XXGgraph_vertex_vertex|207|0|329|0|{Send_From_Extract_Value_to_Rollup|}1|123|}
{2010212001|XXGvertex_oport_oport|208|0|331|0|{0|out|}123|124|}
{2010213001|XXGoport_dst_flow|209|0|333|0|{0|}124|18|}
{2010007001|XXGobject_proto_object|210|0|334|0|{}123|125|}
{2010212001|XXGvertex_oport_oport|211|0|336|0|{0|out|}125|126|}
{2010006001|XXGobject_psameas_object|212|0|338|0|{metadata|out_metadata|1|0|}126|125|}
{2010006001|XXGobject_psameas_object|213|0|339|0|{Layout|Layout|0|0|}123|109|}
{2010006001|XXGobject_psameas_object|214|0|340|0|{out_metadata|read_metadata|0|0|}123|91|}
{2010601001|XXGgraph_vertex_vertex|215|0|341|0|{Send_Primer_Row_to_Reformat|}1|127|}
{2010212001|XXGvertex_oport_oport|216|0|343|0|{0|out|}127|128|}
{2010213001|XXGoport_dst_flow|217|0|345|0|{0|}128|19|}
{2010007001|XXGobject_proto_object|218|0|346|0|{}127|129|}
{2010212001|XXGvertex_oport_oport|219|0|348|0|{0|out|}129|130|}
{2010006001|XXGobject_psameas_object|220|0|350|0|{metadata|out_metadata|1|0|}130|129|}
{2010006001|XXGobject_psameas_object|221|0|351|0|{Layout|Layout|0|0|}127|131|}
{2010212001|XXGvertex_oport_oport|222|0|353|0|{0|out0|}131|132|}
{2010213001|XXGoport_dst_flow|223|0|355|0|{0|}132|14|}
{2010212001|XXGvertex_oport_oport|224|0|356|0|{1|reject0|}131|133|}
{2010212001|XXGvertex_oport_oport|225|0|358|0|{2|error0|}131|134|}
{2010212001|XXGvertex_oport_oport|226|0|360|0|{3|log|}131|135|}
{2010211001|XXGvertex_iport_iport|227|0|362|0|{0|in|}131|136|}
{2010214001|XXGiport_src_flow|228|0|364|0|{0|}136|19|}
{2010007001|XXGobject_proto_object|229|0|365|0|{}131|137|}
{2010212001|XXGvertex_oport_oport|230|0|367|0|{0|out0|}137|138|}
{2010006001|XXGobject_psameas_object|231|0|369|0|{metadata|out0_metadata|1|0|}138|137|}
{2010006001|XXGobject_psameas_object|232|0|370|0|{transform0|transform0|1|0|}138|137|}
{2010212001|XXGvertex_oport_oport|233|0|371|0|{1|reject0|}137|139|}
{2010006001|XXGobject_psameas_object|234|0|373|0|{metadata|reject0_metadata|1|0|}139|137|}
{2010212001|XXGvertex_oport_oport|235|0|374|0|{2|error0|}137|140|}
{2010006001|XXGobject_psameas_object|236|0|376|0|{metadata|error0_metadata|1|0|}140|137|}
{2010212001|XXGvertex_oport_oport|237|0|377|0|{3|log|}137|141|}
{2010006001|XXGobject_psameas_object|238|0|379|0|{metadata|log_metadata|1|0|}141|137|}
{2010211001|XXGvertex_iport_iport|239|0|380|0|{0|in|}137|142|}
{2010006001|XXGobject_psameas_object|240|0|382|0|{metadata|in_metadata|1|0|}142|137|}
{2010006001|XXGobject_psameas_object|241|0|383|0|{in_metadata|out_metadata|0|0|}131|127|}
{2010006001|XXGobject_psameas_object|242|0|384|0|{out0_metadata|write_metadata|0|0|}131|113|}
{2010006001|XXGobject_psameas_object|243|0|385|0|{reject0_metadata|out_metadata|0|0|}131|127|}
{2010601001|XXGgraph_vertex_vertex|244|0|386|0|{Update_Last_Extract_Value_File|}1|131|}
{2010601001|XXGgraph_vertex_vertex|245|0|387|0|{chmod_FEXP_LOGFILE|}1|143|}
{2010212001|XXGvertex_oport_oport|246|0|389|0|{0|out|}143|144|}
{2010211001|XXGvertex_iport_iport|247|0|391|0|{0|in|}143|145|}
{2010007001|XXGobject_proto_object|248|0|393|0|{}143|146|}
{2010212001|XXGvertex_oport_oport|249|0|395|0|{0|out|}146|147|}
{2010006001|XXGobject_psameas_object|250|0|397|0|{metadata|out_metadata|1|0|}147|146|}
{2010211001|XXGvertex_iport_iport|251|0|398|0|{0|in|}146|148|}
{2010006001|XXGobject_psameas_object|252|0|400|0|{metadata|in_metadata|1|0|}148|146|}
{2010109001|XXGobject_property_value|253|0|401|0|{0|TrackingThumbprint|72|}1|149|}
