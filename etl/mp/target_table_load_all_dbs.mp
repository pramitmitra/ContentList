@
{18|Type|XXGrepository|Primary_Name|XXGgraph|_ab_object_schema|0|_ab_semantic_schema|1|eme_flatten_format|1|schema_version|34|root_id|0|ent_count|41|rel_count|65|}
{2010001002|XXGdirectory|0|1|1|0|{@{}@}}
{2010600005|XXGgraph|1|0|1|0|{|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|ETL_ID||3|2|REFO$|subject_area.table_id|{0|}}
{30001002|XXparameter|JOB_ENV||3|2|REFO$|primary or secondary|{0|}}
{30001002|XXparameter|SQL_FILENAME||3|2|REFO$|sql file|{0|}}
{30001002|XXparameter|ETL_CFG_FILE|$DW_CFG/$ETL_ID.cfg|3|2|RE$||{0|}}
{30001002|XXparameter|SUBJECT_AREA|$\{ETL_ID%%.*\}|3|6|REk||{0|}}
{30001002|XXparameter|TABLE_ID|$\{ETL_ID##*.\}|3|6|REk||{0|}}
{30001002|XXparameter|CFG_DBC_PARAM|$(JOB_ENV_UPPER=$(print $JOB_ENV \| tr [:lower:] [:upper:]) 
  eval print $\{JOB_ENV_UPPER\}_DBC)|3|6|REk||{0|}}
{30001002|XXparameter|DEFAULT_DB_NAME|$(JOB_ENV_UPPER=$(print $JOB_ENV \| tr [:lower:] [:upper:])
 eval print teradata_\\$DW_$\{JOB_ENV_UPPER\}_DB)|3|6|REk||{0|}}
{30001002|XXparameter|AB_JOB|$(if [ $ETL_ENV ]
then
   print $AB_JOB.$TABLE_ID.$\{SQL_FILENAME%.sql\}.$ETL_ENV.$JOB_ENV
else
   print $AB_JOB.$TABLE_ID.$\{SQL_FILENAME%.sql\}.$JOB_ENV
fi)|3|6|REk||{0|}}
{30001002|XXparameter|DB_NAME|$(grep "^$CFG_DBC_PARAM\\>" $ETL_CFG_FILE \| read PARAM VALUE COMMENT; print $\{VALUE:-$DEFAULT_DB_NAME\})|3|6|REk||{0|}}
{30001002|XXparameter|AB_IDB_CONFIG|$\{DB_NAME\}.dbc|3|6|REk|database configuration file|{0|}}
{30001002|XXparameter|DB_TYPE|$(grep "^dbms\\>" $DW_DBC/$AB_IDB_CONFIG \| tr [:lower:] [:upper:] \| read PARAM VALUE COMMENT; print $\{VALUE:-0\})|3|6|REk||{0|}}
{30001002|XXparameter|TNS_NAME|$(grep "^db_name\\>" $DW_DBC/$AB_IDB_CONFIG \| read A TNS_NAME_TMP
  print $\{TNS_NAME_TMP#\@\})|3|6|REk||{0|}}
{30001002|XXparameter|ORA_USERNAME|$(grep "^$TNS_NAME\\>" $DW_LOGINS/ora_logins.dat \| read TNS_NAME ORA_USERNAME ORA_PASSWORD ; print $ORA_USERNAME)|3|6|REk||{0|}}
{30001002|XXparameter|ORA_PASSWORD|$(grep "^$TNS_NAME\\>" $DW_LOGINS/ora_logins.dat \| read TNS_NAME ORA_USERNAME ORA_PASSWORD ; print $ORA_PASSWORD)|3|6|REk||{0|}}
{30001002|XXparameter|DW_SA_LOG|$DW_LOG/$JOB_ENV/$SUBJECT_AREA|3|2|RE$||{0|}}
{30001002|XXparameter|DW_SA_TMP|$DW_TMP/$JOB_ENV/$SUBJECT_AREA|3|2|RE$||{0|}}
{30001002|XXparameter|FILE_DATETIME|$(date '+%Y%m%d-%H%M%S')|3|6|REk||{0|}}
{30001002|XXparameter|CREATE_TMP_SQL_FILE|$(set -e
  print "cat <<EOF" > $DW_SA_TMP/$TABLE_ID.bt.$SQL_FILENAME.tmp
  if [[ $DB_TYPE == 'TERADATA' ]] 
  then 
     print "select session;" >> $DW_SA_TMP/$TABLE_ID.bt.$SQL_FILENAME.tmp 
  fi
  cat $DW_SQL/$SQL_FILENAME >> $DW_SA_TMP/$TABLE_ID.bt.$SQL_FILENAME.tmp
  print "EOF" >> $DW_SA_TMP/$TABLE_ID.bt.$SQL_FILENAME.tmp
  set +e)|3|6|REk||{0|}}
{30001002|XXparameter|RUN_SQL_FILE|$(. $DW_SA_TMP/$TABLE_ID.bt.$SQL_FILENAME.tmp)|3|6|REk|File with SQL to run|{0|}}
{30001002|XXparameter|RUN_SQL_LOGFILE|$DW_SA_LOG/$TABLE_ID.bt.$\{SQL_FILENAME%.sql\}.$FILE_DATETIME.log|3|6|REk||{0|}}
{30001002|XXparameter|_parameters_of_Run_SQL_Log_File|@34|9|FHK|@{0|}}
{30001002|XXparameter|_parameters_of_Reformat|@34|9|FHK|@{0|}}
{30001002|XXparameter|_parameters_of_Run_SQL_Log_File_Tmp|@34|9|FHK|@{0|}}
{30001002|XXparameter|_parameters_of_Rename_Run_SQL_Log_File_Tmp|@34|9|FHK|@{0|}}
{30001002|XXparameter|_parameters_of_Run_SQL|@34|9|FHK|@{0|}}
{30001002|XXparameter|!ab_compatibility|3.1.4.4|3|9|P|@{0|}}
}}@0|@0|0|0|0|517000|390000|0|target_table_load|Craig Werre||1|10|-1||6||2049|{0|}0|0|{0|}{0|}{0|}{0|}1.0|517000|390000|7|}}
{2010703001|XXGgraphinfo|2|0|3|0|{@{}@1|3.0.3.1|{3|1|1|1|}Job 'target_table_load' Execution Status Report

Compilation of graph "target_table_load" started at Wednesday, August 13, 2014 13:37:04
No compilation errors
||||@|31633919|}}
{2010705002|XXGrunsettings|3|0|5|0|{@{}@lvsdpelega001||||1|300|0|1|1|0|0|0|0|1|1|0|0|1|1|1|1|0|0|0|@16|}}
{2010242002|XXGface|4|0|7|0|{@{}@Arial|0|100|0|}}
{2010210004|XXGflow|5|0|9|0|{@{}@384|.5|.5|{10|78000|118000|78000|138000|78000|183000|84000|183000|115000|183000|}7|17|}}
{2010210004|XXGflow|6|0|11|0|{@{}@384|.5|.5|{8|176000|183000|208000|183000|210000|183000|230000|183000|}2|17|}}
{2010210004|XXGflow|7|0|13|0|{@{}@384|.5|.5|{8|384000|183000|384000|183000|430000|183000|437000|183000|}4|17|}}
{2010501005|XXGpvertex|8|0|15|0|{|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|transform0|out::reformat(in) =
begin
  out.event_text :: in.event_text;
end;|3|1|l|@{0|}}
{30001002|XXparameter|out0_metadata|record
  string("\\n") event_text;
end;|3|1|l|@{0|}}
{30001002|XXparameter|error0_metadata|string('\\n')|3|1|l|@{0|}}
{30001002|XXparameter|log_metadata|record string("\|") node, timestamp, component, subcomponent, event_type; string("\|\\n") event_text; end|3|1|l|@{0|}}
{30001002|XXparameter|!prototype_path|C:\\Program Files\\Ab Initio\\Ab Initio GDE\\Components\\Transform\\Reformat.mpc|3|2|Pw$|@{0|}}
{30001002|XXparameter|Layout||3|8|s=|@{0|}}
{30001002|XXparameter|in_metadata||3|8|s=|@{0|}}
{30001002|XXparameter|reject0_metadata||3|8|s=|@{0|}}
}}@1|@221000|130500|241000|151000|133000|65000|1|Reformat|Ab Initio Software|Built-in|1|10|-1||6||36865|1|{1|0|}}}
{2010203004|XXGoport|9|0|17|0|{@{}@373000|178000|11000|11000|out0|0.0|count|out|0|2448|0|}}
{2010203004|XXGoport|10|0|20|0|{@{}@0|0|0|0|reject0|0.0|count|reject|0|1168|0|}}
{2010203004|XXGoport|11|0|22|0|{@{}@0|0|0|0|error0|0.0|count|error|0|1176|0|}}
{2010203004|XXGoport|12|0|24|0|{@{}@0|0|0|0|log|0.0|@@@1176|0|}}
{2010202004|XXGiport|13|0|26|0|{@{}@231000|178000|11000|11000|in|0.0|@@@1808|0|}}
{2010501005|XXGpvertex|14|0|29|0|{Changes the record format of your data by dropping fields or by using DML expressions to add fields, combine fields, or modify the data.|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|mpname|reformat-transform|3|1|Hl|The name used on the mp command line for this component|{0|}}
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
{30001002|XXparameter|in_metadata||7|1|RFsl||{0|}}
{30001002|XXparameter|out0_metadata||7|1|RFl||{0|}}
{30001002|XXparameter|reject0_metadata||7|1|Fsl||{0|}}
{30001002|XXparameter|error0_metadata||7|1|Fl||{0|}}
{30001002|XXparameter|log_metadata||7|1|Fl||{0|}}
{30001002|XXparameter|doc_transform||8|2|FHs$|Document your transformation for dependency analysis|{0|}}
{30001002|XXparameter|doc_operation1|out[*],reject[*],error[*],log::document(in)|3|1|RHl|The custom transformation|{0|}}
{30001002|XXparameter|mpcmodtime|1099733698|1|1|Hl|The last modification time of this component's template|{0|}}
{30001002|XXparameter|output_index_signature|int out::output_index(in)|3|2|H$|Function returning index of output port|{0|}}
{30001002|XXparameter|output_index_signature.display_name|output-index_signature|3|9|P|@{0|}}
{30001002|XXparameter|output_indexes_signature|int[int] out::output_indexes(in)|3|2|H$|Function returning vector of indexes of output ports|{0|}}
{30001002|XXparameter|output_indexes_signature.display_name|output-indexes_signature|3|9|P|@{0|}}
{30001002|XXparameter|num_operations|1|1|2|H$|Number of operations per reformat|{0|}}
{30001002|XXparameter|operation1|out::reformat(in)|3|2|H$|Reformat operation|{0|}}
{30001002|XXparameter|port_analysis|reject*=in|3|2|H$||{0|}}
{30001002|XXparameter|continuous_analysis||3|2|H$||{0|}}
{30001002|XXparameter|main_mp_port|count out|3|1|l||{0|}}
{30001002|XXparameter|_propagation|metadata type: error = "string('\\\\n')"
metadata type: reject = in
metadata type: log = "record string(\\"\|\\") node, timestamp, component, subcomponent, event_type; string(\\"\|\\\\n\\") event_text; end"|3|1|HKl|@{0|}}
{30001002|XXparameter|_propagate_through||3|1|FHKl|@{0|}}
}}@0|@0|0|0|0|0|0|0|@@@1|10|-1|@6|@1|1|{1|0|}}}
{2010203004|XXGoport|15|0|31|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
{30001002|XXparameter|transform0||8|8|FO=|Reformat transform|{0|}}
{30001002|XXparameter|transform0.index|count|3|9|P|@{0|}}
{30001002|XXparameter|transform0.index_value|0|3|9|P|@{0|}}
{30001002|XXparameter|transform0.index_name|transform|3|9|P|@{0|}}
}}@0|0|0|0|out0|0.0|count|out|0|2448|0|}}
{2010203004|XXGoport|16|0|35|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|F=||{0|}}
}}@0|0|0|0|reject0|0.0|count|reject|0|1168|0|}}
{2010203004|XXGoport|17|0|38|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|F=||{0|}}
}}@0|0|0|0|error0|0.0|count|error|0|1176|0|}}
{2010203004|XXGoport|18|0|41|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|F=||{0|}}
}}@0|0|0|0|log|0.0|@@@1176|0|}}
{2010202004|XXGiport|19|0|44|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|in|0.0|@@@1808|0|}}
{2010503005|XXGfvertex|20|0|47|0|{|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|mode|0x0462|3|2|$|@{0|}}
{30001002|XXparameter|Layout|$[[record kind 85 subkind 0 parts [vector _interp_("file:$\{RUN_SQL_LOGFILE\}.tmp", "dollar_substitution")]]]|3|9||@{0|}}
{30001002|XXparameter|!prototype_path|C:\\Program Files\\Ab Initio\\Ab Initio GDE\\Components\\Datasets\\Output_File.mdc|3|2|Pw$|@{0|}}
{30001002|XXparameter|write_metadata||3|8|s=|@{0|}}
}}@1|@423000|143500|448000|164000|40000|40000|5|Run SQL Log File Tmp|Craig Werre||1|10|-1||6||32769|-1|-1|}}
{2010202004|XXGiport|21|0|49|0|{@{}@438000|178000|11000|11000|write|0.0|@@@1776|0|}}
{2010503005|XXGfvertex|22|0|52|0|{Represents one file, many files, or a multifile as an output from your graph.  |{30100001|XXparameter_set|@@@@{{30001002|XXparameter|protection|0666|12|2|RF$||{0|}}
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
{30001002|XXparameter|mpcmodtime|1002319848|1|1|Hl|The last modification time of this component's template|{0|}}
{30001002|XXparameter|eme_dataset_location||3|2|F$|Place in the EME to create a dataset corresponding to this file.|{0|}}
}}@0|@0|0|0|0|0|0|0|@@@1|10|-1|@6|@1|-1|-1|}}
{2010202004|XXGiport|23|0|54|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|write|0.0|@@@1776|0|}}
{2010501005|XXGpvertex|24|0|58|0|{|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|DBConfigFile|$AB_IDB_CONFIG|3|2|f$|@{0|}}
{30001002|XXparameter|dbms|$DB_TYPE|3|2|$|@{0|}}
{30001002|XXparameter|interface|utility|3|1|l|@{0|}}
{30001002|XXparameter|ansimode|False|3|1|l|@{0|}}
{30001002|XXparameter|SQLFile|$RUN_SQL_FILE|3|6|k|@{0|}}
{30001002|XXparameter|log_metadata|record string("\|") node, timestamp, component, subcomponent, event_type; string("\|\\n") event_text; end|3|1|l|@{0|}}
{30001002|XXparameter|!prototype_path|C:\\Program Files\\Ab Initio\\Ab Initio GDE\\Components\\Database\\Run SQL.mpc|3|2|Pw$|@{0|}}
{30001002|XXparameter|Layout||3|8|s=|@{0|}}
}}@0|@14000|15500|34000|36000|90000|72000|8|Run SQL|Ab Initio Software|Built-in 2.2:|1|10|0||6||32769|1|{1|0|}}}
{2010203004|XXGoport|25|0|60|0|{@{}@73000|107000|11000|11000|log|0.0|@@@1176|0|}}
{2010501005|XXGpvertex|26|0|63|0|{Executes SQL statements in a database.|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|mpname|db-runsql|3|1|Hl|The name used on the mp command line for this component|{0|}}
{30001002|XXparameter|image__|~ab_home/db/bin/db-runsql|3|2|H$|The image used if this component was a custom component|{0|}}
{30001002|XXparameter|DBConfigFile||22|2|RFOf$|Database Description File|{0|}}
{30001002|XXparameter|dbms||3|2|F$||{0|}}
{30001002|XXparameter|interface|api|15|1|FKl|Run commands using utility or via api|{2|api|utility|}}
{30001002|XXparameter|ansimode|True|13|1|FKl|Teradata: Use ANSI semantics if True; BTET semantics if False|{0|}}
{30001002|XXparameter|ansimode.condition|param_exact dbms teradata|3|15|P?|@{0|}}
{30001002|XXparameter|ignore_drop_table_errors|False|13|1|FKl|Ignore failures from drop table statements|{0|}}
{30001002|XXparameter|ignore_drop_table_errors.condition|param_exact interface api|3|15|P?|@{0|}}
{30001002|XXparameter|num_errors|0|1|2|FK$|Maximum errors allowed|{0|}}
{30001002|XXparameter|num_errors.condition|param_exact interface api|3|15|P?|@{0|}}
{30001002|XXparameter|SQLFile||32|2|RFO$|File with SQL to run|{0|}}
{30001002|XXparameter|keyword_map|num_errors maximum_errors|3|2|H$||{0|}}
{30001002|XXparameter|keyword_map.display_name|keyword-map|3|9|P|@{0|}}
{30001002|XXparameter|LayoutFlags|idb|3|2|H$|Enable Database Layouts|{0|}}
{30001002|XXparameter|condition||3|2|F$||{0|}}
{30001002|XXparameter|conditionInputPort||3|2|F$||{0|}}
{30001002|XXparameter|conditionOutputPort||3|2|F$||{0|}}
{30001002|XXparameter|condition_interpretation|Remove completely|15|1|Fl||{2|Replace with flow|Remove completely|}}
{30001002|XXparameter|condition_interpretation.display_name|condition-interpretation|3|9|P|@{0|}}
{30001002|XXparameter|log_metadata||7|1|RFl||{0|}}
{30001002|XXparameter|doc_transform||8|2|FHs$|Document your transformation for dependency analysis|{0|}}
{30001002|XXparameter|doc_operation1|log::document()|3|1|RHl|The custom transformation|{0|}}
{30001002|XXparameter|mpcmodtime|1049135758|1|1|Hl|The last modification time of this component's template|{0|}}
{30001002|XXparameter|Layout|$[[record kind 78 subkind 68 parts [vector]]]|9|9|RFs||{0|}}
{30001002|XXparameter|_propagation|metadata type: log = "record string(\\"\|\\") node, timestamp, component, subcomponent, event_type; string(\\"\|\\\\n\\") event_text; end"|3|1|HKl|@{0|}}
}}@0|@0|0|0|0|0|0|0|@@@1|10|-1|@6|@1|1|{1|0|}}}
{2010203004|XXGoport|27|0|65|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|log|0.0|@@@1176|0|}}
{2010503005|XXGfvertex|28|0|68|0|{|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|mode|0x0463|3|2|$|@{0|}}
{30001002|XXparameter|Layout|$[[record kind 85 subkind 0 parts [vector _interp_("file:$RUN_SQL_LOGFILE", "dollar_substitution")]]]|3|9||@{0|}}
{30001002|XXparameter|!prototype_path|C:\\Program Files\\Ab Initio\\Ab Initio GDE\\Components\\Datasets\\Intermediate_File.mdc|3|2|Pw$|@{0|}}
{30001002|XXparameter|write_metadata||3|8|s=|@{0|}}
{30001002|XXparameter|read_metadata||3|8|s=|@{0|}}
}}@0|@101000|143500|126000|164000|40000|40000|3|Run SQL Log File|Craig Werre||1|-1|-1||6||32769|-1|-1|}}
{2010203004|XXGoport|29|0|70|0|{@{}@165000|178000|11000|11000|read|0.0|@@@2160|0|}}
{2010202004|XXGiport|30|0|73|0|{@{}@116000|178000|11000|11000|write|0.0|@@@1776|0|}}
{2010503005|XXGfvertex|31|0|76|0|{Represents one file, many files, or a multifile as an output from your graph.  |{30100001|XXparameter_set|@@@@{{30001002|XXparameter|protection|0666|12|2|RF$||{0|}}
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
{30001002|XXparameter|read_metadata||7|1|RFsl||{0|}}
{30001002|XXparameter|_propagate_through|metadata type: read = write
metadata type: write = read|3|1|FHKl|@{0|}}
{30001002|XXparameter|eme_dataset_location||3|2|F$|Place in the EME to create a dataset corresponding to this file.|{0|}}
}}@0|@0|0|0|0|0|0|0|@@@1|10|-1|@6|@1|-1|-1|}}
{2010203004|XXGoport|32|0|78|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|read|0.0|@@@2160|0|}}
{2010202004|XXGiport|33|0|81|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|write|0.0|@@@1776|0|}}
{2010501005|XXGpvertex|34|0|87|0|{|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|commandline|mv $\{RUN_SQL_LOGFILE\}.tmp $\{RUN_SQL_LOGFILE\}|3|2|$|@{0|}}
{30001002|XXparameter|Layout|$[[record kind 85 subkind 0 parts [vector _interp_("$DW_TMP", "dollar_substitution")]]]|3|9||@{0|}}
{30001002|XXparameter|!prototype_path|C:\\Program Files\\Ab Initio\\Ab Initio GDE\\Components\\Miscellaneous\\Run Program.mpc|3|2|Pw$|@{0|}}
}}@2|@21000|269500|41000|290000|145000|106000|6|Rename Run SQL Log File Tmp|Ab Initio Software|Built-in|1|10|-1||6||36865|1|{1|0|}}}
{2010203004|XXGoport|35|0|89|0|{@{}@185000|337000|11000|11000|out|0.0|@@@2260|0|}}
{2010202004|XXGiport|36|0|91|0|{@{}@31000|337000|11000|11000|in|0.0|@@@1732|0|}}
{2010501005|XXGpvertex|37|0|93|0|{Executes a standard UNIX or Windows NT program.|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|mpname|filter|3|1|Hl|The name used on the mp command line for this component|{0|}}
{30001002|XXparameter|image__|unitool|3|2|H$|The image used if this component was a custom component|{0|}}
{30001002|XXparameter|commandline||16|2|RFO$|Unix or NT command line|{0|}}
{30001002|XXparameter|NoHidePorts|in out|3|2|H$||{0|}}
{30001002|XXparameter|condition||3|2|F$||{0|}}
{30001002|XXparameter|conditionInputPort|in|3|2|F$||{0|}}
{30001002|XXparameter|conditionOutputPort|out|3|2|F$||{0|}}
{30001002|XXparameter|condition_interpretation|Replace with flow|15|1|Fl||{2|Replace with flow|Remove completely|}}
{30001002|XXparameter|condition_interpretation.display_name|condition-interpretation|3|9|P|@{0|}}
{30001002|XXparameter|Layout|@9|2|RF$||{0|}}
{30001002|XXparameter|in_metadata||7|1|Fsl||{0|}}
{30001002|XXparameter|out_metadata||7|1|Fsl||{0|}}
{30001002|XXparameter|doc_transform||8|2|FHs$|Document your transformation for dependency analysis|{0|}}
{30001002|XXparameter|doc_operation1|out::document(in)|3|1|RHl|The custom transformation|{0|}}
{30001002|XXparameter|mpcmodtime|1003947270|1|1|Hl|The last modification time of this component's template|{0|}}
{30001002|XXparameter|port_analysis|out=in|3|2|H$||{0|}}
{30001002|XXparameter|_propagate_through||3|1|FHKl|@{0|}}
}}@0|@0|0|0|0|0|0|0|@@@1|10|-1|@6|@1|1|{1|0|}}}
{2010203004|XXGoport|38|0|95|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|F=||{0|}}
}}@0|0|0|0|out|0.0|@@@2260|0|}}
{2010202004|XXGiport|39|0|98|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|F=||{0|}}
}}@0|0|0|0|in|0.0|@@@1732|0|}}
{2010110002|XXGconstant|40|0|104|0|{@{}@{1|6225309|}}}
{2010002001|XXGdirectory_object_object|0|1|2|0|{XXGgraph|}0|1|}
{2010704001|XXGgraph_graphinfo_graphinfo|1|0|2|0|{}1|2|}
{2010706001|XXGgraphinfo_runsettings_runsettings|2|0|4|0|{}2|3|}
{2010109001|XXGobject_property_value|3|0|6|0|{0|legend_face|92|}2|4|}
{2010604001|XXGgraph_flow_flow|4|0|8|0|{Flow_1|}1|5|}
{2010604001|XXGgraph_flow_flow|5|0|10|0|{Flow_2|}1|6|}
{2010604001|XXGgraph_flow_flow|6|0|12|0|{Flow_3|}1|7|}
{2010601001|XXGgraph_vertex_vertex|7|0|14|0|{Reformat|}1|8|}
{2010212001|XXGvertex_oport_oport|8|0|16|0|{0|out0|}8|9|}
{2010213001|XXGoport_dst_flow|9|0|18|0|{0|}9|7|}
{2010212001|XXGvertex_oport_oport|10|0|19|0|{1|reject0|}8|10|}
{2010212001|XXGvertex_oport_oport|11|0|21|0|{2|error0|}8|11|}
{2010212001|XXGvertex_oport_oport|12|0|23|0|{3|log|}8|12|}
{2010211001|XXGvertex_iport_iport|13|0|25|0|{0|in|}8|13|}
{2010214001|XXGiport_src_flow|14|0|27|0|{0|}13|6|}
{2010007001|XXGobject_proto_object|15|0|28|0|{}8|14|}
{2010212001|XXGvertex_oport_oport|16|0|30|0|{0|out0|}14|15|}
{2010006001|XXGobject_psameas_object|17|0|32|0|{metadata|out0_metadata|1|0|}15|14|}
{2010006001|XXGobject_psameas_object|18|0|33|0|{transform0|transform0|1|0|}15|14|}
{2010212001|XXGvertex_oport_oport|19|0|34|0|{1|reject0|}14|16|}
{2010006001|XXGobject_psameas_object|20|0|36|0|{metadata|reject0_metadata|1|0|}16|14|}
{2010212001|XXGvertex_oport_oport|21|0|37|0|{2|error0|}14|17|}
{2010006001|XXGobject_psameas_object|22|0|39|0|{metadata|error0_metadata|1|0|}17|14|}
{2010212001|XXGvertex_oport_oport|23|0|40|0|{3|log|}14|18|}
{2010006001|XXGobject_psameas_object|24|0|42|0|{metadata|log_metadata|1|0|}18|14|}
{2010211001|XXGvertex_iport_iport|25|0|43|0|{0|in|}14|19|}
{2010006001|XXGobject_psameas_object|26|0|45|0|{metadata|in_metadata|1|0|}19|14|}
{2010006001|XXGobject_psameas_object|27|0|46|0|{Layout|Layout|0|0|}8|20|}
{2010211001|XXGvertex_iport_iport|28|0|48|0|{0|write|}20|21|}
{2010214001|XXGiport_src_flow|29|0|50|0|{0|}21|7|}
{2010007001|XXGobject_proto_object|30|0|51|0|{}20|22|}
{2010211001|XXGvertex_iport_iport|31|0|53|0|{0|write|}22|23|}
{2010006001|XXGobject_psameas_object|32|0|55|0|{metadata|write_metadata|1|0|}23|22|}
{2010006001|XXGobject_psameas_object|33|0|56|0|{write_metadata|out0_metadata|0|0|}20|8|}
{2010006001|XXGobject_psameas_object|34|0|57|0|{in_metadata|log_metadata|0|0|}8|24|}
{2010212001|XXGvertex_oport_oport|35|0|59|0|{0|log|}24|25|}
{2010213001|XXGoport_dst_flow|36|0|61|0|{0|}25|5|}
{2010007001|XXGobject_proto_object|37|0|62|0|{}24|26|}
{2010212001|XXGvertex_oport_oport|38|0|64|0|{0|log|}26|27|}
{2010006001|XXGobject_psameas_object|39|0|66|0|{metadata|log_metadata|1|0|}27|26|}
{2010006001|XXGobject_psameas_object|40|0|67|0|{Layout|Layout|0|0|}24|28|}
{2010212001|XXGvertex_oport_oport|41|0|69|0|{0|read|}28|29|}
{2010213001|XXGoport_dst_flow|42|0|71|0|{0|}29|6|}
{2010211001|XXGvertex_iport_iport|43|0|72|0|{0|write|}28|30|}
{2010214001|XXGiport_src_flow|44|0|74|0|{0|}30|5|}
{2010007001|XXGobject_proto_object|45|0|75|0|{}28|31|}
{2010212001|XXGvertex_oport_oport|46|0|77|0|{0|read|}31|32|}
{2010006001|XXGobject_psameas_object|47|0|79|0|{metadata|read_metadata|1|0|}32|31|}
{2010211001|XXGvertex_iport_iport|48|0|80|0|{0|write|}31|33|}
{2010006001|XXGobject_psameas_object|49|0|82|0|{metadata|write_metadata|1|0|}33|31|}
{2010006001|XXGobject_psameas_object|50|0|83|0|{read_metadata|log_metadata|0|0|}28|24|}
{2010006001|XXGobject_psameas_object|51|0|84|0|{write_metadata|log_metadata|0|0|}28|24|}
{2010006001|XXGobject_psameas_object|52|0|85|0|{reject0_metadata|log_metadata|0|0|}8|24|}
{2010601001|XXGgraph_vertex_vertex|53|0|86|0|{Rename_Run_SQL_Log_File_Tmp|}1|34|}
{2010212001|XXGvertex_oport_oport|54|0|88|0|{0|out|}34|35|}
{2010211001|XXGvertex_iport_iport|55|0|90|0|{0|in|}34|36|}
{2010007001|XXGobject_proto_object|56|0|92|0|{}34|37|}
{2010212001|XXGvertex_oport_oport|57|0|94|0|{0|out|}37|38|}
{2010006001|XXGobject_psameas_object|58|0|96|0|{metadata|out_metadata|1|0|}38|37|}
{2010211001|XXGvertex_iport_iport|59|0|97|0|{0|in|}37|39|}
{2010006001|XXGobject_psameas_object|60|0|99|0|{metadata|in_metadata|1|0|}39|37|}
{2010601001|XXGgraph_vertex_vertex|61|0|100|0|{Run_SQL|}1|24|}
{2010601001|XXGgraph_vertex_vertex|62|0|101|0|{Run_SQL_Log_File|}1|28|}
{2010601001|XXGgraph_vertex_vertex|63|0|102|0|{Run_SQL_Log_File_Tmp|}1|20|}
{2010109001|XXGobject_property_value|64|0|103|0|{0|TrackingThumbprint|72|}1|40|}
