@
{18|Type|XXGrepository|Primary_Name|XXGgraph|_ab_object_schema|0|_ab_semantic_schema|5|eme_flatten_format|1|schema_version|34|root_id|0|ent_count|48|rel_count|72|}
{2010001002|XXGdirectory|0|1|1|0|{@{}@}}
{2010600005|XXGgraph|1|0|1|0|{Subgraph created from graph "subject_area_table_load"|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|ETL_ID||3|2|REFO$|subject_area.table_id|{0|}}
{30001002|XXparameter|JOB_ENV||3|2|REFO$|primary or secondary|{0|}}
{30001002|XXparameter|INPUT_FILE||3|2|FO$||{0|}}
{30001002|XXparameter|OUTPUT_FILE||3|2|FO$||{0|}}
{30001002|XXparameter|OVRD_DML_FILENAME|$ETL_ID.read.ovrd.dml|3|2|RE$||{0|}}
{30001002|XXparameter|JOB_TYPE|hdfs_load|3|1|REl||{0|}}
{30001002|XXparameter|DW_CFG|$DW_CFG|3|2|RE$||{0|}}
{30001002|XXparameter|DW_DML|$DW_DML|3|2|RE$||{0|}}
{30001002|XXparameter|DW_EXE|$DW_EXE|3|2|RE$||{0|}}
{30001002|XXparameter|ETL_CFG_FILE|$DW_CFG/$ETL_ID.cfg|3|2|RE$||{0|}}
{30001002|XXparameter|SUBJECT_AREA|$\{ETL_ID%%.*\}|3|6|REk||{0|}}
{30001002|XXparameter|TABLE_ID|$\{ETL_ID##*.\}|3|6|REk||{0|}}
{30001002|XXparameter|AB_JOB|$(if [ $ETL_ENV ]
then
   print $AB_JOB.$ETL_ID.$ETL_ENV.$JOB_ENV
else
   print $AB_JOB.$ETL_ID.$JOB_ENV
fi)|3|6|REk||{0|}}
{30001002|XXparameter|DW_SA_ARC|$DW_ARC/$JOB_ENV/$SUBJECT_AREA|3|2|RE$||{0|}}
{30001002|XXparameter|DW_SA_DAT|$DW_DAT/$JOB_ENV/$SUBJECT_AREA|3|2|RE$||{0|}}
{30001002|XXparameter|DW_SA_IN|$DW_IN/$JOB_ENV/$SUBJECT_AREA|3|2|RE$||{0|}}
{30001002|XXparameter|DW_SA_LOG|$\{DW_SA_LOG:-$DW_LOG/$JOB_ENV/$SUBJECT_AREA\}|3|6|REk||{0|}}
{30001002|XXparameter|DW_SA_TMP|$DW_TMP/$JOB_ENV/$SUBJECT_AREA|3|2|RE$||{0|}}
{30001002|XXparameter|FILE_DATETIME|$\{CURR_DATETIME:-$(date '+%Y%m%d-%H%M%S')\}|3|6|REk||{0|}}
{30001002|XXparameter|USE_OVRD_DML|$(grep "^USE_OVRD_DML\\>" $ETL_CFG_FILE \| read PARAM VALUE COMMENT; print $\{VALUE:-0\})|3|6|Kk||{0|}}
{30001002|XXparameter|INPUT_DML|$(if [[ $USE_OVRD_DML -eq 1 ]]
  then
      print $DW_DML/$OVRD_DML_FILENAME
  else
      print $DW_DML/$ETL_ID.read.dml
  fi)|3|6|REk||{0|}}
{30001002|XXparameter|CNDTL_COMPRESSION|$(grep "^CNDTL_COMPRESSION\\>" $ETL_CFG_FILE \| read PARAM VALUE COMMENT; print $\{VALUE:-0\})|3|6|Kk||{0|}}
{30001002|XXparameter|CNDTL_COMPRESSION_SFX|$(grep "^CNDTL_COMPRESSION_SFX\\>" $ETL_CFG_FILE \| read PARAM VALUE COMMENT; print $VALUE)|3|6|Kk||{0|}}
{30001002|XXparameter|CNDTL_REFORMAT|$(grep "^CNDTL_REFORMAT\\>" $ETL_CFG_FILE \| read PARAM VALUE COMMENT; print $\{VALUE:-0\})|3|6|REk||{0|}}
{30001002|XXparameter|REFORMAT_TRANS_FILE|$DW_XFR/$ETL_ID.reformat.xfr|3|2|RE$|Reformat transformation file|{0|}}
{30001002|XXparameter|OUTPUT_DML|$(if (($CNDTL_REFORMAT))
then
   print $DW_DML/$ETL_ID.write.dml
else
   print $INPUT_DML
fi)|3|6|REk||{0|}}
{30001002|XXparameter|XFORM_REJLIMIT|$(grep "^HDFS_LOAD_XFORM_REJLIMIT\\>" $ETL_CFG_FILE \| read PARAM VALUE COMMENT; print $\{VALUE:-0\})|3|6|k||{0|}}
{30001002|XXparameter|XFORM_REJRAMP|$(grep "^HDFS_LOAD_XFORM_REJRAMP\\>" $ETL_CFG_FILE \| read PARAM VALUE COMMENT; print $\{VALUE:-0.0\})|3|6|k||{0|}}
{30001002|XXparameter|XFORM_REJFILE|$DW_SA_TMP/$TABLE_ID.ld.reformat.rej|3|2|RE$||{0|}}
{30001002|XXparameter|XFORM_ERRFILE|$DW_SA_TMP/$TABLE_ID.ld.reformat.err|3|2|RE$||{0|}}
{30001002|XXparameter|HADOOP_CONN|$(grep "^HDFS_TGT_CONN\\>" $ETL_CFG_FILE \| read PARAM VALUE COMMENT; eval print $\{VALUE:-$DW_DBC/hdfs_$HD_CLUSTER.xml\})|3|6|Kk||{0|}}
{30001002|XXparameter|_ab_has_render_fixups|2|1|1|Hl||{0|}}
{30001002|XXparameter|_parameters_of_XForm_Reject_File|@34|9|FHK|@{0|}}
{30001002|XXparameter|_parameters_of_Load_Reformat|@34|9|FHK|@{0|}}
{30001002|XXparameter|_parameters_of_XForm_Error_File|@34|9|FHK|@{0|}}
{30001002|XXparameter|_parameters_of_Write_Hadoop_Files|@34|9|FHK|@{0|}}
{30001002|XXparameter|_parameters_of_Input_File|@34|9|FHK|@{0|}}
{30001002|XXparameter|!ab_compatibility|3.1.4.4|3|9|P|@{0|}}
{30001002|XXparameter|_ab_semantic_schema|5|1|1|Hnl|@{0|}}
}}@0|@10590|52250|0|0|1152000|1166000|0|single_etl_to_hdfs_ab_copy|Michael Weng|Created 06/20/2018|1|10|-1||6||34817|{0|}0|0|{0|}{0|}{0|}{0|}1.0|1151000|1166000|3|}}
{2010703001|XXGgraphinfo|2|0|3|0|{@{}@1|3.1.4.1|{1|1|}Compilation of graph "single_etl_to_hdfs_ab_copy" started at Thursday, June 28, 2018 11:28:12
No compilation errors
||m_env -v

if [ -z $CNDTL_REFORMAT ]
then
   print "$0: Error: CNDTL_REFORMAT variable not set"
   exit 4
fi

export AB_REPORT="$AB_REPORT error-codes"
|


























|@|31634943|}}
{2010705002|XXGrunsettings|3|0|5|0|{@{}@Yubikey phxbastion100 to phxdpelega001||||15|300|0|1|1|1|0|0|0|1|1|0|0|1|1|1|1|0|0|0|@17|}}
{2010242002|XXGface|4|0|7|0|{@{}@Courier New|16711680|100|0|}}
{2010240003|XXGlegend|5|0|9|0|{@{}@chmod $LOAD_LOGFILE will change the permissions on the $LOAD_LOGFILE to 664 once the file is found.|17235|933622|37000|954000|792000|16000|521|}}
{2010242002|XXGface|6|0|11|0|{@{}@Courier New|16711680|100|0|}}
{2010240003|XXGlegend|7|0|13|0|{@{}@Replicate condition is always false.  Allows for connecting all Output Table
components but does not need to run.|17639|974897|38000|995000|608000|32000|583|}}
{2010242002|XXGface|8|0|15|0|{@{}@Courier New|16711680|100|0|}}
{2010240003|XXGlegend|9|0|17|0|{@{}@Input File can be one or many uncompressed files.  The $INPUT_FILES parameter is built by reading
through the $DW_CFG/$ETL_ID.sources.lis and, if processing caty files, the $DW_CFG/dw_caty.sources.lis
file.|17031|856646|37000|877000|816000|48000|1171|}}
{2010242002|XXGface|10|0|19|0|{@{}@Courier New|16711680|100|0|}}
{2010240003|XXGlegend|11|0|21|0|{@{}@The Output Table, Reject File and Gather Logs components are conditional based on
the value of the $FLOAD_RECORD_COUNT variable compared to the number of records in
the Input File and the $USE_PARALLEL_FLOAD variable. The number of records in the
Input File is read from the file $DW_SA_IN/$TABLE_ID.record_count.dat.$BATCH_SEQ_NUM.
If this number is less than $FLOAD_RECORD_COUNT, Output Table - Tpump is run. If
this number is greater than $FLOAD_RECORD_COUNT, then the value of $USE_PARALLEL_FLOAD
determines the flow. If this has a value of 1, then Output Table - Teradata Parallel
Transport is used. Otherwise, the flow is directed to Output Table - Fastload.|17430|1022473|37000|1042000|688000|128000|2005|}}
{2010242002|XXGface|12|0|23|0|{@{}@Courier New|16711680|100|0|}}
{2010240003|XXGlegend|13|0|25|0|{@{}@Utility Load Table Check performs a drop of any working tables used by Fastload or Tpump.  It then
performs the delete of the stage table.  If Fastload has a lock on the stage table, the table is
dropped and recreated.  Phase 0 is not checkpointed.  It will run everytime this graph is run.|17205|784246|37000|804000|784000|48000|2006|}}
{2010242002|XXGface|14|0|27|0|{@{}@Courier New|16711680|100|0|}}
{2010210004|XXGflow|15|0|29|0|{@{}@384|.5|.5|{12|732000|234000|752000|234000|884000|234000|884000|374000|1017000|374000|1038000|374000|}3252|17|}}
{2010210004|XXGflow|16|0|31|0|{@{}@384|.5|.5|{12|634000|287000|634000|307000|634000|375000|569000|375000|569000|443000|569000|464000|}3254|19|}}
{2010210004|XXGflow|17|0|33|0|{@{}@384|.5|.5|{12|261000|369000|281000|369000|426000|369000|426000|234000|572000|234000|593000|234000|}3250|17|}}
{2010210004|XXGflow|18|0|35|0|{@{}@384|.5|.5|{12|694000|287000|694000|307000|694000|370000|759000|370000|759000|433000|759000|454000|}3256|19|}}
{2010503005|XXGfvertex|19|0|37|0|{|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|condition||3|6|k|@{0|}}
{30001002|XXparameter|Layout|$[[record kind 67 subkind 87 parts [vector _interp_("$(cat $INPUT_FILE)", "dollar_substitution")]]]|3|9||@{0|}}
{30001002|XXparameter|read_metadata|$INPUT_DML|3|2|f$|@{0|}}
{30001002|XXparameter|eme_dataset_mapping|$[[record map_component 1 datasets [vector [record variable _interp_("read", "pdl") map_dataset 1 mapping [record dataset_path _interp_("$INPUT_FILE", "pdl") create_update 1 is_db 0]]]]]|3|9||@{0|}}
{30001002|XXparameter|!prototype_path|$AB_COMPONENTS/Datasets/Input_File.mdc|3|2|Pf$|@{0|}}
}}@0|@172927|324838|195000|345000|56000|50000|3251|Input File|Craig Werre||1|100|0||6||32769|-1|-1|}}
{2010203004|XXGoport|20|0|39|0|{@{}@250000|364000|11000|11000|read|0.0|@@@2160|0|}}
{2010503005|XXGfvertex|21|0|42|0|{Represents one file, many files, or a multifile as an input to your graph.|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|protection|0666|12|2|RF$||{0|}}
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
{30001002|XXparameter|eme_dataset_mapping||40|9|F|Place in the EME to create the dataset(s) corresponding to this component.|{0|}}
}}@0|@0|0|0|0|0|0|0|@@@1|100|-1|@6|@1|-1|-1|}}
{2010203004|XXGoport|22|0|44|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|read|0.0|@@@2160|0|}}
{2010501005|XXGpvertex|23|0|47|0|{|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|transform0|$REFORMAT_TRANS_FILE|3|2|f$|@{0|}}
{30001002|XXparameter|reject_threshold|Use limit/ramp|3|1|l|@{0|}}
{30001002|XXparameter|limit|$XFORM_REJLIMIT|3|2|$|@{0|}}
{30001002|XXparameter|ramp|$XFORM_REJRAMP|3|2|$|@{0|}}
{30001002|XXparameter|condition|$CNDTL_REFORMAT|3|2|$|@{0|}}
{30001002|XXparameter|out0_metadata|$OUTPUT_DML|3|2|f$|@{0|}}
{30001002|XXparameter|error0_metadata|string('\\n')|3|1|l|@{0|}}
{30001002|XXparameter|log_metadata|record string("\|") node, timestamp, component, subcomponent, event_type; string("\|\\n") event_text; end|3|1|l|@{0|}}
{30001002|XXparameter|!prototype_path|$AB_COMPONENTS/Transform/Reformat.mpc|3|2|Pf$|@{0|}}
{30001002|XXparameter|Layout||3|8|s=|@{0|}}
{30001002|XXparameter|in_metadata||3|8|s=|@{0|}}
{30001002|XXparameter|reject0_metadata||3|8|s=|@{0|}}
}}@0|@584360|168251|604000|188000|118000|89000|3249|Load Reformat|Ab Initio Software|Built-in|1|10|0||6||32769|1|{1|0|}}}
{2010203004|XXGoport|24|0|49|0|{@{}@721000|229000|11000|11000|out0|0.0|count|out|0|2448|0|}}
{2010203004|XXGoport|25|0|52|0|{@{}@629000|276000|11000|11000|reject0|0.0|count|reject|0|1168|0|}}
{2010203004|XXGoport|26|0|55|0|{@{}@689000|276000|11000|11000|error0|0.0|count|error|0|1176|0|}}
{2010203004|XXGoport|27|0|58|0|{@{}@0|0|0|0|log|0.0|@@@1176|0|}}
{2010202004|XXGiport|28|0|60|0|{@{}@594000|229000|11000|11000|in|0.0|@@@1808|0|}}
{2010501005|XXGpvertex|29|0|63|0|{Changes the record format of your data by dropping fields or by using DML expressions to add fields, combine fields, or modify the data.|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|mpname|reformat-transform|3|1|Hl||{0|}}
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
}}@0|@0|0|0|0|0|0|0|@@@1|100|-1|@6|@1|1|{1|0|}}}
{2010203004|XXGoport|30|0|65|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
{30001002|XXparameter|transform0||8|8|FO=|Reformat transform|{0|}}
{30001002|XXparameter|transform0.index|count|3|9|P|@{0|}}
{30001002|XXparameter|transform0.index_value|0|3|9|P|@{0|}}
{30001002|XXparameter|transform0.index_name|transform|3|9|P|@{0|}}
}}@0|0|0|0|out0|0.0|count|out|0|2448|0|}}
{2010203004|XXGoport|31|0|69|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|reject0|0.0|count|reject|0|1168|0|}}
{2010203004|XXGoport|32|0|72|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|error0|0.0|count|error|0|1176|0|}}
{2010203004|XXGoport|33|0|75|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|F=||{0|}}
}}@0|0|0|0|log|0.0|@@@1176|0|}}
{2010202004|XXGiport|34|0|78|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|in|0.0|@@@1808|0|}}
{2010501005|XXGpvertex|35|0|84|0|{|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|output_file|$OUTPUT_FILE|3|2|$|@{0|}}
{30001002|XXparameter|job_config|$HADOOP_CONN|3|2|$|@{0|}}
{30001002|XXparameter|overwrite|False|3|1|l|@{0|}}
{30001002|XXparameter|!prototype_path|$AB_COMPONENTS/Datasets/Hadoop/Write_Hadoop_Files.mpc|3|2|Pf$|@{0|}}
{30001002|XXparameter|Layout||3|8|=|@{0|}}
{30001002|XXparameter|in_metadata||3|8|s=|@{0|}}
}}@0|@1028606|325653|1049000|346000|100000|60000|3253|Write Hadoop Files|Ab Initio Software|Built-in 3.1.0:|1|10|0||6||32769|1|{1|0|}}}
{2010202004|XXGiport|36|0|86|0|{@{}@1039000|369000|11000|11000|in|0.0|@@@1984|0|}}
{2010501005|XXGpvertex|37|0|89|0|{Writes files to HDFS (Hadoop Distributed File System).|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|mpname||3|1|Hl||{0|}}
{30001002|XXparameter|image__|/~ab_home/bin/write-hadoop-files.ksh|3|2|H$||{0|}}
{30001002|XXparameter|output_file||3|2|RFO$|The absolute path for file(s) in an HDFS file system (e.g., /user/jones/output.dat).  If this component is run in more than one partition, the partition number is added to the file name before the file's extension, if present, otherwise, as a suffix.  The file's extension is anything after the first '.' in the file name.|{0|}}
{30001002|XXparameter|job_config||3|2|FO$|An optional XML-formatted configuration file containing additional properties for Hadoop.|{0|}}
{30001002|XXparameter|overwrite|True|13|1|FKl|If True, overwrite existing files.|{0|}}
{30001002|XXparameter|compression|none|15|1|FKl|The compression type for output file (none, by default).  Other compression types are 'gzip' and 'bzip2'.|{3|none|gzip|bzip2|}}
{30001002|XXparameter|permissions|644|3|2|FK$|The unix-style file permissions, for newly-created files.|{0|}}
{30001002|XXparameter|HADOOP_HOME|$HADOOP_HOME|3|2|FK$|Home directory for Hadoop installation (e.g., /usr/local/hadoop)|{0|}}
{30001002|XXparameter|dataset_analysis|eme_dataset(file)=in,w|3|2|H$||{0|}}
{30001002|XXparameter|Layout|@9|2|RF$||{0|}}
{30001002|XXparameter|in_metadata||7|1|RFsl||{0|}}
{30001002|XXparameter|doc_transform||8|2|FHs$|Document your transformation for dependency analysis|{0|}}
{30001002|XXparameter|doc_operation1|::document(in)|3|2|RH$||{0|}}
{30001002|XXparameter|mpcmodtime|1325635136|1|1|Hl|The last modification time of this component's template|{0|}}
{30001002|XXparameter|condition||3|2|F$||{0|}}
{30001002|XXparameter|condition_interpretation||15|1|Fl||{2|Replace with flow|Remove completely|}}
{30001002|XXparameter|condition_interpretation.display_name|condition-interpretation|3|9|P|@{0|}}
{30001002|XXparameter|conditionInputPort||3|2|F$||{0|}}
{30001002|XXparameter|conditionOutputPort||3|2|F$||{0|}}
{30001002|XXparameter|eme_dataset_mapping||40|9|F|Place in the EME to create the dataset(s) corresponding to this component.|{0|}}
}}@0|@0|0|0|0|0|0|0|@@@1|100|-1|@6|@1|1|{1|0|}}}
{2010202004|XXGiport|38|0|91|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|in|0.0|@@@1984|0|}}
{2010501005|XXGpvertex|39|0|96|0|{Shows the error associated with the rejected record.|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|LogFile|$XFORM_ERRFILE|3|2|$|@{0|}}
{30001002|XXparameter|condition|$CNDTL_REFORMAT|3|2|$|@{0|}}
{30001002|XXparameter|!prototype_path|$AB_COMPONENTS/Miscellaneous/Gather_Logs.mpc|3|2|Pf$|@{0|}}
{30001002|XXparameter|in_metadata||3|8|s=|@{0|}}
}}@0|@724978|459207|740000|465000|40000|47000|3257|XForm Error File|Ab Initio Software|Built-in|1|10|0||6||32769|1|{1|0|}}}
{2010202004|XXGiport|40|0|98|0|{@{}@754000|455000|11000|11000|in|0.0|@@@786|0|}}
{2010501005|XXGpvertex|41|0|101|0|{Collects the output from log ports of components for analysis of a graph after execution.|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|mpname|logger|3|1|Hl||{0|}}
{30001002|XXparameter|image__|unitool|3|2|H$||{0|}}
{30001002|XXparameter|LogFile|@28|2|RFO$|Filename to log to|{0|}}
{30001002|XXparameter|StartText|Start|3|2|RFO$|Text to include in start record|{0|}}
{30001002|XXparameter|EndText|End|3|2|RFO$|Text to include in end record|{0|}}
{30001002|XXparameter|condition||3|2|F$||{0|}}
{30001002|XXparameter|conditionInputPort||3|2|F$||{0|}}
{30001002|XXparameter|conditionOutputPort||3|2|F$||{0|}}
{30001002|XXparameter|condition_interpretation|Remove completely|15|1|Fl||{2|Replace with flow|Remove completely|}}
{30001002|XXparameter|condition_interpretation.display_name|condition-interpretation|3|9|P|@{0|}}
{30001002|XXparameter|Layout|file:.|9|2|F$|logger should run serially by default|{0|}}
{30001002|XXparameter|continuous_analysis|publisher, fan_in_ok|3|2|H$||{0|}}
{30001002|XXparameter|in_metadata||7|1|RFsl||{0|}}
{30001002|XXparameter|doc_transform||8|2|FHs$|Document your transformation for dependency analysis|{0|}}
{30001002|XXparameter|doc_operation1|::document(in)|3|2|RH$||{0|}}
{30001002|XXparameter|mpcmodtime|1299550339|1|1|Hl|The last modification time of this component's template|{0|}}
}}@0|@0|0|0|0|0|0|0|@@@1|100|-1|@6|@1|1|{1|0|}}}
{2010202004|XXGiport|42|0|103|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|in|0.0|@@@786|0|}}
{2010501005|XXGpvertex|43|0|107|0|{Rejected records from the load transform component.|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|LogFile|$XFORM_REJFILE|3|2|$|@{0|}}
{30001002|XXparameter|condition|$CNDTL_REFORMAT|3|2|$|@{0|}}
{30001002|XXparameter|!prototype_path|$AB_COMPONENTS/Miscellaneous/Gather_Logs.mpc|3|2|Pf$|@{0|}}
{30001002|XXparameter|in_metadata||3|8|s=|@{0|}}
}}@0|@533998|469179|549000|475000|40000|47000|3255|XForm Reject File|Ab Initio Software|Built-in|1|10|0||6||32769|1|{1|0|}}}
{2010202004|XXGiport|44|0|109|0|{@{}@564000|465000|11000|11000|in|0.0|@@@786|0|}}
{2010501005|XXGpvertex|45|0|112|0|{Collects the output from log ports of components for analysis of a graph after execution.|{30100001|XXparameter_set|@@@@{{30001002|XXparameter|mpname|logger|3|1|Hl||{0|}}
{30001002|XXparameter|image__|unitool|3|2|H$||{0|}}
{30001002|XXparameter|LogFile|@28|2|RFO$|Filename to log to|{0|}}
{30001002|XXparameter|StartText|Start|3|2|RFO$|Text to include in start record|{0|}}
{30001002|XXparameter|EndText|End|3|2|RFO$|Text to include in end record|{0|}}
{30001002|XXparameter|condition||3|2|F$||{0|}}
{30001002|XXparameter|conditionInputPort||3|2|F$||{0|}}
{30001002|XXparameter|conditionOutputPort||3|2|F$||{0|}}
{30001002|XXparameter|condition_interpretation|Remove completely|15|1|Fl||{2|Replace with flow|Remove completely|}}
{30001002|XXparameter|condition_interpretation.display_name|condition-interpretation|3|9|P|@{0|}}
{30001002|XXparameter|Layout|file:.|9|2|F$|logger should run serially by default|{0|}}
{30001002|XXparameter|continuous_analysis|publisher, fan_in_ok|3|2|H$||{0|}}
{30001002|XXparameter|in_metadata||7|1|RFsl||{0|}}
{30001002|XXparameter|doc_transform||8|2|FHs$|Document your transformation for dependency analysis|{0|}}
{30001002|XXparameter|doc_operation1|::document(in)|3|2|RH$||{0|}}
{30001002|XXparameter|mpcmodtime|1299550339|1|1|Hl|The last modification time of this component's template|{0|}}
}}@0|@0|0|0|0|0|0|0|@@@1|100|-1|@6|@1|1|{1|0|}}}
{2010202004|XXGiport|46|0|114|0|{@{30100001|XXparameter_set|@@@@{{30001002|XXparameter|metadata||7|8|RF=||{0|}}
}}@0|0|0|0|in|0.0|@@@786|0|}}
{2010110002|XXGconstant|47|0|118|0|{@{}@{1|1084086|}}}
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
{2010605001|XXGgraph_legend_legend|10|0|20|0|{3|}1|11|}
{2010241001|XXGlegend_face_face|11|0|22|0|{}11|12|}
{2010605001|XXGgraph_legend_legend|12|0|24|0|{4|}1|13|}
{2010241001|XXGlegend_face_face|13|0|26|0|{}13|14|}
{2010604001|XXGgraph_flow_flow|14|0|28|0|{Flow_1|}1|15|}
{2010604001|XXGgraph_flow_flow|15|0|30|0|{Flow_13|}1|16|}
{2010604001|XXGgraph_flow_flow|16|0|32|0|{Flow_2|}1|17|}
{2010604001|XXGgraph_flow_flow|17|0|34|0|{Flow_33|}1|18|}
{2010601001|XXGgraph_vertex_vertex|18|0|36|0|{Input_File|}1|19|}
{2010212001|XXGvertex_oport_oport|19|0|38|0|{0|read|}19|20|}
{2010213001|XXGoport_dst_flow|20|0|40|0|{0|}20|17|}
{2010007001|XXGobject_proto_object|21|0|41|0|{}19|21|}
{2010212001|XXGvertex_oport_oport|22|0|43|0|{0|read|}21|22|}
{2010006001|XXGobject_psameas_object|23|0|45|0|{metadata|read_metadata|1|0|}22|21|}
{2010601001|XXGgraph_vertex_vertex|24|0|46|0|{Load_Reformat|}1|23|}
{2010212001|XXGvertex_oport_oport|25|0|48|0|{0|out0|}23|24|}
{2010213001|XXGoport_dst_flow|26|0|50|0|{0|}24|15|}
{2010212001|XXGvertex_oport_oport|27|0|51|0|{1|reject0|}23|25|}
{2010213001|XXGoport_dst_flow|28|0|53|0|{0|}25|16|}
{2010212001|XXGvertex_oport_oport|29|0|54|0|{2|error0|}23|26|}
{2010213001|XXGoport_dst_flow|30|0|56|0|{0|}26|18|}
{2010212001|XXGvertex_oport_oport|31|0|57|0|{3|log|}23|27|}
{2010211001|XXGvertex_iport_iport|32|0|59|0|{0|in|}23|28|}
{2010214001|XXGiport_src_flow|33|0|61|0|{0|}28|17|}
{2010007001|XXGobject_proto_object|34|0|62|0|{}23|29|}
{2010212001|XXGvertex_oport_oport|35|0|64|0|{0|out0|}29|30|}
{2010006001|XXGobject_psameas_object|36|0|66|0|{metadata|out0_metadata|1|0|}30|29|}
{2010006001|XXGobject_psameas_object|37|0|67|0|{transform0|transform0|1|0|}30|29|}
{2010212001|XXGvertex_oport_oport|38|0|68|0|{1|reject0|}29|31|}
{2010006001|XXGobject_psameas_object|39|0|70|0|{metadata|reject0_metadata|1|0|}31|29|}
{2010212001|XXGvertex_oport_oport|40|0|71|0|{2|error0|}29|32|}
{2010006001|XXGobject_psameas_object|41|0|73|0|{metadata|error0_metadata|1|0|}32|29|}
{2010212001|XXGvertex_oport_oport|42|0|74|0|{3|log|}29|33|}
{2010006001|XXGobject_psameas_object|43|0|76|0|{metadata|log_metadata|1|0|}33|29|}
{2010211001|XXGvertex_iport_iport|44|0|77|0|{0|in|}29|34|}
{2010006001|XXGobject_psameas_object|45|0|79|0|{metadata|in_metadata|1|0|}34|29|}
{2010006001|XXGobject_psameas_object|46|0|80|0|{Layout|Layout|0|0|}23|19|}
{2010006001|XXGobject_psameas_object|47|0|81|0|{in_metadata|read_metadata|0|0|}23|19|}
{2010006001|XXGobject_psameas_object|48|0|82|0|{reject0_metadata|read_metadata|0|0|}23|19|}
{2010601001|XXGgraph_vertex_vertex|49|0|83|0|{Write_Hadoop_Files|}1|35|}
{2010211001|XXGvertex_iport_iport|50|0|85|0|{0|in|}35|36|}
{2010214001|XXGiport_src_flow|51|0|87|0|{0|}36|15|}
{2010007001|XXGobject_proto_object|52|0|88|0|{}35|37|}
{2010211001|XXGvertex_iport_iport|53|0|90|0|{0|in|}37|38|}
{2010006001|XXGobject_psameas_object|54|0|92|0|{metadata|in_metadata|1|0|}38|37|}
{2010006001|XXGobject_psameas_object|55|0|93|0|{Layout|Layout|0|0|}35|19|}
{2010006001|XXGobject_psameas_object|56|0|94|0|{in_metadata|out0_metadata|0|0|}35|23|}
{2010601001|XXGgraph_vertex_vertex|57|0|95|0|{XForm_Error_File|}1|39|}
{2010211001|XXGvertex_iport_iport|58|0|97|0|{0|in|}39|40|}
{2010214001|XXGiport_src_flow|59|0|99|0|{0|}40|18|}
{2010007001|XXGobject_proto_object|60|0|100|0|{}39|41|}
{2010211001|XXGvertex_iport_iport|61|0|102|0|{0|in|}41|42|}
{2010006001|XXGobject_psameas_object|62|0|104|0|{metadata|in_metadata|1|0|}42|41|}
{2010006001|XXGobject_psameas_object|63|0|105|0|{in_metadata|error0_metadata|0|0|}39|23|}
{2010601001|XXGgraph_vertex_vertex|64|0|106|0|{XForm_Reject_File|}1|43|}
{2010211001|XXGvertex_iport_iport|65|0|108|0|{0|in|}43|44|}
{2010214001|XXGiport_src_flow|66|0|110|0|{0|}44|16|}
{2010007001|XXGobject_proto_object|67|0|111|0|{}43|45|}
{2010211001|XXGvertex_iport_iport|68|0|113|0|{0|in|}45|46|}
{2010006001|XXGobject_psameas_object|69|0|115|0|{metadata|in_metadata|1|0|}46|45|}
{2010006001|XXGobject_psameas_object|70|0|116|0|{in_metadata|read_metadata|0|0|}43|19|}
{2010109001|XXGobject_property_value|71|0|117|0|{0|TrackingThumbprint|72|}1|47|}
