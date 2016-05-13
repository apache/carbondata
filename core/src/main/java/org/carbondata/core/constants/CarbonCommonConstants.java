/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.carbondata.core.constants;

public final class CarbonCommonConstants {
  /**
   * RETENTION DETAILS FILE
   */
  public static final String RETENTION_RECORD = "RetentionDeletionRecord.txt";
  /**
   * MERGER_FOLDER_EXT
   */
  public static final String MERGER_FOLDER_EXT = ".merge";
  /**
   * SLICE_MERGER_LOCK_FILE_NAME
   */
  public static final String SLICE_MERGER_LOCK_FILE_NAME = "merger.lock";
  /**
   * OFFLINEOFFPEAKMERGER_TASK_ID
   */
  public static final String OFFLINEOFFPEAKMERGER_TASK_ID = "OFFLINEOFFPEAKMERGER";
  /**
   * OFFLINEOFFPEAKMERGER_TASK_ID
   */
  public static final String OFFLINEOFFPEAKMERGER_TASK_DEC = "Offline Offpeak merger";
  /**
   * integer size in bytes
   */
  public static final int INT_SIZE_IN_BYTE = 4;
  /**
   * short size in bytes
   */
  public static final int SHORT_SIZE_IN_BYTE = 2;
  /**
   * char size in bytes
   */
  public static final int CHAR_SIZE_IN_BYTE = 2;
  /**
   * CARDIANLITY_INCREMENTOR
   */
  public static final int CARDIANLITY_INCREMENTOR = 10;
  /**
   * DOUBLE size in bytes
   */
  public static final int DOUBLE_SIZE_IN_BYTE = 8;
  /**
   * ONLINE_MERGE_MIN_VALUE
   */
  public static final int ONLINE_MERGE_MIN_VALUE = 10;
  /**
   * ONLINE_MERGE_MAX_VALUE
   */
  public static final int ONLINE_MERGE_MAX_VALUE = 100;
  /**
   * OFFLINE_MERGE_MIN_VALUE
   */
  public static final int OFFLINE_MERGE_MIN_VALUE = 100;
  /**
   * OFFLINE_MERGE_MAX_VALUE
   */
  public static final int OFFLINE_MERGE_MAX_VALUE = 500;
  /**
   * LONG size in bytes
   */
  public static final int LONG_SIZE_IN_BYTE = 8;
  /**
   * byte to KB conversion factor
   */
  public static final int BYTE_TO_KB_CONVERSION_FACTOR = 1024;
  /**
   * BYTE_ENCODING
   */
  public static final String BYTE_ENCODING = "ISO-8859-1";
  /**
   * measure meta data file name
   */
  public static final String MEASURE_METADATA_FILE_NAME = "/msrMetaData_";
  /**
   * MEASURE_METADATA_PREFIX
   */
  public static final String MEASURE_METADATA_PREFIX = "msrMetaData_";
  /**
   * location of the carbon member, hierarchy and fact files
   */
  public static final String STORE_LOCATION = "carbon.storelocation";
  /**
   * The keystore type
   */
  public static final String KEYSTORE_TYPE = "carbon.keystore.type";
  /**
   * The value store type
   */
  public static final String VALUESTORE_TYPE = "carbon.valuestore.type";
  /**
   * online merge file size
   */
  public static final String ONLINE_MERGE_FILE_SIZE = "carbon.online.merge.file.size";
  /**
   * online merge file size default value
   */
  public static final String ONLINE_MERGE_FILE_SIZE_DEFAULT_VALUE = "10";
  /**
   * offline merge file size
   */
  public static final String OFFLINE_MERGE_FILE_SIZE = "carbon.offline.merge.file.size";
  /**
   * offline merge file size default value
   */
  public static final String OFFLINE_MERGE_FILE_SIZE_DEFAULT_VALUE = "100";
  /**
   * blocklet size in carbon file
   */
  public static final String BLOCKLET_SIZE = "carbon.blocklet.size";
  /**
   * Hierarchy blocklet size
   */
  public static final String HIERARCHY_BLOCKLET_SIZE = "carbon.hier.blocklet.size";
  /**
   * TODO: max number of blocklets written in a single file?
   */
  public static final String MAX_FILE_SIZE = "carbon.max.file.size";
  /**
   * Number of cores to be used
   */
  public static final String NUM_CORES = "carbon.number.of.cores";
  /**
   * carbon batchsize
   */
  public static final String BATCH_SIZE = "carbon.batch.size";
  /**
   * CARDINALITY_INCREMENT_VALUE
   */
  public static final String CARDINALITY_INCREMENT_VALUE = "carbon.cardinality.increment.value";
  /**
   * carbon sort size
   */
  public static final String SORT_SIZE = "carbon.sort.size";
  /**
   * default location of the carbon member, hierarchy and fact files
   */
  public static final String STORE_LOCATION_DEFAULT_VAL = "../unibi-solutions/system/carbon/store";
  /**
   * default keystore type
   */
  public static final String KEYSTORE_TYPE_DEFAULT_VAL = "COMPRESSED_SINGLE_ARRAY";
  /**
   * default value store type
   */
  public static final String VALUESTORE_TYPE_DEFAULT_VAL = "HEAVY_VALUE_COMPRESSION";
  /**
   * CARDINALITY_INCREMENT_DEFAULT_VALUE
   */
  public static final String CARDINALITY_INCREMENT_VALUE_DEFAULT_VAL = "10";
  /**
   * CARDINALITY_INCREMENT_MIN_VALUE
   */
  public static final int CARDINALITY_INCREMENT_MIN_VAL = 5;
  /**
   * CARDINALITY_INCREMENT_DEFAULT_VALUE
   */
  public static final int CARDINALITY_INCREMENT_MAX_VAL = 30;
  /**
   * default blocklet size
   */
  public static final String BLOCKLET_SIZE_DEFAULT_VAL = "120000";
  /**
   * min blocklet size
   */
  public static final int BLOCKLET_SIZE_MIN_VAL = 50;
  /**
   * max blocklet size
   */
  public static final int BLOCKLET_SIZE_MAX_VAL = 12000000;
  /**
   * max Hierarchy blocklet size
   */
  public static final String HIERARCHY_BLOCKLET_SIZE_DEFAULT_VAL = "1024";
  /**
   * TODO: default value of max number of blocklet written in a single file?
   */
  public static final String MAX_FILE_SIZE_DEFAULT_VAL = "100";
  /**
   * TODO: min value of max number of blocklets written in a single file?
   */
  public static final int MAX_FILE_SIZE_DEFAULT_VAL_MIN_VAL = 1;
  /**
   * max allowed block size for a file. If block size is greater than this value
   * then the value is reset to default block size for a file
   */
  public static final int MAX_FILE_SIZE_DEFAULT_VAL_MAX_VAL = 2048;
  /**
   * default value of number of cores to be used
   */
  public static final String NUM_CORES_DEFAULT_VAL = "2";
  /**
   * min value of number of cores to be used
   */
  public static final int NUM_CORES_MIN_VAL = 1;
  /**
   * max value of number of cores to be used
   */
  public static final int NUM_CORES_MAX_VAL = 32;
  /**
   * default carbon batchsize
   */
  public static final String BATCH_SIZE_DEFAULT_VAL = "1000";
  /**
   * min carbon batchsize
   */
  public static final int BATCH_SIZE_MIN_VAL = 500;
  /**
   * max carbon batchsize
   */
  public static final int BATCH_SIZE_MAX_VAL = 100000;
  /**
   * default carbon sort size
   */
  public static final String SORT_SIZE_DEFAULT_VAL = "5000";
  /**
   * min carbon sort size
   */
  public static final int SORT_SIZE_MIN_VAL = 1000;
  /**
   * max carbon sort size
   */
  public static final int SORT_SIZE_MAX_VAL = 1000000;
  /**
   * carbon properties file path
   */
  public static final String CARBON_PROPERTIES_FILE_PATH = "../../../conf/carbon.properties";
  /**
   * CARBON_BADRECORDS_ENCRYPTION
   */
  public static final String CARBON_BADRECORDS_ENCRYPTION = "carbon.badRecords.encryption";
  /**
   * CARBON_DDL_BASE_HDFS_URL
   */
  public static final String CARBON_DDL_BASE_HDFS_URL = "carbon.ddl.base.hdfs.url";
  /**
   * CARBON_BADRECORDS_ENCRYPTION_DEFAULT_VAL
   */
  public static final String CARBON_BADRECORDS_ENCRYPTION_DEFAULT_VAL = "false";
  /**
   * carbon properties file path
   */

  public static final String CARBON_REALTIMEDATA_MAPPER_FILE_PATH =
      "../unibi-solutions/system/carbon/realtimedata.properties";
  /**
   * how the CARBON URL will start.
   */

  public static final String CARBON_URL_START = "carbon://";
  /**
   * carbon url
   */

  public static final String CARBON_URL_KEY = "url";
  /**
   * carbon schema file path
   */

  public static final String CARBON_SCHEMA_FILE_PATH = "schemafilepath";
  /**
   * carbon key
   */

  public static final String CARBON_KEY = "carbon";
  /**
   * carbon mode key
   */

  public static final String CARBON_MODE_KEY = "mode";
  /**
   * carbon mode default val
   */
  public static final String CARBON_MODE_DEFAULT_VAL = "file";
  /**
   * carbon mode in-memory val
   */
  public static final String CARBON_MODE_IN_MEM_VAL = "in-memory";
  /**
   * Slice Meta data file.
   */
  public static final String SLICE_METADATA_FILENAME = "sliceMetaData";
  /**
   * Load Folder Name
   */
  public static final String LOAD_FOLDER = "Segment_";
  /**
   * RESTructure Folder
   */
  public static final String RESTRUCTRE_FOLDER = "RS_";
  /**
   * BYTEBUFFER_SIZE
   */

  public static final int BYTEBUFFER_SIZE = 24 * 1024;
  /**
   * location of the graphs files
   */
  public static final String ETL_STORE_LOCATION = "../unibi-solutions/system/carbon/etl";
  /**
   * location of the graphs files
   */
  public static final String RESTRUCT_FILE_METADATA = "../unibi-solutions/system/carbon/tmp";
  /**
   * Average constant
   */
  public static final String AVERAGE = "avg";
  /**
   * Count constant
   */
  public static final String COUNT = "count";
  /**
   * Max constant
   */
  public static final String MAX = "max";
  /**
   * Min constant
   */
  public static final String MIN = "min";
  /**
   * distinct count
   */
  public static final String DISTINCT_COUNT = "distinct-count";
  /**
   * CUSTOM
   */
  public static final String CUSTOM = "custom";
  /**
   * SUM
   */
  public static final String SUM = "sum";
  /**
   * DUMMY aggregation function
   */
  public static final String DUMMY = "dummy";
  /**
   * MEMBER_DEFAULT_VAL
   */
  public static final String MEMBER_DEFAULT_VAL = "@NU#LL$!";
  /**
   * CARBON_CACHE_LOCATION
   */
  public static final String CARBON_CACHE_LOCATION = "../unibi-solutions/system/carbon/cache";
  /**
   * FILE STATUS IN-PROGRESS
   */
  public static final String FILE_INPROGRESS_STATUS = ".inprogress";
  /**
   * Generated count measure
   */
  public static final String GEN_COUNT_MEASURE = "$^&DEFAULT_GENERATED_COUNT_MEASURE$^&";
  /**
   * CARBON_BADRECORDS_LOCATION
   */
  public static final String CARBON_BADRECORDS_LOC = "carbon.badRecords.location";
  /**
   * CARBON_BADRECORDS_LOCATION_DEFAULT
   */
  public static final String CARBON_BADRECORDS_LOC_DEFAULT_VAL =
      "../unibi-solutions/system/carbon/badRecords";
  /**
   * HIERARCHY_FILE_EXTENSION
   */
  public static final String HIERARCHY_FILE_EXTENSION = ".hierarchy";
  /**
   * RS_TEMP_PREFIX
   */
  public static final String RS_TEMP_PREFIX = "tmpRS_";
  /**
   * RS_BAK_PREFIX
   */
  public static final String RS_BAK_PREFIX = "bakRS_";
  /**
   * SORT_TEMP_FILE_LOCATION
   */
  public static final String SORT_TEMP_FILE_LOCATION = "sortrowtmp";
  /**
   * MERGE_TEMP_FILE_LOCATION
   */
  public static final String MERGE_TEMP_FILE_LOCATION = "mergetmp";
  /**
   * GROUP_BY_TEMP_FILE_LOCATION
   */
  public static final String GROUP_BY_TEMP_FILE_LOCATION = "groupbytmp";
  /**
   * GROUP_BY_TEMP_FILE_EXTENSION
   */
  public static final String GROUP_BY_TEMP_FILE_EXTENSION = ".groupby";
  /**
   * SORT_BUFFER_SIZE
   */
  public static final String SORT_BUFFER_SIZE = "carbon.sort.buffer.size";
  /**
   * SORT_BUFFER_SIZE_DEFAULT_SIZE
   */
  public static final String SORT_BUFFER_SIZE_DEFAULT_VALUE = "5000";
  /**
   * SORT_BUFFER_SIZE_MIN_SIZE
   */
  public static final int SORT_BUFFER_SIZE_MIN_VALUE = 5;
  /**
   * DATA_LOAD_Q_SIZE
   */
  public static final String DATA_LOAD_Q_SIZE = "carbon.dataload.queuesize";
  /**
   * DATA_LOAD_Q_SIZE_DEFAULT
   */
  public static final String DATA_LOAD_Q_SIZE_DEFAULT = "100";
  /**
   * DATA_LOAD_Q_SIZE_MIN
   */
  public static final int DATA_LOAD_Q_SIZE_MIN = 1;
  /**
   * DATA_LOAD_Q_SIZE_MAX
   */
  public static final int DATA_LOAD_Q_SIZE_MAX = 100;
  /**
   * DATA_LOAD_Q_SIZE
   */
  public static final String CARBON_MEMBERUPDATE_DATA_LOAD_Q_SIZE =
      "carbon.memberupdate.dataloader.queuesize";
  /**
   * DATA_LOAD_Q_SIZE_MIN
   */
  public static final int CARBON_MEMBERUPDATE_DATA_LOAD_Q_SIZE_MIN = 1;
  /**
   * DATA_LOAD_Q_SIZE_MAX
   */
  public static final int CARBON_MEMBERUPDATE_DATA_LOAD_Q_SIZE_MAX = 100;
  /**
   * DATA_LOAD_Q_SIZE_DEFAULT
   */
  public static final String CARBON_MEMBERUPDATE_DATA_LOAD_Q_SIZE_DEFAULT = "100";
  /**
   * DATA_LOAD_CONC_EXE_SIZE
   */
  public static final String CARBON_MEMBERUPDATE_DATA_LOAD_CONC_EXE_SIZE =
      "carbon.memberupdate.dataload.concurrent.execution.size";
  /**
   * DATA_LOAD_CONC_EXE_SIZE_DEFAULT
   */
  public static final String CARBON_MEMBERUPDATE_DATA_LOAD_CONC_EXE_SIZE_DEFAULT = "1";
  /**
   * DATA_LOAD_CONC_EXE_SIZE_MIN
   */
  public static final int CARBON_MEMBERUPDATE_DATA_LOAD_CONC_EXE_SIZE_MIN = 1;
  /**
   * DATA_LOAD_CONC_EXE_SIZE_MAX
   */
  public static final int CARBON_MEMBERUPDATE_DATA_LOAD_CONC_EXE_SIZE_MAX = 5;
  /**
   * DATA_LOAD_CONC_EXE_SIZE
   */
  public static final String DATA_LOAD_CONC_EXE_SIZE = "carbon.dataload.concurrent.execution.size";
  /**
   * DATA_LOAD_CONC_EXE_SIZE_DEFAULT
   */
  public static final String DATA_LOAD_CONC_EXE_SIZE_DEFAULT = "1";
  /**
   * DATA_LOAD_CONC_EXE_SIZE_MIN
   */
  public static final int DATA_LOAD_CONC_EXE_SIZE_MIN = 1;
  /**
   * DATA_LOAD_CONC_EXE_SIZE_MAX
   */
  public static final int DATA_LOAD_CONC_EXE_SIZE_MAX = 5;
  /**
   * CARBON_Realtime_data
   */
  public static final String CARBON_REALTIMEDATA_FILE =
      "../unibi-solutions/system/carbon/realtimedata.properties";
  /**
   * CARBON_RESULT_SIZE_KEY
   */
  public static final String CARBON_RESULT_SIZE_KEY = "carbon.result.limit";
  /**
   * CARBON_RESULT_SIZE_DEFAULT
   */
  public static final String CARBON_RESULT_SIZE_DEFAULT = "5000";
  /**
   * MYSQL_NULL_VALUE
   */
  public static final String MYSQL_NULL_VALUE = "mysql.null.value";
  /**
   * MSSQL_NULL_VALUE
   */
  public static final String MSSQL_NULL_VALUE = "mssql.null.value";
  /**
   * CARBON_RESULT_SIZE_DEFAULT
   */
  public static final String ORACLE_NULL_VALUE = "oracle.null.value";
  /**
   * CARBON_RESULT_SIZE_DEFAULT
   */
  public static final String LEVEL_FILE_EXTENSION = ".level";
  /**
   * FACT_FILE_EXT
   */
  public static final String FACT_FILE_EXT = ".carbondata";
  /**
   * MEASUREMETADATA_FILE_EXT
   */
  public static final String MEASUREMETADATA_FILE_EXT = ".msrmetadata";
  /**
   * QUERY_OUT_FILE_EXT
   */
  public static final String QUERY_OUT_FILE_EXT = ".querypart";
  /**
   * QUERY_MERGED_FILE_EXT
   */
  public static final String QUERY_MERGED_FILE_EXT = ".querymergred";
  /**
   * PAGINATED_CACHE_FOLDER
   */
  public static final String PAGINATED_CACHE_FOLDER = "paginationCache";
  /**
   * PAGINATED CACHE ALLOCATED SIZE in MB
   */
  public static final String PAGINATED_CACHE_DISK_SIZE = "carbon.paginated.cache.disk.size";
  /**
   * PAGINATED CACHE ALLOCATED SIZE in bytes
   */
  public static final Long PAGINATED_CACHE_DISK_SIZE_DEFAULT = 2048L;
  /**
   * PAGINATED CACHE ALLOCATED SIZE in bytes
   */
  public static final Long PAGINATED_CACHE_DISK_SIZE_MAX = 102400L;
  /**
   * PAGINATED CACHE ALLOCATED SIZE in bytes
   */
  public static final Long PAGINATED_CACHE_DISK_SIZE_MIN = 256L;
  /**
   * Measure Source Data type
   */
  public static final String MEASURE_SRC_DATA_TYPE = "MeasureSourceDataType";
  /**
   * PAGINATED Internal merge size limit
   */
  public static final String PAGINATED_INTERNAL_MERGE_SIZE_LIMIT =
      "carbon.paginated.internal.merge.size.limit";
  /**
   * PAGINATED Internal merge size limit default in MB
   */
  public static final String PAGINATED_INTERNAL_MERGE_SIZE_LIMIT_DEFAULT = "10";
  /**
   * PAGINATED Internal merge size limit
   */
  public static final String PAGINATED_INTERNAL_FILE_ROW_LIMIT =
      "carbon.paginated.internal.file.row.limit";
  /**
   * PAGINATED Internal merge size limit default in MB
   */
  public static final String PAGINATED_INTERNAL_FILE_ROW_LIMIT_DEFAULT = "10000";
  /**
   * MAP
   */
  public static final String MAP = "map";
  /**
   * HEAP
   */
  public static final String HEAP = "heap";
  /**
   * DIMENSION_DEFAULT
   */
  public static final int DIMENSION_DEFAULT = 1;
  /**
   * MEASURE_SORT_FOLDER
   */
  public static final String MEASURE_SORT_FOLDER = "msrSort";
  /**
   * GRAPH_ROWSET_SIZE
   */
  public static final String GRAPH_ROWSET_SIZE = "carbon.graph.rowset.size";
  /**
   * GRAPH_ROWSET_SIZE_DEFAULT
   */
  public static final String GRAPH_ROWSET_SIZE_DEFAULT = "500";
  /**
   * Comment for <code>TYPE_MYSQL</code>
   */
  public static final String TYPE_MYSQL = "MYSQL";
  /**
   * Comment for <code>TYPE_MSSQL</code>
   */
  public static final String TYPE_MSSQL = "MSSQL";
  /**
   * Comment for <code>TYPE_ORACLE</code>
   */
  public static final String TYPE_ORACLE = "ORACLE";
  /**
   * Comment for <code>TYPE_SYBASE</code>
   */
  public static final String TYPE_SYBASE = "SYBASE";
  /**
   * SORT_INTERMEDIATE_FILES_LIMIT
   */
  public static final String SORT_INTERMEDIATE_FILES_LIMIT = "carbon.sort.intermediate.files.limit";
  /**
   * SORT_INTERMEDIATE_FILES_LIMIT_DEFAULT_VALUE
   */
  public static final String SORT_INTERMEDIATE_FILES_LIMIT_DEFAULT_VALUE = "10";
  /**
   * MERGERD_EXTENSION
   */
  public static final String MERGERD_EXTENSION = ".merge";
  /**
   * SORT_FILE_BUFFER_SIZE
   */
  public static final String SORT_FILE_BUFFER_SIZE = "carbon.sort.file.buffer.size";
  /**
   * SORT_FILE_BUFFER_SIZE_DEFAULT_VALUE
   */
  public static final String SORT_FILE_BUFFER_SIZE_DEFAULT_VALUE = "10";
  /**
   * DATA_LOAD_LOG_COUNTER_DEFAULT_COUNTER
   */
  public static final String DATA_LOAD_LOG_COUNTER_DEFAULT_COUNTER = "500000";
  /**
   * SORT_FILE_WRITE_BUFFER_SIZE
   */
  public static final String CARBON_SORT_FILE_WRITE_BUFFER_SIZE =
      "carbon.sort.file.write.buffer.size";
  /**
   * SORT_FILE_WRITE_BUFFER_SIZE_DEFAULT_VALUE
   */
  public static final String CARBON_SORT_FILE_WRITE_BUFFER_SIZE_DEFAULT_VALUE = "50000";
  /**
   * IS_DATA_LOAD_LOG_ENABLED
   */
  public static final String IS_DATA_LOAD_LOG_ENABLED = "carbon.dataload.log.enabled";
  /**
   * IS_DATA_LOAD_LOG_ENABLED_DEFAULTVALUE
   */
  public static final String IS_DATA_LOAD_LOG_ENABLED_DEFAULT_VALUE = "false";
  /**
   * NUMBER_OF_THERADS_FOR_INTERMEDIATE_MERGING
   */
  public static final String NUMBER_OF_THERADS_FOR_INTERMEDIATE_MERGING =
      "carbon.sort.intermedaite.number.of.threads";
  /**
   * NUMBER_OF_THERADS_FOR_INTERMEDIATE_MERGING_DEFAULT_VALUE
   */
  public static final String NUMBER_OF_THERADS_FOR_INTERMEDIATE_MERGING_DEFAULT_VALUE = "1";
  /**
   * WRITE_ALL_NODE_IN_SINGLE_TIME_DEFAULT_VALUE
   */
  public static final String WRITE_ALL_NODE_IN_SINGLE_TIME_DEFAULT_VALUE = "true";
  /**
   * Number of cores to be used while loading
   */
  public static final String NUM_CORES_LOADING = "carbon.number.of.cores.while.loading";
  /**
   * CSV_READ_BUFFER_SIZE
   */
  public static final String CSV_READ_BUFFER_SIZE = "carbon.csv.read.buffersize.byte";
  /**
   * CSV_READ_BUFFER_SIZE
   */
  public static final String CSV_READ_BUFFER_SIZE_DEFAULT = "50000";
  /**
   * CSV_READ_COPIES
   */
  public static final String DEFAULT_NUMBER_CORES = "2";
  /**
   * CSV_FILE_EXTENSION
   */
  public static final String CSV_FILE_EXTENSION = ".csv";
  /**
   * COLON_SPC_CHARACTER
   */
  public static final String COLON_SPC_CHARACTER = ":!@#COLON#@!:";
  /**
   * HASH_SPC_CHARATER
   */
  public static final String HASH_SPC_CHARACTER = "#!@:HASH:@!#";
  /**
   * SEMICOLON_SPC_CHARATER
   */
  public static final String SEMICOLON_SPC_CHARACTER = ";#!@:SEMIC:@!#;";
  /**
   * AMPERSAND_SPC_CHARATER
   */
  public static final String AMPERSAND_SPC_CHARACTER = "&#!@:AMPER:@!#&";
  /**
   * STAR_SPC_CHARATER
   */
  public static final String STAR_SPC_CHARACTER = "#&!@:STAR:@!&#";
  /**
   * ATTHERATE_SPC_CHARATER
   */
  public static final String ATTHERATE_SPC_CHARACTER = "@#!:ATTHER:!#@";
  /**
   * ATTHERATE_SPC_CHARATER
   */
  public static final String COMA_SPC_CHARACTER = ",#!:COMA:!#,";
  /**
   * HYPHEN_SPC_CHARACTER
   */
  public static final String HYPHEN_SPC_CHARACTER = "-#!:HYPHEN:!#-";
  /**
   * CARBON_DECIMAL_POINTERS
   */
  public static final String CARBON_DECIMAL_POINTERS = "carbon.decimal.pointers";
  /**
   * CARBON_DECIMAL_POINTERS_DEFAULT
   */
  public static final String CARBON_DECIMAL_POINTERS_DEFAULT = "5";
  /**
   * CARBON_DECIMAL_POINTERS_AGG
   */
  public static final String CARBON_DECIMAL_POINTERS_AGG = "carbon.decimal.pointers.agg";
  /**
   * CARBON_DECIMAL_POINTERS_AGG_DEFAULT
   */
  public static final String CARBON_DECIMAL_POINTERS_AGG_DEFAULT = "4";
  /**
   * CHECKPOINT
   */
  public static final String CHECKPOINT_EXT = ".checkpoint";
  /**
   * CHECKPOINT_FILE_NAME
   */
  public static final String CHECKPOINT_FILE_NAME = "checkpointfile_";
  /**
   * SORT_TEMP_FILE_EXT
   */
  public static final String SORT_TEMP_FILE_EXT = ".sorttemp";
  /**
   * CARBON_DATALOAD_CHECKPOINT
   */
  public static final String CARBON_DATALOAD_CHECKPOINT = "carbon.dataload.checkpoint";
  /**
   * CARBON_DATALOAD_CHECKPOINT_DEFAULTVALUE
   */
  public static final String CARBON_DATALOAD_CHECKPOINT_DEFAULTVALUE = "false";
  /**
   * BAK_EXT
   */
  public static final String BAK_EXT = ".bak";
  /**
   * DONE_EXT
   */
  public static final String DONE_EXT = ".done";
  /**
   * CARBON_CHECKPOINT_QUEUE_THRESHOLD
   */
  public static final String CARBON_CHECKPOINT_QUEUE_THRESHOLD =
      "carbon.checkpoint.queue.threshold";
  /**
   * CARBON_CHECKPOINT_CHUNK_SIZE
   */
  public static final String CARBON_CHECKPOINT_CHUNK_SIZE = "carbon.checkpoint.chunk.size";
  /**
   * CARBON_CHECKPOINT_QUEUE_INITIAL_CAPACITY
   */
  public static final String CARBON_CHECKPOINT_QUEUE_INITIAL_CAPACITY =
      "carbon.checkpoint.queue.initial.capacity";
  /**
   * CARBON_CHECKPOINT_TOCOPY_FROM_QUEUE
   */
  public static final String CARBON_CHECKPOINT_TOCOPY_FROM_QUEUE =
      "carbon.checkpoint.tocopy.from.queue";
  /**
   * CARBON_CHECKPOINT_QUEUE_THRESHOLD_DEFAULT_VAL
   */
  public static final String CARBON_CHECKPOINT_QUEUE_THRESHOLD_DEFAULT_VAL = "20";
  /**
   * CARBON_CHECKPOINT_CHUNK_SIZE_DEFAULT_VAL
   */
  public static final String CARBON_CHECKPOINT_CHUNK_SIZE_DEFAULT_VAL = "500";
  /**
   * CARBON_CHECKPOINT_QUEUE_INITIAL_CAPACITY_DEFAULT_VAL
   */
  public static final String CARBON_CHECKPOINT_QUEUE_INITIAL_CAPACITY_DEFAULT_VAL = "25";
  /**
   * CARBON_CHECKPOINT_TOCOPY_FROM_QUEUE_DEFAULT_VAL
   */
  public static final String CARBON_CHECKPOINT_TOCOPY_FROM_QUEUE_DEFAULT_VAL = "5";
  /**
   * IS_PRODUCERCONSUMER_BASED_SORTING
   */
  public static final String IS_PRODUCERCONSUMER_BASED_SORTING =
      "carbon.is.producer.consumer.based.sorting";
  /**
   * PRODUCERCONSUMER_BASED_SORTING_ENABLED
   */
  public static final String PRODUCERCONSUMER_BASED_SORTING_ENABLED_DEFAULTVALUE = "false";
  /**
   * BACKGROUND_MERGER_TASK_DEC
   */
  public static final String BACKGROUND_MERGER_TASK_DEC = "background merger task";
  /**
   * BACKGROUND_MERGER_TASK_ID
   */
  public static final String BACKGROUND_MERGER_TASK_ID = "BACKGROUNDMERGER";
  /**
   * BACKGROUND_MERGER_ENABLED
   */
  public static final String BACKGROUND_MERGER_TYPE = "carbon.background.merger.type";
  /**
   * BACKGROUND_MERGER_TIME_INTERVAL_IN_MINUTE
   */
  public static final String BACKGROUND_MERGER_TIME_INTERVAL_IN_MINUTE =
      "carbon.background.merger.time.interval";
  /**
   * BACKGROUND_MERGER_TIME_INTERVAL_IN_MINUTE_DEFAULT_VALUE
   */
  public static final String BACKGROUND_MERGER_TIME_INTERVAL_IN_MINUTE_DEFAULT_VALUE = "30";
  /**
   * CARBON_SEQ_GEN_INMEMORY_LRU_CACHE_SIZE
   */
  public static final String CARBON_SEQ_GEN_INMEMORY_LRU_CACHE_SIZE =
      "carbon.seqgen.inmemory.lru.cache.size";
  /**
   * CARBON_SEQ_GEN_INMEMORY_LRU_CACHE_SIZE_DEFAULT_VALUE
   */
  public static final String CARBON_SEQ_GEN_INMEMORY_LRU_CACHE_SIZE_DEFAULT_VALUE = "4";
  /**
   * CARBON_SEQ_GEN_INMEMORY_LRU_CACHE_ENABLED
   */
  public static final String CARBON_SEQ_GEN_INMEMORY_LRU_CACHE_ENABLED =
      "carbon.seqgen.inmemory.lru.cache.enabled";
  /**
   * CARBON_SEQ_GEN_INMEMORY_LRU_CACHE_ENABLED_DEFAULT_VALUE
   */
  public static final String CARBON_SEQ_GEN_INMEMORY_LRU_CACHE_ENABLED_DEFAULT_VALUE = "false";
  /**
   * CARBON_SEQ_GEN_INMEMORY_LRU_CACHE_FLUSH_INTERVAL_INHOUR
   */
  public static final String CARBON_SEQ_GEN_INMEMORY_LRU_CACHE_FLUSH_INTERVAL_INHOUR =
      "carbon.seqgen.inmemory.lru.cache.flush.interval.in.minute";
  /**
   * CARBON_SEQ_GEN_INMEMORY_LRU_CACHE_FLUSH_INTERVAL_INHOUR_DEFAULTVALUE
   */
  public static final String CARBON_SEQ_GEN_INMEMORY_LRU_CACHE_FLUSH_INTERVAL_INHOUR_DEFAULTVALUE =
      "300";
  /**
   * CARBON_MAX_THREAD_FOR_SORTING
   */
  public static final String CARBON_MAX_THREAD_FOR_SORTING = "carbon.max.thread.for.sorting";
  /**
   * CARBON_MAX_THREAD_FOR_SORTING
   */
  public static final String CARBON_MAX_THREAD_FOR_SORTING_DEFAULTVALUE = "2";
  /**
   * CARBON_AUTOAGGREGATION_TYPE
   */
  public static final String CARBON_AUTOAGGREGATION_TYPE = "carbon.autoaggregation.type";
  /**
   * CARBON_AUTOAGGREGATION_TYPE_MANUALVALUE
   */
  public static final String CARBON_MANUAL_TYPE_VALUE = "MANUAL";
  /**
   * CARBON_AUTOAGGREGATION_TYPE_AUTOVALUE
   */
  public static final String CARBON_AUTO_TYPE_VALUE = "AUTO";
  /**
   * CARBON_AUTOAGGREGATION_NO_OF_PARALLEL_AGGREGATETABLE_GENERATION
   */
  public static final String CARBON_AUTOAGGREGATION_NO_OF_PARALLEL_AGGREGATETABLE_GENERATION =
      "carbon.autoaggregation.no.of.parallel.aggregatetable.generation";
  /**
   * CARBON_AUTOAGGREGATION_NO_OF_PARALLEL_AGGREGATETABLE_GENERATION_DEFAULT
   */
  public static final String
      CARBON_AUTOAGGREGATION_NO_OF_PARALLEL_AGGREGATETABLE_GENERATION_DEFAULT = "1";
  /**
   * CARBON_AUTO_AGG_CONST
   */
  public static final String CARBON_AUTO_AGG_CONST = "AutoAgg";
  /**
   * CARBON_MANUAL_AGG_CONST
   */
  public static final String CARBON_MANUAL_AGG_CONST = "ManualAgg";
  /**
   * CARBON_SEND_LOAD_SIGNAL_TO_ENGINE
   */
  public static final String CARBON_SEND_LOAD_SIGNAL_TO_ENGINE =
      "carbon.send.load.signal.to.engine";
  /**
   * CARBON_SEND_LOAD_SIGNAL_TO_ENGINE_DEFAULTVALUE
   */
  public static final String CARBON_SEND_LOAD_SIGNAL_TO_ENGINE_DEFAULTVALUE = "true";
  /**
   * IS_SORT_TEMP_FILE_COMPRESSION_ENABLED
   */
  public static final String IS_SORT_TEMP_FILE_COMPRESSION_ENABLED =
      "carbon.is.sort.temp.file.compression.enabled";
  /**
   * IS_SORT_TEMP_FILE_COMPRESSION_ENABLED_DEFAULTVALUE
   */
  public static final String IS_SORT_TEMP_FILE_COMPRESSION_ENABLED_DEFAULTVALUE = "false";
  /**
   * SORT_TEMP_FILE_NO_OF_RECORDS_FOR_COMPRESSION
   */
  public static final String SORT_TEMP_FILE_NO_OF_RECORDS_FOR_COMPRESSION =
      "carbon.sort.temp.file.no.of.records.for.compression";
  /**
   * SORT_TEMP_FILE_NO_OF_RECORD_FOR_COMPRESSION_DEFAULTVALUE
   */
  public static final String SORT_TEMP_FILE_NO_OF_RECORD_FOR_COMPRESSION_DEFAULTVALUE = "50";
  /**
   * CARBON_BACKGROUND_MERGER_SIZE_IN_GB
   */
  public static final String CARBON_BACKGROUND_MERGER_FILE_SIZE =
      "carbon.background.merge.file.size";
  /**
   * DEFAULT_COLLECTION_SIZE
   */
  public static final int DEFAULT_COLLECTION_SIZE = 16;
  /**
   * CARBON_DATALOAD_VALID_CSVFILE_SIZE
   */
  public static final String CARBON_DATALOAD_VALID_CSVFILE_SIZE =
      "carbon.dataload.valid.csvfile.size(in GB)";
  /**
   * CARBON_DATALOAD_VALID_CSVFILE_SIZE_DEFAULTVALUE
   */
  public static final String CARBON_DATALOAD_VALID_CSVFILE_SIZE_DEFAULTVALUE = "5";
  /**
   * CARBON_TIMESTAMP_DEFAULT_FORMAT
   */
  public static final String CARBON_TIMESTAMP_DEFAULT_FORMAT = "yyyy-MM-dd HH:mm:ss";
  /**
   * CARBON_TIMESTAMP_DEFAULT_FORMAT
   */
  public static final String CARBON_TIMESTAMP_FORMAT = "carbon.timestamp.format";
  /**
   * CARBON_DATALOAD_VALID_CSVFILE_SIZE
   */
  public static final String CARBON_DATALOAD_VALID_NUMBAER_OF_CSVFILE =
      "carbon.dataload.csv.filecount";
  /**
   * CARBON_DATALOAD_VALID_CSVFILE_SIZE_DEFAULTVALUE
   */
  public static final String CARBON_DATALOAD_VALID_NUMBAER_OF_CSVFILE_DEFAULTVALUE = "100";
  /**
   * CARBON_IS_GROUPBY_IN_SORT_DEFAULTVALUE
   */
  public static final String CARBON_IS_GROUPBY_IN_SORT_DEFAULTVALUE = "false";
  /**
   * CARBON_IS_LOAD_FACT_TABLE_IN_MEMORY
   */
  public static final String CARBON_IS_LOAD_FACT_TABLE_IN_MEMORY =
      "carbon.is.load.facttable.in.memory";
  /**
   * CARBON_IS_LOAD_FACT_TABLE_IN_MEMORY_DEFAULTVALUE
   */
  public static final String CARBON_IS_LOAD_FACT_TABLE_IN_MEMORY_DEFAULTVALUE = "true";
  /**
   * CARBON_READ_ONLY_UNPROCESSED_LOAD_FOLER
   */
  public static final String CARBON_READ_ONLY_UNPROCESSED_LOAD_FOLDER =
      "carbon.read.only.unprocessed.load.folder";
  /**
   * CARBON_READ_ONLY_UNPROCESSED_LOAD_FOLDER_DEFAULT_VALUE
   */
  public static final String CARBON_READ_ONLY_UNPROCESSED_LOAD_FOLDER_DEFAULT_VALUE = "false";
  /**
   * CARBON_READ_ONLY_UNPROCESSED_LOAD_FOLER
   */
  public static final String CARBON_PARALLEL_AGGREGATE_CREATION_FROM_FACT =
      "carbon.parallel.aggregate.creation.from.fact";
  /**
   * CARBON_READ_ONLY_UNPROCESSED_LOAD_FOLDER_DEFAULT_VALUE
   */
  public static final String CARBON_PARALLEL_AGGREGATE_CREATION_FROM_FACT_DEFAULT_VALUE = "false";
  /**
   * IS_PRODUCERCONSUMER_BASED_SORTING
   */
  public static final String IS_PRODUCERCONSUMER_BASED_SORTING_IN_AGG =
      "carbon.is.producer.consumer.based.sorting.in.agg";
  /**
   * PRODUCERCONSUMER_BASED_SORTING_ENABLED
   */
  public static final String PRODUCERCONSUMER_BASED_SORTING_ENABLED_IN_AGG_DEFAULTVALUE = "false";
  /**
   * lockObject
   */
  public static final Object LOCKOBJECT = new Object();
  /**
   * MEMBERUPDATE_FOLDERNAME
   */
  public static final String MEMBERUPDATE_FOLDERNAME = "memberUpdate";
  /**
   * SPARK_URL
   */
  public static final String SPARK_URL = "spark.url";
  /**
   * SPARK_HOME
   */
  public static final String SPARK_HOME = "spark.home";
  /**
   * SPARK_STORE_LOCATION
   */
  public static final String SPARK_STORE_LOCATION = "spark.carbon.storelocation";
  /**
   * STORE_LOCATION_HDFS
   */
  public static final String STORE_LOCATION_HDFS = "carbon.storelocation.hdfs";
  /**
   * STORE_LOCATION_TEMP
   */
  public static final String STORE_LOCATION_TEMP = "carbon.storelocation.temp";
  /**
   * STORE_LOCATION_TEMP_PATH
   */
  public static final String STORE_LOCATION_TEMP_PATH = "carbon.tempstore.location";
  /**
   * DATALOAD_KETTLE_PATH
   */
  public static final String DATALOAD_KETTLE_PATH = "carbon.dataload.kettleplugins.path";
  /**
   * IS_COLUMNAR_STORAGE
   */
  public static final String IS_COLUMNAR_STORAGE = "carbon.is.columnar.storage";
  /**
   * IS_COLUMNAR_STORAGE_DEFAULTVALUE
   */
  public static final String IS_COLUMNAR_STORAGE_DEFAULTVALUE = "true";
  /**
   * DIMENSION_SPLIT_VALUE_IN_COLUMNAR
   */
  public static final String DIMENSION_SPLIT_VALUE_IN_COLUMNAR =
      "carbon.dimension.split.value.in.columnar";
  /**
   * DIMENSION_SPLIT_VALUE_IN_COLUMNAR_DEFAULTVALUE
   */
  public static final String DIMENSION_SPLIT_VALUE_IN_COLUMNAR_DEFAULTVALUE = "1";
  /**
   * MEMBERUPDATE_FILEEXT
   */
  public static final String MEMBERUPDATE_FILEEXT = ".memberUpdate";
  /**
   * IS_FULLY_FILLED_BITS
   */
  public static final String IS_FULLY_FILLED_BITS = "carbon.is.fullyfilled.bits";
  /**
   * IS_FULLY_FILLED_BITS_DEFAULT_VALUE
   */
  public static final String IS_FULLY_FILLED_BITS_DEFAULT_VALUE = "true";
  /**
   * IS_INT_BASED_INDEXER
   */
  public static final String IS_INT_BASED_INDEXER = "is.int.based.indexer";
  /**
   * IS_INT_BASED_INDEXER_DEFAULTVALUE
   */
  public static final String IS_INT_BASED_INDEXER_DEFAULTVALUE = "true";
  /**
   * IS_INT_BASED_INDEXER
   */
  public static final String AGGREAGATE_COLUMNAR_KEY_BLOCK = "aggregate.columnar.keyblock";
  /**
   * IS_INT_BASED_INDEXER_DEFAULTVALUE
   */
  public static final String AGGREAGATE_COLUMNAR_KEY_BLOCK_DEFAULTVALUE = "true";
  /**
   * IS_INT_BASED_INDEXER
   */
  public static final String HIGH_CARDINALITY_VALUE = "high.cardinality.value";
  /**
   * IS_INT_BASED_INDEXER_DEFAULTVALUE
   */
  public static final String HIGH_CARDINALITY_VALUE_DEFAULTVALUE = "100000";
  /**
   * IS_COMPRESSED_KEYBLOCK
   */
  public static final String IS_COMPRESSED_KEYBLOCK = "is.compressed.keyblock";
  /**
   * IS_COMPRESSED_KEYBLOCK
   */
  public static final String IS_COMPRESSED_KEYBLOCK_DEFAULTVALUE = "false";
  /**
   * CARBON_USE_HASHBASED_AGG_INSORT
   */
  public static final String CARBON_USE_HASHBASED_AGG_INSORT = "carbon.use.hashbased.agg.insort";
  /**
   * CARBON_USE_HASHBASED_AGG_INSORT_DEFAULTVALUE
   */
  public static final String CARBON_USE_HASHBASED_AGG_INSORT_DEFAULTVALUE = "false";
  public static final String SQLNULLVALUE = "#null";
  /**
   * CONSTANT_SIZE_TEN
   */
  public static final int CONSTANT_SIZE_TEN = 10;
  /**
   * LEVEL_METADATA_FILE
   */
  public static final String LEVEL_METADATA_FILE = "levelmetadata_";
  public static final String ENABLE_BASE64_ENCODING = "enable.base64.encoding";
  public static final String ENABLE_BASE64_ENCODING_DEFAULT = "false";
  /**
   * LOAD_STATUS SUCCESS
   */
  public static final String STORE_LOADSTATUS_SUCCESS = "Success";
  /**
   * LOAD_STATUS FAILURE
   */
  public static final String STORE_LOADSTATUS_FAILURE = "Failure";
  /**
   * LOAD_STATUS PARTIAL_SUCCESS
   */
  public static final String STORE_LOADSTATUS_PARTIAL_SUCCESS = "Partial Success";
  /**
   * LOAD_STATUS
   */
  public static final String CARBON_METADATA_EXTENSION = ".metadata";
  /**
   * LOAD_STATUS
   */
  public static final String CARBON_DEFAULT_STREAM_ENCODEFORMAT = "UTF-8";
  /**
   * AGGREGATE_TABLE_START_TAG
   */
  public static final String AGGREGATE_TABLE_START_TAG = "agg";
  /**
   * UNDERSCORE
   */
  public static final String UNDERSCORE = "_";
  /**
   * File separator
   */
  public static final String FILE_SEPARATOR = "/";
  /**
   * MAX_QUERY_EXECUTION_TIME
   */
  public static final String MAX_QUERY_EXECUTION_TIME = "max.query.execution.time";
  /**
   * CARBON_TIMESTAMP
   */
  public static final String CARBON_TIMESTAMP = "dd-MM-yyyy HH:mm:ss";
  /**
   * METADATA_LOCK
   */
  public static final String METADATA_LOCK = "meta.lock";
  /**
   * NUMBER_OF_TRIES_FOR_LOAD_METADATA_LOCK
   */
  public static final int NUMBER_OF_TRIES_FOR_LOAD_METADATA_LOCK_DEFAULT = 3;
  /**
   * MAX_TIMEOUT_FOR_LOAD_METADATA_LOCK
   */
  public static final int MAX_TIMEOUT_FOR_LOAD_METADATA_LOCK_DEFAULT = 5;
  /**
   * NUMBER_OF_TRIES_FOR_LOAD_METADATA_LOCK
   */
  public static final String NUMBER_OF_TRIES_FOR_LOAD_METADATA_LOCK =
      "carbon.load.metadata.lock.retries";
  /**
   * MAX_TIMEOUT_FOR_LOAD_METADATA_LOCK
   */
  public static final String MAX_TIMEOUT_FOR_LOAD_METADATA_LOCK =
      "carbon.load.metadata.lock.retry.timeout.sec";
  /**
   * MARKED_FOR_DELETION
   */
  public static final String MARKED_FOR_DELETE = "Marked for Delete";
  public static final String MARKED_FOR_UPDATE = "Marked for Update";
  /**
   * Load version in the build
   * This version no is saved in the .version file in each Load folder.
   */
  public static final String DATA_VERSION = "data.version";
  /**
   * Cube version in the build
   * This contastant will be
   * public static final String CUBE_VERSION="metadata.version";
   */

  public static final String STRING_TYPE = "StringType";
  public static final String INTEGER_TYPE = "IntegerType";
  public static final String LONG_TYPE = "LongType";
  public static final String DOUBLE_TYPE = "DoubleType";
  public static final String FLOAT_TYPE = "FloatType";
  public static final String DATE_TYPE = "DateType";
  public static final String BOOLEAN_TYPE = "BooleanType";
  public static final String TIMESTAMP_TYPE = "TimestampType";
  public static final String ARRAY_TYPE = "ArrayType";
  public static final String STRUCT_TYPE = "StructType";
  public static final String BYTE_TYPE = "ByteType";
  public static final String SHORT_TYPE = "ShortType";
  public static final String BINARY_TYPE = "BinaryType";
  public static final String DECIMAL_TYPE = "DecimalType";
  public static final String NULL_TYPE = "NullType";
  public static final String STRING = "String";
  public static final String COLUMNAR = "columnar";

  public static final String INTEGER = "Integer";
  public static final String NUMERIC = "Numeric";
  public static final String TIMESTAMP = "Timestamp";
  public static final String ARRAY = "ARRAY";
  public static final String STRUCT = "STRUCT";
  public static final String INCLUDE = "include";
  public static final String FROM = "from";
  public static final String WITH = "with";
  /**
   * FACT_UPDATE_EXTENSION.
   */
  public static final String FACT_UPDATE_EXTENSION = ".carbondata_update";
  public static final String FACT_DELETE_EXTENSION = "_delete";
  /**
   * MARKED_FOR_UPDATION
   */
  public static final String FACT_FILE_UPDATED = "update";
  /**
   * MAX_QUERY_EXECUTION_TIME
   */
  public static final int DEFAULT_MAX_QUERY_EXECUTION_TIME = 60;
  /**
   * LOADMETADATA_FILENAME
   */
  public static final String LOADMETADATA_FILENAME = "tablestatus";
  /**
   * LOAD_LOCK
   */
  public static final String LOAD_LOCK = "load.lock";
  public static final String SUM_DISTINCT = "sum-distinct";
  /**
   * INMEMORY_REOCRD_SIZE
   */
  public static final String INMEMORY_REOCRD_SIZE = "carbon.inmemory.record.size";
  public static final int INMEMORY_REOCRD_SIZE_DEFAULT = 240000;
  /**
   * SPILL_OVER_DISK_PATH
   */
  public static final String SPILL_OVER_DISK_PATH = "Temp/paginationcache/";
  /**
   * SPILL_OVER_DISK_PATH
   */
  public static final String SCHEMAS_MODIFIED_TIME_FILE = "modifiedTime.mdt";
  public static final String DEFAULT_INVISIBLE_DUMMY_MEASURE = "default_dummy_measure";
  public static final String IS_FORCED_IN_MEMORY_CUBE = "carbon.forced.in.memory.cube";
  public static final String IS_FORCED_IN_MEMORY_CUBE_DEFAULT_VALUE = "false";
  /**
   * UPDATING_METADATA
   */
  public static final String UPDATING_METADATA = ".tmp";
  /**
   * LOADCUBE_STARTUP.
   */
  public static final String LOADCUBE_STARTUP = "carbon.is.loadcube.startup";
  /**
   * LOADCUBE_DATALOAD.
   */
  public static final String LOADCUBE_DATALOAD = "carbon.is.loadcube.dataload";
  /**
   * sort index file extension
   */
  public static final String LEVEL_SORT_INDEX_FILE_EXT = ".sortindex";
  /**
   * LEVEL_ARRAY_SIZE
   */
  public static final int LEVEL_ARRAY_SIZE = 10000;
  /**
   * max level cache size upto which level cache will be loaded in memory
   */
  public static final String CARBON_MAX_LEVEL_CACHE_SIZE = "carbon.max.level.cache.size";
  /**
   * max level cache size default value in GB
   */
  public static final String CARBON_MAX_LEVEL_CACHE_SIZE_DEFAULT = "-1";
  /**
   * max retry count to acquire a cube for querying or loading a level file in
   * memory
   */
  public static final int MAX_RETRY_COUNT = 5;
  /**
   * retry interval after which loading a level file will be retried
   */
  public static final String CARBON_LOAD_LEVEL_RETRY_INTERVAL = "Carbon.load.level.retry.interval";
  /**
   * retry interval default value
   */
  public static final String CARBON_LOAD_LEVEL_RETRY_INTERVAL_DEFAULT = "12";
  /**
   * DOUBLE_VALUE_MEASURE
   */
  public static final char SUM_COUNT_VALUE_MEASURE = 'n';
  /**
   * BYTE_VALUE_MEASURE
   */
  public static final char BYTE_VALUE_MEASURE = 'c';
  /**
   * BIG_DECIMAL_MEASURE
   */
  public static final char BIG_DECIMAL_MEASURE = 'b';

  /**
   * BIG_INT_MEASURE
   */
  public static final char BIG_INT_MEASURE = 'l';

  /**
   * MEASURE_NOT_NULL_VALUE
   */

  public static final byte MEASURE_NOT_NULL_VALUE = 1;
  /**
   * MEASURE_NULL_VALUE
   */
  public static final byte MEASURE_NULL_VALUE = 0;
  /**
   * short required to store the length of following byte array(high card dim)
   */
  public static final int BYTESREQTOSTORELENGTH = 2;
  /**
   * This determines the size of array to be processed in data load steps. one
   * for dimensions , one of ignore dictionary dimensions , one for measures.
   */
  public static final int ARRAYSIZE = 3;
  public static final String CARBON_UNIFIED_STORE_PATH = "carbon.unified.store.path";
  public static final String CARBON_UNIFIED_STORE_PATH_DEFAULT = "false";
  /**
   * Carbon enable quick filter to enable quick filter
   */
  public static final String CARBON_ENABLE_QUICK_FILTER = "carbon.enable.quick.filter";
  /**
   * CARBON_PREFETCH_BUFFERSIZE
   */
  public static final int CARBON_PREFETCH_BUFFERSIZE = 20000;
  /**
   * CARBON_PREFETCH_IN_MERGE
   */
  public static final boolean CARBON_PREFETCH_IN_MERGE_VALUE = false;
  /**
   * TEMPWRITEFILEEXTENSION
   */
  public static final String TEMPWRITEFILEEXTENSION = ".write";
  /**
   * MERGE_THRESHOLD_VALUE
   */
  public static final String MERGE_THRESHOLD_VALUE = "carbon.merge.threshold";
  /**
   * MERGE_THRESHOLD_DEFAULT_VAL
   */
  public static final String MERGE_THRESHOLD_DEFAULT_VAL = "10";
  /**
   * MERGE_FACTSIZE_THRESHOLD_VALUE
   */
  public static final String MERGE_FACTSIZE_THRESHOLD_VALUE = "carbon.merge.factsize.threshold";
  /**
   * MERGE_FACTSIZE_THRESHOLD_DEFAULT_VAL
   */
  public static final String MERGE_FACTSIZE_THRESHOLD_DEFAULT_VAL = "10";
  /**
   * MARKED_FOR_MERGE
   */
  public static final String MARKED_FOR_MERGE = "Marked For Merge";
  /**
   * TO_LOAD_MERGE_MAX_SIZE
   */
  public static final String TO_LOAD_MERGE_MAX_SIZE = "to.merge.load.max.size";
  /**
   * TO_LOAD_MERGE_MAX_SIZE_DEFAULT
   */
  public static final String TO_LOAD_MERGE_MAX_SIZE_DEFAULT = "1";
  /**
   * ENABLE_LOAD_MERGE
   */
  public static final String ENABLE_LOAD_MERGE = "carbon.enable.load.merge";
  /**
   * DEFAULT_ENABLE_LOAD_MERGE
   */
  public static final String DEFAULT_ENABLE_LOAD_MERGE = "false";

  /**
   * ZOOKEEPER_ENABLE_LOCK if this is set to true then zookeeper will be used to handle locking
   * mechanism of carbon
   */
  public static final String LOCK_TYPE = "carbon.lock.type";

  /**
   * ZOOKEEPER_ENABLE_DEFAULT the default value for zookeeper will be true for carbon
   */
  public static final String LOCK_TYPE_DEFAULT = "LOCALLOCK";

  /**
   * ZOOKEEPER_LOCATION this is the location in zookeeper file system where locks are created.
   * mechanism of carbon
   */
  public static final String ZOOKEEPER_LOCATION = "/CarbonLocks";

  /**
   * shared directory folder name
   */
  public static final String SHARED_DIRECTORY = "shared_dictionary";

  /**
   * metadata constant
   */
  public static final String METADATA_CONSTANT = "_metadata";

  /**
   * dictionary folder name
   */
  public static final String DICTIONARY_CONSTANT = "dictionary";

  /**
   * folder extension
   */
  public static final String FILE_EXTENSION = ".file";

  /**
   * File separator charcater
   */
  public static final char FILE_SEPARATOR_CHAR = '/';

  /**
   * maximum dictionary chunk size that can be kept in memory while writing dictionary file
   */
  public static final String DICTIONARY_ONE_CHUNK_SIZE = "carbon.dictionary.chunk.size";

  /**
   * dictionary chunk default size
   */
  public static final String DICTIONARY_ONE_CHUNK_SIZE_DEFAULT = "10000";

  /**
   * xxhash algorithm property for hashmap
   */
  public static final String ENABLE_XXHASH = "carbon.enableXXHash";

  /**
   * xxhash algorithm property for hashmap. Default value false
   */
  public static final String ENABLE_XXHASH_DEFAULT = "false";

  /**
   * default charset to be used for reading and writing
   */
  public static final String DEFAULT_CHARSET = "UTF-8";

  /**
   * surrogate key that will be sent whenever in the dictionary chunks
   * a valid surrogate key is not found for a given dictionary value
   */
  public static final int INVALID_SURROGATE_KEY = -1;

  /**
   * table split partition
   */
  public static final String TABLE_SPLIT_PARTITION = "carbon.table.split.partition.enable";

  /**
   * table split partition default value
   */
  public static final String TABLE_SPLIT_PARTITION_DEFAULT_VALUE = "false";

  public static final int INVALID_SEGMENT_ID = -1;

  /**
   * space reserved for writing block meta data in carbon data file
   */
  public static final String CARBON_BLOCK_META_RESERVED_SPACE =
      "carbon.block.meta.size.reserved.percentage";

  /**
   * default value for space reserved for writing block meta data in carbon data file
   */
  public static final String CARBON_BLOCK_META_RESERVED_SPACE_DEFAULT = "10";

  /**
   * property to enable min max during filter query
   */
  public static final String CARBON_QUERY_MIN_MAX_ENABLED = "carbon.enableMinMax";

  /**
   * default value to enable min or max during filter query execution
   */
  public static final String MIN_MAX_DEFAULT_VALUE = "true";

  private CarbonCommonConstants() {

  }
}


