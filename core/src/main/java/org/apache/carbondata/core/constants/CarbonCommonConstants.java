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

package org.apache.carbondata.core.constants;

public interface CarbonCommonConstants {
  /**
   * integer size in bytes
   */
  int INT_SIZE_IN_BYTE = 4;
  /**
   * short size in bytes
   */
  int SHORT_SIZE_IN_BYTE = 2;
  /**
   * DOUBLE size in bytes
   */
  int DOUBLE_SIZE_IN_BYTE = 8;
  /**
   * LONG size in bytes
   */
  int LONG_SIZE_IN_BYTE = 8;
  /**
   * byte to KB conversion factor
   */
  int BYTE_TO_KB_CONVERSION_FACTOR = 1024;
  /**
   * BYTE_ENCODING
   */
  String BYTE_ENCODING = "ISO-8859-1";
  /**
   * measure meta data file name
   */
  String MEASURE_METADATA_FILE_NAME = "/msrMetaData_";
  /**
   * location of the carbon member, hierarchy and fact files
   */
  String STORE_LOCATION = "carbon.storelocation";
  /**
   * blocklet size in carbon file
   */
  String BLOCKLET_SIZE = "carbon.blocklet.size";
  /**
   * Number of cores to be used
   */
  String NUM_CORES = "carbon.number.of.cores";
  /**
   * carbon sort size
   */
  String SORT_SIZE = "carbon.sort.size";
  /**
   * default location of the carbon member, hierarchy and fact files
   */
  String STORE_LOCATION_DEFAULT_VAL = "../carbon.store";
  /**
   * the folder name of kettle home path
   */
  String KETTLE_HOME_NAME = "carbonplugins";
  /**
   * CARDINALITY_INCREMENT_DEFAULT_VALUE
   */
  int CARDINALITY_INCREMENT_VALUE_DEFAULT_VAL = 10;
  /**
   * default blocklet size
   */
  String BLOCKLET_SIZE_DEFAULT_VAL = "120000";
  /**
   * min blocklet size
   */
  int BLOCKLET_SIZE_MIN_VAL = 50;
  /**
   * max blocklet size
   */
  int BLOCKLET_SIZE_MAX_VAL = 12000000;

  String BLOCK_SIZE_DEFAULT_VAL = "1024";

  int BLOCK_SIZE_MIN_VAL = 1;

  int BLOCK_SIZE_MAX_VAL = 2048;
  /**
   * default value of number of cores to be used
   */
  String NUM_CORES_DEFAULT_VAL = "2";
  /**
   * min value of number of cores to be used
   */
  int NUM_CORES_MIN_VAL = 1;
  /**
   * max value of number of cores to be used
   */
  int NUM_CORES_MAX_VAL = 32;
  /**
   * default carbon sort size
   */
  String SORT_SIZE_DEFAULT_VAL = "100000";
  /**
   * min carbon sort size
   */
  int SORT_SIZE_MIN_VAL = 1000;
  /**
   * carbon properties file path
   */
  String CARBON_PROPERTIES_FILE_PATH = "../../../conf/carbon.properties";
  /**
   * CARBON_DDL_BASE_HDFS_URL
   */
  String CARBON_DDL_BASE_HDFS_URL = "carbon.ddl.base.hdfs.url";
  /**
   * Load Folder Name
   */
  String LOAD_FOLDER = "Segment_";
  /**
   * HDFSURL_PREFIX
   */
  String HDFSURL_PREFIX = "hdfs://";
  /**
   * FS_DEFAULT_FS
   */
  String FS_DEFAULT_FS = "fs.defaultFS";
  /**
   * BYTEBUFFER_SIZE
   */

  int BYTEBUFFER_SIZE = 24 * 1024;
  /**
   * Average constant
   */
  String AVERAGE = "avg";
  /**
   * Count constant
   */
  String COUNT = "count";
  /**
   * SUM
   */
  String SUM = "sum";
  /**
   * DUMMY aggregation function
   */
  String DUMMY = "dummy";
  /**
   * MEMBER_DEFAULT_VAL
   */
  String MEMBER_DEFAULT_VAL = "@NU#LL$!";
  /**
   * FILE STATUS IN-PROGRESS
   */
  String FILE_INPROGRESS_STATUS = ".inprogress";
  /**
   * CARBON_BADRECORDS_LOCATION
   */
  String CARBON_BADRECORDS_LOC = "carbon.badRecords.location";
  /**
   * CARBON_BADRECORDS_LOCATION_DEFAULT
   */
  String CARBON_BADRECORDS_LOC_DEFAULT_VAL =
          "../unibi-solutions/system/carbon/badRecords";
  /**
   * HIERARCHY_FILE_EXTENSION
   */
  String HIERARCHY_FILE_EXTENSION = ".hierarchy";
  /**
   * SORT_TEMP_FILE_LOCATION
   */
  String SORT_TEMP_FILE_LOCATION = "sortrowtmp";
  /**
   * CARBON_RESULT_SIZE_DEFAULT
   */
  String LEVEL_FILE_EXTENSION = ".level";
  /**
   * FACT_FILE_EXT
   */
  String FACT_FILE_EXT = ".carbondata";
  /**
   * MEASUREMETADATA_FILE_EXT
   */
  String MEASUREMETADATA_FILE_EXT = ".msrmetadata";
  /**
   * GRAPH_ROWSET_SIZE
   */
  String GRAPH_ROWSET_SIZE = "carbon.graph.rowset.size";
  /**
   * GRAPH_ROWSET_SIZE_DEFAULT
   */
  String GRAPH_ROWSET_SIZE_DEFAULT = "500";
  /**
   * Comment for <code>TYPE_MYSQL</code>
   */
  String TYPE_MYSQL = "MYSQL";
  /**
   * Comment for <code>TYPE_MSSQL</code>
   */
  String TYPE_MSSQL = "MSSQL";
  /**
   * Comment for <code>TYPE_ORACLE</code>
   */
  String TYPE_ORACLE = "ORACLE";
  /**
   * Comment for <code>TYPE_SYBASE</code>
   */
  String TYPE_SYBASE = "SYBASE";
  /**
   * SORT_INTERMEDIATE_FILES_LIMIT
   */
  String SORT_INTERMEDIATE_FILES_LIMIT = "carbon.sort.intermediate.files.limit";
  /**
   * SORT_INTERMEDIATE_FILES_LIMIT_DEFAULT_VALUE
   */
  String SORT_INTERMEDIATE_FILES_LIMIT_DEFAULT_VALUE = "20";
  /**
   * MERGERD_EXTENSION
   */
  String MERGERD_EXTENSION = ".merge";
  /**
   * SORT_FILE_BUFFER_SIZE
   */
  String SORT_FILE_BUFFER_SIZE = "carbon.sort.file.buffer.size";
  /**
   * no.of records after which counter to be printed
   */
  String DATA_LOAD_LOG_COUNTER = "carbon.load.log.counter";
  /**
   * DATA_LOAD_LOG_COUNTER_DEFAULT_COUNTER
   */
  String DATA_LOAD_LOG_COUNTER_DEFAULT_COUNTER = "500000";
  /**
   * SORT_FILE_WRITE_BUFFER_SIZE
   */
  String CARBON_SORT_FILE_WRITE_BUFFER_SIZE =
          "carbon.sort.file.write.buffer.size";
  /**
   * SORT_FILE_WRITE_BUFFER_SIZE_DEFAULT_VALUE
   */
  String CARBON_SORT_FILE_WRITE_BUFFER_SIZE_DEFAULT_VALUE = "50000";
  /**
   * Number of cores to be used while loading
   */
  String NUM_CORES_LOADING = "carbon.number.of.cores.while.loading";
  /**
   * Number of cores to be used while compacting
   */
  String NUM_CORES_COMPACTING = "carbon.number.of.cores.while.compacting";
  /**
   * Number of cores to be used for block sort
   */
  String NUM_CORES_BLOCK_SORT = "carbon.number.of.cores.block.sort";
  /**
   * Default value of number of cores to be used for block sort
   */
  String NUM_CORES_BLOCK_SORT_DEFAULT_VAL = "7";
  /**
   * Max value of number of cores to be used for block sort
   */
  int NUM_CORES_BLOCK_SORT_MAX_VAL = 12;
  /**
   * Min value of number of cores to be used for block sort
   */
  int NUM_CORES_BLOCK_SORT_MIN_VAL = 1;
  /**
   * CSV_READ_BUFFER_SIZE
   */
  String CSV_READ_BUFFER_SIZE = "carbon.csv.read.buffersize.byte";
  /**
   * CSV_READ_BUFFER_SIZE
   */
  String CSV_READ_BUFFER_SIZE_DEFAULT = "50000";
  /**
   * CSV_READ_COPIES
   */
  String DEFAULT_NUMBER_CORES = "2";
  /**
   * CSV_FILE_EXTENSION
   */
  String CSV_FILE_EXTENSION = ".csv";

  /**
   * LOG_FILE_EXTENSION
   */
  String LOG_FILE_EXTENSION = ".log";

  /**
   * COLON_SPC_CHARACTER
   */
  String COLON_SPC_CHARACTER = ":!@#COLON#@!:";
  /**
   * HASH_SPC_CHARATER
   */
  String HASH_SPC_CHARACTER = "#!@:HASH:@!#";
  /**
   * SEMICOLON_SPC_CHARATER
   */
  String SEMICOLON_SPC_CHARACTER = ";#!@:SEMIC:@!#;";
  /**
   * AMPERSAND_SPC_CHARATER
   */
  String AMPERSAND_SPC_CHARACTER = "&#!@:AMPER:@!#&";
  /**
   * ATTHERATE_SPC_CHARATER
   */
  String COMA_SPC_CHARACTER = ",#!:COMA:!#,";
  /**
   * HYPHEN_SPC_CHARACTER
   */
  String HYPHEN_SPC_CHARACTER = "-#!:HYPHEN:!#-";
  /**
   * SORT_TEMP_FILE_EXT
   */
  String SORT_TEMP_FILE_EXT = ".sorttemp";
  /**
   * CARBON_MERGE_SORT_READER_THREAD
   */
  String CARBON_MERGE_SORT_READER_THREAD = "carbon.merge.sort.reader.thread";
  /**
   * CARBON_MERGE_SORT_READER_THREAD_DEFAULTVALUE
   */
  String CARBON_MERGE_SORT_READER_THREAD_DEFAULTVALUE = "3";
  /**
   * IS_SORT_TEMP_FILE_COMPRESSION_ENABLED
   */
  String IS_SORT_TEMP_FILE_COMPRESSION_ENABLED =
          "carbon.is.sort.temp.file.compression.enabled";
  /**
   * IS_SORT_TEMP_FILE_COMPRESSION_ENABLED_DEFAULTVALUE
   */
  String IS_SORT_TEMP_FILE_COMPRESSION_ENABLED_DEFAULTVALUE = "false";
  /**
   * SORT_TEMP_FILE_NO_OF_RECORDS_FOR_COMPRESSION
   */
  String SORT_TEMP_FILE_NO_OF_RECORDS_FOR_COMPRESSION =
          "carbon.sort.temp.file.no.of.records.for.compression";
  /**
   * SORT_TEMP_FILE_NO_OF_RECORD_FOR_COMPRESSION_DEFAULTVALUE
   */
  String SORT_TEMP_FILE_NO_OF_RECORD_FOR_COMPRESSION_DEFAULTVALUE = "50";
  /**
   * DEFAULT_COLLECTION_SIZE
   */
  int DEFAULT_COLLECTION_SIZE = 16;
  /**
   * CARBON_TIMESTAMP_DEFAULT_FORMAT
   */
  String CARBON_TIMESTAMP_DEFAULT_FORMAT = "yyyy-MM-dd HH:mm:ss";
  /**
   * CARBON_TIMESTAMP_DEFAULT_FORMAT
   */
  String CARBON_TIMESTAMP_FORMAT = "carbon.timestamp.format";
  /**
   * STORE_LOCATION_HDFS
   */
  String STORE_LOCATION_HDFS = "carbon.storelocation.hdfs";
  /**
   * STORE_LOCATION_TEMP_PATH
   */
  String STORE_LOCATION_TEMP_PATH = "carbon.tempstore.location";
  /**
   * IS_COLUMNAR_STORAGE_DEFAULTVALUE
   */
  String IS_COLUMNAR_STORAGE_DEFAULTVALUE = "true";
  /**
   * DIMENSION_SPLIT_VALUE_IN_COLUMNAR_DEFAULTVALUE
   */
  String DIMENSION_SPLIT_VALUE_IN_COLUMNAR_DEFAULTVALUE = "1";
  /**
   * IS_FULLY_FILLED_BITS_DEFAULT_VALUE
   */
  String IS_FULLY_FILLED_BITS_DEFAULT_VALUE = "true";
  /**
   * IS_INT_BASED_INDEXER
   */
  String AGGREAGATE_COLUMNAR_KEY_BLOCK = "aggregate.columnar.keyblock";
  /**
   * IS_INT_BASED_INDEXER_DEFAULTVALUE
   */
  String AGGREAGATE_COLUMNAR_KEY_BLOCK_DEFAULTVALUE = "true";
  /**
   * ENABLE_QUERY_STATISTICS
   */
  String ENABLE_QUERY_STATISTICS = "enable.query.statistics";
  /**
   * ENABLE_QUERY_STATISTICS_DEFAULT
   */
  String ENABLE_QUERY_STATISTICS_DEFAULT = "false";
  /**
   * TIME_STAT_UTIL_TYPE
   */
  String ENABLE_DATA_LOADING_STATISTICS = "enable.data.loading.statistics";
  /**
   * TIME_STAT_UTIL_TYPE_DEFAULT
   */
  String ENABLE_DATA_LOADING_STATISTICS_DEFAULT = "false";
  /**
   * IS_INT_BASED_INDEXER
   */
  String HIGH_CARDINALITY_VALUE = "high.cardinality.value";
  /**
   * IS_INT_BASED_INDEXER_DEFAULTVALUE
   */
  String HIGH_CARDINALITY_VALUE_DEFAULTVALUE = "100000";
  /**
   * CONSTANT_SIZE_TEN
   */
  int CONSTANT_SIZE_TEN = 10;
  /**
   * LEVEL_METADATA_FILE
   */
  String LEVEL_METADATA_FILE = "levelmetadata_";
  /**
   * LOAD_STATUS SUCCESS
   */
  String STORE_LOADSTATUS_SUCCESS = "Success";
  /**
   * LOAD_STATUS FAILURE
   */
  String STORE_LOADSTATUS_FAILURE = "Failure";
  /**
   * LOAD_STATUS PARTIAL_SUCCESS
   */
  String STORE_LOADSTATUS_PARTIAL_SUCCESS = "Partial Success";
  /**
   * LOAD_STATUS
   */
  String CARBON_METADATA_EXTENSION = ".metadata";
  /**
   * COMMA
   */
  String COMMA = ",";
  /**
   * UNDERSCORE
   */
  String UNDERSCORE = "_";
  /**
   * POINT
   */
  String POINT = ".";
  /**
   * File separator
   */
  String FILE_SEPARATOR = "/";
  /**
   * MAX_QUERY_EXECUTION_TIME
   */
  String MAX_QUERY_EXECUTION_TIME = "max.query.execution.time";
  /**
   * CARBON_TIMESTAMP
   */
  String CARBON_TIMESTAMP = "dd-MM-yyyy HH:mm:ss";
  /**
   * NUMBER_OF_TRIES_FOR_LOAD_METADATA_LOCK
   */
  int NUMBER_OF_TRIES_FOR_LOAD_METADATA_LOCK_DEFAULT = 3;
  /**
   * MAX_TIMEOUT_FOR_LOAD_METADATA_LOCK
   */
  int MAX_TIMEOUT_FOR_LOAD_METADATA_LOCK_DEFAULT = 5;
  /**
   * NUMBER_OF_TRIES_FOR_LOAD_METADATA_LOCK
   */
  String NUMBER_OF_TRIES_FOR_LOAD_METADATA_LOCK =
          "carbon.load.metadata.lock.retries";
  /**
   * MAX_TIMEOUT_FOR_LOAD_METADATA_LOCK
   */
  String MAX_TIMEOUT_FOR_LOAD_METADATA_LOCK =
          "carbon.load.metadata.lock.retry.timeout.sec";
  /**
   * MARKED_FOR_DELETION
   */
  String MARKED_FOR_DELETE = "Marked for Delete";
  String MARKED_FOR_UPDATE = "Marked for Update";
  String STRING_TYPE = "StringType";
  String INTEGER_TYPE = "IntegerType";
  String LONG_TYPE = "LongType";
  String DOUBLE_TYPE = "DoubleType";
  String FLOAT_TYPE = "FloatType";
  String DATE_TYPE = "DateType";
  String BOOLEAN_TYPE = "BooleanType";
  String TIMESTAMP_TYPE = "TimestampType";
  String BYTE_TYPE = "ByteType";
  String SHORT_TYPE = "ShortType";
  String DECIMAL_TYPE = "DecimalType";
  String STRING = "String";
  String COLUMNAR = "columnar";

  String INTEGER = "Integer";
  String SHORT = "Short";
  String NUMERIC = "Numeric";
  String TIMESTAMP = "Timestamp";
  String ARRAY = "ARRAY";
  String STRUCT = "STRUCT";
  String INCLUDE = "include";
  String FROM = "from";
  String WITH = "with";
  /**
   * FACT_UPDATE_EXTENSION.
   */
  String FACT_DELETE_EXTENSION = "_delete";
  /**
   * MARKED_FOR_UPDATION
   */
  String FACT_FILE_UPDATED = "update";
  /**
   * MAX_QUERY_EXECUTION_TIME
   */
  int DEFAULT_MAX_QUERY_EXECUTION_TIME = 60;
  /**
   * LOADMETADATA_FILENAME
   */
  String LOADMETADATA_FILENAME = "tablestatus";
  /**
   * INMEMORY_REOCRD_SIZE
   */
  String DETAIL_QUERY_BATCH_SIZE = "carbon.detail.batch.size";
  int DETAIL_QUERY_BATCH_SIZE_DEFAULT = 10000;
  /**
   * SPILL_OVER_DISK_PATH
   */
  String SCHEMAS_MODIFIED_TIME_FILE = "modifiedTime.mdt";
  String DEFAULT_INVISIBLE_DUMMY_MEASURE = "default_dummy_measure";
  /**
   * max level cache size upto which level cache will be loaded in memory
   */
  String CARBON_MAX_LEVEL_CACHE_SIZE = "carbon.max.level.cache.size";
  /**
   * max level cache size default value in GB
   */
  String CARBON_MAX_LEVEL_CACHE_SIZE_DEFAULT = "-1";
  /**
   * DOUBLE_VALUE_MEASURE
   */
  char SUM_COUNT_VALUE_MEASURE = 'n';
  /**
   * BYTE_VALUE_MEASURE
   */
  char BYTE_VALUE_MEASURE = 'c';
  /**
   * BIG_DECIMAL_MEASURE
   */
  char BIG_DECIMAL_MEASURE = 'b';

  /**
   * BIG_INT_MEASURE
   */
  char BIG_INT_MEASURE = 'l';

  /**
   * This determines the size of array to be processed in data load steps. one
   * for dimensions , one of ignore dictionary dimensions , one for measures.
   */
  int ARRAYSIZE = 3;
  /**
   * CARBON_PREFETCH_BUFFERSIZE
   */
  int CARBON_PREFETCH_BUFFERSIZE = 20000;
  /**
   * CARBON_PREFETCH_IN_MERGE
   */
  boolean CARBON_PREFETCH_IN_MERGE_VALUE = false;
  /**
   * TEMPWRITEFILEEXTENSION
   */
  String TEMPWRITEFILEEXTENSION = ".write";
  /**
   * ENABLE_AUTO_LOAD_MERGE
   */
  String ENABLE_AUTO_LOAD_MERGE = "carbon.enable.auto.load.merge";
  /**
   * DEFAULT_ENABLE_AUTO_LOAD_MERGE
   */
  String DEFAULT_ENABLE_AUTO_LOAD_MERGE = "false";

  /**
   * ZOOKEEPER_ENABLE_LOCK if this is set to true then zookeeper will be used to handle locking
   * mechanism of carbon
   */
  String LOCK_TYPE = "carbon.lock.type";

  /**
   * ZOOKEEPER_ENABLE_DEFAULT the default value for zookeeper will be true for carbon
   */
  String LOCK_TYPE_DEFAULT = "LOCALLOCK";

  /**
   * ZOOKEEPER_LOCATION this is the location in zookeeper file system where locks are created.
   * mechanism of carbon
   */
  String ZOOKEEPER_LOCATION = "/CarbonLocks";

  /**
   * maximum dictionary chunk size that can be kept in memory while writing dictionary file
   */
  String DICTIONARY_ONE_CHUNK_SIZE = "carbon.dictionary.chunk.size";

  /**
   * dictionary chunk default size
   */
  String DICTIONARY_ONE_CHUNK_SIZE_DEFAULT = "10000";

  /**
   * xxhash algorithm property for hashmap
   */
  String ENABLE_XXHASH = "carbon.enableXXHash";

  /**
   * xxhash algorithm property for hashmap. Default value false
   */
  String ENABLE_XXHASH_DEFAULT = "true";

  /**
   * default charset to be used for reading and writing
   */
  String DEFAULT_CHARSET = "UTF-8";

  /**
   * surrogate key that will be sent whenever in the dictionary chunks
   * a valid surrogate key is not found for a given dictionary value
   */
  int INVALID_SURROGATE_KEY = -1;

  /**
   * surrogate key for MEMBER_DEFAULT_VAL
   */
  int MEMBER_DEFAULT_VAL_SURROGATE_KEY = 1;

  String INVALID_SEGMENT_ID = "-1";

  /**
   * Size of Major Compaction in MBs
   */
  String MAJOR_COMPACTION_SIZE = "carbon.major.compaction.size";

  /**
   * By default size of major compaction in MBs.
   */
  String DEFAULT_MAJOR_COMPACTION_SIZE = "1024";

  /**
   * This property is used to tell how many segments to be preserved from merging.
   */
  java.lang.String PRESERVE_LATEST_SEGMENTS_NUMBER = "carbon.numberof.preserve.segments";

  /**
   * If preserve property is enabled then 2 segments will be preserved.
   */
  String DEFAULT_PRESERVE_LATEST_SEGMENTS_NUMBER = "0";

  /**
   * This property will determine the loads of how many days can be compacted.
   */
  java.lang.String DAYS_ALLOWED_TO_COMPACT = "carbon.allowed.compaction.days";

  /**
   * Default value of 1 day loads can be compacted
   */
  String DEFAULT_DAYS_ALLOWED_TO_COMPACT = "0";

  /**
   * space reserved for writing block meta data in carbon data file
   */
  String CARBON_BLOCK_META_RESERVED_SPACE = "carbon.block.meta.size.reserved.percentage";

  /**
   * default value for space reserved for writing block meta data in carbon data file
   */
  String CARBON_BLOCK_META_RESERVED_SPACE_DEFAULT = "10";

  /**
   * property to enable min max during filter query
   */
  String CARBON_QUERY_MIN_MAX_ENABLED = "carbon.enableMinMax";

  /**
   * default value to enable min or max during filter query execution
   */
  String MIN_MAX_DEFAULT_VALUE = "true";

  /**
   * this variable is to enable/disable prefetch of data during merge sort while
   * reading data from sort temp files
   */
  String CARBON_MERGE_SORT_PREFETCH = "carbon.merge.sort.prefetch";
  String CARBON_MERGE_SORT_PREFETCH_DEFAULT = "true";

  /**
   *  default name of data base
   */
  String DATABASE_DEFAULT_NAME = "default";

  // tblproperties
  String COLUMN_GROUPS = "column_groups";
  String DICTIONARY_EXCLUDE = "dictionary_exclude";
  String DICTIONARY_INCLUDE = "dictionary_include";
  String PARTITIONCLASS = "partitionclass";
  String PARTITIONCOUNT = "partitioncount";
  String COLUMN_PROPERTIES = "columnproperties";
  String TABLE_BLOCKSIZE = "table_blocksize";

  /**
   * this variable is to enable/disable identify high cardinality during first data loading
   */
  String HIGH_CARDINALITY_IDENTIFY_ENABLE =
          "high.cardinality.identify.enable";
  String HIGH_CARDINALITY_IDENTIFY_ENABLE_DEFAULT = "true";

  /**
   * threshold of high cardinality
   */
  String HIGH_CARDINALITY_THRESHOLD = "high.cardinality.threshold";
  String HIGH_CARDINALITY_THRESHOLD_DEFAULT = "1000000";
  int HIGH_CARDINALITY_THRESHOLD_MIN = 10000;

  /**
   * percentage of cardinality in row count
   */
  String HIGH_CARDINALITY_IN_ROW_COUNT_PERCENTAGE =
          "high.cardinality.row.count.percentage";
  String HIGH_CARDINALITY_IN_ROW_COUNT_PERCENTAGE_DEFAULT = "80";

  /**
   * 16 mb size
   */
  long CARBON_16MB = 16*1024*1024;
  /**
   * 256 mb size
   */
  long CARBON_256MB = 256*1024*1024;

  /**
   * SEGMENT_COMPACTED is property to indicate whether seg is compacted or not.
   */
  String SEGMENT_COMPACTED = "Compacted";

  /**
   * property for number of core to load the blocks in driver
   */
  String NUMBER_OF_CORE_TO_LOAD_DRIVER_SEGMENT =
          "no.of.cores.to.load.blocks.in.driver";
  /**
   * default number of cores
   */
  int NUMBER_OF_CORE_TO_LOAD_DRIVER_SEGMENT_DEFAULT_VALUE = 10;

  /**
   * ZOOKEEPERLOCK TYPE
   */
  String CARBON_LOCK_TYPE_ZOOKEEPER = "ZOOKEEPERLOCK";

  /**
   * LOCALLOCK TYPE
   */
  String CARBON_LOCK_TYPE_LOCAL = "LOCALLOCK";

  /**
   * HDFSLOCK TYPE
   */
  String CARBON_LOCK_TYPE_HDFS =
          "HDFSLOCK";

  /**
   * Invalid filter member log string
   */
  String FILTER_INVALID_MEMBER = " Invalid Record(s) are present "
          + "while filter evaluation. ";

  /**
   * Number of unmerged segments to be merged.
   */
  String COMPACTION_SEGMENT_LEVEL_THRESHOLD =
          "carbon.compaction.level.threshold";

  /**
   * Default count for Number of segments to be merged in levels is 4,3
   */
  String DEFAULT_SEGMENT_LEVEL_THRESHOLD = "4,3";

  /**
   * default location of the carbon metastore db
   */
  String METASTORE_LOCATION_DEFAULT_VAL = "../carbon.metastore";

  /**
   * hive connection url
   */
  String HIVE_CONNECTION_URL = "javax.jdo.option.ConnectionURL";

  /**
   * Rocord size in case of compaction.
   */
  int COMPACTION_INMEMORY_RECORD_SIZE = 120000;

  /**
   * If the level 2 compaction is done in minor then new compacted segment will end with .2
   */
  String LEVEL2_COMPACTION_INDEX = ".2";

  /**
   * Indicates compaction
   */
  String COMPACTION_KEY_WORD = "COMPACTION";

  /**
   * hdfs temporary directory key
   */
  String HDFS_TEMP_LOCATION = "hadoop.tmp.dir";

  /**
   * zookeeper url key
   */
  String ZOOKEEPER_URL = "spark.deploy.zookeeper.url";

  /**
   * configure the minimum blocklet size eligible for blocklet distribution
   */
  String CARBON_BLOCKLETDISTRIBUTION_MIN_REQUIRED_SIZE =
          "carbon.blockletdistribution.min.blocklet.size";

  /**
   * default blocklet size eligible for blocklet distribution
   */
  int DEFAULT_CARBON_BLOCKLETDISTRIBUTION_MIN_REQUIRED_SIZE = 2;

  /**
   * File created in case of minor compaction request
   */
  String minorCompactionRequiredFile = "compactionRequired_minor";

  /**
   * File created in case of major compaction request
   */
  String majorCompactionRequiredFile = "compactionRequired_major";

  /**
   * @Deprecated : This property has been deprecated.
   * Property for enabling system level compaction lock.1 compaction can run at once.
   */
  String ENABLE_CONCURRENT_COMPACTION = "carbon.concurrent.compaction";

  /**
   * Default value of Property for enabling system level compaction lock.1 compaction can run
   * at once.
   */
  String DEFAULT_ENABLE_CONCURRENT_COMPACTION = "true";

  /**
   * Compaction system level lock folder.
   */
  String SYSTEM_LEVEL_COMPACTION_LOCK_FOLDER = "SystemCompactionLock";

  /**
   * to enable blocklet distribution
   */
  String ENABLE_BLOCKLET_DISTRIBUTION = "enable.blocklet.distribution";

  /**
   * to enable blocklet distribution default value
   */
  String ENABLE_BLOCKLET_DISTRIBUTION_DEFAULTVALUE = "true";

}


