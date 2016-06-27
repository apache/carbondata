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
   * integer size in bytes
   */
  public static final int INT_SIZE_IN_BYTE = 4;
  /**
   * short size in bytes
   */
  public static final int SHORT_SIZE_IN_BYTE = 2;
  /**
   * DOUBLE size in bytes
   */
  public static final int DOUBLE_SIZE_IN_BYTE = 8;
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
   * location of the carbon member, hierarchy and fact files
   */
  public static final String STORE_LOCATION = "carbon.storelocation";
  /**
   * blocklet size in carbon file
   */
  public static final String BLOCKLET_SIZE = "carbon.blocklet.size";
  /**
   * TODO: max number of blocklets written in a single file?
   */
  public static final String MAX_FILE_SIZE = "carbon.max.file.size";
  /**
   * Number of cores to be used
   */
  public static final String NUM_CORES = "carbon.number.of.cores";
  /**
   * carbon sort size
   */
  public static final String SORT_SIZE = "carbon.sort.size";
  /**
   * default location of the carbon member, hierarchy and fact files
   */
  public static final String STORE_LOCATION_DEFAULT_VAL = "../carbon.store";
  /**
   * CARDINALITY_INCREMENT_DEFAULT_VALUE
   */
  public static final int CARDINALITY_INCREMENT_VALUE_DEFAULT_VAL = 10;
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
   * TODO: default value of max number of blocklet written in a single file?
   */
  public static final String MAX_FILE_SIZE_DEFAULT_VAL = "1024";
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
   * default carbon sort size
   */
  public static final String SORT_SIZE_DEFAULT_VAL = "100000";
  /**
   * min carbon sort size
   */
  public static final int SORT_SIZE_MIN_VAL = 1000;
  /**
   * carbon properties file path
   */
  public static final String CARBON_PROPERTIES_FILE_PATH = "../../../conf/carbon.properties";
  /**
   * CARBON_DDL_BASE_HDFS_URL
   */
  public static final String CARBON_DDL_BASE_HDFS_URL = "carbon.ddl.base.hdfs.url";
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
   * BLANK_LINE_FLAG
   */
  public static final String BLANK_LINE_FLAG = "@NU#LL$!BLANKLINE";
  /**
   * FILE STATUS IN-PROGRESS
   */
  public static final String FILE_INPROGRESS_STATUS = ".inprogress";
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
   * SORT_TEMP_FILE_LOCATION
   */
  public static final String SORT_TEMP_FILE_LOCATION = "sortrowtmp";
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
  public static final String SORT_INTERMEDIATE_FILES_LIMIT_DEFAULT_VALUE = "20";
  /**
   * MERGERD_EXTENSION
   */
  public static final String MERGERD_EXTENSION = ".merge";
  /**
   * SORT_FILE_BUFFER_SIZE
   */
  public static final String SORT_FILE_BUFFER_SIZE = "carbon.sort.file.buffer.size";
  /**
   * no.of records after which counter to be printed
   */
  public static final String DATA_LOAD_LOG_COUNTER = "carbon.load.log.counter";
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
   * Number of cores to be used while loading
   */
  public static final String NUM_CORES_LOADING = "carbon.number.of.cores.while.loading";
  /**
   * Number of cores to be used while compacting
   */
  public static final String NUM_CORES_COMPACTING = "carbon.number.of.cores.while.compacting";
  /**
   * Number of cores to be used for block sort
   */
  public static final String NUM_CORES_BLOCK_SORT = "carbon.number.of.cores.block.sort";
  /**
   * Default value of number of cores to be used for block sort
   */
  public static final String NUM_CORES_BLOCK_SORT_DEFAULT_VAL = "7";
  /**
   * Max value of number of cores to be used for block sort
   */
  public static final int NUM_CORES_BLOCK_SORT_MAX_VAL = 12;
  /**
   * Min value of number of cores to be used for block sort
   */
  public static final int NUM_CORES_BLOCK_SORT_MIN_VAL = 1;
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
   * ATTHERATE_SPC_CHARATER
   */
  public static final String COMA_SPC_CHARACTER = ",#!:COMA:!#,";
  /**
   * HYPHEN_SPC_CHARACTER
   */
  public static final String HYPHEN_SPC_CHARACTER = "-#!:HYPHEN:!#-";
  /**
   * CARBON_DECIMAL_POINTERS_DEFAULT
   */
  public static final byte CARBON_DECIMAL_POINTERS_DEFAULT = 5;
  /**
   * SORT_TEMP_FILE_EXT
   */
  public static final String SORT_TEMP_FILE_EXT = ".sorttemp";
  /**
   * CARBON_MERGE_SORT_READER_THREAD
   */
  public static final String CARBON_MERGE_SORT_READER_THREAD = "carbon.merge.sort.reader.thread";
  /**
   * CARBON_MERGE_SORT_READER_THREAD_DEFAULTVALUE
   */
  public static final String CARBON_MERGE_SORT_READER_THREAD_DEFAULTVALUE = "3";
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
   * DEFAULT_COLLECTION_SIZE
   */
  public static final int DEFAULT_COLLECTION_SIZE = 16;
  /**
   * CARBON_TIMESTAMP_DEFAULT_FORMAT
   */
  public static final String CARBON_TIMESTAMP_DEFAULT_FORMAT = "yyyy-MM-dd HH:mm:ss";
  /**
   * CARBON_TIMESTAMP_DEFAULT_FORMAT
   */
  public static final String CARBON_TIMESTAMP_FORMAT = "carbon.timestamp.format";
  /**
   * STORE_LOCATION_HDFS
   */
  public static final String STORE_LOCATION_HDFS = "carbon.storelocation.hdfs";
  /**
   * STORE_LOCATION_TEMP_PATH
   */
  public static final String STORE_LOCATION_TEMP_PATH = "carbon.tempstore.location";
  /**
   * IS_COLUMNAR_STORAGE_DEFAULTVALUE
   */
  public static final String IS_COLUMNAR_STORAGE_DEFAULTVALUE = "true";
  /**
   * DIMENSION_SPLIT_VALUE_IN_COLUMNAR_DEFAULTVALUE
   */
  public static final String DIMENSION_SPLIT_VALUE_IN_COLUMNAR_DEFAULTVALUE = "1";
  /**
   * IS_FULLY_FILLED_BITS_DEFAULT_VALUE
   */
  public static final String IS_FULLY_FILLED_BITS_DEFAULT_VALUE = "true";
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
   * COMMA
   */
  public static final String COMMA = ",";
  /**
   * UNDERSCORE
   */
  public static final String UNDERSCORE = "_";
  /**
   * POINT
   */
  public static final String POINT = ".";
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
  public static final String STRING_TYPE = "StringType";
  public static final String INTEGER_TYPE = "IntegerType";
  public static final String LONG_TYPE = "LongType";
  public static final String DOUBLE_TYPE = "DoubleType";
  public static final String FLOAT_TYPE = "FloatType";
  public static final String DATE_TYPE = "DateType";
  public static final String BOOLEAN_TYPE = "BooleanType";
  public static final String TIMESTAMP_TYPE = "TimestampType";
  public static final String BYTE_TYPE = "ByteType";
  public static final String SHORT_TYPE = "ShortType";
  public static final String BINARY_TYPE = "BinaryType";
  public static final String DECIMAL_TYPE = "DecimalType";
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
  public static final String SUM_DISTINCT = "sum-distinct";
  /**
   * INMEMORY_REOCRD_SIZE
   */
  public static final String INMEMORY_REOCRD_SIZE = "carbon.inmemory.record.size";
  public static final int INMEMORY_REOCRD_SIZE_DEFAULT = 240000;
  /**
   * SPILL_OVER_DISK_PATH
   */
  public static final String SCHEMAS_MODIFIED_TIME_FILE = "modifiedTime.mdt";
  public static final String DEFAULT_INVISIBLE_DUMMY_MEASURE = "default_dummy_measure";
  /**
   * max level cache size upto which level cache will be loaded in memory
   */
  public static final String CARBON_MAX_LEVEL_CACHE_SIZE = "carbon.max.level.cache.size";
  /**
   * max level cache size default value in GB
   */
  public static final String CARBON_MAX_LEVEL_CACHE_SIZE_DEFAULT = "-1";
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
   * This determines the size of array to be processed in data load steps. one
   * for dimensions , one of ignore dictionary dimensions , one for measures.
   */
  public static final int ARRAYSIZE = 3;
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
   * ENABLE_AUTO_LOAD_MERGE
   */
  public static final String ENABLE_AUTO_LOAD_MERGE = "carbon.enable.auto.load.merge";
  /**
   * DEFAULT_ENABLE_AUTO_LOAD_MERGE
   */
  public static final String DEFAULT_ENABLE_AUTO_LOAD_MERGE = "false";

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
  public static final String ENABLE_XXHASH_DEFAULT = "true";

  /**
   * default charset to be used for reading and writing
   */
  public static final String DEFAULT_CHARSET = "UTF-8";

  /**
   * surrogate key that will be sent whenever in the dictionary chunks
   * a valid surrogate key is not found for a given dictionary value
   */
  public static final int INVALID_SURROGATE_KEY = -1;

  public static final String INVALID_SEGMENT_ID = "-1";

  /**
   * Size of Major Compaction in MBs
   */
  public static final String MAJOR_COMPACTION_SIZE = "carbon.major.compaction.size";

  /**
   * By default size of major compaction in MBs.
   */
  public static final String DEFAULT_MAJOR_COMPACTION_SIZE = "1024";

  /**
   * This property is used to tell how many segments to be preserved from merging.
   */
  public static final java.lang.String PRESERVE_LATEST_SEGMENTS_NUMBER =
      "carbon.numberof.preserve.segments";

  /**
   * If preserve property is enabled then 2 segments will be preserved.
   */
  public static final String DEFAULT_PRESERVE_LATEST_SEGMENTS_NUMBER = "0";

  /**
   * This property will determine the loads of how many days can be compacted.
   */
  public static final java.lang.String DAYS_ALLOWED_TO_COMPACT = "carbon.allowed.compaction.days";

  /**
   * Default value of 1 day loads can be compacted
   */
  public static final String DEFAULT_DAYS_ALLOWED_TO_COMPACT = "0";

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

  /**
   * this variable is to enable/disable prefetch of data during merge sort while
   * reading data from sort temp files
   */
  public static final String CARBON_MERGE_SORT_PREFETCH = "carbon.merge.sort.prefetch";
  public static final String CARBON_MERGE_SORT_PREFETCH_DEFAULT = "true";

  /**
   * this variable is to enable/disable identify high cardinality during first data loading
   */
  public static final String HIGH_CARDINALITY_IDENTIFY_ENABLE =
      "high.cardinality.identify.enable";
  public static final String HIGH_CARDINALITY_IDENTIFY_ENABLE_DEFAULT = "true";

  /**
   * threshold of high cardinality
   */
  public static final String HIGH_CARDINALITY_THRESHOLD = "high.cardinality.threshold";
  public static final String HIGH_CARDINALITY_THRESHOLD_DEFAULT = "1000000";
  public static final int HIGH_CARDINALITY_THRESHOLD_MIN = 10000;

  /**
   * percentage of cardinality in row count
   */
  public static final String HIGH_CARDINALITY_IN_ROW_COUNT_PERCENTAGE =
      "high.cardinality.row.count.percentage";
  public static final String HIGH_CARDINALITY_IN_ROW_COUNT_PERCENTAGE_DEFAULT = "80";

  /**
   * 16 mb size
   */
  public static final long CARBON_16MB = 16*1024*1024;
  /**
   * 256 mb size
   */
  public static final long CARBON_256MB = 256*1024*1024;

  /**
   * Data type String.
   */
  public static final String DATATYPE_STRING = "STRING";

  /**
   * SEGMENT_COMPACTED is property to indicate whether seg is compacted or not.
   */
  public static final String SEGMENT_COMPACTED = "Compacted";

  /**
   * property for number of core to load the blocks in driver
   */
  public static final String NUMBER_OF_CORE_TO_LOAD_DRIVER_SEGMENT =
      "no.of.cores.to.load.blocks.in.driver";
  /**
   * default number of cores
   */
  public static final int NUMBER_OF_CORE_TO_LOAD_DRIVER_SEGMENT_DEFAULT_VALUE = 10;

  /**
   * ZOOKEEPERLOCK TYPE
   */
  public static final String CARBON_LOCK_TYPE_ZOOKEEPER =
      "ZOOKEEPERLOCK";

  /**
   * LOCALLOCK TYPE
   */
  public static final String CARBON_LOCK_TYPE_LOCAL =
      "LOCALLOCK";

  /**
   * HDFSLOCK TYPE
   */
  public static final String CARBON_LOCK_TYPE_HDFS =
      "HDFSLOCK";

  /**
   * Lock file in zoo keeper will be of this name.
   */
  public static final String ZOOKEEPER_LOCK = "zookeeperLock";

  /**
   * Invalid filter member log string
   */
  public static final String FILTER_INVALID_MEMBER = " Invalid Record(s) are present "
                                                     + "while filter evaluation. ";

  /**
   * Number of unmerged segments to be merged.
   */
  public static final String COMPACTION_SEGMENT_LEVEL_THRESHOLD =
      "carbon.compaction.level.threshold";

  /**
   * Default count for Number of segments to be merged in levels is 4,3
   */
  public static final String DEFAULT_SEGMENT_LEVEL_THRESHOLD = "4,3";

  private CarbonCommonConstants() {
  }
}


