/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.carbondata.core.constants;

import java.nio.charset.Charset;

import org.apache.carbondata.core.util.CarbonProperty;

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
  @CarbonProperty
  public static final String STORE_LOCATION = "carbon.storelocation";
  /**
   * blocklet size in carbon file
   */
  @CarbonProperty
  public static final String BLOCKLET_SIZE = "carbon.blocklet.size";
  /**
   * Number of cores to be used
   */
  @CarbonProperty
  public static final String NUM_CORES = "carbon.number.of.cores";
  /**
   * carbon sort size
   */
  @CarbonProperty
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
  public static final int BLOCKLET_SIZE_MIN_VAL = 2000;
  /**
   * max blocklet size
   */
  public static final int BLOCKLET_SIZE_MAX_VAL = 12000000;
  /**
   * default block size in MB
   */
  public static final String BLOCK_SIZE_DEFAULT_VAL = "1024";
  /**
   * min block size in MB
   */
  public static final int BLOCK_SIZE_MIN_VAL = 1;
  /**
   * max block size in MB
   */
  public static final int BLOCK_SIZE_MAX_VAL = 2048;
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
  @CarbonProperty
  public static final String CARBON_DDL_BASE_HDFS_URL = "carbon.ddl.base.hdfs.url";
  /**
   * Load Folder Name
   */
  public static final String LOAD_FOLDER = "Segment_";
  /**
   * HDFSURL_PREFIX
   */
  public static final String HDFSURL_PREFIX = "hdfs://";
  /**
   * VIEWFSURL_PREFIX
   */
  public static final String VIEWFSURL_PREFIX = "viewfs://";
  /**
   * FS_DEFAULT_FS
   */
  @CarbonProperty
  public static final String FS_DEFAULT_FS = "fs.defaultFS";
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
   * MEMBER_DEFAULT_VAL_ARRAY
   */
  public static final byte[] MEMBER_DEFAULT_VAL_ARRAY = MEMBER_DEFAULT_VAL.getBytes();

  /**
   * Bytes for string 0, it is used in codegen in case of null values.
   */
  public static final byte[] ZERO_BYTE_ARRAY = "0".getBytes();
  /**
   * FILE STATUS IN-PROGRESS
   */
  public static final String FILE_INPROGRESS_STATUS = ".inprogress";
  /**
   * CARBON_BADRECORDS_LOCATION
   */
  @CarbonProperty
  public static final String CARBON_BADRECORDS_LOC = "carbon.badRecords.location";
  /**
   * CARBON_BADRECORDS_LOCATION_DEFAULT
   */
  public static final String CARBON_BADRECORDS_LOC_DEFAULT_VAL = "";
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
   * DELETE_DELTA_FILE_EXT
   */
  public static final String DELETE_DELTA_FILE_EXT = ".deletedelta";

  /**
   * UPDATE_DELTA_FILE_EXT
   */
  public static final String UPDATE_DELTA_FILE_EXT = FACT_FILE_EXT;
  /**
   * MEASUREMETADATA_FILE_EXT
   */
  public static final String MEASUREMETADATA_FILE_EXT = ".msrmetadata";
  /**
   * GRAPH_ROWSET_SIZE
   */
  @CarbonProperty
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
  @CarbonProperty
  public static final String SORT_INTERMEDIATE_FILES_LIMIT = "carbon.sort.intermediate.files.limit";
  /**
   * SORT_INTERMEDIATE_FILES_LIMIT_DEFAULT_VALUE
   */
  public static final String SORT_INTERMEDIATE_FILES_LIMIT_DEFAULT_VALUE = "20";
  /**
   * BAD_RECORD_KEY_VALUE
   */
  public static final String BAD_RECORD_KEY = "BADRECORD";
  /**
   * MERGERD_EXTENSION
   */
  public static final String MERGERD_EXTENSION = ".merge";
  /**
   * SORT_FILE_BUFFER_SIZE
   */
  @CarbonProperty
  public static final String SORT_FILE_BUFFER_SIZE = "carbon.sort.file.buffer.size";
  /**
   * no.of records after which counter to be printed
   */
  @CarbonProperty
  public static final String DATA_LOAD_LOG_COUNTER = "carbon.load.log.counter";
  /**
   * DATA_LOAD_LOG_COUNTER_DEFAULT_COUNTER
   */
  public static final String DATA_LOAD_LOG_COUNTER_DEFAULT_COUNTER = "500000";
  /**
   * SORT_FILE_WRITE_BUFFER_SIZE
   */
  @CarbonProperty
  public static final String CARBON_SORT_FILE_WRITE_BUFFER_SIZE =
      "carbon.sort.file.write.buffer.size";
  /**
   * SORT_FILE_WRITE_BUFFER_SIZE_DEFAULT_VALUE
   */
  public static final String CARBON_SORT_FILE_WRITE_BUFFER_SIZE_DEFAULT_VALUE = "16384";
  /**
   * Number of cores to be used while loading
   */
  @CarbonProperty
  public static final String NUM_CORES_LOADING = "carbon.number.of.cores.while.loading";
  /**
   * Number of cores to be used while compacting
   */
  @CarbonProperty
  public static final String NUM_CORES_COMPACTING = "carbon.number.of.cores.while.compacting";
  /**
   * Number of cores to be used for block sort
   */
  @CarbonProperty
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
  @CarbonProperty
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
   * LOG_FILE_EXTENSION
   */
  public static final String LOG_FILE_EXTENSION = ".log";

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
   * SORT_TEMP_FILE_EXT
   */
  public static final String SORT_TEMP_FILE_EXT = ".sorttemp";
  /**
   * CARBON_MERGE_SORT_READER_THREAD
   */
  @CarbonProperty
  public static final String CARBON_MERGE_SORT_READER_THREAD = "carbon.merge.sort.reader.thread";
  /**
   * CARBON_MERGE_SORT_READER_THREAD_DEFAULTVALUE
   */
  public static final String CARBON_MERGE_SORT_READER_THREAD_DEFAULTVALUE = "3";
  /**
   * IS_SORT_TEMP_FILE_COMPRESSION_ENABLED
   */
  @CarbonProperty
  public static final String IS_SORT_TEMP_FILE_COMPRESSION_ENABLED =
      "carbon.is.sort.temp.file.compression.enabled";
  /**
   * IS_SORT_TEMP_FILE_COMPRESSION_ENABLED_DEFAULTVALUE
   */
  public static final String IS_SORT_TEMP_FILE_COMPRESSION_ENABLED_DEFAULTVALUE = "false";
  /**
   * SORT_TEMP_FILE_NO_OF_RECORDS_FOR_COMPRESSION
   */
  @CarbonProperty
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
   * Property for specifying the format of TIMESTAMP data type column.
   * e.g. yyyy/MM/dd HH:mm:ss, or using CARBON_TIMESTAMP_DEFAULT_FORMAT
   */
  @CarbonProperty
  public static final String CARBON_TIMESTAMP_FORMAT = "carbon.timestamp.format";

  /**
   * CARBON_DATE_DEFAULT_FORMAT
   */
  public static final String CARBON_DATE_DEFAULT_FORMAT = "yyyy-MM-dd";
  /**
   * Property for specifying the format of DATE data type column.
   * e.g. yyyy/MM/dd , or using CARBON_DATE_DEFAULT_FORMAT
   */
  @CarbonProperty
  public static final String CARBON_DATE_FORMAT = "carbon.date.format";
  /**
   * STORE_LOCATION_HDFS
   */
  @CarbonProperty
  public static final String STORE_LOCATION_HDFS = "carbon.storelocation.hdfs";
  /**
   * STORE_LOCATION_TEMP_PATH
   */
  @CarbonProperty
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
  @CarbonProperty
  public static final String AGGREAGATE_COLUMNAR_KEY_BLOCK = "aggregate.columnar.keyblock";
  /**
   * IS_INT_BASED_INDEXER_DEFAULTVALUE
   */
  public static final String AGGREAGATE_COLUMNAR_KEY_BLOCK_DEFAULTVALUE = "true";
  /**
   * ENABLE_QUERY_STATISTICS
   */
  @CarbonProperty
  public static final String ENABLE_QUERY_STATISTICS = "enable.query.statistics";
  /**
   * ENABLE_QUERY_STATISTICS_DEFAULT
   */
  public static final String ENABLE_QUERY_STATISTICS_DEFAULT = "false";
  /**
   * TIME_STAT_UTIL_TYPE
   */
  @CarbonProperty
  public static final String ENABLE_DATA_LOADING_STATISTICS = "enable.data.loading.statistics";
  /**
   * TIME_STAT_UTIL_TYPE_DEFAULT
   */
  public static final String ENABLE_DATA_LOADING_STATISTICS_DEFAULT = "false";
  /**
   * IS_INT_BASED_INDEXER
   */
  @CarbonProperty
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
  /**
   * LOAD_STATUS SUCCESS
   */
  public static final String STORE_LOADSTATUS_SUCCESS = "Success";
  /**
   * LOAD_STATUS UPDATE
   */
  public static final String STORE_LOADSTATUS_UPDATE = "Update";
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
   * Windows File separator
   */
  public static final String WINDOWS_FILE_SEPARATOR = "\\";
  /**
   * File separator
   */
  public static final String FILE_SEPARATOR = "/";
  /**
   * MAX_QUERY_EXECUTION_TIME
   */
  @CarbonProperty
  public static final String MAX_QUERY_EXECUTION_TIME = "max.query.execution.time";
  /**
   * CARBON_TIMESTAMP
   */
  public static final String CARBON_TIMESTAMP = "dd-MM-yyyy HH:mm:ss";

  /**
   * CARBON_TIMESTAMP
   */
  public static final String CARBON_TIMESTAMP_MILLIS = "dd-MM-yyyy HH:mm:ss:SSS";
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
  @CarbonProperty
  public static final String NUMBER_OF_TRIES_FOR_LOAD_METADATA_LOCK =
      "carbon.load.metadata.lock.retries";
  /**
   * MAX_TIMEOUT_FOR_LOAD_METADATA_LOCK
   */
  @CarbonProperty
  public static final String MAX_TIMEOUT_FOR_LOAD_METADATA_LOCK =
      "carbon.load.metadata.lock.retry.timeout.sec";

  /**
   * compressor for writing/reading carbondata file
   */
  @CarbonProperty
  public static final String COMPRESSOR = "carbon.column.compressor";

  /**
   * default compressor is snappy
   */
  public static final String DEFAULT_COMPRESSOR = "snappy";

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
  public static final String DECIMAL_TYPE = "DecimalType";
  public static final String STRING = "String";

  public static final String INTEGER = "Integer";
  public static final String SHORT = "Short";
  public static final String NUMERIC = "Numeric";
  public static final String TIMESTAMP = "Timestamp";
  public static final String ARRAY = "ARRAY";
  public static final String STRUCT = "STRUCT";
  public static final String FROM = "from";
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
   * TABLE UPDATE STATUS FILENAME
   */
  public static final String TABLEUPDATESTATUS_FILENAME = "tableupdatestatus";
  /**
   * The batch size of records which returns to client.
   */
  @CarbonProperty
  public static final String DETAIL_QUERY_BATCH_SIZE = "carbon.detail.batch.size";

  public static final int DETAIL_QUERY_BATCH_SIZE_DEFAULT = 100;
  /**
   * SPILL_OVER_DISK_PATH
   */
  public static final String SCHEMAS_MODIFIED_TIME_FILE = "modifiedTime.mdt";
  public static final String DEFAULT_INVISIBLE_DUMMY_MEASURE = "default_dummy_measure";
  public static final String CARBON_IMPLICIT_COLUMN_POSITIONID = "positionId";
  public static final String CARBON_IMPLICIT_COLUMN_TUPLEID = "tupleId";
  /**
   * max driver lru cache size upto which lru cache will be loaded in memory
   */
  @CarbonProperty
  public static final String CARBON_MAX_DRIVER_LRU_CACHE_SIZE = "carbon.max.driver.lru.cache.size";
  public static final String POSITION_REFERENCE = "positionReference";
  /**
   * implicit column which will be added to each carbon table
   */
  public static final String POSITION_ID = "positionId";
  /**
   * max driver lru cache size upto which lru cache will be loaded in memory
   */
  @CarbonProperty
  public static final String CARBON_MAX_LEVEL_CACHE_SIZE = "carbon.max.level.cache.size";
  /**
   * max executor lru cache size upto which lru cache will be loaded in memory
   */
  @CarbonProperty
  public static final String CARBON_MAX_EXECUTOR_LRU_CACHE_SIZE =
      "carbon.max.executor.lru.cache.size";
  /**
   * max lru cache size default value in MB
   */
  public static final String CARBON_MAX_LRU_CACHE_SIZE_DEFAULT = "-1";

  /**
   * CARBON_PREFETCH_BUFFERSIZE
   */
  @CarbonProperty
  public static final String CARBON_PREFETCH_BUFFERSIZE = "carbon.prefetch.buffersize";
  /**
   * CARBON_PREFETCH_BUFFERSIZE DEFAULT VALUE
   */
  public static final String CARBON_PREFETCH_BUFFERSIZE_DEFAULT = "1000";
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
  @CarbonProperty
  public static final String ENABLE_AUTO_LOAD_MERGE = "carbon.enable.auto.load.merge";
  /**
   * DEFAULT_ENABLE_AUTO_LOAD_MERGE
   */
  public static final String DEFAULT_ENABLE_AUTO_LOAD_MERGE = "false";

  /**
   * ZOOKEEPER_ENABLE_LOCK if this is set to true then zookeeper will be used to handle locking
   * mechanism of carbon
   */
  @CarbonProperty
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
  @CarbonProperty
  public static final String DICTIONARY_ONE_CHUNK_SIZE = "carbon.dictionary.chunk.size";

  /**
   *  Dictionary Server Worker Threads
   */
  @CarbonProperty
  public static final String DICTIONARY_WORKER_THREADS = "dictionary.worker.threads";

  /**
   *  Dictionary Server Worker Threads
   */
  public static final String DICTIONARY_WORKER_THREADS_DEFAULT = "1";

  /**
   * dictionary chunk default size
   */
  public static final String DICTIONARY_ONE_CHUNK_SIZE_DEFAULT = "10000";

  /**
   * xxhash algorithm property for hashmap
   */
  @CarbonProperty
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
   * default charset class to be used for reading and writing
   */
  public static final Charset DEFAULT_CHARSET_CLASS = Charset.forName(DEFAULT_CHARSET);

  /**
   * surrogate key that will be sent whenever in the dictionary chunks
   * a valid surrogate key is not found for a given dictionary value
   */
  public static final int INVALID_SURROGATE_KEY = -1;

  /**
   * surrogate key for MEMBER_DEFAULT_VAL
   */
  public static final int MEMBER_DEFAULT_VAL_SURROGATE_KEY = 1;

  public static final String INVALID_SEGMENT_ID = "-1";

  /**
   * Size of Major Compaction in MBs
   */
  @CarbonProperty
  public static final String MAJOR_COMPACTION_SIZE = "carbon.major.compaction.size";

  /**
   * By default size of major compaction in MBs.
   */
  public static final String DEFAULT_MAJOR_COMPACTION_SIZE = "1024";

  /**
   * This property is used to tell how many segments to be preserved from merging.
   */
  @CarbonProperty
  public static final java.lang.String PRESERVE_LATEST_SEGMENTS_NUMBER =
      "carbon.numberof.preserve.segments";

  /**
   * If preserve property is enabled then 2 segments will be preserved.
   */
  public static final String DEFAULT_PRESERVE_LATEST_SEGMENTS_NUMBER = "0";

  /**
   * This property will determine the loads of how many days can be compacted.
   */
  @CarbonProperty
  public static final java.lang.String DAYS_ALLOWED_TO_COMPACT = "carbon.allowed.compaction.days";

  /**
   * Default value of 1 day loads can be compacted
   */
  public static final String DEFAULT_DAYS_ALLOWED_TO_COMPACT = "0";

  /**
   * space reserved for writing block meta data in carbon data file
   */
  @CarbonProperty
  public static final String CARBON_BLOCK_META_RESERVED_SPACE =
      "carbon.block.meta.size.reserved.percentage";

  /**
   * default value for space reserved for writing block meta data in carbon data file
   */
  public static final String CARBON_BLOCK_META_RESERVED_SPACE_DEFAULT = "10";

  /**
   * property to enable min max during filter query
   */
  @CarbonProperty
  public static final String CARBON_QUERY_MIN_MAX_ENABLED = "carbon.enableMinMax";

  /**
   * default value to enable min or max during filter query execution
   */
  public static final String MIN_MAX_DEFAULT_VALUE = "true";

  /**
   * this variable is to enable/disable prefetch of data during merge sort while
   * reading data from sort temp files
   */
  @CarbonProperty
  public static final String CARBON_MERGE_SORT_PREFETCH = "carbon.merge.sort.prefetch";
  public static final String CARBON_MERGE_SORT_PREFETCH_DEFAULT = "true";

  /**
   * default name of data base
   */
  public static final String DATABASE_DEFAULT_NAME = "default";

  // tblproperties
  public static final String COLUMN_GROUPS = "column_groups";
  public static final String DICTIONARY_EXCLUDE = "dictionary_exclude";
  public static final String DICTIONARY_INCLUDE = "dictionary_include";
  public static final String SORT_COLUMNS = "sort_columns";
  public static final String PARTITION_TYPE = "partition_type";
  public static final String NUM_PARTITIONS = "num_partitions";
  public static final String RANGE_INFO = "range_info";
  public static final String LIST_INFO = "list_info";
  public static final String COLUMN_PROPERTIES = "columnproperties";
  // table block size in MB
  public static final String TABLE_BLOCKSIZE = "table_blocksize";
  // set in column level to disable inverted index
  public static final String NO_INVERTED_INDEX = "no_inverted_index";

  /**
   * this variable is to enable/disable identify high cardinality during first data loading
   */
  @CarbonProperty
  public static final String HIGH_CARDINALITY_IDENTIFY_ENABLE = "high.cardinality.identify.enable";
  public static final String HIGH_CARDINALITY_IDENTIFY_ENABLE_DEFAULT = "true";

  /**
   * threshold of high cardinality
   */
  @CarbonProperty
  public static final String HIGH_CARDINALITY_THRESHOLD = "high.cardinality.threshold";
  public static final String HIGH_CARDINALITY_THRESHOLD_DEFAULT = "1000000";
  public static final int HIGH_CARDINALITY_THRESHOLD_MIN = 10000;

  /**
   * percentage of cardinality in row count
   */
  @CarbonProperty
  public static final String HIGH_CARDINALITY_IN_ROW_COUNT_PERCENTAGE =
      "high.cardinality.row.count.percentage";
  public static final String HIGH_CARDINALITY_IN_ROW_COUNT_PERCENTAGE_DEFAULT = "80";

  /**
   * 16 mb size
   */
  public static final long CARBON_16MB = 16 * 1024 * 1024;
  /**
   * 256 mb size
   */
  public static final long CARBON_256MB = 256 * 1024 * 1024;

  /**
   * COMPACTED is property to indicate whether seg is compacted or not.
   */
  public static final String COMPACTED = "Compacted";

  /**
   * ZOOKEEPERLOCK TYPE
   */
  public static final String CARBON_LOCK_TYPE_ZOOKEEPER = "ZOOKEEPERLOCK";

  /**
   * LOCALLOCK TYPE
   */
  public static final String CARBON_LOCK_TYPE_LOCAL = "LOCALLOCK";

  /**
   * HDFSLOCK TYPE
   */
  public static final String CARBON_LOCK_TYPE_HDFS = "HDFSLOCK";

  /**
   * Invalid filter member log string
   */
  public static final String FILTER_INVALID_MEMBER =
      " Invalid Record(s) are present while filter evaluation. ";

  /**
   * Number of unmerged segments to be merged.
   */
  @CarbonProperty
  public static final String COMPACTION_SEGMENT_LEVEL_THRESHOLD =
      "carbon.compaction.level.threshold";

  /**
   * Default count for Number of segments to be merged in levels is 4,3
   */
  public static final String DEFAULT_SEGMENT_LEVEL_THRESHOLD = "4,3";

  /**
   * Number of Update Delta files which is the Threshold for IUD compaction.
   * Only accepted Range is 0 - 10000. Outside this range system will pick default value.
   */
  @CarbonProperty
  public static final String UPDATE_DELTAFILE_COUNT_THRESHOLD_IUD_COMPACTION =
      "carbon.horizontal.update.compaction.threshold";
  /**
   * Default count of segments which act as a threshold for IUD compaction merge.
   */
  public static final String DEFAULT_UPDATE_DELTAFILE_COUNT_THRESHOLD_IUD_COMPACTION = "1";

  /**
   * Number of Delete Delta files which is the Threshold for IUD compaction.
   * Only accepted Range is 0 - 10000. Outside this range system will pick default value.
   */
  @CarbonProperty
  public static final String DELETE_DELTAFILE_COUNT_THRESHOLD_IUD_COMPACTION =
      "carbon.horizontal.delete.compaction.threshold";
  /**
   * Default count of segments which act as a threshold for IUD compaction merge.
   */
  public static final String DEFAULT_DELETE_DELTAFILE_COUNT_THRESHOLD_IUD_COMPACTION = "1";

  /**
   * default location of the carbon metastore db
   */
  public static final String METASTORE_LOCATION_DEFAULT_VAL = "../carbon.metastore";

  /**
   * hive connection url
   */
  @CarbonProperty
  public static final String HIVE_CONNECTION_URL = "javax.jdo.option.ConnectionURL";

  /**
   * If the level 2 compaction is done in minor then new compacted segment will end with .2
   */
  public static String LEVEL2_COMPACTION_INDEX = ".2";

  /**
   * Indicates compaction
   */
  public static String COMPACTION_KEY_WORD = "COMPACTION";

  /**
   * hdfs temporary directory key
   */
  @CarbonProperty
  public static final String HDFS_TEMP_LOCATION = "hadoop.tmp.dir";

  /**
   * zookeeper url key
   */
  @CarbonProperty
  public static final String ZOOKEEPER_URL = "spark.deploy.zookeeper.url";

  /**
   * File created in case of minor compaction request
   */
  public static String minorCompactionRequiredFile = "compactionRequired_minor";

  /**
   * File created in case of major compaction request
   */
  public static String majorCompactionRequiredFile = "compactionRequired_major";

  /**
   * @Deprecated : This property has been deprecated.
   * Property for enabling system level compaction lock.1 compaction can run at once.
   */
  @CarbonProperty
  public static String ENABLE_CONCURRENT_COMPACTION = "carbon.concurrent.compaction";

  /**
   * Default value of Property for enabling system level compaction lock.1 compaction can run
   * at once.
   */
  public static String DEFAULT_ENABLE_CONCURRENT_COMPACTION = "true";

  /**
   * Compaction system level lock folder.
   */
  public static String SYSTEM_LEVEL_COMPACTION_LOCK_FOLDER = "SystemCompactionLock";

  /**
   * This batch size is used to send rows from load step to another step in batches.
   */
  public static final String DATA_LOAD_BATCH_SIZE = "DATA_LOAD_BATCH_SIZE";

  /**
   * Default size of data load batch size.
   */
  public static final String DATA_LOAD_BATCH_SIZE_DEFAULT = "1000";
  /**
   * carbon data file version property
   */
  @CarbonProperty
  public static final String CARBON_DATA_FILE_VERSION = "carbon.data.file.version";

  /**
   * property to set is IS_DRIVER_INSTANCE
   */
  public static final String UPDATE_INDEX_FILE_EXT = ".carbonindex";

  /**
   * Key word for true
   */
  public static final String KEYWORD_TRUE = "TRUE";

  /**
   * Key word for false
   */
  public static final String KEYWORD_FALSE = "FALSE";

  /**
   * hyphen
   */
  public static final String HYPHEN = "-";

  /**
   * columns which gets updated in update will have header ends with this extension.
   */
  public static String UPDATED_COL_EXTENSION = "-updatedColumn";

  /**
   * appending the key to differentiate the update flow with insert flow.
   */
  public static String RDDUTIL_UPDATE_KEY = "UPDATE_";

  /**
   * to determine to use the rdd persist or not.
   */
  @CarbonProperty
  public static String isPersistEnabled = "carbon.update.persist.enable";

  /**
   * for enabling or disabling Horizontal Compaction.
   */
  @CarbonProperty
  public static String isHorizontalCompactionEnabled = "carbon.horizontal.compaction.enable";

  /**
   * Default value for HorizontalCompaction is true.
   */
  public static String defaultIsHorizontalCompactionEnabled = "true";

  /**
   * by default rdd will be persisted in the update case.
   */
  public static String defaultValueIsPersistEnabled = "true";

  /**
   * current data file version
   */
  public static final String CARBON_DATA_FILE_DEFAULT_VERSION = "V3";

  /**
   * data file version header
   */
  public static final String CARBON_DATA_VERSION_HEADER = "CARBONDATAVERSION#";
  /**
   * Maximum no of column supported
   */
  public static int DEFAULT_MAX_NUMBER_OF_COLUMNS = 20000;

  /**
   * Maximum waiting time (in seconds) for a query for requested executors to be started
   */
  @CarbonProperty
  public static final String CARBON_EXECUTOR_STARTUP_TIMEOUT =
      "carbon.max.executor.startup.timeout";

  /**
   * default value for executor start up waiting time out
   */
  public static final String CARBON_EXECUTOR_WAITING_TIMEOUT_DEFAULT = "5";

  /**
   * Max value. If value configured by user is more than this than this value will value will be
   * considered
   */
  public static final int CARBON_EXECUTOR_WAITING_TIMEOUT_MAX = 60;

  /**
   * time for which thread will sleep and check again if the requested number of executors
   * have been started
   */
  public static final int CARBON_EXECUTOR_STARTUP_THREAD_SLEEP_TIME = 250;

  /**
   * to enable unsafe column page in write step
   */
  public static final String ENABLE_UNSAFE_COLUMN_PAGE_LOADING = "enable.unsafe.columnpage";

  /**
   * default value of ENABLE_UNSAFE_COLUMN_PAGE_LOADING
   */
  public static final String ENABLE_UNSAFE_COLUMN_PAGE_LOADING_DEFAULT = "false";

  /**
   * to enable offheap sort
   */
  @CarbonProperty
  public static final String ENABLE_UNSAFE_SORT = "enable.unsafe.sort";

  /**
   * to enable offheap sort
   */
  public static final String ENABLE_UNSAFE_SORT_DEFAULT = "false";

  /**
   * to enable offheap sort
   */
  @CarbonProperty
  public static final String ENABLE_OFFHEAP_SORT = "enable.offheap.sort";

  /**
   * to enable offheap sort
   */
  public static final String ENABLE_OFFHEAP_SORT_DEFAULT = "true";
  @CarbonProperty
  public static final String ENABLE_INMEMORY_MERGE_SORT = "enable.inmemory.merge.sort";

  public static final String ENABLE_INMEMORY_MERGE_SORT_DEFAULT = "false";
  @CarbonProperty
  public static final String OFFHEAP_SORT_CHUNK_SIZE_IN_MB = "offheap.sort.chunk.size.inmb";

  public static final String OFFHEAP_SORT_CHUNK_SIZE_IN_MB_DEFAULT = "64";
  @CarbonProperty
  public static final String IN_MEMORY_FOR_SORT_DATA_IN_MB = "sort.inmemory.size.inmb";

  public static final String IN_MEMORY_FOR_SORT_DATA_IN_MB_DEFAULT = "1024";

  /**
   * Sorts the data in batches and writes the batch data to store with index file.
   */
  @CarbonProperty
  public static final String LOAD_SORT_SCOPE = "carbon.load.sort.scope";
  @CarbonProperty
  public static final String LOAD_USE_BATCH_SORT = "carbon.load.use.batch.sort";

  /**
   * If set to BATCH_SORT, the sorting scope is smaller and more index tree will be created,
   * thus loading is faster but query maybe slower.
   * If set to LOCAL_SORT, the sorting scope is bigger and one index tree per data node will be
   * created, thus loading is slower but query is faster.
   * If set to GLOBAL_SORT, the sorting scope is bigger and one index tree per task will be
   * created, thus loading is slower but query is faster.
   */
  public static final String LOAD_SORT_SCOPE_DEFAULT = "LOCAL_SORT";

  /**
   * Size of batch data to keep in memory, as a thumb rule it supposed
   * to be less than 45% of sort.inmemory.size.inmb otherwise it may spill intermediate data to disk
   */
  @CarbonProperty
  public static final String LOAD_BATCH_SORT_SIZE_INMB = "carbon.load.batch.sort.size.inmb";
  public static final String LOAD_BATCH_SORT_SIZE_INMB_DEFAULT = "0";
  @CarbonProperty
  /**
   * The Number of partitions to use when shuffling data for sort. If user don't configurate or
   * configurate it less than 1, it uses the number of map tasks as reduce tasks. In general, we
   * recommend 2-3 tasks per CPU core in your cluster.
   */
  public static final String LOAD_GLOBAL_SORT_PARTITIONS = "carbon.load.global.sort.partitions";

  public static final String LOAD_GLOBAL_SORT_PARTITIONS_DEFAULT = "0";
  @CarbonProperty
  public static final String ENABLE_VECTOR_READER = "carbon.enable.vector.reader";

  public static final String ENABLE_VECTOR_READER_DEFAULT = "true";

  /*
   * carbon dictionary server port
   */
  @CarbonProperty
  public static final String DICTIONARY_SERVER_PORT = "carbon.dictionary.server.port";

  /**
   * Default carbon dictionary server port
   */
  public static final String DICTIONARY_SERVER_PORT_DEFAULT = "2030";

  /**
   * property to set is IS_DRIVER_INSTANCE
   */
  @CarbonProperty
  public static final String IS_DRIVER_INSTANCE = "is.driver.instance";

  /**
   * maximum length of column
   */
  public static final int DEFAULT_COLUMN_LENGTH = 100000;

  /**
   * property for enabling unsafe based query processing
   */
  @CarbonProperty
  public static final String ENABLE_UNSAFE_IN_QUERY_EXECUTION = "enable.unsafe.in.query.processing";

  /**
   * default property of unsafe processing
   */
  public static final String ENABLE_UNSAFE_IN_QUERY_EXECUTION_DEFAULTVALUE = "false";

  /**
   * property for offheap based processing
   */
  @CarbonProperty
  public static final String USE_OFFHEAP_IN_QUERY_PROCSSING = "use.offheap.in.query.processing";

  /**
   * default value of offheap based processing
   */
  public static final String USE_OFFHEAP_IN_QUERY_PROCSSING_DEFAULT = "true";

  /**
   * whether to prefetch data while loading.
   */
  @CarbonProperty
  public static final String USE_PREFETCH_WHILE_LOADING = "carbon.loading.prefetch";

  /**
   * default value for prefetch data while loading.
   */
  public static final String USE_PREFETCH_WHILE_LOADING_DEFAULT = "false";

  public static final String MINOR = "minor";

  public static final String MAJOR = "major";

  public static final String LOCAL_FILE_PREFIX = "file://";
  @CarbonProperty
  public static final String CARBON_CUSTOM_BLOCK_DISTRIBUTION = "carbon.custom.block.distribution";
  public static final String CARBON_CUSTOM_BLOCK_DISTRIBUTION_DEFAULT = "false";

  public static final int DICTIONARY_DEFAULT_CARDINALITY = 1;
  @CarbonProperty
  public static final String SPARK_SCHEMA_STRING_LENGTH_THRESHOLD =
      "spark.sql.sources.schemaStringLengthThreshold";

  public static final int SPARK_SCHEMA_STRING_LENGTH_THRESHOLD_DEFAULT = 4000;
  @CarbonProperty
  public static final String CARBON_BAD_RECORDS_ACTION = "carbon.bad.records.action";

  public static final String CARBON_BAD_RECORDS_ACTION_DEFAULT = "FORCE";

  private CarbonCommonConstants() {
  }
}


