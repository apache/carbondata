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
   * surrogate value of null
   */
  public static final int DICT_VALUE_NULL = 1;
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
   * set the segment ids to query from the table
   */
  public static final String CARBON_INPUT_SEGMENTS = "carbon.input.segments.";

  /**
   * Fetch and validate the segments.
   * Used for aggregate table load as segment validation is not required.
   */
  public static final String VALIDATE_CARBON_INPUT_SEGMENTS = "validate.carbon.input.segments.";

  /**
   * Whether load/insert command is fired internally or by the user.
   * Used to block load/insert on pre-aggregate if fired by user
   */
  public static final String IS_INTERNAL_LOAD_CALL = "is.internal.load.call";

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

  public static final String HDFSURL_PREFIX = "hdfs://";

  public static final String LOCAL_FILE_PREFIX = "file://";

  public static final String VIEWFSURL_PREFIX = "viewfs://";

  public static final String ALLUXIOURL_PREFIX = "alluxio://";

  public static final String S3_PREFIX = "s3://";

  public static final String S3N_PREFIX = "s3n://";

  public static final String S3A_PREFIX = "s3a://";
  /**
   * Access Key for s3n
   */
  public static final String S3N_ACCESS_KEY = "fs.s3n.awsAccessKeyId";
  /**
   * Secret Key for s3n
   */
  public static final String S3N_SECRET_KEY = "fs.s3n.awsSecretAccessKey";
  /**
   * Access Key for s3
   */
  public static final String S3_ACCESS_KEY = "fs.s3.awsAccessKeyId";
  /**
   * Secret Key for s3
   */
  public static final String S3_SECRET_KEY = "fs.s3.awsSecretAccessKey";

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
   * default charset to be used for reading and writing
   */
  public static final String DEFAULT_CHARSET = "UTF-8";

  /**
   * MEMBER_DEFAULT_VAL_ARRAY
   */
  public static final byte[] MEMBER_DEFAULT_VAL_ARRAY =
      MEMBER_DEFAULT_VAL.getBytes(Charset.forName(DEFAULT_CHARSET));

  /**
   * Bytes for string 0, it is used in codegen in case of null values.
   */
  public static final byte[] ZERO_BYTE_ARRAY = "0".getBytes(Charset.forName(DEFAULT_CHARSET));

  /**
   * Empty byte array
   */
  public static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

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

  public static final int SORT_INTERMEDIATE_FILES_LIMIT_MIN = 2;

  public static final int SORT_INTERMEDIATE_FILES_LIMIT_MAX = 50;
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
  public static final int CARBON_SORT_FILE_WRITE_BUFFER_SIZE_MIN = 10240;
  public static final int CARBON_SORT_FILE_WRITE_BUFFER_SIZE_MAX = 10485760;
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
   * Number of cores to be used while alter partition
   */
  public static final String NUM_CORES_ALT_PARTITION = "carbon.number.of.cores.while.altPartition";
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
   * min value for csv read buffer size
   */
  public static final int CSV_READ_BUFFER_SIZE_MIN = 10240; //10 kb
  /**
   * max value for csv read buffer size
   */
  public static final int CSV_READ_BUFFER_SIZE_MAX = 10485760; // 10 mb

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
   * CONSTANT_SIZE_TEN
   */
  public static final int CONSTANT_SIZE_TEN = 10;
  /**
   * LEVEL_METADATA_FILE
   */
  public static final String LEVEL_METADATA_FILE = "levelmetadata_";

  /**
   * COMMA
   */
  public static final String COMMA = ",";
  /**
   * UNDERSCORE
   */
  public static final String UNDERSCORE = "_";
  /**
   * DASH
   */
  public static final String DASH = "-";
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
   * NUMBER_OF_TRIES_FOR_LOAD_METADATA_LOCK.
   *
   * Because we want concurrent loads to be completed even if they have to wait for the lock
   * therefore taking the default as 100.
   *
   * Example: Concurrent loads will use this to wait to acquire the table status lock.
   */
  public static final int NUMBER_OF_TRIES_FOR_CONCURRENT_LOCK_DEFAULT = 100;
  /**
   * MAX_TIMEOUT_FOR_LOAD_METADATA_LOCK
   *
   * * Example: Concurrent loads will use this to wait to acquire the table status lock.
   */
  public static final int MAX_TIMEOUT_FOR_CONCURRENT_LOCK_DEFAULT = 1;
  /**
   * NUMBER_OF_TRIES_FOR_LOAD_METADATA_LOCK
   */
  @CarbonProperty
  public static final String NUMBER_OF_TRIES_FOR_CONCURRENT_LOCK =
      "carbon.concurrent.lock.retries";
  /**
   * MAX_TIMEOUT_FOR_LOAD_METADATA_LOCK
   */
  @CarbonProperty
  public static final String MAX_TIMEOUT_FOR_CONCURRENT_LOCK =
      "carbon.concurrent.lock.retry.timeout.sec";

  /**
   * NUMBER_OF_TRIES_FOR_CARBON_LOCK
   */
  public static final int NUMBER_OF_TRIES_FOR_CARBON_LOCK_DEFAULT = 3;
  /**
   * MAX_TIMEOUT_FOR_CARBON_LOCK
   */
  public static final int MAX_TIMEOUT_FOR_CARBON_LOCK_DEFAULT = 5;
  /**
   * NUMBER_OF_TRIES_FOR_CARBON_LOCK
   */
  @CarbonProperty
  public static final String NUMBER_OF_TRIES_FOR_CARBON_LOCK =
      "carbon.lock.retries";
  /**
   * MAX_TIMEOUT_FOR_CARBON_LOCK
   */
  @CarbonProperty
  public static final String MAX_TIMEOUT_FOR_CARBON_LOCK =
      "carbon.lock.retry.timeout.sec";

  /**
   * compressor for writing/reading carbondata file
   */
  @CarbonProperty
  public static final String COMPRESSOR = "carbon.column.compressor";

  /**
   * default compressor is snappy
   */
  public static final String DEFAULT_COMPRESSOR = "snappy";

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
  public static final String ARRAY = "array";
  public static final String STRUCT = "struct";
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
  public static final String CARBON_IMPLICIT_COLUMN_SEGMENTID = "segId";
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
   * default load time of the segment
   */
  public static final long SEGMENT_LOAD_TIME_DEFAULT = -1;

  /**
   * Size of Major Compaction in MBs
   */
  @CarbonProperty
  public static final String CARBON_MAJOR_COMPACTION_SIZE = "carbon.major.compaction.size";

  /**
   * By default size of major compaction in MBs.
   */
  public static final String DEFAULT_CARBON_MAJOR_COMPACTION_SIZE = "1024";

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
   * If we are executing insert into query from source table using select statement
   * & loading the same source table concurrently, when select happens on source table
   * during the data load , it gets new record for which dictionary is not generated,
   * So there will be inconsistency. To avoid this condition we can persist the dataframe
   * into MEMORY_AND_DISK and perform insert into operation. By default this value
   * will be false because no need to persist the dataframe in all cases. If user want
   * to run load and insert queries on source table concurrently then user can enable this flag
   */
  @CarbonProperty
  public static final String CARBON_INSERT_PERSIST_ENABLED = "carbon.insert.persist.enable";

  /**
   * by default rdd will not be persisted in the insert case.

   */
  public static final String CARBON_INSERT_PERSIST_ENABLED_DEFAULT = "false";

  /**
   * default name of data base
   */
  public static final String DATABASE_DEFAULT_NAME = "default";

  // tblproperties
  public static final String COLUMN_GROUPS = "column_groups";
  public static final String DICTIONARY_EXCLUDE = "dictionary_exclude";
  public static final String DICTIONARY_INCLUDE = "dictionary_include";
  /**
   * key for dictionary path
   */
  public static final String DICTIONARY_PATH = "dictionary_path";
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
  // table property name of major compaction size
  public static final String TABLE_MAJOR_COMPACTION_SIZE = "major_compaction_size";
  // table property name of auto load merge
  public static final String TABLE_AUTO_LOAD_MERGE = "auto_load_merge";
  // table property name of compaction level threshold
  public static final String TABLE_COMPACTION_LEVEL_THRESHOLD = "compaction_level_threshold";
  // table property name of preserve segments numbers while compaction
  public static final String TABLE_COMPACTION_PRESERVE_SEGMENTS = "compaction_preserve_segments";
  // table property name of allowed compaction days while compaction
  public static final String TABLE_ALLOWED_COMPACTION_DAYS = "allowed_compaction_days";

  /**
   * 16 mb size
   */
  public static final long CARBON_16MB = 16 * 1024 * 1024;
  /**
   * 256 mb size
   */
  public static final long CARBON_256MB = 256 * 1024 * 1024;

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
   * S3LOCK TYPE
   */
  public static final String CARBON_LOCK_TYPE_S3 = "S3LOCK";

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
  public static final String LEVEL2_COMPACTION_INDEX = ".2";

  /**
   * Indicates compaction
   */
  public static final String COMPACTION_KEY_WORD = "COMPACTION";

  /**
   * Indicates alter partition
   */
  public static String ALTER_PARTITION_KEY_WORD = "ALTER_PARTITION";

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
  public static final String minorCompactionRequiredFile = "compactionRequired_minor";

  /**
   * File created in case of major compaction request
   */
  public static final String majorCompactionRequiredFile = "compactionRequired_major";

  /**
   * @Deprecated : This property has been deprecated.
   * Property for enabling system level compaction lock.1 compaction can run at once.
   */
  @CarbonProperty
  public static final String ENABLE_CONCURRENT_COMPACTION = "carbon.concurrent.compaction";

  /**
   * Default value of Property for enabling system level compaction lock.1 compaction can run
   * at once.
   */
  public static final String DEFAULT_ENABLE_CONCURRENT_COMPACTION = "true";

  /**
   * Compaction system level lock folder.
   */
  public static final String SYSTEM_LEVEL_COMPACTION_LOCK_FOLDER = "SystemCompactionLock";

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
   * Index file name will end with this extension when update.
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
  public static final String UPDATED_COL_EXTENSION = "-updatedColumn";

  /**
   * appending the key to differentiate the update flow with insert flow.
   */
  public static final String RDDUTIL_UPDATE_KEY = "UPDATE_";

  /**
   * to determine to use the rdd persist or not.
   */
  @CarbonProperty
  public static final String isPersistEnabled = "carbon.update.persist.enable";

  /**
   * for enabling or disabling Horizontal Compaction.
   */
  @CarbonProperty
  public static final String isHorizontalCompactionEnabled = "carbon.horizontal.compaction.enable";

  /**
   * Default value for HorizontalCompaction is true.
   */
  public static final String defaultIsHorizontalCompactionEnabled = "true";

  /**
   * by default rdd will be persisted in the update case.
   */
  public static final String defaultValueIsPersistEnabled = "true";

  /**
   * Which storage level to persist dataset when updating data
   * with 'carbon.update.persist.enable'='true'
   */
  @CarbonProperty
  public static final String CARBON_UPDATE_STORAGE_LEVEL =
      "carbon.update.storage.level";

  /**
   * The default value(MEMORY_AND_DISK) is the same as the default storage level of Dataset.
   * Unlike `RDD.cache()`, the default storage level is set to be `MEMORY_AND_DISK` because
   * recomputing the in-memory columnar representation of the underlying table is expensive.
   *
   * if user's executor has less memory, set the CARBON_UPDATE_STORAGE_LEVEL
   * to MEMORY_AND_DISK_SER or other storage level to correspond to different environment.
   * You can get more recommendations about storage level in spark website:
   * http://spark.apache.org/docs/latest/rdd-programming-guide.html#rdd-persistence.
   */
  public static final String CARBON_UPDATE_STORAGE_LEVEL_DEFAULT = "MEMORY_AND_DISK";

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
  public static final int DEFAULT_MAX_NUMBER_OF_COLUMNS = 20000;

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

  @CarbonProperty
  public static final String UNSAFE_WORKING_MEMORY_IN_MB = "carbon.unsafe.working.memory.in.mb";
  public static final String UNSAFE_WORKING_MEMORY_IN_MB_DEFAULT = "512";
  /**
   * Sorts the data in batches and writes the batch data to store with index file.
   */
  @CarbonProperty
  public static final String LOAD_SORT_SCOPE = "carbon.load.sort.scope";

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

  @CarbonProperty
  public static final String CARBON_CUSTOM_BLOCK_DISTRIBUTION = "carbon.custom.block.distribution";

  public static final int DICTIONARY_DEFAULT_CARDINALITY = 1;
  @CarbonProperty
  public static final String SPARK_SCHEMA_STRING_LENGTH_THRESHOLD =
      "spark.sql.sources.schemaStringLengthThreshold";

  public static final int SPARK_SCHEMA_STRING_LENGTH_THRESHOLD_DEFAULT = 4000;

  @CarbonProperty
  public static final String CARBON_BAD_RECORDS_ACTION = "carbon.bad.records.action";

  /**
   * FAIL action will fail the load in case of bad records in loading data
   */
  public static final String CARBON_BAD_RECORDS_ACTION_DEFAULT = "FAIL";

  public static final String ENABLE_HIVE_SCHEMA_META_STORE = "spark.carbon.hive.schema.store";

  public static final String ENABLE_HIVE_SCHEMA_META_STORE_DEFAULT = "false";

  /**
   * There is more often that in production uses different drivers for load and queries. So in case
   * of load driver user should set this property to enable loader specific clean up.
   */
  public static final String DATA_MANAGEMENT_DRIVER = "spark.carbon.datamanagement.driver";

  public static final String DATA_MANAGEMENT_DRIVER_DEFAULT = "true";

  public static final String CARBON_SESSIONSTATE_CLASSNAME = "spark.carbon.sessionstate.classname";

  @CarbonProperty
  public static final String CARBON_LEASE_RECOVERY_RETRY_COUNT =
      "carbon.lease.recovery.retry.count";
  @CarbonProperty
  public static final String CARBON_LEASE_RECOVERY_RETRY_INTERVAL =
      "carbon.lease.recovery.retry.interval";

  @CarbonProperty
  public static final String CARBON_SECURE_DICTIONARY_SERVER =
      "carbon.secure.dictionary.server";

  public static final String CARBON_SECURE_DICTIONARY_SERVER_DEFAULT = "true";

  /**
   * whether to use multi directories when loading data,
   * the main purpose is to avoid single-disk-hot-spot
   */
  @CarbonProperty
  public static final String CARBON_USE_MULTI_TEMP_DIR = "carbon.use.multiple.temp.dir";

  /**
   * default value for multi temp dir
   */
  public static final String CARBON_USE_MULTI_TEMP_DIR_DEFAULT = "false";

  /**
   * name of compressor to compress sort temp files
   */
  @CarbonProperty
  public static final String CARBON_SORT_TEMP_COMPRESSOR = "carbon.sort.temp.compressor";

  /**
   * The optional values are 'SNAPPY','GZIP','BZIP2','LZ4'.
   * By default, empty means that Carbondata will not compress the sort temp files.
   */
  public static final String CARBON_SORT_TEMP_COMPRESSOR_DEFAULT = "";
  /**
   * Which storage level to persist rdd when sort_scope=global_sort
   */
  @CarbonProperty
  public static final String CARBON_GLOBAL_SORT_RDD_STORAGE_LEVEL =
      "carbon.global.sort.rdd.storage.level";

  /**
   * The default value(MEMORY_ONLY) is designed for executors with big memory, if user's executor
   * has less memory, set the CARBON_GLOBAL_SORT_RDD_STORAGE_LEVEL to MEMORY_AND_DISK_SER or
   * other storage level to correspond to different environment.
   * You can get more recommendations about storage level in spark website:
   * http://spark.apache.org/docs/latest/rdd-programming-guide.html#rdd-persistence.
   */
  public static final String CARBON_GLOBAL_SORT_RDD_STORAGE_LEVEL_DEFAULT = "MEMORY_ONLY";

  /**
   * property for configuring parallelism per segment when doing an update. Increase this
   * value will avoid data screw problem for a large segment.
   * Refer to CARBONDATA-1373 for more details.
   */
  @CarbonProperty
  public static final String CARBON_UPDATE_SEGMENT_PARALLELISM =
      "carbon.update.segment.parallelism";

  /**
   * In default we will not optimize the update
   */
  public static final String CARBON_UPDATE_SEGMENT_PARALLELISM_DEFAULT = "1";

  public static final String USE_DISTRIBUTED_DATAMAP = "carbon.enable.distributed.datamap";

  public static final String USE_DISTRIBUTED_DATAMAP_DEFAULT = "false";

  /**
   * This property defines how the tasks are splitted/combined and launch spark tasks during query
   */
  @CarbonProperty
  public static final String CARBON_TASK_DISTRIBUTION = "carbon.task.distribution";

  /**
   * It combines the available blocks as per the maximum available tasks in the cluster.
   */
  public static final String CARBON_TASK_DISTRIBUTION_CUSTOM = "custom";

  /**
   * It creates the splits as per the number of blocks/carbondata files available for query.
   */
  public static final String CARBON_TASK_DISTRIBUTION_BLOCK = "block";

  /**
   * It creates the splits as per the number of blocklets available for query.
   */
  public static final String CARBON_TASK_DISTRIBUTION_BLOCKLET = "blocklet";

  /**
   * It merges all the small files and create tasks as per the configurable partition size.
   */
  public static final String CARBON_TASK_DISTRIBUTION_MERGE_FILES = "merge_small_files";

  /**
   * Default task distribution.
   */
  public static final String CARBON_TASK_DISTRIBUTION_DEFAULT = CARBON_TASK_DISTRIBUTION_BLOCK;


  /**
   * The property to configure the mdt file folder path, earlier it was pointing to the
   * fixed carbon store path. This is needed in case of the federation setup when user removes
   * the fixedtorepath namesevice
   */
  @CarbonProperty
  public static final String CARBON_UPDATE_SYNC_FOLDER = "carbon.update.sync.folder";

  public static final String CARBON_UPDATE_SYNC_FOLDER_DEFAULT = "/tmp/carbondata";

  /**
   * this will be used to pass bitset value in filter to another filter for
   * faster execution of filter query
   */
  @CarbonProperty
  public static final String BITSET_PIPE_LINE = "carbon.use.bitset.pipe.line";

  /**
   * this will be used to provide comment for table
   */
  public static final String TABLE_COMMENT = "comment";

  /**
   * this will be used to provide comment for table
   */
  public static final String COLUMN_COMMENT = "comment";

  public static final String BITSET_PIPE_LINE_DEFAULT = "true";

  /**
   * It is internal configuration and used only for test purpose.
   * It will merge the carbon index files with in the segment to single segment.
   */
  public static final String CARBON_MERGE_INDEX_IN_SEGMENT = "carbon.merge.index.in.segment";

  public static final String CARBON_MERGE_INDEX_IN_SEGMENT_DEFAULT = "true";

  /*
   * The total size of carbon data
   */
  public static final String CARBON_TOTAL_DATA_SIZE = "datasize";

  /**
   * The total size of carbon index
   */
  public static final String CARBON_TOTAL_INDEX_SIZE = "indexsize";

  /**
   * ENABLE_CALCULATE_DATA_INDEX_SIZE
   */
  @CarbonProperty public static final String ENABLE_CALCULATE_SIZE = "carbon.enable.calculate.size";

  /**
   * DEFAULT_ENABLE_CALCULATE_DATA_INDEX_SIZE
   */
  @CarbonProperty public static final String DEFAULT_ENABLE_CALCULATE_SIZE = "true";

  public static final String TABLE_DATA_SIZE = "Table Data Size";

  public static final String TABLE_INDEX_SIZE = "Table Index Size";

  public static final String LAST_UPDATE_TIME = "Last Update Time";

  /**
   * this will be used to skip / ignore empty lines while loading
   */
  @CarbonProperty public static final String CARBON_SKIP_EMPTY_LINE = "carbon.skip.empty.line";

  public static final String CARBON_SKIP_EMPTY_LINE_DEFAULT = "false";

  /**
   * if the byte size of streaming segment reach this value,
   * the system will create a new stream segment
   */
  public static final String HANDOFF_SIZE = "carbon.streaming.segment.max.size";

  /**
   * enable auto handoff streaming segment
   */
  public static final String ENABLE_AUTO_HANDOFF = "carbon.streaming.auto.handoff.enabled";

  public static final String ENABLE_AUTO_HANDOFF_DEFAULT = "true";

  /**
   * the min handoff size of streaming segment, the unit is byte
   */
  public static final long HANDOFF_SIZE_MIN = 1024L * 1024 * 64;

  /**
   * the default handoff size of streaming segment, the unit is byte
   */
  public static final long HANDOFF_SIZE_DEFAULT = 1024L * 1024 * 1024;

  /**
   * It allows queries on hive metastore directly along with filter information, otherwise first
   * fetches all partitions from hive and apply filters on it.
   */
  @CarbonProperty
  public static final String CARBON_READ_PARTITION_HIVE_DIRECT =
      "carbon.read.partition.hive.direct";
  public static final String CARBON_READ_PARTITION_HIVE_DIRECT_DEFAULT = "true";

  // As Short data type is used for storing the length of a column during data processing hence
  // the maximum characters that can be supported should be less than Short max value
  public static final int MAX_CHARS_PER_COLUMN_DEFAULT = 32000;

  /**
   * Enabling page level reader for compaction reduces the memory usage while compacting more
   * number of segments. It allows reading only page by page instead of reaing whole blocklet to
   * memory.
   */
  @CarbonProperty
  public static final String CARBON_ENABLE_PAGE_LEVEL_READER_IN_COMPACTION =
      "carbon.enable.page.level.reader.in.compaction";

  public static final String CARBON_ENABLE_PAGE_LEVEL_READER_IN_COMPACTION_DEFAULT = "true";

  private CarbonCommonConstants() {
  }
}


