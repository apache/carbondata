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

import org.apache.carbondata.common.annotations.InterfaceStability;
import org.apache.carbondata.core.util.Property;
import org.apache.carbondata.core.util.annotations.CarbonProperty;

public final class CarbonCommonConstants {

  private CarbonCommonConstants() {
  }
  //////////////////////////////////////////////////////////////////////////////////////////
  // System level property start here
  //////////////////////////////////////////////////////////////////////////////////////////
  // System level property is the global property for CarbonData
  // application, these properties are stored in a singleton instance
  // so that all processing logic in CarbonData uses the same
  // property value

  /**
   * location of the carbon member, hierarchy and fact files
   */
  @CarbonProperty
  public static final String STORE_LOCATION = "carbon.storelocation";

  public static final Property BLOCKLET_SIZE = Property.buildIntProperty()
      .key("carbon.blocklet.size")
      .defaultValue(120000)
      .minValue(2000)
      .maxValue(12000000)
      .doc("blocklet size in carbon file")
      .build();

  public static final Property CARBON_PROPERTIES_FILE_PATH = Property.buildStringProperty()
      .key("carbon.properties.filepath")
      .defaultValue("../../../conf/carbon.properties")
      .doc("carbon properties file path")
      .build();

  public static final Property CARBON_DDL_BASE_HDFS_URL = Property.buildStringProperty()
      .key("carbon.ddl.base.hdfs.url")
      .defaultValue("")
      .doc("CARBON_DDL_BASE_HDFS_URL")
      .build();

  public static final Property CARBON_BADRECORDS_LOC = Property.buildStringProperty()
      .key("carbon.badRecords.location")
      .defaultValue("")
      .doc("CARBON_BADRECORDS_LOCATION")
      .build();

  public static final Property CARBON_TIMESTAMP_FORMAT =
      Property.buildStringProperty()
          .key("carbon.timestamp.format")
          .defaultValue("yyyy-MM-dd HH:mm:ss")
          .doc("Property for specifying the format of TIMESTAMP data type column. " +
              "e.g. yyyy/MM/dd HH:mm:ss, or using default value")
          .build();

  public static final Property CARBON_DATE_FORMAT = Property.buildStringProperty()
      .key("carbon.date.format")
      .defaultValue("yyyy-MM-dd")
      .doc("Property for specifying the format of DATE data type column." +
          " e.g. yyyy/MM/dd , or using default value")
      .build();

  public static final Property COMPRESSOR = Property.buildStringProperty()
      .key("carbon.column.compressor")
      .defaultValue("snappy")
      .doc("compressor for writing/reading CarbonData file")
      .build();

  public static final Property LOCK_TYPE = Property.buildStringProperty()
      .key("carbon.lock.type")
      .defaultValue("LOCALLOCK")
      .doc("ZOOKEEPER_ENABLE_LOCK if this is set to true then zookeeper will" +
          " be used to handle locking mechanism of carbon")
      .build();

  public static final Property LOCK_PATH = Property.buildStringProperty()
      .key("carbon.lock.path")
      .defaultValue("")
      .doc("Specifies the path where the lock files have to be created. " +
          "By default, lock files are created in table path.")
      .build();

  public static final Property ENABLE_XXHASH = Property.buildBooleanProperty()
      .key("carbon.enableXXHash")
      .defaultValue(true)
      .doc("xxhash algorithm property for hashmap")
      .build();


  public static final Property LOCAL_DICTIONARY_SYSTEM_ENABLE =
      Property.buildBooleanProperty()
          .key("carbon.local.dictionary.enable")
          .defaultValue(true)
          .doc("System property to enable or disable local dictionary generation")
          .build();

  public static final Property LOCAL_DICTIONARY_DECODER_BASED_FALLBACK =
      Property.buildBooleanProperty()
          .key("carbon.local.dictionary.decoder.fallback")
          .doc("System property to enable or disable decoder based local dictionary fallback")
          .defaultValue(true)
          .build();

  /**
   * zookeeper url key
   */
  @CarbonProperty
  public static final String ZOOKEEPER_URL = "spark.deploy.zookeeper.url";

  public static final Property CARBON_DATA_FILE_VERSION =
      Property.buildStringProperty()
          .key("carbon.data.file.version")
          .defaultValue("V3")
          .doc("carbon data file version property")
          .build();

  public static final Property SPARK_SCHEMA_STRING_LENGTH_THRESHOLD =
      Property.buildIntProperty()
          .key("spark.sql.sources.schemaStringLengthThreshold")
          .defaultValue(4000)
          .doc("")
          .build();

  public static final Property CARBON_BAD_RECORDS_ACTION = Property.buildStringProperty()
      .key("carbon.bad.records.action")
      .defaultValue("FAIL")
      .doc("CarbonData in addition to identifying the bad records, can take certain " +
          "actions on such data. This configuration can have four types of actions for " +
          "bad records namely FORCE, REDIRECT, IGNORE and FAIL. If set to FORCE then it" +
          " auto-corrects the data by storing the bad records as NULL. If set to REDIRECT " +
          "then bad records are written to the raw CSV instead of being loaded. " +
          "If set to IGNORE then bad records are neither loaded nor written to the raw CSV. " +
          "If set to FAIL then data loading fails if any bad records are found. " +
          "Default value is FAIL, FAIL action will fail the load in case of " +
          "bad records in loading data")
      .build();

  public static final Property ENABLE_HIVE_SCHEMA_META_STORE =
      Property.buildBooleanProperty()
          .key("spark.carbon.hive.schema.store")
          .defaultValue(false)
          .doc("")
          .build();

  public static final Property DATA_MANAGEMENT_DRIVER = Property.buildBooleanProperty()
      .key("spark.carbon.datamanagement.driver")
      .defaultValue(true)
      .doc("There is more often that in production uses different drivers for load " +
          "and queries. So in case of load driver user should set this property " +
          "to enable loader specific clean up.")
      .build();

  public static final String CARBON_SESSIONSTATE_CLASSNAME = "spark.carbon.sessionstate.classname";

  public static final Property CARBON_SQLASTBUILDER_CLASSNAME =
      Property.buildStringProperty()
          .key("spark.carbon.sqlastbuilder.classname")
          .defaultValue("org.apache.spark.sql.hive.CarbonSqlAstBuilder")
          .doc("This property will be used to configure the sqlastbuilder class.")
          .build();

  public static final Property CARBON_LEASE_RECOVERY_RETRY_COUNT =
      Property.buildIntProperty()
          .key("carbon.lease.recovery.retry.count")
          .defaultValue(5)
          .minValue(1)
          .maxValue(50)
          .doc("")
          .build();

  public static final Property CARBON_LEASE_RECOVERY_RETRY_INTERVAL =
      Property.buildIntProperty()
          .key("carbon.lease.recovery.retry.interval")
          .defaultValue(1000)
          .minValue(1000)
          .maxValue(10000)
          .doc("")
          .build();

  public static final Property CARBON_SECURE_DICTIONARY_SERVER =
      Property.buildBooleanProperty()
          .key("carbon.secure.dictionary.server")
          .defaultValue(true)
          .doc("")
          .build();

  public static final Property ENABLE_CALCULATE_SIZE = Property.buildBooleanProperty()
      .key("carbon.enable.calculate.size")
      .defaultValue(true)
      .doc("ENABLE_CALCULATE_DATA_INDEX_SIZE")
      .build();

  public static final Property CARBON_SKIP_EMPTY_LINE = Property.buildBooleanProperty()
      .key("carbon.skip.empty.line")
      .defaultValue(false)
      .doc("this will be used to skip / ignore empty lines while loading")
      .build();

  public static final Property CARBON_SEGMENT_LOCK_FILES_PRESERVE_HOURS =
      Property.buildIntProperty()
          .key("carbon.segment.lock.files.preserve.hours")
          .defaultValue(48)
          .doc("Currently the segment lock files are not deleted immediately when unlock, " +
              "this value indicates the number of hours the segment lock files will " +
              "be preserved. default value is 2 days")
          .build();

  public static final Property CARBON_INVISIBLE_SEGMENTS_PRESERVE_COUNT =
      Property.buildIntProperty()
          .key("carbon.invisible.segments.preserve.count")
          .defaultValue(200)
          .doc("The number of invisible segment info which will be preserved in " +
              "tablestatus file, if it exceeds this value, they will be removed " +
              "and write to tablestatus.history file. Default value is 200, it means " +
              "that it will preserve 200 invisible segment info in tablestatus file. " +
              "The size of one segment info is about 500 bytes, so the size of " +
              "tablestatus file will remain at 100KB.")
          .build();

  /**
   * System older location to store system level data like datamap schema and status files.
   */
  public static final String CARBON_SYSTEM_FOLDER_LOCATION = "carbon.system.folder.location";

  public static final Property CARBON_MERGE_INDEX_IN_SEGMENT =
      Property.buildBooleanProperty()
          .key("carbon.merge.index.in.segment")
          .defaultValue(true)
          .doc("It is internal configuration and used only for test purpose. " +
              "It will merge the carbon index files with in the segment to " +
              "single segment.")
          .build();


  public static final Property CARBON_MINMAX_ALLOWED_BYTE_COUNT =
      Property.buildIntProperty()
          .key("carbon.minmax.allowed.byte.count")
          .defaultValue(200)
          .doc("property to be used for specifying the max byte limit for string/varchar" +
              " data type till where storing min/max in data file will be considered")
          .build();

  //////////////////////////////////////////////////////////////////////////////////////////
  // Table level property start here
  //////////////////////////////////////////////////////////////////////////////////////////
  // Table level property is the table property for Carbon table

  // Flat folder support on table. when it is true all CarbonData files store directly under table
  // path instead of sub folders.
  public static final String FLAT_FOLDER = "flat_folder";

  /**
   * FLAT_FOLDER default value
   */
  public static final String DEFAULT_FLAT_FOLDER = "false";

  public static final String DICTIONARY_EXCLUDE = "dictionary_exclude";

  public static final String DICTIONARY_INCLUDE = "dictionary_include";

  public static final String LONG_STRING_COLUMNS = "long_string_columns";

  /**
   * Table property to enable or disable local dictionary generation
   */
  public static final String LOCAL_DICTIONARY_ENABLE = "local_dictionary_enable";

  /**
   * default value for local dictionary generation
   */
  public static final String LOCAL_DICTIONARY_ENABLE_DEFAULT = "true";

  /**
   * Threshold value for local dictionary
   */
  public static final String LOCAL_DICTIONARY_THRESHOLD = "local_dictionary_threshold";

  /**
   * default value for local dictionary
   */
  public static final String LOCAL_DICTIONARY_THRESHOLD_DEFAULT = "10000";

  /**
   * max dictionary threshold
   */
  public static final int LOCAL_DICTIONARY_MAX = 100000;

  /**
   * min dictionary threshold
   */
  public static final int LOCAL_DICTIONARY_MIN = 1000;

  /**
   * Table property to specify the columns for which local dictionary needs to be generated.
   */
  public static final String LOCAL_DICTIONARY_INCLUDE = "local_dictionary_include";

  /**
   * Table property to specify the columns for which local dictionary should not be to be generated.
   */
  public static final String LOCAL_DICTIONARY_EXCLUDE = "local_dictionary_exclude";

  /**
   * DMPROPERTY for Index DataMap, like lucene, bloomfilter DataMap,
   * to indicate a list of column name to be indexed
   */
  public static final String INDEX_COLUMNS = "INDEX_COLUMNS";

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

  // default block size in MB
  public static final String TABLE_BLOCK_SIZE_DEFAULT = "1024";

  // table blocklet size in MB
  public static final String TABLE_BLOCKLET_SIZE = "table_blocklet_size";

  // default blocklet size value in MB
  public static final String TABLE_BLOCKLET_SIZE_DEFAULT = "64";

  /**
   * set in column level to disable inverted index
   *
   * @Deprecated :This property is deprecated, it is kept just for compatibility
   */
  public static final String NO_INVERTED_INDEX = "no_inverted_index";
  // set in column level to enable inverted index
  public static final String INVERTED_INDEX = "inverted_index";
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
   * property to be specified for caching min/max of required columns
   */
  public static final String COLUMN_META_CACHE = "column_meta_cache";

  /**
   * property to be specified for caching level (Block/Blocket)
   */
  public static final String CACHE_LEVEL = "cache_level";

  /**
   * default value for cache level
   */
  public static final String CACHE_LEVEL_DEFAULT_VALUE = "BLOCK";

  //////////////////////////////////////////////////////////////////////////////////////////
  // Data loading parameter start here
  //////////////////////////////////////////////////////////////////////////////////////////

  public static final Property NUM_CORES_LOADING = Property.buildIntProperty()
      .key("carbon.number.of.cores.while.loading")
      .defaultValue(2)
      .dynamicConfigurable(true)
      .doc("Number of cores to be used while loading")
      .build();

  public static final Property NUM_CORES_COMPACTING = Property.buildIntProperty()
      .key("carbon.number.of.cores.while.compacting")
      .defaultValue(2)
      .dynamicConfigurable(true)
      .doc("Number of cores to be used while compacting")
      .build();

  public static final Property NUM_CORES_ALT_PARTITION = Property.buildIntProperty()
      .key("carbon.number.of.cores.while.altPartition")
      .defaultValue(2)
      .doc("Number of cores to be used while alter partition")
      .build();

  /**
   * BYTEBUFFER_SIZE
   */
  public static final int BYTEBUFFER_SIZE = 24 * 1024;

  /**
   * SORT_TEMP_FILE_LOCATION
   */
  public static final String SORT_TEMP_FILE_LOCATION = "sortrowtmp";

  public static final Property SORT_INTERMEDIATE_FILES_LIMIT = Property.buildIntProperty()
      .key("carbon.sort.intermediate.files.limit")
      .defaultValue(20)
      .minValue(2)
      .maxValue(50)
      .doc("SORT_INTERMEDIATE_FILES_LIMIT")
      .build();

  public static final Property CARBON_SORT_FILE_WRITE_BUFFER_SIZE = Property.buildIntProperty()
      .key("carbon.sort.file.write.buffer.size")
      .defaultValue(16384)
      .minValue(10240)
      .maxValue(10485760)
      .doc("SORT_FILE_WRITE_BUFFER_SIZE")
      .build();

  public static final Property CSV_READ_BUFFER_SIZE = Property.buildIntProperty()
      .key("carbon.csv.read.buffersize.byte")
      .defaultValue(1048576)
      .minValue(10240)
      .maxValue(10485760)
      .doc("CSV_READ_BUFFER_SIZE, default value is 1mb." +
          "min value for csv read buffer size, 10 kb." +
          "max value for csv read buffer size, 10 mb")
      .build();

  public static final Property CARBON_MERGE_SORT_READER_THREAD = Property.buildIntProperty()
      .key("carbon.merge.sort.reader.thread")
      .defaultValue(3)
      .doc("CARBON_MERGE_SORT_READER_THREAD")
      .build();

  public static final Property ENABLE_DATA_LOADING_STATISTICS = Property.buildBooleanProperty()
      .key("enable.data.loading.statistics")
      .defaultValue(false)
      .doc("TIME_STAT_UTIL_TYPE")
      .build();

  public static final Property NUMBER_OF_TRIES_FOR_CONCURRENT_LOCK = Property.buildIntProperty()
      .key("carbon.concurrent.lock.retries")
      .defaultValue(100)
      .doc("NUMBER_OF_TRIES_FOR_LOAD_METADATA_LOCK. " +
          "Because we want concurrent loads to be completed even if they have to wait " +
          "for the lock therefore taking the default as 100." +
          " Example: Concurrent loads will use this to wait to acquire the table status lock.")
      .build();

  public static final Property MAX_TIMEOUT_FOR_CONCURRENT_LOCK = Property.buildIntProperty()
      .key("carbon.concurrent.lock.retry.timeout.sec")
      .defaultValue(1)
      .doc("MAX_TIMEOUT_FOR_LOAD_METADATA_LOCK." +
          "Example: Concurrent loads will use this to wait to acquire the table status lock.")
      .build();

  public static final Property NUMBER_OF_TRIES_FOR_CARBON_LOCK = Property.buildIntProperty()
      .key("carbon.lock.retries")
      .defaultValue(3)
      .doc("NUMBER_OF_TRIES_FOR_CARBON_LOCK")
      .build();

  public static final Property MAX_TIMEOUT_FOR_CARBON_LOCK = Property.buildIntProperty()
      .key("carbon.lock.retry.timeout.sec")
      .defaultValue(5)
      .doc("MAX_TIMEOUT_FOR_CARBON_LOCK")
      .build();

  public static final Property CARBON_PREFETCH_BUFFERSIZE = Property.buildIntProperty()
      .key("carbon.prefetch.buffersize")
      .defaultValue(1000)
      .doc("CARBON_PREFETCH_BUFFERSIZE")
      .build();

  /**
   * CARBON_PREFETCH_IN_MERGE
   */
  public static final boolean CARBON_PREFETCH_IN_MERGE_VALUE = false;

  public static final Property ENABLE_AUTO_LOAD_MERGE = Property.buildBooleanProperty()
      .key("carbon.enable.auto.load.merge")
      .defaultValue(false)
      .dynamicConfigurable(true)
      .doc("ENABLE_AUTO_LOAD_MERGE")
      .build();

  public static final Property DICTIONARY_ONE_CHUNK_SIZE = Property.buildIntProperty()
      .key("carbon.dictionary.chunk.size")
      .defaultValue(10000)
      .doc("maximum dictionary chunk size that can be kept" +
          " in memory while writing dictionary file")
      .build();

  public static final Property DICTIONARY_WORKER_THREADS = Property.buildIntProperty()
      .key("dictionary.worker.threads")
      .defaultValue(1)
      .doc("Dictionary Server Worker Threads")
      .build();

  public static final Property CARBON_MAJOR_COMPACTION_SIZE = Property.buildIntProperty()
      .key("carbon.major.compaction.size")
      .defaultValue(1024)
      .dynamicConfigurable(true)
      .doc("Size of Major Compaction in MBs")
      .build();

  public static final Property PRESERVE_LATEST_SEGMENTS_NUMBER = Property.buildIntProperty()
      .key("carbon.numberof.preserve.segments")
      .defaultValue(0)
      .doc("This property is used to tell how many segments to be preserved from merging." +
          "If preserve property is enabled then 2 segments will be preserved.")
      .build();

  public static final Property DAYS_ALLOWED_TO_COMPACT = Property.buildIntProperty()
      .key("carbon.allowed.compaction.days")
      .defaultValue(0)
      .doc("This property will determine the loads of how many days can be compacted." +
          "Default value of 1 day loads can be compacted")
      .build();

  public static final Property CARBON_BLOCK_META_RESERVED_SPACE = Property.buildIntProperty()
      .key("carbon.block.meta.size.reserved.percentage")
      .defaultValue(10)
      .doc("space reserved for writing block meta data in carbon data file")
      .build();

  public static final Property CARBON_MERGE_SORT_PREFETCH = Property.buildBooleanProperty()
      .key("carbon.merge.sort.prefetch")
      .defaultValue(true)
      .doc("this variable is to enable/disable prefetch of data during merge sort while " +
          "reading data from sort temp files")
      .build();

  @InterfaceStability.Evolving
  public static final Property CARBON_INSERT_PERSIST_ENABLED = Property.buildBooleanProperty()
      .key("carbon.insert.persist.enable")
      .defaultValue(false)
      .doc("If we are executing insert into query from source table using select statement " +
          "& loading the same source table concurrently, when select happens on source table " +
          "during the data load , it gets new record for which dictionary is not generated," +
          " So there will be inconsistency. To avoid this condition we can persist the dataframe" +
          " into MEMORY_AND_DISK and perform insert into operation. By default this value will be" +
          " false because no need to persist the dataframe in all cases. If user want to run load" +
          " and insert queries on source table concurrently then user can enable this flag")
      .build();

  @InterfaceStability.Evolving
  public static final Property CARBON_INSERT_STORAGE_LEVEL = Property.buildStringProperty()
      .key("carbon.insert.storage.level")
      .defaultValue("MEMORY_AND_DISK")
      .doc("Which storage level to persist dataset when insert into data" +
          " with 'carbon.insert.persist.enable'='true'" +
          " The default value(MEMORY_AND_DISK) is the same as the default storage level of" +
          " Dataset. Unlike `RDD.cache()`, the default storage level is set to be " +
          "`MEMORY_AND_DISK` because recomputing the in-memory columnar representation of" +
          " the underlying table is expensive. if user's executor has less memory, set the " +
          "CARBON_INSERT_STORAGE_LEVEL to MEMORY_AND_DISK_SER or other storage level to " +
          "correspond to different environment." +
          " You can get more recommendations about storage level in spark website:" +
          " http://spark.apache.org/docs/latest/rdd-programming-guide.html#rdd-persistence.")
      .build();

  public static final Property COMPACTION_SEGMENT_LEVEL_THRESHOLD =
      Property.buildStringProperty()
          .key("carbon.compaction.level.threshold")
          .defaultValue("4,3")
          .dynamicConfigurable(true)
          .doc("Number of unmerged segments to be merged." +
              "Default count for Number of segments to be merged in levels is 4,3")
          .build();

  public static final Property UPDATE_DELTAFILE_COUNT_THRESHOLD_IUD_COMPACTION =
      Property.buildIntProperty()
          .key("carbon.horizontal.update.compaction.threshold")
          .defaultValue(1)
          .doc("Number of Update Delta files which is the Threshold for IUD compaction. Only " +
              "accepted Range is 0 - 10000. Outside this range system will pick default value." +
              "Default count of segments which act as a threshold for IUD compaction merge.")
          .build();

  public static final Property DELETE_DELTAFILE_COUNT_THRESHOLD_IUD_COMPACTION =
      Property.buildIntProperty()
          .key("carbon.horizontal.delete.compaction.threshold")
          .defaultValue(1)
          .doc("Number of Delete Delta files which is the Threshold for IUD compaction. Only " +
              "accepted Range is 0 - 10000. Outside this range system will pick default value.")
          .build();

  public static final Property ENABLE_CONCURRENT_COMPACTION = Property.buildBooleanProperty()
      .key("carbon.concurrent.compaction")
      .defaultValue(true)
      .doc(" @Deprecated : This property has been deprecated." +
          "Property for enabling system level compaction lock.1 compaction can run at once." +
          " Default value of Property for enabling system level compaction lock.1 compaction" +
          " can run at once.")
      .build();

  public static final Property DATA_LOAD_BATCH_SIZE = Property.buildIntProperty()
      .key("DATA_LOAD_BATCH_SIZE")
      .defaultValue(1000)
      .doc("This batch size is used to send rows from load step to another step in batches.")
      .build();

  @InterfaceStability.Evolving
  public static final Property CARBON_UPDATE_PERSIST_ENABLE = Property.buildBooleanProperty()
      .key("carbon.update.persist.enable")
      .defaultValue(true)
      .doc("to determine to use the rdd persist or not." +
          "by default rdd will be persisted in the update case.")
      .build();

  public static final Property CARBON_HORIZONTAL_COMPACTION_ENABLE = Property.buildBooleanProperty()
      .key("carbon.horizontal.compaction.enable")
      .defaultValue(true)
      .doc("for enabling or disabling Horizontal Compaction.")
      .build();

  @InterfaceStability.Evolving
  public static final Property CARBON_UPDATE_STORAGE_LEVEL =
      Property.buildStringProperty()
          .key("carbon.update.storage.level")
          .defaultValue("MEMORY_AND_DISK")
          .doc("Which storage level to persist dataset when updating data" +
              " with 'carbon.update.persist.enable'='true'" +
              " The default value(MEMORY_AND_DISK) is the same as the default storage level of " +
              " Dataset. Unlike `RDD.cache()`, the default storage level is set to be " +
              " `MEMORY_AND_DISK` because recomputing the in-memory columnar representation of " +
              " the underlying table is expensive. if user's executor has less memory, set the " +
              "CARBON_UPDATE_STORAGE_LEVEL to MEMORY_AND_DISK_SER or other storage level to " +
              "correspond to different environment." +
              " You can get more recommendations about storage level in spark website:" +
              " http://spark.apache.org/docs/latest/rdd-programming-guide.html#rdd-persistence.")
          .build();

  public static final Property ENABLE_UNSAFE_COLUMN_PAGE = Property.buildBooleanProperty()
      .key("enable.unsafe.columnpage")
      .defaultValue(true)
      .doc("to enable unsafe column page")
      .build();

  public static final Property ENABLE_UNSAFE_SORT = Property.buildBooleanProperty()
      .key("enable.unsafe.sort")
      .defaultValue(true)
      .dynamicConfigurable(true)
      .doc("to enable offheap sort")
      .build();

  public static final Property ENABLE_OFFHEAP_SORT = Property.buildBooleanProperty()
      .key("enable.offheap.sort")
      .defaultValue(true)
      .dynamicConfigurable(true)
      .doc("to enable offheap sort")
      .build();

  public static final Property ENABLE_INMEMORY_MERGE_SORT = Property.buildBooleanProperty()
      .key("enable.inmemory.merge.sort")
      .defaultValue(false)
      .doc("to enable inmemory merge sort")
      .build();

  public static final Property OFFHEAP_SORT_CHUNK_SIZE_IN_MB = Property.buildIntProperty()
      .key("offheap.sort.chunk.size.inmb")
      .defaultValue(64)
      .doc("")
      .build();

  public static final Property UNSAFE_WORKING_MEMORY_IN_MB = Property.buildIntProperty()
      .key("carbon.unsafe.working.memory.in.mb")
      .defaultValue(512)
      .doc("")
      .build();

  public static final Property UNSAFE_DRIVER_WORKING_MEMORY_IN_MB = Property.buildStringProperty()
      .key("carbon.unsafe.driver.working.memory.in.mb")
      .defaultValue("")
      .doc("")
      .build();

  public static final Property LOAD_SORT_SCOPE = Property.buildStringProperty()
      .key("carbon.load.sort.scope")
      .defaultValue("NO_SORT")
      .doc("Sorts the data in batches and writes the batch data to store with index file." +
          " If set to BATCH_SORT, the sorting scope is smaller and more index tree will be " +
          " created, thus loading is faster but query maybe slower." +
          " If set to LOCAL_SORT, the sorting scope is bigger and one index tree per data " +
          " node will be created, thus loading is slower but query is faster." +
          " If set to GLOBAL_SORT, the sorting scope is bigger and one index tree per " +
          " task will be created, thus loading is slower but query is faster.")
      .build();

  public static final Property LOAD_BATCH_SORT_SIZE_INMB = Property.buildIntProperty()
      .key("carbon.load.batch.sort.size.inmb")
      .defaultValue(0)
      .doc("Size of batch data to keep in memory, as a thumb rule it supposed" +
          " to be less than 45% of sort.inmemory.size.inmb otherwise it may spill " +
          "intermediate data to disk")
      .build();

  public static final Property LOAD_GLOBAL_SORT_PARTITIONS = Property.buildIntProperty()
      .key("carbon.load.global.sort.partitions")
      .defaultValue(0)
      .doc("The Number of partitions to use when shuffling data for sort. If user don't " +
          "configurate or configurate it less than 1, it uses the number of map tasks as " +
          "reduce tasks. In general, we recommend 2-3 tasks per CPU core in your cluster.")
      .build();

  public static final Property DICTIONARY_SERVER_PORT = Property.buildIntProperty()
      .key("carbon.dictionary.server.port")
      .defaultValue(2030)
      .doc("carbon dictionary server port")
      .build();

  public static final Property USE_PREFETCH_WHILE_LOADING = Property.buildBooleanProperty()
      .key("carbon.loading.prefetch")
      .defaultValue(false)
      .doc("whether to prefetch data while loading.")
      .build();

  public static final Property CARBON_LOADING_USE_YARN_LOCAL_DIR = Property.buildBooleanProperty()
      .key("carbon.use.local.dir")
      .defaultValue(true)
      .doc("for loading, whether to use yarn's local dir the main purpose " +
          " is to avoid single disk hot spot")
      .build();

  public static final Property CARBON_SORT_TEMP_COMPRESSOR = Property.buildStringProperty()
      .key("carbon.sort.temp.compressor")
      .defaultValue("SNAPPY")
      .doc("name of compressor to compress sort temp files." +
          " The optional values are 'SNAPPY','GZIP','BZIP2','LZ4','ZSTD' and empty." +
          " Specially, empty means that Carbondata will not compress the sort temp files.")
      .build();

  @InterfaceStability.Evolving
  public static final Property CARBON_GLOBAL_SORT_RDD_STORAGE_LEVEL = Property.buildStringProperty()
      .key("carbon.global.sort.rdd.storage.level")
      .defaultValue("MEMORY_ONLY")
      .doc("Which storage level to persist rdd when sort_scope=global_sort. " +
          " The default value(MEMORY_ONLY) is designed for executors with big memory, if user's " +
          "executor has less memory, set the CARBON_GLOBAL_SORT_RDD_STORAGE_LEVEL to " +
          "MEMORY_AND_DISK_SER or other storage level to correspond to different environment." +
          " You can get more recommendations about storage level in spark website:" +
          " http://spark.apache.org/docs/latest/rdd-programming-guide.html#rdd-persistence.")
      .build();

  public static final Property CARBON_UPDATE_SEGMENT_PARALLELISM = Property.buildIntProperty()
      .key("carbon.update.segment.parallelism")
      .defaultValue(1)
      .doc("property for configuring parallelism per segment when doing an update. Increase this" +
          " value will avoid data screw problem for a large segment." +
          " Refer to CarbonData-1373 for more details. " +
          " In default we will not optimize the update")
      .build();

  public static final Property CARBON_UPDATE_SYNC_FOLDER = Property.buildStringProperty()
      .key("carbon.update.sync.folder")
      .defaultValue("/tmp/carbondata")
      .doc("The property to configure the mdt file folder path, earlier it was pointing to the" +
          " fixed carbon store path. This is needed in case of the federation setup when user " +
          " removes the fixed store path nameService")
      .build();

  /**
   * Configures the parser/writer to limit the length of displayed contents being parsed/written
   * in the exception message when an error occurs.
   * Here {@code 0} means no exceptions will include the content being manipulated in their
   * attributes.
   */
  public static final int CARBON_ERROR_CONTENT_LENGTH = 0;

  public static final Property HANDOFF_SIZE = Property.buildLongProperty()
      .key("carbon.streaming.segment.max.size")
      .defaultValue(1024L * 1024 * 1024)
      .doc("if the byte size of streaming segment reach this value," +
          " the system will create a new stream segment." +
          " the default handoff size of streaming segment, the unit is byte")
      .build();

  /**
   * the min handoff size of streaming segment, the unit is byte
   */
  public static final long HANDOFF_SIZE_MIN = 1024L * 1024 * 64;

  public static final Property ENABLE_AUTO_HANDOFF = Property.buildBooleanProperty()
      .key("carbon.streaming.auto.handoff.enabled")
      .defaultValue(true)
      .doc("enable auto handoff streaming segment")
      .build();

  @InterfaceStability.Evolving
  public static final Property CARBON_ENABLE_PAGE_LEVEL_READER_IN_COMPACTION = Property
      .buildBooleanProperty()
      .key("carbon.enable.page.level.reader.in.compaction")
      .defaultValue(false)
      .doc("Enabling page level reader for compaction reduces the memory usage while compacting " +
          " more number of segments. It allows reading only page by page instead of reaing whole " +
          "blocklet to memory." +
          " Note: If this property is set to true it can impact compaction performance " +
          "as IO will increase")
      .build();

  public static final Property CARBON_SORT_STORAGE_INMEMORY_IN_MB = Property.buildIntProperty()
      .key("carbon.sort.storage.inmemory.size.inmb")
      .defaultValue(512)
      .doc("")
      .build();

  public static final Property CARBON_COMPACTION_PREFETCH_ENABLE = Property.buildBooleanProperty()
      .key("carbon.compaction.prefetch.enable")
      .defaultValue(false)
      .doc("whether to enable prefetch for rowbatch to enhance row reconstruction " +
          " during compaction")
      .build();

  public static final Property CARBON_LUCENE_COMPRESSION_MODE = Property.buildStringProperty()
      .key("carbon.lucene.compression.mode")
      .defaultValue("speed")
      .doc("compression mode used by lucene for index writing, this conf will be passed " +
          " to lucene writer while writing index files." +
          " default lucene index compression mode, in this mode writing speed will be less and " +
          " speed is given priority, another mode is compression mode, where the index size is" +
          " given importance to make it less and not the index writing speed.")
      .build();

  public static final Property CARBON_LOAD_MIN_SIZE_INMB = Property.buildIntProperty()
      .key("load_min_size_inmb")
      .defaultValue(0)
      .doc("The node loads the smallest amount of data")
      .build();

  public static final Property IN_MEMORY_FOR_SORT_DATA_IN_MB = Property.buildIntProperty()
      .key("sort.inmemory.size.inmb")
      .defaultValue(1024)
      .doc("")
      .build();

  public static final Property SORT_SIZE = Property.buildIntProperty()
      .key("carbon.sort.size")
      .defaultValue(100000)
      .minValue(1000)
      .doc("carbon sort size")
      .build();

  //////////////////////////////////////////////////////////////////////////////////////////
  // Query parameter start here
  //////////////////////////////////////////////////////////////////////////////////////////

  public static final Property CARBON_INPUT_SEGMENTS = Property.buildStringProperty()
      .key("carbon.input.segments.")
      .defaultValue("*")
      .dynamicConfigurable(true)
      .doc("set the segment ids to query from the table")
      .build();

  public static final Property ENABLE_QUERY_STATISTICS = Property.buildBooleanProperty()
      .key("enable.query.statistics")
      .defaultValue(false)
      .doc("ENABLE_QUERY_STATISTICS")
      .build();

  public static final Property MAX_QUERY_EXECUTION_TIME = Property.buildIntProperty()
      .key("max.query.execution.time")
      .defaultValue(60)
      .doc("MAX_QUERY_EXECUTION_TIME")
      .build();

  public static final Property DETAIL_QUERY_BATCH_SIZE = Property.buildIntProperty()
      .key("carbon.detail.batch.size")
      .defaultValue(100)
      .doc("The batch size of records which returns to client.")
      .build();

  public static final Property CARBON_MAX_DRIVER_LRU_CACHE_SIZE = Property.buildIntProperty()
      .key("carbon.max.driver.lru.cache.size")
      .defaultValue(-1)
      .doc("max driver lru cache size upto which lru cache will be loaded in memory." +
          "max lru cache size default value in MB")
      .build();

  public static final Property CARBON_MAX_EXECUTOR_LRU_CACHE_SIZE = Property.buildIntProperty()
      .key("carbon.max.executor.lru.cache.size")
      .defaultValue(-1)
      .doc("max executor lru cache size upto which lru cache will be loaded in memory." +
          " max lru cache size default value in MB")
      .build();

  public static final Property CARBON_QUERY_MIN_MAX_ENABLED = Property.buildBooleanProperty()
      .key("carbon.enableMinMax")
      .defaultValue(true)
      .doc("property to enable min max during filter query. " +
          "default value to enable min or max during filter query execution")
      .build();

  public static final Property ENABLE_VECTOR_READER = Property.buildBooleanProperty()
      .key("carbon.enable.vector.reader")
      .defaultValue(true)
      .dynamicConfigurable(true)
      .doc("Spark added vector processing to optimize cpu cache miss and there by " +
          "increase the query performance. This configuration enables to fetch data " +
          "as columnar batch of size 4*1024 rows instead of fetching data row by row " +
          "and provide it to spark so that there is improvement in  select queries performance.")
      .build();

  public static final Property IS_DRIVER_INSTANCE = Property.buildBooleanProperty()
      .key("is.driver.instance")
      .defaultValue(false)
      .doc("property to set is IS_DRIVER_INSTANCE")
      .build();

  public static final Property ENABLE_UNSAFE_IN_QUERY_EXECUTION = Property.buildBooleanProperty()
      .key("enable.unsafe.in.query.processing")
      .defaultValue(false)
      .dynamicConfigurable(true)
      .doc("property for enabling unsafe based query processing")
      .build();

  public static final Property CARBON_CUSTOM_BLOCK_DISTRIBUTION = Property.buildBooleanProperty()
      .key("carbon.custom.block.distribution")
      .dynamicConfigurable(true)
      .defaultValue(false)
      .doc("To set carbon task distribution.")
      .build();

  public static final Property CARBON_TASK_DISTRIBUTION = Property.buildStringProperty()
      .key("carbon.task.distribution")
      .defaultValue("block")
      .doc("This property defines how the tasks are split/combined " +
          "and launch spark tasks during query")
      .build();

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

  public static final Property BITSET_PIPE_LINE = Property.buildBooleanProperty()
      .key("carbon.use.bitset.pipe.line")
      .defaultValue(true)
      .doc("this will be used to pass bitset value in filter to another filter for" +
          "faster execution of filter query")
      .build();

  public static final Property CARBON_SCHEDULER_MIN_REGISTERED_RESOURCES_RATIO =
      Property.buildDoubleProperty()
          .key("carbon.scheduler.min.registered.resources.ratio")
          .defaultValue(0.8d)
          .minValue(0.1d)
          .maxValue(1.0d)
          .doc("minimum required registered resource for starting block distribution")
          .build();

  public static final Property CARBON_DYNAMIC_ALLOCATION_SCHEDULER_TIMEOUT =
      Property.buildIntProperty()
          .key("carbon.dynamical.location.scheduler.timeout")
          .defaultValue(5)
          .minValue(5)
          .maxValue(15)
          .doc("To define how much time scheduler should wait for the " +
              "resource in dynamic allocation.")
          .build();

  /**
   * time for which thread will sleep and check again if the requested number of executors
   * have been started
   */
  public static final int CARBON_DYNAMIC_ALLOCATION_SCHEDULER_THREAD_SLEEP_TIME = 250;

  public static final Property CARBON_READ_PARTITION_HIVE_DIRECT = Property.buildBooleanProperty()
      .key("carbon.read.partition.hive.direct")
      .defaultValue(true)
      .doc("It allows queries on hive metastore directly along with filter information, " +
          "otherwise first fetches all partitions from hive and apply filters on it.")
      .build();

  public static final Property CARBON_HEAP_MEMORY_POOLING_THRESHOLD_BYTES =
      Property.buildIntProperty()
          .key("carbon.heap.memory.pooling.threshold.bytes")
          .defaultValue(1048576)
          .doc("If the heap memory allocations of the given size is " +
              "greater or equal than this value, it should go through the pooling mechanism. " +
              "But if set this size to -1, it should not go through the pooling mechanism. " +
              "Default value is 1048576(1MB, the same as Spark). Unit: byte.")
          .build();

  public static final Property CARBON_PUSH_ROW_FILTERS_FOR_VECTOR =
      Property.buildBooleanProperty()
          .key("carbon.push.rowfilters.for.vector")
          .defaultValue(false)
          .dynamicConfigurable(true)
          .doc("When enabled complete row filters will be handled by carbon in case of vector. " +
              "If it is disabled then only page level pruning will be done by carbon and row " +
              "level filtering will be done by spark for vector. There is no change in flow " +
              "for non-vector based queries.")
          .build();

  public static final Property CARBON_MAX_DRIVER_THREADS_FOR_BLOCK_PRUNING =
      Property.buildIntProperty()
          .key("carbon.max.driver.threads.for.block.pruning")
          .defaultValue(4)
          .doc("max driver threads used for block pruning [1 to 4 threads]")
          .build();

  // block prune in multi-thread if files size more than 100K files.
  public static final int CARBON_DRIVER_PRUNING_MULTI_THREAD_ENABLE_FILES_COUNT = 100000;

  //////////////////////////////////////////////////////////////////////////////////////////
  // Datamap parameter start here
  //////////////////////////////////////////////////////////////////////////////////////////

  @InterfaceStability.Unstable
  public static final Property CARBON_DATAMAP_VISIBLE = Property.buildStringProperty()
      .key("carbon.datamap.visible.")
      .defaultValue("default.*")  // TODO: confirm
      .dynamicConfigurable(true)
      .doc("key prefix for set command. 'carbon.datamap.visible.dbName.tableName.dmName = false'" +
          " means that the query on 'dbName.table' will not use the datamap 'dmName'")
      .build();

  public static final Property VALIDATE_CARBON_INPUT_SEGMENTS = Property.buildBooleanProperty()
      .key("validate.carbon.input.segments.")
      .defaultValue(true)   //TODO: confirm
      .dynamicConfigurable(true)
      .doc("Fetch and validate the segments." +
          " Used for aggregate table load as segment validation is not required.")
      .build();

  public static final Property IS_INTERNAL_LOAD_CALL = Property.buildBooleanProperty()
      .key("is.internal.load.call")
      .defaultValue(false)
      .doc("Whether load/insert command is fired internally or by the user." +
          " Used to block load/insert on pre-aggregate if fired by user")
      .build();

  public static final Property USE_DISTRIBUTED_DATAMAP = Property.buildBooleanProperty()
      .key("carbon.enable.distributed.datamap")
      .defaultValue(false)
      .doc("")
      .build();

  public static final Property SUPPORT_DIRECT_QUERY_ON_DATAMAP = Property.buildBooleanProperty()
      .key("carbon.query.directQueryOnDataMap.enabled")
      .defaultValue(false)
      .dynamicConfigurable(true)
      .doc("")
      .build();

  public static final Property VALIDATE_DIRECT_QUERY_ON_DATAMAP = Property.buildBooleanProperty()
      .key("carbon.query.validate.direct.query.on.datamap")
      .defaultValue(true)
      .doc("")
      .build();

  public static final Property CARBON_SHOW_DATAMAPS = Property.buildBooleanProperty()
      .key("carbon.query.show.datamaps")
      .defaultValue(true)
      .doc("")
      .build();

  public static final Property CARBON_LOAD_DATAMAPS_PARALLEL = Property.buildBooleanProperty()
      .key("carbon.load.datamaps.parallel.")
      .defaultValue(false)  //TODO: confirm
      .dynamicConfigurable(true)
      .doc("Property to enable parallel datamap loading for a table")
      .build();

  public static final Property CARBON_LUCENE_INDEX_STOP_WORDS = Property.buildBooleanProperty()
      .key("carbon.lucene.index.stop.words")
      .defaultValue(false)
      .doc("by default lucene will not store or create index for stop words like \"is\",\"the\", " +
          " if this property is set to true lucene will index for stop words also and gives " +
          " result for the filter with stop words(example: TEXT_MATCH('description':'the'))")
      .build();

  //////////////////////////////////////////////////////////////////////////////////////////
  // Constant value start here
  //////////////////////////////////////////////////////////////////////////////////////////

  /**
   * ZOOKEEPER_LOCATION this is the location in zookeeper file system where locks are created.
   * mechanism of carbon
   */
  public static final String ZOOKEEPER_LOCATION = "/CarbonLocks";

  /**
   * min block size in MB
   */
  public static final int BLOCK_SIZE_MIN_VAL = 1;

  /**
   * max block size in MB
   */
  public static final int BLOCK_SIZE_MAX_VAL = 2048;

  /**
   * CARBON_TIMESTAMP
   */
  public static final String CARBON_TIMESTAMP = "dd-MM-yyyy HH:mm:ss";

  /**
   * CARBON_TIMESTAMP
   */
  public static final String CARBON_TIMESTAMP_MILLIS = "dd-MM-yyyy HH:mm:ss:SSS";

  /**
   * surrogate value of null
   */
  public static final int DICT_VALUE_NULL = 1;

  /**
   * surrogate value of null for direct dictionary
   */
  public static final int DIRECT_DICT_VALUE_NULL = 1;

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
   * CARDINALITY_INCREMENT_DEFAULT_VALUE
   */
  public static final int CARDINALITY_INCREMENT_VALUE_DEFAULT_VAL = 10;

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
   * Empty byte array
   */
  public static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

  /**
   * FILE STATUS IN-PROGRESS
   */
  public static final String FILE_INPROGRESS_STATUS = ".inprogress";

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
   * MERGERD_EXTENSION
   */
  public static final String MERGERD_EXTENSION = ".merge";

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
   * SEMICOLON_SPC_CHARACTER
   */
  public static final String SEMICOLON_SPC_CHARACTER = ";#!@:SEMIC:@!#;";

  /**
   * AMPERSAND_SPC_CHARACTER
   */
  public static final String AMPERSAND_SPC_CHARACTER = "&#!@:AMPER:@!#&";

  /**
   * ATTHERATE_SPC_CHARACTER
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
   * DEFAULT_COLLECTION_SIZE
   */
  public static final int DEFAULT_COLLECTION_SIZE = 16;

  /**
   * DIMENSION_SPLIT_VALUE_IN_COLUMNAR_DEFAULTVALUE
   */
  public static final String DIMENSION_SPLIT_VALUE_IN_COLUMNAR_DEFAULTVALUE = "1";

  public static final String IS_FULLY_FILLED_BITS_DEFAULT_VALUE = "true";

  /**
   * CONSTANT_SIZE_TEN
   */
  public static final int CONSTANT_SIZE_TEN = 10;

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
   * ARRAY separator
   */
  public static final String ARRAY_SEPARATOR = "\001";
  public static final String STRING = "String";
  public static final String SHORT = "Short";
  public static final String TIMESTAMP = "Timestamp";
  public static final String ARRAY = "array";
  public static final String STRUCT = "struct";
  public static final String MAP = "map";
  public static final String FROM = "from";

  /**
   * TABLE UPDATE STATUS FILENAME
   */
  public static final String TABLEUPDATESTATUS_FILENAME = "tableupdatestatus";

  /**
   * SPILL_OVER_DISK_PATH
   */
  public static final String SCHEMAS_MODIFIED_TIME_FILE = "modifiedTime.mdt";
  public static final String DEFAULT_INVISIBLE_DUMMY_MEASURE = "default_dummy_measure";
  public static final String CARBON_IMPLICIT_COLUMN_POSITIONID = "positionId";
  public static final String CARBON_IMPLICIT_COLUMN_TUPLEID = "tupleId";
  public static final String CARBON_IMPLICIT_COLUMN_SEGMENTID = "segId";
  public static final String POSITION_REFERENCE = "positionReference";

  /**
   * implicit column which will be added to each carbon table
   */
  public static final String POSITION_ID = "positionId";

  /**
   * TEMPWRITEFILEEXTENSION
   */
  public static final String TEMPWRITEFILEEXTENSION = ".write";

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
   * default name of data base
   */
  public static final String DATABASE_DEFAULT_NAME = "default";

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
   * default location of the carbon metastore db
   */
  public static final String METASTORE_LOCATION_DEFAULT_VAL = "../carbon.metastore";

  /**
   * hive connection url
   */
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
  public static final String ALTER_PARTITION_KEY_WORD = "ALTER_PARTITION";

  /**
   * hdfs temporary directory key
   */
  public static final String HDFS_TEMP_LOCATION = "hadoop.tmp.dir";

  /**
   * File created in case of minor compaction request
   */
  public static final String minorCompactionRequiredFile = "compactionRequired_minor";

  /**
   * File created in case of major compaction request
   */
  public static final String majorCompactionRequiredFile = "compactionRequired_major";

  /**
   * Compaction system level lock folder.
   */
  public static final String SYSTEM_LEVEL_COMPACTION_LOCK_FOLDER = "SystemCompactionLock";

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
   * data file version header
   */
  @Deprecated
  public static final String CARBON_DATA_VERSION_HEADER = "CARBONDATAVERSION#";

  /**
   * Maximum no of column supported
   */
  public static final int DEFAULT_MAX_NUMBER_OF_COLUMNS = 20000;

  public static final String MINOR = "minor";
  public static final String MAJOR = "major";

  public static final int DICTIONARY_DEFAULT_CARDINALITY = 1;

  /**
   * this will be used to provide comment for table
   */
  public static final String TABLE_COMMENT = "comment";

  /**
   * this will be used to provide comment for table
   */
  public static final String COLUMN_COMMENT = "comment";

  /*
   * The total size of carbon data
   */
  public static final String CARBON_TOTAL_DATA_SIZE = "datasize";

  /**
   * The total size of carbon index
   */
  public static final String CARBON_TOTAL_INDEX_SIZE = "indexsize";
  public static final String TABLE_DATA_SIZE = "Table Data Size";

  public static final String TABLE_INDEX_SIZE = "Table Index Size";

  public static final String LAST_UPDATE_TIME = "Last Update Time";

  // As Short data type is used for storing the length of a column during data processing hence
  // the maximum characters that can be supported should be less than Short max value
  public static final int MAX_CHARS_PER_COLUMN_DEFAULT = 32000;
  // todo: use infinity first, will switch later
  public static final int MAX_CHARS_PER_COLUMN_INFINITY = -1;
  public static final short LOCAL_DICT_ENCODED_BYTEARRAY_SIZE = 3;
  public static final int CARBON_MINMAX_ALLOWED_BYTE_COUNT_MIN = 10;
  public static final int CARBON_MINMAX_ALLOWED_BYTE_COUNT_MAX = 1000;

  /**
   * Written by detail to be written in CarbonData footer for better maintanability
   */
  public static final String CARBON_WRITTEN_BY_FOOTER_INFO = "written_by";

  /**
   * CarbonData project version used while writing the CarbonData file
   */
  public static final String CARBON_WRITTEN_VERSION = "version";

  /**
   * property to set the appName of who is going to write the CarbonData
   */
  public static final String CARBON_WRITTEN_BY_APPNAME = "carbon.writtenby.app.name";

  /**
   * When more global dictionary columns are there then there is issue in generating codegen to them
   * and it slows down the query.So we limit to 100 for now
   */
  public static final int CARBON_ALLOW_DIRECT_FILL_DICT_COLS_LIMIT = 100;

  //////////////////////////////////////////////////////////////////////////////////////////
  // Unused constants and parameters start here
  //////////////////////////////////////////////////////////////////////////////////////////

  /**
   * BYTE_ENCODING
   */
  public static final String BYTE_ENCODING = "ISO-8859-1";

  /**
   * measure meta data file name
   */
  public static final String MEASURE_METADATA_FILE_NAME = "/msrMetaData_";

  /**
   * DUMMY aggregation function
   */
  public static final String DUMMY = "dummy";

  /**
   * Bytes for string 0, it is used in codegen in case of null values.
   */
  public static final byte[] ZERO_BYTE_ARRAY = "0" .getBytes(Charset.forName(DEFAULT_CHARSET));

  /**
   * HIERARCHY_FILE_EXTENSION
   */
  public static final String HIERARCHY_FILE_EXTENSION = ".hierarchy";

  /**
   * CARBON_RESULT_SIZE_DEFAULT
   */
  public static final String LEVEL_FILE_EXTENSION = ".level";

  /**
   * MEASUREMETADATA_FILE_EXT
   */
  public static final String MEASUREMETADATA_FILE_EXT = ".msrmetadata";

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
   * BAD_RECORD_KEY_VALUE
   */
  public static final String BAD_RECORD_KEY = "BADRECORD";

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
   * LEVEL_METADATA_FILE
   */
  public static final String LEVEL_METADATA_FILE = "levelmetadata_";

  /**
   * DASH
   */
  public static final String DASH = "-";

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
   * default value in size for cache size of bloom filter datamap.
   */
  public static final String CARBON_QUERY_DATAMAP_BLOOM_CACHE_SIZE_DEFAULT_VAL = "512";

}
