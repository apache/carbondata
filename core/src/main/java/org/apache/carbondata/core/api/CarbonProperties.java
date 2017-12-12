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

package org.apache.carbondata.core.api;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.constants.CarbonV3DataFormatConstants;
import org.apache.carbondata.core.metadata.ColumnarFormatVersion;
import org.apache.carbondata.core.util.CarbonSessionInfo;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.SessionParams;
import org.apache.carbondata.core.util.ThreadLocalSessionInfo;

import org.apache.hadoop.conf.Configuration;

public final class CarbonProperties {

  // A property with default value and documentation, it is created by PropertyBuilder
  public static class RuntimeProperty<T> {
    private String name;
    private T value;
    private Class<T> type;
    private String doc;
    private T defaultValue;

    private RuntimeProperty(Class<T> type, String name, T defaultValue, String doc) {
      this.type = type;
      this.name = name;
      this.defaultValue = defaultValue;
      this.doc = doc;
    }

    private RuntimeProperty<T> addTo(Map<String, RuntimeProperty> properties) {
      properties.put(name, this);
      return this;
    }

    /**
     * Return property name
     */
    public String getName() {
      return name;
    }

    /**
     * Set `value`, and convert it to the exact type of the property,
     * throw IllegalArgumentException or NumberFormatException if conversion failed.
     * This function does not accept null.
     */
    private void setValue(T value) {
      if (value == null) {
        throw new IllegalArgumentException("Invalid property value: null");
      }
      // check the input type and target type, convert the value based on type
      if (type == Integer.class) {
        int intValue;
        if (value instanceof Integer) {
          intValue = (Integer) value;
        } else if (value instanceof String){
          intValue = Integer.parseInt((String)value);
        } else {
          throw new IllegalArgumentException("Invalid type: " + value.getClass());
        }
        if (intValue <= 0) {
          throw new IllegalArgumentException("Invalid int property value: " + value);
        }
        this.value = type.cast(intValue);
      } else if (type == Boolean.class) {
        boolean boolValue;
        if (value instanceof Boolean) {
          boolValue = (Boolean) value;
        } else if (value instanceof String) {
          if (!CarbonUtil.validateBoolean((String) value)) {
            throw new IllegalArgumentException("Invalid boolean property value: " + value);
          }
          boolValue = Boolean.parseBoolean((String)value);
        } else {
          throw new IllegalArgumentException("Invalid boolean property value: " + value);
        }
        this.value = type.cast(boolValue);
      } else {
        assert (type == String.class);
        if (!(value instanceof String)) {
          throw new IllegalArgumentException("Invalid type: " + value.getClass());
        }
        this.value = type.cast(value);
      }
    }

    // set property value to use default value
    private void setToDefault() {
      setValue(this.defaultValue);
    }

    /**
     * Return property value, could be null if user doesn't set this property
     */
    public T getValue() {
      return value;
    }

    /**
     * Return property value if not null, otherwise return default value
     */
    public T getOrDefault() {
      if (value == null) {
        return defaultValue;
      } else {
        return value;
      }
    }

    /**
     * Return default value
     */
    public T getDefaultValue() {
      return defaultValue;
    }

    public String getDoc() {
      return doc;
    }
  }

  private static class PropertyBuilder<T> {
    private String name;
    private Class<T> type;
    private String doc;
    private T defaultValue;

    private PropertyBuilder(Class<T> type, String name) {
      this.type = type;
      this.name = name;
    }
    private PropertyBuilder<T> doc(String doc) {
      this.doc = doc;
      return this;
    }
    private RuntimeProperty<T> createWithDefault(T value) {
      this.defaultValue = type.cast(value);
      return new RuntimeProperty<>(type, name, defaultValue, doc);
    }
  }

  private static Map<String, RuntimeProperty> properties = new HashMap<>();

  private static PropertyBuilder<Integer> newIntProperty(String name) {
    return new PropertyBuilder<>(Integer.class, name);
  }

  private static PropertyBuilder<Boolean> newBooleanProperty(String name) {
    return new PropertyBuilder<>(Boolean.class, name);
  }

  @SuppressWarnings("unchecked")
  private static PropertyBuilder<String> newStringProperty(String name) {
    return new PropertyBuilder<>(String.class, name);
  }

  public static final RuntimeProperty<String> STORE_LOCATION =
      newStringProperty("carbon.storelocation")
          .doc("absolute path of the carbondata store")
          .createWithDefault("")
          .addTo(properties);

  public static final RuntimeProperty<String> STORE_LOCATION_TEMP_PATH =
      newStringProperty("carbon.tempstore.location")
          .doc("")
          .createWithDefault(System.getProperty("java.io.tmpdir"))
          .addTo(properties);

  public static final RuntimeProperty<String> HDFS_TEMP_LOCATION =
      newStringProperty("hadoop.tmp.dir")
          .doc("")
          .createWithDefault(System.getProperty("java.io.tmpdir"))
          .addTo(properties);

  public static final RuntimeProperty<String> LOCK_TYPE =
      newStringProperty("carbon.lock.type")
          .doc("Lock used for table")
          .createWithDefault("LOCALLOCK")
          .addTo(properties);

  public static final RuntimeProperty<String> ZOOKEEPER_URL =
      newStringProperty("spark.deploy.zookeeper.url")
          .doc("")
          .createWithDefault("")
          .addTo(properties);

  public static final RuntimeProperty<Integer> BLOCKLET_SIZE =
      newIntProperty("carbon.blocklet.size")
          .doc("Blocklet size in carbon file")
          .createWithDefault(120000)
          .addTo(properties);

  public static final RuntimeProperty<Integer> NUM_CORES =
      newIntProperty("carbon.number.of.cores")
          .doc("")
          .createWithDefault(2)
          .addTo(properties);

  public static final RuntimeProperty<Integer> NUM_CORES_LOADING =
      newIntProperty("carbon.number.of.cores.while.loading")
          .doc("Number of cores to be used while loading")
          .createWithDefault(2)
          .addTo(properties);

  public static final RuntimeProperty<Integer> SORT_SIZE =
      newIntProperty("carbon.sort.size")
          .doc("")
          .createWithDefault(100000)
          .addTo(properties);

  public static final RuntimeProperty<Integer> SORT_FILE_BUFFER_SIZE =
      newIntProperty("carbon.sort.file.buffer.size")
          .doc("")
          .createWithDefault(-1)
          .addTo(properties);

  public static final RuntimeProperty<Integer> SORT_FILE_WRITE_BUFFER_SIZE =
      newIntProperty("carbon.sort.file.write.buffer.size")
          .doc("")
          .createWithDefault(16384)
          .addTo(properties);

  public static final RuntimeProperty<String> DATA_FILE_VERSION =
      newStringProperty("carbon.data.file.version")
          .doc("carbon data file version number")
          .createWithDefault("V3")
          .addTo(properties);

  public static final RuntimeProperty<Integer> LOAD_GLOBAL_SORT_PARTITIONS =
      newIntProperty("carbon.load.global.sort.partitions")
          .doc("The Number of partitions to use when shuffling data for sort. If user don't configurate or "
              + "configurate it less than 1, it uses the number of map tasks as reduce tasks. In general, we "
              + "recommend 2-3 tasks per CPU core in your cluster")
          .createWithDefault(0)
          .addTo(properties);

  public static final RuntimeProperty<Integer> LOAD_BATCH_SORT_SIZE_INMB =
      newIntProperty("carbon.load.batch.sort.size.inmb")
          .doc("Size of batch data to keep in memory, as a thumb rule it supposed "
              + "to be less than 45% of sort.inmemory.size.inmb otherwise it may spill intermediate data to disk")
          .createWithDefault(0)
          .addTo(properties);

  public static final RuntimeProperty<String> LOAD_SORT_SCOPE =
      newStringProperty("carbon.load.sort.scope")
          .doc("If set to BATCH_SORT, the sorting scope is smaller and more index tree will be created, "
              + "thus loading is faster but query maybe slower. "
              + "If set to LOCAL_SORT, the sorting scope is bigger and one index tree per data node will be "
              + "created, thus loading is slower but query is faster. "
              + "If set to GLOBAL_SORT, the sorting scope is bigger and one index tree per task will be "
              + "created, thus loading is slower but query is faster.")
          .createWithDefault("LOCAL_SORT")
          .addTo(properties);

  public static final RuntimeProperty<String> BAD_RECORDS_LOCATION =
      newStringProperty("carbon.badRecords.location")
          .doc("File path for bad record log")
          .createWithDefault("")
          .addTo(properties);

  public static final RuntimeProperty<String> BAD_RECORDS_ACTION =
      newStringProperty("carbon.badRecords.action")
          .doc("FAIL action will fail the load in case of bad records in loading data")
          .createWithDefault("FAIL")
          .addTo(properties);

  public static final RuntimeProperty<Integer> SORT_INTERMEDIATE_FILES_LIMIT =
      newIntProperty("carbon.sort.intermediate.files.limit")
          .doc("")
          .createWithDefault(20)
          .addTo(properties);

  public static final RuntimeProperty<Integer> OFFHEAP_SORT_CHUNK_SIZE_IN_MB =
      newIntProperty("offheap.sort.chunk.size.inmb")
          .doc("")
          .createWithDefault(64)
          .addTo(properties);

  public static final RuntimeProperty<Integer> DATA_LOAD_BATCH_SIZE =
      newIntProperty("carbon.load.batch.size")
          .doc("Used to send rows from load step to another step in batches")
          .createWithDefault(1000)
          .addTo(properties);

  public static final RuntimeProperty<Integer> NUM_CORES_BLOCK_SORT =
      newIntProperty("carbon.number.of.cores.block.sort")
          .doc("Number of cores to be used for block sort")
          .createWithDefault(7)
          .addTo(properties);

  public static final RuntimeProperty<Integer> CSV_READ_BUFFER_SIZE =
      newIntProperty("carbon.csv.read.buffersize.byte")
          .doc("")
          .createWithDefault(50000)
          .addTo(properties);

  public static final RuntimeProperty<Integer> MERGE_SORT_READER_THREAD =
      newIntProperty("carbon.merge.sort.reader.thread")
          .doc("")
          .createWithDefault(3)
          .addTo(properties);

  public static final RuntimeProperty<Integer> BLOCK_META_RESERVED_SPACE =
      newIntProperty("carbon.block.meta.size.reserved.percentage")
          .doc("space reserved for writing block meta data in carbon data file")
          .createWithDefault(10)
          .addTo(properties);

  public static final RuntimeProperty<Boolean> ENABLE_UNSAFE_SORT =
      newBooleanProperty("enable.unsafe.sort")
          .doc("To enable/ disable unsafe sort during loading, unsafe sort can reduce GC.")
          .createWithDefault(false)
          .addTo(properties);

  public static final RuntimeProperty<Boolean> ENABLE_OFFHEAP_SORT =
      newBooleanProperty("enable.offheap.sort")
          .doc("Offheap sort during data loading can reduce GC.")
          .createWithDefault(true)
          .addTo(properties);

  public static final RuntimeProperty<Boolean> ENABLE_INMEMORY_MERGE_SORT =
      newBooleanProperty("enable.inmemory.merge.sort")
          .doc("")
          .createWithDefault(false)
          .addTo(properties);

  public static final RuntimeProperty<Boolean> ENABLE_PREFETCH_WHILE_LOADING =
      newBooleanProperty("carbon.loading.prefetch")
          .doc("")
          .createWithDefault(false)
          .addTo(properties);

  public static final RuntimeProperty<Boolean> USE_LOCAL_DIR_LOADING =
      newBooleanProperty("carbon.use.local.dir")
          .doc("")
          .createWithDefault(false)
          .addTo(properties);

  public static final RuntimeProperty<Boolean> ENABLE_UNSAFE_COLUMN_PAGE_LOADING =
      newBooleanProperty("enable.unsafe.columnpage")
          .doc("to enable unsafe column page in write step")
          .createWithDefault(false)
          .addTo(properties);

  public static final RuntimeProperty<Boolean> ENABLE_MERGE_SORT_PREFETCH =
      newBooleanProperty("carbon.merge.sort.prefetch")
          .doc("Used to enable/disable prefetch of data during merge sort while "
              + "reading data from sort temp files")
          .createWithDefault(true)
          .addTo(properties);

  public static final RuntimeProperty<Boolean> ENABLE_SORT_TEMP_FILE_COMPRESSION =
      newBooleanProperty("carbon.is.sort.temp.file.compression.enabled")
          .doc("")
          .createWithDefault(false)
          .addTo(properties);

  public static final RuntimeProperty<Integer> SORT_TEMP_FILE_NO_OF_RECORDS_FOR_COMPRESSION =
      newIntProperty("carbon.sort.temp.file.no.of.records.for.compression")
          .doc("")
          .createWithDefault(50)
          .addTo(properties);

  public static final RuntimeProperty<Boolean> USE_MULTI_TEMP_DIR =
      newBooleanProperty("carbon.use.multiple.temp.dir")
          .doc("")
          .createWithDefault(true)
          .addTo(properties);

  public static final RuntimeProperty<String> DDL_BASE_HDFS_URL =
      newStringProperty("carbon.ddl.base.hdfs.url")
          .doc("")
          .createWithDefault("")
          .addTo(properties);

  public static final RuntimeProperty<String> GLOBAL_SORT_RDD_STORAGE_LEVEL =
      newStringProperty("carbon.global.sort.rdd.storage.level")
          .doc("Which storage level to persist rdd when sort_scope=global_sort. " +
          "The default value(MEMORY_ONLY) is designed for executors with big memory, if user's executor "
              + " has less memory, set the CARBON_GLOBAL_SORT_RDD_STORAGE_LEVEL to MEMORY_AND_DISK_SER or "
              + " other storage level to correspond to different environment. "
              + " You can get more recommendations about storage level in spark website: "
              + " http://spark.apache.org/docs/latest/rdd-programming-guide.html#rdd-persistence.")
          .createWithDefault("MEMORY_ONLY")
          .addTo(properties);

  public static final RuntimeProperty<String> UPDATE_STORAGE_LEVEL =
      newStringProperty("carbon.update.storage.level")
          .doc("Which storage level to persist rdd when sort_scope=global_sort. " +
              "The default value(MEMORY_AND_DISK) is the same as the default storage level of Dataset. "
              + "Unlike `RDD.cache()`, the default storage level is set to be `MEMORY_AND_DISK` because "
              + "recomputing the in-memory columnar representation of the underlying table is expensive. "
              + "if user's executor has less memory, set the CARBON_UPDATE_STORAGE_LEVEL "
              + "to MEMORY_AND_DISK_SER or other storage level to correspond to different environment. "
              + "You can get more recommendations about storage level in spark website: "
              + "http://spark.apache.org/docs/latest/rdd-programming-guide.html#rdd-persistence.")
          .createWithDefault("MEMORY_AND_DISK")
          .addTo(properties);


  public static final RuntimeProperty<Integer> UPDATE_SEGMENT_PARALLELISM =
      newIntProperty("carbon.update.segment.parallelism")
          .doc("property for configuring parallelism per segment when doing an update. Increase this "
              + " value will avoid data screw problem for a large segment. "
              + " Refer to CARBONDATA-1373 for more details")
          .createWithDefault(1)
          .addTo(properties);

  public static final RuntimeProperty<Boolean> ENABLE_XXHASH =
      newBooleanProperty("carbon.sort.intermediate.files.limit")
          .doc("Use multi directories when loading data, " +
               "the main purpose is to avoid single-disk-hot-spot")
          .createWithDefault(false)
          .addTo(properties);

  public static final RuntimeProperty<Integer> MAX_EXECUTOR_LRU_CACHE_SIZE =
      newIntProperty("carbon.max.executor.lru.cache.size")
          .doc("max executor lru cache size in MB upto which lru cache will be loaded in memory")
          .createWithDefault(-1)
          .addTo(properties);

  public static final RuntimeProperty<Integer> MAX_DRIVER_LRU_CACHE_SIZE =
      newIntProperty("carbon.max.driver.lru.cache.size")
          .doc("max driver lru cache size in MB upto which lru cache will be loaded in memory")
          .createWithDefault(-1)
          .addTo(properties);


  public static final RuntimeProperty<Integer> DICTIONARY_CHUNK_SIZE =
      newIntProperty("carbon.dictionary.chunk.size")
          .doc("maximum dictionary chunk size that can be kept in memory while writing " +
               "dictionary file")
          .createWithDefault(10000)
          .addTo(properties);

  public static final RuntimeProperty<Integer> DICTIONARY_WORKER_THREADS =
      newIntProperty("dictionary.worker.threads")
          .doc("Dictionary Server Worker Threads")
          .createWithDefault(1)
          .addTo(properties);

  public static final RuntimeProperty<Boolean> ENABLE_DATA_LOADING_STATISTICS =
      newBooleanProperty("enable.data.loading.statistics")
          .doc("")
          .createWithDefault(false)
          .addTo(properties);

  public static final RuntimeProperty<Boolean> ENABLE_CUSTOM_BLOCK_DISTRIBUTION =
      newBooleanProperty("carbon.custom.block.distribution")
          .doc("To enable/ disable carbon custom block distribution.")
          .createWithDefault(false)
          .addTo(properties);

  public static final RuntimeProperty<Boolean> ENABLE_BLOCKLET_DISTRIBUTION =
      newBooleanProperty("carbon.blocklet.distribution")
          .doc("")
          .createWithDefault(true)
          .addTo(properties);

  public static final RuntimeProperty<Boolean> ENABLE_DISTRIBUTED_DATAMAP =
      newBooleanProperty("carbon.enable.distributed.datamap")
          .doc("")
          .createWithDefault(false)
          .addTo(properties);

  public static final RuntimeProperty<Integer> BLOCKLET_SIZE_IN_MB =
      newIntProperty("carbon.blockletgroup.size.in.mb")
          .doc("blocklet size in MB")
          .createWithDefault(64)
          .addTo(properties);

  public static final RuntimeProperty<Integer> NUMBER_OF_COLUMN_TO_READ_IN_IO =
      newIntProperty("number.of.column.to.read.in.io")
          .doc("number of column to be read in one IO in query")
          .createWithDefault(10)
          .addTo(properties);


  public static final RuntimeProperty<Boolean> ENABLE_QUERY_STATISTICS =
      newBooleanProperty("enable.query.statistics")
          .doc("")
          .createWithDefault(false)
          .addTo(properties);


  public static final RuntimeProperty<Boolean> ENABLE_RDD_PERSIST_UPDATE =
      newBooleanProperty("carbon.update.persist.enable")
          .doc("Persist RDD or not while doing data update")
          .createWithDefault(true)
          .addTo(properties);

  public static final RuntimeProperty<Integer> NUM_CORES_COMPACTING =
      newIntProperty("carbon.number.of.cores.while.compacting")
          .doc("Number of cores to be used while compacting")
          .createWithDefault(2)
          .addTo(properties);

  public static final RuntimeProperty<Boolean> ENABLE_HORIZONTAL_COMPACTION =
      newBooleanProperty("carbon.horizontal.compaction.enable")
          .doc("Horizontal Compaction is to merge base file and delta file which can " +
               "improve query performance on table after many updates. This property controls" +
               "whether to do compaction after each update")
          .createWithDefault(true)
          .addTo(properties);

  public static final RuntimeProperty<Boolean> ENABLE_AUTO_LOAD_MERGE =
      newBooleanProperty("carbon.enable.auto.load.merge")
          .doc("")
          .createWithDefault(false)
          .addTo(properties);

  public static final RuntimeProperty<Integer> MAJOR_COMPACTION_SIZE =
      newIntProperty("carbon.major.compaction.size")
          .doc("Size of Major Compaction in MBs")
          .createWithDefault(1024)
          .addTo(properties);

  public static final RuntimeProperty<Integer> PRESERVE_LATEST_SEGMENTS_NUMBER =
      newIntProperty("carbon.numberof.preserve.segments")
          .doc("Used to tell how many segments to be preserved from merging")
          .createWithDefault(0)
          .addTo(properties);

  public static final RuntimeProperty<String> COMPACTION_SEGMENT_LEVEL_THRESHOLD =
      newStringProperty("carbon.compaction.level.threshold")
          .doc("Number of unmerged segments to be merged")
          .createWithDefault("4,3")
          .addTo(properties);

  public static final RuntimeProperty<Boolean> ENABLE_CONCURRENT_COMPACTION =
      newBooleanProperty("carbon.concurrent.compaction")
          .doc("This property has been deprecated. "
              + "Property for enabling system level compaction lock.1 compaction can run at once.")
          .createWithDefault(true)
          .addTo(properties);

  public static final RuntimeProperty<Integer> DAYS_ALLOWED_TO_COMPACT =
      newIntProperty("carbon.allowed.compaction.days")
          .doc("This property will determine the loads of how many days can be compacted.")
          .createWithDefault(0)
          .addTo(properties);

  public static final RuntimeProperty<Integer> UPDATE_DELTAFILE_COUNT_THRESHOLD_IUD_COMPACTION =
      newIntProperty("carbon.horizontal.update.compaction.threshold")
          .doc("Number of Update Delta files which is the Threshold for IUD compaction. " +
               "Only accepted Range is 0 - 10000. Outside this range system will pick default value")
          .createWithDefault(1)
          .addTo(properties);

  public static final RuntimeProperty<Integer> DELETE_DELTAFILE_COUNT_THRESHOLD_IUD_COMPACTION =
      newIntProperty("carbon.horizontal.update.compaction.threshold")
          .doc("Number of Delete Delta files which is the Threshold for IUD compaction. " +
               "Only accepted Range is 0 - 10000. Outside this range system will pick default value")
          .createWithDefault(1)
          .addTo(properties);

  public static final RuntimeProperty<Integer> MAX_QUERY_EXECUTION_TIME =
      newIntProperty("max.query.execution.time")
          .doc("")
          .createWithDefault(-1)
          .addTo(properties);

  public static final RuntimeProperty<Integer> PREFETCH_BUFFERSIZE =
      newIntProperty("carbon.prefetch.buffersize")
          .doc("")
          .createWithDefault(1000)
          .addTo(properties);

  public static final RuntimeProperty<Boolean> ENABLE_VECTOR_READER =
      newBooleanProperty("carbon.enable.vector.reader")
          .doc("")
          .createWithDefault(true)
          .addTo(properties);

  public static final RuntimeProperty<Boolean> ENABLE_UNSAFE_IN_QUERY_EXECUTION =
      newBooleanProperty("enable.unsafe.in.query.processing")
          .doc("")
          .createWithDefault(false)
          .addTo(properties);

  public static final RuntimeProperty<Integer> DETAIL_QUERY_BATCH_SIZE =
      newIntProperty("carbon.detail.batch.size")
          .doc("The batch size of records which returns to client")
          .createWithDefault(100)
          .addTo(properties);

  public static final RuntimeProperty<Boolean> ENABLE_BITSET_PIPE_LINE =
      newBooleanProperty("carbon.use.bitset.pipe.line")
          .doc("Used to pass bitset value in filter to another filter for " +
               "faster execution of filter query")
          .createWithDefault(true)
          .addTo(properties);

  public static final RuntimeProperty<Boolean> ENABLE_CALCULATE_SIZE =
      newBooleanProperty("carbon.enable.calculate.size")
          .doc("")
          .createWithDefault(true)
          .addTo(properties);

  public static final RuntimeProperty<Boolean> SKIP_EMPTY_LINE =
      newBooleanProperty("carbon.skip.empty.line")
          .doc("Used to skip / ignore empty lines while loading")
          .createWithDefault(false)
          .addTo(properties);


  public static final RuntimeProperty<Integer> IN_MEMORY_FOR_SORT_DATA_IN_MB =
      newIntProperty("sort.inmemory.size.inmb")
          .doc("")
          .createWithDefault(1024)
          .addTo(properties);

  public static final RuntimeProperty<Integer> UNSAFE_WORKING_MEMORY_IN_MB =
      newIntProperty("carbon.unsafe.working.memory.in.mb")
          .doc("")
          .createWithDefault(512)
          .addTo(properties);

  public static final RuntimeProperty<String> COMPRESSOR =
      newStringProperty("carbon.column.compressor")
          .doc("compressor for writing/reading carbondata file")
          .createWithDefault("snappy")
          .addTo(properties);

  public static final RuntimeProperty<String> TIMESTAMP_FORMAT =
      newStringProperty("carbon.timestamp.format")
          .doc("Property for specifying the format of TIMESTAMP data type column")
          .createWithDefault("yyyy-MM-dd HH:mm:ss")
          .addTo(properties);

  public static final RuntimeProperty<String> DATE_FORMAT =
      newStringProperty("carbon.date.format")
          .doc("Property for specifying the format of DATE data type column")
          .createWithDefault("yyyy-MM-dd")
          .addTo(properties);



  public static final RuntimeProperty<Integer> DICTIONARY_SERVER_PORT =
      newIntProperty("carbon.dictionary.server.port")
          .doc("")
          .createWithDefault(2030)
          .addTo(properties);

  public static final RuntimeProperty<Boolean> IS_DRIVER_INSTANCE =
      newBooleanProperty("is.driver.instance")
          .doc("")
          .createWithDefault(false)
          .addTo(properties);

  public static final RuntimeProperty<Integer> LEASE_RECOVERY_RETRY_COUNT =
      newIntProperty("carbon.lease.recovery.retry.count")
          .doc("")
          .createWithDefault(5)
          .addTo(properties);

  public static final RuntimeProperty<Integer> LEASE_RECOVERY_RETRY_INTERVAL =
      newIntProperty("carbon.lease.recovery.retry.interval")
          .doc("")
          .createWithDefault(1000)
          .addTo(properties);

  public static final RuntimeProperty<String> UPDATE_SYNC_FOLDER =
      newStringProperty("carbon.update.sync.folder")
          .doc("The property to configure the mdt file folder path, earlier it was pointing " +
              "to the fixed carbon store path. This is needed in case of the federation setup " +
              "when user removes the fixedtorepath namesevice")
          .createWithDefault("/tmp/carbondata")
          .addTo(properties);

  public static final RuntimeProperty<Integer> HANDOFF_SIZE =
      newIntProperty("carbon.streaming.segment.max.size")
          .doc("If the size of streaming segment reach this value in MB, the system will " +
               "create a new stream segment")
          .createWithDefault(1024)
          .addTo(properties);

  public static final RuntimeProperty<Integer> EXECUTOR_STARTUP_TIMEOUT =
      newIntProperty("carbon.max.executor.startup.timeout")
          .doc("Maximum waiting time (in seconds) for a query for requested executors to be started")
          .createWithDefault(5)
          .addTo(properties);

  public static final RuntimeProperty<String> CUTOFF_TIMESTAMP =
      newStringProperty("carbon.cutOffTimestamp")
          .doc("The property to set the date to be considered as start date for calculating the timestamp. " +
              "java counts the number of milliseconds from  start of \"January 1, 1970\", this property is " +
              "customized the start of position. for example \"January 1, 2000\"")
          .createWithDefault("")
          .addTo(properties);

  public static final RuntimeProperty<String> TIME_GRANULARITY =
      newStringProperty("carbon.timegranularity")
          .doc("The property to set the timestamp (ie milis) conversion to the SECOND, MINUTE, HOUR or DAY level")
          .createWithDefault("SECOND")
          .addTo(properties);

  public static final RuntimeProperty<Integer> NUMBER_OF_TRIES_FOR_LOAD_METADATA_LOCK =
      newIntProperty("carbon.load.metadata.lock.retries")
          .doc("")
          .createWithDefault(3)
          .addTo(properties);

  public static final RuntimeProperty<Integer> MAX_TIMEOUT_FOR_LOAD_METADATA_LOCK =
      newIntProperty("carbon.load.metadata.lock.retry.timeout.sec")
          .doc("")
          .createWithDefault(5)
          .addTo(properties);


  /**
   * Load property from file and add them to property map in memory
   */
  @SuppressWarnings("unchecked")
  private void initProperties() {
    String filePath = System.getProperty("carbon.properties.filepath");
    if (null == filePath) {
      filePath = CarbonCommonConstants.CARBON_PROPERTIES_FILE_PATH;
    }
    try {
      Properties loadedProperties = loadFromFile(filePath);
      Enumeration keys = loadedProperties.keys();
      while (keys.hasMoreElements()) {
        String key = (String) keys.nextElement();
        if (properties.containsKey(key)) {
          properties.get(key).setValue(loadedProperties.get(key));
        } else {
          // user specified a non-exist property
          LOGGER.error("Trying to add invalid property from file: " + key);
        }
      }
    } catch (IOException e) {
      LOGGER.error("Error while reading property file: " + filePath + " . Use default properties.");
    }

    print();
  }

  private Properties loadFromFile(String filePath) throws IOException {
    File file = new File(filePath);
    LOGGER.info("Property file path: " + file.getAbsolutePath());

    FileInputStream fis = new FileInputStream(file);
    Properties loadedProperties = new Properties();
    loadedProperties.load(fis);
    fis.close();
    return loadedProperties;
  }

  /**
   * Attribute for Carbon LOGGER.
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(CarbonProperties.class.getName());

  /**
   * class instance.
   */
  private static final CarbonProperties INSTANCE = new CarbonProperties();

  /**
   * It is purely for testing
   */
  private Map<String, String> addedProperty = new HashMap<>();

  /**
   * Private constructor this will call load properties method to load all the
   * carbon properties in memory.
   */
  private CarbonProperties() {
    initProperties();
    validateAndLoadDefaultProperties();
  }

  /**
   * This method will be responsible for get this class instance
   *
   * @return carbon properties instance
   */
  public static CarbonProperties getInstance() {
    return INSTANCE;
  }

  /**
   * This method validates the loaded properties and loads default
   * values in case of wrong values.
   */
  private void validateAndLoadDefaultProperties() {
//    try {
//      initPropertySet();
//    } catch (IllegalAccessException e) {
//      LOGGER.error("Illelagal access to declared field" + e.getMessage());
//    }

    validateBlockletSize();
    validateNumCores();
    validateNumCoresBlockSort();
    validateSortSize();
    validateCarbonDataFileVersion();
    validateExecutorStartUpTime();
    validateBlockletSizeInMB();
    validateNumberOfColumnPerIORead();
    validateLockType();
    validateCarbonCSVReadBufferSizeByte();
    validateHandoffSize();
  }

  private void validateCarbonCSVReadBufferSizeByte() {
    int bufferSize = CSV_READ_BUFFER_SIZE.getOrDefault();
    if (bufferSize < CarbonCommonConstants.CSV_READ_BUFFER_SIZE_MIN ||
        bufferSize > CarbonCommonConstants.CSV_READ_BUFFER_SIZE_MAX) {
      LOGGER.error("The value \"" + bufferSize + "\" configured for key "
          + CSV_READ_BUFFER_SIZE.getName()
          + "\" is not in range. Valid range is (byte) \""
          + CarbonCommonConstants.CSV_READ_BUFFER_SIZE_MIN + " to \""
          + CarbonCommonConstants.CSV_READ_BUFFER_SIZE_MAX + ". Using the default value");
      CSV_READ_BUFFER_SIZE.setToDefault();
    }
  }

  private void validateLockType() {
    String lockTypeConfigured = LOCK_TYPE.getOrDefault();
    switch (lockTypeConfigured.toUpperCase()) {
      // if user is setting the lock type as CARBON_LOCK_TYPE_ZOOKEEPER then no need to validate
      // else validate based on the file system type for LOCAL file system lock will be
      // CARBON_LOCK_TYPE_LOCAL and for the distributed one CARBON_LOCK_TYPE_HDFS
      case CarbonCommonConstants.CARBON_LOCK_TYPE_ZOOKEEPER:
        break;
      case CarbonCommonConstants.CARBON_LOCK_TYPE_LOCAL:
      case CarbonCommonConstants.CARBON_LOCK_TYPE_HDFS:
      default:
        validateAndConfigureLockType(lockTypeConfigured);
    }
  }

  /**
   * the method decide and set the lock type based on the configured system type
   *
   * @param lockTypeConfigured
   */
  private void validateAndConfigureLockType(String lockTypeConfigured) {
    Configuration configuration = new Configuration(true);
    String defaultFs = configuration.get("fs.defaultFS");
    if (null != defaultFs && (defaultFs.startsWith(CarbonCommonConstants.HDFSURL_PREFIX) ||
        defaultFs.startsWith(CarbonCommonConstants.VIEWFSURL_PREFIX) ||
        defaultFs.startsWith(CarbonCommonConstants.ALLUXIOURL_PREFIX)) &&
        !CarbonCommonConstants.CARBON_LOCK_TYPE_HDFS.equalsIgnoreCase(lockTypeConfigured)) {
      LOGGER.error("The value \"" + lockTypeConfigured + "\" configured for key "
          + LOCK_TYPE.getName() + " is invalid for current file system. "
          + "Use the default value " + CarbonCommonConstants.CARBON_LOCK_TYPE_HDFS + " instead.");
      LOCK_TYPE.setValue(CarbonCommonConstants.CARBON_LOCK_TYPE_HDFS);
    } else if (null != defaultFs && defaultFs.startsWith(CarbonCommonConstants.LOCAL_FILE_PREFIX)
        && !CarbonCommonConstants.CARBON_LOCK_TYPE_LOCAL.equalsIgnoreCase(lockTypeConfigured)) {
      LOCK_TYPE.setValue(CarbonCommonConstants.CARBON_LOCK_TYPE_LOCAL);
      LOGGER.error("The value \"" + lockTypeConfigured + "\" configured for key "
          + LOCK_TYPE.getName() + " is invalid for current file system. "
          + "Use the default value " + CarbonCommonConstants.CARBON_LOCK_TYPE_LOCAL + " instead.");
    }
  }

//  private void initPropertySet() throws IllegalAccessException {
//    Field[] declaredFields = CarbonCommonConstants.class.getDeclaredFields();
//    for (Field field : declaredFields) {
//      if (field.isAnnotationPresent(CarbonProperty.class)) {
//        propertySet.add(field.get(field.getName()).toString());
//      }
//    }
//    declaredFields = CarbonV3DataFormatConstants.class.getDeclaredFields();
//    for (Field field : declaredFields) {
//      if (field.isAnnotationPresent(CarbonProperty.class)) {
//        propertySet.add(field.get(field.getName()).toString());
//      }
//    }
//    declaredFields = CarbonLoadOptionConstants.class.getDeclaredFields();
//    for (Field field : declaredFields) {
//      if (field.isAnnotationPresent(CarbonProperty.class)) {
//        propertySet.add(field.get(field.getName()).toString());
//      }
//    }
//  }

  private void validateHandoffSize() {
    long handoffSize = HANDOFF_SIZE.getOrDefault();
    if (handoffSize < CarbonCommonConstants.HANDOFF_SIZE_MIN) {
      LOGGER.error("The streaming segment max size configured value " + handoffSize +
          " is invalid. Using the default value ");
      HANDOFF_SIZE.setToDefault();
    }
  }

  /**
   * This method validates the number of pages per blocklet column
   */
  private void validateBlockletSizeInMB() {
    short numberOfPagePerBlockletColumn = BLOCKLET_SIZE_IN_MB.getOrDefault().shortValue();
    if (numberOfPagePerBlockletColumn < CarbonV3DataFormatConstants.BLOCKLET_SIZE_IN_MB_MIN) {
      LOGGER.error("Blocklet Size Configured value \"" + BLOCKLET_SIZE_IN_MB.getName()
          + "\" is invalid. Using the default value");
    }
    LOGGER.info("Blocklet Size Configured value is \"" + numberOfPagePerBlockletColumn);
  }

  /**
   * This method validates the number of column read in one IO
   */
  private void validateNumberOfColumnPerIORead() {
    short numberofColumnPerIO = NUMBER_OF_COLUMN_TO_READ_IN_IO.getOrDefault().shortValue();
    if (numberofColumnPerIO < CarbonV3DataFormatConstants.NUMBER_OF_COLUMN_TO_READ_IN_IO_MIN
        || numberofColumnPerIO > CarbonV3DataFormatConstants.NUMBER_OF_COLUMN_TO_READ_IN_IO_MAX) {
      LOGGER.error(
          "The Number Of pages per blocklet column value \"" + NUMBER_OF_COLUMN_TO_READ_IN_IO
              .getName() + "\" is invalid. Using the default value");
    }
  }

  /**
   * This method validates the blocklet size
   */
  private void validateBlockletSize() {
    int blockletSize = BLOCKLET_SIZE.getOrDefault();
    if (blockletSize < CarbonCommonConstants.BLOCKLET_SIZE_MIN_VAL ||
        blockletSize > CarbonCommonConstants.BLOCKLET_SIZE_MAX_VAL) {
      LOGGER.info("The blocklet size value \"" + blockletSize
          + "\" is invalid. Using the default value ");
      BLOCKLET_SIZE.setToDefault();
    }
  }

  /**
   * This method validates the number cores specified
   */
  private void validateNumCores() {
    int numCores = CarbonProperties.NUM_CORES.getOrDefault();
    if (numCores < CarbonCommonConstants.NUM_CORES_MIN_VAL ||
        numCores > CarbonCommonConstants.NUM_CORES_MAX_VAL) {
      LOGGER.info("The num Cores  value \"" + numCores + "\" is invalid. Using the default value");
      CarbonProperties.NUM_CORES.setToDefault();
    }
  }

  /**
   * This method validates the number cores specified for mdk block sort
   */
  private void validateNumCoresBlockSort() {
    int numCores = CarbonProperties.NUM_CORES_BLOCK_SORT.getOrDefault();
    if (numCores < CarbonCommonConstants.NUM_CORES_BLOCK_SORT_MIN_VAL ||
        numCores > CarbonCommonConstants.NUM_CORES_BLOCK_SORT_MAX_VAL) {
      LOGGER.info("The num cores value \"" + numCores
          + "\" for block sort is invalid. Using the default value");
      CarbonProperties.NUM_CORES_BLOCK_SORT.setToDefault();
    }
  }

  /**
   * This method validates the sort size
   */
  private void validateSortSize() {
    int sortSize = CarbonProperties.SORT_SIZE.getOrDefault();
    if (sortSize < CarbonCommonConstants.SORT_SIZE_MIN_VAL) {
      LOGGER.error("The batch size value \"" + sortSize + "\" is invalid. Using the default value");
      CarbonProperties.SORT_SIZE.setValue(CarbonCommonConstants.SORT_SIZE_MIN_VAL);
    }
  }

  /**
   * Below method will be used to validate the data file version parameter
   * if parameter is invalid current version will be set
   */
  private void validateCarbonDataFileVersion() {
    try {
      String version = CarbonProperties.DATA_FILE_VERSION.getOrDefault();
      ColumnarFormatVersion.valueOf(version);
      LOGGER.info("Carbon Current data file version: " + version);
    } catch (IllegalArgumentException e) {
      // use default property if user specifies an invalid version property
      LOGGER.error(
          "Specified file version property is invalid: " +
          CarbonProperties.DATA_FILE_VERSION.getOrDefault() + ". Using " +
          CarbonProperties.DATA_FILE_VERSION.getDefaultValue() + " as default file version");
      CarbonProperties.DATA_FILE_VERSION.setToDefault();
    }
  }

  /**
   * This method will read all the properties from file and load it into
   * memory
   */
  private void loadProperties() {
    String property = System.getProperty("carbon.properties.filepath");
    if (null == property) {
      property = CarbonCommonConstants.CARBON_PROPERTIES_FILE_PATH;
    }
    File file = new File(property);
    LOGGER.info("Property file path: " + file.getAbsolutePath());

    FileInputStream fis = null;
    try {
      if (file.exists()) {
        fis = new FileInputStream(file);

//        carbonProperties.load(fis);
      }
    } catch (FileNotFoundException e) {
      LOGGER.error(
          "The file: " + CarbonCommonConstants.CARBON_PROPERTIES_FILE_PATH + " does not exist");
    } catch (IOException e) {
      LOGGER.error(
          "Error while reading the file: " + CarbonCommonConstants.CARBON_PROPERTIES_FILE_PATH);
    } finally {
      if (null != fis) {
        try {
          fis.close();
        } catch (IOException e) {
          LOGGER.error("Error while closing the file stream for file: "
              + CarbonCommonConstants.CARBON_PROPERTIES_FILE_PATH);
        }
      }
    }

    print();
  }

  /**
   * Return the store path
   */
  public static String getStorePath() {
    return STORE_LOCATION.getOrDefault();
  }

  /**
   * Return property value by name, return null if key does not exist or value is null
   */
  public String getProperty(String name) {
    if (properties.containsKey(name) && properties.get(name).getValue() != null) {
      return properties.get(name).getValue().toString();
    } else {
      return null;
    }

//    // get the property value from session parameters,
//    // if its null then get value from carbonProperties
//    String sessionPropertyValue = getSessionPropertyValue(name);
//    if (null != sessionPropertyValue) {
//      return sessionPropertyValue;
//    }
//    //TODO temporary fix
//    if ("carbon.leaf.node.size".equals(name)) {
//      return "120000";
//    }
//    return carbonProperties.getProperty(name);
  }

  /**
   * returns session property value
   *
   * @param key
   * @return
   */
  private String getSessionPropertyValue(String key) {
    String value = null;
    CarbonSessionInfo carbonSessionInfo = ThreadLocalSessionInfo.getCarbonSessionInfo();
    if (null != carbonSessionInfo) {
      SessionParams sessionParams =
          ThreadLocalSessionInfo.getCarbonSessionInfo().getSessionParams();
      if (null != sessionParams) {
        value = sessionParams.getProperty(key);
      }
    }
    return value;
  }

  /**
   * Return property value by name, return defaultValue if key does not exist or value is null
   */
  public String getProperty(String name, String defaultValue) {
    String value = getProperty(name);
    if (null == value) {
      return defaultValue;
    }
    return value;
  }

  /**
   * Set property name with value, it will throw IllegalArgumentException or
   * NumberFormatException if failed to convert value string to the type of this property
   */
  public CarbonProperties addProperty(String name, String value) {
    if (!properties.containsKey(name)) {
      LOGGER.error("Trying to add invalid property: " + name);
      throw new IllegalArgumentException("Invalid property name: " + name);
    }
    properties.get(name).setValue(value);
    addedProperty.put(name, value);
    return this;
  }

  public void setToDefault(String name) {
    if (properties.containsKey(name)) {
      properties.get(name).setToDefault();
    }
  }

  public ColumnarFormatVersion getFormatVersion() {
    String versionStr = DATA_FILE_VERSION.getOrDefault();
    return ColumnarFormatVersion.valueOf(versionStr);
  }

  public void print() {
    LOGGER.info("------Using Carbon.properties --------");
    LOGGER.info(properties.toString());
  }

  /**
   * gettting the unmerged segment numbers to be merged.
   *
   * @return corrected value of unmerged segments to be merged
   */
  public int[] getCompactionSegmentLevelCount() {
    String commaSeparatedLevels = COMPACTION_SEGMENT_LEVEL_THRESHOLD.getOrDefault();
    int[] compactionSize = getIntArray(commaSeparatedLevels);
    if (0 == compactionSize.length) {
      compactionSize = new int[0];
    }
    return compactionSize;
  }

  /**
   * Separating the count for Number of segments to be merged in levels by comma
   *
   * @param commaSeparatedLevels the string format value before separating
   * @return the int array format value after separating by comma
   */
  private int[] getIntArray(String commaSeparatedLevels) {
    String[] levels = commaSeparatedLevels.split(",");
    int[] compactionSize = new int[levels.length];
    int i = 0;
    for (String levelSize : levels) {
      try {
        int size = Integer.parseInt(levelSize.trim());
        if (validate(size, 100, 0, -1) < 0) {
          // if given size is out of boundary then take default value for all levels.
          return new int[0];
        }
        compactionSize[i++] = size;
      } catch (NumberFormatException e) {
        LOGGER.error("Given value for property" + commaSeparatedLevels + " is not proper.");
        return new int[0];
      }
    }
    return compactionSize;
  }

  /**
   * Validate the restrictions
   *
   * @param actual the actual value for minor compaction
   * @param max max value for minor compaction
   * @param min min value for minor compaction
   * @param defaultVal default value when the actual is improper
   * @return  corrected Value after validating
   */
  public int validate(int actual, int max, int min, int defaultVal) {
    if (actual <= max && actual >= min) {
      return actual;
    }
    return defaultVal;
  }

  /**
   * This method will validate and set the value for executor start up waiting time out
   */
  private void validateExecutorStartUpTime() {
    int executorStartUpTimeOut = EXECUTOR_STARTUP_TIMEOUT.getOrDefault();
    // If value configured by user is more than max value of time out then consider the max value
    if (executorStartUpTimeOut > CarbonCommonConstants.CARBON_EXECUTOR_WAITING_TIMEOUT_MAX) {
      LOGGER.error("The specified value for property "
          + EXECUTOR_STARTUP_TIMEOUT.getName()
          + " is incorrect. Taking the default value.");
      EXECUTOR_STARTUP_TIMEOUT.setValue(CarbonCommonConstants.CARBON_EXECUTOR_WAITING_TIMEOUT_MAX);
    }
    LOGGER.info("Executor start up wait time: " + EXECUTOR_STARTUP_TIMEOUT.getOrDefault());
  }

  /**
   * Returns configured update deleta files value for IUD compaction
   *
   * @return numberOfDeltaFilesThreshold
   */
  public int getNoUpdateDeltaFilesThresholdForIUDCompaction() {
    int numDeltaFilesThreshold = UPDATE_DELTAFILE_COUNT_THRESHOLD_IUD_COMPACTION.getOrDefault();
    if (numDeltaFilesThreshold < 0 || numDeltaFilesThreshold > 10000) {
      LOGGER.error("The specified value for property "
          + UPDATE_DELTAFILE_COUNT_THRESHOLD_IUD_COMPACTION.getName()
          + " is incorrect. Taking the default value.");
      UPDATE_DELTAFILE_COUNT_THRESHOLD_IUD_COMPACTION.setToDefault();
    }
    return UPDATE_DELTAFILE_COUNT_THRESHOLD_IUD_COMPACTION.getOrDefault();
  }

  /**
   * Returns configured delete deleta files value for IUD compaction
   *
   * @return numberOfDeltaFilesThreshold
   */
  public int getNoDeleteDeltaFilesThresholdForIUDCompaction() {
      int numberOfDeltaFilesThreshold = DELETE_DELTAFILE_COUNT_THRESHOLD_IUD_COMPACTION.getOrDefault();

      if (numberOfDeltaFilesThreshold < 0 || numberOfDeltaFilesThreshold > 10000) {
        LOGGER.error("The specified value for property "
            + DELETE_DELTAFILE_COUNT_THRESHOLD_IUD_COMPACTION.getName()
            + "is incorrect."
            + " Correct value should be in range of 0 -10000. Taking the default value.");
        DELETE_DELTAFILE_COUNT_THRESHOLD_IUD_COMPACTION.setToDefault();
      }
    return DELETE_DELTAFILE_COUNT_THRESHOLD_IUD_COMPACTION.getOrDefault();
  }

  /**
   * Return valid storage level
   * @return String
   */
  public String getGlobalSortRddStorageLevel() {
    String storageLevel = CarbonProperties.GLOBAL_SORT_RDD_STORAGE_LEVEL.getOrDefault();
    boolean validateStorageLevel = CarbonUtil.isValidStorageLevel(storageLevel);
    if (!validateStorageLevel) {
      LOGGER.error("The " + GLOBAL_SORT_RDD_STORAGE_LEVEL.getName()
          + " configuration value is invalid. It will use default storage level to persist rdd.");
      GLOBAL_SORT_RDD_STORAGE_LEVEL.setToDefault();
    }
    return GLOBAL_SORT_RDD_STORAGE_LEVEL.getOrDefault();
  }

  /**
   * Returns parallelism for segment update
   * @return int
   */
  public int getParallelismForSegmentUpdate() {
    int parallelism = CarbonProperties.UPDATE_SEGMENT_PARALLELISM.getOrDefault();
    if (parallelism <= 0 || parallelism > 1000) {
      LOGGER.error("The specified value for property "
          + UPDATE_SEGMENT_PARALLELISM.getName()
          + " is incorrect. Correct value should be in range of 0 - 1000."
          + " Taking the default value");
      UPDATE_SEGMENT_PARALLELISM.setToDefault();
    }
    return UPDATE_SEGMENT_PARALLELISM.getOrDefault();
  }

  /**
   * Return valid storage level for CARBON_UPDATE_STORAGE_LEVEL
   * @return String
   */
  public String getUpdateDatasetStorageLevel() {
    String storageLevel = UPDATE_STORAGE_LEVEL.getOrDefault();
    boolean validateStorageLevel = CarbonUtil.isValidStorageLevel(storageLevel);
    if (!validateStorageLevel) {
      LOGGER.error("The " + UPDATE_STORAGE_LEVEL.getName()
          + " configuration value is invalid. It will use default storage level to persist dataset.");
      UPDATE_STORAGE_LEVEL.setToDefault();
    }
    return UPDATE_STORAGE_LEVEL.getOrDefault();
  }

  /**
   * returns true if carbon property
   * @param key
   * @return
   */
  public boolean isCarbonProperty(String key) {
    return properties.containsKey(key);
  }

  public Map<String, String> getAddedProperty() {
    return addedProperty;
  }

}
