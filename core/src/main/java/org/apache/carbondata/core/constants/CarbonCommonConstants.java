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

import org.apache.carbondata.core.api.CarbonProperties;

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
   * CARDINALITY_INCREMENT_DEFAULT_VALUE
   */
  public static final int CARDINALITY_INCREMENT_VALUE_DEFAULT_VAL = 10;

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
   * BYTEBUFFER_SIZE
   */
  public static final int BYTEBUFFER_SIZE = 24 * 1024;
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
   * SORT_TEMP_FILE_LOCATION
   */
  public static final String SORT_TEMP_FILE_LOCATION = "sortrowtmp";
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
   * Number of cores to be used while alter partition
   */
  public static final String NUM_CORES_ALT_PARTITION = "carbon.number.of.cores.while.altPartition";

  /**
   * Max value of number of cores to be used for block sort
   */
  public static final int NUM_CORES_BLOCK_SORT_MAX_VAL = 12;
  /**
   * Min value of number of cores to be used for block sort
   */
  public static final int NUM_CORES_BLOCK_SORT_MIN_VAL = 1;

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
   * SORT_TEMP_FILE_EXT
   */
  public static final String SORT_TEMP_FILE_EXT = ".sorttemp";

  /**
   * DEFAULT_COLLECTION_SIZE
   */
  public static final int DEFAULT_COLLECTION_SIZE = 16;

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
   * CARBON_TIMESTAMP
   */
  public static final String CARBON_TIMESTAMP = "dd-MM-yyyy HH:mm:ss";

  /**
   * CARBON_TIMESTAMP
   */
  public static final String CARBON_TIMESTAMP_MILLIS = "dd-MM-yyyy HH:mm:ss:SSS";



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
   * SPILL_OVER_DISK_PATH
   */
  public static final String SCHEMAS_MODIFIED_TIME_FILE = "modifiedTime.mdt";
  public static final String DEFAULT_INVISIBLE_DUMMY_MEASURE = "default_dummy_measure";
  public static final String CARBON_IMPLICIT_COLUMN_POSITIONID = "positionId";
  public static final String CARBON_IMPLICIT_COLUMN_TUPLEID = "tupleId";
  public static final String CARBON_IMPLICIT_COLUMN_SEGMENTID = "segId";

  /**
   * implicit column which will be added to each carbon table
   */
  public static final String POSITION_ID = "positionId";


  /**
   * CARBON_PREFETCH_IN_MERGE
   */
  public static final boolean CARBON_PREFETCH_IN_MERGE_VALUE = false;
  /**
   * TEMPWRITEFILEEXTENSION
   */
  public static final String TEMPWRITEFILEEXTENSION = ".write";



  /**
   * ZOOKEEPER_LOCATION this is the location in zookeeper file system where locks are created.
   * mechanism of carbon
   */
  public static final String ZOOKEEPER_LOCATION = "/CarbonLocks";


  /**
   * hive connection url
   */
  public static final String HIVE_CONNECTION_URL = "javax.jdo.option.ConnectionURL";


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
   * Invalid filter member log string
   */
  public static final String FILTER_INVALID_MEMBER =
      " Invalid Record(s) are present while filter evaluation. ";




  /**
   * default location of the carbon metastore db
   */
  public static final String METASTORE_LOCATION_DEFAULT_VAL = "../carbon.metastore";


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
   * hyphen
   */
  public static final String HYPHEN = "-";

  /**
   * columns which gets updated in update will have header ends with this extension.
   */
  public static final String UPDATED_COL_EXTENSION = "-updatedColumn";





  /**
   * Maximum no of column supported
   */
  public static final int DEFAULT_MAX_NUMBER_OF_COLUMNS = 20000;


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


  public static final String MINOR = "minor";

  public static final String MAJOR = "major";

  public static final int DICTIONARY_DEFAULT_CARDINALITY = 1;




  public static final String ENABLE_HIVE_SCHEMA_META_STORE = "spark.carbon.hive.schema.store";

  public static final String ENABLE_HIVE_SCHEMA_META_STORE_DEFAULT = "false";

  /**
   * There is more often that in production uses different drivers for load and queries. So in case
   * of load driver user should set this property to enable loader specific clean up.
   */
  public static final String DATA_MANAGEMENT_DRIVER = "spark.carbon.datamanagement.driver";

  public static final String DATA_MANAGEMENT_DRIVER_DEFAULT = "false";

  public static final String CARBON_SESSIONSTATE_CLASSNAME = "spark.carbon.sessionstate.classname";


  /**
   * this will be used to provide comment for table
   */
  public static final String TABLE_COMMENT = "comment";


  /**
   * It is internal configuration and used only for test purpose.
   * It will merge the carbon index files with in the segment to single segment.
   */
  public static final String CARBON_MERGE_INDEX_IN_SEGMENT = "carbon.merge.index.in.segment";

  public static final String CARBON_MERGE_INDEX_IN_SEGMENT_DEFAULT = "true";

  public static final String AGGREGATIONDATAMAPSCHEMA = "AggregateDataMapHandler";
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




  /**
   * the min handoff size of streaming segment, the unit is MB
   */
  public static final long HANDOFF_SIZE_MIN = 64;



  public static final String TIMESERIES_EVENTTIME = "timeseries.eventtime";

  public static final String TIMESERIES_HIERARCHY = "timeseries.hierarchy";

  private CarbonCommonConstants() {
  }

  public static final CarbonProperties.RuntimeProperty<String> BAD_RECORDS_ACTION =
      newStringProperty("carbon.bad.records.action")
          .doc("FAIL action will fail the load in case of bad records in loading data")
          .createWithDefault("FAIL");

  @SuppressWarnings("unchecked")
  private static CarbonProperties.PropertyBuilder<String> newStringProperty(String name) {
    return new CarbonProperties.PropertyBuilder<>(String.class, name);
  }
}


