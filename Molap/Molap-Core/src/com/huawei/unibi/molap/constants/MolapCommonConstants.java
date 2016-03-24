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

package com.huawei.unibi.molap.constants;

public final class MolapCommonConstants {
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
     * location of the molap member, hierarchy and fact files
     */
    public static final String STORE_LOCATION = "molap.storelocation";
    /**
     * The keystore type
     */
    public static final String KEYSTORE_TYPE = "molap.keystore.type";
    /**
     * The value store type
     */
    public static final String VALUESTORE_TYPE = "molap.valuestore.type";
    /**
     * online merge file size
     */
    public static final String ONLINE_MERGE_FILE_SIZE = "molap.online.merge.file.size";
    /**
     * online merge file size default value
     */
    public static final String ONLINE_MERGE_FILE_SIZE_DEFAULT_VALUE = "10";
    /**
     * offline merge file size
     */
    public static final String OFFLINE_MERGE_FILE_SIZE = "molap.offline.merge.file.size";
    /**
     * offline merge file size default value
     */
    public static final String OFFLINE_MERGE_FILE_SIZE_DEFAULT_VALUE = "100";
    /**
     * Btree leafnode size
     */
    public static final String LEAFNODE_SIZE = "molap.leaf.node.size";
    /**
     * Hierarchy Btree leafnode size
     */
    public static final String HIERARCHY_LEAFNODE_SIZE = "molap.hier.leaf.node.size";
    /**
     * max number of leaf nodes written in a single file
     */
    public static final String MAX_FILE_SIZE = "molap.max.file.size";
    /**
     * Number of cores to be used
     */
    public static final String NUM_CORES = "molap.number.of.cores";
    /**
     * molap batchsize
     */
    public static final String BATCH_SIZE = "molap.batch.size";
    /**
     * CARDINALITY_INCREMENT_VALUE
     */
    public static final String CARDINALITY_INCREMENT_VALUE = "molap.cardinality.increment.value";
    /**
     * molap sort size
     */
    public static final String SORT_SIZE = "molap.sort.size";
    /**
     * default location of the molap member, hierarchy and fact files
     */
    public static final String STORE_LOCATION_DEFAULT_VAL = "../unibi-solutions/system/molap/store";
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
     * default Btree leafnode size
     */
    public static final String LEAFNODE_SIZE_DEFAULT_VAL = "120000";
    /**
     * min Btree leafnode size
     */
    public static final int LEAFNODE_SIZE_MIN_VAL = 50;
    /**
     * max Btree leafnode size
     */
    public static final int LEAFNODE_SIZE_MAX_VAL = 12000000;
    /**
     * max Hierarchy  Btree leafnode size
     */
    public static final String HIERARCHY_LEAFNODE_SIZE_DEFAULT_VAL = "1024";
    /**
     * default value of max number of leaf nodes written in a single file
     */
    public static final String MAX_FILE_SIZE_DEFAULT_VAL = "100";
    /**
     * min value of max number of leaf nodes written in a single file
     */
    public static final int MAX_FILE_SIZE_DEFAULT_VAL_MIN_VAL = 1;
    /**
     * max value of max number of leaf nodes written in a single file
     */
    public static final int MAX_FILE_SIZE_DEFAULT_VAL_MAX_VAL = 1000;
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
     * default molap batchsize
     */
    public static final String BATCH_SIZE_DEFAULT_VAL = "1000";
    /**
     * min molap batchsize
     */
    public static final int BATCH_SIZE_MIN_VAL = 500;
    /**
     * max molap batchsize
     */
    public static final int BATCH_SIZE_MAX_VAL = 100000;
    /**
     * default molap sort size
     */
    public static final String SORT_SIZE_DEFAULT_VAL = "5000";
    /**
     * min molap sort size
     */
    public static final int SORT_SIZE_MIN_VAL = 1000;
    /**
     * max molap sort size
     */
    public static final int SORT_SIZE_MAX_VAL = 1000000;
    /**
     * molap properties file path
     */
    //    public static final String MOLAP_PROPERTIES_FILE_PATH = "../unibi-solutions/system/molap/molap.properties";
    public static final String MOLAP_PROPERTIES_FILE_PATH = "../../../conf/molap.properties";
    /**
     * MOLAP_BADRECORDS_ENCRYPTION
     */
    public static final String MOLAP_BADRECORDS_ENCRYPTION = "molap.badRecords.encryption";
    /**
     * CARBON_DDL_BASE_HDFS_URL
     */
    public static final String CARBON_DDL_BASE_HDFS_URL = "carbon.ddl.base.hdfs.url";
    /**
     * MOLAP_BADRECORDS_ENCRYPTION_DEFAULT_VAL
     */
    public static final String MOLAP_BADRECORDS_ENCRYPTION_DEFAULT_VAL = "false";
    /**
     * molap properties file path
     */

    public static final String MOLAP_REALTIMEDATA_MAPPER_FILE_PATH =
            "../unibi-solutions/system/molap/realtimedata.properties";
    /**
     * how the MOLAP URL will start.
     */

    public static final String MOLAP_URL_START = "molap://";
    /**
     * molap url
     */

    public static final String MOLAP_URL_KEY = "url";
    /**
     * molap schema file path
     */

    public static final String MOLAP_SCHEMA_FILE_PATH = "schemafilepath";
    /**
     * molap key
     */

    public static final String MOLAP_KEY = "molap";
    /**
     * molap mode key
     */

    public static final String MOLAP_MODE_KEY = "mode";
    /**
     * molap mode default val
     */
    public static final String MOLAP_MODE_DEFAULT_VAL = "file";
    /**
     * molap mode in-memory val
     */
    public static final String MOLAP_MODE_IN_MEM_VAL = "in-memory";
    /**
     * Slice Meta data file.
     */
    public static final String SLICE_METADATA_FILENAME = "sliceMetaData";
    /**
     * Load Folder Name
     */
    public static final String LOAD_FOLDER = "Load_";
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
    public static final String ETL_STORE_LOCATION = "../unibi-solutions/system/molap/etl";
    /**
     * location of the graphs files
     */
    public static final String RESTRUCT_FILE_METADATA = "../unibi-solutions/system/molap/tmp";
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
     * MOLAP_CACHE_LOCATION
     */
    public static final String MOLAP_CACHE_LOCATION = "../unibi-solutions/system/molap/cache";
    /**
     * FILE STATUS IN-PROGRESS
     */
    public static final String FILE_INPROGRESS_STATUS = ".inprogress";
    /**
     * Generated count measure
     */
    public static final String GEN_COUNT_MEASURE = "$^&DEFAULT_GENERATED_COUNT_MEASURE$^&";
    /**
     * MOLAP_BADRECORDS_LOCATION
     */
    public static final String MOLAP_BADRECORDS_LOC = "molap.badRecords.location";
    /**
     * MOLAP_BADRECORDS_LOCATION_DEFAULT
     */
    public static final String MOLAP_BADRECORDS_LOC_DEFAULT_VAL =
            "../unibi-solutions/system/molap/badRecords";
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
    public static final String SORT_BUFFER_SIZE = "molap.sort.buffer.size";
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
    public static final String DATA_LOAD_Q_SIZE = "molap.dataload.queuesize";
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
    public static final String MOLAP_MEMBERUPDATE_DATA_LOAD_Q_SIZE =
            "molap.memberupdate.dataloader.queuesize";
    /**
     * DATA_LOAD_Q_SIZE_MIN
     */
    public static final int MOLAP_MEMBERUPDATE_DATA_LOAD_Q_SIZE_MIN = 1;
    /**
     * DATA_LOAD_Q_SIZE_MAX
     */
    public static final int MOLAP_MEMBERUPDATE_DATA_LOAD_Q_SIZE_MAX = 100;
    /**
     * DATA_LOAD_Q_SIZE_DEFAULT
     */
    public static final String MOLAP_MEMBERUPDATE_DATA_LOAD_Q_SIZE_DEFAULT = "100";
    /**
     * DATA_LOAD_CONC_EXE_SIZE
     */
    public static final String MOLAP_MEMBERUPDATE_DATA_LOAD_CONC_EXE_SIZE =
            "molap.memberupdate.dataload.concurrent.execution.size";
    /**
     * DATA_LOAD_CONC_EXE_SIZE_DEFAULT
     */
    public static final String MOLAP_MEMBERUPDATE_DATA_LOAD_CONC_EXE_SIZE_DEFAULT = "1";
    /**
     * DATA_LOAD_CONC_EXE_SIZE_MIN
     */
    public static final int MOLAP_MEMBERUPDATE_DATA_LOAD_CONC_EXE_SIZE_MIN = 1;
    /**
     * DATA_LOAD_CONC_EXE_SIZE_MAX
     */
    public static final int MOLAP_MEMBERUPDATE_DATA_LOAD_CONC_EXE_SIZE_MAX = 5;
    /**
     * DATA_LOAD_CONC_EXE_SIZE
     */
    public static final String DATA_LOAD_CONC_EXE_SIZE = "molap.dataload.concurrent.execution.size";
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
     * MOLAP_Realtime_data
     */
    public static final String MOLAP_REALTIMEDATA_FILE =
            "../unibi-solutions/system/molap/realtimedata.properties";
    /**
     * MOLAP_RESULT_SIZE_KEY
     */
    public static final String MOLAP_RESULT_SIZE_KEY = "molap.result.limit";
    /**
     * MOLAP_RESULT_SIZE_DEFAULT
     */
    public static final String MOLAP_RESULT_SIZE_DEFAULT = "5000";
    /**
     * MYSQL_NULL_VALUE
     */
    public static final String MYSQL_NULL_VALUE = "mysql.null.value";
    /**
     * MSSQL_NULL_VALUE
     */
    public static final String MSSQL_NULL_VALUE = "mssql.null.value";
    /**
     * MOLAP_RESULT_SIZE_DEFAULT
     */
    public static final String ORACLE_NULL_VALUE = "oracle.null.value";
    /**
     * MOLAP_RESULT_SIZE_DEFAULT
     */
    public static final String LEVEL_FILE_EXTENSION = ".level";
    /**
     * FACT_FILE_EXT
     */
    public static final String FACT_FILE_EXT = ".fact";
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
    public static final String PAGINATED_CACHE_DISK_SIZE = "molap.paginated.cache.disk.size";
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
            "molap.paginated.internal.merge.size.limit";
    /**
     * PAGINATED Internal merge size limit default in MB
     */
    public static final String PAGINATED_INTERNAL_MERGE_SIZE_LIMIT_DEFAULT = "10";
    /**
     * PAGINATED Internal merge size limit
     */
    public static final String PAGINATED_INTERNAL_FILE_ROW_LIMIT =
            "molap.paginated.internal.file.row.limit";
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
    public static final String GRAPH_ROWSET_SIZE = "molap.graph.rowset.size";
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
    public static final String SORT_INTERMEDIATE_FILES_LIMIT =
            "molap.sort.intermediate.files.limit";
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
    public static final String SORT_FILE_BUFFER_SIZE = "molap.sort.file.buffer.size";
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
    public static final String MOLAP_SORT_FILE_WRITE_BUFFER_SIZE =
            "molap.sort.file.write.buffer.size";
    /**
     * SORT_FILE_WRITE_BUFFER_SIZE_DEFAULT_VALUE
     */
    public static final String MOLAP_SORT_FILE_WRITE_BUFFER_SIZE_DEFAULT_VALUE = "50000";
    /**
     * IS_DATA_LOAD_LOG_ENABLED
     */
    public static final String IS_DATA_LOAD_LOG_ENABLED = "molap.dataload.log.enabled";
    /**
     * IS_DATA_LOAD_LOG_ENABLED_DEFAULTVALUE
     */
    public static final String IS_DATA_LOAD_LOG_ENABLED_DEFAULT_VALUE = "false";
    /**
     * NUMBER_OF_THERADS_FOR_INTERMEDIATE_MERGING
     */
    public static final String NUMBER_OF_THERADS_FOR_INTERMEDIATE_MERGING =
            "molap.sort.intermedaite.number.of.threads";
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
    public static final String NUM_CORES_LOADING = "molap.number.of.cores.while.loading";
    /**
     * CSV_READ_BUFFER_SIZE
     */
    public static final String CSV_READ_BUFFER_SIZE = "molap.csv.read.buffersize.byte";
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
     * MOLAP_DECIMAL_POINTERS
     */
    public static final String MOLAP_DECIMAL_POINTERS = "molap.decimal.pointers";
    /**
     * MOLAP_DECIMAL_POINTERS_DEFAULT
     */
    public static final String MOLAP_DECIMAL_POINTERS_DEFAULT = "5";
    /**
     * MOLAP_DECIMAL_POINTERS_AGG
     */
    public static final String MOLAP_DECIMAL_POINTERS_AGG = "molap.decimal.pointers.agg";
    /**
     * MOLAP_DECIMAL_POINTERS_AGG_DEFAULT
     */
    public static final String MOLAP_DECIMAL_POINTERS_AGG_DEFAULT = "4";
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
     * MOLAP_DATALOAD_CHECKPOINT
     */
    public static final String MOLAP_DATALOAD_CHECKPOINT = "molap.dataload.checkpoint";
    /**
     * MOLAP_DATALOAD_CHECKPOINT_DEFAULTVALUE
     */
    public static final String MOLAP_DATALOAD_CHECKPOINT_DEFAULTVALUE = "false";
    /**
     * BAK_EXT
     */
    public static final String BAK_EXT = ".bak";
    /**
     * DONE_EXT
     */
    public static final String DONE_EXT = ".done";
    /**
     * MOLAP_CHECKPOINT_QUEUE_THRESHOLD
     */
    public static final String MOLAP_CHECKPOINT_QUEUE_THRESHOLD =
            "molap.checkpoint.queue.threshold";
    /**
     * MOLAP_CHECKPOINT_CHUNK_SIZE
     */
    public static final String MOLAP_CHECKPOINT_CHUNK_SIZE = "molap.checkpoint.chunk.size";
    /**
     * MOLAP_CHECKPOINT_QUEUE_INITIAL_CAPACITY
     */
    public static final String MOLAP_CHECKPOINT_QUEUE_INITIAL_CAPACITY =
            "molap.checkpoint.queue.initial.capacity";
    /**
     * MOLAP_CHECKPOINT_TOCOPY_FROM_QUEUE
     */
    public static final String MOLAP_CHECKPOINT_TOCOPY_FROM_QUEUE =
            "molap.checkpoint.tocopy.from.queue";
    /**
     * MOLAP_CHECKPOINT_QUEUE_THRESHOLD_DEFAULT_VAL
     */
    public static final String MOLAP_CHECKPOINT_QUEUE_THRESHOLD_DEFAULT_VAL = "20";
    /**
     * MOLAP_CHECKPOINT_CHUNK_SIZE_DEFAULT_VAL
     */
    public static final String MOLAP_CHECKPOINT_CHUNK_SIZE_DEFAULT_VAL = "500";
    /**
     * MOLAP_CHECKPOINT_QUEUE_INITIAL_CAPACITY_DEFAULT_VAL
     */
    public static final String MOLAP_CHECKPOINT_QUEUE_INITIAL_CAPACITY_DEFAULT_VAL = "25";
    /**
     * MOLAP_CHECKPOINT_TOCOPY_FROM_QUEUE_DEFAULT_VAL
     */
    public static final String MOLAP_CHECKPOINT_TOCOPY_FROM_QUEUE_DEFAULT_VAL = "5";
    /**
     * IS_PRODUCERCONSUMER_BASED_SORTING
     */
    public static final String IS_PRODUCERCONSUMER_BASED_SORTING =
            "molap.is.producer.consumer.based.sorting";
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
    public static final String BACKGROUND_MERGER_TYPE = "molap.background.merger.type";
    /**
     * BACKGROUND_MERGER_TIME_INTERVAL_IN_MINUTE
     */
    public static final String BACKGROUND_MERGER_TIME_INTERVAL_IN_MINUTE =
            "molap.background.merger.time.interval";
    /**
     * BACKGROUND_MERGER_TIME_INTERVAL_IN_MINUTE_DEFAULT_VALUE
     */
    public static final String BACKGROUND_MERGER_TIME_INTERVAL_IN_MINUTE_DEFAULT_VALUE = "30";
    /**
     * MOLAP_SEQ_GEN_INMEMORY_LRU_CACHE_SIZE
     */
    public static final String MOLAP_SEQ_GEN_INMEMORY_LRU_CACHE_SIZE =
            "molap.seqgen.inmemory.lru.cache.size";
    /**
     * MOLAP_SEQ_GEN_INMEMORY_LRU_CACHE_SIZE_DEFAULT_VALUE
     */
    public static final String MOLAP_SEQ_GEN_INMEMORY_LRU_CACHE_SIZE_DEFAULT_VALUE = "4";
    /**
     * MOLAP_SEQ_GEN_INMEMORY_LRU_CACHE_ENABLED
     */
    public static final String MOLAP_SEQ_GEN_INMEMORY_LRU_CACHE_ENABLED =
            "molap.seqgen.inmemory.lru.cache.enabled";
    /**
     * MOLAP_SEQ_GEN_INMEMORY_LRU_CACHE_ENABLED_DEFAULT_VALUE
     */
    public static final String MOLAP_SEQ_GEN_INMEMORY_LRU_CACHE_ENABLED_DEFAULT_VALUE = "false";
    /**
     * MOLAP_SEQ_GEN_INMEMORY_LRU_CACHE_FLUSH_INTERVAL_INHOUR
     */
    public static final String MOLAP_SEQ_GEN_INMEMORY_LRU_CACHE_FLUSH_INTERVAL_INHOUR =
            "molap.seqgen.inmemory.lru.cache.flush.interval.in.minute";
    /**
     * MOLAP_SEQ_GEN_INMEMORY_LRU_CACHE_FLUSH_INTERVAL_INHOUR_DEFAULTVALUE
     */
    public static final String MOLAP_SEQ_GEN_INMEMORY_LRU_CACHE_FLUSH_INTERVAL_INHOUR_DEFAULTVALUE =
            "300";
    /**
     * MOLAP_MAX_THREAD_FOR_SORTING
     */
    public static final String MOLAP_MAX_THREAD_FOR_SORTING = "molap.max.thread.for.sorting";
    /**
     * MOLAP_MAX_THREAD_FOR_SORTING
     */
    public static final String MOLAP_MAX_THREAD_FOR_SORTING_DEFAULTVALUE = "2";
    /**
     * MOLAP_AUTOAGGREGATION_TYPE
     */
    public static final String MOLAP_AUTOAGGREGATION_TYPE = "molap.autoaggregation.type";
    /**
     * MOLAP_AUTOAGGREGATION_TYPE_MANUALVALUE
     */
    public static final String MOLAP_MANUAL_TYPE_VALUE = "MANUAL";
    /**
     * MOLAP_AUTOAGGREGATION_TYPE_AUTOVALUE
     */
    public static final String MOLAP_AUTO_TYPE_VALUE = "AUTO";
    /**
     * MOLAP_AUTOAGGREGATION_NO_OF_PARALLEL_AGGREGATETABLE_GENERATION
     */
    public static final String MOLAP_AUTOAGGREGATION_NO_OF_PARALLEL_AGGREGATETABLE_GENERATION =
            "molap.autoaggregation.no.of.parallel.aggregatetable.generation";
    /**
     * MOLAP_AUTOAGGREGATION_NO_OF_PARALLEL_AGGREGATETABLE_GENERATION_DEFAULT
     */
    public static final String
            MOLAP_AUTOAGGREGATION_NO_OF_PARALLEL_AGGREGATETABLE_GENERATION_DEFAULT = "1";
    /**
     * MOLAP_AUTO_AGG_CONST
     */
    public static final String MOLAP_AUTO_AGG_CONST = "AutoAgg";
    /**
     * MOLAP_MANUAL_AGG_CONST
     */
    public static final String MOLAP_MANUAL_AGG_CONST = "ManualAgg";
    /**
     * MOLAP_SEND_LOAD_SIGNAL_TO_ENGINE
     */
    public static final String MOLAP_SEND_LOAD_SIGNAL_TO_ENGINE =
            "molap.send.load.signal.to.engine";
    /**
     * MOLAP_SEND_LOAD_SIGNAL_TO_ENGINE_DEFAULTVALUE
     */
    public static final String MOLAP_SEND_LOAD_SIGNAL_TO_ENGINE_DEFAULTVALUE = "true";
    /**
     * IS_SORT_TEMP_FILE_COMPRESSION_ENABLED
     */
    public static final String IS_SORT_TEMP_FILE_COMPRESSION_ENABLED =
            "molap.is.sort.temp.file.compression.enabled";
    /**
     * IS_SORT_TEMP_FILE_COMPRESSION_ENABLED_DEFAULTVALUE
     */
    public static final String IS_SORT_TEMP_FILE_COMPRESSION_ENABLED_DEFAULTVALUE = "false";
    /**
     * SORT_TEMP_FILE_NO_OF_RECORDS_FOR_COMPRESSION
     */
    public static final String SORT_TEMP_FILE_NO_OF_RECORDS_FOR_COMPRESSION =
            "molap.sort.temp.file.no.of.records.for.compression";
    /**
     * SORT_TEMP_FILE_NO_OF_RECORD_FOR_COMPRESSION_DEFAULTVALUE
     */
    public static final String SORT_TEMP_FILE_NO_OF_RECORD_FOR_COMPRESSION_DEFAULTVALUE = "50";
    /**
     * MOLAP_BACKGROUND_MERGER_SIZE_IN_GB
     */
    public static final String MOLAP_BACKGROUND_MERGER_FILE_SIZE =
            "molap.background.merge.file.size";
    /**
     * DEFAULT_COLLECTION_SIZE
     */
    public static final int DEFAULT_COLLECTION_SIZE = 16;
    /**
     * MOLAP_DATALOAD_VALID_CSVFILE_SIZE
     */
    public static final String MOLAP_DATALOAD_VALID_CSVFILE_SIZE =
            "molap.dataload.valid.csvfile.size(in GB)";
    /**
     * MOLAP_DATALOAD_VALID_CSVFILE_SIZE_DEFAULTVALUE
     */
    public static final String MOLAP_DATALOAD_VALID_CSVFILE_SIZE_DEFAULTVALUE = "5";
    /**
     * MOLAP_TIMESTAMP_DEFAULT_FORMAT
     */
    public static final String MOLAP_TIMESTAMP_DEFAULT_FORMAT = "yyyy-MM-dd HH:mm:ss";
    /**
     * MOLAP_TIMESTAMP_DEFAULT_FORMAT
     */
    public static final String MOLAP_TIMESTAMP_FORMAT = "molap.timestamp.format";
    /**
     * MOLAP_DATALOAD_VALID_CSVFILE_SIZE
     */
    public static final String MOLAP_DATALOAD_VALID_NUMBAER_OF_CSVFILE =
            "molap.dataload.csv.filecount";
    /**
     * MOLAP_DATALOAD_VALID_CSVFILE_SIZE_DEFAULTVALUE
     */
    public static final String MOLAP_DATALOAD_VALID_NUMBAER_OF_CSVFILE_DEFAULTVALUE = "100";
    /**
     * MOLAP_IS_GROUPBY_IN_SORT_DEFAULTVALUE
     */
    public static final String MOLAP_IS_GROUPBY_IN_SORT_DEFAULTVALUE = "false";
    /**
     * MOLAP_IS_LOAD_FACT_TABLE_IN_MEMORY
     */
    public static final String MOLAP_IS_LOAD_FACT_TABLE_IN_MEMORY =
            "molap.is.load.facttable.in.memory";
    /**
     * MOLAP_IS_LOAD_FACT_TABLE_IN_MEMORY_DEFAULTVALUE
     */
    public static final String MOLAP_IS_LOAD_FACT_TABLE_IN_MEMORY_DEFAULTVALUE = "true";
    /**
     * MOLAP_READ_ONLY_UNPROCESSED_LOAD_FOLER
     */
    public static final String MOLAP_READ_ONLY_UNPROCESSED_LOAD_FOLDER =
            "molap.read.only.unprocessed.load.folder";
    /**
     * MOLAP_READ_ONLY_UNPROCESSED_LOAD_FOLDER_DEFAULT_VALUE
     */
    public static final String MOLAP_READ_ONLY_UNPROCESSED_LOAD_FOLDER_DEFAULT_VALUE = "false";
    /**
     * MOLAP_READ_ONLY_UNPROCESSED_LOAD_FOLER
     */
    public static final String MOLAP_PARALLEL_AGGREGATE_CREATION_FROM_FACT =
            "molap.parallel.aggregate.creation.from.fact";
    /**
     * MOLAP_READ_ONLY_UNPROCESSED_LOAD_FOLDER_DEFAULT_VALUE
     */
    public static final String MOLAP_PARALLEL_AGGREGATE_CREATION_FROM_FACT_DEFAULT_VALUE = "false";
    /**
     * IS_PRODUCERCONSUMER_BASED_SORTING
     */
    public static final String IS_PRODUCERCONSUMER_BASED_SORTING_IN_AGG =
            "molap.is.producer.consumer.based.sorting.in.agg";
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
    public static final String SPARK_STORE_LOCATION = "spark.molap.storelocation";
    /**
     * STORE_LOCATION_HDFS
     */
    public static final String STORE_LOCATION_HDFS = "molap.storelocation.hdfs";
    /**
     * STORE_LOCATION_TEMP
     */
    public static final String STORE_LOCATION_TEMP = "molap.storelocation.temp";
    /**
     * STORE_LOCATION_TEMP_PATH
     */
    public static final String STORE_LOCATION_TEMP_PATH = "molap.tempstore.location";
    /**
     * DATALOAD_KETTLE_PATH
     */
    public static final String DATALOAD_KETTLE_PATH = "molap.dataload.kettleplugins.path";
    /**
     * IS_COLUMNAR_STORAGE
     */
    public static final String IS_COLUMNAR_STORAGE = "molap.is.columnar.storage";
    /**
     * IS_COLUMNAR_STORAGE_DEFAULTVALUE
     */
    public static final String IS_COLUMNAR_STORAGE_DEFAULTVALUE = "true";
    /**
     * DIMENSION_SPLIT_VALUE_IN_COLUMNAR
     */
    public static final String DIMENSION_SPLIT_VALUE_IN_COLUMNAR =
            "molap.dimension.split.value.in.columnar";
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
    public static final String IS_FULLY_FILLED_BITS = "molap.is.fullyfilled.bits";
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
     * MOLAP_USE_HASHBASED_AGG_INSORT
     */
    public static final String MOLAP_USE_HASHBASED_AGG_INSORT = "molap.use.hashbased.agg.insort";
    /**
     * MOLAP_USE_HASHBASED_AGG_INSORT_DEFAULTVALUE
     */
    public static final String MOLAP_USE_HASHBASED_AGG_INSORT_DEFAULTVALUE = "false";
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
    public static final String MOLAP_METADATA_EXTENSION = ".metadata";
    /**
     * LOAD_STATUS
     */
    public static final String MOLAP_DEFAULT_STREAM_ENCODEFORMAT = "UTF-8";
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
     * MOLAP_TIMESTAMP
     */
    public static final String MOLAP_TIMESTAMP = "dd-MM-yyyy HH:mm:ss";
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
            "molap.load.metadata.lock.retries";
    /**
     * MAX_TIMEOUT_FOR_LOAD_METADATA_LOCK
     */
    public static final String MAX_TIMEOUT_FOR_LOAD_METADATA_LOCK =
            "molap.load.metadata.lock.retry.timeout.sec";
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
    public static final String ARRAY = "Array";
    public static final String STRUCT = "Struct";
    public static final String INCLUDE = "include";
    public static final String FROM = "from";
    public static final String WITH = "with";
    /**
     * FACT_UPDATE_EXTENSION.
     */
    public static final String FACT_UPDATE_EXTENSION = ".fact_update";
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
    public static final String LOADMETADATA_FILENAME = "loadmetadata";
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
    public static final String IS_FORCED_IN_MEMORY_CUBE = "molap.forced.in.memory.cube";
    public static final String IS_FORCED_IN_MEMORY_CUBE_DEFAULT_VALUE = "false";
    /**
     * UPDATING_METADATA
     */
    public static final String UPDATING_METADATA = ".tmp";
    /**
     * LOADCUBE_STARTUP.
     */
    public static final String LOADCUBE_STARTUP = "molap.is.loadcube.startup";
    /**
     * LOADCUBE_DATALOAD.
     */
    public static final String LOADCUBE_DATALOAD = "molap.is.loadcube.dataload";
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
    public static final String CARBON_LOAD_LEVEL_RETRY_INTERVAL =
            "Carbon.load.level.retry.interval";
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
     * MOLAP_PREFETCH_BUFFERSIZE
     */
    public static final int MOLAP_PREFETCH_BUFFERSIZE = 20000;
    /**
     * MOLAP_PREFETCH_IN_MERGE
     */
    public static final boolean MOLAP_PREFETCH_IN_MERGE_VALUE = false;
    /**
     * TEMPWRITEFILEEXTENSION
     */
    public static final String TEMPWRITEFILEEXTENSION = ".write";
    /**
     * MERGE_THRESHOLD_VALUE
     */
    public static final String MERGE_THRESHOLD_VALUE = "molap.merge.threshold";
    /**
     * MERGE_THRESHOLD_DEFAULT_VAL
     */
    public static final String MERGE_THRESHOLD_DEFAULT_VAL = "10";
    /**
     * MERGE_FACTSIZE_THRESHOLD_VALUE
     */
    public static final String MERGE_FACTSIZE_THRESHOLD_VALUE = "molap.merge.factsize.threshold";
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
    public static final String ENABLE_LOAD_MERGE = "molap.enable.load.merge";
    /**
     * DEFAULT_ENABLE_LOAD_MERGE
     */
    public static final String DEFAULT_ENABLE_LOAD_MERGE = "false";

    private MolapCommonConstants() {

    }
}


