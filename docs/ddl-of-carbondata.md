<!--
    Licensed to the Apache Software Foundation (ASF) under one or more 
    contributor license agreements.  See the NOTICE file distributed with
    this work for additional information regarding copyright ownership. 
    The ASF licenses this file to you under the Apache License, Version 2.0
    (the "License"); you may not use this file except in compliance with 
    the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0
    
    Unless required by applicable law or agreed to in writing, software 
    distributed under the License is distributed on an "AS IS" BASIS, 
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and 
    limitations under the License.
-->

# CarbonData Data Definition Language

CarbonData DDL statements are documented here,which includes:

* [CREATE TABLE](#create-table)
  * [Dictionary Encoding](#dictionary-encoding-configuration)
  * [Local Dictionary](#local-dictionary-configuration)
  * [Inverted Index](#inverted-index-configuration)
  * [Sort Columns](#sort-columns-configuration)
  * [Sort Scope](#sort-scope-configuration)
  * [Table Block Size](#table-block-size-configuration)
  * [Table Compaction](#table-compaction-configuration)
  * [Streaming](#streaming)
  * [Caching Column Min/Max](#caching-minmax-value-for-required-columns)
  * [Caching Level](#caching-at-block-or-blocklet-level)
  * [Hive/Parquet folder Structure](#support-flat-folder-same-as-hiveparquet)
  * [Extra Long String columns](#string-longer-than-32000-characters)
  * [Compression for Table](#compression-for-table)
  * [Bad Records Path](#bad-records-path) 
  * [Load Minimum Input File Size](#load-minimum-data-size)
  * [Range Column](#range-column)

* [CREATE TABLE AS SELECT](#create-table-as-select)
* [CREATE EXTERNAL TABLE](#create-external-table)
  * [External Table on Transactional table location](#create-external-table-on-managed-table-data-location)
  * [External Table on non-transactional table location](#create-external-table-on-non-transactional-table-data-location)
* [CREATE DATABASE](#create-database)
* [TABLE MANAGEMENT](#table-management)
  * [SHOW TABLE](#show-table)
  * [ALTER TABLE](#alter-table)
    * [RENAME TABLE](#rename-table)
    * [ADD COLUMNS](#add-columns)
    * [DROP COLUMNS](#drop-columns)
    * [RENAME COLUMN](#change-column-nametype)
    * [CHANGE COLUMN NAME/TYPE](#change-column-nametype)
    * [MERGE INDEXES](#merge-index)
    * [SET/UNSET](#set-and-unset)
  * [DROP TABLE](#drop-table)
  * [REFRESH TABLE](#refresh-table)
  * [COMMENTS](#table-and-column-comment)
* [PARTITION](#partition)
  * [STANDARD PARTITION(HIVE)](#standard-partition)
    * [INSERT OVERWRITE PARTITION](#insert-overwrite)
  * [SHOW PARTITIONS](#show-partitions)
  * [ADD PARTITION](#add-a-new-partition)
  * [SPLIT PARTITION](#split-a-partition)
  * [DROP PARTITION](#drop-a-partition)
* [BUCKETING](#bucketing)
* [CACHE](#cache)

## CREATE TABLE

  This command can be used to create a CarbonData table by specifying the list of fields along with the table properties. You can also specify the location where the table needs to be stored.

  ```
  CREATE TABLE [IF NOT EXISTS] [db_name.]table_name[(col_name data_type , ...)]
  STORED AS carbondata
  [TBLPROPERTIES (property_name=property_value, ...)]
  [LOCATION 'path']
  ```

  **NOTE:** CarbonData also supports "STORED AS carbondata" and "USING carbondata". Find example code at [CarbonSessionExample](https://github.com/apache/carbondata/blob/master/examples/spark/src/main/scala/org/apache/carbondata/examples/CarbonSessionExample.scala) in the CarbonData repo.
### Usage Guidelines

**Supported properties:**

| Property                                                     | Description                                                  |
| ------------------------------------------------------------ | ------------------------------------------------------------ |
| [NO_INVERTED_INDEX](#inverted-index-configuration)           | Columns to exclude from inverted index generation            |
| [INVERTED_INDEX](#inverted-index-configuration)              | Columns to include for inverted index generation             |
| [SORT_COLUMNS](#sort-columns-configuration)                  | Columns to include in sort and its order of sort             |
| [SORT_SCOPE](#sort-scope-configuration)                      | Sort scope of the load.Options include no sort, local sort ,batch sort and global sort |
| [TABLE_BLOCKSIZE](#table-block-size-configuration)           | Size of blocks to write onto hdfs                            |
| [TABLE_BLOCKLET_SIZE](#table-blocklet-size-configuration)    | Size of blocklet to write in the file                        |
| [TABLE_PAGE_SIZE_INMB](#table-page-size-configuration)       | Size of page in MB; if page size crosses this value before 32000 rows, page will be cut to this many rows and remaining rows are processed in the subsequent pages. This helps in keeping page size to fit in cpu cache size|
| [MAJOR_COMPACTION_SIZE](#table-compaction-configuration)     | Size upto which the segments can be combined into one        |
| [AUTO_LOAD_MERGE](#table-compaction-configuration)           | Whether to auto compact the segments                         |
| [COMPACTION_LEVEL_THRESHOLD](#table-compaction-configuration) | Number of segments to compact into one segment               |
| [COMPACTION_PRESERVE_SEGMENTS](#table-compaction-configuration) | Number of latest segments that needs to be excluded from compaction |
| [ALLOWED_COMPACTION_DAYS](#table-compaction-configuration)   | Segments generated within the configured time limit in days will be compacted, skipping others |
| [STREAMING](#streaming)                                      | Whether the table is a streaming table                       |
| [LOCAL_DICTIONARY_ENABLE](#local-dictionary-configuration)   | Enable local dictionary generation                           |
| [LOCAL_DICTIONARY_THRESHOLD](#local-dictionary-configuration) | Cardinality upto which the local dictionary can be generated |
| [LOCAL_DICTIONARY_INCLUDE](#local-dictionary-configuration)  | Columns for which local dictionary needs to be generated. Useful when local dictionary need not be generated for all string/varchar/char columns |
| [LOCAL_DICTIONARY_EXCLUDE](#local-dictionary-configuration)  | Columns for which local dictionary generation should be skipped. Useful when local dictionary need not be generated for few string/varchar/char columns |
| [COLUMN_META_CACHE](#caching-minmax-value-for-required-columns) | Columns whose metadata can be cached in Driver for efficient pruning and improved query performance |
| [CACHE_LEVEL](#caching-at-block-or-blocklet-level)           | Column metadata caching level. Whether to cache column metadata of block or blocklet |
| [FLAT_FOLDER](#support-flat-folder-same-as-hiveparquet)      | Whether to write all the carbondata files in a single folder.Not writing segments folder during incremental load |
| [LONG_STRING_COLUMNS](#string-longer-than-32000-characters)  | Columns which are greater than 32K characters                |
| [BUCKETNUMBER](#bucketing)                                   | Number of buckets to be created                              |
| [BUCKETCOLUMNS](#bucketing)                                  | Columns which are to be placed in buckets                    |
| [LOAD_MIN_SIZE_INMB](#load-minimum-data-size)                | Minimum input data size per node for data loading          |
| [Range Column](#range-column)                                | partition input data by range                              |

 Following are the guidelines for TBLPROPERTIES, CarbonData's additional table options can be set via carbon.properties.

   - ##### Local Dictionary Configuration

   Columns for which dictionary is not generated needs more storage space and in turn more IO. Also since more data will have to be read during query, query performance also would suffer.Generating dictionary per blocklet for such columns would help in saving storage space and assist in improving query performance as carbondata is optimized for handling dictionary encoded columns more effectively.Generating dictionary internally per blocklet is termed as local dictionary. Please refer to [File structure of Carbondata](./file-structure-of-carbondata.md) for understanding about the file structure of carbondata and meaning of terms like blocklet.

   Local Dictionary helps in:
   1. Getting more compression.
   2. Filter queries and full scan queries will be faster as filter will be done on encoded data.
   3. Reducing the store size and memory footprint as only unique values will be stored as part of local dictionary and corresponding data will be stored as encoded data.
   4. Getting higher IO throughput.

   **NOTE:** 

   * Following Data Types are Supported for Local Dictionary:
      * STRING
      * VARCHAR
      * CHAR

   * Following Data Types are not Supported for Local Dictionary: 
      * SMALLINT
      * INTEGER
      * BIGINT
      * DOUBLE
      * DECIMAL
      * TIMESTAMP
      * DATE
      * BOOLEAN
      * FLOAT
      * BYTE
      * Binary
   * In case of multi-level complex dataType columns, primitive string/varchar/char columns are considered for local dictionary generation.

   System Level Properties for Local Dictionary: 
   
   
   | Properties | Default value | Description |
   | ---------- | ------------- | ----------- |
   | carbon.local.dictionary.enable | true | By default, Local Dictionary will be enabled for the carbondata table. |
   | carbon.local.dictionary.decoder.fallback | true | Page Level data will not be maintained for the blocklet. During fallback, actual data will be retrieved from the encoded page data using local dictionary. **NOTE:** Memory footprint decreases significantly as compared to when this property is set to false |
    
   Local Dictionary can be configured using the following properties during create table command: 
          

| Properties | Default value | Description |
| ---------- | ------------- | ----------- |
| LOCAL_DICTIONARY_ENABLE | false | Whether to enable local dictionary generation. **NOTE:** If this property is defined, it will override the value configured at system level by '***carbon.local.dictionary.enable***'.Local dictionary will be generated for all string/varchar/char columns unless LOCAL_DICTIONARY_INCLUDE, LOCAL_DICTIONARY_EXCLUDE is configured. |
| LOCAL_DICTIONARY_THRESHOLD | 10000 | The maximum cardinality of a column upto which carbondata can try to generate local dictionary (maximum - 100000). **NOTE:** When LOCAL_DICTIONARY_THRESHOLD is defined for Complex columns, the count of distinct records of all child columns are summed up. |
| LOCAL_DICTIONARY_INCLUDE | string/varchar/char columns| Columns for which Local Dictionary has to be generated. This property needs to be configured only when local dictionary needs to be generated for few columns, skipping others. This property takes effect only when **LOCAL_DICTIONARY_ENABLE** is true or **carbon.local.dictionary.enable** is true |
| LOCAL_DICTIONARY_EXCLUDE | none | Columns for which Local Dictionary need not be generated. This property needs to be configured only when local dictionary needs to be skipped for few columns, generating for others. This property takes effect only when **LOCAL_DICTIONARY_ENABLE** is true or **carbon.local.dictionary.enable** is true |

   **Fallback behavior:** 

   * When the cardinality of a column exceeds the threshold, it triggers a fallback and the generated dictionary will be reverted and data loading will be continued without dictionary encoding.
   
   * In case of complex columns, fallback is triggered when the summation value of all child columns' distinct records exceeds the defined LOCAL_DICTIONARY_THRESHOLD value.

   **NOTE:** When fallback is triggered, the data loading performance will decrease as encoded data will be discarded and the actual data is written to the temporary sort files.

   **Points to be noted:**

   * Reduce Block size:
   
      Number of Blocks generated is less in case of Local Dictionary as compression ratio is high. This may reduce the number of tasks launched during query, resulting in degradation of query performance if the pruned blocks are less compared to the number of parallel tasks which can be run. So it is recommended to configure smaller block size which in turn generates more number of blocks.
      
### Example:

   ```
   CREATE TABLE carbontable(             
     column1 string,             
     column2 string,             
     column3 LONG)
   STORED AS carbondata
   TBLPROPERTIES('LOCAL_DICTIONARY_ENABLE'='true','LOCAL_DICTIONARY_THRESHOLD'='1000',
   'LOCAL_DICTIONARY_INCLUDE'='column1','LOCAL_DICTIONARY_EXCLUDE'='column2')
   ```

   **NOTE:** 

   * We recommend to use Local Dictionary when cardinality is high but is distributed across multiple loads
      
   - ##### Inverted Index Configuration

     By default inverted index is disabled as store size will be reduced, it can be enabled by using a table property. It might help to improve compression ratio and query speed, especially for low cardinality columns which are in reward position.
     Suggested use cases : For high cardinality columns, you can disable the inverted index for improving the data loading performance.
     
     **NOTE**: Columns specified in INVERTED_INDEX should also be present in SORT_COLUMNS.

     ```
     TBLPROPERTIES ('SORT_COLUMNS'='column2,column3','NO_INVERTED_INDEX'='column1', 'INVERTED_INDEX'='column2, column3')
     ```

   - ##### Sort Columns Configuration

     This property is for users to specify which columns belong to the MDK(Multi-Dimensions-Key) index.
     * If users don't specify "SORT_COLUMNS" property, by default no columns are sorted 
     * If this property is specified but with empty argument, then the table will be loaded without sort.
     * This supports only string, date, timestamp, short, int, long, byte and boolean data types.
     Suggested use cases : Only build MDK index for required columns,it might help to improve the data loading performance.

     ```
     TBLPROPERTIES ('SORT_COLUMNS'='column1, column3')
     ```

     **NOTE**: Sort_Columns for Complex datatype columns and binary data type is not supported.

   - ##### Sort Scope Configuration
   
     This property is for users to specify the scope of the sort during data load, following are the types of sort scope.
     
     * LOCAL_SORT: data will be locally sorted (task level sorting)             
     * NO_SORT: default scope. It will load the data in unsorted manner, it will significantly increase load performance.       
     * GLOBAL_SORT: It increases the query performance, especially high concurrent point query.
       And if you care about loading resources isolation strictly, because the system uses the spark GroupBy to sort data, the resource can be controlled by spark. 

 ### Example:

   ```
   CREATE TABLE IF NOT EXISTS productSchema.productSalesTable (
     productNumber INT,
     productName STRING,
     storeCity STRING,
     storeProvince STRING,
     productCategory STRING,
     productBatch STRING,
     saleQuantity INT,
     revenue INT)
   STORED AS carbondata
   TBLPROPERTIES ('SORT_COLUMNS'='productName,storeCity',
                  'SORT_SCOPE'='NO_SORT')
   ```

   **NOTE:** CarbonData also supports "using carbondata". Find example code at [SparkSessionExample](https://github.com/apache/carbondata/blob/master/examples/spark/src/main/scala/org/apache/carbondata/examples/SparkSessionExample.scala) in the CarbonData repo.

   - ##### Table Block Size Configuration

     This property is for setting block size of this table, the default value is 1024 MB and supports a range of 1 MB to 2048 MB.

     ```
     TBLPROPERTIES ('TABLE_BLOCKSIZE'='512')
     ```

     **NOTE:** 512 or 512M both are accepted.

   - ##### Table Blocklet Size Configuration

     This property is for setting blocklet size in the carbondata file, the default value is 64 MB.
     Blocklet is the minimum IO read unit, in case of point queries reduce blocklet size might improve the query performance.

     Example usage:
     ```
     TBLPROPERTIES ('TABLE_BLOCKLET_SIZE'='8')
     ```

   - ##### Table page Size Configuration

     This property is for setting page size in the carbondata file 
     and supports a range of 1 MB to 1755 MB.
     If page size crosses this value before 32000 rows, page will be cut to that many rows. 
     Helps in keeping page size to fit cpu cache size.

     This property can be configured if the table has string, varchar, binary or complex datatype columns.
     Because for these columns 32000 rows in one page may exceed 1755 MB and snappy compression will fail in that scenario.
     Also if page size is huge, page cannot be fit in CPU cache. 
     So, configuring smaller values of this property (say 1 MB) can result in better use of CPU cache for pages.

     Example usage:
     ```
     TBLPROPERTIES ('TABLE_PAGE_SIZE_INMB'='5')
     ```

   - ##### Table Compaction Configuration
   
     These properties are table level compaction configurations, if not specified, system level configurations in carbon.properties will be used.
     Following are 5 configurations:
     
     * MAJOR_COMPACTION_SIZE: same meaning as carbon.major.compaction.size, size in MB.
     * AUTO_LOAD_MERGE: same meaning as carbon.enable.auto.load.merge.
     * COMPACTION_LEVEL_THRESHOLD: same meaning as carbon.compaction.level.threshold.
     * COMPACTION_PRESERVE_SEGMENTS: same meaning as carbon.numberof.preserve.segments.
     * ALLOWED_COMPACTION_DAYS: same meaning as carbon.allowed.compaction.days.     

     ```
     TBLPROPERTIES ('MAJOR_COMPACTION_SIZE'='2048',
                    'AUTO_LOAD_MERGE'='true',
                    'COMPACTION_LEVEL_THRESHOLD'='5,6',
                    'COMPACTION_PRESERVE_SEGMENTS'='10',
                    'ALLOWED_COMPACTION_DAYS'='5')
     ```
     
   - ##### Streaming

     CarbonData supports streaming ingestion for real-time data. You can create the 'streaming' table using the following table properties.

     ```
     TBLPROPERTIES ('streaming'='true')
     ```


   - ##### Caching Min/Max Value for Required Columns

     By default, CarbonData caches min and max values of all the columns in schema.  As the load increases, the memory required to hold the min and max values increases considerably. This feature enables you to configure min and max values only for the required columns, resulting in optimized memory usage. This feature doesn't support binary data type.

      Following are the valid values for COLUMN_META_CACHE:
      * If you want no column min/max values to be cached in the driver.

      ```
      COLUMN_META_CACHE=''
      ```

      * If you want only col1 min/max values to be cached in the driver.

      ```
      COLUMN_META_CACHE='col1'
      ```

      * If you want min/max values to be cached in driver for all the specified columns.

      ```
      COLUMN_META_CACHE='col1,col2,col3,…'
      ```

      Columns to be cached can be specified either while creating table or after creation of the table.
      During create table operation; specify the columns to be cached in table properties.

      Syntax:

      ```
      CREATE TABLE [dbName].tableName (col1 String, col2 String, col3 int,…) STORED AS carbondata TBLPROPERTIES ('COLUMN_META_CACHE'='col1,col2,…')
      ```

      Example:

      ```
      CREATE TABLE employee (name String, city String, id int) STORED AS carbondata TBLPROPERTIES ('COLUMN_META_CACHE'='name')
      ```

      After creation of table or on already created tables use the alter table command to configure the columns to be cached.

      Syntax:

      ```
      ALTER TABLE [dbName].tableName SET TBLPROPERTIES ('COLUMN_META_CACHE'='col1,col2,…')
      ```

      Example:

      ```
      ALTER TABLE employee SET TBLPROPERTIES ('COLUMN_META_CACHE'='city')
      ```

   - ##### Caching at Block or Blocklet Level

     This feature allows you to maintain the cache at Block level, resulting in optimized usage of the memory. The memory consumption is high if the Blocklet level caching is maintained as a Block can have multiple Blocklet.

      Following are the valid values for CACHE_LEVEL:

      *Configuration for caching in driver at Block level (default value).*

      ```
      CACHE_LEVEL= 'BLOCK'
      ```

      *Configuration for caching in driver at Blocklet level.*

      ```
      CACHE_LEVEL= 'BLOCKLET'
      ```

      Cache level can be specified either while creating table or after creation of the table.
      During create table operation specify the cache level in table properties.

      Syntax:

      ```
      CREATE TABLE [dbName].tableName (col1 String, col2 String, col3 int,…) STORED AS carbondata TBLPROPERTIES ('CACHE_LEVEL'='Blocklet')
      ```

      Example:

      ```
      CREATE TABLE employee (name String, city String, id int) STORED AS carbondata TBLPROPERTIES ('CACHE_LEVEL'='Blocklet')
      ```

      After creation of table or on already created tables use the alter table command to configure the cache level.

      Syntax:

      ```
      ALTER TABLE [dbName].tableName SET TBLPROPERTIES ('CACHE_LEVEL'='Blocklet')
      ```

      Example:

      ```
      ALTER TABLE employee SET TBLPROPERTIES ('CACHE_LEVEL'='Blocklet')
      ```

   - ##### Support Flat folder same as Hive/Parquet

       This feature allows all carbondata and index files to keep directy under tablepath. Currently all carbondata/carbonindex files written under tablepath/Fact/Part0/Segment_NUM folder and it is not same as hive/parquet folder structure. This feature makes all files written will be directly under tablepath, it does not maintain any segment folder structure. This is useful for interoperability between the execution engines and plugin with other execution engines like hive or presto becomes easier.

       Following table property enables this feature and default value is false.
       ```
       'flat_folder'='true'
       ```

       Example:
       ```
       CREATE TABLE employee (name String, city String, id int) STORED AS carbondata TBLPROPERTIES ('flat_folder'='true')
       ```

   - ##### String longer than 32000 characters

     In common scenarios, the length of string is less than 32000,
     so carbondata stores the length of content using Short to reduce memory and space consumption.
     To support string longer than 32000 characters, carbondata introduces a table property called `LONG_STRING_COLUMNS`.
     For these columns, carbondata internally stores the length of content using Integer.

     You can specify the columns as 'long string column' using below tblProperties:

     ```
     // specify col1, col2 as long string columns
     TBLPROPERTIES ('LONG_STRING_COLUMNS'='col1,col2')
     ```

     Besides, you can also use this property through DataFrame by
     ```
     df.format("carbondata")
       .option("tableName", "carbonTable")
       .option("long_string_columns", "col1, col2")
       .save()
     ```

     If you are using Carbon-SDK, you can specify the datatype of long string column as `varchar`.
     You can refer to SDKwriterTestCase for example.

     **NOTE:** The LONG_STRING_COLUMNS can only be string/char/varchar columns and cannot be sort_columns/complex columns.

   - ##### Compression for table

     Data compression is also supported by CarbonData.
     By default, Snappy is used to compress the data. CarbonData also supports ZSTD compressor.
     User can specify the compressor in the table property:

     ```
     TBLPROPERTIES('carbon.column.compressor'='snappy')
     ```
     or
     ```
     TBLPROPERTIES('carbon.column.compressor'='zstd')
     ```
     If the compressor is configured, all the data loading and compaction will use that compressor.
     If the compressor is not configured, the data loading and compaction will use the compressor from current system property.
     In this scenario, the compressor for each load may differ if the system property is changed each time. This is helpful if you want to change the compressor for a table.
     The corresponding system property is configured in carbon.properties file as below:
     ```
     carbon.column.compressor=snappy
     ```
     or
     ```
     carbon.column.compressor=zstd
     ```

   - ##### Bad Records Path
     This property is used to specify the location where bad records would be written.
     As the table path remains the same after rename therefore the user can use this property to
     specify bad records path for the table at the time of creation, so that the same path can
     be later viewed in table description for reference.

     ```
     TBLPROPERTIES('BAD_RECORD_PATH'='/opt/badrecords')
     ```
     
   - ##### Load minimum data size
     This property indicates the minimum input data size per node for data loading.
     By default it is not enabled. Setting a non-zero integer value will enable this feature.
     This property is useful if you have a large cluster and only want a small portion of the nodes to process data loading.
     For example, if you have a cluster with 10 nodes and the input data is about 1GB. Without this property, each node will process about 100MB input data and result in at least 10 data files. With this property configured with 512, only 2 nodes will be chosen to process the input data, each with about 512MB input and result in about 2 or 4 files based on the compress ratio.
     Moreover, this property can also be specified in the load option.
     Notice that once you enable this feature, for load balance, carbondata will ignore the data locality while assigning input data to nodes, this will cause more network traffic.

     ```
     TBLPROPERTIES('LOAD_MIN_SIZE_INMB'='256')
     ```

   - ##### Range Column
     This property is used to specify a column to partition the input data by range.
     Only one column can be configured. During data loading, you can use "global_sort_partitions" or "scale_factor" to avoid generating small files.
     This feature doesn't support binary data type.

     ```
     TBLPROPERTIES('RANGE_COLUMN'='col1')
     ```

## CREATE TABLE AS SELECT
  This function allows user to create a Carbon table from any of the Parquet/Hive/Carbon table. This is beneficial when the user wants to create Carbon table from any other Parquet/Hive table and use the Carbon query engine to query and achieve better query results for cases where Carbon is faster than other file formats. Also this feature can be used for backing up the data.

  ```
  CREATE TABLE [IF NOT EXISTS] [db_name.]table_name 
  STORED AS carbondata 
  [TBLPROPERTIES (key1=val1, key2=val2, ...)] 
  AS select_statement;
  ```

### Examples
  ```
  carbon.sql(
             s"""
                | CREATE TABLE source_table(
                |   id INT,
                |   name STRING,
                |   city STRING,
                |   age INT)
                | STORED AS parquet
             """.stripMargin)
                
  carbon.sql("INSERT INTO source_table SELECT 1,'bob','shenzhen',27")
  
  carbon.sql("INSERT INTO source_table SELECT 2,'david','shenzhen',31")
  
  carbon.sql(
             s"""
                | CREATE TABLE target_table
                | STORED AS carbondata
                | AS SELECT city, avg(age) 
                |    FROM source_table 
                |    GROUP BY city
             """.stripMargin)
              
  carbon.sql("SELECT * FROM target_table").show
  
  // results:
  //    +--------+--------+
  //    |    city|avg(age)|
  //    +--------+--------+
  //    |shenzhen|    29.0|
  //    +--------+--------+

  ```

## CREATE EXTERNAL TABLE
  This function allows user to create external table by specifying location.
  ```
  CREATE EXTERNAL TABLE [IF NOT EXISTS] [db_name.]table_name 
  STORED AS carbondata LOCATION '$FilesPath'
  ```

### Create external table on managed table data location.
  Managed table data location provided will have both FACT and Metadata folder. 
  This data can be generated by creating a normal carbon table and use this path as $FilesPath in the above syntax.

  **Example:**
  ```
  sql("CREATE TABLE origin(key INT, value STRING) STORED AS carbondata")
  sql("INSERT INTO origin select 100,'spark'")
  sql("INSERT INTO origin select 200,'hive'")
  // creates a table in $storeLocation/origin
  
  sql(
      s"""
         | CREATE EXTERNAL TABLE source
         | STORED AS carbondata
         | LOCATION '$storeLocation/origin'
      """.stripMargin)
  checkAnswer(sql("SELECT count(*) from source"), sql("SELECT count(*) from origin"))
  ```

### Create external table on Non-Transactional table data location.
  Non-Transactional table data location will have only carbondata and carbonindex files, there will not be a metadata folder (table status and schema).
  Our SDK module currently supports writing data in this format.

  **Example:**
  ```
  sql(
      s"""
         | CREATE EXTERNAL TABLE sdkOutputTable STORED AS carbondata LOCATION
         |'$writerPath'
      """.stripMargin)
  ```

  Here writer path will have carbondata and index files.
  This can be SDK output or C++ SDK output. Refer [SDK Guide](./sdk-guide.md) and [C++ SDK Guide](./csdk-guide.md). 

  **Note:**
  1. Dropping of the external table should not delete the files present in the location.
  2. When external table is created on non-transactional table data, 
    external table will be registered with the schema of carbondata files.
    If multiple files with different schema is present, exception will be thrown.
    So, If table registered with one schema and files are of different schema, 
    suggest to drop the external table and create again to register table with new schema.  


## CREATE DATABASE 
  This function creates a new database. By default the database is created in Carbon store location, but you can also specify custom location.
  ```
  CREATE DATABASE [IF NOT EXISTS] database_name [LOCATION path];
  ```

### Example
  ```
  CREATE DATABASE carbon LOCATION "hdfs://name_cluster/dir1/carbonstore";
  ```

## TABLE MANAGEMENT  

### SHOW TABLE

  This command can be used to list all the tables in current database or all the tables of a specific database.
  ```
  SHOW TABLES [IN db_Name]
  ```

  Example:
  ```
  SHOW TABLES
  OR
  SHOW TABLES IN defaultdb
  ```

### ALTER TABLE

  The following section introduce the commands to modify the physical or logical state of the existing table(s).

   - #### RENAME TABLE
   
     This command is used to rename the existing table.
     ```
     ALTER TABLE [db_name.]table_name RENAME TO new_table_name
     ```

     Examples:
     ```
     ALTER TABLE carbon RENAME TO carbonTable
     OR
     ALTER TABLE test_db.carbon RENAME TO test_db.carbonTable
     ```

   - #### ADD COLUMNS
   
     This command is used to add a new column to the existing table.
     ```
     ALTER TABLE [db_name.]table_name ADD COLUMNS (col_name data_type,...)
     TBLPROPERTIES('DEFAULT.VALUE.COLUMN_NAME'='default_value')
     ```

     Examples:
     ```
     ALTER TABLE carbon ADD COLUMNS (a1 INT, b1 STRING)
     ```

     ```
     ALTER TABLE carbon ADD COLUMNS (a1 INT, b1 STRING) TBLPROPERTIES('DEFAULT.VALUE.a1'='10')
     ```
      **NOTE:** Add Complex datatype columns is not supported.

Users can specify which columns to include and exclude for local dictionary generation after adding new columns. These will be appended with the already existing local dictionary include and exclude columns of main table respectively.
     ```
     ALTER TABLE carbon ADD COLUMNS (a1 STRING, b1 STRING) TBLPROPERTIES('LOCAL_DICTIONARY_INCLUDE'='a1','LOCAL_DICTIONARY_EXCLUDE'='b1')
     ```

   - #### DROP COLUMNS
   
     This command is used to delete the existing column(s) in a table.

     ```
     ALTER TABLE [db_name.]table_name DROP COLUMNS (col_name, ...)
     ```

     Examples:

     ```
     ALTER TABLE carbon DROP COLUMNS (b1)
     OR
     ALTER TABLE test_db.carbon DROP COLUMNS (b1)
     
     ALTER TABLE carbon DROP COLUMNS (c1,d1)
     ```

     **NOTE:** Drop Complex child column is not supported.

   - #### CHANGE COLUMN NAME/TYPE
   
     This command is used to change column name and the data type from INT to BIGINT or decimal precision from lower to higher.
     Change of decimal data type from lower precision to higher precision will only be supported for cases where there is no data loss.

     ```
     ALTER TABLE [db_name.]table_name CHANGE col_old_name col_new_name column_type
     ```

     Valid Scenarios
     - Invalid scenario - Change of decimal precision from (10,2) to (10,5) is invalid as in this case only scale is increased but total number of digits remains the same.
     - Valid scenario - Change of decimal precision from (10,2) to (12,3) is valid as the total number of digits are increased by 2 but scale is increased only by 1 which will not lead to any data loss.
     - **NOTE:** The allowed range is 38,38 (precision, scale) and is a valid upper case scenario which is not resulting in data loss.

     Example1:Change column a1's name to a2 and its data type from INT to BIGINT.

     ```
     ALTER TABLE test_db.carbon CHANGE a1 a2 BIGINT
     ```
     
     Example2:Changing decimal precision of column a1 from 10 to 18.

     ```
     ALTER TABLE test_db.carbon CHANGE a1 a1 DECIMAL(18,2)
     ```

     Example3:Change column a3's name to a4.

     ```
     ALTER TABLE test_db.carbon CHANGE a3 a4 STRING
     ```

     **NOTE:** Once the column is renamed, user has to take care about replacing the fileheader with the new name or changing the column header in csv file.
   
   - #### MERGE INDEX

     This command is used to merge all the CarbonData index files (.carbonindex) inside a segment to a single CarbonData index merge file (.carbonindexmerge). This enhances the first query performance.

     ```
     ALTER TABLE [db_name.]table_name COMPACT 'SEGMENT_INDEX'
     ```

     Examples:

     ```
     ALTER TABLE test_db.carbon COMPACT 'SEGMENT_INDEX'
     ```

     **NOTE:**

     * Merge index is not supported on streaming table.

   - #### SET and UNSET
   
     When set command is used, all the newly set properties will override the corresponding old properties if exists.
  
     - ##### Local Dictionary Properties
       Example to SET Local Dictionary Properties:
       ```
       ALTER TABLE tablename SET TBLPROPERTIES('LOCAL_DICTIONARY_ENABLE'='false','LOCAL_DICTIONARY_THRESHOLD'='1000','LOCAL_DICTIONARY_INCLUDE'='column1','LOCAL_DICTIONARY_EXCLUDE'='column2')
       ```
       When Local Dictionary properties are unset, corresponding default values will be used for these properties.
    
       Example to UNSET Local Dictionary Properties:
       ```
       ALTER TABLE tablename UNSET TBLPROPERTIES('LOCAL_DICTIONARY_ENABLE','LOCAL_DICTIONARY_THRESHOLD','LOCAL_DICTIONARY_INCLUDE','LOCAL_DICTIONARY_EXCLUDE')
       ```
    
       **NOTE:** For old tables, by default, local dictionary is disabled. If user wants local dictionary for these tables, user can enable/disable local dictionary for new data at their discretion.
       This can be achieved by using the alter table set command.
  
     - ##### SORT SCOPE
       Example to SET SORT SCOPE:
       ```
       ALTER TABLE tablename SET TBLPROPERTIES('SORT_SCOPE'='NO_SORT')
       ```
       When Sort Scope is unset, the default values (NO_SORT) will be used.
    
       Example to UNSET SORT SCOPE:
       ```
       ALTER TABLE tablename UNSET TBLPROPERTIES('SORT_SCOPE')
       ```

     - ##### SORT COLUMNS
       Example to SET SORT COLUMNS:
       ```
       ALTER TABLE tablename SET TBLPROPERTIES('SORT_COLUMNS'='column1')
       ```
       After this operation, the new loading will use the new SORT_COLUMNS. The user can adjust 
       the SORT_COLUMNS according to the query, but it will not impact the old data directly. So 
       it will not impact the query performance of the old data segments which are not sorted by 
       new SORT_COLUMNS.  
       
       UNSET is not supported, but it can set SORT_COLUMNS to empty string instead of using UNSET.
       NOTE: When SORT_SCOPE is not NO_SORT, then setting SORT_COLUMNS to empty string is not valid.
       ```
       ALTER TABLE tablename SET TBLPROPERTIES('SORT_COLUMNS'='')
       ```

       **NOTE:**
        * The "custom" compaction support re-sorting the old segment one by one in version 1.6 or later.
        * The streaming table is not supported for SORT_COLUMNS modification.
        * If the inverted index columns are removed from the new SORT_COLUMNS, they will not 
        create the inverted index. But the old configuration of INVERTED_INDEX will be kept.

### DROP TABLE

  This command is used to delete an existing table.
  ```
  DROP TABLE [IF EXISTS] [db_name.]table_name
  ```

  Example:
  ```
  DROP TABLE IF EXISTS productSchema.productSalesTable
  ```

### REFRESH TABLE

  This command is used to register Carbon table to HIVE meta store catalogue from existing Carbon table data.
  ```
  REFRESH TABLE $db_NAME.$table_NAME
  ```

  Example:
  ```
  REFRESH TABLE dbcarbon.productSalesTable
  ```

  **NOTE:** 
  * The new database name and the old database name should be same.
  * Before executing this command the old table schema and data should be copied into the new database location.
  * If the table is aggregate table, then all the aggregate tables should be copied to the new database location.
  * For old store, the time zone of the source and destination cluster should be same.
  * If old cluster used HIVE meta store to store schema, refresh will not work as schema file does not exist in file system.

### Table and Column Comment

  You can provide more information on table by using table comment. Similarly you can provide more information about a particular column using column comment. 
  You can see the column comment of an existing table using describe formatted command.

  ```
  CREATE TABLE [IF NOT EXISTS] [db_name.]table_name[(col_name data_type [COMMENT col_comment], ...)]
    [COMMENT table_comment]
  STORED AS carbondata
  [TBLPROPERTIES (property_name=property_value, ...)]
  ```

  Example:
  ```
  CREATE TABLE IF NOT EXISTS productSchema.productSalesTable (
                                productNumber Int COMMENT 'unique serial number for product')
  COMMENT "This is table comment"
  STORED AS carbondata
  ```

  You can also SET and UNSET table comment using ALTER command.

  Example to SET table comment:

  ```
  ALTER TABLE carbon SET TBLPROPERTIES ('comment'='this table comment is modified');
  ```

  Example to UNSET table comment:

  ```
  ALTER TABLE carbon UNSET TBLPROPERTIES ('comment');
  ```

## PARTITION

### STANDARD PARTITION

  The partition is similar as spark and hive partition, user can use any column to build partition:

#### Create Partition Table

  This command allows you to create table with partition.

  ```
  CREATE TABLE [IF NOT EXISTS] [db_name.]table_name 
    [(col_name data_type , ...)]
    [COMMENT table_comment]
    [PARTITIONED BY (col_name data_type , ...)]
    [STORED AS file_format]
    [TBLPROPERTIES (property_name=property_value, ...)]
  ```

  Example:
  ```
  CREATE TABLE IF NOT EXISTS productSchema.productSalesTable (
                                productNumber INT,
                                productName STRING,
                                storeCity STRING,
                                storeProvince STRING,
                                saleQuantity INT,
                                revenue INT)
  PARTITIONED BY (productCategory STRING, productBatch STRING)
  STORED AS carbondata
  ```
   **NOTE:** Hive partition is not supported on complex data type columns.


#### Show Partitions

  This command gets the Hive partition information of the table

  ```
  SHOW PARTITIONS [db_name.]table_name
  ```

#### Drop Partition

  This command drops the specified Hive partition only.
  ```
  ALTER TABLE table_name DROP [IF EXISTS] PARTITION (part_spec, ...)
  ```

  Example:
  ```
  ALTER TABLE locationTable DROP PARTITION (country = 'US');
  ```

#### Insert OVERWRITE

  This command allows you to insert or load overwrite on a specific partition.

  ```
  INSERT OVERWRITE TABLE table_name
  PARTITION (column = 'partition_name')
  select_statement
  ```

  Example:
  ```
  INSERT OVERWRITE TABLE partitioned_user
  PARTITION (country = 'US')
  SELECT * FROM another_user au 
  WHERE au.country = 'US';
  ```


### Show Partitions

  The following command is executed to get the partition information of the table

  ```
  SHOW PARTITIONS [db_name.]table_name
  ```

### Add a new partition

  ```
  ALTER TABLE [db_name].table_name ADD PARTITION('new_partition')
  ```

### Split a partition

  ```
  ALTER TABLE [db_name].table_name SPLIT PARTITION(partition_id) INTO('new_partition1', 'new_partition2'...)
  ```

### Drop a partition

  Only drop partition definition, but keep data
  ```
  ALTER TABLE [db_name].table_name DROP PARTITION(partition_id)
  ```

  Drop both partition definition and data
  ```
  ALTER TABLE [db_name].table_name DROP PARTITION(partition_id) WITH DATA
  ```

  **NOTE:**
  * Hash partition table is not supported for ADD, SPLIT and DROP commands.
  * Partition Id: in CarbonData like the hive, folders are not used to divide partitions instead partition id is used to replace the task id. It could make use of the characteristic and meanwhile reduce some metadata.

  ```
  SegmentDir/0_batchno0-0-1502703086921.carbonindex
            ^
  SegmentDir/part-0-0_batchno0-0-1502703086921.carbondata
                     ^
  ```

  Here are some useful tips to improve query performance of carbonData partition table:
  * The partitioned column can be excluded from SORT_COLUMNS, this will let other columns to do the efficient sorting.
  * When writing SQL on a partition table, try to use filters on the partition column.

## BUCKETING

  Bucketing feature can be used to distribute/organize the table/partition data into multiple files such
  that similar records are present in the same file. While creating a table, user needs to specify the
  columns to be used for bucketing and the number of buckets. For the selection of bucket the Hash value
  of columns is used.

  ```
  CREATE TABLE [IF NOT EXISTS] [db_name.]table_name
                    [(col_name data_type, ...)]
  STORED AS carbondata
  TBLPROPERTIES('BUCKETNUMBER'='noOfBuckets',
  'BUCKETCOLUMNS'='columnname')
  ```

  **NOTE:**
  * Bucketing cannot be performed for columns of Complex Data Types.
  * Columns in the BUCKETCOLUMN parameter must be dimensions. The BUCKETCOLUMN parameter cannot be a measure or a combination of measures and dimensions.

  Example:
  ```
  CREATE TABLE IF NOT EXISTS productSchema.productSalesTable (
    productNumber INT,
    saleQuantity INT,
    productName STRING,
    storeCity STRING,
    storeProvince STRING,
    productCategory STRING,
    productBatch STRING,
    revenue INT)
  STORED AS carbondata
  TBLPROPERTIES ('BUCKETNUMBER'='4', 'BUCKETCOLUMNS'='productName')
  ```

## CACHE

  CarbonData internally uses LRU caching to improve the performance. The user can get information 
  about current cache used status in memory through the following command:

  ```sql
  SHOW METACACHE
  ``` 
  
  This shows the overall memory consumed in the cache by categories - index files, dictionary and 
  datamaps. This also shows the cache usage by all the tables and children tables in the current 
  database.
  
   ```sql
    SHOW EXECUTOR METACACHE
   ``` 
    
   This shows the overall memory consumed by the cache in each executor of the Index
   Server. This command is only allowed when the carbon property `carbon.enable.index.server` 
   is set to true.
  
  ```sql
  SHOW METACACHE ON TABLE tableName
  ```
  
  This shows detailed information on cache usage by the table `tableName` and its carbonindex files, 
  its dictionary files, its datamaps and children tables.
  
  This command is not allowed on child tables.

  ```sql
    DROP METACACHE ON TABLE tableName
   ```
    
  This clears any entry in cache by the table `tableName`, its carbonindex files, 
  its dictionary files, its datamaps and children tables.
    
  This command is not allowed on child tables.

### Important points

  1. Cache information is updated only after the select query is executed. 
  
  2. In case of alter table the already loaded cache is invalidated when any subsequent select query
  is fired.

  3. Dictionary is loaded in cache only when the dictionary columns are queried upon. If we don't do
  direct query on dictionary column, cache will not be loaded.
  If we do `SELECT * FROM t1`, and even though for this case dictionary is loaded, it is loaded in
  executor and not on driver, and the final result rows are returned back to driver, and thus will
  produce no trace on driver cache if we do `SHOW METACACHE` or `SHOW METACACHE ON TABLE t1`.
