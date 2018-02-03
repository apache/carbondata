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

# Data Management on CarbonData

This tutorial is going to introduce all commands and data operations on CarbonData.

* [CREATE TABLE](#create-table)
* [CREATE DATABASE] (#create-database)
* [TABLE MANAGEMENT](#table-management)
* [LOAD DATA](#load-data)
* [UPDATE AND DELETE](#update-and-delete)
* [COMPACTION](#compaction)
* [PARTITION](#partition)
* [PRE-AGGREGATE TABLES](#agg-tables)
* [BUCKETING](#bucketing)
* [SEGMENT MANAGEMENT](#segment-management)

## CREATE TABLE

  This command can be used to create a CarbonData table by specifying the list of fields along with the table properties. You can also specify the location where the table needs to be stored.
  
  ```
  CREATE TABLE [IF NOT EXISTS] [db_name.]table_name[(col_name data_type , ...)]
  STORED BY 'carbondata'
  [TBLPROPERTIES (property_name=property_value, ...)]
  [LOCATION 'path']
  ```  
  
### Usage Guidelines

  Following are the guidelines for TBLPROPERTIES, CarbonData's additional table options can be set via carbon.properties.
  
   - **Dictionary Encoding Configuration**

     Dictionary encoding is turned off for all columns by default from 1.3 onwards, you can use this command for including or excluding columns to do dictionary encoding.
     Suggested use cases : do dictionary encoding for low cardinality columns, it might help to improve data compression ratio and performance.

     ```
     TBLPROPERTIES ('DICTIONARY_INCLUDE'='column1, column2')
	 ```
     
	 NOTE: DICTIONARY_EXCLUDE supports only int, string, timestamp, long, bigint, and varchar data types.
	 
   - **Inverted Index Configuration**

     By default inverted index is enabled, it might help to improve compression ratio and query speed, especially for low cardinality columns which are in reward position.
     Suggested use cases : For high cardinality columns, you can disable the inverted index for improving the data loading performance.

     ```
     TBLPROPERTIES ('NO_INVERTED_INDEX'='column1, column3')
     ```

   - **Sort Columns Configuration**

     This property is for users to specify which columns belong to the MDK(Multi-Dimensions-Key) index.
     * If users don't specify "SORT_COLUMN" property, by default MDK index be built by using all dimension columns except complex data type column. 
     * If this property is specified but with empty argument, then the table will be loaded without sort.
	 * This supports only string, date, timestamp, short, int, long, and boolean data types.
     Suggested use cases : Only build MDK index for required columns,it might help to improve the data loading performance.

     ```
     TBLPROPERTIES ('SORT_COLUMNS'='column1, column3')
     OR
     TBLPROPERTIES ('SORT_COLUMNS'='')
     ```

   - **Sort Scope Configuration**
   
     This property is for users to specify the scope of the sort during data load, following are the types of sort scope.
     
     * LOCAL_SORT: It is the default sort scope.             
     * NO_SORT: It will load the data in unsorted manner, it will significantly increase load performance.       
     * BATCH_SORT: It increases the load performance but decreases the query performance if identified blocks > parallelism.
     * GLOBAL_SORT: It increases the query performance, especially high concurrent point query.
       And if you care about loading resources isolation strictly, because the system uses the spark GroupBy to sort data, the resource can be controlled by spark. 
 
   - **Table Block Size Configuration**

     This command is for setting block size of this table, the default value is 1024 MB and supports a range of 1 MB to 2048 MB.

     ```
     TBLPROPERTIES ('TABLE_BLOCKSIZE'='512')
     ```
     NOTE: 512 or 512M both are accepted.

   - **Table Compaction Configuration**
   
     These properties are table level compaction configurations, if not specified, system level configurations in carbon.properties will be used.
     Following are 5 configurations:
     
     * MAJOR_COMPACTION_SIZE: same meaning with carbon.major.compaction.size, size in MB.
     * AUTO_LOAD_MERGE: same meaning with carbon.enable.auto.load.merge.
     * COMPACTION_LEVEL_THRESHOLD: same meaning with carbon.compaction.level.threshold.
     * COMPACTION_PRESERVE_SEGMENTS: same meaning with carbon.numberof.preserve.segments.
     * ALLOWED_COMPACTION_DAYS: same meaning with carbon.allowed.compaction.days.     

     ```
     TBLPROPERTIES ('MAJOR_COMPACTION_SIZE'='2048',
                    'AUTO_LOAD_MERGE'='true',
                    'COMPACTION_LEVEL_THRESHOLD'='5,6',
                    'COMPACTION_PRESERVE_SEGMENTS'='10',
                    'ALLOWED_COMPACTION_DAYS'='5')
     ```
     
   - **Streaming**

     CarbonData supports streaming ingestion for real-time data. You can create the ‘streaming’ table using the following table properties.

     ```
     TBLPROPERTIES ('streaming'='true')
     ```

### Example:

   ```
    CREATE TABLE IF NOT EXISTS productSchema.productSalesTable (
                                   productNumber Int,
                                   productName String,
                                   storeCity String,
                                   storeProvince String,
                                   productCategory String,
                                   productBatch String,
                                   saleQuantity Int,
                                   revenue Int)
    STORED BY 'carbondata'
    TBLPROPERTIES ('DICTIONARY_INCLUDE'='productNumber',
                   'NO_INVERTED_INDEX'='productBatch',
                   'SORT_COLUMNS'='productName,storeCity',
                   'SORT_SCOPE'='NO_SORT',
                   'TABLE_BLOCKSIZE'='512',
                   'MAJOR_COMPACTION_SIZE'='2048',
                   'AUTO_LOAD_MERGE'='true',
                   'COMPACTION_LEVEL_THRESHOLD'='5,6',
                   'COMPACTION_PRESERVE_SEGMENTS'='10',
				   'streaming'='true',
                   'ALLOWED_COMPACTION_DAYS'='5')
   ```

## CREATE DATABASE 
  This function creates a new database. By default the database is created in Carbon store location, but you can also specify custom location.
  ```
  CREATE DATABASE [IF NOT EXISTS] database_name [LOCATION path];
  ```
  
### Example
  ```
  CREATE DATABASE carbon LOCATION “hdfs://name_cluster/dir1/carbonstore”;
  ```

## CREATE TABLE As SELECT
  This function allows you to create a Carbon table from any of the Parquet/Hive/Carbon table. This is beneficial when the user wants to create Carbon table from any other Parquet/Hive table and use the Carbon query engine to query and achieve better query results for cases where Carbon is faster than other file formats. Also this feature can be used for backing up the data.
  ```
  CREATE TABLE [IF NOT EXISTS] [db_name.]table_name STORED BY 'carbondata' [TBLPROPERTIES (key1=val1, key2=val2, ...)] AS select_statement;
  ```

### Examples
  ```
  CREATE TABLE ctas_select_parquet STORED BY 'carbondata' as select * from parquet_ctas_test;
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

### ALTER TALBE

  The following section introduce the commands to modify the physical or logical state of the existing table(s).

   - **RENAME TABLE**
   
     This command is used to rename the existing table.
     ```
     ALTER TABLE [db_name.]table_name RENAME TO new_table_name
     ```

     Examples:
     ```
     ALTER TABLE carbon RENAME TO carbondata
     OR
     ALTER TABLE test_db.carbon RENAME TO test_db.carbondata
     ```

   - **ADD COLUMNS**
   
     This command is used to add a new column to the existing table.
     ```
     ALTER TABLE [db_name.]table_name ADD COLUMNS (col_name data_type,...)
     TBLPROPERTIES('DICTIONARY_INCLUDE'='col_name,...',
     'DEFAULT.VALUE.COLUMN_NAME'='default_value')
     ```

     Examples:
     ```
     ALTER TABLE carbon ADD COLUMNS (a1 INT, b1 STRING)
     ```

     ```
     ALTER TABLE carbon ADD COLUMNS (a1 INT, b1 STRING) TBLPROPERTIES('DICTIONARY_INCLUDE'='a1')
     ```

     ```
     ALTER TABLE carbon ADD COLUMNS (a1 INT, b1 STRING) TBLPROPERTIES('DEFAULT.VALUE.a1'='10')
     ```

   - **DROP COLUMNS**
   
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

   - **CHANGE DATA TYPE**
   
     This command is used to change the data type from INT to BIGINT or decimal precision from lower to higher.
     Change of decimal data type from lower precision to higher precision will only be supported for cases where there is no data loss.
     ```
     ALTER TABLE [db_name.]table_name CHANGE col_name col_name changed_column_type
     ```

     Valid Scenarios
     - Invalid scenario - Change of decimal precision from (10,2) to (10,5) is invalid as in this case only scale is increased but total number of digits remains the same.
     - Valid scenario - Change of decimal precision from (10,2) to (12,3) is valid as the total number of digits are increased by 2 but scale is increased only by 1 which will not lead to any data loss.
     - NOTE: The allowed range is 38,38 (precision, scale) and is a valid upper case scenario which is not resulting in data loss.

     Example1:Changing data type of column a1 from INT to BIGINT.
     ```
     ALTER TABLE test_db.carbon CHANGE a1 a1 BIGINT
     ```
     
     Example2:Changing decimal precision of column a1 from 10 to 18.
     ```
     ALTER TABLE test_db.carbon CHANGE a1 a1 DECIMAL(18,2)
     ```
- **MERGE INDEX**
   
     This command is used to merge all the CarbonData index files (.carbonindex) inside a segment to a single CarbonData index merge file (.carbonindexmerge). This enhances the first query performance.
     ```
      ALTER TABLE [db_name.]table_name COMPACT 'SEGMENT_INDEX'
      ```
      
      Examples:
      ```
      ALTER TABLE test_db.carbon COMPACT 'SEGMENT_INDEX'
      ```

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
  NOTE: 
  * The new database name and the old database name should be same.
  * Before executing this command the old table schema and data should be copied into the new database location.
  * If the table is aggregate table, then all the aggregate tables should be copied to the new database location.
  * For old store, the time zone of the source and destination cluster should be same.
  * If old cluster uses HIVE meta store, refresh will not work as schema file does not exist in file system.
  

## LOAD DATA

### LOAD FILES TO CARBONDATA TABLE
  
  This command is used to load csv files to carbondata, OPTIONS are not mandatory for data loading process. 
  Inside OPTIONS user can provide either of any options like DELIMITER, QUOTECHAR, FILEHEADER, ESCAPECHAR, MULTILINE as per requirement.
  
  ```
  LOAD DATA [LOCAL] INPATH 'folder_path' 
  INTO TABLE [db_name.]table_name 
  OPTIONS(property_name=property_value, ...)
  ```

  You can use the following options to load data:
  
  - **DELIMITER:** Delimiters can be provided in the load command.

    ``` 
    OPTIONS('DELIMITER'=',')
    ```

  - **QUOTECHAR:** Quote Characters can be provided in the load command.

    ```
    OPTIONS('QUOTECHAR'='"')
    ```

  - **COMMENTCHAR:** Comment Characters can be provided in the load command if user want to comment lines.

    ```
    OPTIONS('COMMENTCHAR'='#')
    ```

  - **FILEHEADER:** Headers can be provided in the LOAD DATA command if headers are missing in the source files.

    ```
    OPTIONS('FILEHEADER'='column1,column2') 
    ```

  - **MULTILINE:** CSV with new line character in quotes.

    ```
    OPTIONS('MULTILINE'='true') 
    ```

  - **ESCAPECHAR:** Escape char can be provided if user want strict validation of escape character on CSV.

    ```
    OPTIONS('ESCAPECHAR'='\') 
    ```
  - **SKIP_EMPTY_LINE:** This option will ignore the empty line in the CSV file during the data load.

    ```
    OPTIONS('SKIP_EMPTY_LINE'='TRUE/FALSE') 
    ```

  - **COMPLEX_DELIMITER_LEVEL_1:** Split the complex type data column in a row (eg., a$b$c --> Array = {a,b,c}).

    ```
    OPTIONS('COMPLEX_DELIMITER_LEVEL_1'='$') 
    ```

  - **COMPLEX_DELIMITER_LEVEL_2:** Split the complex type nested data column in a row. Applies level_1 delimiter & applies level_2 based on complex data type (eg., a:b$c:d --> Array> = {{a,b},{c,d}}).

    ```
    OPTIONS('COMPLEX_DELIMITER_LEVEL_2'=':')
    ```

  - **ALL_DICTIONARY_PATH:** All dictionary files path.

    ```
    OPTIONS('ALL_DICTIONARY_PATH'='/opt/alldictionary/data.dictionary')
    ```

  - **COLUMNDICT:** Dictionary file path for specified column.

    ```
    OPTIONS('COLUMNDICT'='column1:dictionaryFilePath1,column2:dictionaryFilePath2')
    ```
    NOTE: ALL_DICTIONARY_PATH and COLUMNDICT can't be used together.
    
  - **DATEFORMAT/TIMESTAMPFORMAT:** Date and Timestamp format for specified column.

    ```
    OPTIONS('DATEFORMAT' = 'yyyy-MM-dd','TIMESTAMPFORMAT'='yyyy-MM-dd HH:mm:ss')
    ```
    NOTE: Date formats are specified by date pattern strings. The date pattern letters in CarbonData are same as in JAVA. Refer to [SimpleDateFormat](http://docs.oracle.com/javase/7/docs/api/java/text/SimpleDateFormat.html).

  - **SINGLE_PASS:** Single Pass Loading enables single job to finish data loading with dictionary generation on the fly. It enhances performance in the scenarios where the subsequent data loading after initial load involves fewer incremental updates on the dictionary.

  This option specifies whether to use single pass for loading data or not. By default this option is set to FALSE.

   ```
    OPTIONS('SINGLE_PASS'='TRUE')
   ```

   NOTE:
   * If this option is set to TRUE then data loading will take less time.
   * If this option is set to some invalid value other than TRUE or FALSE then it uses the default value.

   Example:

   ```
   LOAD DATA local inpath '/opt/rawdata/data.csv' INTO table carbontable
   options('DELIMITER'=',', 'QUOTECHAR'='"','COMMENTCHAR'='#',
   'FILEHEADER'='empno,empname,designation,doj,workgroupcategory,
   workgroupcategoryname,deptno,deptname,projectcode,
   projectjoindate,projectenddate,attendance,utilization,salary',
   'MULTILINE'='true','ESCAPECHAR'='\','COMPLEX_DELIMITER_LEVEL_1'='$',
   'COMPLEX_DELIMITER_LEVEL_2'=':',
   'ALL_DICTIONARY_PATH'='/opt/alldictionary/data.dictionary',
   'SINGLE_PASS'='TRUE')
   ```

  - **BAD RECORDS HANDLING:** Methods of handling bad records are as follows:

    * Load all of the data before dealing with the errors.
    * Clean or delete bad records before loading data or stop the loading when bad records are found.

    ```
    OPTIONS('BAD_RECORDS_LOGGER_ENABLE'='true', 'BAD_RECORD_PATH'='hdfs://hacluster/tmp/carbon', 'BAD_RECORDS_ACTION'='REDIRECT', 'IS_EMPTY_DATA_BAD_RECORD'='false')
    ```

  NOTE:
  * BAD_RECORDS_ACTION property can have four type of actions for bad records FORCE, REDIRECT, IGNORE and FAIL.
  * FAIL option is its Default value. If the FAIL option is used, then data loading fails if any bad records are found.
  * If the REDIRECT option is used, CarbonData will add all bad records in to a separate CSV file. However, this file must not be used for subsequent data loading because the content may not exactly match the source record. You are advised to cleanse the original source record for further data ingestion. This option is used to remind you which records are bad records.
  * If the FORCE option is used, then it auto-corrects the data by storing the bad records as NULL before Loading data.
  * If the IGNORE option is used, then bad records are neither loaded nor written to the separate CSV file.
  * In loaded data, if all records are bad records, the BAD_RECORDS_ACTION is invalid and the load operation fails.
  * The maximum number of characters per column is 100000. If there are more than 100000 characters in a column, data loading will fail.

  Example:

  ```
  LOAD DATA INPATH 'filepath.csv' INTO TABLE tablename
  OPTIONS('BAD_RECORDS_LOGGER_ENABLE'='true','BAD_RECORD_PATH'='hdfs://hacluster/tmp/carbon',
  'BAD_RECORDS_ACTION'='REDIRECT','IS_EMPTY_DATA_BAD_RECORD'='false')
  ```

### INSERT DATA INTO CARBONDATA TABLE

  This command inserts data into a CarbonData table, it is defined as a combination of two queries Insert and Select query respectively. 
  It inserts records from a source table into a target CarbonData table, the source table can be a Hive table, Parquet table or a CarbonData table itself. 
  It comes with the functionality to aggregate the records of a table by performing Select query on source table and load its corresponding resultant records into a CarbonData table.

  ```
  INSERT INTO TABLE <CARBONDATA TABLE> SELECT * FROM sourceTableName 
  [ WHERE { <filter_condition> } ]
  ```

  You can also omit the `table` keyword and write your query as:
 
  ```
  INSERT INTO <CARBONDATA TABLE> SELECT * FROM sourceTableName 
  [ WHERE { <filter_condition> } ]
  ```

  Overwrite insert data:
  ```
  INSERT OVERWRITE <CARBONDATA TABLE> SELECT * FROM sourceTableName 
  [ WHERE { <filter_condition> } ]
  ```

  NOTE:
  * The source table and the CarbonData table must have the same table schema.
  * The data type of source and destination table columns should be same
  * INSERT INTO command does not support partial success if bad records are found, it will fail.
  * Data cannot be loaded or updated in source table while insert from source table to target table is in progress.

  Examples
  ```
  INSERT INTO table1 SELECT item1, sum(item2 + 1000) as result FROM table2 group by item1
  ```

  ```
  INSERT INTO table1 SELECT item1, item2, item3 FROM table2 where item2='xyz'
  ```

  ```
  INSERT OVERWRITE table1 SELECT * FROM TABLE2
  ```

## UPDATE AND DELETE
  
### UPDATE
  
  This command will allow to update the CarbonData table based on the column expression and optional filter conditions.
    
  ```
  UPDATE <table_name> 
  SET (column_name1, column_name2, ... column_name n) = (column1_expression , column2_expression, ... column n_expression )
  [ WHERE { <filter_condition> } ]
  ```
  
  alternatively the following the command can also be used for updating the CarbonData Table :
  
  ```
  UPDATE <table_name>
  SET (column_name1, column_name2) =(select sourceColumn1, sourceColumn2 from sourceTable [ WHERE { <filter_condition> } ] )
  [ WHERE { <filter_condition> } ]
  ```
  
  NOTE:The update command fails if multiple input rows in source table are matched with single row in destination table.
  
  Examples:
  ```
  UPDATE t3 SET (t3_salary) = (t3_salary + 9) WHERE t3_name = 'aaa1'
  ```
  
  ```
  UPDATE t3 SET (t3_date, t3_country) = ('2017-11-18', 'india') WHERE t3_salary < 15003
  ```
  
  ```
  UPDATE t3 SET (t3_country, t3_name) = (SELECT t5_country, t5_name FROM t5 WHERE t5_id = 5) WHERE t3_id < 5
  ```
  
  ```
  UPDATE t3 SET (t3_date, t3_serialname, t3_salary) = (SELECT '2099-09-09', t5_serialname, '9999' FROM t5 WHERE t5_id = 5) WHERE t3_id < 5
  ```
  
  
  ```
  UPDATE t3 SET (t3_country, t3_salary) = (SELECT t5_country, t5_salary FROM t5 FULL JOIN t3 u WHERE u.t3_id = t5_id and t5_id=6) WHERE t3_id >6
  ```
    
### DELETE

  This command allows us to delete records from CarbonData table.
  ```
  DELETE FROM table_name [WHERE expression]
  ```
  
  Examples:
  
  ```
  DELETE FROM carbontable WHERE column1  = 'china'
  ```
  
  ```
  DELETE FROM carbontable WHERE column1 IN ('china', 'USA')
  ```
  
  ```
  DELETE FROM carbontable WHERE column1 IN (SELECT column11 FROM sourceTable2)
  ```
  
  ```
  DELETE FROM carbontable WHERE column1 IN (SELECT column11 FROM sourceTable2 WHERE column1 = 'USA')
  ```

## COMPACTION

  Compaction improves the query performance significantly. 
  During the load data, several CarbonData files are generated, this is because data is sorted only within each load (per load segment and one B+ tree index).
  
  There are two types of compaction, Minor and Major compaction.
  
  ```
  ALTER TABLE [db_name.]table_name COMPACT 'MINOR/MAJOR'
  ```

  - **Minor Compaction**
  
  In Minor compaction, user can specify the number of loads to be merged. 
  Minor compaction triggers for every data load if the parameter carbon.enable.auto.load.merge is set to true. 
  If any segments are available to be merged, then compaction will run parallel with data load, there are 2 levels in minor compaction:
  * Level 1: Merging of the segments which are not yet compacted.
  * Level 2: Merging of the compacted segments again to form a larger segment.
  
  ```
  ALTER TABLE table_name COMPACT 'MINOR'
  ```
  
  - **Major Compaction**
  
  In Major compaction, multiple segments can be merged into one large segment. 
  User will specify the compaction size until which segments can be merged, Major compaction is usually done during the off-peak time.
  This command merges the specified number of segments into one segment: 
     
  ```
  ALTER TABLE table_name COMPACT 'MAJOR'
  ```

  - **CLEAN SEGMENTS AFTER Compaction**
  
  Clean the segments which are compacted:
  ```
  CLEAN FILES FOR TABLE carbon_table
  ```

## STANDARD PARTITION

  The partition is same as Spark, the creation partition command as below:
  
  ```
  CREATE TABLE [IF NOT EXISTS] [db_name.]table_name
                    [(col_name data_type , ...)]
  PARTITIONED BY (partition_col_name data_type)
  STORED BY 'carbondata'
  [TBLPROPERTIES (property_name=property_value, ...)]
  ```

  Example:
  ```
  CREATE TABLE partitiontable0
                  (id Int,
                  vin String,
                  phonenumber Long,
                  area String,
                  salary Int)
                  PARTITIONED BY (country String)
                  STORED BY 'org.apache.carbondata.format'
                  TBLPROPERTIES('SORT_COLUMNS'='id,vin')
                  )
  ```


## CARBONDATA PARTITION(HASH,RANGE,LIST)

  The partition supports three type:(Hash,Range,List), similar to other system's partition features, CarbonData's partition feature can be used to improve query performance by filtering on the partition column.

### Create Hash Partition Table

  This command allows us to create hash partition.
  
  ```
  CREATE TABLE [IF NOT EXISTS] [db_name.]table_name
                    [(col_name data_type , ...)]
  PARTITIONED BY (partition_col_name data_type)
  STORED BY 'carbondata'
  [TBLPROPERTIES ('PARTITION_TYPE'='HASH',
                  'NUM_PARTITIONS'='N' ...)]
  ```
  NOTE: N is the number of hash partitions


  Example:
  ```
  CREATE TABLE IF NOT EXISTS hash_partition_table(
      col_A String,
      col_B Int,
      col_C Long,
      col_D Decimal(10,2),
      col_F Timestamp
  ) PARTITIONED BY (col_E Long)
  STORED BY 'carbondata' TBLPROPERTIES('PARTITION_TYPE'='HASH','NUM_PARTITIONS'='9')
  ```

### Create Range Partition Table

  This command allows us to create range partition.
  ```
  CREATE TABLE [IF NOT EXISTS] [db_name.]table_name
                    [(col_name data_type , ...)]
  PARTITIONED BY (partition_col_name data_type)
  STORED BY 'carbondata'
  [TBLPROPERTIES ('PARTITION_TYPE'='RANGE',
                  'RANGE_INFO'='2014-01-01, 2015-01-01, 2016-01-01, ...')]
  ```

  NOTE:
  * The 'RANGE_INFO' must be defined in ascending order in the table properties.
  * The default format for partition column of Date/Timestamp type is yyyy-MM-dd. Alternate formats for Date/Timestamp could be defined in CarbonProperties.

  Example:
  ```
  CREATE TABLE IF NOT EXISTS range_partition_table(
      col_A String,
      col_B Int,
      col_C Long,
      col_D Decimal(10,2),
      col_E Long
   ) partitioned by (col_F Timestamp)
   PARTITIONED BY 'carbondata'
   TBLPROPERTIES('PARTITION_TYPE'='RANGE',
   'RANGE_INFO'='2015-01-01, 2016-01-01, 2017-01-01, 2017-02-01')
  ```

### Create List Partition Table

  This command allows us to create list partition.
  ```
  CREATE TABLE [IF NOT EXISTS] [db_name.]table_name
                    [(col_name data_type , ...)]
  PARTITIONED BY (partition_col_name data_type)
  STORED BY 'carbondata'
  [TBLPROPERTIES ('PARTITION_TYPE'='LIST',
                  'LIST_INFO'='A, B, C, ...')]
  ```
  NOTE: List partition supports list info in one level group.

  Example:
  ```
  CREATE TABLE IF NOT EXISTS list_partition_table(
      col_B Int,
      col_C Long,
      col_D Decimal(10,2),
      col_E Long,
      col_F Timestamp
   ) PARTITIONED BY (col_A String)
   STORED BY 'carbondata'
   TBLPROPERTIES('PARTITION_TYPE'='LIST',
   'LIST_INFO'='aaaa, bbbb, (cccc, dddd), eeee')
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

  NOTE:
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

## PRE-AGGREGATE TABLES
  Carbondata supports pre aggregating of data so that OLAP kind of queries can fetch data 
  much faster.Aggregate tables are created as datamaps so that the handling is as efficient as 
  other indexing support.Users can create as many aggregate tables they require as datamaps to 
  improve their query performance,provided the storage requirements and loading speeds are 
  acceptable.
  
  For main table called **sales** which is defined as 
  
  ```
  CREATE TABLE sales (
  order_time timestamp,
  user_id string,
  sex string,
  country string,
  quantity int,
  price bigint)
  STORED BY 'carbondata'
  ```
  
  user can create pre-aggregate tables using the DDL
  
  ```
  CREATE DATAMAP agg_sales
  ON TABLE sales
  USING "preaggregate"
  AS
  SELECT country, sex, sum(quantity), avg(price)
  FROM sales
  GROUP BY country, sex
  ```
  
<b><p align="left">Functions supported in pre-aggregate tables</p></b>

| Function | Rollup supported |
|-----------|----------------|
| SUM | Yes |
| AVG | Yes |
| MAX | Yes |
| MIN | Yes |
| COUNT | Yes |


##### How pre-aggregate tables are selected
For the main table **sales** and pre-aggregate table **agg_sales** created above, queries of the 
kind
```
SELECT country, sex, sum(quantity), avg(price) from sales GROUP BY country, sex

SELECT sex, sum(quantity) from sales GROUP BY sex

SELECT sum(price), country from sales GROUP BY country
``` 

will be transformed by Query Planner to fetch data from pre-aggregate table **agg_sales**

But queries of kind
```
SELECT user_id, country, sex, sum(quantity), avg(price) from sales GROUP BY country, sex

SELECT sex, avg(quantity) from sales GROUP BY sex

SELECT max(price), country from sales GROUP BY country
```

will fetch the data from the main table **sales**

##### Loading data to pre-aggregate tables
For existing table with loaded data, data load to pre-aggregate table will be triggered by the 
CREATE DATAMAP statement when user creates the pre-aggregate table.
For incremental loads after aggregates tables are created, loading data to main table triggers 
the load to pre-aggregate tables once main table loading is complete.These loads are automic 
meaning that data on main table and aggregate tables are only visible to the user after all tables 
are loaded

##### Querying data from pre-aggregate tables
Pre-aggregate tables cannot be queries directly.Queries are to be made on main table.Internally 
carbondata will check associated pre-aggregate tables with the main table and if the 
pre-aggregate tables satisfy the query condition, the plan is transformed automatically to use 
pre-aggregate table to fetch the data

##### Compacting pre-aggregate tables
Compaction is an optional operation for pre-aggregate table. If compaction is performed on main 
table but not performed on pre-aggregate table, all queries still can benefit from pre-aggregate 
table.To further improve performance on pre-aggregate table, compaction can be triggered on 
pre-aggregate tables directly, it will merge the segments inside pre-aggregation table. 
To do that, use ALTER TABLE COMPACT command on the pre-aggregate table just like the main table

  NOTE:
  * If the aggregate function used in the pre-aggregate table creation included distinct-count,
     during compaction, the pre-aggregate table values are recomputed.This would a costly 
     operation as compared to the compaction of pre-aggregate tables containing other aggregate 
     functions alone
 
##### Update/Delete Operations on pre-aggregate tables
This functionality is not supported.

  NOTE (<b>RESTRICTION</b>):
  * Update/Delete operations are <b>not supported</b> on main table which has pre-aggregate tables 
  created on it.All the pre-aggregate tables <b>will have to be dropped</b> before update/delete 
  operations can be performed on the main table.Pre-aggregate tables can be rebuilt manually 
  after update/delete operations are completed
 
##### Delete Segment Operations on pre-aggregate tables
This functionality is not supported.

  NOTE (<b>RESTRICTION</b>):
  * Delete Segment operations are <b>not supported</b> on main table which has pre-aggregate tables 
  created on it.All the pre-aggregate tables <b>will have to be dropped</b> before update/delete 
  operations can be performed on the main table.Pre-aggregate tables can be rebuilt manually 
  after delete segment operations are completed
  
##### Alter Table Operations on pre-aggregate tables
This functionality is not supported.

  NOTE (<b>RESTRICTION</b>):
  * Adding new column in new table does not have any affect on pre-aggregate tables. However if 
  dropping or renaming a column has impact in pre-aggregate table, such operations will be 
  rejected and error will be thrown.All the pre-aggregate tables <b>will have to be dropped</b> 
  before Alter Operations can be performed on the main table.Pre-aggregate tables can be rebuilt 
  manually after Alter Table operations are completed
  
### Supporting timeseries data
Carbondata has built-in understanding of time hierarchy and levels: year, month, day, hour, minute.
Multiple pre-aggregate tables can be created for the hierarchy and Carbondata can do automatic 
roll-up for the queries on these hierarchies.

  ```
  CREATE DATAMAP agg_year
  ON TABLE sales
  USING "timeseries"
  DMPROPERTIES (
  'event_time’=’order_time’,
  'year_granualrity’=’1’,
  ) AS
  SELECT order_time, country, sex, sum(quantity), max(quantity), count(user_id), sum(price),
   avg(price) FROM sales GROUP BY order_time, country, sex
    
  CREATE DATAMAP agg_month
  ON TABLE sales
  USING "timeseries"
  DMPROPERTIES (
  'event_time’=’order_time’,
  'month_granualrity’=’1’,
  ) AS
  SELECT order_time, country, sex, sum(quantity), max(quantity), count(user_id), sum(price),
   avg(price) FROM sales GROUP BY order_time, country, sex
    
  CREATE DATAMAP agg_day
  ON TABLE sales
  USING "timeseries"
  DMPROPERTIES (
  'event_time’=’order_time’,
  'day_granualrity’=’1’,
  ) AS
  SELECT order_time, country, sex, sum(quantity), max(quantity), count(user_id), sum(price),
   avg(price) FROM sales GROUP BY order_time, country, sex
        
  CREATE DATAMAP agg_sales_hour
  ON TABLE sales
  USING "timeseries"
  DMPROPERTIES (
  'event_time’=’order_time’,
  'hour_granualrity’=’1’,
  ) AS
  SELECT order_time, country, sex, sum(quantity), max(quantity), count(user_id), sum(price),
   avg(price) FROM sales GROUP BY order_time, country, sex
  
  CREATE DATAMAP agg_minute
  ON TABLE sales
  USING "timeseries"
  DMPROPERTIES (
  'event_time’=’order_time’,
  'minute_granualrity’=’1’,
  ) AS
  SELECT order_time, country, sex, sum(quantity), max(quantity), count(user_id), sum(price),
   avg(price) FROM sales GROUP BY order_time, country, sex
    
  CREATE DATAMAP agg_minute
  ON TABLE sales
  USING "timeseries"
  DMPROPERTIES (
  'event_time’=’order_time’,
  'minute_granualrity’=’1’,
  ) AS
  SELECT order_time, country, sex, sum(quantity), max(quantity), count(user_id), sum(price),
   avg(price) FROM sales GROUP BY order_time, country, sex
  ```
  
  For Querying data and automatically roll-up to the desired aggregation level,Carbondata supports 
  UDF as
  ```
  timeseries(timeseries column name, ‘aggregation level’)
  ```
  ```
  Select timeseries(order_time, ‘hour’), sum(quantity) from sales group by timeseries(order_time,
  ’hour’)
  ```
  
  It is **not necessary** to create pre-aggregate tables for each granularity unless required for 
  query
  .Carbondata
   can roll-up the data and fetch it
   
  For Example: For main table **sales** , If pre-aggregate tables were created as  
  
  ```
  CREATE DATAMAP agg_day
    ON TABLE sales
    USING "timeseries"
    DMPROPERTIES (
    'event_time’=’order_time’,
    'day_granualrity’=’1’,
    ) AS
    SELECT order_time, country, sex, sum(quantity), max(quantity), count(user_id), sum(price),
     avg(price) FROM sales GROUP BY order_time, country, sex
          
    CREATE DATAMAP agg_sales_hour
    ON TABLE sales
    USING "timeseries"
    DMPROPERTIES (
    'event_time’=’order_time’,
    'hour_granualrity’=’1’,
    ) AS
    SELECT order_time, country, sex, sum(quantity), max(quantity), count(user_id), sum(price),
     avg(price) FROM sales GROUP BY order_time, country, sex
  ```
  
  Queries like below will be rolled-up and fetched from pre-aggregate tables
  ```
  Select timeseries(order_time, ‘month’), sum(quantity) from sales group by timeseries(order_time,
    ’month’)
    
  Select timeseries(order_time, ‘year’), sum(quantity) from sales group by timeseries(order_time,
    ’year’)
  ```
  
  NOTE (<b>RESTRICTION</b>):
  * Only value of 1 is supported for hierarchy levels. Other hierarchy levels are not supported. 
  Other hierarchy levels are not supported
  * pre-aggregate tables for the desired levels needs to be created one after the other
  * pre-aggregate tables created for each level needs to be dropped separately 
    

## BUCKETING

  Bucketing feature can be used to distribute/organize the table/partition data into multiple files such
  that similar records are present in the same file. While creating a table, user needs to specify the
  columns to be used for bucketing and the number of buckets. For the selection of bucket the Hash value
  of columns is used.

  ```
  CREATE TABLE [IF NOT EXISTS] [db_name.]table_name
                    [(col_name data_type, ...)]
  STORED BY 'carbondata'
  TBLPROPERTIES('BUCKETNUMBER'='noOfBuckets',
  'BUCKETCOLUMNS'='columnname')
  ```

  NOTE:
  * Bucketing can not be performed for columns of Complex Data Types.
  * Columns in the BUCKETCOLUMN parameter must be only dimension. The BUCKETCOLUMN parameter can not be a measure or a combination of measures and dimensions.

  Example:
  ```
  CREATE TABLE IF NOT EXISTS productSchema.productSalesTable (
                                productNumber Int,
                                saleQuantity Int,
                                productName String,
                                storeCity String,
                                storeProvince String,
                                productCategory String,
                                productBatch String,
                                revenue Int)
  STORED BY 'carbondata'
  TBLPROPERTIES ('BUCKETNUMBER'='4', 'BUCKETCOLUMNS'='productName')
  ```
  
## SEGMENT MANAGEMENT  

### SHOW SEGMENT

  This command is used to get the segments of CarbonData table.

  ```
  SHOW SEGMENTS FOR TABLE [db_name.]table_name LIMIT number_of_segments
  ```
  
  Example:
  ```
  SHOW SEGMENTS FOR TABLE CarbonDatabase.CarbonTable LIMIT 4
  ```

### DELETE SEGMENT BY ID

  This command is used to delete segment by using the segment ID. Each segment has a unique segment ID associated with it. 
  Using this segment ID, you can remove the segment.

  The following command will get the segmentID.

  ```
  SHOW SEGMENTS FOR TABLE [db_name.]table_name LIMIT number_of_segments
  ```

  After you retrieve the segment ID of the segment that you want to delete, execute the following command to delete the selected segment.

  ```
  DELETE FROM TABLE [db_name.]table_name WHERE SEGMENT.ID IN (segment_id1, segments_id2, ...)
  ```

  Example:

  ```
  DELETE FROM TABLE CarbonDatabase.CarbonTable WHERE SEGMENT.ID IN (0)
  DELETE FROM TABLE CarbonDatabase.CarbonTable WHERE SEGMENT.ID IN (0,5,8)
  ```

### DELETE SEGMENT BY DATE

  This command will allow to delete the CarbonData segment(s) from the store based on the date provided by the user in the DML command. 
  The segment created before the particular date will be removed from the specific stores.

  ```
  DELETE FROM TABLE [db_name.]table_name WHERE SEGMENT.STARTTIME BEFORE DATE_VALUE
  ```

  Example:
  ```
  DELETE FROM TABLE CarbonDatabase.CarbonTable WHERE SEGMENT.STARTTIME BEFORE '2017-06-01 12:05:06' 
  ```

### QUERY DATA WITH SPECIFIED SEGMENTS

  This command is used to read data from specified segments during CarbonScan.
  
  Get the Segment ID:
  ```
  SHOW SEGMENTS FOR TABLE [db_name.]table_name LIMIT number_of_segments
  ```
  
  Set the segment IDs for table
  ```
  SET carbon.input.segments.<database_name>.<table_name> = <list of segment IDs>
  ```
  
  NOTE:
  carbon.input.segments: Specifies the segment IDs to be queried. This property allows you to query specified segments of the specified table. The CarbonScan will read data from specified segments only.
  
  If user wants to query with segments reading in multi threading mode, then CarbonSession.threadSet can be used instead of SET query.
  ```
  CarbonSession.threadSet ("carbon.input.segments.<database_name>.<table_name>","<list of segment IDs>");
  ```
  
  Reset the segment IDs
  ```
  SET carbon.input.segments.<database_name>.<table_name> = *;
  ```
  
  If user wants to query with segments reading in multi threading mode, then CarbonSession.threadSet can be used instead of SET query. 
  ```
  CarbonSession.threadSet ("carbon.input.segments.<database_name>.<table_name>","*");
  ```
  
  **Examples:**
  
  * Example to show the list of segment IDs,segment status, and other required details and then specify the list of segments to be read.
  
  ```
  SHOW SEGMENTS FOR carbontable1;
  
  SET carbon.input.segments.db.carbontable1 = 1,3,9;
  ```
  
  * Example to query with segments reading in multi threading mode:
  
  ```
  CarbonSession.threadSet ("carbon.input.segments.db.carbontable_Multi_Thread","1,3");
  ```
  
  * Example for threadset in multithread environment (following shows how it is used in Scala code):
  
  ```
  def main(args: Array[String]) {
  Future {          
    CarbonSession.threadSet ("carbon.input.segments.db.carbontable_Multi_Thread","1")
    spark.sql("select count(empno) from carbon.input.segments.db.carbontable_Multi_Thread").show();
     }
   }
  ```
