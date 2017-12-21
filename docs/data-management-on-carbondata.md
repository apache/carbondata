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
* [TABLE MANAGEMENT](#table-management)
* [LOAD DATA](#load-data)
* [UPDATE AND DELETE](#update-and-delete)
* [COMPACTION](#compaction)
* [PARTITION](#partition)
* [BUCKETING](#bucketing)
* [SEGMENT MANAGEMENT](#segment-management)

## CREATE TABLE

  This command can be used to create a CarbonData table by specifying the list of fields along with the table properties.
  
  ```
  CREATE TABLE [IF NOT EXISTS] [db_name.]table_name[(col_name data_type , ...)]
  STORED BY 'carbondata'
  [TBLPROPERTIES (property_name=property_value, ...)]
  ```  
  
### Usage Guidelines

  Following are the guidelines for TBLPROPERTIES, CarbonData's additional table options can be set via carbon.properties.
  
   - **Dictionary Encoding Configuration**

     Dictionary encoding is turned off for all columns by default from 1.3 onwards, you can use this command for including columns to do dictionary encoding.
     Suggested use cases : do dictionary encoding for low cardinality columns, it might help to improve data compression ratio and performance.

     ```
     TBLPROPERTIES ('DICTIONARY_INCLUDE'='column1, column2')
     ```
     
   - **Inverted Index Configuration**

     By default inverted index is enabled, it might help to improve compression ratio and query speed, especially for low cardinality columns which are in reward position.
     Suggested use cases : For high cardinality columns, you can disable the inverted index for improving the data loading performance.

     ```
     TBLPROPERTIES ('NO_INVERTED_INDEX'='column1, column3')
     ```

   - **Sort Columns Configuration**

     This property is for users to specify which columns belong to the MDK(Multi-Dimensions-Key) index.
     * If users don't specify "SORT_COLUMN" property, by default MDK index be built by using all dimension columns except complex datatype column. 
     * If this property is specified but with empty argument, then the table will be loaded without sort..
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
                   'ALLOWED_COMPACTION_DAYS'='5')
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

### DROP TABLE
  
  This command is used to delete an existing table.
  ```
  DROP TABLE [IF EXISTS] [db_name.]table_name
  ```

  Example:
  ```
  DROP TABLE IF EXISTS productSchema.productSalesTable
  ```
  
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
   * If this option is set to TRUE, then high.cardinality.identify.enable property will be disabled during data load.

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
  * BAD_RECORD_ACTION property can have four type of actions for bad records FORCE, REDIRECT, IGNORE and FAIL.
  * If the REDIRECT option is used, CarbonData will add all bad records in to a separate CSV file. However, this file must not be used for subsequent data loading because the content may not exactly match the source record. You are advised to cleanse the original source record for further data ingestion. This option is used to remind you which records are bad records.
  * If the FORCE option is used, then it auto-corrects the data by storing the bad records as NULL before Loading data.
  * If the IGNORE option is used, then bad records are neither loaded nor written to the separate CSV file.
  * IF the FAIL option is used, then data loading fails if any bad records are found.
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

## PARTITION

  Similar to other system's partition features, CarbonData's partition feature also can be used to improve query performance by filtering on the partition column.

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