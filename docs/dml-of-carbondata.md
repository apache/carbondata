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

# CarbonData Data Manipulation Language

CarbonData DML statements are documented here,which includes:

* [LOAD DATA](#load-data)
* [INSERT DATA](#insert-data-into-carbondata-table)
* [INSERT DATA STAGE](#insert-data-into-carbondata-table-from-stage-input-files)
* [Load Data Using Static Partition](#load-data-using-static-partition)
* [Load Data Using Dynamic Partition](#load-data-using-dynamic-partition)
* [UPDATE AND DELETE](#update-and-delete)
* [COMPACTION](#compaction)
* [SEGMENT MANAGEMENT](./segment-management-on-carbondata.md)


## LOAD DATA

### LOAD FILES TO CARBONDATA TABLE

  This command is used to load csv files to carbondata, OPTIONS are not mandatory for data loading process. 

  ```
  LOAD DATA INPATH 'folder_path'
  INTO TABLE [db_name.]table_name 
  OPTIONS(property_name=property_value, ...)
  ```
  **NOTE**:
    * Use 'file://' prefix to indicate local input files path, but it just supports local mode.
    * If run on cluster mode, please upload all input files to distributed file system, for example 'hdfs://' for hdfs.

  **Supported Properties:**

| Property                                                | Description                                                  |
| ------------------------------------------------------- | ------------------------------------------------------------ |
| [DELIMITER](#delimiter)                                 | Character used to separate the data in the input csv file    |
| [QUOTECHAR](#quotechar)                                 | Character used to quote the data in the input csv file       |
| [LINE_SEPARATOR](#line_separator)                       | Characters used to specify the line separator in the input csv file. If not provided, csv parser will detect it automatically. | 
| [COMMENTCHAR](#commentchar)                             | Character used to comment the rows in the input csv file. Those rows will be skipped from processing |
| [HEADER](#header)                                       | Whether the input csv files have header row                  |
| [FILEHEADER](#fileheader)                               | If header is not present in the input csv, what is the column names to be used for data read from input csv |
| [SORT_SCOPE](#sort_scope)                               | Sort Scope to be used for current load.                      |
| [MULTILINE](#multiline)                                 | Whether a row data can span across multiple lines.           |
| [ESCAPECHAR](#escapechar)                               | Escape character used to excape the data in input csv file.For eg.,\ is a standard escape character |
| [SKIP_EMPTY_LINE](#skip_empty_line)                     | Whether empty lines in input csv file should be skipped or loaded as null row |
| [COMPLEX_DELIMITER_LEVEL_1](#complex_delimiter_level_1) | Starting delimiter for complex type data in input csv file   |
| [COMPLEX_DELIMITER_LEVEL_2](#complex_delimiter_level_2) | Ending delimiter for complex type data in input csv file     |
| [COMPLEX_DELIMITER_LEVEL_3](#complex_delimiter_level_3) | Ending delimiter for nested complex type data in input csv file of level 3. |
| [DATEFORMAT](#dateformattimestampformat)                | Format of date in the input csv file                         |
| [TIMESTAMPFORMAT](#dateformattimestampformat)           | Format of timestamp in the input csv file                    |
| [SORT_COLUMN_BOUNDS](#sort-column-bounds)               | How to partition the sort columns to make the evenly distributed |
| [BAD_RECORDS_LOGGER_ENABLE](#bad-records-handling)      | Whether to enable bad records logging                        |
| [BAD_RECORD_PATH](#bad-records-handling)                | Bad records logging path. Useful when bad record logging is enabled |
| [BAD_RECORDS_ACTION](#bad-records-handling)             | Behavior of data loading when bad record is found            |
| [IS_EMPTY_DATA_BAD_RECORD](#bad-records-handling)       | Whether empty data of a column to be considered as bad record or not |
| [GLOBAL_SORT_PARTITIONS](#global_sort_partitions)       | Number of partition to use for shuffling of data during sorting |
| [SCALE_FACTOR](#scale_factor)                           | Control the partition size for RANGE_COLUMN feature          |
-
  You can use the following options to load data:

  - ##### DELIMITER: 
    Delimiters can be provided in the load command.

    ``` 
    OPTIONS('DELIMITER'=',')
    ```

  - ##### QUOTECHAR:
    Quote Characters can be provided in the load command.

    ```
    OPTIONS('QUOTECHAR'='"')
    ```

  - ##### LINE_SEPARATOR:
    Line separator Characters can be provided in the load command.

    ```
    OPTIONS('LINE_SEPARATOR'='\n')
    ```

  - ##### COMMENTCHAR:
    Comment Characters can be provided in the load command if user wants to comment lines.
    ```
    OPTIONS('COMMENTCHAR'='#')
    ```

  - ##### HEADER:
    When you load the CSV file without the file header and the file header is the same with the table schema, then add 'HEADER'='false' to load data SQL as user need not provide the file header. By default the value is 'true'.
    false: CSV file is without file header.
    true: CSV file is with file header.

    ```
    OPTIONS('HEADER'='false') 
    ```

    **NOTE:** If the HEADER option exist and is set to 'true', then the FILEHEADER option is not required.

  - ##### FILEHEADER:
    Headers can be provided in the LOAD DATA command if headers are missing in the source files.

    ```
    OPTIONS('FILEHEADER'='column1,column2') 
    ```

  - ##### SORT_SCOPE:
    Sort Scope to be used for the current load. This overrides the Sort Scope of Table.
    Requirement: Sort Columns must be set while creating table. If Sort Columns is null, Sort Scope is always NO_SORT.
  
    ```
    OPTIONS('SORT_SCOPE'='GLOBAL_SORT')
    ```
    
    Priority order for choosing Sort Scope is:
    * Load Data Command
    * ```CARBON.TABLE.LOAD.SORT.SCOPE.<db>.<table>``` session property.
    * Table level Sort Scope
    * ```CARBON.OPTIONS.SORT.SCOPE``` session property
    * Default Value: NO_SORT
    
  - ##### MULTILINE:

    CSV with new line character in quotes.

    ```
    OPTIONS('MULTILINE'='true') 
    ```

  - ##### ESCAPECHAR: 

    Escape char can be provided if user want strict validation of escape character in CSV files.

    ```
    OPTIONS('ESCAPECHAR'='\') 
    ```

  - ##### SKIP_EMPTY_LINE:

    This option will ignore the empty line in the CSV file during the data load.

    ```
    OPTIONS('SKIP_EMPTY_LINE'='TRUE/FALSE') 
    ```

  - ##### COMPLEX_DELIMITER_LEVEL_1:

    Split the complex type data column in a row (eg., a\001b\001c --> Array = {a,b,c}).

    ```
    OPTIONS('COMPLEX_DELIMITER_LEVEL_1'='\001')
    ```

  - ##### COMPLEX_DELIMITER_LEVEL_2:

    Split the complex type nested data column in a row. Applies level_1 delimiter & applies level_2 based on complex data type (eg., a\002b\001c\002d --> Array> = {{a,b},{c,d}}).

    ```
    OPTIONS('COMPLEX_DELIMITER_LEVEL_2'='\002')
    ```

  - ##### COMPLEX_DELIMITER_LEVEL_3:

    Split the complex type nested data column in a row. Applies level_1 delimiter, applies level_2 and then level_3 delimiter based on complex data type.
     Used in case of nested Complex Map type. (eg., 'a\003b\002b\003c\001aa\003bb\002cc\003dd' --> Array Of Map> = {{a -> b, b -> c},{aa -> bb, cc -> dd}}).

    ```
    OPTIONS('COMPLEX_DELIMITER_LEVEL_3'='\003')
    ```

  - ##### DATEFORMAT/TIMESTAMPFORMAT:

    Date and Timestamp format for specified column.

    ```
    OPTIONS('DATEFORMAT' = 'yyyy-MM-dd','TIMESTAMPFORMAT'='yyyy-MM-dd HH:mm:ss')
    ```
    **NOTE:** Date formats are specified by date pattern strings. The date pattern in CarbonData is the same as in JAVA. Refer to [SimpleDateFormat](http://docs.oracle.com/javase/7/docs/api/java/text/SimpleDateFormat.html).

  - ##### SORT COLUMN BOUNDS:

    Range bounds for sort columns.

    Suppose the table is created with 'SORT_COLUMNS'='name,id' and the range for name is aaa to zzz, the value range for id is 0 to 1000. Then during data loading, we can specify the following option to enhance data loading performance.
    ```
    OPTIONS('SORT_COLUMN_BOUNDS'='f,250;l,500;r,750')
    ```
    Each bound is separated by ';' and each field value in bound is separated by ','. In the example above, we provide 3 bounds to distribute records to 4 partitions. The values 'f','l','r' can evenly distribute the records. Inside carbondata, for a record we compare the value of sort columns with that of the bounds and decide which partition the record will be forwarded to.

    **NOTE:**
    * SORT_COLUMN_BOUNDS will be used only when the SORT_SCOPE is 'local_sort'.
    * Carbondata will use these bounds as ranges to process data concurrently during the final sort procedure. The records will be sorted and written out inside each partition. Since the partition is sorted, all records will be sorted.
    * The option works better if your CPU usage during loading is low. If your current system CPU usage is high, better not to use this option. Besides, it depends on the user to specify the bounds. If user does not know the exactly bounds to make the data distributed evenly among the bounds, loading performance will still be better than before or at least the same as before.
    * Users can find more information about this option in the description of [PR1953](https://github.com/apache/carbondata/pull/1953).

  - ##### BAD RECORDS HANDLING:

    Methods of handling bad records are as follows:

    * Load all of the data before dealing with the errors.
    * Clean or delete bad records before loading data or stop the loading when bad records are found.

    ```
    OPTIONS('BAD_RECORDS_LOGGER_ENABLE'='true', 'BAD_RECORD_PATH'='hdfs://hacluster/tmp/carbon', 'BAD_RECORDS_ACTION'='REDIRECT', 'IS_EMPTY_DATA_BAD_RECORD'='false')
    ```

    **NOTE:**
    * BAD_RECORDS_ACTION property can have four types of actions for bad records FORCE, REDIRECT, IGNORE, and FAIL.
    * FAIL option is its Default value. If the FAIL option is used, then data loading fails if any bad records are found.
    * If the REDIRECT option is used, CarbonData will add all bad records into a separate CSV file. However, this file must not be used for subsequent data loading because the content may not exactly match the source record. You are advised to cleanse the source record for further data ingestion. This option is used to remind you which records are bad.
    * If the FORCE option is used, then it auto-converts the data by storing the bad records as NULL before Loading data.
    * If the IGNORE option is used, then bad records are neither loaded nor written to the separate CSV file.
    * In loaded data, if all records are bad records, the BAD_RECORDS_ACTION is invalid and the load operation fails.
    * The default maximum number of characters per column is 32000. If there are more than 32000 characters in a column, please refer to [String longer than 32000 characters](https://github.com/apache/carbondata/blob/master/docs/ddl-of-carbondata.md#string-longer-than-32000-characters) section.
    * Since Bad Records Path can be specified in create, load and carbon properties. 
      Therefore, the value specified in load will have the highest priority, and value specified in carbon properties will have the least priority.

    Example:

    ```
    LOAD DATA INPATH 'filepath.csv' INTO TABLE tablename
    OPTIONS('BAD_RECORDS_LOGGER_ENABLE'='true','BAD_RECORD_PATH'='hdfs://hacluster/tmp/carbon',
    'BAD_RECORDS_ACTION'='REDIRECT','IS_EMPTY_DATA_BAD_RECORD'='false')
    ```

  - ##### GLOBAL_SORT_PARTITIONS:

    If the SORT_SCOPE is defined as GLOBAL_SORT, then the user can specify the number of partitions to use while shuffling data for sort using GLOBAL_SORT_PARTITIONS. If it is not configured, or configured less than 1, then it uses the number of map tasks as reduce tasks. It is recommended that each reduce task deals with 512MB-1GB data.
    For RANGE_COLUMN, GLOBAL_SORT_PARTITIONS is used to specify the number of range partitions also.
    GLOBAL_SORT_PARTITIONS should be specified optimally during RANGE_COLUMN LOAD because if a higher number is configured then the load time may be less but it will result in the creation of more files which would degrade the query and compaction performance.
    Conversely, if fewer partitions are configured then the load performance may degrade due to less use of parallelism but the query and compaction will become faster. Hence the user may choose an optimal number depending on the use case.
    ```
    OPTIONS('GLOBAL_SORT_PARTITIONS'='2')
    ```

     **NOTE:**
     * GLOBAL_SORT_PARTITIONS should be Integer type, the range is [1,Integer.MaxValue].
     * It is only used when the SORT_SCOPE is GLOBAL_SORT.

  - ##### SCALE_FACTOR

    For RANGE_COLUMN, SCALE_FACTOR is used to control the number of range partitions as following.
    ```
      splitSize = max(blocklet_size, (block_size - blocklet_size)) * scale_factor
      numPartitions = total size of input data / splitSize
    ```
    The default value is 3, and the range is [1, 300].
 
    ```
      OPTIONS('SCALE_FACTOR'='10')
    ```
    **NOTE:**
    * If both GLOBAL_SORT_PARTITIONS and SCALE_FACTOR are used at the same time, only GLOBAL_SORT_PARTITIONS is valid.
    * The compaction on RANGE_COLUMN will use LOCAL_SORT by default.


## INSERT DATA INTO CARBONDATA TABLE

  This command inserts data into a CarbonData table, it is defined as a combination of two queries Insert and Select query respectively. 
  It inserts records from a source table into a target CarbonData table, the source table can be a Hive table, Parquet table or a CarbonData table itself. 
  It comes with the functionality to aggregate the records of a table by performing Select query on source table and load its corresponding resultant records into a CarbonData table.

  ```
  INSERT INTO TABLE <CARBONDATA TABLE> SELECT * FROM sourceTableName 
  [ WHERE { <filter_condition> } ]
  ```

  User can also omit the `table` keyword and write the query as:

  ```
  INSERT INTO <CARBONDATA TABLE> SELECT * FROM sourceTableName 
  [ WHERE { <filter_condition> } ]
  ```

  Overwrite insert data:
  ```
  INSERT OVERWRITE TABLE <CARBONDATA TABLE> SELECT * FROM sourceTableName 
  [ WHERE { <filter_condition> } ]
  ```

  **NOTE:**
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
  INSERT OVERWRITE TABLE table1 SELECT * FROM TABLE2
  ```

## INSERT DATA INTO CARBONDATA TABLE From Stage Input Files

  Stage input files are data files written by external application (such as Flink). These files 
  are committed but not loaded into the table. 
  
  User can use this command to insert them into the table, thus making them visible for a query.
  
  ```
  INSERT INTO <CARBONDATA TABLE> STAGE OPTIONS(property_name=property_value, ...)
  ```
  **Supported Properties:**

| Property                                                | Description                                                  |
| ------------------------------------------------------- | ------------------------------------------------------------ |
| [BATCH_FILE_COUNT](#batch_file_count)                   | The number of stage files per processing                     |
| [BATCH_FILE_ORDER](#batch_file_order)                   | The order type of stage files in per processing                     |

-
  User can use the following options to load data:

  - ##### BATCH_FILE_COUNT: 
    The number of stage files per processing.

    ``` 
    OPTIONS('batch_file_count'='5')
    ```

  - ##### BATCH_FILE_ORDER: 
    The order type of stage files in per processing, choices: ASC, DESC.
    The default is ASC.
    Stage files will order by the last modified time with the specified order type.

    ``` 
    OPTIONS('batch_file_order'='DESC')
    ```

    Examples:
    ```
    INSERT INTO table1 STAGE
  
    INSERT INTO table1 STAGE OPTIONS('batch_file_count' = '5')
    Note: This command uses the default file order, will insert the earliest stage files into the table.
  
    INSERT INTO table1 STAGE OPTIONS('batch_file_count' = '5', 'batch_file_order'='DESC')
    Note: This command will insert the latest stage files into the table.
    ```

## Load Data Using Static Partition 

  This command allows you to load data using static partition.

  ```
  LOAD DATA INPATH 'folder_path'
  INTO TABLE [db_name.]table_name PARTITION (partition_spec) 
  OPTIONS(property_name=property_value, ...)

  INSERT INTO TABLE [db_name.]table_name PARTITION (partition_spec) <SELECT STATEMENT>
  ```

  Example:
  ```
  LOAD DATA INPATH '${env:HOME}/staticinput.csv'
  INTO TABLE locationTable
  PARTITION (country = 'US', state = 'CA')

  INSERT INTO TABLE locationTable
  PARTITION (country = 'US', state = 'AL')
  SELECT <columns list excluding partition columns> FROM another_user
  ```

## Load Data Using Dynamic Partition

  This command allows you to load data using dynamic partition. If partition spec is not specified, then the partition is considered as dynamic.

  Example:
  ```
  LOAD DATA INPATH '${env:HOME}/staticinput.csv'
  INTO TABLE locationTable

  INSERT INTO TABLE locationTable
  SELECT <columns list excluding partition columns> FROM another_user
  ```

## UPDATE AND DELETE

### UPDATE

  This command will allow to update the CarbonData table based on the column expression and optional filter conditions.
    
  ```
  UPDATE <table_name> 
  SET (column_name1, column_name2, ... column_name n) = (column1_expression , column2_expression, ... column n_expression )
  [ WHERE { <filter_condition> } ]
  ```

  alternatively the following command can also be used for updating the CarbonData Table :

  ```
  UPDATE <table_name>
  SET (column_name1, column_name2) =(select sourceColumn1, sourceColumn2 from sourceTable [ WHERE { <filter_condition> } ] )
  [ WHERE { <filter_condition> } ]
  ```

  **NOTE:** The update command fails if multiple input rows in source table are matched with single row in destination table.

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
   NOTE: Update Complex datatype columns is not supported.
    
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
    
### DELETE STAGE

  This command allows us to delete the data files (stage data) which is already loaded into the table.
  ```
  DELETE FROM TABLE [db_name.]table_name STAGE OPTIONS(property_name=property_value, ...)
  ```  
  **Supported Properties:**

| Property                                                | Description                                                 |
| ------------------------------------------------------- | ----------------------------------------------------------- |
| [retain_hour](#retain_hour)                             | Data file retain time in hours                              |

-
  You can use the following options to delete data:
  - ##### retain_hour: 
    Data file retain time in second, the command just delete overdue files only.

    ``` 
    OPTIONS('retain_hour'='1')
    ```

  Examples:

  ```
  DELETE FROM TABLE carbontable STAGE
  ```

  ```
  DELETE FROM TABLE carbontable STAGE OPTIONS ('retain_hour'='1')
  ```

## COMPACTION

  Compaction improves the query performance significantly. 

  There are several types of compaction.

  ```
  ALTER TABLE [db_name.]table_name COMPACT 'MINOR/MAJOR/CUSTOM'
  ```

  - **Minor Compaction**

  In Minor compaction, the user can specify the number of loads to be merged. 
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
  Configure the property carbon.major.compaction.size with appropriate value in MB.

  This command merges the specified number of segments into one segment: 
     
  ```
  ALTER TABLE table_name COMPACT 'MAJOR'
  ```

  - **Custom Compaction**

  In Custom compaction, user can directly specify segment ids to be merged into one large segment. 
  All specified segment ids should exist and be valid, otherwise compaction will fail. 
  Custom compaction is usually done during the off-peak time. 

  ```
  ALTER TABLE table_name COMPACT 'CUSTOM' WHERE SEGMENT.ID IN (2,3,4)
  ```


  - **CLEAN SEGMENTS AFTER Compaction**

  Clean the segments which are compacted:
  ```
  CLEAN FILES FOR TABLE carbon_table
  ```
