<!--
    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.
-->

# DML Operations on CarbonData
This tutorial guides you through the data manipulation language support provided by CarbonData.

## Overview 
The following DML operations are supported in CarbonData :

* [LOAD DATA](#load-data)
* [INSERT DATA INTO A CARBONDATA TABLE](#insert-data-into-a-carbondata-table)
* [SHOW SEGMENTS](#show-segments)
* [DELETE SEGMENT BY ID](#delete-segment-by-id)
* [DELETE SEGMENT BY DATE](#delete-segment-by-date)
* [UPDATE CARBONDATA TABLE](#update-carbondata-table)
* [DELETE RECORDS FROM CARBONDATA TABLE](#delete-records-from-carbondata-table)

## LOAD DATA

This command loads the user data in raw format to the CarbonData specific data format store, this allows CarbonData to provide good performance while querying the data.
Please visit [Data Management](data-management.md) for more details on LOAD.

### Syntax

```
LOAD DATA [LOCAL] INPATH 'folder_path' 
INTO TABLE [db_name.]table_name 
OPTIONS(property_name=property_value, ...)
```

OPTIONS are not mandatory for data loading process. Inside OPTIONS user can provide either of any options like DELIMITER, QUOTECHAR, ESCAPECHAR, MULTILINE as per requirement.

NOTE: The path shall be canonical path.

### Parameter Description

| Parameter     | Description                                                          | Optional |
| ------------- | ---------------------------------------------------------------------| -------- |
| folder_path   | Path of raw csv data folder or file.                                 | NO       |
| db_name       | Database name, if it is not specified then it uses the current database. | YES      |
| table_name    | The name of the table in provided database.                          | NO       |
| OPTIONS       | Extra options provided to Load                                       | YES      |
 

### Usage Guidelines

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
    OPTIONS('COLUMNDICT'='column1:dictionaryFilePath1,
    column2:dictionaryFilePath2')
    ```

    NOTE: ALL_DICTIONARY_PATH and COLUMNDICT can't be used together.
    
- **DATEFORMAT:** Date format for specified column.

    ```
    OPTIONS('DATEFORMAT'='column1:dateFormat1, column2:dateFormat2')
    ```

    NOTE: Date formats are specified by date pattern strings. The date pattern letters in CarbonData are same as in JAVA. Refer to [SimpleDateFormat](http://docs.oracle.com/javase/7/docs/api/java/text/SimpleDateFormat.html).

- **SINGLE_PASS:** Single Pass Loading enables single job to finish data loading with dictionary generation on the fly. It enhances performance in the scenarios where the subsequent data loading after initial load involves fewer incremental updates on the dictionary.

   This option specifies whether to use single pass for loading data or not. By default this option is set to FALSE.

    ```
    OPTIONS('SINGLE_PASS'='TRUE')
    ```

   Note :

   * If this option is set to TRUE then data loading will take less time.

   * If this option is set to some invalid value other than TRUE or FALSE then it uses the default value.
### Example:

```
LOAD DATA local inpath '/opt/rawdata/data.csv' INTO table carbontable
options('DELIMITER'=',', 'QUOTECHAR'='"','COMMENTCHAR'='#',
'FILEHEADER'='empno,empname,designation,doj,workgroupcategory,
 workgroupcategoryname,deptno,deptname,projectcode,
 projectjoindate,projectenddate,attendance,utilization,salary',
'MULTILINE'='true','ESCAPECHAR'='\','COMPLEX_DELIMITER_LEVEL_1'='$',
'COMPLEX_DELIMITER_LEVEL_2'=':',
'ALL_DICTIONARY_PATH'='/opt/alldictionary/data.dictionary',
'SINGLE_PASS'='TRUE'
)
```


## INSERT DATA INTO A CARBONDATA TABLE

This command inserts data into a CarbonData table. It is defined as a combination of two queries Insert and Select query respectively. It inserts records from a source table into a target CarbonData table. The source table can be a Hive table, Parquet table or a CarbonData table itself. It comes with the functionality to aggregate the records of a table by performing Select query on source table and load its corresponding resultant records into a CarbonData table.

**NOTE** :  The client node where the INSERT command is executing, must be part of the cluster.

### Syntax

```
INSERT INTO TABLE <CARBONDATA TABLE> SELECT * FROM sourceTableName 
[ WHERE { <filter_condition> } ];
```

You can also omit the `table` keyword and write your query as:
 
```
INSERT INTO <CARBONDATA TABLE> SELECT * FROM sourceTableName 
[ WHERE { <filter_condition> } ];
```

### Parameter Description

| Parameter | Description |
|--------------|---------------------------------------------------------------------------------|
| CARBON TABLE | The name of the Carbon table in which you want to perform the insert operation. |
| sourceTableName | The table from which the records are read and inserted into destination CarbonData table. |

### Usage Guidelines
The following condition must be met for successful insert operation :

- The source table and the CarbonData table must have the same table schema.
- The table must be created.
- Overwrite is not supported for CarbonData table.
- The data type of source and destination table columns should be same, else the data from source table will be treated as bad records and the INSERT command fails.
- INSERT INTO command does not support partial success if bad records are found, it will fail.
- Data cannot be loaded or updated in source table while insert from source table to target table is in progress.

To enable data load or update during insert operation, configure the following property to true.

```
carbon.insert.persist.enable=true
```

By default the above configuration will be false.

**NOTE**: Enabling this property will reduce the performance.

### Examples
```
INSERT INTO table1 SELECT item1 ,sum(item2 + 1000) as result FROM 
table2 group by item1;
```

```
INSERT INTO table1 SELECT item1, item2, item3 FROM table2 
where item2='xyz';
```

```
INSERT INTO table1 SELECT * FROM table2 
where exists (select * from table3 
where table2.item1 = table3.item1);
```

**The Status Success/Failure shall be captured in the driver log.**

## SHOW SEGMENTS

This command is used to get the segments of CarbonData table.

```
SHOW SEGMENTS FOR TABLE [db_name.]table_name 
LIMIT number_of_segments;
```

### Parameter Description

| Parameter          | Description                                                          | Optional |
| ------------------ | ---------------------------------------------------------------------| ---------|
| db_name            | Database name, if it is not specified then it uses the current database. | YES      |
| table_name         | The name of the table in provided database.                          | NO       |
| number_of_segments | Limit the output to this number.                                     | YES      |

### Example:

```
SHOW SEGMENTS FOR TABLE CarbonDatabase.CarbonTable LIMIT 4;
```

## DELETE SEGMENT BY ID

This command is used to delete segment by using the segment ID. Each segment has a unique segment ID associated with it. 
Using this segment ID, you can remove the segment.

The following command will get the segmentID.

```
SHOW SEGMENTS FOR Table dbname.tablename LIMIT number_of_segments
```

After you retrieve the segment ID of the segment that you want to delete, execute the following command to delete the selected segment.

```
DELETE SEGMENT segment_sequence_id1, segments_sequence_id2, .... 
FROM TABLE tableName
```

### Parameter Description
| Parameter  | Description                                                          | Optional |
| -----------| ---------------------------------------------------------------------|----------|
| segment_id | Segment Id of the load.                                              | NO       |
| db_name    | Database name, if it is not specified then it uses the current database. | YES      |
| table_name | The name of the table in provided database.                          | NO       |

### Example:

```
DELETE SEGMENT 0 FROM TABLE CarbonDatabase.CarbonTable;
DELETE SEGMENT 0.1,5,8 FROM TABLE CarbonDatabase.CarbonTable;
```
  NOTE: Here 0.1 is compacted segment sequence id. 

## DELETE SEGMENT BY DATE

This command will allow to delete the CarbonData segment(s) from the store based on the date provided by the user in the DML command. 
The segment created before the particular date will be removed from the specific stores.

```
DELETE FROM TABLE [schema_name.]table_name 
WHERE[DATE_FIELD]BEFORE [DATE_VALUE]
```

### Parameter Description

| Parameter  | Description                                                                                        | Optional |
| ---------- | ---------------------------------------------------------------------------------------------------| -------- |
| DATE_VALUE | Valid segment load start time value. All the segments before this specified date will be deleted. | NO       |
| db_name    | Database name, if it is not specified then it uses the current database.                               | YES      |
| table_name | The name of the table in provided database.                                                        | NO       |

### Example:

```
 DELETE SEGMENTS FROM TABLE CarbonDatabase.CarbonTable 
 WHERE STARTTIME BEFORE '2017-06-01 12:05:06';  
```

## Update CarbonData Table
This command will allow to update the carbon table based on the column expression and optional filter conditions.

### Syntax

```
 UPDATE <table_name>
 SET (column_name1, column_name2, ... column_name n) =
 (column1_expression , column2_expression . .. column n_expression )
 [ WHERE { <filter_condition> } ];
```

alternatively the following the command can also be used for updating the CarbonData Table :

```
UPDATE <table_name>
SET (column_name1, column_name2,) =
(select sourceColumn1, sourceColumn2 from sourceTable
[ WHERE { <filter_condition> } ] )
[ WHERE { <filter_condition> } ];
```

### Parameter Description

| Parameter | Description |
|--------------|---------------------------------------------------------------------------------|
| table_name | The name of the Carbon table in which you want to perform the update operation. |
| column_name | The destination columns to be updated. |
| sourceColumn | The source table column values to be updated in destination table. |
| sourceTable | The table from which the records are updated into destination Carbon table. |

### Usage Guidelines
The following conditions must be met for successful updation :

- The update command fails if multiple input rows in source table are matched with single row in destination table.
- If the source table generates empty records, the update operation will complete successfully without updating the table.
- If a source table row does not correspond to any of the existing rows in a destination table, the update operation will complete successfully without updating the table.
- In sub-query, if the source table and the target table are same, then the update operation fails.
- If the sub-query used in UPDATE statement contains aggregate method or group by query, then the UPDATE operation fails.

### Examples

 Update is not supported for queries that contain aggregate or group by.

```
 UPDATE t_carbn01 a
 SET (a.item_type_code, a.profit) = ( SELECT b.item_type_cd,
 sum(b.profit) from t_carbn01b b
 WHERE item_type_cd =2 group by item_type_code);
```

Here the Update Operation fails as the query contains aggregate function sum(b.profit) and group by clause in the sub-query.


```
UPDATE carbonTable1 d
SET(d.column3,d.column5 ) = (SELECT s.c33 ,s.c55
FROM sourceTable1 s WHERE d.column1 = s.c11)
WHERE d.column1 = 'china' EXISTS( SELECT * from table3 o where o.c2 > 1);
```


```
UPDATE carbonTable1 d SET (c3) = (SELECT s.c33 from sourceTable1 s
WHERE d.column1 = s.c11)
WHERE exists( select * from iud.other o where o.c2 > 1);
```


```
UPDATE carbonTable1 SET (c2, c5 ) = (c2 + 1, concat(c5 , "y" ));
```


```
UPDATE carbonTable1 d SET (c2, c5 ) = (c2 + 1, "xyx")
WHERE d.column1 = 'india';
```


```
UPDATE carbonTable1 d SET (c2, c5 ) = (c2 + 1, "xyx")
WHERE d.column1 = 'india'
and EXISTS( SELECT * FROM table3 o WHERE o.column2 > 1);
```

**The Status Success/Failure shall be captured in the driver log and the client.**


## Delete Records from CarbonData Table
This command allows us to delete records from CarbonData table.

### Syntax

```
DELETE FROM table_name [WHERE expression];
```

### Parameter Description

| Parameter | Description |
|--------------|-----------------------------------------------------------------------|
| table_name | The name of the Carbon table in which you want to perform the delete. |


### Examples

```
DELETE FROM columncarbonTable1 d WHERE d.column1  = 'china';
```

```
DELETE FROM dest WHERE column1 IN ('china', 'USA');
```

```
DELETE FROM columncarbonTable1
WHERE column1 IN (SELECT column11 FROM sourceTable2);
```

```
DELETE FROM columncarbonTable1
WHERE column1 IN (SELECT column11 FROM sourceTable2 WHERE
column1 = 'USA');
```

```
DELETE FROM columncarbonTable1 WHERE column2 >= 4
```

**The Status Success/Failure shall be captured in the driver log and the client.**
