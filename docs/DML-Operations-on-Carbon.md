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

* [LOAD DATA](#LOAD DATA)
* [SHOW SEGMENTS](#SHOW SEGMENTS)
* [DELETE SEGMENT BY ID](#DELETE SEGMENT BY ID)
* [DELETE SEGMENT BY DATE](#DELETE SEGMENT BY DATE)
* [UPDATE CARBON TABLE](#UPDATE CARBON TABLE)
* [DELETE RECORDS from CARBON TABLE](#DELETE RECORDS from CARBON TABLE)

***

# LOAD DATA
 This command loads the user data in raw format to the Carbon specific data format store, this way Carbon provides good performance while querying the data.Please visit [Data Management](Carbondata-Management.md) for more details on LOAD

### Syntax

  ```ruby
  LOAD DATA [LOCAL] INPATH 'folder_path' INTO TABLE [db_name.]table_name 
              OPTIONS(property_name=property_value, ...)
  ```

### Parameter Description

| Parameter | Description | Optional |
| ------------- | -----| -------- |
| folder_path | Path of raw csv data folder or file. | NO |
| db_name | Database name, if it is not specified then it uses current database. | YES |
| table_name | The name of the table in provided database.| NO |
| OPTIONS | Extra options provided to Load | YES |
 

### Usage Guideline
Following are the options that can be used in load data:
- **DELIMITER:** Delimiters can be provided in the load command.
    
    ``` ruby
    OPTIONS('DELIMITER'=',')
    ```
- **QUOTECHAR:** Quote Characters can be provided in the load command.

    ```ruby
    OPTIONS('QUOTECHAR'='"')
    ```
- **COMMENTCHAR:** Comment Characters can be provided in the load command if user want to comment lines.

    ```ruby
    OPTIONS('COMMENTCHAR'='#')
    ```
- **FILEHEADER:** Headers can be provided in the LOAD DATA command if headers are missing in the source files.

    ```ruby
    OPTIONS('FILEHEADER'='column1,column2') 
    ```
- **MULTILINE:** CSV with new line character in quotes.

    ```ruby
    OPTIONS('MULTILINE'='true') 
    ```
- **ESCAPECHAR:** Escape char can be provided if user want strict validation of escape character on CSV.

    ```ruby
    OPTIONS('ESCAPECHAR'='\') 
    ```
- **COMPLEX_DELIMITER_LEVEL_1:** Split the complex type data column in a row (eg., a$b$c --> Array = {a,b,c}).

    ```ruby
    OPTIONS('COMPLEX_DELIMITER_LEVEL_1'='$') 
    ```
- **COMPLEX_DELIMITER_LEVEL_2:** Split the complex type nested data column in a row. Applies level_1 delimiter & applies level_2 based on complex data type (eg., a:b$c:d --> Array> = {{a,b},{c,d}}).

    ```ruby
    OPTIONS('COMPLEX_DELIMITER_LEVEL_2'=':') 
    ```
- **ALL_DICTIONARY_PATH:** All dictionary files path.

    ```ruby
    OPTIONS('ALL_DICTIONARY_PATH'='/opt/alldictionary/data.dictionary')
    ```
- **COLUMNDICT:** Dictionary file path for specified column.

    ```ruby
    OPTIONS('COLUMNDICT'='column1:dictionaryFilePath1, column2:dictionaryFilePath2')
    ```
    Note: ALL_DICTIONARY_PATH and COLUMNDICT can't be used together.
- **DATEFORMAT:** Date format for specified column.

    ```ruby
    OPTIONS('DATEFORMAT'='column1:dateFormat1, column2:dateFormat2')
    ```
    Note: Date formats are specified by date pattern strings. The date pattern letters in Carbon are
    the same as in JAVA [SimpleDateFormat](http://docs.oracle.com/javase/7/docs/api/java/text/SimpleDateFormat.html).

**Example:**

  ```ruby
  LOAD DATA local inpath '/opt/rawdata/data.csv' INTO table carbontable
                         options('DELIMITER'=',', 'QUOTECHAR'='"', 'COMMENTCHAR'='#',
                                 'FILEHEADER'='empno,empname,
                                  designation,doj,workgroupcategory,
                                  workgroupcategoryname,deptno,deptname,projectcode,
                                  projectjoindate,projectenddate,attendance,utilization,salary',
                                 'MULTILINE'='true', 'ESCAPECHAR'='\', 
                                 'COMPLEX_DELIMITER_LEVEL_1'='$', 
                                 'COMPLEX_DELIMITER_LEVEL_2'=':',
                                 'ALL_DICTIONARY_PATH'='/opt/alldictionary/data.dictionary',
                                 'DATEFORMAT'='projectjoindate:yyyy-MM-dd'
                                 )
  ```

***

# SHOW SEGMENTS
This command is to show the segments of carbon table to the user.

  ```ruby
  SHOW SEGMENTS FOR TABLE [db_name.]table_name LIMIT number_of_segments;
  ```

### Parameter Description

| Parameter | Description | Optional |
| ------------- | -----| --------- |
| db_name | Database name, if it is not specified then it uses current database. | YES |
| table_name | The name of the table in provided database.| NO |
| number_of_segments | limit the output to this number. | YES |

**Example:**

  ```ruby
  SHOW SEGMENTS FOR TABLE CarbonDatabase.CarbonTable LIMIT 2;
  ```

***

# DELETE SEGMENT BY ID

This command is to delete segment by using the segment ID.

  ```ruby
  DELETE SEGMENT segment_id1,segment_id2 FROM TABLE [db_name.]table_name;
  ```

### Parameter Description

| Parameter | Description | Optional |
| ------------- | -----| --------- |
| segment_id | Segment Id of the load. | NO |
| db_name | Database name, if it is not specified then it uses current database. | YES |
| table_name | The name of the table in provided database.| NO |

**Example:**

  ```ruby
  DELETE SEGMENT 0 FROM TABLE CarbonDatabase.CarbonTable;
  DELETE SEGMENT 0.1,5,8 FROM TABLE CarbonDatabase.CarbonTable;
  ```
  Note: Here 0.1 is compacted segment sequence id.  

***

# DELETE SEGMENT BY DATE
This command will allow to deletes the Carbon segment(s) from the store based on the date provided by the user in the DML command. The segment created before the particular date will be removed from the specific stores.

  ```ruby
  DELETE SEGMENTS FROM TABLE [db_name.]table_name WHERE STARTTIME BEFORE [DATE_VALUE];
  ```

### Parameter Description

| Parameter | Description | Optional |
| ------------- | -----| ------ |
| DATE_VALUE | Valid segement load start time value. All the segments before this specified date will be deleted. | NO |
| db_name | Database name, if it is not specified then it uses current database. | YES |
| table_name | The name of the table in provided database.| NO |

**Example:**

  ```ruby
  DELETE SEGMENTS FROM TABLE CarbonDatabase.CarbonTable WHERE STARTTIME BEFORE '2017-06-01 12:05:06';  
  ```

***

# UPDATE CARBON TABLE
This command updates the carbon table based on the column expression and optional filter conditions.
 
The client node where the UPDATE command is executed should be part of the cluster.
### Syntax
Syntax1:
  ```ruby
  UPDATE <CARBON TABLE>
  SET (column_name1, column_name2, ... column_name n) =  (column1_expression , column2_expression , column3_expression . .. column n_expression )
  [ WHERE { <filter_condition> } ]; 
  ```
Syntax2:
  ```ruby
  UPDATE <CARBON TABLE>
  SET (column_name1, column_name2,) =  (select sourceColumn1, sourceColumn2 from sourceTable [ WHERE { <filter_condition> } ] )
  [ WHERE { <filter_condition> } ];
  ```

### Parameter Description

| Parameter | Description |
| ------------- | -----|
| CARBON TABLE | The name of the Carbon table in which you want to perform the update operation. |
| column_name | The destination columns to be updated. |
| sourceColumn | The source table column values to be updated in destination table. |
| sourceTable | The table from which the records are updated into destination Carbon table. |
 
### Usage Guidelines
Following are the conditions to use UPDATE:
- The update command fails if multiple input rows in source table are matched with single row in destination table.
- If the source table generates empty records, the update operation will complete without updating the table.
- If a source table row does not correspond to any of the existing rows in a destination table, the update operation will complete without updating the table.
- In sub-query, if the source table and the target table are same, then the update operation fails.
- If the sub-query used in UPDATE statement contains aggregate method or group by query, then the UPDATE operation fails.
  For example, update t_carbn01 a set (a.item_type_code, a.profit) = ( select b.item_type_cd, sum(b.profit) from t_carbn01b b where item_type_cd =2 group by item_type_code);
  Here, aggregate function sum(b.profit) and group by clause are used in the sub-query, hence the UPDATE operation fails.

**Example1:**
  ```ruby
  update carbonTable1 d set (d.column3,d.column5 ) = (select s.c33 ,s.c55 from sourceTable1 s where d.column1 = s.c11) where d.column1 = 'china' exists( select * from table3 o where o.c2 > 1);
  ```

**Example2:**
  ```ruby
  update carbonTable1 d set (c3) = (select s.c33 from sourceTable1 s where d.column1 = s.c11) where exists( select * from iud.other o where o.c2 > 1);
  ```

**Example3:**
  ```ruby
  update carbonTable1 set (c2, c5 ) = (c2 + 1, concat(c5 , "y" ));
  ```

**Example4:**
  ```ruby
  update carbonTable1 d set (c2, c5 ) = (c2 + 1, "xyx") where d.column1 = 'india';
  ```

**Example5:**
  ```ruby
  update carbonTable1 d set (c2, c5 ) = (c2 + 1, "xyx") where d.column1 = 'india' and exists( select * from table3 o where o.column2 > 1);
  ```

### System Response
Success/Failure will be captured in the driver log and the client.

***
 
# DELETE RECORDS from CARBON TABLE
This command delete records from Carbon table.
The client node where the DELETE RECORDS command is executed should be part of the cluster.

### Syntax
DELETE FROM CARBON_TABLE [WHERE expression];

### Parameter Description

| Parameter | Description |
| ------------- | -----|
| CARBON TABLE | The name of the Carbon table in which you want to perform the delete. |
 
**Example1:**
  ```ruby
  delete from columncarbonTable1 d where d.column1  = 'china'
  ```
**Example2:**
  ```ruby
  delete from dest where column1 IN ('china', 'USA')
  ```

**Example3:**
  ```ruby
  delete from columncarbonTable1 where column1 IN (select column11 from sourceTable2)
  ```

**Example4:**
  ```ruby
  delete from columncarbonTable1 where column1 IN (select column11 from sourceTable2 where column1 = 'USA')
  ```

**Example5:**
  ```ruby
  delete from columncarbonTable1 where column2 >= 4
  ```

### System Response
Success/Failure will be captured in the driver log and client.

***
