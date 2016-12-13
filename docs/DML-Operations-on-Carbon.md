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