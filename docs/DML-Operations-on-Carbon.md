* [LOAD DATA](#LOAD DATA)
* [SHOW LOADS](#SHOW LOADS)
* [DELETE SEGMENT BY ID](#DELETE SEGMENT BY ID)
* [DELETE SEGMENT BY DATE](#DELETE SEGMENT BY DATE)

***

# LOAD DATA
### Function
 This command loads the user data in raw format to the Carbon specific data format store, this way Carbon provides good performance while querying the data.

### Syntax

  ```ruby
  LOAD DATA [LOCAL] INPATH 'folder_path' INTO TABLE [db_name.]table_name 
              OPTIONS(property_name=property_value, ...)
  ```

**Example:**

  ```ruby
  LOAD DATA local inpath '/opt/rawdata/data.csv' INTO table carbontable
                         options('DELIMITER'=',', 'QUOTECHAR'='"',
                                 'FILEHEADER'='empno,empname,
                                  designation,doj,workgroupcategory,
                                  workgroupcategoryname,deptno,deptname,projectcode,
                                  projectjoindate,projectenddate,attendance,utilization,salary',
                                 'MULTILINE'='true', 'ESCAPECHAR'='\', 
                                 'COMPLEX_DELIMITER_LEVEL_1'='$', 
                                 'COMPLEX_DELIMITER_LEVEL_2'=':',
                                 'LOCAL_DICTIONARY_PATH'='/opt/localdictionary/',
                                 'DICTIONARY_FILE_EXTENSION'='.dictionary') 
  ```

### Parameter Description

| Parameter | Description |
| ------------- | -----|
| folder_path | Path of raw csv data folder or file. |
| db_name | Database name, if it is not specified then it uses current database. |
| table_name | The name of the table in provided database.|
 

### Usage Guideline
Following are the options that can be used in load data:
- **DELIMITER:** Delimiters and Quote Characters can be provided in the load command.
    
    ``` ruby
    OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"') 
    ```
- **QUOTECHAR:** Delimiters and Quote Characters can be provided in the load command.

    ```ruby
    OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"') 
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
- **LOCAL_DICTIONARY_PATH:** Local dictionary files path.

    ```ruby
    OPTIONS('LOCAL_DICTIONARY_PATH'='/opt/localdictionary/') 
    ```
- **DICTIONARY_FILE_EXTENSION:** local Dictionary file extension.

    ```ruby
    OPTIONS('DICTIONARY_FILE_EXTENSION'='.dictionary') 
    ```

### Scenarios

#### Load from CSV files

To load carbon table from CSV file, use the following syntax.

  ```ruby
  LOAD DATA [LOCAL] INPATH 'folder path' INTO TABLE tablename OPTIONS(property_name=property_value, ...)
  ```

 **Example:**
  
  ```ruby
  LOAD DATA local inpath './src/test/resources/data.csv' INTO table carbontable 
                      options('DELIMITER'=',', 'QUOTECHAR'='"', 
                              'FILEHEADER'='empno,empname,designation,doj,
                               workgroupcategory,workgroupcategoryname,
                               deptno,deptname,projectcode,projectjoindate,
                               projectenddate,attendance,utilization,salary', 
                              'MULTILINE'='true', 'ESCAPECHAR'='\', 
                              'COMPLEX_DELIMITER_LEVEL_1'='$', 'COMPLEX_DELIMITER_LEVEL_2'=':', 
                              'LOCAL_DICTIONARY_PATH'='/opt/localdictionary/','DICTIONARY_FILE_EXTENSION'='.dictionary')
  ```

***

# SHOW SEGMENTS
### Function
This command is to show the segments of carbon table to the user.

### Syntax

  ```ruby
  SHOW SEGMENTS FOR TABLE [db_name.]table_name LIMIT number_of_segments;
  ```

**Example:**

  ```ruby
  SHOW SEGMENTS FOR TABLE CarbonDatabase.CarbonTable LIMIT 2;
  ```

### Parameter Description

| Parameter | Description |
| ------------- | -----|
| db_name | Database name, if it is not specified then it uses current database. |
| table_name | The name of the table in provided database.|
| number_of_loads | limit the output to this number. |

### Usage Guideline
NA

### Scenarios
NA

***

# DELETE SEGMENT BY ID
### Function

This command is to delete segment by using the segment ID.

### Syntax

  ```ruby
  DELETE SEGMENT segment_id1,segment_id2 FROM TABLE [db_name.]table_name;
  ```

**Example:**

  ```ruby
  DELETE LOAD 0 FROM TABLE CarbonDatabase.CarbonTable;
  DELETE LOAD 0.1,5,8 FROM TABLE CarbonDatabase.CarbonTable;
  Note: Here 0.1 is compacted segment sequence id.  
  ```

### Parameter Description

| Parameter | Description |
| ------------- | -----|
| segment_id | Segment Id of the load. |
| db_name | Database name, if it is not specified then it uses current database. |
| table_name | The name of the table in provided database.|

### Usage Guideline
NA

### Scenarios
NA

***

# DELETE SEGMENT BY DATE
### Function

This command will allow to deletes the Carbon segment(s) from the store based on the date provided by the user in the DML command. The segment created before the particular date will be removed from the specific stores.

### Syntax

  ```ruby
  DELETE SEGMENTS FROM TABLE [db_name.]table_name WHERE STARTTIME BEFORE [DATE_VALUE];
  ```

**Example:**

  ```ruby
  DELETE SEGMENTS FROM TABLE CarbonDatabase.CarbonTable WHERE STARTTIME BEFORE '2017-06-01 12:05:06';  
  ```

### Parameter Description

| Parameter | Description |
| ------------- | -----|
| DATE_VALUE | Valid segement load start time value. All the segments before this specified date will be deleted. |
| db_name | Database name, if it is not specified then it uses current database. |
| table_name | The name of the table in provided database.|

### Usage Guideline
NA

### Scenarios
NA

***