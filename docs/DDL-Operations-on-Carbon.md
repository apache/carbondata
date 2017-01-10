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

* [CREATE TABLE](#CREATE TABLE)
* [SHOW TABLE](#SHOW TABLE)
* [DROP TABLE](#DROP TABLE)
* [COMPACTION](#COMPACTION)

***


# CREATE TABLE
This command can be used to create carbon table by specifying the list of fields along with the table properties.

  ```
  CREATE TABLE [IF NOT EXISTS] [db_name.]table_name 
               [(col_name data_type , ...)]               
         STORED BY 'carbondata'
               [TBLPROPERTIES (property_name=property_value, ...)]
               // All Carbon's additional table options will go into properties
  ```
     

### Parameter Description

| Parameter | Description | Optional |
| ------------- | -----| ---------- |
| db_name | Name of the Database. Database name should consist of Alphanumeric characters and underscore(_) special character. | YES |
| field_list | Comma separated List of fields with data type. The field names should consist of Alphanumeric characters and underscore(_) special character.| NO |
|table_name | The name of the table in Database. Table Name should consist of Alphanumeric characters and underscore(_) special character. | NO |
| STORED BY | "org.apache.carbondata.format", identifies and creates carbon table. | NO |
| TBLPROPERTIES | List of carbon table properties. | YES |

### Usage Guideline
Following are the table properties usage.

 - **Dictionary Encoding Configuration**

   By Default dictionary encoding will be enabled for all String columns, and disabled for non-String columns. User can include and exclude columns for dictionary encoding.

  ```ruby
  TBLPROPERTIES ("DICTIONARY_EXCLUDE"="column1, column2") 
  TBLPROPERTIES ("DICTIONARY_INCLUDE"="column1, column2") 
  ```
Here, DICTIONARY_EXCLUDE will exclude dictionary creation. This is applicable for high-cardinality columns and is a optional parameter. DICTIONARY_INCLUDE will generate dictionary for the columns specified in the list.

 - **Row/Column Format Configuration**

   Column groups with more than one column are stored in row format, instead of columnar format. By default, each column is a separate column group.

  ```ruby
  TBLPROPERTIES ("COLUMN_GROUPS"="(column1,column3),(Column4,Column5,Column6)") 
  ```
 - **Table Block Size Configuration**

   The block size of one table's files on hdfs can be defined using an int value whose size is in MB, the range is form 1MB to 2048MB and the default value is 1024MB, if user didn't define this values in ddl, it would use default value to set.

  ```ruby
  TBLPROPERTIES ("TABLE_BLOCKSIZE"="512 MB")
  ```
Here 512 MB means the block size of this table is 512 MB, user also can set it as 512M or 512.
 - **Inverted Index Configuration**

   Inverted index is very useful to improve compression ratio and query speed, especially for those low-cardinality columns who are in reward position.
   By default inverted index will be enabled, but user can also set not to use inverted index for some columns.

  ```ruby
  TBLPROPERTIES ("NO_INVERTED_INDEX"="column1,column3")
  ```
Here, NO_INVERTED_INDEX will not use inverted index for the specified columns. This is applicable for high-cardinality columns and is a optional parameter.

*Note : By default all columns except numeric datatype columns are treated as dimensions and all numeric datatype columns are treated as measures. All dimensions except complex datatype columns are part of multi dimensional key(MDK). This behavior can be overridden by using TBLPROPERTIES, If user wants to keep any column (except complex datatype) in multi dimensional key then he can keep the columns either in DICTIONARY_EXCLUDE or DICTIONARY_INCLUDE*


**Example:**

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
       TBLPROPERTIES ('COLUMN_GROUPS'='(productName,productCategory)',
                     'DICTIONARY_EXCLUDE'='productName',
                     'DICTIONARY_INCLUDE'='productNumber',
                     'NO_INVERTED_INDEX'='productBatch')
  ```
***

# SHOW TABLE
This command can be used to list all the tables in current database or all the tables of a specific database.

  ```ruby
  SHOW TABLES [IN db_Name];
  ```

### Parameter Description
| Parameter | Description | Optional |
|-----------|-------------| -------- |
| IN db_Name | Name of the database. Required only if tables of this specific database are to be listed. | YES |

**Example:**

  ```ruby
  SHOW TABLES IN ProductSchema;
  ```

***

# DROP TABLE
This command can be used to delete the existing table.

  ```ruby
  DROP TABLE [IF EXISTS] [db_name.]table_name;
  ```

### Parameter Description
| Parameter | Description | Optional |
|-----------|-------------| -------- |
| db_Name | Name of the database. If not specified, current database will be selected. | YES |
| table_name | Name of the table to be deleted. | NO |

**Example:**

  ```ruby
  DROP TABLE IF EXISTS productSchema.productSalesTable;
  ```

***

# COMPACTION
 This command will merge the specified number of segments into one segment. This will enhance the query performance of the table.

  ```ruby
  ALTER TABLE [db_name.]table_name COMPACT 'MINOR/MAJOR'
  ```

### Parameter Description

| Parameter | Description | Optional |
| ------------- | -----| ----------- |
| db_name | Database name, if it is not specified then it uses current database. | YES |
| table_name | The name of the table in provided database.| NO |
 
### Usage Guideline
Minor Compaction:
  In Minor compaction, you can specify number of loads to be merged (compacted). 
  Minor compaction triggers for every data load if the parameter carbon.enable.auto.load.merge is set. 
  If any segments are available to be merged, then compaction will run parallel with data load. 
  There are 2 levels in minor compaction.
   - Level 1: Merging of the segments which are not yet compacted.
   - Level 2: Merging of the compacted segments again to form a bigger segment.

Major Compaction:
 In Major compaction, many segments can be merged into one big segment. 
 You can specify the compaction size until which the segments will be merged. 
 Major compaction is usually done during the off-peak time.

**Example:**

  ```ruby
  ALTER TABLE carbontable COMPACT MINOR
  ALTER TABLE carbontable COMPACT MAJOR
  ```

***
