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

# DDL Operations on CarbonData
This tutorial guides you through the data definition language support provided by CarbonData.

## Overview 
The following DDL operations are supported in CarbonData :

* [CREATE TABLE](#create-table)
* [SHOW TABLE](#show-table)
* [DROP TABLE](#drop-table)
* [COMPACTION](#compaction)
* [BUCKETING](#bucketing)

## CREATE TABLE
  This command can be used to create a CarbonData table by specifying the list of fields along with the table properties.
```
   CREATE TABLE [IF NOT EXISTS] [db_name.]table_name
                    [(col_name data_type , ...)]
   STORED BY 'carbondata'
   [TBLPROPERTIES (property_name=property_value, ...)]
   // All Carbon's additional table options will go into properties
```

### Parameter Description

| Parameter | Description | Optional |
|---------------|-----------------------------------------------------------------------------------------------------------------------------------------------|----------|
| db_name | Name of the database. Database name should consist of alphanumeric characters and underscore(_) special character. | Yes |
| field_list | Comma separated List of fields with data type. The field names should consist of alphanumeric characters and underscore(_) special character. | No |
| table_name | The name of the table in Database. Table Name should consist of alphanumeric characters and underscore(_) special character. | No |
| STORED BY | "org.apache.carbondata.format", identifies and creates a CarbonData table. | No |
| TBLPROPERTIES | List of CarbonData table properties. |  |

### Usage Guidelines

   Following are the guidelines for using table properties.

   - **Dictionary Encoding Configuration**

       Dictionary encoding is enabled by default for all String columns, and disabled for non-String columns. You can include and exclude columns for dictionary encoding.
```
       TBLPROPERTIES ('DICTIONARY_EXCLUDE'='column1, column2')
       TBLPROPERTIES ('DICTIONARY_INCLUDE'='column1, column2')
```

   Here, DICTIONARY_EXCLUDE will exclude dictionary creation. This is applicable for high-cardinality columns and is an optional parameter. DICTIONARY_INCLUDE will generate dictionary for the columns specified in the list.

   - **Row/Column Format Configuration**

       Column groups with more than one column are stored in row format, instead of columnar format. By default, each column is a separate column group.
```
TBLPROPERTIES ('COLUMN_GROUPS'='(column1, column3),
(Column4,Column5,Column6)')
```

   - **Table Block Size Configuration**

     The block size of table files can be defined using the property TABLE_BLOCKSIZE. It accepts only integer values. The default value is 1024 MB and supports a range of 1 MB to 2048 MB.
     If you do not specify this value in the DDL command, default value is used.
```
       TBLPROPERTIES ('TABLE_BLOCKSIZE'='512')
```

  Here 512 MB means the block size of this table is 512 MB, you can also set it as 512M or 512.

   - **Inverted Index Configuration**

      Inverted index is very useful to improve compression ratio and query speed, especially for those low-cardinality columns who are in reward position.
      By default inverted index is enabled. The user can disable the inverted index creation for some columns.
```
       TBLPROPERTIES ('NO_INVERTED_INDEX'='column1, column3')
```

  No inverted index shall be generated for the columns specified in NO_INVERTED_INDEX. This property is applicable on columns with high-cardinality and is an optional parameter.

   NOTE:

   - By default all columns other than numeric datatype are treated as dimensions and all columns of numeric datatype are treated as measures.

   - All dimensions except complex datatype columns are part of multi dimensional key(MDK). This behavior can be overridden by using TBLPROPERTIES. If the user wants to keep any column (except columns of complex datatype) in multi dimensional key then he can keep the columns either in DICTIONARY_EXCLUDE or DICTIONARY_INCLUDE.

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
      TBLPROPERTIES ('COLUMN_GROUPS'='(productNumber,productName)',
                     'DICTIONARY_EXCLUDE'='storeCity',
                     'DICTIONARY_INCLUDE'='productNumber',
                     'NO_INVERTED_INDEX'='productBatch')
```

## SHOW TABLE

  This command can be used to list all the tables in current database or all the tables of a specific database.
```
  SHOW TABLES [IN db_Name];
```

### Parameter Description
| Parameter  | Description                                                                               | Optional |
|------------|-------------------------------------------------------------------------------------------|----------|
| IN db_Name | Name of the database. Required only if tables of this specific database are to be listed. | Yes      |

### Example:
```
  SHOW TABLES IN ProductSchema;
```

## DROP TABLE

 This command is used to delete an existing table.
```
  DROP TABLE [IF EXISTS] [db_name.]table_name;
```

### Parameter Description
| Parameter | Description | Optional |
|-----------|-------------| -------- |
| db_Name | Name of the database. If not specified, current database will be selected. | YES |
| table_name | Name of the table to be deleted. | NO |

### Example:
```
  DROP TABLE IF EXISTS productSchema.productSalesTable;
```

## COMPACTION

This command merges the specified number of segments into one segment. This enhances the query performance of the table.
```
  ALTER TABLE [db_name.]table_name COMPACT 'MINOR/MAJOR';
```

  To get details about Compaction refer to [Data Management](data-management.md)

### Parameter Description

| Parameter | Description | Optional |
| ------------- | -----| ----------- |
| db_name | Database name, if it is not specified then it uses current database. | YES |
| table_name | The name of the table in provided database.| NO |

### Syntax

- **Minor Compaction**
```
ALTER TABLE table_name COMPACT 'MINOR';
```
- **Major Compaction**
```
ALTER TABLE table_name COMPACT 'MAJOR';
```

## BUCKETING

Bucketing feature can be used to distribute/organize the table/partition data into multiple files such
that similar records are present in the same file. While creating a table, a user needs to specify the
columns to be used for bucketing and the number of buckets. For the selction of bucket the Hash value
of columns is used.

```
   CREATE TABLE [IF NOT EXISTS] [db_name.]table_name
                    [(col_name data_type, ...)]
   STORED BY 'carbondata'
   TBLPROPERTIES(“BUCKETNUMBER”=”noOfBuckets”,
   “BUCKETCOLUMNS”=’’columnname”)
```
  
## Parameter Description

| Parameter 	| Description 	| Optional 	|
|---------------	|------------------------------------------------------------------------------------------------------------------------------	|----------	|
| BUCKETNUMBER 	| Specifies the number of Buckets to be created. 	| No 	|
| BUCKETCOLUMNS 	| Specify the columns to be considered for Bucketing  	| No 	|

## Usage Guidelines

- The feature is supported for Spark 1.6.2 onwards, but the performance optimization is evident from Spark 2.1 onwards.

- Bucketing can not be performed for columns of Complex Data Types.

- Columns in the BUCKETCOLUMN parameter must be only dimension. The BUCKETCOLUMN parameter can not be a measure or a combination of measures and dimensions.


## Example :

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
   TBLPROPERTIES ('COLUMN_GROUPS'='(productName,productNumber)',
                  'DICTIONARY_EXCLUDE'='productName',
                  'DICTIONARY_INCLUDE'='productNumber',
                  'NO_INVERTED_INDEX'='productBatch',
                  'BUCKETNUMBER'='4',
                  'BUCKETCOLUMNS'='productName')
 ```

