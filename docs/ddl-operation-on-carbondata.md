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
* [ALTER TABLE](#alter-table)
  - [RENAME TABLE](#rename-table)
  - [ADD COLUMN](#add-column)
  - [DROP COLUMNS](#drop-columns)
  - [CHANGE DATA TYPE](#change-data-type)
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
| db_name | Name of the database. Database name should consist of alphanumeric characters and underscore(\_) special character. | Yes |
| field_list | Comma separated List of fields with data type. The field names should consist of alphanumeric characters and underscore(\_) special character. | No |
| table_name | The name of the table in Database. Table Name should consist of alphanumeric characters and underscore(\_) special character. | No |
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



   - **Table Block Size Configuration**

     The block size of table files can be defined using the property TABLE_BLOCKSIZE. It accepts only integer values. The default value is 1024 MB and supports a range of 1 MB to 2048 MB.
     If you do not specify this value in the DDL command, default value is used.

```
       TBLPROPERTIES ('TABLE_BLOCKSIZE'='512')
```

  Here 512 MB means the block size of this table is 512 MB, you can also set it as 512M or 512.

   - **Inverted Index Configuration**

      Inverted index is very useful to improve compression ratio and query speed, especially for those low-cardinality columns which are in reward position.
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
      TBLPROPERTIES ('DICTIONARY_EXCLUDE'='storeCity',
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

## ALTER TABLE

The following section shall discuss the commands to modify the physical or logical state of the existing table(s).

### **RENAME TABLE**

This command is used to rename the existing table.
```
    ALTER TABLE [db_name.]table_name RENAME TO new_table_name;
```

#### Parameter Description
| Parameter     | Description                                                                                   |
|---------------|-----------------------------------------------------------------------------------------------|
| db_Name       | Name of the database. If this parameter is left unspecified, the current database is selected.|
|table_name     | Name of the existing table.                                                                   |
|new_table_name | New table name for the existing table.                                                        |

#### Usage Guidelines

- Queries that require the formation of path using the table name for reading carbon store files, running in parallel with Rename command might fail during the renaming operation.

- Renaming of Secondary index table(s) is not permitted.

#### Examples:

```
    ALTER TABLE carbon RENAME TO carbondata;
```

```
    ALTER TABLE test_db.carbon RENAME TO test_db.carbondata;
```

### **ADD COLUMN**

This command is used to add a new column to the existing table.

```
    ALTER TABLE [db_name.]table_name ADD COLUMNS (col_name data_type,...)
    TBLPROPERTIES('DICTIONARY_INCLUDE'='col_name,...',
    'DICTIONARY_EXCLUDE'='col_name,...',
    'DEFAULT.VALUE.COLUMN_NAME'='default_value');
```

#### Parameter Description
| Parameter          | Description                                                                                               |
|--------------------|-----------------------------------------------------------------------------------------------------------|
| db_Name            | Name of the database. If this parameter is left unspecified, the current database is selected.            |
| table_name         | Name of the existing table.                                                                               |
| col_name data_type | Name of comma-separated column with data type. Column names contain letters, digits, and underscores (\_). |

NOTE: Do not name the column after name, tupleId, PositionId, and PositionReference when creating Carbon tables because they are used internally by UPDATE, DELETE, and secondary index.

#### Usage Guidelines

- Apart from DICTIONARY_INCLUDE, DICTIONARY_EXCLUDE and default_value no other property will be read. If any other property name is specified, error will not be thrown, it will be ignored.

- If default value is not specified, then NULL will be considered as the default value for the column.

- For addition of column, if DICTIONARY_INCLUDE and DICTIONARY_EXCLUDE are not specified, then the decision will be taken based on data type of the column.

#### Examples:

```
    ALTER TABLE carbon ADD COLUMNS (a1 INT, b1 STRING);
```

```
    ALTER TABLE carbon ADD COLUMNS (a1 INT, b1 STRING)
    TBLPROPERTIES('DICTIONARY_EXCLUDE'='b1');
```

```
    ALTER TABLE carbon ADD COLUMNS (a1 INT, b1 STRING)
    TBLPROPERTIES('DICTIONARY_INCLUDE'='a1');
```

```
    ALTER TABLE carbon ADD COLUMNS (a1 INT, b1 STRING)
    TBLPROPERTIES('DEFAULT.VALUE.a1'='10');
```


### **DROP COLUMNS**

This command is used to delete a existing column or multiple columns in a table.

```
    ALTER TABLE [db_name.]table_name DROP COLUMNS (col_name, ...);
```

#### Parameter Description
| Parameter  | Description                                                                                              |
|------------|----------------------------------------------------------------------------------------------------------|
| db_Name    | Name of the database. If this parameter is left unspecified, the current database is selected.           |
| table_name | Name of the existing table.                                                                              |
| col_name   | Name of comma-separated column with data type. Column names contain letters, digits, and underscores (\_) |

#### Usage Guidelines

- Deleting a column will also clear the dictionary files, provided the column is of type dictionary.

- For delete column operation, there should be at least one key column that exists in the schema after deletion else error message will be displayed and the operation shall fail.

#### Examples:

If the table contains 4 columns namely a1, b1, c1, and d1.

- **To delete a single column:**

```
   ALTER TABLE carbon DROP COLUMNS (b1);
```

```
    ALTER TABLE test_db.carbon DROP COLUMNS (b1);
```


- **To delete multiple columns:**

```
   ALTER TABLE carbon DROP COLUMNS (b1,c1);
```

```
   ALTER TABLE carbon DROP COLUMNS (b1,c1);
```

### **CHANGE DATA TYPE**

This command is used to change the data type from INT to BIGINT or decimal precision from lower to higher.

```
    ALTER TABLE [db_name.]table_name
    CHANGE col_name col_name changed_column_type;
```

#### Parameter Description
| Parameter           | Description                                                                                               |
|---------------------|-----------------------------------------------------------------------------------------------------------|
| db_Name             | Name of the database. If this parameter is left unspecified, the current database is selected.            |
| table_name          | Name of the existing table.                                                                               |
| col_name            | Name of comma-separated column with data type. Column names contain letters, digits, and underscores (\_). |
| changed_column_type | The change in the data type.                                                                              |

#### Usage Guidelines

- Change of decimal data type from lower precision to higher precision will only be supported for cases where there is no data loss.

#### Valid Scenarios
- Invalid scenario - Change of decimal precision from (10,2) to (10,5) is invalid as in this case only scale is increased but total number of digits remains the same.

- Valid scenario - Change of decimal precision from (10,2) to (12,3) is valid as the total number of digits are increased by 2 but scale is increased only by 1 which will not lead to any data loss.

- Note :The allowed range is 38,38 (precision, scale) and is a valid upper case scenario which is not resulting in data loss.

#### Examples:
- **Changing data type of column a1 from INT to BIGINT**

```
   ALTER TABLE test_db.carbon CHANGE a1 a1 BIGINT;
```
- **Changing decimal precision of column a1 from 10 to 18.**

```
   ALTER TABLE test_db.carbon CHANGE a1 a1 DECIMAL(18,2);
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
columns to be used for bucketing and the number of buckets. For the selection of bucket the Hash value
of columns is used.

```
   CREATE TABLE [IF NOT EXISTS] [db_name.]table_name
                    [(col_name data_type, ...)]
   STORED BY 'carbondata'
   TBLPROPERTIES('BUCKETNUMBER'='noOfBuckets',
   'BUCKETCOLUMNS'='columnname')
```

### Parameter Description

| Parameter 	| Description 	| Optional 	|
|---------------	|------------------------------------------------------------------------------------------------------------------------------	|----------	|
| BUCKETNUMBER 	| Specifies the number of Buckets to be created. 	| No 	|
| BUCKETCOLUMNS 	| Specify the columns to be considered for Bucketing  	| No 	|

### Usage Guidelines

- The feature is supported for Spark 1.6.2 onwards, but the performance optimization is evident from Spark 2.1 onwards.

- Bucketing can not be performed for columns of Complex Data Types.

- Columns in the BUCKETCOLUMN parameter must be only dimension. The BUCKETCOLUMN parameter can not be a measure or a combination of measures and dimensions.


### Example:

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
   TBLPROPERTIES ('DICTIONARY_EXCLUDE'='productName',
                  'DICTIONARY_INCLUDE'='productNumber,saleQuantity',
                  'NO_INVERTED_INDEX'='productBatch',
                  'BUCKETNUMBER'='4',
                  'BUCKETCOLUMNS'='productName')
```

