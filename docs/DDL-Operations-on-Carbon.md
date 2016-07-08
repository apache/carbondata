
* [CREATE TABLE](#CREATE TABLE)
* [SHOW TABLE](#SHOW TABLE)
* [DROP TABLE](#DROP TABLE)
* [COMPACTION](#COMPACTION)

***


# CREATE TABLE
### Function
This command can be used to create carbon table by specifying the list of fields along with the table properties.

### Syntax

  ```ruby
  CREATE TABLE [IF NOT EXISTS] [db_name.]table_name 
               [(col_name data_type , ...)]               
         STORED BY 'org.apache.carbondata.format'
               [TBLPROPERTIES (property_name=property_value, ...)]
               // All Carbon's additional table options will go into properties
  ```
     
**Example:**

  ```ruby
  CREATE TABLE IF NOT EXISTS productSchema.productSalesTable (
                  productNumber Int,
                  productName String, 
                  storeCity String, 
                  storeProvince String, 
                  productCategory String, 
                  productBatch String,
                  saleQuantity Int,
                  revenue Int)       
       STORED BY 'org.apache.carbondata.format' 
       TBLPROPERTIES ('COLUMN_GROUPS'='(productName,productCategory)',
                     'DICTIONARY_EXCLUDE'='productName',
                     'DICTIONARY_INCLUDE'='productNumber'
                     'NO_INVERTED_INDEX'='productBatch')
  ```

### Parameter Description

| Parameter | Description |
| ------------- | -----|
| db_name | Name of the Database. Database name should consist of Alphanumeric characters and underscore(_) special character. |
| field_list | Comma separated List of fields with data type. The field names should consist of Alphanumeric characters and underscore(_) special character.|
|table_name | The name of the table in Database. Table Name should consist of Alphanumeric characters and underscore(_) special character. |
| STORED BY | "org.apache.carbondata.format", identifies and creates carbon table. |
| TBLPROPERTIES | List of carbon table properties. |

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
 - **Inverted Index Configuration**

   Inverted index is very useful to improve compression ratio and query speed, especially for those low-cardinality columns who are in reward position.
   By default inverted index will be enabled, but user can also set not to use inverted index for some columns.

  ```ruby
  TBLPROPERTIES ("NO_INVERTED_INDEX"="column1,column3")
  ```
Here, NO_INVERTED_INDEX will not use inverted index for the specified columns. This is applicable for high-cardinality columns and is a optional parameter.
### Scenarios
#### Create table by specifying schema

 The create table command is same as the Hive DDL. The Carbon's extra configurations are given as table properties.

  ```ruby
  CREATE TABLE [IF NOT EXISTS] [db_name.]table_name
               [(col_name data_type , ...)]
         STORED BY ‘org.carbondata.hive.CarbonHanlder’
               [TBLPROPERTIES (property_name=property_value ,...)]             
  ```
***

# SHOW TABLE
### Function
This command can be used to list all the tables in current database or all the tables of a specific database.

### Syntax

  ```ruby
  SHOW TABLES [IN db_Name];
  ```

**Example:**

  ```ruby
  SHOW TABLES IN ProductSchema;
  ```

### Parameter Description
| Parameter | Description |
|-----------|-------------|
| IN db_Name | Name of the database. Required only if tables of this specific database are to be listed. |

### Usage Guideline
IN db_Name is optional.

### Scenarios
NA

***

# DROP TABLE
### Function
This command can be used to delete the existing table.

### Syntax

  ```ruby
  DROP TABLE [IF EXISTS] [db_name.]table_name;
  ```

**Example:**

  ```ruby
  DROP TABLE IF EXISTS productSchema.productSalesTable;
  ```

### Parameter Description
| Parameter | Description |
|-----------|-------------|
| db_Name | Name of the database. If not specified, current database will be selected. |
| table_name | Name of the table to be deleted. |

### Usage Guideline
In this command IF EXISTS and db_name are optional.

### Scenarios
NA

***

# COMPACTION
### Function
 This command will merge the specified number of segments into one segment. This will enhance the query performance of the table.

### Syntax

  ```ruby
  ALTER TABLE [db_name.]table_name COMPACT 'MINOR/MAJOR'
  ```

**Example:**

  ```ruby
  ALTER TABLE carbontable COMPACT MINOR
  ALTER TABLE carbontable COMPACT MAJOR
  ```

### Parameter Description

| Parameter | Description |
| ------------- | -----|
| db_name | Database name, if it is not specified then it uses current database. |
| table_name | The name of the table in provided database.|
 

### Usage Guideline
NA

### Scenarios
NA

***