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

# CarbonData Secondary Index

* [Quick Example](#quick-example)
* [Secondary Index Table](#Secondary-Index-Introduction)
* [Loading Data](#loading-data)
* [Querying Data](#querying-data)
* [Compaction](#compacting-SI-table)
* [DDLs on Secondary Index](#DDLs-on-Secondary-Index)

## Quick example

Start spark-sql in terminal and run the following queries,
```
CREATE TABLE maintable(a int, b string, c string) stored as carbondata;
insert into maintable select 1, 'ab', 'cd';
CREATE index index1 on table maintable(c) AS 'carbondata';
SELECT a from maintable where c = 'cd';
// NOTE: run explain query and check if query hits the SI table from the plan
EXPLAIN SELECT a from maintable where c = 'cd';
```

## Secondary Index Introduction
  Secondary index tables are created as indexes and managed as child tables internally by
  Carbondata. Users can create a secondary index based on the column position in the main table(Recommended
  for right columns) and the queries should have filter on that column to improve the filter query
  performance.
  
  Data refresh to the secondary index is always automatic. Once SI table is created, Carbondata's 
  CarbonOptimizer with the help of `CarbonSITransformationRule`, transforms the query plan to hit the
  SI table based on the filter condition or set of filter conditions present in the query.
  So the first level of pruning will be done on the SI table as it stores blocklets and main table/parent
  table pruning will be based on the SI output, which helps in giving the faster query results with
  better pruning.

  Secondary Index table can be created with the below syntax

   ```
   CREATE INDEX [IF NOT EXISTS] index_name
   ON TABLE maintable(index_column)
   AS
   'carbondata'
   [PROPERTIES('table_blocksize'='1')]
   ```
> NOTE: Keywords given inside `[]` is optional.

  For instance, main table called **sales** which is defined as

  ```
  CREATE TABLE sales (
    order_time timestamp,
    user_id string,
    sex string,
    country string,
    quantity int,
    price bigint)
  STORED AS carbondata
  ```

  User can create SI table using the Create Index DDL

  ```
  CREATE INDEX index_sales
  ON TABLE sales(user_id)
  AS
  'carbondata'
  PROPERTIES('table_blocksize'='1')
  ```
 
 
#### How SI tables are selected

When a user executes a filter query, during the query planning phase, CarbonData with the help of
`CarbonSITransformationRule`, checks if there are any index tables present on the filter column of
query. If there are any, then the filter query plan will be transformed in such a way that execution will
first hit the corresponding SI table and give input to the main table for further pruning.


For the main table **sales** and SI table  **index_sales** created above, following queries
```
SELECT country, sex from sales where user_id = 'xxx'

SELECT country, sex from sales where user_id = 'xxx' and country = 'INDIA'
```

will be transformed by CarbonData's `CarbonSITransformationRule` to query against SI table
**index_sales** first which will be input to the main table **sales**


## Loading data

### Loading data to Secondary Index table(s).

*case1:* When the SI table is created and the main table does not have any data. In this case every
consecutive load to the main table, will load data to the SI table once the main table data load is finished.

*case2:* When the SI table is created and the main table already contains some data, then SI creation will
also load data to the SI table with the same number of segments as the main table. Thereafter, consecutive load to
the main table will also load data to the SI table.

 **NOTE**:
 * In case of data load failure to the SI table, then we make the SI table disable by setting a hive serde
 property. The subsequent main table load will load the old failed loads along with current load and
 makes the SI table enable and available for query.

## Querying data
Direct query can be made on SI tables to check the data present in position reference columns.
When a filter query is fired, and if the filter column is a secondary index column, then plan is
transformed accordingly to hit the SI table first to make better pruning with the main table and in turn
helps for faster query results.

Users can verify whether a query can leverage the SI table or not by executing the `EXPLAIN`
command, which will show the transformed logical plan, and thus users can check whether the SI table
is selected.


## Compacting SI table

### Compacting SI table table through Main Table compaction
Running Compaction command (`ALTER TABLE COMPACT`)[COMPACTION TYPE-> MINOR/MAJOR] on main table will
automatically delete all the old segments of SI and creates a new segment with same name as main
table compacted segment and loads data to it.

### Compacting SI table's individual segment(s) through REFRESH INDEX command
Where there are so many small files present in the SI table, then we can use the REFRESH INDEX command to
compact the files within an SI segment to avoid many small files.

  ```
  REFRESH INDEX sales_index ON TABLE sales
  ```
This command merges data files in each segment of the SI table.

  ```
  REFRESH INDEX sales_index ON TABLE sales WHERE SEGMENT.ID IN(1)
  ```
This command merges data files within a specified segment of the SI table.

## How to skip Secondary Index?
When Secondary indexes are created on a table(s), data fetching happens from secondary
indexes created on the main tables for better performance. But sometimes, data fetching from the
secondary index might degrade query performance in cases where the data is sparse and most of the
blocklets need to be scanned. So to avoid such secondary indexes, we use NI as a function on filters
within WHERE clause.

  ```
  SELECT country, sex from sales where NI(user_id = 'xxx')
  ```
The above query ignores column `user_id` from the secondary index and fetches data from the main table.

## DDLs on Secondary Index

### Show index Command
This command is used to get information about all the secondary indexes on a table.

Syntax
  ```
  SHOW INDEXES ON [TABLE] [db_name.]table_name
  ```

### Drop index Command
This command is used to drop an existing secondary index on a table

Syntax
  ```
  DROP INDEX [IF EXISTS] index_name ON [TABLE] [db_name.]table_name
  ```

### Register index Command
This command registers the secondary index with the main table in case of compatibility scenarios 
where we have old stores.

Syntax
  ```
  REGISTER INDEX TABLE index_name ON [TABLE] [db_name.]table_name
  ```