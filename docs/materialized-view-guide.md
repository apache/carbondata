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

# CarbonData Materialized View

* [Quick Example](#quick-example)
* [MV DataMap](#mv-datamap-introduction)
* [Loading Data](#loading-data)
* [Querying Data](#querying-data)
* [Compaction](#compacting-mv-datamap)
* [Data Management](#data-management-with-mv-tables)
* [MV TimeSeries Support](#mv-timeseries-support)
* [MV TimeSeries RollUp Support](#mv-timeseries-rollup-support)

## Quick example

Start spark-sql in terminal and run the following queries,
```
CREATE TABLE maintable(a int, b string, c int) stored as carbondata;
insert into maintable select 1, 'ab', 2;
CREATE MATERIALIZED VIEW mv1 as SELECT a, sum(b) from maintable group by a;
SELECT a, sum(b) from maintable group by a;
// NOTE: run explain query and check if query hits the MV table from the plan
EXPLAIN SELECT a, sum(b) from maintable group by a;
```

## Materialized View Introduction
  MV tables are created as MV and managed as tables internally by CarbonData. User can create
  limitless MV on a table to improve query performance provided the storage requirements
  and loading time is acceptable.

  MV can be lazy or non-lazy. Once MV are created, CarbonData's
  CarbonAnalyzer helps to select the most efficient MV based on the user query and rewrite
  the SQL to select the data from MV instead of main table. Since the data size of MV
  is smaller and data is pre-processed, user queries are much faster.

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

  User can create MV tables using

  ```
  CREATE MATERIALIZED VIEW agg_sales
  PROPERTIES('TABLE_BLOCKSIZE'='256 MB','LOCAL_DICTIONARY_ENABLE'='false')
  AS
    SELECT country, sex, sum(quantity), avg(price)
    FROM sales
    GROUP BY country, sex
  ```
 **NOTE**:
 * Group by columns has to be provided in projection list while creating MV
 * If only single parent table is involved in MV creation, then TableProperties of Parent table
   (if not present in a aggregate function like sum(col)) listed below will be
   inherited to MV table
    1. SORT_COLUMNS
    2. SORT_SCOPE
    3. TABLE_BLOCKSIZE
    4. FLAT_FOLDER
    5. LONG_STRING_COLUMNS
    6. LOCAL_DICTIONARY_ENABLE
    7. LOCAL_DICTIONARY_THRESHOLD
    8. LOCAL_DICTIONARY_EXCLUDE
    9. INVERTED_INDEX
   10. NO_INVERTED_INDEX
   11. COLUMN_COMPRESSOR

 * Creating MV with select query containing only project of all columns of maintable is unsupported 
      
   **Example:**
   If table 'x' contains columns 'a,b,c',
   then creating MV with below queries is not supported.
   
   1. ```select a,b,c from x```
   2. ```select * from x```
 * TableProperties can be provided in DMProperties excluding LOCAL_DICTIONARY_INCLUDE,
   LOCAL_DICTIONARY_EXCLUDE, INVERTED_INDEX,
   NO_INVERTED_INDEX, SORT_COLUMNS, LONG_STRING_COLUMNS, RANGE_COLUMN & COLUMN_META_CACHE
 * TableProperty given in DMProperties will be considered for mv creation, eventhough if same
   property is inherited from parent table, which allows user to provide different tableproperties
   for child table
 * MV creation with limit or union all ctas queries is unsupported
 * MV does not support Streaming

#### How MV tables are selected

When a user query is submitted, during query planning phase, CarbonData will collect modular plan
candidates and process the the ModularPlan based on registered summary data sets. Then,
MV table for this query will be selected among the candidates.

For the main table **sales** and mv table  **agg_sales** created above, following queries
```
SELECT country, sex, sum(quantity), avg(price) from sales GROUP BY country, sex

SELECT sex, sum(quantity) from sales GROUP BY sex

SELECT avg(price), country from sales GROUP BY country
```

will be transformed by CarbonData's query planner to query against MV table
**agg_sales** instead of the main table **sales**

However, for following queries
```
SELECT user_id, country, sex, sum(quantity), avg(price) from sales GROUP BY user_id, country, sex

SELECT sex, avg(quantity) from sales GROUP BY sex

SELECT country, max(price) from sales GROUP BY country
```

will query against main table **sales** only, because it does not satisfy mv table
selection logic.

## Loading data

### Loading data to Non-Lazy MV

In case of WITHOUT DEFERRED REFRESH, for existing table with loaded data, data load to MV table will
be triggered by the CREATE MATERIALIZED VIEW statement when user creates the MV table.
For incremental loads to main table, data to MV will be loaded once the corresponding main
table load is completed.

### Loading data to Lazy MV

In case of WITH DEFERRED REFRESH, data load to MV table will be triggered by the [Manual Refresh](index-management.md#manual-refresh)
command. MV will be in DISABLED state in below scenarios,
  * when MV is created
  * when data of main table and MV are not in sync

User should fire REFRESH MATERIALIZED VIEW command to sync all segments of main table with MV table and
which ENABLES the MV for query

### Loading data to Multiple MV's
During load to main table, if anyone of the load to MV table fails, then that corresponding
MV will be DISABLED and load to other MVs mapped to main table will continue. User can
fire REFRESH MATERIALIZED VIEW command to sync or else the subsequent table load will load the old failed
loads along with current load and enable the disabled MV.

 **NOTE**:
 * In case of InsertOverwrite/Update operation on parent table, all segments of MV table will
   be MARKED_FOR_DELETE and reload to MV table will happen by REFRESH MATERIALIZED VIEW, in case of Lazy
   MV once InsertOverwrite/Update operation on parent table is finished, in case of
   Non-Lazy MV.
 * In case of full scan query, Data Size and Index Size of main table and child table will not the
   same, as main table and child table has different column names.

## Querying data
As a technique for query acceleration, MV tables cannot be queried directly.
Queries are to be made on main table. While doing query planning, internally CarbonData will check
associated MV tables with the main table, and do query plan transformation accordingly.

User can verify whether a query can leverage MV table or not by executing `EXPLAIN`
command, which will show the transformed logical plan, and thus user can check whether MV
table is selected.


## Compacting MV

### Compacting MV table through Main Table compaction
Running Compaction command (`ALTER TABLE COMPACT`)[COMPACTION TYPE-> MINOR/MAJOR] on main table will
automatically compact the MV tables created on the main table, once compaction on main table
is done.

### Compacting MV table through DDL command
Compaction on MV can be triggered by running the following DDL command(supported only for mv).
  ```
  ALTER TABLE mvname_table COMPACT 'COMPACTION_TYPE'
  ```

## Data Management with MV tables
In current implementation, data consistency needs to be maintained for both main table and MV
tables. Once there is MV table created on the main table, following command on the main
table is not supported:
1. Data management command: `DELETE SEGMENT`.
2. Schema management command: `ALTER TABLE DROP COLUMN`, `ALTER TABLE CHANGE DATATYPE`,
   `ALTER TABLE RENAME`, `ALTER COLUMN RENAME`. Note that adding a new column is supported, and for
   dropping columns and change datatype command, CarbonData will check whether it will impact the
   MV table, if not, the operation is allowed, otherwise operation will be rejected by
   throwing exception.
3. Partition management command: `ALTER TABLE ADD/DROP PARTITION`. Note that dropping a partition
   will be allowed only if partition is participating in all MVs associated with main table.
   Drop Partition is not allowed, if any MV is associated with more than one parent table.
   Drop Partition directly on MV table is not allowed.
4. Complex Datatype's for MV is not supported.

However, there is still way to support these operations on main table, in current CarbonData
release, user can do as following:
1. Remove the MV table by `DROP MATERIALIZED VIEW` command
2. Carry out the data management operation on main table
3. Create the MV table again by `CREATE MATERIALIZED VIEW` command
Basically, user can manually trigger the operation by refreshing the MV.

## MV TimeSeries Support
MV non-lazy supports TimeSeries queries. Supported granularity strings are: year, month, week, day,
hour,thirty_minute, fifteen_minute, ten_minute, five_minute, minute and second.

 User can create MV with timeseries queries like the below example:

  ```
  CREATE MATERIALIZED VIEW agg_sales
  AS
    SELECT timeseries(order_time,'second'),avg(price)
    FROM sales
    GROUP BY timeseries(order_time,'second')
  ```
Supported columns that can be provided in timeseries udf should be of TimeStamp/Date type.
Timeseries queries with Date type support's only year, month, day and week granularities.

 **NOTE**:
 1. Single select statement cannot contain timeseries udf(s) neither with different granularity nor
 with different timestamp/date columns.
 
 ## MV TimeSeries RollUp Support
  MV Timeseries queries can be rolledUp from existing MV.
  ### Query RollUp
 Consider an example where the query is on hour level granularity, but the MV
 of hour is not present but  minute level MV is present, then we can get the data
 from minute level and the aggregate the hour level data and give output.
 This is called query rollup.
 
 Consider if user create's below timeseries MV,
   ```
   CREATE MATERIALIZED VIEW agg_sales
   AS
     SELECT timeseries(order_time,'minute'),avg(price)
     FROM sales
     GROUP BY timeseries(order_time,'minute')
   ```
 and fires the below query with hour level granularity.
   ```
    SELECT timeseries(order_time,'hour'),avg(price)
    FROM sales
    GROUP BY timeseries(order_time,'hour')
   ```
 Then, the above query can be rolled up from 'agg_sales' MV, by adding hour
 level timeseries aggregation on minute level MV. Users can fire explain command
 to check if query is rolled up from existing MV.
 
  **NOTE**:
  1. Queries cannot be rolled up, if filter contains timeseries function.
  2. RollUp is not yet supported for queries having join clause or order by functions.