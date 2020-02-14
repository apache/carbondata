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

# CarbonData MV DataMap

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
CREATE DATAMAP datamap_1 on table maintable as SELECT a, sum(b) from maintable group by a;
SELECT a, sum(b) from maintable group by a;
// NOTE: run explain query and check if query hits the datamap table from the plan
EXPLAIN SELECT a, sum(b) from maintable group by a;
```

## MV DataMap Introduction
  MV tables are created as DataMaps and managed as tables internally by CarbonData. User can create
  limitless MV datamaps on a table to improve query performance provided the storage requirements
  and loading time is acceptable.

  MV datamap can be a lazy or a non-lazy datamap. Once MV datamaps are created, CarbonData's
  CarbonAnalyzer helps to select the most efficient MV datamap based on the user query and rewrite
  the SQL to select the data from MV datamap instead of main table. Since the data size of MV
  datamap is smaller and data is pre-processed, user queries are much faster.

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

  User can create MV tables using the Create DataMap DDL

  ```
  CREATE DATAMAP agg_sales
  ON TABLE sales
  USING "MV"
  DMPROPERTIES('TABLE_BLOCKSIZE'='256 MB','LOCAL_DICTIONARY_ENABLE'='false')
  AS
    SELECT country, sex, sum(quantity), avg(price)
    FROM sales
    GROUP BY country, sex
  ```
 **NOTE**:
 * Group by columns has to be provided in projection list while creating mv datamap
 * If only single parent table is involved in mv datamap creation, then TableProperties of Parent table
   (if not present in a aggregate function like sum(col)) listed below will be
   inherited to datamap table
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

 * Creating MV datamap with select query containing only project of all columns of maintable is unsupported 
      
   **Example:**
   If table 'x' contains columns 'a,b,c',
   then creating MV datamap with below queries is not supported.
   
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
mv datamap table for this query will be selected among the candidates.

For the main table **sales** and mv table  **agg_sales** created above, following queries
```
SELECT country, sex, sum(quantity), avg(price) from sales GROUP BY country, sex

SELECT sex, sum(quantity) from sales GROUP BY sex

SELECT avg(price), country from sales GROUP BY country
```

will be transformed by CarbonData's query planner to query against mv table
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

### Loading data to Non-Lazy MV Datamap

In case of WITHOUT DEFERRED REBUILD, for existing table with loaded data, data load to MV table will
be triggered by the CREATE DATAMAP statement when user creates the MV table.
For incremental loads to main table, data to datamap will be loaded once the corresponding main
table load is completed.

### Loading data to Lazy MV Datamap

In case of WITH DEFERRED REBUILD, data load to MV table will be triggered by the [Manual Refresh](./datamap-management.md#manual-refresh)
command. MV datamap will be in DISABLED state in below scenarios,
  * when mv datamap is created
  * when data of main table and datamap are not in sync

User should fire REBUILD DATAMAP command to sync all segments of main table with datamap table and
which ENABLES the datamap for query

### Loading data to Multiple MV's
During load to main table, if anyone of the load to datamap table fails, then that corresponding
datamap will be DISABLED and load to other datamaps mapped to main table will continue. User can
fire REBUILD DATAMAP command to sync or else the subsequent table load will load the old failed
loads along with current load and enable the disabled datamap.

 **NOTE**:
 * In case of InsertOverwrite/Update operation on parent table, all segments of datamap table will
   be MARKED_FOR_DELETE and reload to datamap table will happen by REBUILD DATAMAP, in case of Lazy
   mv datamap/ once InsertOverwrite/Update operation on parent table is finished, in case of
   Non-Lazy mv.
 * In case of full scan query, Data Size and Index Size of main table and child table will not the
   same, as main table and child table has different column names.

## Querying data
As a technique for query acceleration, MV tables cannot be queried directly.
Queries are to be made on main table. While doing query planning, internally CarbonData will check
associated mv datamap tables with the main table, and do query plan transformation accordingly.

User can verify whether a query can leverage mv datamap table or not by executing `EXPLAIN`
command, which will show the transformed logical plan, and thus user can check whether mv datamap
table is selected.


## Compacting MV datamap

### Compacting MV datamap table through Main Table compaction
Running Compaction command (`ALTER TABLE COMPACT`)[COMPACTION TYPE-> MINOR/MAJOR] on main table will
automatically compact the mv datamap tables created on the main table, once compaction on main table
is done.

### Compacting MV datamap table through DDL command
Compaction on mv datamap can be triggered by running the following DDL command(supported only for mv).
  ```
  ALTER DATAMAP datamap_name COMPACT 'COMPACTION_TYPE'
  ```

## Data Management with mv tables
In current implementation, data consistency needs to be maintained for both main table and mv datamap
tables. Once there is mv datamap table created on the main table, following command on the main
table is not supported:
1. Data management command: `DELETE SEGMENT`.
2. Schema management command: `ALTER TABLE DROP COLUMN`, `ALTER TABLE CHANGE DATATYPE`,
   `ALTER TABLE RENAME`, `ALTER COLUMN RENAME`. Note that adding a new column is supported, and for
   dropping columns and change datatype command, CarbonData will check whether it will impact the
   mv datamap table, if not, the operation is allowed, otherwise operation will be rejected by
   throwing exception.
3. Partition management command: `ALTER TABLE ADD/DROP PARTITION`. Note that dropping a partition
   will be allowed only if partition is participating in all datamaps associated with main table.
   Drop Partition is not allowed, if any mv datamap is associated with more than one parent table.
   Drop Partition directly on datamap table is not allowed.
4. Complex Datatype's for mv datamap is not supported.

However, there is still way to support these operations on main table, in current CarbonData
release, user can do as following:
1. Remove the mv datamap table by `DROP DATAMAP` command
2. Carry out the data management operation on main table
3. Create the mv datamap table again by `CREATE DATAMAP` command
Basically, user can manually trigger the operation by re-building the datamap.

## MV TimeSeries Support
MV non-lazy datamap supports TimeSeries queries. Supported granularity strings are: year, month, week, day,
hour,thirty_minute, fifteen_minute, ten_minute, five_minute, minute and second.

 User can create MV datamap with timeseries queries like the below example:

  ```
  CREATE DATAMAP agg_sales
  ON TABLE sales
  USING "MV"
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
  MV Timeseries queries can be rolledUp from existing mv datamaps.
  ### Query RollUp
 Consider an example where the query is on hour level granularity, but the datamap
 of hour is not present but  minute level datamap is present, then we can get the data
 from minute level and the aggregate the hour level data and give output.
 This is called query rollup.
 
 Consider if user create's below timeseries datamap,
   ```
   CREATE DATAMAP agg_sales
   ON TABLE sales
   USING "MV"
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
 Then, the above query can be rolled up from 'agg_sales' mv datamap, by adding hour
 level timeseries aggregation on minute level datamap. Users can fire explain command
 to check if query is rolled up from existing mv datamaps.
 
  **NOTE**:
  1. Queries cannot be rolled up, if filter contains timeseries function.
  2. RollUp is not yet supported for queries having join clause or order by functions.