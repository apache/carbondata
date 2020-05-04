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
* [Introduction](#introduction)
* [Loading Data](#loading-data)
* [Querying Data](#querying-data)
* [Compaction](#compacting)
* [Data Management](#data-management)
* [Time Series Support](#time-series-support)
* [Time Series RollUp Support](#time-series-rollup-support)

## Quick example

 Start spark-sql in terminal and run the following queries,

   ```
     CREATE TABLE maintable(a int, b string, c int) stored as carbondata;
     INSERT INTO maintable SELECT 1, 'ab', 2;
     CREATE MATERIALIZED VIEW view1 AS SELECT a, sum(b) FROM maintable GROUP BY a;
     SELECT a, sum(b) FROM maintable GROUP BY a;
     // NOTE: run explain query and check if query hits the mv table from the plan
     EXPLAIN SELECT a, sum(b) FROM maintable GROUP BY a;
   ```

## Introduction

 Materialized views are created as tables from queries. Users can create limitless materialized views 
 to improve query performance provided the storage requirements and loading time is acceptable.
 
 Materialized view can be refreshed on commit or on manual. Once materialized views are created, 
 CarbonData's `MVRewriteRule` helps to select the most efficient materialized view based on 
 the user query and rewrite the SQL to select the data from materialized view instead of 
 fact tables. Since the data size of materialized view is smaller and data is pre-processed, 
 user queries are much faster.
 
 For instance, fact table called **sales** which is defined as.
 
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

 Users can create a materialized view using the CREATE MATERIALIZED VIEW statement.
 
   ```
     CREATE MATERIALIZED VIEW agg_sales
     PROPERTIES('TABLE_BLOCKSIZE'='256 MB','LOCAL_DICTIONARY_ENABLE'='false')
     AS
       SELECT country, sex, sum(quantity), avg(price)
       FROM sales
       GROUP BY country, sex
   ```

 **NOTE**:
   * Group by and Order by columns has to be provided in the projection list while creating a materialized view.
   * If only single fact table is involved in materialized view creation, then TableProperties of 
     fact table (if not present in a aggregate function like sum(col)) listed below will be 
     inherited to materialized view.
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
   * Creating materialized view with select query containing only project of all columns of fact 
     table is unsupported.
     **Example:**
       If table 'x' contains columns 'a,b,c', then creating MV with below queries is not supported.
         1. ```SELECT a,b,c FROM x```
         2. ```SELECT * FROM x```
   * TableProperties can be provided in Properties excluding LOCAL_DICTIONARY_INCLUDE,
     LOCAL_DICTIONARY_EXCLUDE, INVERTED_INDEX, NO_INVERTED_INDEX, SORT_COLUMNS, LONG_STRING_COLUMNS, 
     RANGE_COLUMN & COLUMN_META_CACHE.
   * TableProperty given in Properties will be considered for materialized view creation, even though 
     if same property is inherited from fact table, which allows user to provide different table 
     properties for materialized view.
   * Materialized view creation with limit or union all CTAS queries is unsupported.
   * Materialized view does not support streaming.

#### How materialized views are selected

 When a user query is submitted, during the query planning phase, CarbonData will collect modular plan
 candidates and process the ModularPlan based on registered summary data sets. Then,
 a materialized view for this query will be selected among the candidates.

 For the fact table **sales** and materialized view **agg_sales** created above, following queries
   ```
     SELECT country, sex, sum(quantity), avg(price) FROM sales GROUP BY country, sex
     SELECT sex, sum(quantity) FROM sales GROUP BY sex
     SELECT avg(price), country FROM sales GROUP BY country
   ```

 will be transformed by CarbonData's query planner to query against materialized view **agg_sales** 
 instead of the fact table **sales**.
 
 However, for following queries

   ```
     SELECT user_id, country, sex, sum(quantity), avg(price) FROM sales GROUP BY user_id, country, sex
     SELECT sex, avg(quantity) FROM sales GROUP BY sex
     SELECT country, max(price) FROM sales GROUP BY country
   ```

 will query against fact table **sales** only, because it does not satisfy materialized view
 selection logic.

## Loading data

### Loading data on commit

 In case of WITHOUT DEFERRED REFRESH, for existing table with loaded data, data load to materialized 
 view will be triggered by the CREATE MATERIALIZED VIEW statement when user creates the materialized 
 view.

 For incremental loads to the fact table, data to materialized view will be loaded once the 
 corresponding fact table load is completed.

### Loading data on manual

 In case of WITH DEFERRED REFRESH, data load to materialized view will be triggered by the refresh 
 command. Materialized view will be in DISABLED state in below scenarios.

   * when a materialized view is created.
   * when data of fact table and materialized view are not in sync.
  
 User should fire REFRESH MATERIALIZED VIEW command to sync all segments of fact table with 
 materialized view, which ENABLES the materialized view for query.

 Command example:
   ```
     REFRESH MATERIALIZED VIEW agg_sales
   ```

### Loading data to multiple materialized views

 During load to fact table, if anyone of the load to materialized view fails, then that 
 corresponding materialized view will be DISABLED and load to other materialized views mapped 
 to the fact table will continue. 

 User can fire REFRESH MATERIALIZED VIEW command to sync or else the subsequent table load 
 will load the old failed loads along with current load and enable the disabled materialized view.

 **NOTE**:
   * In case of InsertOverwrite/Update operation on fact table, all segments of materialized view 
     will be MARKED_FOR_DELETE and reload to mv table will happen by REFRESH MATERIALIZED VIEW, 
     in case of materialized view which refresh on manual and once the InsertOverwrite/Update 
     operation on fact table is finished, in case of materialized view which refresh on commit.
   * In case of full scan query, Data Size and Index Size of fact table and materialized view 
     will not be the same, as fact table and materialized view have different column names.

## Querying data

 Queries are to be made on the fact table. While doing query planning, internally CarbonData will check
 for the materialized views which are associated with the fact table, and do query plan 
 transformation accordingly.
 
 Users can verify whether a query can leverage materialized view or not by executing the `EXPLAIN` command, 
 which will show the transformed logical plan, and thus the user can check whether a materialized view 
 is selected.

## Compacting

 Running Compaction command (`ALTER TABLE COMPACT`)[COMPACTION TYPE-> MINOR/MAJOR] on fact table 
 will automatically compact the materialized view created on the fact table, once compaction 
 on fact table is done.

## Data Management

 In current implementation, data consistency needs to be maintained for both fact table and 
 materialized views. 
 
 Once there is materialized view created on the fact table, following command on the fact
 table is not supported:
 
   1. Data management command: `DELETE SEGMENT`.
   2. Schema management command: `ALTER TABLE DROP COLUMN`, `ALTER TABLE CHANGE DATATYPE`,
      `ALTER TABLE RENAME`, `ALTER COLUMN RENAME`. Note that adding a new column is supported, and for
      dropping columns and change datatype command, CarbonData will check whether it will impact the
      materialized view, if not, the operation is allowed, otherwise operation will be rejected by
      throwing exception.
   3. Partition management command: `ALTER TABLE ADD/DROP PARTITION`. Note that dropping a partition
      will be allowed only if the partition column of fact table is participating in all of the table's materialized views.
      Drop Partition is not allowed, if any materialized view is associated with more than one 
      fact table. Drop Partition directly on materialized view is not allowed.
   4. Complex Datatype's for materialized view is not supported.
   
 However, there is still way to support these operations on fact table, in current CarbonData
 release, user can do as following:
 
   1. Remove the materialized view by `DROP MATERIALIZED VIEW` command.
   2. Carry out the data management operation on fact table.
   3. Create the materialized view again by `CREATE MATERIALIZED VIEW` command.
   
 Basically, user can manually trigger the operation by re-building the materialized view.

## Time Series Support

 Time series data are simply measurements or events that are tracked, monitored, down sampled, and 
 aggregated over time. Materialized views with automatic refresh mode supports TimeSeries queries.

 CarbonData provides built-in time-series udf with the below definition.

   ```
     timeseries(event_time_column, 'granularity')
   ```

 Event time columns provided in time series udf should be of TimeStamp/Date type.

 Below table describes the time hierarchy and levels that can be provided in a time-series udf, 
 so that it supports automatic roll-up in time dimension for query.

 | Granularity    | Description                                           |
 |----------------|-------------------------------------------------------|
 | year           | Data will be aggregated over year                     |
 | month          | Data will be aggregated over month                    | 
 | week           | Data will be aggregated over week                     |
 | day            | Data will be aggregated over day                      |
 | hour           | Data will be aggregated over hour                     |
 | thirty_minute  | Data will be aggregated over every thirty minutes     |
 | fifteen_minute | Data will be aggregated over every fifteen minutes    |
 | ten_minute     | Data will be aggregated over every ten minutes        |
 | five_minute    | Data will be aggregated over every five minutes       |
 | minute         | Data will be aggregated over every one minute         |
 | second         | Data will be aggregated over every second             |

 Time series udf having column as Date type support's only year, month, day and week granularities.

 Below is the sample data loaded to the fact table **sales**.
  
   ```
     order_time,          user_id, sex,    country, quantity, price
     2016-02-23 09:01:30, c001,    male,   xxx,     100,      2
     2016-02-23 09:01:50, c002,    male,   yyy,     200,      5
     2016-02-23 09:03:30, c003,    female, xxx,     400,      1
     2016-02-23 09:03:50, c004,    male,   yyy,     300,      5
     2016-02-23 09:07:50, c005,    female, xxx,     500,      5
   ```

 Users can create materialized views with time series queries like the below example:

   ```
     CREATE MATERIALIZED VIEW agg_sales AS
     SELECT timeseries(order_time, 'minute'),avg(price)
     FROM sales
     GROUP BY timeseries(order_time, 'minute')
   ```
 And execute the below query to check time series data. In this example, a materialized view of 
 the aggregated table on the price column will be created, which will be aggregated every one minute.
  
   ```
     SELECT timeseries(order_time,'minute'), avg(price)
     FROM sales
     GROUP BY timeseries(order_time,'minute')
   ```
 Find below the result of the above query aggregated over a minute.
 
   ```
     +---------------------------------------+----------------+
     |UDF:timeseries(order_time, minute)     |avg(price)      |
     +---------------------------------------+----------------+
     |2016-02-23 09:01:00                    |3.5             |
     |2016-02-23 09:07:00                    |5.0             |
     |2016-02-23 09:03:00                    |3.0             |
     +---------------------------------------+----------------+
   ```

 The data loading, querying, compaction command and its behavior is the same as materialized views.

#### How data is aggregated over time?

 On each load to materialized view, data will be aggregated based on the specified time interval of 
 granularity provided during creation and stored on each segment.
 
 **NOTE**:
   1. Retention policies for time series is not supported yet.
 
## Time Series RollUp Support

 Time series queries can be rolled up from an existing materialized view.
 
### Query RollUp

 Consider an example where the query is on hour level granularity, but the materialized view
 with hour level granularity is not present but materialized view with minute level granularity is 
 present, then we can get the data from minute level and aggregate the hour level data and 
 give output. This is called query rollup.
 
 Consider if user create's below time series materialized view,
 
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

 Then, the above query can be rolled up from materialized view 'agg_sales', by adding hour
 level time series aggregation on minute level aggregation. Users can fire the `EXPLAIN` command
 to check if a query is rolled up from an existing materialized view.
 
  **NOTE**:
    1. Queries cannot be rolled up, if the filter contains a time series function.
    2. Roll up is not yet supported for queries having join clause or order by functions.
  