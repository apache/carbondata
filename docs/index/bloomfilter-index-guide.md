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

# CarbonData BloomFilter DataMap

* [DataMap Management](#datamap-management)
* [BloomFilter Datamap Introduction](#bloomfilter-datamap-introduction)
* [Loading Data](#loading-data)
* [Querying Data](#querying-data)
* [Data Management](#data-management-with-bloomfilter-datamap)
* [Useful Tips](#useful-tips)

#### DataMap Management
Creating BloomFilter DataMap
  ```
  CREATE DATAMAP [IF NOT EXISTS] datamap_name
  ON TABLE main_table
  USING 'bloomfilter'
  DMPROPERTIES ('index_columns'='city, name', 'BLOOM_SIZE'='640000', 'BLOOM_FPP'='0.00001')
  ```

Dropping Specified DataMap
  ```
  DROP DATAMAP [IF EXISTS] datamap_name
  ON TABLE main_table
  ```

Showing all DataMaps on this table
  ```
  SHOW DATAMAP
  ON TABLE main_table
  ```

Disable DataMap
> The datamap by default is enabled. To support tuning on query, we can disable a specific datamap during query to observe whether we can gain performance enhancement from it. This is effective only for current session.

  ```
  // disable the datamap
  SET carbon.datamap.visible.dbName.tableName.dataMapName = false
  // enable the datamap
  SET carbon.datamap.visible.dbName.tableName.dataMapName = true
  ```


## BloomFilter DataMap Introduction
A Bloom filter is a space-efficient probabilistic data structure that is used to test whether an element is a member of a set.
Carbondata introduced BloomFilter as an index datamap to enhance the performance of querying with precise value.
It is well suitable for queries that do precise match on high cardinality columns(such as Name/ID).
Internally, CarbonData maintains a BloomFilter per blocklet for each index column to indicate that whether a value of the column is in this blocklet.
Just like the other datamaps, BloomFilter datamap is managed along with main tables by CarbonData.
User can create BloomFilter datamap on specified columns with specified BloomFilter configurations such as size and probability.

For instance, main table called **datamap_test** which is defined as:

  ```
  CREATE TABLE datamap_test (
    id string,
    name string,
    age int,
    city string,
    country string)
  STORED AS carbondata
  TBLPROPERTIES('SORT_COLUMNS'='id')
  ```

In the above example, `id` and `name` are high cardinality columns
and we always query on `id` and `name` with precise value.
since `id` is in the sort_columns and it is orderd,
query on it will be fast because CarbonData can skip all the irrelative blocklets.
But queries on `name` may be bad since the blocklet minmax may not help,
because in each blocklet the range of the value of `name` may be the same -- all from A* to z*.
In this case, user can create a BloomFilter DataMap on column `name`.
Moreover, user can also create a BloomFilter DataMap on the sort_columns.
This is useful if user has too many segments and the range of the value of sort_columns are almost the same.

User can create BloomFilter DataMap using the Create DataMap DDL:

  ```
  CREATE DATAMAP dm
  ON TABLE datamap_test
  USING 'bloomfilter'
  DMPROPERTIES ('INDEX_COLUMNS' = 'name,id', 'BLOOM_SIZE'='640000', 'BLOOM_FPP'='0.00001', 'BLOOM_COMPRESS'='true')
  ```

**Properties for BloomFilter DataMap**

| Property | Is Required | Default Value | Description |
|-------------|----------|--------|---------|
| INDEX_COLUMNS | YES |  | Carbondata will generate BloomFilter index on these columns. Queries on these columns are usually like 'COL = VAL'. |
| BLOOM_SIZE | NO | 640000 | This value is internally used by BloomFilter as the number of expected insertions, it will affect the size of BloomFilter index. Since each blocklet has a BloomFilter here, so the default value is the approximate distinct index values in a blocklet assuming that each blocklet contains 20 pages and each page contains 32000 records. The value should be an integer. |
| BLOOM_FPP | NO | 0.00001 | This value is internally used by BloomFilter as the False-Positive Probability, it will affect the size of bloomfilter index as well as the number of hash functions for the BloomFilter. The value should be in the range (0, 1). In one test scenario, a 96GB TPCH customer table with bloom_size=320000 and bloom_fpp=0.00001 will result in 18 false positive samples. |
| BLOOM_COMPRESS | NO | true | Whether to compress the BloomFilter index files. |


## Loading Data
When loading data to main table, BloomFilter files will be generated for all the
index_columns given in DMProperties which contains the blockletId and a BloomFilter for each index column.
These index files will be written inside a folder named with DataMap name
inside each segment folders.


## Querying Data

User can verify whether a query can leverage BloomFilter DataMap by executing `EXPLAIN` command,
which will show the transformed logical plan, and thus user can check whether the BloomFilter DataMap can skip blocklets during the scan.
If the DataMap does not prune blocklets well, you can try to increase the value of property `BLOOM_SIZE` and decrease the value of property `BLOOM_FPP`.

## Data Management With BloomFilter DataMap
Data management with BloomFilter DataMap has no difference with that on Lucene DataMap.
You can refer to the corresponding section in `CarbonData Lucene DataMap`.

## Useful Tips
+ BloomFilter DataMap is suggested to be created on the high cardinality columns.
 Query conditions on these columns are always simple `equal` or `in`,
 such as 'col1=XX', 'col1 in (XX, YY)'.
+ We can create multiple BloomFilter DataMaps on one table,
 but we do recommend you to create one BloomFilter DataMap that contains multiple index columns,
 because the data loading and query performance will be better.
+ `BLOOM_FPP` is only the expected number from user, the actually FPP may be worse.
 If the BloomFilter DataMap does not work well,
 you can try to increase `BLOOM_SIZE` and decrease `BLOOM_FPP` at the same time.
 Notice that bigger `BLOOM_SIZE` will increase the size of index file
 and smaller `BLOOM_FPP` will increase runtime calculation while performing query.
+ '0' skipped blocklets of BloomFilter DataMap in explain output indicates that
 BloomFilter DataMap does not prune better than Main DataMap.
 (For example since the data is not ordered, a specific value may be contained in many blocklets. In this case, bloom may not work better than Main DataMap.)
 If this occurs very often, it means that current BloomFilter is useless. You can disable or drop it.
 Sometimes we cannot see any pruning result about BloomFilter DataMap in the explain output,
 this indicates that the previous DataMap has pruned all the blocklets and there is no need to continue pruning.
+ In some scenarios, the BloomFilter DataMap may not enhance the query performance significantly
 but if it can reduce the number of spark task,
 there is still a chance that BloomFilter DataMap can enhance the performance for concurrent query.
+ Note that BloomFilter DataMap will decrease the data loading performance and may cause slightly storage expansion (for DataMap index file).

