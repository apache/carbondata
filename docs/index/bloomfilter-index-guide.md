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

# CarbonData BloomFilter Index

* [Index Management](#index-management)
* [BloomFilter Index Introduction](#bloomfilter-index-introduction)
* [Loading Data](#loading-data)
* [Querying Data](#querying-data)
* [Data Management](#data-management-with-bloomfilter-index)
* [Useful Tips](#useful-tips)

#### Index Management
Creating BloomFilter Index
  ```
  CREATE INDEX [IF NOT EXISTS] index_name
  ON TABLE main_table (city,name)
  AS 'bloomfilter'
  PROPERTIES ('BLOOM_SIZE'='640000', 'BLOOM_FPP'='0.00001')
  ```

Dropping Specified Index
  ```
  DROP INDEX [IF EXISTS] index_name
  ON [TABLE] main_table
  ```

Showing all Indexes on this table
  ```
  SHOW INDEXES
  ON [TABLE] main_table
  ```
> NOTE: Keywords given inside `[]` is optional.

Disable Index
> The index by default is enabled. To support tuning on query, we can disable a specific index during query to observe whether we can gain performance enhancement from it. This is effective only for current session.

  ```
  // disable the index
  SET carbon.index.visible.dbName.tableName.indexName = false
  // enable the index
  SET carbon.index.visible.dbName.tableName.indexName = true
  ```


## BloomFilter Index Introduction
A Bloom filter is a space-efficient probabilistic data structure that is used to test whether an element is a member of a set.
Carbondata introduced BloomFilter as an index to enhance the performance of querying with precise value.
It is well suitable for queries that do precise matching on high cardinality columns(such as Name/ID).
Internally, CarbonData maintains a BloomFilter per blocklet for each index column to indicate that whether a value of the column is in this blocklet.
Just like the other indexes, BloomFilter index is managed along with main tables by CarbonData.
User can create BloomFilter index on specified columns with specified BloomFilter configurations such as size and probability.

For instance, main table called **index_test** which is defined as:

  ```
  CREATE TABLE index_test (
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
since `id` is in the sort_columns and it is ordered,
query on it will be fast because CarbonData can skip all the irrelative blocklets.
But queries on `name` may be bad since the blocklet minmax may not help,
because in each blocklet the range of the value of `name` may be the same -- all from A* to z*.
In this case, user can create a BloomFilter Index on column `name`.
Moreover, user can also create a BloomFilter Index on the sort_columns.
This is useful if user has too many segments and the range of the value of sort_columns are almost the same.

User can create BloomFilter Index using the Create Index DDL:

  ```
  CREATE INDEX dm
  ON TABLE index_test (name,id)
  AS 'bloomfilter'
  PROPERTIES ('BLOOM_SIZE'='640000', 'BLOOM_FPP'='0.00001', 'BLOOM_COMPRESS'='true')
  ```

Here, (name,id) are INDEX_COLUMNS. Carbondata will generate BloomFilter index on these columns. Queries on these columns are usually like `'COL = VAL'`.

**Properties for BloomFilter Index**

| Property | Is Required | Default Value | Description |
|-------------|----------|--------|---------|
| BLOOM_SIZE | NO | 640000 | This value is internally used by BloomFilter as the number of expected insertions, it will affect the size of BloomFilter index. Since each blocklet has a BloomFilter here, so the default value is the approximate distinct index values in a blocklet assuming that each blocklet contains 20 pages and each page contains 32000 records. The value should be an integer. |
| BLOOM_FPP | NO | 0.00001 | This value is internally used by BloomFilter as the False-Positive Probability, it will affect the size of bloomfilter index as well as the number of hash functions for the BloomFilter. The value should be in the range (0, 1). In one test scenario, a 96GB TPCH customer table with bloom_size=320000 and bloom_fpp=0.00001 will result in 18 false positive samples. |
| BLOOM_COMPRESS | NO | true | Whether to compress the BloomFilter index files. |


## Loading Data
When loading data to main table, BloomFilter files will be generated for all the
index_columns provided in the CREATE statement which contains the blockletId and a BloomFilter for each index column.
These index files will be written inside a folder named with Index name
inside each segment folders.


## Querying Data

User can verify whether a query can leverage BloomFilter Index by executing `EXPLAIN` command,
which will show the transformed logical plan, and thus user can check whether the BloomFilter Index can skip blocklets during the scan.
If the Index does not prune blocklets well, you can try to increase the value of property `BLOOM_SIZE` and decrease the value of property `BLOOM_FPP`.

## Data Management With BloomFilter Index
Data management with BloomFilter Index has no difference with that on Lucene Index.
You can refer to the corresponding section in [CarbonData Lucene Index](https://github.com/apache/carbondata/blob/master/docs/index/lucene-index-guide.md)

## Useful Tips
+ BloomFilter Index is suggested to be created on the high cardinality columns.
 Query conditions on these columns are always simple `equal` or `in`,
 such as 'col1=XX', 'col1 in (XX, YY)'.
+ We can create multiple BloomFilter Indexes on one table,
 but we do recommend you to create one BloomFilter Index that contains multiple index columns,
 because the data loading and query performance will be better.
+ `BLOOM_FPP` is only the expected number from user, the actual FPP may be worse.
 If the BloomFilter Index does not work well,
 you can try to increase `BLOOM_SIZE` and decrease `BLOOM_FPP` at the same time.
 Notice that bigger `BLOOM_SIZE` will increase the size of index file
 and smaller `BLOOM_FPP` will increase runtime calculation while performing query.
+ '0' skipped blocklets of BloomFilter Index in explain output indicates that
 BloomFilter Index does not prune better than Main Index.
 (For example since the data is not ordered, a specific value may be contained in many blocklets. In this case, bloom may not work better than Main Index.)
 If this occurs very often, it means that current BloomFilter is useless. You can disable or drop it.
 Sometimes we cannot see any pruning result about BloomFilter Index in the explain output,
 this indicates that the previous Index has pruned all the blocklets and there is no need to continue pruning.
+ In some scenarios, the BloomFilter Index may not enhance the query performance significantly
 but if it can reduce the number of spark task,
 there is still a chance that BloomFilter Index can enhance the performance for concurrent query.
+ Note that BloomFilter Index will decrease the data loading performance and may cause slight storage expansion (for index file).

