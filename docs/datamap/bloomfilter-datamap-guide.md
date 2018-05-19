# CarbonData BloomFilter DataMap (Alpha feature in 1.4.0)

* [DataMap Management](#datamap-management)
* [BloomFilter Datamap Introduction](#bloomfilter-datamap-introduction)
* [Loading Data](#loading-data)
* [Querying Data](#querying-data)
* [Data Management](#data-management-with-bloomfilter-datamap)

#### DataMap Management
Creating BloomFilter DataMap
  ```
  CREATE DATAMAP [IF NOT EXISTS] datamap_name
  ON TABLE main_table
  USING 'bloomfilter'
  DMPROPERTIES ('index_columns'='city, name', 'BLOOM_SIZE'='640000', 'BLOOM_FPP'='0.00001')
  ```

Dropping specified datamap
  ```
  DROP DATAMAP [IF EXISTS] datamap_name
  ON TABLE main_table
  ```

Showing all DataMaps on this table
  ```
  SHOW DATAMAP
  ON TABLE main_table
  ```
It will show all DataMaps created on main table.


## BloomFilter DataMap Introduction
A Bloom filter is a space-efficient probabilistic data structure that is used to test whether an element is a member of a set.
Carbondata introduce BloomFilter as an index datamap to enhance the performance of querying with precise value.
It is well suitable for queries that do precise match on high cardinality columns(such as Name/ID).
Internally, CarbonData maintains a BloomFilter per blocklet for each index column to indicate that whether a value of the column is in this blocklet.
Just like the other datamaps, BloomFilter datamap is managed ablong with main tables by CarbonData.
User can create BloomFilter datamap on specified columns with specified BloomFilter configurations such as size and probability.

For instance, main table called **datamap_test** which is defined as:

  ```
  CREATE TABLE datamap_test (
    id string,
    name string,
    age int,
    city string,
    country string)
  STORED BY 'carbondata'
  TBLPROPERTIES('SORT_COLUMNS'='id')
  ```

In the above example, `id` and `name` are high cardinality columns
and we always query on `id` and `name` with precise value.
since `id` is in the sort_columns and it is orderd,
query on it will be fast because CarbonData can skip all the irrelative blocklets.
But queries on `name` may be bad since the blocklet minmax may not help,
because in each blocklet the range of the value of `name` may be the same -- all from A*~z*.
In this case, user can create a BloomFilter datamap on column `name`.
Moreover, user can also create a BloomFilter datamap on the sort_columns.
This is useful if user has too many segments and the range of the value of sort_columns are almost the same.

User can create BloomFilter datamap using the Create DataMap DDL:

  ```
  CREATE DATAMAP dm
  ON TABLE datamap_test
  USING 'bloomfilter'
  DMPROPERTIES ('INDEX_COLUMNS' = 'name,id', 'BLOOM_SIZE'='640000', 'BLOOM_FPP'='0.00001', 'BLOOM_COMPRESS'='true')
  ```

**Properties for BloomFilter DataMap**

| Property | Is Required | Default Value | Description |
|-------------|----------|--------|---------|
| INDEX_COLUMNS | YES |  | Carbondata will generate BloomFilter index on these columns. Queries on there columns are usually like 'COL = VAL'. |
| BLOOM_SIZE | NO | 32000 | This value is internally used by BloomFilter as the number of expected insertions, it will affects the size of BloomFilter index. Since each blocklet has a BloomFilter here, so the value is the approximate records in a blocklet. In another word, the value 32000 * #noOfPagesInBlocklet. The value should be an integer. |
| BLOOM_FPP | NO | 0.01 | This value is internally used by BloomFilter as the False-Positive Probability, it will affects the size of bloomfilter index as well as the number of hash functions for the BloomFilter. The value should be in range (0, 1). |
| BLOOM_COMPRESS | NO | true | Whether to compress the BloomFilter index files. |


## Loading Data
When loading data to main table, BloomFilter files will be generated for all the
index_columns given in DMProperties which contains the blockletId and a BloomFilter for each index column.
These index files will be written inside a folder named with datamap name
inside each segment folders.


## Querying Data
A system level configuration `carbon.query.datamap.bloom.cache.size` can used to enhance query performance with BloomFilter datamap by providing a cache for the bloomfilter index files.
The default value is `512` and its unit is `MB`. Internally the cache will be expired after it's idle for 2 hours.

User can verify whether a query can leverage BloomFilter datamap by executing `EXPLAIN` command,
which will show the transformed logical plan, and thus user can check whether the BloomFilter datamap can skip blocklets during the scan.
If the datamap does not prune blocklets well, you can try to increase the value of property `BLOOM_SIZE` and decrease the value of property `BLOOM_FPP`.

## Data Management With BloomFilter DataMap
Data management with BloomFilter datamap has no difference with that on Lucene datamap. You can refer to the corresponding section in `CarbonData BloomFilter DataMap`.
