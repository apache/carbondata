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

# CarbonData Lucene Index (Alpha Feature)
  
* [Index Management](#index-management)
* [Lucene Index](#lucene-index-introduction)
* [Loading Data](#loading-data)
* [Querying Data](#querying-data)
* [Data Management](#data-management-with-lucene-index)

#### Index Management 
Lucene Index can be created using following DDL
  ```
  CREATE INDEX [IF NOT EXISTS] index_name
  ON TABLE main_table (index_columns)
  AS 'lucene'
  [PROPERTIES ('key'='value')]
  ```
index_columns is the list of string columns on which lucene creates indexes.

Index can be dropped using following DDL:
  ```
  DROP INDEX [IF EXISTS] index_name
  ON [TABLE] main_table
  ```
To show all Indexes created, use:
  ```
  SHOW INDEXES
  ON [TABLE] main_table
  ```
It will show all Indexes created on the main table.
> NOTE: Keywords given inside `[]` is optional.


## Lucene Index Introduction
  Lucene is a high performance, full featured text search engine. Lucene is integrated to carbon as
  an index and managed along with main tables by CarbonData. User can create lucene index 
  to improve query performance on string columns which has content of more length. So, user can 
  search tokenized word or pattern of it using lucene query on text content.
  
  For instance, main table called **index_test** which is defined as:
  
  ```
  CREATE TABLE index_test (
    name string,
    age int,
    city string,
    country string)
  STORED AS carbondata
  ```
  
  User can create Lucene index using the Create Index DDL:
  
  ```
  CREATE INDEX dm
  ON TABLE index_test (name,country)
  AS 'lucene'
  ```

**Properties**
1. FLUSH_CACHE: size of the cache to maintain in Lucene writer, if specified then it tries to 
   aggregate the unique data till the cache limit and flush to Lucene. It is best suitable for low 
   cardinality dimensions.
2. SPLIT_BLOCKLET: when made as true then store the data in blocklet wise in lucene , it means new 
   folder will be created for each blocklet, thus, it eliminates storing blockletid in lucene and 
   also it makes lucene small chunks of data.
   
## Loading data
When loading data to main table, lucene index files will be generated for all the
index_columns(String Columns) given in CREATE statement which contains information about the data
location of index_columns. These index files will be written inside a folder named with index name
inside each segment folder.

A system level configuration `carbon.lucene.compression.mode` can be added for best compression of
lucene index files. The default value is speed, where the index writing speed will be more. If the
value is compression, the index file size will be compressed.

## Querying data
As a technique for query acceleration, Lucene indexes cannot be queried directly.
Queries are to be made on the main table. When a query with TEXT_MATCH('name:c10') or 
TEXT_MATCH_WITH_LIMIT('name:n10',10)[the second parameter represents the number of result to be 
returned, if user does not specify this value, all results will be returned without any limit] is 
fired, two jobs will be launched. The first job writes the temporary files in folder created at table level 
which contains lucene's search results and these files will be read in second job to give faster 
results. These temporary files will be cleared once the query finishes.

User can verify whether a query can leverage Lucene index or not by executing the `EXPLAIN`
command, which will show the transformed logical plan, and thus user can check whether TEXT_MATCH()
filter is applied on query or not.

**Note:**
 1. The filter columns in TEXT_MATCH or TEXT_MATCH_WITH_LIMIT must be always in lowercase and 
filter conditions like 'AND','OR' must be in upper case.

      Ex: 
      ```
      select * from index_test where TEXT_MATCH('name:*10 AND name:*n*')
      ```
     
2. Query supports only one TEXT_MATCH udf for filter condition and not multiple udfs.

   The following query is supported:
   ```
   select * from index_test where TEXT_MATCH('name:*10 AND name:*n*')
   ```
       
   The following query is not supported:
   ```
   select * from index_test where TEXT_MATCH('name:*10) AND TEXT_MATCH(name:*n*')
   ```
       
          
Below `like` queries can be converted to text_match queries as following:
```
select * from index_test where name='n10'

select * from index_test where name like 'n1%'

select * from index_test where name like '%10'

select * from index_test where name like '%n%'

select * from index_test where name like '%10' and name not like '%n%'
```
Lucene TEXT_MATCH Queries:
```
select * from index_test where TEXT_MATCH('name:n10')

select * from index_test where TEXT_MATCH('name:n1*')

select * from index_test where TEXT_MATCH('name:*10')

select * from index_test where TEXT_MATCH('name:*n*')

select * from index_test where TEXT_MATCH('name:*10 -name:*n*')
```
**Note:** For lucene queries and syntax, refer to [lucene-syntax](http://www.lucenetutorial.com/lucene-query-syntax.html)

## Data Management with lucene index
Once there is a lucene index created on the main table, following command on the main
table is not supported:
1. Data management command: `UPDATE/DELETE`.
2. Schema management command: `ALTER TABLE DROP COLUMN`, `ALTER TABLE CHANGE DATATYPE`, 
`ALTER TABLE RENAME`.

**Note**: Adding a new column is supported, and for dropping columns and change datatype 
command, CarbonData will check whether it will impact the lucene index, if not, the operation 
is allowed, otherwise operation will be rejected by throwing exception.


3. Partition management command: `ALTER TABLE ADD/DROP PARTITION`.

However, there is still way to support these operations on main table, in current CarbonData 
release, user can do as following:
1. Remove the lucene index by `DROP INDEX` command.
2. Carry out the data management operation on main table.
3. Create the lucene index again by `CREATE INDEX` command.
Basically, user can manually trigger the operation by refreshing the index.


