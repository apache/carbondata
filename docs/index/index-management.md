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

# CarbonData Index Management

- [Overview](#overview)
- [Index Management](#index-management)
- [Automatic Refresh](#automatic-refresh)
- [Manual Refresh](#manual-refresh)
- [Index Related Commands](#index-related-commands)
  - [Explain](#explain)
  - [Show Index](#show-index)



## Overview

Index can be created using following DDL

```
CREATE INDEX [IF NOT EXISTS] index_name
ON TABLE [db_name.]table_name (column_name, ...)
AS carbondata/bloomfilter/lucene
[WITH DEFERRED REFRESH]
[PROPERTIES ('key'='value')]
```

Index can be refreshed using following DDL

```
REFRESH INDEX index_name ON [TABLE] [db_name.]table_name
```

Currently, there are 3 Index implementations in CarbonData.

| Index Provider   | Description                                                                      | Management |
| ---------------- | -------------------------------------------------------------------------------- |  --------- |
| secondary-index  | secondary-index tables to hold blocklets as indexes and managed as child tables  | Automatic |
| lucene           | lucene indexing for text column                                                  | Automatic |
| bloomfilter      | bloom filter for high cardinality column, geospatial column                      | Automatic |

## Index Management

There are two kinds of management semantic for Index.

1. Automatic Refresh
2. Manual Refresh

### Automatic Refresh

When a user creates an index on the main table without using `WITH DEFERRED REFRESH` syntax, the index will be managed by the system automatically.
For every data load to the main table, the system will immediately trigger a load to the index automatically. These two data loading (to main table and index) is executed in a transactional manner, meaning that it will be either both success or neither success. 

The data loading to index is incremental based on Segment concept, avoiding an expensive total refresh.

If a user performs the following command on the main table, the system will return failure. (reject the operation)

1. Data management command: `UPDATE/DELETE/DELETE SEGMENT`.
2. Schema management command: `ALTER TABLE DROP COLUMN`, `ALTER TABLE CHANGE DATATYPE`,
   `ALTER TABLE RENAME`. Note that adding a new column is supported, and for dropping columns and
   change datatype command, CarbonData will check whether it will impact the index table, if
    not, the operation is allowed, otherwise operation will be rejected by throwing an exception.
3. Partition management command: `ALTER TABLE ADD/DROP PARTITION`.

If a user does want to perform above operations on the main table, the user can first drop the index, perform the operation, and re-create the index again.

If a user drops the main table, the index will be dropped immediately too.

We do recommend you to use this management for indexing.

### Manual Refresh

When a user creates an index on the main table using `WITH DEFERRED REFRESH` syntax, the index will be created with status *disabled* and query will NOT use this index until the user issues `REFRESH INDEX` command to build the index. For every `REFRESH INDEX` command, the system will trigger a full refresh of the index. Once the refresh operation is finished, system will change index status to *enabled*, so that it can be used in query rewrite.

For every new data loading, data update, delete, the related index will be made *disabled*,
which means that the following queries will not benefit from the index before it becomes *enabled* again.

If the main table is dropped by the user, the related index will be dropped immediately.

**Note**:
+ If you are creating an index on an external table, you need to do manual management of the index.
+ Currently, all types of indexes supported by carbon will be automatically refreshed by default, which means its data will get refreshed immediately after the index is created or the main table is loaded. Manual refresh on these indexes is not supported.

## Index Related Commands

### Explain

How can users know whether an index is used in the query?

User can set `enable.query.statistics = true` and use `EXPLAIN` command to know, it will print out something like

```text
== CarbonData Profiler ==
Table Scan on default.main
+- total: 1 blocks, 1 blocklets
+- filter:
+- pruned by CG Index
   - name: index1
   - provider: lucene
   - skipped: 0 blocks, 0 blocklets
```

### Show Index

There is a SHOW INDEXES command, when this is issued, the system will read all indexes from the carbon table and print all information on screen. The current information includes:

- Name
- Provider like lucene
- Indexed Columns
- Properties
- Status (ENABLED/DISABLED)
- Sync Info - which displays Last segment Id of main table synced with index table and its load
  end time
