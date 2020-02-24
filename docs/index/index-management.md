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
- [DataMap Management](#datamap-management)
- [Automatic Refresh](#automatic-refresh)
- [Manual Refresh](#manual-refresh)
- [DataMap Catalog](#datamap-catalog)
- [DataMap Related Commands](#datamap-related-commands)
  - [Explain](#explain)
  - [Show DataMap](#show-datamap)



## Overview

DataMap can be created using following DDL

```
CREATE DATAMAP [IF NOT EXISTS] datamap_name
[ON TABLE main_table]
USING "datamap_provider"
[WITH DEFERRED REBUILD]
DMPROPERTIES ('key'='value', ...)
AS
  SELECT statement
```

Currently, there are 5 DataMap implementations in CarbonData.

| DataMap Provider | Description                              | DMPROPERTIES                             | Management       |
| ---------------- | ---------------------------------------- | ---------------------------------------- | ---------------- |
| mv               | multi-table pre-aggregate table          | No DMPROPERTY is required                | Manual/Automatic           |
| lucene           | lucene indexing for text column          | index_columns to specifying the index columns | Automatic |
| bloomfilter      | bloom filter for high cardinality column, geospatial column | index_columns to specifying the index columns | Automatic |

## DataMap Management

There are two kinds of management semantic for DataMap.

1. Automatic Refresh: Create datamap without `WITH DEFERRED REBUILD` in the statement, which is by default.
2. Manual Refresh: Create datamap with `WITH DEFERRED REBUILD` in the statement

### Automatic Refresh

When user creates a datamap on the main table without using `WITH DEFERRED REBUILD` syntax, the datamap will be managed by system automatically.
For every data load to the main table, system will immediately trigger a load to the datamap automatically. These two data loading (to main table and datamap) is executed in a transactional manner, meaning that it will be either both success or neither success. 

The data loading to datamap is incremental based on Segment concept, avoiding a expensive total rebuild.

If user perform following command on the main table, system will return failure. (reject the operation)

1. Data management command: `UPDATE/DELETE/DELETE SEGMENT`.
2. Schema management command: `ALTER TABLE DROP COLUMN`, `ALTER TABLE CHANGE DATATYPE`,
   `ALTER TABLE RENAME`. Note that adding a new column is supported, and for dropping columns and
   change datatype command, CarbonData will check whether it will impact the pre-aggregate table, if
    not, the operation is allowed, otherwise operation will be rejected by throwing exception.
3. Partition management command: `ALTER TABLE ADD/DROP PARTITION`.

If user do want to perform above operations on the main table, user can first drop the datamap, perform the operation, and re-create the datamap again.

If user drop the main table, the datamap will be dropped immediately too.

We do recommend you to use this management for index datamap.

### Manual Refresh

When user creates a datamap specifying manual refresh semantic, the datamap is created with status *disabled* and query will NOT use this datamap until user can issue REBUILD DATAMAP command to build the datamap. For every REBUILD DATAMAP command, system will trigger a full rebuild of the datamap. After rebuild is done, system will change datamap status to *enabled*, so that it can be used in query rewrite.

For every new data loading, data update, delete, the related datamap will be made *disabled*,
which means that the following queries will not benefit from the datamap before it becomes *enabled* again.

If the main table is dropped by user, the related datamap will be dropped immediately.

**Note**:
+ If you are creating a datamap on external table, you need to do manual management of the datamap.
+ For index datamap such as BloomFilter datamap, there is no need to do manual refresh.
 By default it is automatic refresh,
 which means its data will get refreshed immediately after the datamap is created or the main table is loaded.
 Manual refresh on this datamap will has no impact.



## DataMap Catalog

Currently, when user creates a datamap, system will store the datamap metadata in a configurable *system* folder in HDFS or S3.

In this *system* folder, it contains:

- DataMapSchema file. It is a json file containing schema for one datamap. Ses DataMapSchema class. If user creates 100 datamaps (on different tables), there will be 100 files in *system* folder.
- DataMapStatus file. Only one file, it is in json format, and each entry in the file represents for one datamap. Ses DataMapStatusDetail class

There is a DataMapCatalog interface to retrieve schema of all datamap, it can be used in optimizer to get the metadata of datamap.



## DataMap Related Commands

### Explain

How can user know whether datamap is used in the query?

User can set enable.query.statistics = true and use EXPLAIN command to know, it will print out something like

```text
== CarbonData Profiler ==
Hit mv DataMap: datamap1
Scan Table: default.datamap1_table
+- filter:
+- pruning by CG DataMap
+- all blocklets: 1
   skipped blocklets: 0
```

### Show DataMap

There is a SHOW DATAMAPS command, when this is issued, system will read all datamap from *system* folder and print all information on screen. The current information includes:

- DataMapName
- DataMapProviderName like mv
- Associated Table
- DataMap Properties
- DataMap status (ENABLED/DISABLED)
- Sync Status - which displays Last segment Id of main table synced with datamap table and its load
  end time (Applicable only for mv datamap)
