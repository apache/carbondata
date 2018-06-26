# CarbonData DataMap Management

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

Currently, there are 5 DataMap implementation in CarbonData.

| DataMap Provider | Description                              | DMPROPERTIES                             | Management       |
| ---------------- | ---------------------------------------- | ---------------------------------------- | ---------------- |
| preaggregate     | single table pre-aggregate table         | No DMPROPERTY is required                | Automatic        |
| timeseries       | time dimension rollup table.             | event_time, xx_granularity, please refer to [Timeseries DataMap](https://github.com/apache/carbondata/blob/master/docs/datamap/timeseries-datamap-guide.md) | Automatic        |
| mv               | multi-table pre-aggregate table,         | No DMPROPERTY is required                | Manual           |
| lucene           | lucene indexing for text column          | index_columns to specifying the index columns | Manual/Automatic |
| bloom            | bloom filter for high cardinality column, geospatial column | index_columns to specifying the index columns | Manual/Automatic |

## DataMap Management

There are two kinds of management semantic for DataMap.

1. Autmatic Refresh: Create datamap without `WITH DEFERED REBUILD` in the statement
2. Manual Refresh: Create datamap with `WITH DEFERED REBUILD` in the statement

### Automatic Refresh

When user creates a datamap on the main table without using `WITH DEFERED REBUILD` syntax, the datamap will be managed by system automatically.
For every data load to the main table, system will immediately triger a load to the datamap automatically. These two data loading (to main table and datamap) is executed in a transactional manner, meaning that it will be either both success or neither success. 

The data loading to datamap is incremental based on Segment concept, avoiding a expesive total rebuild.

If user perform following command on the main table, system will return failure. (reject the operation)

1. Data management command: `UPDATE/DELETE/DELETE SEGMENT`.
2. Schema management command: `ALTER TABLE DROP COLUMN`, `ALTER TABLE CHANGE DATATYPE`,
   `ALTER TABLE RENAME`. Note that adding a new column is supported, and for dropping columns and
   change datatype command, CarbonData will check whether it will impact the pre-aggregate table, if
    not, the operation is allowed, otherwise operation will be rejected by throwing exception.
3. Partition management command: `ALTER TABLE ADD/DROP PARTITION

If user do want to perform above operations on the main table, user can first drop the datamap, perform the operation, and re-create the datamap again.

If user drop the main table, the datamap will be dropped immediately too.

### Manual Refresh

When user creates a datamap specifying maunal refresh semantic, the datamap is created with status *disabled* and query will NOT use this datamap until user can issue REBUILD DATAMAP command to build the datamap. For every REBUILD DATAMAP command, system will trigger a full rebuild of the datamap. After rebuild is done, system will change datamap status to *enabled*, so that it can be used in query rewrite.

For every new data loading, data update, delete, the related datamap will be made *disabled*.

If the main table is dropped by user, the related datamap will be dropped immediately.

*Note: If you are creating a datamap on external table, you need to do manual managment of the datamap.*



## DataMap Catalog

Currently, when user creates a datamap, system will store the datamap metadata in a configurable *system* folder in HDFS or S3.

In this *system* folder, it contains:

- DataMapSchema file. It is a json file containing schema for one datamap. Ses DataMapSchema class. If user creates 100 datamaps (on different tables), there will be 100 files in *system* folder.
- DataMapStatus file. Only one file, it is in json format, and each entry in the file represents for one datamap. Ses DataMapStatusDetail class

There is a DataMapCatalog interface to retrieve schema of all datamap, it can be used in optimizer to get the metadata of datamap.



## DataMap Related Commands

### Explain

How can user know whether datamap is used in the query?

User can use EXPLAIN command to know, it will print out something like

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
- DataMapProviderName like mv, preaggreagte, timeseries, etc
- Associated Table

### Compaction on DataMap

This feature applies for preaggregate datamap only

Running Compaction command (`ALTER TABLE COMPACT`) on main table will **not automatically** compact the pre-aggregate tables created on the main table. User need to run Compaction command separately on each pre-aggregate table to compact them.

Compaction is an optional operation for pre-aggregate table. If compaction is performed on main table but not performed on pre-aggregate table, all queries still can benefit from pre-aggregate tables. To further improve the query performance, compaction on pre-aggregate tables can be triggered to merge the segments and files in the pre-aggregate tables.
