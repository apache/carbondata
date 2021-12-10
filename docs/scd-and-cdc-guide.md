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

# Upsert into a Carbon DataSet using Merge and UPSERT APIs

## SCD and CDC Scenarios
Change Data Capture (CDC), is to apply all data changes generated from an external data set 
into a target dataset. In other words, a set of changes (update/delete/insert) applied to an external 
table needs to be applied to a target table.

Slowly Changing Dimensions (SCD), are the dimensions in which the data changes slowly, rather 
than changing regularly on a time basis.

SCD and CDC data changes can be merged to a carbon dataset online using the data frame level `MERGE`, `UPSERT`, `UPDATE`, `DELETE` and `INSERT` APIs.

### MERGE API

Below API merges the datasets online and applies the actions as per the conditions. 

```
  targetDS.merge(sourceDS, <condition>)
          .whenMatched(<condition>)
          .updateExpr(updateMap)
          .insertExpr(insertMap_u)
          .whenNotMatched(<condition>)
          .insertExpr(insertMap)
          .whenNotMatchedAndExistsOnlyOnTarget(<condition>)
          .delete()
          .execute()
```

### UPSERT API
Below API upsert the input source dataset onto the target carbondata table based on the key column and dataset provided by the user or the application.

```
  targetDS.upsert(sourceDS, <key_column>)
          .execute()
```

### DELETE API
Below API deletes the data present in the target carbondata table based on the key column and dataset provided by the user or the application.

```
  targetDS.delete(sourceDS, <key_column>)
          .execute()
```

### UPDATE API
Below API updates the data present in the target carbondata table based on the key column and dataset provided by the user or the application.

```
  targetDS.update(sourceDS, <key_column>)
          .execute()
```

### INSERT API
Below API inserts the input source dataset onto the target carbondata table based on the key column and dataset provided by the user or the application.

```
  targetDS.insert(sourceDS, <key_column>)
          .execute()
```

#### MERGE API Operation Semantics
Below is the detailed description of the `merge` API operation.
* `merge` will merge the datasets based on a condition.
* `whenMatched` clauses are executed when a source row matches a target table row based on the match condition.
   These clauses have the following semantics.
    * `whenMatched` clauses can have at most one updateExpr and one delete action. The `updateExpr` action in merge only updates the specified columns of the matched target row. The `delete` action deletes the matched row.
    * If there are two `whenMatched` clauses, then they are evaluated in order they are specified. The first clause must have a clause condition (otherwise the second clause is never executed).
    * If both `whenMatched` clauses have conditions and neither of the conditions are true for a matching source-target row pair, then the matched target row is left unchanged.
* `whenNotMatched` clause is executed when a source row does not match any target row based on the match condition.
   * `whenNotMatched` clause can have only the `insertExpr` action. The new row is generated based on the specified column and corresponding expressions. Users do not need to specify all the columns in the target table. For unspecified target columns, NULL is inserted.
* `whenNotMatchedAndExistsOnlyOnTarget` clause is executed when row does not match source and exists only in target. This clause can have only delete action.

#### UPSERT API Operation Semantics
* `upsert`, `delete`, `insert` and `update` APIs will help to perform specified operations on the target carbondata table.
* All the APIs expects two parameters source dataset and the key column on which the merge has to be performed.

#### MERGE SQL

Below sql merges a set of updates, insertions, and deletions based on a source table
into a target carbondata table. 

```
    MERGE INTO target_table_identifier
    USING source_table_identifier
    ON <merge_condition>
    [ WHEN MATCHED [ AND <condition> ] THEN <matched_action> ]
    [ WHEN MATCHED [ AND <condition> ] THEN <matched_action> ]
    [ WHEN NOT MATCHED [ AND <condition> ]  THEN <not_matched_action> ]
```

#### MERGE SQL Operation Semantics
Below is the detailed description of the `merge` SQL operation.
* `table_identifier` a table name, optionally qualified with a database name
* `merge_condition` how the rows from one relation are combined with the rows of another relation. An expression with a return type of Boolean.
* `WHEN MATCHED` clauses are executed when a source row matches a target table row based on the match condition,
clauses can have at most one UPDATE and one DELETE action, These clauses have the following semantics.
    * The UPDATE action in merge only updates the specified columns of the matched target row.
    * The DELETE action will delete the matched row.
    * WHEN MATCHED clauses can have at most one UPDATE and one DELETE action. The UPDATE action in merge only updates the specified columns of the matched target row. The DELETE action will delete the matched row.
    * Each WHEN MATCHED clause can have an optional condition. If this clause condition exists, the UPDATE or DELETE action is executed for any matching source-target row pair row only when when the clause condition is true.
    * If there are multiple WHEN MATCHED clauses, then they are evaluated in order they are specified (that is, the order of the clauses matter). All WHEN MATCHED clauses, except the last one, must have conditions.
    * If both WHEN MATCHED clauses have conditions and neither of the conditions are true for a matching source-target row pair, then the matched target row is left unchanged.
    * To update all the columns of the target carbondata table with the corresponding columns of the source dataset, use UPDATE SET *. This is equivalent to UPDATE SET col1 = source.col1 [, col2 = source.col2 ...] for all the columns of the target carbondata table. Therefore, this action assumes that the source table has the same columns as those in the target table, otherwise the query will throw an analysis error.
* `matched_action` can be DELETE  | UPDATE SET *  |UPDATE SET column1 = value1 [, column2 = value2 ...]
* `WHEN NOT MATCHED` clause is executed when a source row does not match any target row based on the match condition, these clauses have the following semantics.
    * WHEN NOT MATCHED clauses can only have the INSERT action. The new row is generated based on the specified column and corresponding expressions. All the columns in the target table do not need to be specified. For unspecified target columns, NULL is inserted.
    * Each WHEN NOT MATCHED clause can have an optional condition. If the clause condition is present, a source row is inserted only if that condition is true for that row. Otherwise, the source column is ignored.
    * If there are multiple WHEN NOT MATCHED clauses, then they are evaluated in order they are specified (that is, the order of the clauses matter). All WHEN NOT  MATCHED clauses, except the last one, must have conditions.
    * To insert all the columns of the target carbondata table with the corresponding columns of the source dataset, use INSERT *. This is equivalent to INSERT (col1 [, col2 ...]) VALUES (source.col1 [, source.col2 ...]) for all the columns of the target carbondata table. Therefore, this action assumes that the source table has the same columns as those in the target table, otherwise the query will throw an error.
* `not_matched_action` can be INSERT *  | INSERT (column1 [, column2 ...]) VALUES (value1 [, value2 ...])

**Note: Merge SQL is not yet supported for the UPSERT, DELETE, INSERT and UPDATE APIs.**
##### Example code to implement cdc/scd scenario

* Please refer example class [MergeTestCase](https://github.com/apache/carbondata/blob/master/integration/spark/src/test/scala/org/apache/carbondata/spark/testsuite/merge/MergeTestCase.scala) to understand and implement scd and cdc scenarios using APIs.
* Please refer example class [DataMergeIntoExample](https://github.com/apache/carbondata/blob/master/examples/spark/src/main/scala/org/apache/carbondata/examples/DataMergeIntoExample.scala) to understand and implement scd and cdc scenarios using sql. 
* Please refer example class [DataUPSERTExample](https://github.com/apache/carbondata/blob/master/examples/spark/src/main/scala/org/apache/carbondata/examples/DataUPSERTExample.scala) to understand and implement cdc using UPSERT APIs.

### Streamer Tool

Carbondata streamer tool is a very powerful tool for incrementally capturing change events from varied sources like kafka or DFS and merging them into target carbondata table. This essentially means one needs to integrate with external solutions like Debezium or Maxwell for moving the change events to kafka, if one wishes to capture changes from primary databases like mysql. The tool currently requires incoming data to be present in avro format and incoming schema to evolve in backwards compatible way.

Below is a high level architecture of how the overall pipeline looks like -

![Carbondata streamer tool pipeline](../docs/images/carbondata-streamer-tool-pipeline.png?raw=true)

#### Configs

Streamer tool exposes below configs for users to cater to their CDC use cases - 

| Parameter                         | Default Value                                              | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
|-----------------------------------|------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| carbon.streamer.target.database   | Current database from spark session                        | The database name where the target table is present to merge the incoming data. If not given by user, system will take the current database in the spark session.                                                                                                                                                                                                                                                                                                                                          |
| carbon.streamer.target.table      | (none)                                                     | The target carbondata table where the data has to be merged. If this is not configured by user, the operation will fail.                                                                                                                                                                                                                                                                                                                                                                                   |
| carbon.streamer.source.type       | kafka                                                      | Streamer tool currently supports two types of data sources. One can ingest data from either kafka or DFS into target carbondata table using streamer tool.                                                                                                                                                                                                                                                                                                                                        |
| carbon.streamer.dfs.input.path    | (none)                                                     | An absolute path on a given file system from where data needs to be read to ingest into the target carbondata table. Mandatory if the ingestion source type is DFS.                                                                                                                                                                                                                                                                                                                                        |
| schema.registry.url               | (none)                                                     | Streamer tool supports 2 different ways to supply schema of incoming data. Schemas can be supplied using avro files (file based schema provider) or using schema registry. This property defines the url to connect to in case schema registry is used as the schema source.                                                                                                                                                                                                                               |
| carbon.streamer.input.kafka.topic | (none)                                                     | This is a mandatory property to be set in case kafka is chosen as the source of data. This property defines the topics from where streamer tool will consume the data.                                                                                                                                                                                                                                                                                                                                     |
| bootstrap.servers                 | (none)                                                     | This is another mandatory property in case kafka is chosen as the source of data. This defines the end points for kafka brokers.                                                                                                                                                                                                                                                                                                                                                                           |
| auto.offset.reset | earliest                                                   | Streamer tool maintains checkpoints to keep a track of the incoming messages which are already consumed. In case of first ingestion using kafka source, this property defines the offset from where ingestion will start. This property can take only 2 valid values - `latest` and `earliest`                                                                                                                                                                                                             |
| enable.auto.commit | false                                                      | Kafka maintains an internal topic for storing offsets corresponding to the consumer groups. This property determines if kafka should actually go forward and commit the offsets consumed in this internal topic. We recommend to keep it as false since we use spark streaming checkpointing to take care of the same.                                                                                                                                                                                     |
| group.id | (none)                                                     | Streamer tool is ultimately a consumer for kafka. This property determines the consumer group id streamer tool belongs to.                                                                                                                                                                                                                                                                                                                                                                                 |
| carbon.streamer.input.payload.format | avro                                                       | This determines the format of the incoming messages from source. Currently only avro is supported. We have plans to extend this support to json as well in near future. Avro is the most preferred format for CDC use cases since it helps in making the message size very compact and has good support for schema evolution use cases as well.                                                                                                                                                            |
| carbon.streamer.schema.provider | SchemaRegistry                                             | As discussed earlier, streamer tool supports 2 ways of supplying schema for incoming messages - schema registry and avro files. Confluent schema registry is the preferred way when using avro as the input format.                                                                                                                                                                                                                                                                                        |
| carbon.streamer.source.schema.path | (none)                                                     | This property defines the absolute path where files containing schemas for incoming messages are present.                                                                                                                                                                                                                                                                                                                                                                                                  |
| carbon.streamer.merge.operation.type | upsert                                                     | This defines the operation that needs to be performed on the incoming batch of data while writing it to target data set.                                                                                                                                                                                                                                                                                                                                                                                   |
| carbon.streamer.merge.operation.field | (none)                                                     | This property defines the field in incoming schema which contains the type of operation performed at source. For example, Debezium includes a field called `op` when reading change events from primary database. Do not confuse this property with `carbon.streamer.merge.operation.type` which defines the operation to be performed on the incoming batch of data. However this property is needed so that streamer tool is able to identify rows deleted at source when the operation type is `upsert`. |
| carbon.streamer.record.key.field | (none)                                                     | This defines the record key for a particular incoming record. This is used by the streamer tool for performing deduplication. In case this is not defined, operation will fail.                                                                                                                                                                                                                                                                                                                            |
| carbon.streamer.batch.interval | 10                                                         | Minimum batch interval time between 2 continuous ingestion in continuous mode. Should be specified in seconds.                                                                                                                                                                                                                                                                                                                                                                                             |
| carbon.streamer.source.ordering.field | <none>                                                     | Name of the field from source schema whose value can be used for picking the latest updates for a particular record in the incoming batch in case of multiple updates for the same record key. Useful if the write operation type is UPDATE or UPSERT. This will be used only if `carbon.streamer.upsert.deduplicate` is enabled.                                                                                                                                                                          |
| carbon.streamer.insert.deduplicate | false                                                      | This property specifies if the incoming batch needs to be deduplicated in case of INSERT operation type. If set to true, the incoming batch will be deduplicated against the existing data in the target carbondata table.                                                                                                                                                                                                                                                                                 |
| carbon.streamer.upsert.deduplicate | true                                                       | This property specifies if the incoming batch needs to be deduplicated (when multiple updates for the same record key are present in the incoming batch) in case of UPSERT/UPDATE operation type. If set to true, the user needs to provide proper value for the source ordering field as well.                                                                                                                                                                                                            |
| carbon.streamer.meta.columns | (none)                                                     | Generally when performing CDC operations on primary databases, few metadata columns are added along with the actual columns for book keeping purposes. This property enables users to list down all such metadata fields (comma separated) which should not be merged with the target carboondata table.                                                                                                                                                                                                   |
| carbon.enable.schema.enforcement | true                                                       | This flag decides if table schema needs to change as per the incoming batch schema. If set to true, incoming schema will be validated with existing table schema. If the schema has evolved, the incoming batch cannot be ingested and job will simply fail.                                                                                                                                                                                                                                               |

#### Commands

1. For kafka source - 

```
bin/spark-submit --class org.apache.carbondata.streamer.CarbonDataStreamer \
--master <spark_master_url> \
<carbondata_assembly_jar_path> \
--database-name testdb \
--target-table target \
--record-key-field name \
--source-ordering-field age \
--source-type kafka \
--deduplicate false \
--input-kafka-topic test_topic \
--brokers <comma_separated_list_of_brokers> \
--schema-registry-url <schema_registry_url> \
--group-id testgroup \
--meta-columns __table,__db,__ts_ms,__file,__pos,__deleted
```

2. For DFS source - 

```
bin/spark-submit --class org.apache.carbondata.streamer.CarbonDataStreamer \
--master <spark_master_url> \
<carbondata_assembly_jar_path> \
--database-name carbondb \
--target-table test \
--record-key-field name \
--source-ordering-field age \
--source-type dfs \
--dfs-source-input-path /home/root1/Projects/avrodata \
--schema-provider-type FileSchema \
--deduplicate false \
--source-schema-file-path /home/root1/Projects/avroschema/
```

#### Future Scope

Contributions are welcome from community members for supporting the below use cases and proposing new scenarios for streamer tool - 

1. Support JSON format for incoming messages
2. Support full schema evolution