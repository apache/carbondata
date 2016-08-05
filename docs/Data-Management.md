<!--
    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.
-->

* [Load Data](#Load Data)
* [Deleting Data](#Deleting Data)
* [Compacting Data](#Compacting Data)


***


# Load Data
### Scenario
Once the table is created, data can be loaded into table using LOAD DATA command and will be available for query. When data load is triggered, the data is encoded in Carbon format and copied into HDFS Carbon store path(mentioned in carbon.properties file) in compressed, multi dimentional columnar format for quick analysis queries.
The same command can be used for loading the new data or to update the existing data.
Only one data load can be triggered for one table. The high cardinality columns of the dictionary encoding are automatically recognized and these columns will not be used as dictionary encoding.

### Procedure

Data loading is a process that involves execution of various steps to read, sort, and encode the date in Carbon store format. Each step will be executed in different threads.
After data loading process is complete, the status (success/partial success) will be updated to Carbon store metadata. Following are the data load status:

1. Success: All the data is loaded into table and no bad records found.
2. Partial Success: Data is loaded into table and bad records are found. Bad records are stored at carbon.badrecords.location.

In case of failure, the error will be logged in error log.
Details of loads can be seen with SHOW SEGMENTS command.
* Sequence Id
* Status of data load
* Load Start time
* Load End time
Following steps needs to be performed for invoking data load.
Run the following command for historical data load:
```ruby
LOAD DATA [LOCAL] INPATH 'folder_path' [OVERWRITE] INTO TABLE [db_name.]table_name
OPTIONS(property_name=property_value, ...)
```
OPTIONS are not mandatory for data loading process. Inside OPTIONS user can provide either of any options like DELIMITER,QUOTECHAR, ESCAPERCHAR,MULTILINE as per need.

Note: The path shall be canonical path.

***

# Deleting Data
### Scenario
If you have loaded wrong data into the table, or too many bad records and wanted to modify and reload the data, you can delete required loads data. The load can be deleted using the load ID or if the table contains date field then the data can be deleted using the date field.

### Delete by Segment ID

Each segment has a unique segment ID associated with it. Using this segment ID, you can remove the segment.
Run the following command to get the segmentID.
```ruby
SHOW SEGMENTS FOR Table dbname.tablename LIMIT number_of_segments
```
Example:
```ruby
SHOW SEGMENTS FOR TABLE carbonTable
```
The above command will show all the segments of the table carbonTable.
```ruby
SHOW SEGMENTS FOR TABLE carbonTable LIMIT 3
```
The above DDL will show only limited number of segments specified by number_of_segments.

output: 

| SegmentSequenceId | Status | Load Start Time | Load End Time | 
|--------------|-----------------|--------------------|--------------------| 
| 2| Success | 2015-11-19 20:25:... | 2015-11-19 20:49:... | 
| 1| Marked for Delete | 2015-11-19 19:54:... | 2015-11-19 20:08:... | 
| 0| Marked for Update | 2015-11-19 19:14:... | 2015-11-19 19:14:... | 
 
The show segment command output consists of SegmentSequenceID, START_TIME OF LOAD, END_TIME OF LOAD, and LOAD STATUS. The latest load will be displayed first in the output.
After you get the segment ID of the segment that you want to delete, execute the following command to delete the selected segment.
Command:
```ruby
DELETE SEGMENT segment_sequence_id1, segments_sequence_id2, .... FROM TABLE tableName
```
Example:
```ruby
DELETE SEGMENT l,2,3 FROM TABLE carbonTable
```

### Delete by Date Field

If the table contains date field, you can delete the data based on a specific date.
Command:
```ruby
DELETE FROM TABLE [schema_name.]table_name WHERE[DATE_FIELD]BEFORE [DATE_VALUE]
```
Example:
```ruby
DELETE FROM TABLE table_name WHERE productionDate BEFORE '2017-07-01'
```
Here productionDate is the column of type time stamp.
The above DDL will delete all the data before the date '2017-07-01'.


Note: 
* When the delete segment DML is called, segment will not be deleted physically from the file system. Instead the segment status will be marked as "Marked for Delete". For the query execution, this deleted segment will be excluded.
* The deleted segment will be deleted physically during the next load operation and only after the maximum query execution time configured using "max.query.execution.time". By default it is 60 minutes.
* If the user wants to force delete the segment physically then he can use CLEAN FILES DML.
Example:
```ruby
CLEAN FILES FOR TABLE table1
```
This DML will physically delete the segment which are "Marked for delete" immediately.



***

# Compacting Data
### Scenario
Frequent data ingestion results in several fragmented carbon files in the store directory. Since data is sorted only within each load, the indices perform only within each load. This mean that there will be one index for each load and as number of data load increases, the number of indices also increases. As each index works only on one load, the performance of indices is reduced. Carbon provides provision for compacting the loads. Compaction process combines several segments into one large segment by merge sorting the data from across the segments.

### Prerequisite

 The data should be loaded multiple times.

### Procedure

There are two types of compaction Minor and Major compaction.

#### Minor Compaction:
In minor compaction the user can specify how many loads to be merged. Minor compaction triggers for every data load if the parameter carbon.enable.auto.load.merge is set. If any segments are available to be merged, then compaction will run parallel with data load. 

There are 2 levels in minor compaction.
* Level 1: Merging of the segments which are not yet compacted.
* Level 2: Merging of the compacted segments again to form a bigger segment.
    
#### Major Compaction:
In Major compaction, many segments can be merged into one big segment. User will specify the compaction size until which segments can be merged. Major compaction is usually done during the off-peak time.

### Parameters of Compaction
| Parameter | Default | Applicable | Description | 
| --------- | --------| -----------|-------------|
| carbon.compaction.level.threshold | 4,3 | Minor | This property is for minor compaction which decides how many segments to be merged.**Example**: if it is set as 2,3 then minor compaction will be triggered for every 2 segments. 3 is the number of level 1 compacted segment which is further compacted to new segment.
Valid values are from 0-100. |
| carbon.major.compaction.size | 1024 mb | Major | Major compaction size can be configured using this parameter. Sum of the segments which is below this threshold will be merged. |
| carbon.numberof.preserve.segments | 0 | Minor/Major| If the user wants to preserve some number of segments from being compacted then he can set this property.**Example**:carbon.numberof.preserve.segments=2 then 2 latest segments will always be excluded from the compaction.No segments will be preserved by default. |
| carbon.allowed.compaction.days | 0 | Minor/Major| Compaction will merge the segments which are loaded with in the specific number of days configured.**Example**: if the configuration is 2, then the segments which are loaded in the time frame of 2 days only will get merged. Segments which are loaded 2 days apart will not be merged.This is disabled by default. |
| carbon.number.of.cores.while.compacting | 2 | Minor/Major| Number of cores which is used to write data during compaction. |
