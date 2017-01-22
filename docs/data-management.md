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

# Data Management
This tutorial is going to introduce you to the conceptual details of data management like:

* [Loading Data](#loading-data)
* [Deleting Data](#deleting-data)
* [Compacting Data](#compacting-data)
* [Updating Data](#updating-data)

## Loading Data

* **Scenario**

   After creating a table, you can load data to the table using the [LOAD DATA](dml-operation-on-carbondata.md) command. The loaded data is available for querying.
   When data load is triggered, the data is encoded in CarbonData format and copied into HDFS CarbonData store path (specified in carbon.properties file) 
   in compressed, multi dimensional columnar format for quick analysis queries. The same command can be used to load new data or to
   update the existing data. Only one data load can be triggered for one table. The high cardinality columns of the dictionary encoding are 
   automatically recognized and these columns will not be used for dictionary encoding.

* **Procedure**
  
   Data loading is a process that involves execution of multiple steps to read, sort and encode the data in CarbonData store format.
   Each step is executed on different threads. After data loading process is complete, the status (success/partial success) is updated to 
   CarbonData store metadata. The table below lists the possible load status.
   
   
| Status | Description |
|-----------------|------------------------------------------------------------------------------------------------------------|
| Success | All the data is loaded into table and no bad records found. |
| Partial Success | Data is loaded into table and bad records are found. Bad records are stored at carbon.badrecords.location. |
   
   In case of failure, the error will be logged in error log. Details of loads can be seen with [SHOW SEGMENTS](dml-operation-on-carbondata.md) command. The show segment command output consists of :
   
   - SegmentSequenceID
   - START_TIME OF LOAD
   - END_TIME OF LOAD 
   - LOAD STATUS
 
   The latest load will be displayed first in the output.
   
   Refer to [DML operations on CarbonData](dml-operation-on-carbondata.md) for load commands.
   
   
## Deleting Data  

* **Scenario**
   
   If you have loaded wrong data into the table, or too many bad records are present and you want to modify and reload the data, you can delete required data loads. 
   The load can be deleted using the Segment Sequence Id or if the table contains date field then the data can be deleted using the date field.
   If there are some specific records that need to be deleted based on some filter condition(s) we can delete by records.
   
* **Procedure** 

   The loaded data can be deleted in the following ways:
   
   * Delete by Segment ID
      
      After you get the segment ID of the segment that you want to delete, execute the delete command for the selected segment.
      The status of deleted segment is updated to Marked for delete / Marked for Update.
      
| SegmentSequenceId | Status            | Load Start Time      | Load End Time        |
|-------------------|-------------------|----------------------|----------------------|
| 0                 | Success           | 2015-11-19 19:14:... | 2015-11-19 19:14:... |
| 1                 | Marked for Update | 2015-11-19 19:54:... | 2015-11-19 20:08:... |
| 2                 | Marked for Delete | 2015-11-19 20:25:... | 2015-11-19 20:49:... |

   * Delete by Date Field
   
      If the table contains date field, you can delete the data based on a specific date.

   * Delete by Record

      To delete records from CarbonData table based on some filter Condition(s).
      
      For delete commands refer to [DML operations on CarbonData](dml-operation-on-carbondata.md).
      
   * **NOTE**:
    
     - When the delete segment DML is called, segment will not be deleted physically from the file system. Instead the segment status will be marked as "Marked for Delete". For the query execution, this deleted segment will be excluded.
     
     - The deleted segment will be deleted physically during the next load operation and only after the maximum query execution time configured using "max.query.execution.time". By default it is 60 minutes.
     
     - If the user wants to force delete the segment physically then he can use CLEAN FILES Command.
        
Example :
          
```
CLEAN FILES FOR TABLE table1
```

 This DML will physically delete the segment which are "Marked for delete" immediately.

## Compacting Data
      
* **Scenario**
  
  Frequent data ingestion results in several fragmented CarbonData files in the store directory. Since data is sorted only within each load, the indices perform only within each 
  load. This means that there will be one index for each load and as number of data load increases, the number of indices also increases. As each index works only on one load, 
  the performance of indices is reduced. CarbonData provides provision for compacting the loads. Compaction process combines several segments into one large segment by merge sorting the data from across the segments.  
      
* **Procedure**

  There are two types of compaction Minor and Major compaction.
  
  - **Minor Compaction**
    
     In minor compaction the user can specify how many loads to be merged. Minor compaction triggers for every data load if the parameter carbon.enable.auto.load.merge is set. If any segments are available to be merged, then compaction will 
     run parallel with data load. There are 2 levels in minor compaction.
     
     - Level 1: Merging of the segments which are not yet compacted.
     - Level 2: Merging of the compacted segments again to form a bigger segment. 
  - **Major Compaction**
     
     In Major compaction, many segments can be merged into one big segment. User will specify the compaction size until which segments can be merged. Major compaction is usually done during the off-peak time. 
      
   There are number of parameters related to Compaction that can be set in carbon.properties file 
   
| Parameter | Default | Application | Description | Valid Values |
|-----------------------------------------|---------|-------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------|
| carbon.compaction.level.threshold | 4, 3 | Minor | This property is for minor compaction which decides how many segments to be merged. Example: If it is set as 2, 3 then minor compaction will be triggered for every 2 segments. 3 is the number of level 1 compacted segment which is further compacted to new segment. | NA |
| carbon.major.compaction.size | 1024 MB | Major | Major compaction size can be configured using this parameter. Sum of the segments which is below this threshold will be merged. | NA |
| carbon.numberof.preserve.segments | 0 | Minor/Major | If the user wants to preserve some number of segments from being compacted then he can set this property. Example: carbon.numberof.preserve.segments=2 then 2 latest segments will always be excluded from the compaction. No segments will be preserved by default. | 0-100 |
| carbon.allowed.compaction.days | 0 | Minor/Major | Compaction will merge the segments which are loaded within the specific number of days configured. Example: If the configuration is 2, then the segments which are loaded in the time frame of 2 days only will get merged. Segments which are loaded 2 days apart will not be merged. This is disabled by default. | 0-100 |
| carbon.number.of.cores.while.compacting | 2 | Minor/Major | Number of cores which is used to write data during compaction. | 0-100 |   
  
   For compaction commands refer to [DDL operations on CarbonData](ddl-operation-on-carbondata.md)

## Updating Data

* **Scenario**

    Sometimes after the data has been ingested into the System, it is required to be updated. Also there may be situations where some specific columns need to be updated
    on the basis of column expression and optional filter conditions.

* **Procedure**

    To update we need to specify the column expression with an optional filter condition(s).

    For update commands refer to [DML operations on CarbonData](dml-operation-on-carbondata.md).


    




 
 
