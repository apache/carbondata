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

# Use Cases

CarbonData is useful in various analytical work loads.Some of the most typical usecases where CarbonData is being used is documented here.

CarbonData is used for but not limited to

- ### Bank

  - fraud detection analysis
  - risk profile analysis
  - As a zip table to update the daily balance of customers

- ### Telecom

  - Detection of signal anamolies for VIP customers for providing improved customer experience
  - Analysis of MR,CHR records of GSM data to determine the tower load at a particular time period and rebalance the tower configuration
  - Analysis of access sites, video, screen size, streaming bandwidth, quality to determine the network quality,routing configuration

- ### Web/Internet

  - Analysis of page or video being accessed,server loads, streaming quality, screen size

- ### Smart City

  - Vehicle tracking analysis
  - Unusual behaviour analysis



These use cases can be broadly classified into below categories:

- Full scan/Detailed/Interactive queries
- Aggregation/OLAP BI queries
- Real time Ingestion(Streaming) and queries



## Detailed Queries in the Telecom scenario

### Scenario

User wants to analyse all the CHR(Call History Record) and MR(Measurement Records) of the mobile subscribers in order to identify the service failures within 10 secs. Also user wants to run machine learning models on the data to fairly estimate the reasons and time of probable failures and take action ahead to meet the SLA(Service Level Agreements) of VIP customers. 

### Challenges

- Data incoming rate might vary based on the user concentration at a particular period of time.Hence higher data load speeds are required
- Cluster needs to be well utilised and share the cluster among various applications for better resource consumption and savings
- Queries needs to be interactive.ie., the queries fetch small data and need to be returned in seconds
- Data Loaded into the system every few minutes.

### Solution

Setup a Hadoop + Spark + CarbonData cluster managed by YARN.

Proposed the following configurations for CarbonData.(These tunings were proposed before CarbonData introduced SORT_COLUMNS parameter using which the sort order and schema order could be different.)

Add the frequently used columns to the left of the table definition. Add it in the increasing order of cardinality. It was suggested to keep msisdn,imsi columns in the beginning of the schema. With latest CarbonData, SORT_COLUMNS needs to be configured msisdn,imsi in the beginning.

Add timestamp column to the right of the schema as it is naturally increasing.

Create two separate YARN queues for Query and Data Loading.

Apart from these, the following CarbonData configuration was suggested to be configured in the cluster.



| Configuration for | Parameter                               | Value  | Description |
|------------------ | --------------------------------------- | ------ | ----------- |
| Data Loading | carbon.graph.rowset.size                | 100000 | Based on the size of each row, this determines the memory required during data loading.Higher value leads to increased memory foot print |
| Data Loading | carbon.number.of.cores.while.loading    | 12     | More cores can improve data loading speed |
| Data Loading | carbon.sort.size                        | 100000 | Number of records to sort at a time.More number of records configured will lead to increased memory foot print |
| Data Loading | table_blocksize                         | 256  | To efficiently schedule multiple tasks during query |
| Data Loading | carbon.sort.intermediate.files.limit    | 100    | Increased to 100 as number of cores are more.Can perform merging in backgorund.If less number of files to merge, sort threads would be idle |
| Data Loading | carbon.use.local.dir                    | TRUE   | yarn application directory will be usually on a single disk.YARN would be configured with multiple disks to be used as temp or to assign randomly to applications. Using the yarn temp directory will allow carbon to use multiple disks and improve IO performance |
| Compaction | carbon.compaction.level.threshold       | 6,6    | Since frequent small loads, compacting more segments will give better query results |
| Compaction | carbon.enable.auto.load.merge           | true   | Since data loading is small,auto compacting keeps the number of segments less and also compaction can complete in  time |
| Compaction | carbon.number.of.cores.while.compacting | 4      | Higher number of cores can improve the compaction speed |
| Compaction | carbon.major.compaction.size            | 921600 | Sum of several loads to combine into single segment |



### Results Achieved

| Parameter                                 | Results          |
| ----------------------------------------- | ---------------- |
| Query                                     | < 3 Sec          |
| Data Loading Speed                        | 40 MB/s Per Node |
| Concurrent query performance (20 queries) | < 10 Sec         |



## Detailed Queries in the Smart City scenario

### Scenario

User wants to analyse the person/vehicle movement and behavior during a certain time period. This output data needs to be joined with a external table for Human details extraction. The query will be run with different time period as filter to identify potential behavior mismatch.

### Challenges

Data generated per day is very huge.Data needs to be loaded multiple times per day to accomodate the incoming data size.

Data Loading done once in 6 hours.

### Solution

Setup a Hadoop + Spark + CarbonData cluster managed by YARN.

Since data needs to be queried for a time period, it was recommended to keep the time column at the beginning of schema.

Use table block size as 512MB.

Use local sort mode.

Apart from these, the following CarbonData configuration was suggested to be configured in the cluster.

Use all columns are no-dictionary as the cardinality is high.

| Configuration for | Parameter                               | Value                   | Description |
| ------------------| --------------------------------------- | ----------------------- | ------------------|
| Data Loading | carbon.graph.rowset.size                | 100000                  | Based on the size of each row, this determines the memory required during data loading.Higher value leads to increased memory foot print |
| Data Loading | enable.unsafe.sort                      | TRUE                    | Temporary data generated during sort is huge which causes GC bottlenecks. Using unsafe reduces the pressure on GC |
| Data Loading | enable.offheap.sort                     | TRUE                    | Temporary data generated during sort is huge which causes GC bottlenecks. Using offheap reduces the pressure on GC.offheap can be accessed through java unsafe.hence enable.unsafe.sort needs to be true |
| Data Loading | offheap.sort.chunk.size.in.mb           | 128                     | Size of memory to allocate for sorting.Can increase this based on the memory available |
| Data Loading | carbon.number.of.cores.while.loading    | 12                      | Higher cores can improve data loading speed |
| Data Loading | carbon.sort.size                        | 100000                  | Number of records to sort at a time.More number of records configured will lead to increased memory foot print |
| Data Loading | table_blocksize                         | 512                     | To efficiently schedule multiple tasks during query. This size depends on data scenario.If data is such that the filters would select less number of blocklets to scan, keeping higher number works well.If the number blocklets to scan is more, better to reduce the size as more tasks can be scheduled in parallel. |
| Data Loading | carbon.sort.intermediate.files.limit    | 100                     | Increased to 100 as number of cores are more.Can perform merging in backgorund.If less number of files to merge, sort threads would be idle |
| Data Loading | carbon.use.local.dir                    | TRUE                    | yarn application directory will be usually on a single disk.YARN would be configured with multiple disks to be used as temp or to assign randomly to applications. Using the yarn temp directory will allow carbon to use multiple disks and improve IO performance |
| Data Loading | sort.inmemory.size.in.mb                | 92160 | Memory allocated to do inmemory sorting. When more memory is available in the node, configuring this will retain more sort blocks in memory so that the merge sort is faster due to no/very less IO |
| Compaction | carbon.major.compaction.size            | 921600                  | Sum of several loads to combine into single segment |
| Compaction | carbon.number.of.cores.while.compacting | 12                      | Higher number of cores can improve the compaction speed.Data size is huge.Compaction need to use more threads to speed up the process |
| Compaction | carbon.enable.auto.load.merge           | FALSE                   | Doing auto minor compaction is costly process as data size is huge.Perform manual compaction when the cluster is less loaded |
| Query | carbon.enable.vector.reader             | true                    | To fetch results faster, supporting spark vector processing will speed up the query |
| Query | enable.unsafe.in.query.procressing      | true                    | Data that needs to be scanned in huge which in turn generates more short lived Java objects. This cause pressure of GC.using unsafe and offheap will reduce the GC overhead |
| Query | use.offheap.in.query.processing         | true                    | Data that needs to be scanned in huge which in turn generates more short lived Java objects. This cause pressure of GC.using unsafe and offheap will reduce the GC overhead.offheap can be accessed through java unsafe.hence enable.unsafe.in.query.procressing needs to be true |
| Query | enable.unsafe.columnpage                | TRUE                    | Keep the column pages in offheap memory so that the memory overhead due to java object is less and also reduces GC pressure. |
| Query | carbon.unsafe.working.memory.in.mb      | 10240                   | Amount of memory to use for offheap operations, you can increase this memory based on the data size |



### Results Achieved

| Parameter                              | Results          |
| -------------------------------------- | ---------------- |
| Query (Time Period spanning 1 segment) | < 10 Sec         |
| Data Loading Speed                     | 45 MB/s Per Node |



## OLAP/BI Queries in the web/Internet scenario

### Scenario

An Internet company wants to analyze the average download speed, kind of handsets used in a particular region/area,kind of Apps being used, what kind of videos are trending in a particular region to enable them to identify the appropriate resolution size of videos to speed up transfer, and perform many more analysis to serve th customers better.

### Challenges

Since data is being queried by a BI tool, all the queries contain group by, which means CarbonData need to return more records as limit cannot be pushed down to carbondata layer.

Results have to be returned faster as the BI tool would not respond till the data is fetched, causing bad user experience.

Data might be loaded less frequently(once or twice in a day), but raw data size is huge, which causes the group by queries to run slower.

Concurrent queries can be more due to the BI dashboard

### Goal

1. Aggregation queries are faster
2. Concurrency is high(Number of concurrent queries supported)

### Solution

- Use table block size as 128MB so that pruning is more effective
- Use global sort mode so that the data to be fetched are grouped together
- Create pre-aggregate tables for non timestamp based group by queries
- For queries containing group by date, create timeseries based Datamap(pre-aggregate) tables so that the data is rolled up during creation and fetch is faster
- Reduce the Spark shuffle partitions.(In our configuration on 14 node cluster, it was reduced to 35 from default of 200)
- Enable global dictionary for columns which have less cardinalities. Aggregation can be done on encoded data, there by improving the performance
- For columns whose cardinality is high,enable the local dictionary so that store size is less and can take dictionary benefit for scan

## Handling near realtime data ingestion scenario

### Scenario

Need to support storing of continously arriving data and make it available immediately for query.

### Challenges

When the data ingestion is near real time and the data needs to be available for query immediately, usual scenario is to do data loading in micro batches.But this causes the problem of generating many small files. This poses two problems:

1. Small file handling in HDFS is inefficient
2. CarbonData will suffer in query performance as all the small files will have to be queried when filter is on non time column

CarbonData will suffer in query performance as all the small files will have to be queried when filter is on non time column.

Since data is continously arriving, allocating resources for compaction might not be feasible.

### Goal

1. Data is available in near real time for query as it arrives
2. CarbonData doesnt suffer from small files problem

### Solution

- Use Streaming tables support of CarbonData
- Configure the carbon.streaming.segment.max.size property to higher value(default is 1GB) if a bit slower query performance is not a concern
- Configure carbon.streaming.auto.handoff.enabled to true so that after the  carbon.streaming.segment.max.size is reached, the segment is converted into format optimized for query
- Disable auto compaction.Manually trigger the minor compaction with default 4,3 when the cluster is not busy
- Manually trigger Major compaction based on the size of segments and the frequency with which the segments are being created
- Enable local dictionary



