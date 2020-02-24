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

## What is CarbonData

CarbonData is a fully indexed columnar and Hadoop native data-store for processing heavy analytical workloads and detailed queries on big data with Spark SQL. CarbonData allows faster interactive queries over PetaBytes of data.



## What does this mean

CarbonData has specially engineered optimizations like multi level indexing, compression and encoding techniques targeted to improve performance of analytical queries which can include filters, aggregation and distinct counts where users expect sub second response time for queries on TB level data on commodity hardware clusters with just a few nodes.

CarbonData has 

- **Unique data organisation** for faster retrievals and minimise amount of data retrieved

- **Advanced push down optimisations** for deep integration with Spark so as to improvise the Spark DataSource API and other experimental features thereby ensure computing is performed close to the data to minimise amount of data read, processed, converted and transmitted(shuffled) 

- **Multi level indexing** to efficiently prune the files and data to be scanned and hence reduce I/O scans and CPU processing

## CarbonData Features & Functions

CarbonData has rich set of features to support various use cases in Big Data analytics. The below table lists the major features supported by CarbonData.



### Table Management

- ##### DDL (Create, Alter,Drop,CTAS)
  
  CarbonData provides its own DDL to create and manage carbondata tables. These DDL conform to Hive,Spark SQL format and support additional properties and configuration to take advantages of CarbonData functionalities.

- ##### DML(Load,Insert)

  CarbonData provides its own DML to manage data in carbondata tables.It adds many customizations through configurations to completely customize the behavior as per user requirement scenarios.

- ##### Update and Delete

  CarbonData supports Update and Delete on Big Data.CarbonData provides the syntax similar to Hive to support IUD operations on CarbonData tables.

- ##### Segment Management

  CarbonData has unique concept of segments to manage incremental loads to CarbonData tables effectively.Segment management helps to easily control the table, perform easy retention, and is also used to provide transaction capability for operations being performed.

- ##### Partition

  CarbonData supports 2 kinds of partitions.1.partition similar to hive partition.2.CarbonData partition supporting hash,list,range partitioning.

- ##### Compaction

  CarbonData manages incremental loads as segments. Compaction helps to compact the growing number of segments and also to improve query filter pruning.

- ##### External Tables

  CarbonData can read any carbondata file and automatically infer schema from the file and provide a relational table view to perform sql queries using Spark or any other applicaion.

### Index

- ##### Bloom filter

  CarbonData supports bloom filter index in order to quickly and efficiently prune the data for scanning and acheive faster query performance.

- ##### Lucene

  Lucene is popular for indexing text data which are long.CarbonData supports lucene index so that text columns can be indexed using lucene and use the index result for efficient pruning of data to be retrieved during query.

- ##### MV (Materialized Views)

  MVs are kind of pre-aggregate and pre-join tables which can support efficient query re-write and processing.CarbonData provides MV which can rewrite query to fetch from any table(including non-carbondata tables). Typical usecase is to store the aggregated data of a non-carbondata fact table into carbondata and use mv to rewrite the query to fetch from carbondata.

### Streaming

- ##### Spark Streaming

  CarbonData supports streaming of data into carbondata in near-realtime and make it immediately available for query.CarbonData provides a DSL to create source and sink tables easily without the need for the user to write his application.

### SDK

- ##### CarbonData writer

  CarbonData supports writing data from non-spark application using SDK.Users can use SDK to generate carbondata files from custom applications. Typical usecase is to write the streaming application plugged in to kafka and use carbondata as sink(target) table for storing.

- ##### CarbonData reader

  CarbonData supports reading of data from non-spark application using SDK. Users can use the SDK to read the carbondata files from their application and do custom processing.

### Storage

- ##### S3

  CarbonData can write to S3, OBS or any cloud storage confirming to S3 protocol. CarbonData uses the HDFS api to write to cloud object stores.

- ##### HDFS

  CarbonData uses HDFS api to write and read data from HDFS. CarbonData can take advantage of the locality information to efficiently suggest spark to run tasks near to the data.

- ##### Alluxio
  CarbonData also supports read and write with [Alluxio](./quick-start-guide.md#alluxio). 


## Integration with Big Data ecosystem

Refer to Integration with [Spark](./quick-start-guide.md#spark), [Presto](./quick-start-guide.md#presto) for detailed information on integrating CarbonData with these execution engines.

## Scenarios where CarbonData is suitable

CarbonData is useful in various analytical work loads.Some of the most typical usecases where CarbonData is being used is [documented here](./usecases.md).



## Performance Results

![Performance Results](../docs/images/carbondata-performance.png?raw=true)
