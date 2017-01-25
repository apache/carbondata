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

# Configuring CarbonData
 This tutorial guides you through the advanced configurations of CarbonData :
 
 * [System Configuration](#system-configuration)
 * [Performance Configuration](#performance-configuration)
 * [Miscellaneous Configuration](#miscellaneous-configuration)
 * [Spark Configuration](#spark-configuration)
 
 
##  System Configuration
This section provides the details of all the configurations required for the CarbonData System.

<b><p align="center">System Configuration in carbon.properties</p></b>

| Property | Default Value | Description |
|----------------------------|-------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| carbon.storelocation | /user/hive/warehouse/carbon.store | Location where CarbonData will create the store, and write the data in its own format. NOTE: Store location should be in HDFS. |
| carbon.ddl.base.hdfs.url | hdfs://hacluster/opt/data | This property is used to configure the HDFS relative path, the path configured in carbon.ddl.base.hdfs.url will be appended to the HDFS path configured in fs.defaultFS. If this path is configured, then user need not pass the complete path while dataload. For example: If absolute path of the csv file is hdfs://10.18.101.155:54310/data/cnbc/2016/xyz.csv, the path "hdfs://10.18.101.155:54310" will come from property fs.defaultFS and user can configure the /data/cnbc/ as carbon.ddl.base.hdfs.url. Now while dataload user can specify the csv path as /2016/xyz.csv. |
| carbon.badRecords.location | /opt/Carbon/Spark/badrecords | Path where the bad records are stored. |
| carbon.kettle.home | $SPARK_HOME/carbonlib/carbonplugins | Configuration for loading the data with kettle. |
| carbon.data.file.version | 2 | If this parameter value is set to 1, then CarbonData will support the data load which is in old format(0.x version). If the value is set to 2(1.x onwards version), then CarbonData will support the data load of new format only.|                    

##  Performance Configuration
This section provides the details of all the configurations required for CarbonData Performance Optimization.

<b><p align="center">Performance Configuration in carbon.properties</p></b>

* **Data Loading Configuration**

| Parameter | Default Value | Description | Range |
|--------------------------------------|---------------|----------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| carbon.sort.file.buffer.size | 20 | File read buffer size used during sorting. This value is expressed in MB. | Min=1 and Max=100 |
| carbon.graph.rowset.size | 100000 | Rowset size exchanged between data load graph steps. | Min=500 and Max=1000000 |
| carbon.number.of.cores.while.loading | 6 | Number of cores to be used while loading data. |  |
| carbon.sort.size | 500000 | Record count to sort and write intermediate files to temp. |  |
| carbon.enableXXHash | true | Algorithm for hashmap for hashkey calculation. |  |
| carbon.number.of.cores.block.sort | 7 | Number of cores to use for block sort while loading data. |  |
| carbon.max.driver.lru.cache.size | -1 | Max LRU cache size upto which data will be loaded at the driver side. This value is expressed in MB. Default value of -1 means there is no memory limit for caching. Only integer values greater than 0 are accepted. |  |
| carbon.max.executor.lru.cache.size | -1 | Max LRU cache size upto which data will be loaded at the executor side. This value is expressed in MB. Default value of -1 means there is no memory limit for caching. Only integer values greater than 0 are accepted. If this parameter is not configured, then the carbon.max.driver.lru.cache.size value will be considered. |  |
| carbon.merge.sort.prefetch | true | Enable prefetch of data during merge sort while reading data from sort temp files in data loading. |  |
| carbon.update.persist.enable | true | Enabling this parameter considers persistent data. Enabling this will reduce the execution time of UPDATE operation. |  |



* **Compaction Configuration**
  
| Parameter | Default Value | Description | Range |
|-----------------------------------------------|---------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------|
| carbon.number.of.cores.while.compacting | 2 | Number of cores which are used to write data during compaction. |  |
| carbon.compaction.level.threshold | 4, 3 | This property is for minor compaction which decides how many segments to be merged. Example: If it is set as 2, 3 then minor compaction will be triggered for every 2 segments. 3 is the number of level 1 compacted segment which is further compacted to new segment. | Valid values are from 0-100. |
| carbon.major.compaction.size | 1024 | Major compaction size can be configured using this parameter. Sum of the segments which is below this threshold will be merged. This value is expressed in MB. |  |
| carbon.horizontal.compaction.enable | true | This property is used to turn ON/OFF horizontal compaction. After every DELETE and UPDATE statement, horizontal compaction may occur in case the delta (DELETE/ UPDATE) files becomes more than specified threshold. |  |
| carbon.horizontal.UPDATE.compaction.threshold | 1 | This property specifies the threshold limit on number of UPDATE delta files within a segment. In case the number of delta files goes beyond the threshold, the UPDATE delta files within the segment becomes eligible for horizontal compaction and compacted into single UPDATE delta file. | Values between 1 to 10000. |
| carbon.horizontal.DELETE.compaction.threshold | 1 | This property specifies the threshold limit on number of DELETE delta files within a block of a segment. In case the number of delta files goes beyond the threshold, the DELETE delta files for the particular block of the segment becomes eligible for horizontal compaction and compacted into single DELETE delta file. | Values between 1 to 10000. |

  

* **Query Configuration**
  
| Parameter | Default Value | Description | Range |
|--------------------------------------|---------------|---------------------------------------------------|---------------------------|
| carbon.number.of.cores | 4 | Number of cores to be used while querying. |  |
| carbon.inmemory.record.size | 120000 | Number of records to be in memory while querying. | Min=100000 and Max=240000 |
| carbon.enable.quick.filter | false | Improves the performance of filter query. |  |
| no.of.cores.to.load.blocks.in.driver | 10 | Number of core to load the blocks in driver. |  |


##   Miscellaneous Configuration

<b><p align="center">Extra Configuration in carbon.properties</p></b>

* **Time format for CarbonData** 

| Parameter | Default Format | Description |
|-------------------------|---------------------|--------------------------------------------------------------|
| carbon.timestamp.format | yyyy-MM-dd HH:mm:ss | Timestamp format of input data used for timestamp data type. |

* **Dataload Configuration**
  
| Parameter | Default Value | Description |
|---------------------------------------------|--------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| carbon.sort.file.write.buffer.size | 10485760 | File write buffer size used during sorting. |
| carbon.lock.type | LOCALLOCK | This configuration specifies the type of lock to be acquired during concurrent operations on table. There are following types of lock implementation: - LOCALLOCK: Lock is created on local file system as file. This lock is useful when only one spark driver (thrift server) runs on a machine and no other CarbonData spark application is launched concurrently. - HDFSLOCK: Lock is created on HDFS file system as file. This lock is useful when multiple CarbonData spark applications are launched and no ZooKeeper is running on cluster and HDFS supports file based locking. |
| carbon.sort.intermediate.files.limit | 20 | Minimum number of intermediate files after which merged sort can be started. |
| carbon.block.meta.size.reserved.percentage | 10 | Space reserved in percentage for writing block meta data in CarbonData file. |
| carbon.csv.read.buffersize.byte | 1048576 | csv reading buffer size. |
| high.cardinality.value | 100000 | To identify and apply compression for non-high cardinality columns. |
| carbon.merge.sort.reader.thread | 3 | Maximum no of threads used for reading intermediate files for final merging. |
| carbon.load.metadata.lock.retries | 3 | Maximum number of retries to get the metadata lock for loading data to table. |
| carbon.load.metadata.lock.retry.timeout.sec | 5 | Interval between the retries to get the lock. |
| carbon.tempstore.location | /opt/Carbon/TempStoreLoc | Temporary store location. By default it takes System.getProperty("java.io.tmpdir"). |
| carbon.load.log.counter | 500000 | Data loading records count logger. |   


* **Compaction Configuration**

| Parameter | Default Value | Description |
|-----------------------------------|---------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| carbon.numberof.preserve.segments | 0 | If the user wants to preserve some number of segments from being compacted then he can set this property. Example: carbon.numberof.preserve.segments=2 then 2 latest segments will always be excluded from the compaction. No segments will be preserved by default. |
| carbon.allowed.compaction.days | 0 | Compaction will merge the segments which are loaded with in the specific number of days configured. Example: If the configuration is 2, then the segments which are loaded in the time frame of 2 days only will get merged. Segments which are loaded 2 days apart will not be merged. This is disabled by default. |
| carbon.enable.auto.load.merge | false | To enable compaction while data loading. |

 
* **Query Configuration**

| Parameter | Default Value | Description |
|--------------------------|---------------|-----------------------------------------------------------------------------------------------|
| max.query.execution.time | 60 | Maximum time allowed for one query to be executed. The value is in minutes. |
| carbon.enableMinMax | true | Min max is feature added to enhance query performance. To disable this feature, set it false. | 
  
* **Global Dictionary Configurations**
  
| Parameter | Default Value | Description |
|---------------------------------------|---------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| high.cardinality.identify.enable | true | If the parameter is true, the high cardinality columns of the dictionary code are automatically recognized and these columns will not be used as global dictionary encoding. If the parameter is false, all dictionary encoding columns are used as dictionary encoding. The high cardinality column must meet the following requirements: value of cardinality > configured value of high.cardinalityEqually, the value of cardinality is higher than the threshold.value of cardinality/ row number x 100 > configured value of high.cardinality.row.count.percentageEqually, the ratio of the cardinality value to data row number is higher than the configured percentage. |
| high.cardinality.threshold | 1000000 | high.cardinality.threshold | 1000000 | It is a threshold to identify high cardinality of the columns.If the value of columns' cardinality > the configured value, then the columns are excluded from dictionary encoding. |
| high.cardinality.row.count.percentage | 80 | Percentage to identify whether column cardinality is more than configured percent of total row count.Configuration value formula:Value of cardinality/ row number x 100 > configured value of high.cardinality.row.count.percentageThe value of the parameter must be larger than 0. |
| carbon.cutOffTimestamp | 1970-01-01 05:30:00 | Sets the start date for calculating the timestamp. Java counts the number of milliseconds from start of "1970-01-01 00:00:00". This property is used to customize the start of position. For example "2000-01-01 00:00:00". The date must be in the form "carbon.timestamp.format". NOTE: The CarbonData supports data store up to 68 years from the cut-off time defined. For example, if the cut-off time is 1970-01-01 05:30:00, then the data can be stored up to 2038-01-01 05:30:00. |
| carbon.timegranularity | SECOND | The property used to set the data granularity level DAY, HOUR, MINUTE, or SECOND. |
  
##  Spark Configuration
 <b><p align="center">Spark Configuration Reference in spark-defaults.conf</p></b>
 
| Parameter | Default Value | Description |
|----------------------------------------|--------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| spark.driver.memory | 1g | Amount of memory to be used by the driver process. |
| spark.executor.memory | 1g | Amount of memory to be used per executor process. |
| spark.sql.bigdata.register.analyseRule | org.apache.spark.sql.hive.acl.CarbonAccessControlRules | CarbonAccessControlRules need to be set for enabling Access Control. |
   
 