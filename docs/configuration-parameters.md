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

# Configuring CarbonData
 This tutorial guides you through the advanced configurations of CarbonData :
 
 * [System Configuration](#system-configuration)
 * [Performance Configuration](#performance-configuration)
 * [Miscellaneous Configuration](#miscellaneous-configuration)
 * [Spark Configuration](#spark-configuration)
 * [Dynamic Configuration In CarbonData Using SET-RESET](#dynamic-configuration-in-carbondata-using-set-reset)
 
 
##  System Configuration
This section provides the details of all the configurations required for the CarbonData System.

<b><p align="center">System Configuration in carbon.properties</p></b>

| Property | Default Value | Description |
|----------------------------|-------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| carbon.storelocation | /user/hive/warehouse/carbon.store | Location where CarbonData will create the store, and write the data in its own format. NOTE: Store location should be in HDFS. |
| carbon.ddl.base.hdfs.url | hdfs://hacluster/opt/data | This property is used to configure the HDFS relative path, the path configured in carbon.ddl.base.hdfs.url will be appended to the HDFS path configured in fs.defaultFS. If this path is configured, then user need not pass the complete path while dataload. For example: If absolute path of the csv file is hdfs://10.18.101.155:54310/data/cnbc/2016/xyz.csv, the path "hdfs://10.18.101.155:54310" will come from property fs.defaultFS and user can configure the /data/cnbc/ as carbon.ddl.base.hdfs.url. Now while dataload user can specify the csv path as /2016/xyz.csv. |
| carbon.badRecords.location | /opt/Carbon/Spark/badrecords | Path where the bad records are stored. |
| carbon.data.file.version | 3 | If this parameter value is set to 1, then CarbonData will support the data load which is in old format(0.x version). If the value is set to 2(1.x onwards version), then CarbonData will support the data load of new format only. The default value for this parameter is 3(latest version is set as default version). It improves the query performance by ~20% to 50%. For configuring V3 format explicitly, add carbon.data.file.version = V3 in carbon.properties file. |
| carbon.streaming.auto.handoff.enabled | true | If this parameter value is set to true, auto trigger handoff function will be enabled.|
| carbon.streaming.segment.max.size | 1024000000 | This parameter defines the maximum size of the streaming segment. Setting this parameter to appropriate value will avoid impacting the streaming ingestion. The value is in bytes.|

##  Performance Configuration
This section provides the details of all the configurations required for CarbonData Performance Optimization.

<b><p align="center">Performance Configuration in carbon.properties</p></b>

* **Data Loading Configuration**

| Parameter | Default Value | Description | Range |
|--------------------------------------|---------------|----------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| carbon.number.of.cores.while.loading | 6 | Number of cores to be used while loading data. |  |
| carbon.sort.size | 500000 | Record count to sort and write intermediate files to temp. |  |
| carbon.max.driver.lru.cache.size | -1 | Max LRU cache size upto which data will be loaded at the driver side. This value is expressed in MB. Default value of -1 means there is no memory limit for caching. Only integer values greater than 0 are accepted. |  |
| carbon.max.executor.lru.cache.size | -1 | Max LRU cache size upto which data will be loaded at the executor side. This value is expressed in MB. Default value of -1 means there is no memory limit for caching. Only integer values greater than 0 are accepted. If this parameter is not configured, then the carbon.max.driver.lru.cache.size value will be considered. |  |
| carbon.merge.sort.prefetch | true | Enable prefetch of data during merge sort while reading data from sort temp files in data loading. |  |
| carbon.update.persist.enable | true | Enabling this parameter considers persistent data. Enabling this will reduce the execution time of UPDATE operation. |  |
| carbon.load.global.sort.partitions | 0 | The Number of partitions to use when shuffling data for sort. If user don't configurate or configurate it less than 1, it uses the number of map tasks as reduce tasks. In general, we recommend 2-3 tasks per CPU core in your cluster.
| carbon.options.bad.records.logger.enable | false | Whether to create logs with details about bad records. | |
| carbon.bad.records.action | FORCE | This property can have four types of actions for bad records FORCE, REDIRECT, IGNORE and FAIL. If set to FORCE then it auto-corrects the data by storing the bad records as NULL. If set to REDIRECT then bad records are written to the raw CSV instead of being loaded. If set to IGNORE then bad records are neither loaded nor written to the raw CSV. If set to FAIL then data loading fails if any bad records are found. | |
| carbon.options.is.empty.data.bad.record | false | If false, then empty ("" or '' or ,,) data will not be considered as bad record and vice versa. | |
| carbon.options.bad.record.path |  | Specifies the HDFS path where bad records are stored. By default the value is Null. This path must to be configured by the user if bad record logger is enabled or bad record action redirect. | |
| carbon.enable.vector.reader | true | This parameter increases the performance of select queries as it fetch columnar batch of size 4*1024 rows instead of fetching data row by row. | |
| carbon.blockletgroup.size.in.mb | 64 MB | The data are read as a group of blocklets which are called blocklet groups. This parameter specifies the size of the blocklet group. Higher value results in better sequential IO access.The minimum value is 16MB, any value lesser than 16MB will reset to the default value (64MB). |  |
| carbon.task.distribution | block | **block**: Setting this value will launch one task per block. This setting is suggested in case of concurrent queries and queries having big shuffling scenarios. **custom**: Setting this value will group the blocks and distribute it uniformly to the available resources in the cluster. This enhances the query performance but not suggested in case of concurrent queries and queries having big shuffling scenarios. **blocklet**: Setting this value will launch one task per blocklet. This setting is suggested in case of concurrent queries and queries having big shuffling scenarios. **merge_small_files**: Setting this value will merge all the small partitions to a size of (128 MB is the default value of "spark.sql.files.maxPartitionBytes",it is configurable) during querying. The small partitions are combined to a map task to reduce the number of read task. This enhances the performance. | | 

* **Compaction Configuration**
  
| Parameter | Default Value | Description | Range |
|-----------------------------------------------|---------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------|
| carbon.number.of.cores.while.compacting | 2 | Number of cores which are used to write data during compaction. |  |
| carbon.compaction.level.threshold | 4, 3 | This property is for minor compaction which decides how many segments to be merged. Example: If it is set as 2, 3 then minor compaction will be triggered for every 2 segments. 3 is the number of level 1 compacted segment which is further compacted to new segment. | Valid values are from 0-100. |
| carbon.major.compaction.size | 1024 | Major compaction size can be configured using this parameter. Sum of the segments which is below this threshold will be merged. This value is expressed in MB. |  |
| carbon.horizontal.compaction.enable | true | This property is used to turn ON/OFF horizontal compaction. After every DELETE and UPDATE statement, horizontal compaction may occur in case the delta (DELETE/ UPDATE) files becomes more than specified threshold. |  |
| carbon.horizontal.UPDATE.compaction.threshold | 1 | This property specifies the threshold limit on number of UPDATE delta files within a segment. In case the number of delta files goes beyond the threshold, the UPDATE delta files within the segment becomes eligible for horizontal compaction and compacted into single UPDATE delta file. | Values between 1 to 10000. |
| carbon.horizontal.DELETE.compaction.threshold | 1 | This property specifies the threshold limit on number of DELETE delta files within a block of a segment. In case the number of delta files goes beyond the threshold, the DELETE delta files for the particular block of the segment becomes eligible for horizontal compaction and compacted into single DELETE delta file. | Values between 1 to 10000. |
| carbon.update.segment.parallelism | 1 | This property specifies the parallelism for each segment during update. If there are segments that contain too many records to update and the spark job encounter data-spill related errors, it is better to increase this property value. It is recommended to set this value to a multiple of the number of executors for balance. | Values between 1 to 1000. |
  

* **Query Configuration**
  
| Parameter | Default Value | Description | Range |
|--------------------------------------|---------------|---------------------------------------------------|---------------------------|
| carbon.number.of.cores | 4 | Number of cores to be used while querying. |  |
| carbon.enable.quick.filter | false | Improves the performance of filter query. |  |


##   Miscellaneous Configuration

<b><p align="center">Extra Configuration in carbon.properties</p></b>

* **Time format for CarbonData** 

| Parameter | Default Format | Description |
|-------------------------|---------------------|--------------------------------------------------------------|
| carbon.timestamp.format | yyyy-MM-dd HH:mm:ss | Timestamp format of input data used for timestamp data type. |

* **Dataload Configuration**
  
| Parameter | Default Value | Description |
|---------------------------------------------|--------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| carbon.sort.file.write.buffer.size | 16777216 | File write buffer size used during sorting (minValue = 10 KB, maxValue=10MB). |
| carbon.lock.type | LOCALLOCK | This configuration specifies the type of lock to be acquired during concurrent operations on table. There are following types of lock implementation: - LOCALLOCK: Lock is created on local file system as file. This lock is useful when only one spark driver (thrift server) runs on a machine and no other CarbonData spark application is launched concurrently. - HDFSLOCK: Lock is created on HDFS file system as file. This lock is useful when multiple CarbonData spark applications are launched and no ZooKeeper is running on cluster and HDFS supports file based locking. |
| carbon.sort.intermediate.files.limit | 20 | Minimum number of intermediate files after which merged sort can be started (minValue = 2, maxValue=50). |
| carbon.block.meta.size.reserved.percentage | 10 | Space reserved in percentage for writing block meta data in CarbonData file. |
| carbon.csv.read.buffersize.byte | 1048576 | csv reading buffer size. |
| high.cardinality.value | 100000 | To identify and apply compression for non-high cardinality columns. |
| carbon.merge.sort.reader.thread | 3 | Maximum no of threads used for reading intermediate files for final merging. |
| carbon.concurrent.lock.retries | 100 | Specifies the maximum number of retries to obtain the lock for concurrent operations. This is used for concurrent loading. |
| carbon.concurrent.lock.retry.timeout.sec | 1 | Specifies the interval between the retries to obtain the lock for concurrent operations. |
| carbon.lock.retries | 3 | Specifies the maximum number of retries to obtain the lock for any operations other than load. |
| carbon.lock.retry.timeout.sec | 5 | Specifies the interval between the retries to obtain the lock for any operation other than load. |
| carbon.tempstore.location | /opt/Carbon/TempStoreLoc | Temporary store location. By default it takes System.getProperty("java.io.tmpdir"). |
| carbon.load.log.counter | 500000 | Data loading records count logger. |
| carbon.skip.empty.line | false | Setting this property ignores the empty lines in the CSV file during the data load |
| carbon.enable.calculate.size | true | **For Load Operation**: Setting this property calculates the size of the carbon data file (.carbondata) and carbon index file (.carbonindex) for every load and updates the table status file. **For Describe Formatted**: Setting this property calculates the total size of the carbon data files and carbon index files for the respective table and displays in describe formatted command. | 



* **Compaction Configuration**

| Parameter | Default Value | Description |
|-----------------------------------|---------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| carbon.numberof.preserve.segments | 0 | If the user wants to preserve some number of segments from being compacted then he can set this property. Example: carbon.numberof.preserve.segments = 2 then 2 latest segments will always be excluded from the compaction. No segments will be preserved by default. |
| carbon.allowed.compaction.days | 0 | Compaction will merge the segments which are loaded with in the specific number of days configured. Example: If the configuration is 2, then the segments which are loaded in the time frame of 2 days only will get merged. Segments which are loaded 2 days apart will not be merged. This is disabled by default. |
| carbon.enable.auto.load.merge | false | To enable compaction while data loading. |
|carbon.enable.page.level.reader.in.compaction|true|Enabling page level reader for compaction reduces the memory usage while compacting more number of segments. It allows reading only page by page instead of reading whole blocklet to memory.|

 
* **Query Configuration**

| Parameter | Default Value | Description |
|--------------------------|---------------|-----------------------------------------------------------------------------------------------|
| max.query.execution.time | 60 | Maximum time allowed for one query to be executed. The value is in minutes. |
| carbon.enableMinMax | true | Min max is feature added to enhance query performance. To disable this feature, set it false. |
| carbon.dynamicallocation.schedulertimeout | 5 | Specifies the maximum time (unit in seconds) the scheduler can wait for executor to be active. Minimum value is 5 sec and maximum value is 15 sec. |
| carbon.scheduler.minregisteredresourcesratio | 0.8 | Specifies the minimum resource (executor) ratio needed for starting the block distribution. The default value is 0.8, which indicates 80% of the requested resource is allocated for starting block distribution.  The minimum value is 0.1 min and the maximum value is 1.0. | 
  
* **Global Dictionary Configurations**
  
| Parameter | Default Value | Description |
|---------------------------------------|---------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| carbon.cutOffTimestamp | 1970-01-01 05:30:00 | Sets the start date for calculating the timestamp. Java counts the number of milliseconds from start of "1970-01-01 00:00:00". This property is used to customize the start of position. For example "2000-01-01 00:00:00". The date must be in the form "carbon.timestamp.format". |
| carbon.timegranularity | SECOND | The property used to set the data granularity level DAY, HOUR, MINUTE, or SECOND. |
  
##  Spark Configuration
 <b><p align="center">Spark Configuration Reference in spark-defaults.conf</p></b>
 
| Parameter | Default Value | Description |
|----------------------------------------|--------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| spark.driver.memory | 1g | Amount of memory to be used by the driver process. |
| spark.executor.memory | 1g | Amount of memory to be used per executor process. |

##  Dynamic Configuration In CarbonData Using SET-RESET

**SET/RESET** commands are used to add, update, display, or reset the carbondata properties dynamically without restarting the driver.

**Syntax**

* **Add or Update :** This command adds or updates the value of parameter_name.

```
SET parameter_name=parameter_value
```

* Display Property Value: This command displays the value of the specified parameter_name.

```
SET parameter_name
```

* Display Session Parameters: This command displays all the supported session parameters.

```
SET
```

* Display Session Parameters along with usage details: This command displays all the supported session parameters along with their usage details.

```
SET -v
```

* Reset: This command clears all the session parameters.

```
RESET
```

 **Parameter Description:**

| Parameter       | Description                                                                            |
|-----------------|----------------------------------------------------------------------------------------|
| parameter_name  | Name of the property whose value needs to be dynamically added, updated, or displayed. |
| parameter_value | New value of the parameter_name to be set.                                             |

<b><p align="center">Dynamically Configurable Properties of CarbonData</p></b>

| Properties                               | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
|------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| carbon.options.bad.records.logger.enable | To enable or disable bad record logger.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| carbon.options.bad.records.action        | This property can have four types of actions for bad records FORCE, REDIRECT, IGNORE and FAIL. If set to FORCE then it auto-corrects the data by storing the bad records as NULL. If set to REDIRECT then bad records are written to the raw CSV instead of being loaded. If set to IGNORE then bad records are neither loaded nor written to the raw CSV. If set to FAIL then data loading fails if any bad records are found.                                                                                                                                                          |
| carbon.options.is.empty.data.bad.record  | If false, then empty ("" or '' or ,,) data will not be considered as bad record and vice versa.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| carbon.options.batch.sort.size.inmb      | Size of batch data to keep in memory, as a thumb rule it supposed to be less than 45% of sort.inmemory.size.inmb otherwise it may spill intermediate data to disk.                                                                                                                                                                                                                                                                                                                                                                                                                       |
| carbon.options.single.pass               | Single Pass Loading enables single job to finish data loading with dictionary generation on the fly. It enhances performance in the scenarios where the subsequent data loading after initial load involves fewer incremental updates on the dictionary. This option specifies whether to use single pass for loading data or not. By default this option is set to FALSE.                                                                                                                                                                                                               |
| carbon.options.bad.record.path           | Specifies the HDFS path where bad records needs to be stored.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| carbon.custom.block.distribution         | Specifies whether to use the Spark or Carbon block distribution feature.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| enable.unsafe.sort                       | Specifies whether to use unsafe sort during data loading. Unsafe sort reduces the garbage collection during data load operation, resulting in better performance.                                                                                                                                                                                                                                                                                                                                                                                                                        |

**Examples:**

* Add or Update:

```
SET enable.unsafe.sort =true
```

* Display Property Value:

```
SET enable.unsafe.sort
```

* Reset:

```
RESET
```

**System Response:**

* Success will be recorded in the driver log.

* Failure will be displayed in the UI.
