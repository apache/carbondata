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

# FAQs

* [What are Bad Records?](#what-are-bad-records)
* [Where are Bad Records Stored in CarbonData?](#where-are-bad-records-stored-in-carbondata)
* [How to enable Bad Record Logging?](#how-to-enable-bad-record-logging)
* [How to ignore the Bad Records?](#how-to-ignore-the-bad-records)
* [How to specify store location while creating carbon session?](#how-to-specify-store-location-while-creating-carbon-session)
* [What is Carbon Lock Type?](#what-is-carbon-lock-type)
* [How to resolve Abstract Method Error?](#how-to-resolve-abstract-method-error)
* [How Carbon will behave when execute insert operation in abnormal scenarios?](#how-carbon-will-behave-when-execute-insert-operation-in-abnormal-scenarios)
* [Why aggregate query is not fetching data from aggregate table?](#why-aggregate-query-is-not-fetching-data-from-aggregate-table)
* [Why all executors are showing success in Spark UI even after Dataload command failed at Driver side?](#why-all-executors-are-showing-success-in-spark-ui-even-after-dataload-command-failed-at-driver-side)
* [Why different time zone result for select query output when query SDK writer output?](#why-different-time-zone-result-for-select-query-output-when-query-sdk-writer-output)
* [How to check LRU cache memory footprint?](#how-to-check-lru-cache-memory-footprint)

# TroubleShooting

- [Getting tablestatus.lock issues When loading data](#getting-tablestatuslock-issues-when-loading-data)
- [Failed to load thrift libraries](#failed-to-load-thrift-libraries)
- [Failed to launch the Spark Shell](#failed-to-launch-the-spark-shell)
- [Failed to execute load query on cluster](#failed-to-execute-load-query-on-cluster)
- [Failed to execute insert query on cluster](#failed-to-execute-insert-query-on-cluster)
- [Failed to connect to hiveuser with thrift](#failed-to-connect-to-hiveuser-with-thrift)
- [Failed to read the metastore db during table creation](#failed-to-read-the-metastore-db-during-table-creation)
- [Failed to load data on the cluster](#failed-to-load-data-on-the-cluster)
- [Failed to insert data on the cluster](#failed-to-insert-data-on-the-cluster)
- [Failed to execute Concurrent Operations(Load,Insert,Update) on table by multiple workers](#failed-to-execute-concurrent-operations-on-table-by-multiple-workers)
- [Failed to create a table with a single numeric column](#failed-to-create-a-table-with-a-single-numeric-column)

## 

## What are Bad Records?

Records that fail to get loaded into the CarbonData due to data type incompatibility or are empty or have incompatible format are classified as Bad Records.

## Where are Bad Records Stored in CarbonData?
The bad records are stored at the location set in carbon.badRecords.location in carbon.properties file.
By default **carbon.badRecords.location** specifies the following location ``/opt/Carbon/Spark/badrecords``.

## How to enable Bad Record Logging?
While loading data we can specify the approach to handle Bad Records. In order to analyse the cause of the Bad Records the parameter ``BAD_RECORDS_LOGGER_ENABLE`` must be set to value ``TRUE``. There are multiple approaches to handle Bad Records which can be specified  by the parameter ``BAD_RECORDS_ACTION``.

- To pass the incorrect values of the csv rows with NULL value and load the data in CarbonData, set the following in the query :
```
'BAD_RECORDS_ACTION'='FORCE'
```

- To write the Bad Records without passing incorrect values with NULL in the raw csv (set in the parameter **carbon.badRecords.location**), set the following in the query :
```
'BAD_RECORDS_ACTION'='REDIRECT'
```

## How to ignore the Bad Records?
To ignore the Bad Records from getting stored in the raw csv, we need to set the following in the query :
```
'BAD_RECORDS_ACTION'='IGNORE'
```

## How to specify store location while creating carbon session?
The store location specified while creating carbon session is used by the CarbonData to store the meta data like the schema, dictionary files, dictionary meta data and sort indexes.

Try creating ``carbonsession`` with ``storepath`` specified in the following manner :

```
val carbon = SparkSession.builder().config(sc.getConf).getOrCreateCarbonSession(<carbon_store_path>)
```
Example:

```
val carbon = SparkSession.builder().config(sc.getConf).getOrCreateCarbonSession("hdfs://localhost:9000/carbon/store")
```

## What is Carbon Lock Type?
The Apache CarbonData acquires lock on the files to prevent concurrent operation from modifying the same files. The lock can be of the following types depending on the storage location, for HDFS we specify it to be of type HDFSLOCK. By default it is set to type LOCALLOCK.
The property carbon.lock.type configuration specifies the type of lock to be acquired during concurrent operations on table. This property can be set with the following values :
- **LOCALLOCK** : This Lock is created on local file system as file. This lock is useful when only one spark driver (thrift server) runs on a machine and no other CarbonData spark application is launched concurrently.
- **HDFSLOCK** : This Lock is created on HDFS file system as file. This lock is useful when multiple CarbonData spark applications are launched and no ZooKeeper is running on cluster and the HDFS supports, file based locking.

## How to resolve Abstract Method Error?
In order to build CarbonData project it is necessary to specify the spark profile. The spark profile sets the Spark Version. You need to specify the ``spark version`` while using Maven to build project.

## How Carbon will behave when execute insert operation in abnormal scenarios?
Carbon support insert operation, you can refer to the syntax mentioned in [DML Operations on CarbonData](./dml-of-carbondata.md).
First, create a source table in spark-sql and load data into this created table.

```
CREATE TABLE source_table(
id String,
name String,
city String)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ",";
```

```
SELECT * FROM source_table;
id  name    city
1   jack    beijing
2   erlu    hangzhou
3   davi    shenzhen
```

**Scenario 1** :

Suppose, the column order in carbon table is different from source table, use script "SELECT * FROM carbon table" to query, will get the column order similar as source table, rather than in carbon table's column order as expected. 

```
CREATE TABLE IF NOT EXISTS carbon_table(
id String,
city String,
name String)
STORED AS carbondata;
```

```
INSERT INTO TABLE carbon_table SELECT * FROM source_table;
```

```
SELECT * FROM carbon_table;
id  city    name
1   jack    beijing
2   erlu    hangzhou
3   davi    shenzhen
```

As result shows, the second column is city in carbon table, but what inside is name, such as jack. This phenomenon is same with insert data into hive table.

If you want to insert data into corresponding column in carbon table, you have to specify the column order same in insert statement. 

```
INSERT INTO TABLE carbon_table SELECT id, city, name FROM source_table;
```

**Scenario 2** :

Insert operation will be failed when the number of column in carbon table is different from the column specified in select statement. The following insert operation will be failed.

```
INSERT INTO TABLE carbon_table SELECT id, city FROM source_table;
```

**Scenario 3** :

When the column type in carbon table is different from the column specified in select statement. The insert operation will still success, but you may get NULL in result, because NULL will be substitute value when conversion type failed.

## Why aggregate query is not fetching data from aggregate table?
Following are the aggregate queries that won't fetch data from aggregate table:

- **Scenario 1** :
When SubQuery predicate is present in the query.

Example:

```
create table gdp21(cntry smallint, gdp double, y_year date) stored as carbondata;
create datamap ag1 on table gdp21 using 'preaggregate' as select cntry, sum(gdp) from gdp21 group by cntry;
select ctry from pop1 where ctry in (select cntry from gdp21 group by cntry);
```

- **Scenario 2** : 
When aggregate function along with 'in' filter.

Example:

```
create table gdp21(cntry smallint, gdp double, y_year date) stored as carbondata;
create datamap ag1 on table gdp21 using 'preaggregate' as select cntry, sum(gdp) from gdp21 group by cntry;
select cntry, sum(gdp) from gdp21 where cntry in (select ctry from pop1) group by cntry;
```

- **Scenario 3** : 
When aggregate function having 'join' with equal filter.

Example:

```
create table gdp21(cntry smallint, gdp double, y_year date) stored as carbondata;
create datamap ag1 on table gdp21 using 'preaggregate' as select cntry, sum(gdp) from gdp21 group by cntry;
select cntry,sum(gdp) from gdp21,pop1 where cntry=ctry group by cntry;
```

## Why all executors are showing success in Spark UI even after Dataload command failed at Driver side?
Spark executor shows task as failed after the maximum number of retry attempts, but loading the data having bad records and BAD_RECORDS_ACTION (carbon.bad.records.action) is set as "FAIL" will attempt only once but will send the signal to driver as failed instead of throwing the exception to retry, as there is no point to retry if bad record found and BAD_RECORDS_ACTION is set to fail. Hence the Spark executor displays this one attempt as successful but the command has actually failed to execute. Task attempts or executor logs can be checked to observe the failure reason.

## Why different time zone result for select query output when query SDK writer output? 
SDK writer is an independent entity, hence SDK writer can generate carbondata files from a non-cluster machine that has different time zones. But at cluster when those files are read, it always takes cluster time-zone. Hence, the value of timestamp and date datatype fields are not original value.
If wanted to control timezone of data while writing, then set cluster's time-zone in SDK writer by calling below API.
```
TimeZone.setDefault(timezoneValue)
```
**Example:**
``` 
cluster timezone is Asia/Shanghai
TimeZone.setDefault(TimeZone.getTimeZone("Asia/Shanghai"))
```

## How to check LRU cache memory footprint?
To observe the LRU cache memory footprint in the logs, configure the below properties in log4j.properties file.
```
log4j.logger.org.apache.carbondata.core.cache.CarbonLRUCache = DEBUG
```
This property will enable the DEBUG log for the CarbonLRUCache and UnsafeMemoryManager which will print the information of memory consumed using which the LRU cache size can be decided. **Note:** Enabling the DEBUG log will degrade the query performance. Ensure carbon.max.driver.lru.cache.size is configured to observe the current cache size.

**Example:**
```
18/09/26 15:05:29 DEBUG CarbonLRUCache: main Required size for entry /home/target/store/default/stored_as_carbondata_table/Fact/Part0/Segment_0/0_1537954529044.carbonindexmerge :: 181 Current cache size :: 0
18/09/26 15:05:30 INFO CarbonLRUCache: main Removed entry from InMemory lru cache :: /home/target/store/default/stored_as_carbondata_table/Fact/Part0/Segment_0/0_1537954529044.carbonindexmerge
```
**Note:** If  `Removed entry from InMemory LRU cache` are frequently observed in logs, you may have to increase the configured LRU size.

To observe the LRU cache from heap dump, check the heap used by CarbonLRUCache class.
## Getting tablestatus.lock issues When loading data

  **Symptom**
```
17/11/11 16:48:13 ERROR LocalFileLock: main hdfs:/localhost:9000/carbon/store/default/hdfstable/tablestatus.lock (No such file or directory)
java.io.FileNotFoundException: hdfs:/localhost:9000/carbon/store/default/hdfstable/tablestatus.lock (No such file or directory)
	at java.io.FileOutputStream.open0(Native Method)
	at java.io.FileOutputStream.open(FileOutputStream.java:270)
	at java.io.FileOutputStream.<init>(FileOutputStream.java:213)
	at java.io.FileOutputStream.<init>(FileOutputStream.java:101)
```

  **Possible Cause**
  If you use `<hdfs path>` as store path when creating carbonsession, may get the errors,because the default is LOCALLOCK.

  **Procedure**
  Before creating carbonsession, sets as below:
  ```
  import org.apache.carbondata.core.util.CarbonProperties
  import org.apache.carbondata.core.constants.CarbonCommonConstants
  CarbonProperties.getInstance().addProperty(CarbonCommonConstants.LOCK_TYPE, "HDFSLOCK")
  ```

## Failed to load thrift libraries

  **Symptom**

  Thrift throws following exception :

  ```
  thrift: error while loading shared libraries:
  libthriftc.so.0: cannot open shared object file: No such file or directory
  ```

  **Possible Cause**

  The complete path to the directory containing the libraries is not configured correctly.

  **Procedure**

  Follow the Apache thrift docs at [https://thrift.apache.org/docs/install](https://thrift.apache.org/docs/install) to install thrift correctly.

## Failed to launch the Spark Shell

  **Symptom**

  The shell prompts the following error :

  ```
  org.apache.spark.sql.CarbonContext$$anon$$apache$spark$sql$catalyst$analysis
  $OverrideCatalog$_setter_$org$apache$spark$sql$catalyst$analysis
  $OverrideCatalog$$overrides_$e
  ```

  **Possible Cause**

  The Spark Version and the selected Spark Profile do not match.

  **Procedure**

  1. Ensure your spark version and selected profile for spark are correct.

  2. Use the following command :

  ```
  mvn -Pspark-2.1 -Dspark.version {yourSparkVersion} clean package
  ```
  
Note : Refrain from using "mvn clean package" without specifying the profile.

## Failed to execute load query on cluster

  **Symptom**

  Load query failed with the following exception:

  ```
  Dictionary file is locked for updation.
  ```

  **Possible Cause**

  The carbon.properties file is not identical in all the nodes of the cluster.

  **Procedure**

  Follow the steps to ensure the carbon.properties file is consistent across all the nodes:

  1. Copy the carbon.properties file from the master node to all the other nodes in the cluster.
     For example, you can use ssh to copy this file to all the nodes.

  2. For the changes to take effect, restart the Spark cluster.

## Failed to execute insert query on cluster

  **Symptom**

  Load query failed with the following exception:

  ```
  Dictionary file is locked for updation.
  ```

  **Possible Cause**

  The carbon.properties file is not identical in all the nodes of the cluster.

  **Procedure**

  Follow the steps to ensure the carbon.properties file is consistent across all the nodes:

  1. Copy the carbon.properties file from the master node to all the other nodes in the cluster.
       For example, you can use scp to copy this file to all the nodes.

  2. For the changes to take effect, restart the Spark cluster.

## Failed to connect to hiveuser with thrift

  **Symptom**

  We get the following exception :

  ```
  Cannot connect to hiveuser.
  ```

  **Possible Cause**

  The external process does not have permission to access.

  **Procedure**

  Ensure that the Hiveuser in mysql must allow its access to the external processes.

## Failed to read the metastore db during table creation

  **Symptom**

  We get the following exception on trying to connect :

  ```
  Cannot read the metastore db
  ```

  **Possible Cause**

  The metastore db is dysfunctional.

  **Procedure**

  Remove the metastore db from the carbon.metastore in the Spark Directory.

## Failed to load data on the cluster

  **Symptom**

  Data loading fails with the following exception :

   ```
   Data Load failure exception
   ```

  **Possible Cause**

  The following issue can cause the failure :

  1. The core-site.xml, hive-site.xml, yarn-site and carbon.properties are not consistent across all nodes of the cluster.

  2. Path to hdfs ddl is not configured correctly in the carbon.properties.

  **Procedure**

   Follow the steps to ensure the following configuration files are consistent across all the nodes:

   1. Copy the core-site.xml, hive-site.xml, yarn-site,carbon.properties files from the master node to all the other nodes in the cluster.
      For example, you can use scp to copy this file to all the nodes.

      Note : Set the path to hdfs ddl in carbon.properties in the master node.

   2. For the changes to take effect, restart the Spark cluster.



## Failed to insert data on the cluster

  **Symptom**

  Insertion fails with the following exception :

  ```
  Data Load failure exception
  ```

  **Possible Cause**

  The following issue can cause the failure :

  1. The core-site.xml, hive-site.xml, yarn-site and carbon.properties are not consistent across all nodes of the cluster.

  2. Path to hdfs ddl is not configured correctly in the carbon.properties.

  **Procedure**

   Follow the steps to ensure the following configuration files are consistent across all the nodes:

   1. Copy the core-site.xml, hive-site.xml, yarn-site,carbon.properties files from the master node to all the other nodes in the cluster.
      For example, you can use scp to copy this file to all the nodes.

      Note : Set the path to hdfs ddl in carbon.properties in the master node.

   2. For the changes to take effect, restart the Spark cluster.

## Failed to execute Concurrent Operations on table by multiple workers

  **Symptom**

  Execution fails with the following exception :

  ```
  Table is locked for updation.
  ```

  **Possible Cause**

  Concurrency not supported.

  **Procedure**

  Worker must wait for the query execution to complete and the table to release the lock for another query execution to succeed.

## Failed to create a table with a single numeric column

  **Symptom**

  Execution fails with the following exception :

  ```
  Table creation fails.
  ```

  **Possible Cause**

  Behaviour not supported.

  **Procedure**

  A single column that can be considered as dimension is mandatory for table creation.


