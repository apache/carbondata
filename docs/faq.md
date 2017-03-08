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

# FAQs

* [Can we preserve Segments from Compaction?](#can-we-preserve-segments-from-compaction)
* [Can we disable horizontal compaction?](#can-we-disable-horizontal-compaction)
* [What is horizontal compaction?](#what-is-horizontal-compaction)
* [How to enable Compaction while data loading?](#how-to-enable-compaction-while-data-loading)
* [Where are Bad Records Stored in CarbonData?](#where-are-bad-records-stored-in-carbondata)
* [What are Bad Records?](#what-are-bad-records)
* [Can we use CarbonData on Standalone Spark Cluster?](#can-we-use-carbondata-on-standalone-spark-cluster)
* [What versions of Apache Spark are Compatible with CarbonData?](#what-versions-of-apache-spark-are-compatible-with-carbondata)
* [Can we Load Data from excel?](#can-we-load-data-from-excel)
* [How to enable Single Pass Data Loading?](#how-to-enable-single-pass-data-loading)
* [What is Single Pass Data Loading?](#what-is-single-pass-data-loading)
* [How to specify the data loading format for CarbonData ?](#how-to-specify-the-data-loading-format-for-carbondata)
* [How to resolve store location can’t be found?](#how-to-resolve-store-location-can-not-be-found)
* [What is carbon.lock.type?]()
* [How to enable Auto Compaction?](#how-to-enable-auto-compaction)
* [How to resolve Abstract Method Error?](#how-to-resolve-abstract-method-error)
* [Getting Exception on Creating a View](#getting-exception-on-creating-a-view)
* [Is CarbonData supported for Windows?](#is-carbondata-supported-for-windows)
* [How to handle Bad Records?](#how-to-handle-bad-records)

## Can we preserve Segments from Compaction?
If you want to preserve number of segments from being compacted then you can set the property  **carbon.numberof.preserve.segments**  equal to the **value of number of segments to be preserved**.

Note : *No segments are preserved by Default.*

## Can we disable horizontal compaction?
Yes, to disable horizontal compaction, set **carbon.horizontal.compaction.enable** to ``FALSE`` in carbon.properties file.

## What is horizontal compaction?
Compaction performed after Update and Delete operations is referred as Horizontal Compaction. After every DELETE and UPDATE operation, horizontal compaction may occur in case the delta (DELETE/ UPDATE) files becomes more than specified threshold.

By default the parameter **carbon.horizontal.compaction.enable** enabling the horizontal compaction is set to ``TRUE``.

## How to enable Compaction while data loading?
To enable compaction while data loading, set **carbon.enable.auto.load.merge** to ``TRUE`` in carbon.properties file.

## Where are Bad Records Stored in CarbonData?
The bad records are stored at the location set in carbon.badRecords.location in carbon.properties file.
By default **carbon.badRecords.location** specifies the following location ``/opt/Carbon/Spark/badrecords``.

## What are Bad Records?
Records that fail to get loaded into the CarbonData due to data type incompatibility are classified as Bad Records.

## Can we use CarbonData on Standalone Spark Cluster?
Yes, CarbonData can be used on a Standalone spark cluster. But using a standalone cluster has following limitations:
- single node cluster cannot be scaled up
- the maximum memory and the CPU computation power has a fixed limit
- the number of processors are limited in a single node cluster

To harness the actual speed of execution of CarbonData on petabytes of data, it is suggested to use a Multinode Cluster.

## What versions of Apache Spark are Compatible with CarbonData?
Currently **Spark 1.6.2** and **Spark 2.1** is compatible with CarbonData.

## Can we Load Data from excel?
Yes, the data can be loaded from excel provided the data is in CSV format.

## How to enable Single Pass Data Loading?
You need to set **SINGLE_PASS** to ``True`` and append it to ``OPTIONS`` Section in the query as demonstrated in the Load Query below :
```
LOAD DATA local inpath '/opt/rawdata/data.csv' INTO table carbontable
OPTIONS('DELIMITER'=',', 'QUOTECHAR'='"','FILEHEADER'='empno,empname,designation','USE_KETTLE'='FALSE')
```
Refer to [DML-operations-in-CarbonData](https://github.com/PallaviSingh1992/incubator-carbondata/blob/6b4dd5f3dea8c93839a94c2d2c80ab7a799cf209/docs/dml-operation-on-carbondata.md) for more details and example.

## What is Single Pass Data Loading?
Single Pass Loading enables single job to finish data loading with dictionary generation on the fly. It enhances performance in the scenarios where the subsequent data loading after initial load involves fewer incremental updates on the dictionary.
This option specifies whether to use single pass for loading data or not. By default this option is set to ``FALSE``.

## How to specify the data loading format for CarbonData?
Edit carbon.properties file. Modify the value of parameter **carbon.data.file.version**.
Setting the parameter **carbon.data.file.version** to ``1`` will support data loading in ``old format(0.x version)`` and setting **carbon.data.file.version** to ``2`` will support data loading in ``new format(1.x onwards)`` only.
By default the data loading is supported using the new format.

## How to resolve store location can not be found?
Try creating ``carbonsession`` with ``storepath`` specified in the following manner :
```
val carbon = SparkSession.builder().config(sc.getConf).getOrCreateCarbonSession(<store_path>)
```
Example:
```
val carbon = SparkSession.builder().config(sc.getConf).getOrCreateCarbonSession("hdfs://localhost:9000/carbon/store ")
```

## What is carbon.lock.type?
This property configuration specifies the type of lock to be acquired during concurrent operations on table. This property can be set with the following values :
- **LOCALLOCK** : This Lock is created on local file system as file. This lock is useful when only one spark driver (thrift server) runs on a machine and no other CarbonData spark application is launched concurrently.
- **HDFSLOCK** : This Lock is created on HDFS file system as file. This lock is useful when multiple CarbonData spark applications are launched and no ZooKeeper is running on cluster and the HDFS supports, file based locking.

## How to enable Auto Compaction?
To enable compaction set **carbon.enable.auto.load.merge** to ``TRUE`` in the carbon.properties file.

## How to resolve Abstract Method Error?
You need to specify the ``spark version`` while using Maven to build project.

## Getting Exception on Creating a View
View not supported in CarbonData.

## Is CarbonData supported for Windows?
We may provide support for windows in future. You are welcome to contribute if you want to add the support :)

## How to handle Bad Records?
While loading data we can specify the approach to handle Bad Records. In order to analyse the cause of the Bad Records the parameter ``BAD_RECORDS_LOGGER_ENABLE`` must be set to value ``TRUE``. There are three approaches to handle Bad Records which can be specified  by the parameter ``BAD_RECORDS_ACTION``.

- To pad the in-correct values of the csv rows with NULL value and load the data in CarbonData, set the following in the query :
```
'BAD_RECORDS_ACTION'='FORCE'
```

- To write the Bad Records without padding in-correct values with NULL in the raw csv (set in the parameter **carbon.badRecords.location**), set the following in the query :
```
'BAD_RECORDS_ACTION'='REDIRECT'
```

- To ignore the Bad Records from getting stored in the raw csv, we need to set the following in the query :
```
'BAD_RECORDS_ACTION'='INDIRECT'
```
    

