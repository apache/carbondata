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

* [What are Bad Records?](#what-are-bad-records)
* [Where are Bad Records Stored in CarbonData?](#where-are-bad-records-stored-in-carbondata)
* [How to enable Bad Record Logging?](#how-to-enable-bad-record-logging)
* [How to ignore the Bad Records?](#how-to-ignore-the-bad-records)
* [How to specify store location while creating carbon session?](#how-to-specify-store-location-while-creating-carbon-session)
* [What is Carbon Lock Type?](#what-is-carbon-lock-type)
* [How to resolve Abstract Method Error?](#how-to-resolve-abstract-method-error)

## What are Bad Records?
Records that fail to get loaded into the CarbonData due to data type incompatibility or are empty or have incompatible format are classified as Bad Records.

## Where are Bad Records Stored in CarbonData?
The bad records are stored at the location set in carbon.badRecords.location in carbon.properties file.
By default **carbon.badRecords.location** specifies the following location ``/opt/Carbon/Spark/badrecords``.

## How to enable Bad Record Logging?
While loading data we can specify the approach to handle Bad Records. In order to analyse the cause of the Bad Records the parameter ``BAD_RECORDS_LOGGER_ENABLE`` must be set to value ``TRUE``. There are multiple approaches to handle Bad Records which can be specified  by the parameter ``BAD_RECORDS_ACTION``.

- To pad the incorrect values of the csv rows with NULL value and load the data in CarbonData, set the following in the query :
```
'BAD_RECORDS_ACTION'='FORCE'
```

- To write the Bad Records without padding incorrect values with NULL in the raw csv (set in the parameter **carbon.badRecords.location**), set the following in the query :
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
val carbon = SparkSession.builder().config(sc.getConf).getOrCreateCarbonSession(<store_path>)
```
Example:
```
val carbon = SparkSession.builder().config(sc.getConf).getOrCreateCarbonSession("hdfs://localhost:9000/carbon/store ")
```

## What is Carbon Lock Type?
The Apache CarbonData acquires lock on the files to prevent concurrent operation from modifying the same files. The lock can be of the following types depending on the storage location, for HDFS we specify it to be of type HDFSLOCK. By default it is set to type LOCALLOCK.
The property carbon.lock.type configuration specifies the type of lock to be acquired during concurrent operations on table. This property can be set with the following values :
- **LOCALLOCK** : This Lock is created on local file system as file. This lock is useful when only one spark driver (thrift server) runs on a machine and no other CarbonData spark application is launched concurrently.
- **HDFSLOCK** : This Lock is created on HDFS file system as file. This lock is useful when multiple CarbonData spark applications are launched and no ZooKeeper is running on cluster and the HDFS supports, file based locking.

## How to resolve Abstract Method Error?
In order to build CarbonData project it is necessary to specify the spark profile. The spark profile sets the Spark Version. You need to specify the ``spark version`` while using Maven to build project.


