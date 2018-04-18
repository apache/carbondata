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
val carbon = SparkSession.builder().config(sc.getConf)
             .getOrCreateCarbonSession(<store_path>)
```
Example:

```
val carbon = SparkSession.builder().config(sc.getConf)
             .getOrCreateCarbonSession("hdfs://localhost:9000/carbon/store")
```

## What is Carbon Lock Type?
The Apache CarbonData acquires lock on the files to prevent concurrent operation from modifying the same files. The lock can be of the following types depending on the storage location, for HDFS we specify it to be of type HDFSLOCK. By default it is set to type LOCALLOCK.
The property carbon.lock.type configuration specifies the type of lock to be acquired during concurrent operations on table. This property can be set with the following values :
- **LOCALLOCK** : This Lock is created on local file system as file. This lock is useful when only one spark driver (thrift server) runs on a machine and no other CarbonData spark application is launched concurrently.
- **HDFSLOCK** : This Lock is created on HDFS file system as file. This lock is useful when multiple CarbonData spark applications are launched and no ZooKeeper is running on cluster and the HDFS supports, file based locking.

## How to resolve Abstract Method Error?
In order to build CarbonData project it is necessary to specify the spark profile. The spark profile sets the Spark Version. You need to specify the ``spark version`` while using Maven to build project.

## How Carbon will behave when execute insert operation in abnormal scenarios?
Carbon support insert operation, you can refer to the syntax mentioned in [DML Operations on CarbonData](dml-operation-on-carbondata.md).
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
STORED BY 'carbondata';
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
create table gdp21(cntry smallint, gdp double, y_year date) stored by 'carbondata';
create datamap ag1 on table gdp21 using 'preaggregate' as select cntry, sum(gdp) from gdp21 group by cntry;
select ctry from pop1 where ctry in (select cntry from gdp21 group by cntry);
```

- **Scenario 2** : 
When aggregate function along with 'in' filter.

Example:

```
create table gdp21(cntry smallint, gdp double, y_year date) stored by 'carbondata';
create datamap ag1 on table gdp21 using 'preaggregate' as select cntry, sum(gdp) from gdp21 group by cntry;
select cntry, sum(gdp) from gdp21 where cntry in (select ctry from pop1) group by cntry;
```

- **Scenario 3** : 
When aggregate function having 'join' with equal filter.

Example:

```
create table gdp21(cntry smallint, gdp double, y_year date) stored by 'carbondata';
create datamap ag1 on table gdp21 using 'preaggregate' as select cntry, sum(gdp) from gdp21 group by cntry;
select cntry,sum(gdp) from gdp21,pop1 where cntry=ctry group by cntry;
```


