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

## Packaging
Carbon provides following JAR packages:

![carbon modules2](https://cloud.githubusercontent.com/assets/6500698/14255195/831c6e90-fac5-11e5-87ab-3b16d84918fb.png)

- **carbon-store.jar or carbondata-assembly.jar:** This is the main Jar for carbon project, the target user of it are both user and developer. 
      - For MapReduce application users, this jar provides API to read and write carbon files through CarbonInput/OutputFormat in carbon-hadoop module.
      - For developer, this jar can be used to integrate carbon with processing engine like spark and hive, by leveraging API in carbon-processing module.

- **carbon-spark.jar(Currently it is part of assembly jar):** provides support for spark user, spark user can manipulate carbon data files by using native spark DataFrame/SQL interface. Apart from this, in order to leverage carbon's builtin lifecycle management function, higher level concept like Managed Carbon Table, Database and corresponding DDL are introduced.

- **carbon-hive.jar(not yet provided):** similar to carbon-spark, which provides integration to carbon and hive.

## API
Carbon can be used in following scenarios:
### 1. For MapReduce application user
This User API is provided by carbon-hadoop. In this scenario, user can process carbon files in his MapReduce application by choosing CarbonInput/OutputFormat, and is responsible using it correctly.Currently only CarbonInputFormat is provided and OutputFormat will be provided soon.


### 2. For Spark user 
This User API is provided by the Spark itself. There are also two levels of APIs
-  **Carbon File**

Similar to parquet, json, or other data source in Spark, carbon can be used with data source API. For example(please refer to DataFrameAPIExample for the more detail):
```
// User can create a DataFrame from any data source or transformation.
val df = ...

// Write data
// User can write a DataFrame to a carbon file
 df.write
   .format("carbondata")
   .option("tableName", "carbontable")
   .mode(SaveMode.Overwrite)
   .save()


// read carbon data by data source API
df = carbonContext.read
  .format("carbondata")
  .option("tableName", "carbontable")
  .load("/path")

// User can then use DataFrame for analysis
df.count
SVMWithSGD.train(df, numIterations)

// User can also register the DataFrame with a table name, and use SQL for analysis
df.registerTempTable("t1")  // register temporary table in SparkSQL catalog
df.registerHiveTable("t2")  // Or, use a implicit funtion to register to Hive metastore
sqlContext.sql("select count(*) from t1").show
```

- **Managed Carbon Table**

Since carbon has builtin support for high level concept like Table, Database, and supports full data lifecycle management, instead of dealing with just files, user can use carbon specific DDL to manipulate data in Table and Database level. Please refer [DDL](https://github.com/HuaweiBigData/carbondata/wiki/Language-Manual:-DDL) and [DML] (https://github.com/HuaweiBigData/carbondata/wiki/Language-Manual:-DML)

For example:
```
// Use SQL to manage table and query data
create database db1;
use database db1;
show databases;
create table tbl1 using org.apache.carbondata.spark;
load data into table tlb1 path 'some_files';
select count(*) from tbl1;
```

### 3. For developer
For developer who want to integrate carbon into a processing engine like spark/hive/flink, use API provided by carbon-hadoop and carbon-processing:
  - Query: integrate carbon-hadoop with engine specific API, like spark data source API 
  - Data life cycle management: carbon provides utility functions in carbon-processing to manage data life cycle, like data loading, compact, retention, schema evolution. Developer can implement DDLs of their choice and leverage these utility function to do data life cycle management.
