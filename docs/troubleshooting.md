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

# Troubleshooting
This tutorial is designed to provide troubleshooting for end users and developers
who are building, deploying, and using CarbonData.

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
     "mvn -Pspark-2.1 -Dspark.version {yourSparkVersion} clean package"
    ```

    Note :  Refrain from using "mvn clean package" without specifying the profile.

## Failed to execute load query on cluster.

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

## Failed to execute insert query on cluster.

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

## Failure to read the metastore db during table creation.

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
   Data Load failure exeception
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
   Data Load failure exeception
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

## Failed to execute Concurrent Operations(Load,Insert,Update) on table by multiple workers.

  **Symptom**

  Execution fails with the following exception :

   ```
   Table is locked for updation.
   ```

  **Possible Cause**

  Concurrency not supported.

  **Procedure**

  Worker must wait for the query execution to complete and the table to release the lock for another query execution to succeed.

## Failed to create a table with a single numeric column.

  **Symptom**

  Execution fails with the following exception :

   ```
   Table creation fails.
   ```

  **Possible Cause**

  Behaviour not supported.

  **Procedure**

  A single column that can be considered as dimension is mandatory for table creation.
