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

## Pre-requisites to execute the Presto Example
- The HDFS must be running.
- The CarbonData Table should exist with Data Loaded.

**Please follow the below steps to execute example to query CarbonData in Presto**

1. Start the HDFS cluster
2. Create a database named 'demo' in CarbonData

    ```
    create database demo;
    ```
3. Create a table named 'uniqdata_data'

    ```
    CREATE TABLE uniqdata_data (CUST_ID int,CUST_NAME String,ACTIVE_EMUI_VERSION string, DOB date, DOJ date) STORED BY 'org.apache.carbondata.format'
    ```

4. Load Data in the above table after replacing the HdfsStorePath and CsvName.csv with actual values

    ```
    LOAD DATA INPATH 'HdfsStorePath/CsvName.csv' into table uniqdata_data OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='"','FILEHEADER'='CUST_ID,CUST_NAME,ACTIVE_EMUI_VERSION,DOB,DOJ')

    ```

    NOTE : Load the data from the following CSV : **500000_uniqdata.csv** stored in the resources of examples/presto module.

5. Now specify the CARBONDATA_STOREPATH in the PrestoServerRunner
   The default values is set to 'hdfs://localhost:54310/user/hive/warehouse/carbon.store', replace this with your location of CarbonData StorePath.

    NOTE : Ensure that the HDFS must be running in case the store path is defined on the HDFS while executing the example.

