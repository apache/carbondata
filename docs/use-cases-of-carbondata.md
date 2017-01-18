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

# CarbonData Use Cases
This tutorial discusses about the problems that CarbonData addresses. It shall take you through the identified top use cases of CarbonData.

## Introduction
For big data interactive analysis scenarios, many customers expect sub-second response to query TB-PB level data on general hardware clusters with just a few nodes.

In the current big data ecosystem, there are few columnar storage formats such as ORC and Parquet that are designed for SQL on Big Data. Apache Hive’s ORC format is a columnar storage format with basic indexing capability. However, ORC cannot meet the sub-second query response expectation on TB level data, as it performs only stride level dictionary encoding and all analytical operations such as filtering and aggregation is done on the actual data. Apache Parquet is a columnar storage format that can improve performance in comparison to ORC due to its more efficient storage organization. Though Parquet can provide query response on TB level data in a few seconds, it is still far from the sub-second expectation of interactive analysis users. Cloudera Kudu can effectively solve some query performance issues, but kudu is not hadoop native, can’t seamlessly integrate historic HDFS data into new kudu system.

However, CarbonData uses specially engineered optimizations targeted to improve performance of analytical queries which can include filters, aggregation and distinct counts,
the required data to be stored in an indexed, well organized, read-optimized format, CarbonData’s query performance can achieve sub-second response.

## Motivation: Single Format to provide Low Latency Response for all Use Cases
The main motivation behind CarbonData is to provide a single storage format for all the usecases of querying big data on Hadoop. Thus CarbonData is able to cover all use-cases 
into a single storage format.

  ![Motivation](../../../src/site/markdown/images/carbon_data_motivation.png?raw=true)

## Use Cases
### Sequential Access
  - Supports queries that select only a few columns with a group by clause but do not contain any filters. 
  This results in full scan over the complete store for the selected columns.
  
  ![Sequential_Scan](../../../src/site/markdown/images/carbon_data_full_scan.png?raw=true)
  
  **Scenario**
  
  - ETL jobs
  - Log Analysis
    
### Random Access
  - Supports Point Query. These are queries used from operational applications and usually select all or most of the columns and involves a large number of 
  filters which reduce the result to a small size. Such queries generally do not involve any aggregation or group by clause.
    - Row-key query(like HBase)
    - Narrow Scan
    - Requires second/sub-second level low latency
    
   ![random_access](../../../src/site/markdown/images/carbon_data_random_scan.png?raw=true)
    
  **Scenario**

   - Operational Query
   - User Profiling
    
### Olap Style Query
  - Supports Interactive data analysis for any dimensions. These are queries which are typically fired from Interactive Analysis tools. 
  Such queries often select a few columns and involves filters and group by on a column or a grouping expression. 
  It also supports queries that :
    - Involves aggregation/join
    - Roll-up,Drill-down,Slicing and Dicing
    - Low-latency ad-hoc query
    
   ![Olap_style_query](../../../src/site/markdown/images/carbon_data_olap_scan.png?raw=true)
    
   **Scenario**
    
  - Dash-board reporting
  - Fraud & Ad-hoc Analysis
    
