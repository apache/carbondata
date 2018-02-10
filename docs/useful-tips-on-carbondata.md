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

# Useful Tips
  This tutorial guides you to create CarbonData Tables and optimize performance.
  The following sections will elaborate on the above topics :

  * [Suggestions to create CarbonData Table](#suggestions-to-create-carbondata-table)
  * [Configuration for Optimizing Data Loading performance for Massive Data](#configuration-for-optimizing-data-loading-performance-for-massive-data)
  * [Optimizing Mass Data Loading](#configurations-for-optimizing-carbondata-performance)

## Suggestions to Create CarbonData Table

  For example, the results of the analysis for table creation with dimensions ranging from 10 thousand to 10 billion rows and 100 to 300 columns have been summarized below.
  The following table describes some of the columns from the table used.

  - **Table Column Description**

  | Column Name | Data Type     | Cardinality | Attribution |
  |-------------|---------------|-------------|-------------|
  | msisdn      | String        | 30 million  | Dimension   |
  | BEGIN_TIME  | BigInt        | 10 Thousand | Dimension   |
  | HOST        | String        | 1 million   | Dimension   |
  | Dime_1      | String        | 1 Thousand  | Dimension   |
  | counter_1   | Decimal       | NA          | Measure     |
  | counter_2   | Numeric(20,0) | NA          | Measure     |
  | ...         | ...           | NA          | Measure     |
  | counter_100 | Decimal       | NA          | Measure     |


  - **Put the frequently-used column filter in the beginning**

  For example, MSISDN filter is used in most of the query then we must put the MSISDN in the first column.
  The create table command can be modified as suggested below :

  ```
  create table carbondata_table(
    msisdn String,
    BEGIN_TIME bigint,
    HOST String,
    Dime_1 String,
    counter_1, Decimal
    ...
    
    )STORED BY 'carbondata'
    TBLPROPERTIES ('SORT_COLUMNS'='msisdn, Dime_1')
  ```

  Now the query with MSISDN in the filter will be more efficient.

  - **Put the frequently-used columns in the order of low to high cardinality**

  If the table in the specified query has multiple columns which are frequently used to filter the results, it is suggested to put
  the columns in the order of cardinality low to high. This ordering of frequently used columns improves the compression ratio and
  enhances the performance of queries with filter on these columns.

  For example, if MSISDN, HOST and Dime_1 are frequently-used columns, then the column order of table is suggested as
  Dime_1>HOST>MSISDN, because Dime_1 has the lowest cardinality.
  The create table command can be modified as suggested below :

  ```
  create table carbondata_table(
      msisdn String,
      BEGIN_TIME bigint,
      HOST String,
      Dime_1 String,
      counter_1, Decimal
      ...
      
      )STORED BY 'carbondata'
      TBLPROPERTIES ('SORT_COLUMNS'='Dime_1, HOST, MSISDN')
  ```

  - **For measure type columns with non high accuracy, replace Numeric(20,0) data type with Double data type**

  For columns of measure type, not requiring high accuracy, it is suggested to replace Numeric data type with Double to enhance query performance. 
  The create table command can be modified as below :

```
  create table carbondata_table(
    Dime_1 String,
    BEGIN_TIME bigint,
    END_TIME bigint,
    HOST String,
    MSISDN String,
    counter_1 decimal,
    counter_2 double,
    ...
    )STORED BY 'carbondata'
    TBLPROPERTIES ('SORT_COLUMNS'='Dime_1, HOST, MSISDN')
```
  The result of performance analysis of test-case shows reduction in query execution time from 15 to 3 seconds, thereby improving performance by nearly 5 times.

 - **Columns of incremental character should be re-arranged at the end of dimensions**

  Consider the following scenario where data is loaded each day and the begin_time is incremental for each load, it is suggested to put begin_time at the end of dimensions.
  Incremental values are efficient in using min/max index. The create table command can be modified as below :

  ```
  create table carbondata_table(
    Dime_1 String,
    HOST String,
    MSISDN String,
    counter_1 double,
    counter_2 double,
    BEGIN_TIME bigint,
    END_TIME bigint,
    ...
    counter_100 double
    )STORED BY 'carbondata'
    TBLPROPERTIES ('SORT_COLUMNS'='Dime_1, HOST, MSISDN')
  ```

## Configuration for Optimizing Data Loading performance for Massive Data


  CarbonData supports large data load, in this process sorting data while loading consumes a lot of memory and disk IO and
  this can result sometimes in "Out Of Memory" exception.
  If you do not have much memory to use, then you may prefer to slow the speed of data loading instead of data load failure.
  You can configure CarbonData by tuning following properties in carbon.properties file to get a better performance.

  | Parameter | Default Value | Description/Tuning |
  |-----------|-------------|--------|
  |carbon.number.of.cores.while.loading|Default: 2.This value should be >= 2|Specifies the number of cores used for data processing during data loading in CarbonData. |
  |carbon.sort.size|Default: 100000. The value should be >= 100.|Threshold to write local file in sort step when loading data|
  |carbon.sort.file.write.buffer.size|Default:  50000.|DataOutputStream buffer. |
  |carbon.number.of.cores.block.sort|Default: 7 | If you have huge memory and CPUs, increase it as you will|
  |carbon.merge.sort.reader.thread|Default: 3 |Specifies the number of cores used for temp file merging during data loading in CarbonData.|
  |carbon.merge.sort.prefetch|Default: true | You may want set this value to false if you have not enough memory|

  For example, if there are 10 million records, and i have only 16 cores, 64GB memory, will be loaded to CarbonData table.
  Using the default configuration  always fail in sort step. Modify carbon.properties as suggested below:

  ```
  carbon.number.of.cores.block.sort=1
  carbon.merge.sort.reader.thread=1
  carbon.sort.size=5000
  carbon.sort.file.write.buffer.size=5000
  carbon.merge.sort.prefetch=false
  ```

## Configurations for Optimizing CarbonData Performance

  Recently we did some performance POC on CarbonData for Finance and telecommunication Field. It involved detailed queries and aggregation
  scenarios. After the completion of POC, some of the configurations impacting the performance have been identified and tabulated below :

  | Parameter | Location | Used For  | Description | Tuning |
  |----------------------------------------------|-----------------------------------|---------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
  | carbon.sort.intermediate.files.limit | spark/carbonlib/carbon.properties | Data loading | During the loading of data, local temp is used to sort the data. This number specifies the minimum number of intermediate files after which the  merge sort has to be initiated. | Increasing the parameter to a higher value will improve the load performance. For example, when we increase the value from 20 to 100, it increases the data load performance from 35MB/S to more than 50MB/S. Higher values of this parameter consumes  more memory during the load. |
  | carbon.number.of.cores.while.loading | spark/carbonlib/carbon.properties | Data loading | Specifies the number of cores used for data processing during data loading in CarbonData. | If you have more number of CPUs, then you can increase the number of CPUs, which will increase the performance. For example if we increase the value from 2 to 4 then the CSV reading performance can increase about 1 times |
  | carbon.compaction.level.threshold | spark/carbonlib/carbon.properties | Data loading and Querying | For minor compaction, specifies the number of segments to be merged in stage 1 and number of compacted segments to be merged in stage 2. | Each CarbonData load will create one segment, if every load is small in size it will generate many small file over a period of time impacting the query performance. Configuring this parameter will merge the small segment to one big segment which will sort the data and improve the performance. For Example in one telecommunication scenario, the performance improves about 2 times after minor compaction. |
  | spark.sql.shuffle.partitions | spark/conf/spark-defaults.conf | Querying | The number of task started when spark shuffle. | The value can be 1 to 2 times as much as the executor cores. In an aggregation scenario, reducing the number from 200 to 32 reduced the query time from 17 to 9 seconds. |
  | spark.executor.instances/spark.executor.cores/spark.executor.memory | spark/conf/spark-defaults.conf | Querying | The number of executors, CPU cores, and memory used for CarbonData query. | In the bank scenario, we provide the 4 CPUs cores and 15 GB for each executor which can get good performance. This 2 value does not mean more the better. It needs to be configured properly in case of limited resources. For example, In the bank scenario, it has enough CPU 32 cores each node but less memory 64 GB each node. So we cannot give more CPU but less memory. For example, when 4 cores and 12GB for each executor. It sometimes happens GC during the query which impact the query performance very much from the 3 second to more than 15 seconds. In this scenario need to increase the memory or decrease the CPU cores. |
  | carbon.detail.batch.size | spark/carbonlib/carbon.properties | Data loading | The buffer size to store records, returned from the block scan. | In limit scenario this parameter is very important. For example your query limit is 1000. But if we set this value to 3000 that means we get 3000 records from scan but spark will only take 1000 rows. So the 2000 remaining are useless. In one Finance test case after we set it to 100, in the limit 1000 scenario the performance increase about 2 times in comparison to if we set this value to 12000. |
  | carbon.use.local.dir | spark/carbonlib/carbon.properties | Data loading | Whether use YARN local directories for multi-table load disk load balance | If this is set it to true CarbonData will use YARN local directories for multi-table load disk load balance, that will improve the data load performance. |
  | carbon.use.multiple.temp.dir | spark/carbonlib/carbon.properties | Data loading | Whether to use multiple YARN local directories during table data loading for disk load balance | After enabling 'carbon.use.local.dir', if this is set to true, CarbonData will use all YARN local directories during data load for disk load balance, that will improve the data load performance. Please enable this property when you encounter disk hotspot problem during data loading. |
  | carbon.sort.temp.compressor | spark/carbonlib/carbon.properties | Data loading | Specify the name of compressor to compress the intermediate sort temporary files during sort procedure in data loading. | The optional values are 'SNAPPY','GZIP','BZIP2','LZ4' and empty. By default, empty means that Carbondata will not compress the sort temp files. This parameter will be useful if you encounter disk bottleneck. |

  Note: If your CarbonData instance is provided only for query, you may specify the property 'spark.speculation=true' which is in conf directory of spark.
