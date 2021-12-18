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

# Heterogeneous format segments in carbondata

### Background
In the industry, many users already adopted to data with different formats like ORC, Parquet, JSON, CSV etc.,  
If users want to migrate to Carbondata for better performance or for better features then there is no direct way. 
All the existing data needs to be converted to Carbondata to migrate.  
This solution works out if the existing data is less, what if the existing data is more?   
Heterogeneous format segments aims to solve this problem by avoiding data conversion.

### Add segment with path and format
Users can add the existing data as a segment to the carbon table provided the schema of the data
 and the carbon table should be the same. 
 
 Syntax
 
   ```
   ALTER TABLE [db_name.]table_name ADD SEGMENT OPTIONS(property_name=property_value, ...)
   ```

**Supported properties:**

| Property                                                     | Description                                                  |
| ------------------------------------------------------------ | ------------------------------------------------------------ |
| [PATH](#path)           | User external old table path         |
| [FORMAT](#format)       | User external old table file format             |
| [PARTITION](#partition) | Partition info for partition table , should be form of "a:int, b:string"             |


-
  You can use the following options to add segment:

  - ##### PATH: 
    User old table path.
    
    ``` 
    OPTIONS('PATH'='hdfs://usr/oldtable')
    ```

  - ##### FORMAT:
    User old table file format. eg : parquet, orc

    ```
    OPTIONS('FORMAT'='parquet')
    ```
  - ##### PARTITION:
    Partition info for partition table , should be form of "a:int, b:string"

    ```
    OPTIONS('PARTITION'='a:int, b:string')
    ```
  

In the above command user can add the existing data to the carbon table as a new segment and also
 can provide the data format.

During add segment, it will infer the schema from data and validates the schema against the carbon table. 
If the schema doesn’t match it throws an exception.

**Example:**

Exist old hive partition table , stored as orc or parquet file format:


```sql
CREATE TABLE default.log_parquet_par (
	id BIGINT,
	event_time BIGINT,
	ip STRING
)PARTITIONED BY (                              
	day INT,                                    
	hour INT,                                   
	type INT                                    
)                                              
STORED AS parquet
LOCATION 'hdfs://bieremayi/user/hive/warehouse/log_parquet_par';
```

Parquet File Location : 

```
/user/hive/warehouse/log_parquet_par/day=20211123/hour=12/type=0
/user/hive/warehouse/log_parquet_par/day=20211123/hour=12/type=1
```


```sql
CREATE TABLE default.log_orc_par (
	id BIGINT,
	event_time BIGINT,
	ip STRING
)PARTITIONED BY (                              
	day INT,                                    
	hour INT,                                   
	type INT                                    
)                                              
STORED AS orc
LOCATION 'hdfs://bieremayi/user/hive/warehouse/log_orc_par';
```

Orc File Location : 

```
/user/hive/warehouse/log_orc_par/day=20211123/hour=12/type=0
/user/hive/warehouse/log_orc_par/day=20211123/hour=12/type=1
```

**Steps:**

step1: Create carbondata format table with the same schema.

```sql
CREATE TABLE default.log_carbon_par (
	id BIGINT,
	event_time BIGINT,
	ip STRING
)PARTITIONED BY (                              
	day INT,                                    
	hour INT,                                   
	type INT                                    
)                                              
STORED AS carbondata
LOCATION 'hdfs://bieremayi/user/hive/warehouse/log_carbon_par';
```

step2: Execute add segment sql.

|SQL|Is Success|Error Message|
|:---|:---|:---|
|alter table default.log_carbon_par add segment options ('path'= 'hdfs://bieremayi/user/hive/warehouse/log_parquet_par/','format'='parquet','partition'='day:int,hour:int,type:int');|Yes|/|
|alter table default.log_carbon_par add segment options ('path'= 'hdfs://bieremayi/user/hive/warehouse/log_orc_par/','format'='orc','partition'='day:int,hour:int,type:int');|Yes|/|
|alter table default.log_carbon_par add segment options ('path'= 'hdfs://bieremayi/user/hive/warehouse/log_parquet_par/day=20211123/hour=12/type=0','format'='parquet','partition'='day=20211123 and hour=12 and type=0');|No|Error in query: invalid partition option: Map(path -> hdfs://bieremayi/user/hive/warehouse/log_parquet_par/day=20211123/hour=12/type=0, format -> parquet, partition -> day=20211123 and hour=12 and type=0)|
|alter table default.log_carbon_par add segment options ('path'= 'hdfs://bieremayi/user/hive/warehouse/log_parquet_par/day=20211123/hour=12/type=0','format'='parquet','partition'='day=20211123,hour=12,type=0');|No|Error in query: invalid partition option: Map(path -> hdfs://bieremayi/user/hive/warehouse/log_parquet_par/day=20211123/hour=12/type=0, format -> parquet, partition -> day=20211123,hour=12,type=0)|
|alter table default.log_carbon_par add segment options ('path'= 'hdfs://bieremayi/user/hive/warehouse/log_parquet_par/day=20211123/hour=12/type=0','format'='parquet');|No|Error in query: partition option is required when adding segment to partition table|
|alter table default.log_carbon_par add segment options ('path'= 'hdfs://bieremayi/user/hive/warehouse/log_parquet_par/day=20211123/hour=12/type=0','format'='parquet','partition'='day:int,hour:int,type:int');|No|ERROR SparkSQLDriver: Failed in [alter table default.log_carbon_par add segment options ('path'= 'hdfs://bieremayi/user/hive/warehouse/log_parquet_par/day=20211123/hour=12/type=0','format'='parquet','partition'='day:int,hour:int,type:int')]|

step3:  Result check.

```sql
SHOW SEGMENTS FOR TABLE default.log_carbon_par;
```

| ID  |  Status  |     Load Start Time      | Load Time Taken  |              Partition              | Data Size  | Index Size  | File Format  |
|:---|:---|:---|:---|:---|:---|:---|:---|
|4       |Success |2021-11-29 17:59:40.819 |7.026S  |{day=20211123,hour=12,type=1}, ...      |xxx| xxx  |columnar_v3|
|3       |Success |2021-11-29 16:34:28.106 |0.418S  |{day=20211123,hour=12,type=0}   |xxx |NA     | orc|
|2       |Success |2021-11-29 16:34:27.733 |0.222S  |{day=20211123,hour=12,type=1}   |xxx  |NA     | orc|
|1       |Success |2021-11-29 16:30:17.207 |0.275S  |{day=20211123,hour=12,type=0}   |xxx |NA     | parquet|
|0       |Success |2021-11-29 16:30:16.48  |0.452S  |{day=20211123,hour=12,type=1}   |xxx  |NA     | parquet|


### Changes to tablestatus file
Carbon adds the new segment by adding segment information to tablestatus file. In order to add the path and format information to tablestatus, we are going to add `segmentPath`  and `format`  to the tablestatus file. 
And any extra `options` will be added to the segment file.


### Changes to Spark Integration
During select query carbon reads data through RDD which is created by
  CarbonDatasourceHadoopRelation.buildScan, This RDD reads data from physical carbondata files and provides data to spark query plan.
To support multiple formats per segment basis we can create multiple RDD using the existing Spark
 file format scan class FileSourceScanExec . This class can generate scan RDD for all spark supported formats. We can union all these multi-format RDD and create a single RDD and provide it to spark query plan.

**Note**: This integration will be clean as we use the sparks optimized reading, pruning and it
 involves whole codegen and vector processing with unsafe support.

### Changes to Presto Integration
CarbondataSplitManager can create the splits for carbon and as well as for other formats and 
 choose the page source as per the split.  

### Impact on existed feature
**Count(\*) query:**  In case if the segments are mixed with different formats then driver side
 optimization for count(*) query will not work so it will be executed on executor side.

**Index DataMaps:** Datamaps like block/blocklet datamap will only work for carbondata format
 segments so there would not be any driver side pruning for other formats.

**Update/Delete:** Update & Delete operations cannot be allowed on the table which has mixed formats
But it can be allowed if the external segments are added with carbondata format.

**Compaction:** The other format segments cannot be compacted but carbondata segments inside that
 table will be compacted.

**Show Segments:** Now it shows the format and path of the segment along with current information.

**Delete Segments & Clean Files:**  If the segment to be deleted is external then it will not be
 deleted physically. If the segment is present internally only will be deleted physically.

**MV DataMap:** These datamaps can be created on the mixed format table without any
 impact.
 