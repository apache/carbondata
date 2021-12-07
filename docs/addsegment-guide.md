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

```
alter table table_name add segment options ('path'= 'hdfs://usr/oldtable','format'='parquet')
```
In the above command user can add the existing data to the carbon table as a new segment and also
 can provide the data format.

During add segment, it will infer the schema from data and validates the schema against the carbon table. 
If the schema doesn’t match it throws an exception.

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
 