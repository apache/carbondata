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

# S3 Guide

Object storage is the recommended storage format in cloud as it can support storing large data 
files. S3 APIs are widely used for accessing object stores. This can be 
used to store or retrieve data on Amazon cloud, Huawei Cloud(OBS) or on any other object
 stores conforming to S3 API.
Storing data in cloud is advantageous as there are no restrictions on the size of 
data and the data can be accessed from anywhere at any time.
Carbondata can support any Object Storage that conforms to Amazon S3 API.
Carbondata relies on Hadoop provided S3 filesystem APIs to access Object stores.

# Writing to Object Storage

To store carbondata files onto Object Store, `carbon.storelocation` property will have 
to be configured with Object Store path in CarbonProperties file. 

For example:
```
carbon.storelocation=s3a://mybucket/carbonstore
```

If the existing store location cannot be changed or only specific tables need to be stored 
onto cloud object store, it can be done so by specifying the `location` option in the create 
table DDL command.

For example:

```
CREATE TABLE IF NOT EXISTS db1.table1(col1 string, col2 int) STORED AS carbondata LOCATION 's3a://mybucket/carbonstore'
``` 

For more details on create table, Refer [DDL of CarbonData](ddl-of-carbondata.md#create-table)

# Authentication

Authentication properties will have to be configured to store the carbondata files on to S3 location. 

Authentication properties can be set in any of the following ways:
1. Set authentication properties in core-site.xml, refer 
[hadoop authentication document](https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html#Authentication_properties)

2. Set authentication properties in spark-defaults.conf.

Example
```
spark.hadoop.fs.s3a.secret.key=123
spark.hadoop.fs.s3a.access.key=456
```

3. Pass authentication properties with spark-submit as configuration.

Example:
```
./bin/spark-submit \
--master yarn \
--conf spark.hadoop.fs.s3a.secret.key=123 \
--conf spark.hadoop.fs.s3a.access.key=456 \
--class=xxx
```  

4. Set authentication properties to hadoop configuration object in sparkContext.

Example:
```
sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", "123")
sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.access.key","456")
```

# Recommendations

1. Object Storage like S3 does not support file leasing mechanism(supported by HDFS) that is 
required to take locks which ensure consistency between concurrent operations therefore, it is 
recommended to set the configurable lock path property([carbon.lock.path](./configuration-parameters.md#system-configuration))
 to a HDFS directory.
2. Concurrent data manipulation operations are not supported. Object stores follow eventual consistency semantics, i.e., any put request might take some time to reflect when trying to list. This behaviour causes the data read is always not consistent or not the latest.

