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

<img src="/docs/images/format/CarbonData_logo.png" width="200" height="40">

Apache CarbonData is a new big data file format for faster
interactive query using advanced columnar storage, index, compression
and encoding techniques to improve computing efficiency, in turn it will 
help speedup queries an order of magnitude faster over PetaBytes of data. 

### Features
CarbonData file format is a columnar store in HDFS, it has many features that a modern columnar format has, such as splittable, compression schema ,complex data type etc, and CarbonData has following unique features:
* Stores data along with index: it can significantly accelerate query performance and reduces the I/O scans and CPU resources, where there are filters in the query.  CarbonData index consists of multiple level of indices, a processing framework can leverage this index to reduce the task it needs to schedule and process, and it can also do skip scan in more finer grain unit (called blocklet) in task side scanning instead of scanning the whole file. 
* Operable encoded data :Through supporting efficient compression and global encoding schemes, can query on compressed/encoded data, the data can be converted just before returning the results to the users, which is "late materialized". 
* Column group: Allow multiple columns to form a column group that would be stored as row format. This reduces the row reconstruction cost at query time.
* Supports for various use cases with one single Data format : like interactive OLAP-style query, Sequential Access (big scan), Random Access (narrow scan). 

### Documentation
Please visit [CarbonData cwiki](https://cwiki.apache.org/confluence/display/CARBONDATA)

### Building CarbonData,using development tools and cluster deployment guide
Please refer [Building CarbonData and Configuring IDE](https://cwiki.apache.org/confluence/display/CARBONDATA/Building+CarbonData+And+IDE+Configuration)

Please refer [Cluster Deployment Guide](https://cwiki.apache.org/confluence/display/CARBONDATA/Cluster+deployment+guide)

### Getting Started
Read the [quick start](https://cwiki.apache.org/confluence/display/CARBONDATA/Quick+Start)

### Usage of CarbonData
 [DDL Operations on CarbonData](https://cwiki.apache.org/confluence/display/CARBONDATA/DDL+operations+on+CarbonData) 
 
 [DML Operations on CarbonData](https://cwiki.apache.org/confluence/display/CARBONDATA/DML+operations+on+CarbonData)  
 
 [CarbonData data management](https://cwiki.apache.org/confluence/display/CARBONDATA/Data+Management)  

### CarbonData File Structure and interfaces
Please refer [CarbonData File Format](https://cwiki.apache.org/confluence/display/CARBONDATA/CarbonData+File+Structure+and+Format) and [CarbonData Interfaces](https://cwiki.apache.org/confluence/display/CARBONDATA/Carbon+Packaging+and+Interfaces)

### Other Technical Material
[Apache CarbonData meetup material](docs/Apache-CarbonData-meetup-material.pdf)

### Fork and Contribute
This is an active open source project for everyone, and we are always open to people who want to use this system or contribute to it. 
This guide document introduce [how to contribute to CarbonData](https://cwiki.apache.org/confluence/display/CARBONDATA/Contributing+to+CarbonData).

### Contact us
To get involved in CarbonData:

* First join by emailing to [dev-subscribe@carbondata.incubator.apache.org](mailto:dev-subscribe@carbondata.incubator.apache.org),then you can discuss issues by emailing to [dev@carbondata.incubator.apache.org](mailto:dev@carbondata.incubator.apache.org) or visit http://apache-carbondata-mailing-list-archive.1130556.n5.nabble.com
* Report issues on [Apache Jira](https://issues.apache.org/jira/browse/CARBONDATA).

## About
Apache CarbonData is an open source project of The Apache Software Foundation (ASF).
CarbonData project original contributed from the [Huawei](http://www.huawei.com).
