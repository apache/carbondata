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

# Apache CarbonData
CarbonData is a new Apache Hadoop native file format for faster 
interactive query using advanced columnar storage, index, compression 
and encoding techniques to improve computing efficiency, in turn it will 
help speedup queries an order of magnitude faster over PetaBytes of data. 

### Features
CarbonData file format is a columnar store in HDFS, it has many features that a modern columnar format has, such as splittable, compression schema ,complex data type etc, and CarbonData has following unique features:
* Stores data along with index: it can significantly accelerate query performance and reduces the I/O scans and CPU resources, where there are filters in the query.  CarbonData index consists of multiple level of indices, a processing framework can leverage this index to reduce the task it needs to schedule and process, and it can also do skip scan in more finer grain unit (called blocklet) in task side scanning instead of scanning the whole file. 
* Operable encoded data :Through supporting efficient compression and global encoding schemes, can query on compressed/encoded data, the data can be converted just before returning the results to the users, which is "late materialized". 
* Column group: Allow multiple columns to form a column group that would be stored as row format. This reduces the row reconstruction cost at query time.
* Supports for various use cases with one single Data format : like interactive OLAP-style query, Sequential Access (big scan), Random Access (narrow scan). 

### CarbonData File Structure and Format
The online document at [CarbonData File Format](https://github.com/HuaweiBigData/carbondata/wiki/CarbonData-File-Structure-and-Format)

### Building CarbonData
Prerequisites for building CarbonData:
* Unix-like environment (Linux, Mac OS X)
* git
* Apache Maven (we recommend version 3.0.4)
* Java 7 or 8
* Scala 2.10
* Apache Thrift 0.9.3

I. Clone and build CarbonData
```
$ git clone https://github.com/apache/incubator-carbondata.git
```
II. Go to the root of the source tree
```
$ cd carbondata
```
III. Build the project 
* Build without testing:
```
$ mvn -DskipTests clean install 
```
* Build with testing:
```
$ mvn clean install
```
* Build along with integration test cases: (Note : It takes more time to build)
```
$ mvn -Pintegration-test clean install
```

### Developing CarbonData
The CarbonData committers use IntelliJ IDEA and Eclipse IDE to develop.

#### IntelliJ IDEA
* Download IntelliJ at https://www.jetbrains.com/idea/ and install the Scala plug-in for IntelliJ at http://plugins.jetbrains.com/plugin/?id=1347
* Go to "File -> Import Project", locate the CarbonData source directory, and select "Maven Project".
* In the Import Wizard, select "Import Maven projects automatically" and leave other settings at their default. 
* Leave other settings at their default and you should be able to start your development.
* When you run the scala test, sometimes you will get out of memory exception. You can increase your VM memory usage by the following setting, for example:
```
-XX:MaxPermSize=512m -Xmx3072m
```
You can also make those setting to be the default by setting to the "Defaults -> ScalaTest".

#### Eclipse
* Download the Scala IDE (preferred) or install the scala plugin to Eclipse.
* Import the CarbonData Maven projects ("File" -> "Import" -> "Maven" -> "Existing Maven Projects" -> locate the CarbonData source directory).

### Getting Started
Read the [quick start](https://github.com/HuaweiBigData/carbondata/wiki/Quick-Start).

### Fork and Contribute
This is an active open source project for everyone, and we are always open to people who want to use this system or contribute to it. 
This guide document introduce [how to contribute to CarbonData](https://github.com/HuaweiBigData/carbondata/wiki/How-to-contribute-and-Code-Style).

### Contact us
To get involved in CarbonData:

* [Subscribe:dev@carbondata.incubator.apache.org](mailto:dev-subscribe@carbondata.incubator.apache.org) then [mail](mailto:dev@carbondata.incubator.apache.org) to us
* Report issues on [Jira](https://issues.apache.org/jira/browse/CARBONDATA).

## About
Apache CarbonData is an open source project of The Apache Software Foundation (ASF).
CarbonData project original contributed from the [Huawei](http://www.huawei.com).
