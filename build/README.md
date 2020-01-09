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

# Building CarbonData

## Prerequisites
* Unix-like environment (Linux, Mac OS X)
* Git
* [Apache Maven (Recommend version 3.3 or later)](https://maven.apache.org/download.cgi)
* [Oracle Java 7 or 8](http://www.oracle.com/technetwork/java/javase/downloads/index.html)
* [Apache Thrift 0.9.3](http://archive.apache.org/dist/thrift/0.9.3/)

## Build command
Build with different supported versions of Spark, by default using Spark 2.2.1 to build
```
mvn -DskipTests -Pspark-2.1 -Dspark.version=2.1.0 clean package
mvn -DskipTests -Pspark-2.2 -Dspark.version=2.2.1 clean package
mvn -DskipTests -Pspark-2.3 -Dspark.version=2.3.2 clean package
```

Note:
 - If you are working in Windows environment, remember to add `-Pwindows` while building the project.
 - The mv feature is not compiled by default. If you want to use this feature, remember to add `-Pmv` while building the project.

## For contributors : To build the format code after any changes, please follow the below command.
Note:Need install Apache Thrift 0.9.3
```
mvn clean -DskipTests -Pbuild-with-format -Pspark-2.2 package
```