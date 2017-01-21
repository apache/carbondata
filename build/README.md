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

# Building CarbonData

## Prerequisites
* Unix-like environment (Linux, Mac OS X)
* Git
* [Apache Maven (Recommend version 3.3 or later)](https://maven.apache.org/download.cgi)
* [Oracle Java 7 or 8](http://www.oracle.com/technetwork/java/javase/downloads/index.html)
* [Apache Thrift 0.9.3](http://archive.apache.org/dist/thrift/0.9.3/)

## Build release version
Note:Need install Apache Thrift 0.9.3
```
mvn clean -DskipTests -Pbuild-with-format -Pspark-1.6 install
```

## Build dev version(snapshot version,clone from github)
Note:Already uploaded format.jar to snapshot repo for facilitating dev users,
so the compilation command works without "-Pbuild-with-format"

Build without test,by default carbondata takes Spark 1.6.2 to build the project
```
mvn -DskipTests clean package
```

Build with different supported versions of Spark.
```
mvn -DskipTests -Pspark-1.5 -Dspark.version=1.5.1 clean package
mvn -DskipTests -Pspark-1.5 -Dspark.version=1.5.2 clean package
 
mvn -DskipTests -Pspark-1.6 -Dspark.version=1.6.1 clean package
mvn -DskipTests -Pspark-1.6 -Dspark.version=1.6.2 clean package
mvn -DskipTests -Pspark-1.6 -Dspark.version=1.6.3 clean package

mvn -DskipTests -Pspark-2.1 -Dspark.version=2.1.0 clean package
```

Build with test
```
mvn clean package
```
