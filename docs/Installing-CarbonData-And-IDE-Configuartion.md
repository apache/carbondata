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

### Building CarbonData
Prerequisites for building CarbonData:
* Unix-like environment (Linux, Mac OS X)
* [Git](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git)
* [Apache Maven (we recommend version 3.3 or later)](https://maven.apache.org/download.cgi)
* [Java 7 or 8](http://www.oracle.com/technetwork/java/javase/downloads/index.html)
* Scala 2.10
* [Apache Thrift 0.9.3](https://thrift.apache.org/download)

I. Clone CarbonData
```
$ git clone https://github.com/apache/incubator-carbondata.git
```
II. Build the project 
* Build without test.By default carbon takes Spark 1.5.2 to build the project
```
$ mvn -DskipTests clean package 
```
* Build with different spark versions
```
$ mvn -DskipTests -Pspark-1.5 -Dspark.version=1.5.0 clean package
$ mvn -DskipTests -Pspark-1.5 -Dspark.version=1.5.1 clean package
$ mvn -DskipTests -Pspark-1.5 -Dspark.version=1.5.2 clean package
 
$ mvn -DskipTests -Pspark-1.6 -Dspark.version=1.6.0 clean package
$ mvn -DskipTests -Pspark-1.6 -Dspark.version=1.6.1 clean package
$ mvn -DskipTests -Pspark-1.6 -Dspark.version=1.6.2 clean package
```
* Build the assembly jar which includes Spark and Hadoop jars
```
$ mvn clean -DskipTests -Pinclude-all package
```
* Build with test
```
$ mvn clean package
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
