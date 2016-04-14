# CarbonData
CarbonData is a fully indexed columnar and hadoop native datastore designed for fast analytics and multi-dimension query on big data.In customer benchmarks, CarbonData has been shown to manage Petabyte of data running on extraordinarily low-cost hardware and answers queries around 10 times faster than the current open source solutions (column-oriented SQL on Hadoop data-stores). 

### Why CarbonData
The CaronData file format provides a highly efficient way to store structured data,it was designed to overcome limitations of the other Hadoop file formats. 
* CarbonData stores data along with index,the index is not stored separately but the carbondata itself is the index.Indexing helps to accelerate query performance and reduces the I/O scans to the mininum and reduces the CPU required for computation on the data.
* CarbonData is designed to support very efficient compression and global encoding schemes,CarbonData can support actionable compression across data files for query engines to perform all processing on compressed/encoded data without having to convert the data. This improves the processing speed, especially for analytical queries. The data can be converted back to the user readable format just before returning the results to the user.
* CarbonData is designed with various types of usecases in mind and provides flexible storage options. Data can be stored in completely columnar or row based formats or even in a mix of columnar and row based hybrid format.
* CarbonData is designed to integrate into big data ecosystem, leverages Apache Hadoop,Apache Spark etc. for distributed query processing.

### Features
* Fast analytic queries in seconds using built-in index, optimized for interactive OLAP-style query, high through put scan query, low latency point query. 
* Fast data loading speed and support incremental load in period of minutes. 5 mins near realtime loading should be supported
* Support concurrent query.
* Support time based data retention. 
* Supports SQL based query interface.

### CarbonData File Structure and Format
The online document at [CarbonData File Structure and Format](https://github.com/HuaweiBigData/carbondata/wiki/CarbonData-File-Structure-and-Format)

### Building CarbonData
Prerequisites for building CarbonData:
* Unix-like environment (Linux, Mac OS X)
* git
* Apache Maven (we recommend version 3.0.4)
* Java 7 or 8
* Scala 2.10

I. Clone and build CarbonData
```
$ git clone https://github.com/HuaweiBigData/carbondata.git
```
II. Go to the root of the source tree
```
$ cd carbondata
```
III. Build the project 
Build without testing:
```
$ mvn -DskipTests clean install 
```
Or, build with testing:
```
$ mvn clean install
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

### Fork and Contribute
This is an open source project for everyone, and we are always open to people who want to use this system or contribute to it. 
This guide document introduce [how to contribute to CarbonData](https://github.com/HuaweiBigData/carbondata/wiki/How-to-contribute-and-Code-Style).

### About
CarbonData project original contributed from the [Huawei](http://www.huawei.com), in progress of donating this open source project to Apache Software Foundation for leveraging big data ecosystem. 
