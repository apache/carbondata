# CarbonData
CarbonData is a new Apache Hadoop native file format for faster 
interactive query using advanced columnar storage, index, compression 
and encoding techniques to improve computing efficiency, in turn it will 
help speedup queries an order of magnitude faster over PetaBytes of data. 

### Why CarbonData
Based on the below requirements, we investigated existing file formats in the Hadoop eco-system, but we could not find a suitable solution that can satisfy all the requirements at the same time,so we start designing CarbonData. 
* Requirement1:Support big scan & only fetch a few columns 
* Requirement2:Support primary key lookup response in sub-second. 
* Requirement3:Support interactive OLAP-style query over big data which involve many filters in a query, this type of workload should response in seconds. 
* Requirement4:Support fast individual record extraction which fetch all columns of the record. 
* Requirement5:Support HDFS so that customer can leverage existing Hadoop cluster. 

### Features
CarbonData file format is a columnar store in HDFS, it has many features that a modern columnar format has, such as splittable, compression schema ,complex data type etc. And CarbonData has following unique features:
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
$ git clone https://github.com/HuaweiBigData/carbondata.git
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
This is an open source project for everyone, and we are always open to people who want to use this system or contribute to it. 
This guide document introduce [how to contribute to CarbonData](https://github.com/HuaweiBigData/carbondata/wiki/How-to-contribute-and-Code-Style).

### About
CarbonData project original contributed from the [Huawei](http://www.huawei.com), in progress of donating this open source project to Apache Software Foundation for leveraging big data ecosystem. 
