### Building CarbonData
Prerequisites for building CarbonData:
* Unix-like environment (Linux, Mac OS X)
* git
* Apache Maven (we recommend version 3.3 or later)
* Java 7 or 8
* Scala 2.10
* Apache Thrift 0.9.3

I. Clone CarbonData
```
$ git clone https://github.com/apache/incubator-carbondata.git
```
II. Build the project 
* Build without test:
```
$ mvn -DskipTests clean package 
```
* Build along with test:
```
$ mvn clean package
```
* Build with different spark versions (Default it takes Spark 1.5.2 version)
```
$ mvn -Pspark-1.5.2 clean package
            or
$ mvn -Pspark-1.6.1 clean install
```
* Build along with integration test cases: (Note : It takes more time to build)
```
$ mvn -Pintegration-test clean package
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

### Contact us
To get involved in CarbonData:

* [Subscribe](mailto:dev-subscribe@carbondata.incubator.apache.org) then [mail](mailto:dev@carbondata.incubator.apache.org) to us
* Report issues on [Jira](https://issues.apache.org/jira/browse/CARBONDATA).

### About
CarbonData project original contributed from the [Huawei](http://www.huawei.com)
