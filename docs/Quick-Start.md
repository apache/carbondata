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

# Getting started with Apache CarbonData

This tutorial provides a quick introduction to using CarbonData.

## Examples

Firstly suggest you go through
all [examples](https://github.com/apache/incubator-carbondata/tree/master/examples), to understand
how to create table, how to load data, how to make query.

## Interactive Query with the Spark Shell

### 1.Install

* Download a packaged release of  [Spark 1.5.0 or later](http://spark.apache.org/downloads.html)
* Configure the Hive Metastore using Mysql (you can use this key words to search:mysql hive metastore)
and move mysql-connector-java jar to ${SPARK_HOME}/lib
* Download [thrift](https://thrift.apache.org/), rename to thrift and add to path.
* Download [Apache CarbonData code](https://github.com/apache/incubator-carbondata) and build it
```
$ git clone https://github.com/apache/incubator-carbondata.git carbondata
$ cd carbondata
$ mvn clean install -DskipTests
$ cp assembly/target/scala-2.10/carbondata_*.jar ${SPARK_HOME}/lib
$ mkdir ${SPARK_HOME}/carbondata
$ cp -r processing/carbonplugins ${SPARK_HOME}/carbondata
```

### 2 Interactive Data Query

* Run spark shell
```
$ cd ${SPARK_HOME}
$ carbondata_jar=./lib/$(ls -1 lib |grep "^carbondata_.*\.jar$")
$ mysql_jar=./lib/$(ls -1 lib |grep "^mysql.*\.jar$")
$ ./bin/spark-shell --master local --jars ${carbondata_jar},${mysql_jar}
```

* Create CarbonContext instance
```
import org.apache.spark.sql.CarbonContext
import java.io.File
import org.apache.hadoop.hive.conf.HiveConf
val storePath = "hdfs://hacluster/Opt/CarbonStore"
val cc = new CarbonContext(sc, storePath)
cc.setConf("carbon.kettle.home","./carbondata/carbonplugins")
val metadata = new File("").getCanonicalPath + "/carbondata/metadata"
cc.setConf("hive.metastore.warehouse.dir", metadata)
cc.setConf(HiveConf.ConfVars.HIVECHECKFILEFORMAT.varname, "false")
```
*Note*: `storePath` can be a hdfs path or a local path , the path is used to store table data.

* Create table

```
cc.sql("create table if not exists table1 (id string, name string, city string, age Int) STORED BY 'org.apache.carbondata.format'")
```

* Create sample.csv file in ${SPARK_HOME}/carbondata directory

```
cd ${SPARK_HOME}/carbondata
cat > sample.csv << EOF
id,name,city,age
1,david,shenzhen,31
2,eason,shenzhen,27
3,jarry,wuhan,35
EOF
```

* Load data to table1 in spark shell

```
val dataFilePath = new File("").getCanonicalPath + "/carbondata/sample.csv"
cc.sql(s"load data inpath '$dataFilePath' into table table1")
```

Note: Carbondata also support `LOAD DATA LOCAL INPATH 'folder_path' INTO TABLE [db_name.]table_name OPTIONS(property_name=property_value, ...)` syntax, but right now there is no significant meaning to local in carbondata.We just keep it to align with hive syntax. `dataFilePath` can be hdfs path as well like `val dataFilePath = hdfs://hacluster//carbondata/sample.csv`  

* Query data from table1

```
cc.sql("select * from table1").show
cc.sql("select city, avg(age), sum(age) from table1 group by city").show
```
