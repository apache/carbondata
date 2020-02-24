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


# Alluxio guide
This tutorial provides a brief introduction to using Alluxio.
 - How to use Alluxio in CarbonData?
    - [Running alluxio example in CarbonData project by IDEA](#Running alluxio example in CarbonData project by IDEA)
    - [CarbonData supports alluxio by spark-shell](#CarbonData supports alluxio by spark-shell)
    - [CarbonData supports alluxio by spark-submit](#CarbonData supports alluxio by spark-submit)
     
## Running alluxio example in CarbonData project by IDEA

### [Building CarbonData](https://github.com/apache/carbondata/tree/master/build)
 - Please refer to [Building CarbonData](https://github.com/apache/carbondata/tree/master/build).   
 - Users need to install IDEA and scala plugin, and import CarbonData project.
 
### Installing and starting Alluxio
 - Please refer to [https://www.alluxio.org/docs/1.8/en/Getting-Started.html#starting-alluxio](https://www.alluxio.org/docs/1.8/en/Getting-Started.html#starting-alluxio)   
 - Access the Alluxio web: [http://localhost:19999/home](http://localhost:19999/home)   

### Running Example
 - Please refer to [AlluxioExample](https://github.com/apache/carbondata/blob/master/examples/spark/src/main/scala/org/apache/carbondata/examples/AlluxioExample.scala)

## CarbonData supports alluxio by spark-shell

### [Building CarbonData](https://github.com/apache/carbondata/tree/master/build)
 - Please refer to [Building CarbonData](https://github.com/apache/carbondata/tree/master/build).   

### Preparing Spark
 - Please refer to [http://spark.apache.org/docs/latest/](http://spark.apache.org/docs/latest/)

### Downloading alluxio and uncompressing it
 - Please refer to [https://www.alluxio.org/download](https://www.alluxio.org/download)
 
### Running spark-shell
 - Running the command in spark path
 ```$command
./bin/spark-shell --jars ${CARBONDATA_PATH}/assembly/target/scala-2.11/apache-carbondata-2.0.0-SNAPSHOT-bin-spark2.3.4-hadoop2.7.2.jar,${ALLUXIO_PATH}/client/alluxio-1.8.1-client.jar
```
 - Testing use alluxio by CarbonSession
 ```$scala
import org.apache.spark.sql.CarbonSession._
import org.apache.spark.sql.SparkSession
	
val carbon = SparkSession.builder().master("local").appName("test").getOrCreateCarbonSession("alluxio://localhost:19998/carbondata");
carbon.sql("CREATE TABLE carbon_alluxio(id String,name String, city String,age Int) STORED as carbondata");
carbon.sql(s"LOAD DATA LOCAL INPATH '${CARBONDATA_PATH}/integration/spark/src/test/resources/sample.csv' into table carbon_alluxio");
carbon.sql("select * from carbon_alluxio").show
```
 - Result
 ```$scala
 scala> carbon.sql("select * from carbon_alluxio").show
 +---+------+---------+---+
 | id|  name|     city|age|
 +---+------+---------+---+
 |  1| david| shenzhen| 31|
 |  2| eason| shenzhen| 27|
 |  3| jarry|    wuhan| 35|
 |  3| jarry|Bangalore| 35|
 |  4| kunal|    Delhi| 26|
 |  4|vishal|Bangalore| 29|
 +---+------+---------+---+
 ```
## CarbonData supports alluxio by spark-submit

### [Building CarbonData](https://github.com/apache/carbondata/tree/master/build)
 - Please refer to [Building CarbonData](https://github.com/apache/carbondata/tree/master/build).   

### Preparing Spark
 - Please refer to [http://spark.apache.org/docs/latest/](http://spark.apache.org/docs/latest/)

### Downloading alluxio and uncompressing it
 - Please refer to [https://www.alluxio.org/download](https://www.alluxio.org/download)
 
### Running spark-submit
#### Upload data to alluxio
```$command
./bin/alluxio fs  copyFromLocal ${CARBONDATA_PATH}/hadoop/src/test/resources/data.csv /
```
#### Command
```$command
./bin/spark-submit \
--master local \
--jars ${ALLUXIO_PATH}/client/alluxio-1.8.1-client.jar,${CARBONDATA_PATH}/examples/spark/target/carbondata-examples-2.0.0-SNAPSHOT.jar \
--class org.apache.carbondata.examples.AlluxioExample \
${CARBONDATA_PATH}/assembly/target/scala-2.11/apache-carbondata-2.0.0-SNAPSHOT-bin-spark2.3.4-hadoop2.7.2.jar \
false
```
**NOTE**: Please set runShell as false, which can avoid dependency on alluxio shell module.

#### Result
```$command
+-----------------+-------+--------------------+--------------------+---------+-----------+---------+----------+
|SegmentSequenceId| Status|     Load Start Time|       Load End Time|Merged To|File Format|Data Size|Index Size|
+-----------------+-------+--------------------+--------------------+---------+-----------+---------+----------+
|                1|Success|2019-01-09 15:10:...|2019-01-09 15:10:...|       NA|COLUMNAR_V3|  23.92KB|    1.07KB|
|                0|Success|2019-01-09 15:10:...|2019-01-09 15:10:...|       NA|COLUMNAR_V3|  23.92KB|    1.07KB|
+-----------------+-------+--------------------+--------------------+---------+-----------+---------+----------+

+-------+------+
|country|amount|
+-------+------+
| france|   202|
|  china|  1698|
+-------+------+

+-----------------+---------+--------------------+--------------------+---------+-----------+---------+----------+
|SegmentSequenceId|   Status|     Load Start Time|       Load End Time|Merged To|File Format|Data Size|Index Size|
+-----------------+---------+--------------------+--------------------+---------+-----------+---------+----------+
|                3|Compacted|2019-01-09 15:10:...|2019-01-09 15:10:...|      0.1|COLUMNAR_V3|  23.92KB|    1.03KB|
|                2|Compacted|2019-01-09 15:10:...|2019-01-09 15:10:...|      0.1|COLUMNAR_V3|  23.92KB|    1.07KB|
|                1|Compacted|2019-01-09 15:10:...|2019-01-09 15:10:...|      0.1|COLUMNAR_V3|  23.92KB|    1.07KB|
|              0.1|  Success|2019-01-09 15:10:...|2019-01-09 15:10:...|       NA|COLUMNAR_V3|  37.65KB|    1.08KB|
|                0|Compacted|2019-01-09 15:10:...|2019-01-09 15:10:...|      0.1|COLUMNAR_V3|  23.92KB|    1.07KB|
+-----------------+---------+--------------------+--------------------+---------+-----------+---------+----------+

```

## Reference
[1] https://www.alluxio.org/docs/1.8/en/Getting-Started.html
[2] https://www.alluxio.org/docs/1.8/en/compute/Spark.html