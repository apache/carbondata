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

# Quick Start
This tutorial provides a quick introduction to using current integration/hive module.

## Prepare CarbonData in Spark
* Create a sample.csv file using the following commands. The CSV file is required for loading data into CarbonData.

  ```
  cd carbondata
  cat > sample.csv << EOF
  id,name,scale,country,salary
  1,yuhai,1.77,china,33000.1
  2,runlin,1.70,china,33000.2
  EOF
  ```

* copy data to HDFS

```
$HADOOP_HOME/bin/hadoop fs -put sample.csv <hdfs store path>/sample.csv
```

* Add the following params to $SPARK_CONF_DIR/conf/hive-site.xml
```xml
<property>
  <name>hive.metastore.pre.event.listeners</name>
  <value>org.apache.carbondata.hive.CarbonHiveMetastoreListener</value>
</property>
```
* Start Spark shell by running the following command in the Spark directory

```
./bin/spark-shell --jars <carbondata assembly jar path, carbon hive jar path>
```

```
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.CarbonSession._
val rootPath = "hdfs:///user/hadoop/carbon"
val storeLocation = s"$rootPath/store"
val warehouse = s"$rootPath/warehouse"
val metaStoreDB = s"$rootPath/metastore_db"

val carbon = SparkSession.builder().enableHiveSupport().config("spark.sql.warehouse.dir", warehouse).config(org.apache.carbondata.core.constants.CarbonCommonConstants.STORE_LOCATION, storeLocation).getOrCreateCarbonSession(storeLocation, metaStoreDB)

carbon.sql("create table hive_carbon(id int, name string, scale decimal, country string, salary double) STORED BY 'carbondata'")
carbon.sql("LOAD DATA INPATH '<hdfs store path>/sample.csv' INTO TABLE hive_carbon")
scala>carbon.sql("SELECT * FROM hive_carbon").show()
```

## Query Data in Hive
### Configure hive classpath
```
mkdir hive/auxlibs/
cp carbondata/assembly/target/scala-2.11/carbondata_2.11*.jar hive/auxlibs/
cp carbondata/integration/hive/target/carbondata-hive-*.jar hive/auxlibs/
cp $SPARK_HOME/jars/spark-catalyst*.jar hive/auxlibs/
cp $SPARK_HOME/jars/scala*.jar hive/auxlibs/
export HIVE_AUX_JARS_PATH=hive/auxlibs/
```
### Fix snappy issue
```
copy snappy-java-xxx.jar from "./<SPARK_HOME>/jars/" to "./Library/Java/Extensions"
export HADOOP_OPTS="-Dorg.xerial.snappy.lib.path=/Library/Java/Extensions -Dorg.xerial.snappy.lib.name=libsnappyjava.jnilib -Dorg.xerial.snappy.tempdir=/Users/apple/DEMO/tmp"
```

### Carbon Jars to be placed
```
hive/lib/ (for hive server)
yarn/lib/ (for MapReduce)

Carbon Jars to be copied to the above paths.
```

### Start hive beeline to query
```
$HIVE_HOME/bin/beeline
```

### Query data from hive

 - This is to read the carbon table through Hive. It is the integration of the carbon with Hive.

```
set hive.mapred.supports.subdirectories=true;
set mapreduce.dir.recursive=true;
These properties helps to recursively traverse through the directories to read the carbon folder structure.
```

### Example
```
 - In case if the carbon table is not set with the SERDE and the INPUTFORMAT/OUTPUTFORMAT, user can create a new hive managed table like below with the required details for the hive to read.
create table hive_carbon_1(id int, name string, scale decimal, country string, salary double) ROW FORMAT SERDE 'org.apache.carbondata.hive.CarbonHiveSerDe' WITH SERDEPROPERTIES ('mapreduce.input.carboninputformat.databaseName'='default', 'mapreduce.input.carboninputformat.tableName'='HIVE_CARBON_EXAMPLE') STORED AS INPUTFORMAT 'org.apache.carbondata.hive.MapredCarbonInputFormat' OUTPUTFORMAT 'org.apache.carbondata.hive.MapredCarbonOutputFormat' LOCATION 'location_to_the_carbon_table';

 - Query the table
select * from hive_carbon_1;
select count(*) from hive_carbon_1;
select * from hive_carbon_1 order by id;
```

### Note
 - Partition table support is not handled
 - Map data type is not supported


