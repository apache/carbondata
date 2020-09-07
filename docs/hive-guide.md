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
val newSpark = SparkSession.builder().config(sc.getConf).enableHiveSupport.config("spark.sql.extensions","org.apache.spark.sql.CarbonExtensions").getOrCreate()
newSpark.sql("drop table if exists hive_carbon")
newSpark.sql("create table hive_carbon(id int, name string, scale decimal, country string, salary double) STORED AS carbondata")
newSpark.sql("LOAD DATA INPATH '<hdfs store path>/sample.csv' INTO TABLE hive_carbon")
newSpark.sql("SELECT * FROM hive_carbon").show()
```

## Configure Carbon in Hive
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

### Write data from hive

 - Write data from hive into carbondata format.
 
 ```
create table hive_carbon(id int, name string, scale decimal, country string, salary double) stored by 'org.apache.carbondata.hive.CarbonStorageHandler';
insert into hive_carbon select * from parquetTable;
```

**Note**: Only non-transactional tables are supported when created through hive. This means that the standard carbon folder structure would not be followed and all files would be written in a flat folder structure.

### Query data from hive

 - This is to read the carbon table through Hive. It is the integration of the carbon with Hive.

```
set hive.mapred.supports.subdirectories=true;
set mapreduce.dir.recursive=true;
These properties helps to recursively traverse through the directories to read the carbon folder structure.
```

### Example
```
 - Query the table
select * from hive_carbon;
select count(*) from hive_carbon;
select * from hive_carbon order by id;
```

### Note
 - Partition table support is not handled
 - Currently because carbon is implemented as a non-native hive table, therefore the user has to add the `storage_handler` information in tblproperties if the table has to be accessed from hive. Once the tblproperties have been updated, the user would not be able to do certain operations like alter, update/delete, etc., in both spark and hive.
 
 ```
 alter table <tableName> set tblproperties('storage_handler'= 'org.apache.carbondata.hive.CarbonStorageHandler');
```


