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

# Quick Start
This tutorial provides a quick introduction to using current integration/hive module.

## Prerequisites
## Spark Version 2.1
* Build integration/hive
mvn -DskipTests -Pspark-2.1 -Dspark.version=2.1.0 clean package -Phadoop-2.7.2 -Phive-1.2


* Create a sample.csv file using the following commands. The CSV file is required for loading data into CarbonData.

  ```
  cd carbondata
  cat > sample.csv << EOF
  id,name,scale,country,salary
  1,yuhai,1.77,china,33000.1
  2,runlin,1.70,china,33000.2
  EOF
  ```
  $HADOOP_HOME/bin/hadoop fs -put sample.csv /user/hadoop/sample.csv

## Create hive carbon table in spark shell

Please set spark.carbon.hive.schema.compatibility.enable=true in spark-defaults.conf
Start Spark shell by running the following command in the Spark directory:

```
./bin/spark-shell --jars <carbondata assembly jar path, carbondata hive jar path>
```

```
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.CarbonSession._
val rootPath = "hdfs:////user/hadoop/carbon"
val storeLocation = s"$rootPath/store"
val warehouse = s"$rootPath/warehouse"
val metastoredb = s"$rootPath/metastore_db"

val carbon = SparkSession.builder()
    .enableHiveSupport()
    .config("spark.sql.warehouse.dir", warehouse)
    .config(org.apache.carbondata.core.constants.CarbonCommonConstants.STORE_LOCATION, storeLocation)
    .getOrCreateCarbonSession(storeLocation, metastoredb)

carbon.sql("create table hive_carbon(id int, name string, scale decimal, country string, salary double) STORED BY 'carbondata'")
carbon.sql("LOAD DATA INPATH 'hdfs://mycluster/user/hadoop/sample.csv' INTO TABLE hive_carbon")

```

## Query Data from a Table

```
scala>carbon.sql("SELECT * FROM hive_carbon").show()
```

## Query Data in Hive

### Configure hive classpath
```
mkdir hive/auxlibs/
cp carbondata/assembly/target/scala-2.11/carbondata_2.11*.jar hive/auxlibs/
cp carbondata/integration/hive/target/carbondata-hive-*.jar hive/auxlibs/
cp $SPARK_HOME/jars/spark-catalyst*.jar hive/auxlibs/
export HIVE_AUX_JARS_PATH=hive/auxlibs/
```

### Alter schema in Hive
If you already set spark.carbon.hive.schema.compatibility.enable=true in spark-defaults.conf, please skip this secion.
For some tables which already exists, we need to alter schema in Hive.

$HIVE_HOME/bin/hive

```
alter table hive_carbon set FILEFORMAT
INPUTFORMAT "org.apache.carbondata.hive.MapredCarbonInputFormat"
OUTPUTFORMAT "org.apache.carbondata.hive.MapredCarbonOutputFormat"
SERDE "org.apache.carbondata.hive.CarbonHiveSerDe";

alter table hive_carbon set LOCATION 'hdfs://mycluster-tj/user/hadoop/carbon/store/default/hive_carbon';
alter table hive_carbon change col id INT;
alter table hive_carbon add columns(name string, scale decimal(10, 2), country string, salary double);

```

### Query data from hive table
```
set hive.mapred.supports.subdirectories=true;
set mapreduce.input.fileinputformat.input.dir.recursive=true;

select * from hive_carbon;

select * from hive_carbon order by id;
```


