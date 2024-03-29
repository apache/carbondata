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
This tutorial provides a quick introduction to use CarbonData. To follow along with this guide, download a packaged release of CarbonData from the [CarbonData website](https://dist.apache.org/repos/dist/release/carbondata/). Alternatively, it can be created following [Building CarbonData](https://github.com/apache/carbondata/tree/master/build) steps.

##  Prerequisites
* CarbonData supports Spark versions up to 2.4. Please download Spark package from [Spark website](https://spark.apache.org/downloads.html)

* Create a sample.csv file using the following commands. The CSV file is required for loading data into CarbonData

  ```
  cd carbondata
  cat > sample.csv << EOF
  id,name,city,age
  1,david,shenzhen,31
  2,eason,shenzhen,27
  3,jarry,wuhan,35
  EOF
  ```

## Integration

### Integration with Execution Engines
CarbonData can be integrated with Spark, Presto, Flink and Hive execution engines. The below documentation guides on Installing and Configuring with these execution engines.

#### Spark
[Installing and Configuring CarbonData to run locally with Spark SQL CLI](#installing-and-configuring-carbondata-to-run-locally-with-spark-sql-cli)

[Installing and Configuring CarbonData to run locally with Spark Shell](#installing-and-configuring-carbondata-to-run-locally-with-spark-shell)

[Installing and Configuring CarbonData on Standalone Spark Cluster](#installing-and-configuring-carbondata-on-standalone-spark-cluster)

[Installing and Configuring CarbonData on Spark on YARN Cluster](#installing-and-configuring-carbondata-on-spark-on-yarn-cluster)

[Installing and Configuring CarbonData Thrift Server for Query Execution](#query-execution-using-the-thrift-server)

### Notebook
[Using CarbonData in notebook](#using-carbondata-in-notebook.md)  
[Using CarbonData to visualization in notebook](#using-carbondata-to-visualization_in-notebook.md)

#### Presto
[Installing and Configuring CarbonData on Presto](#installing-and-configuring-carbondata-on-presto)

#### Hive
[Installing and Configuring CarbonData on Hive](./hive-guide.md)

### Integration with Storage Engines
#### HDFS
[CarbonData supports read and write with HDFS](#installing-and-configuring-carbondata-on-standalone-spark-cluster)

#### S3
[CarbonData supports read and write with S3](./s3-guide.md) 

#### Alluxio
[CarbonData supports read and write with Alluxio](./alluxio-guide.md)

## Installing and Configuring CarbonData to run locally with Spark SQL CLI

This will work with spark 2.3+ versions. In Spark SQL CLI, it uses CarbonExtensions to customize the SparkSession with CarbonData's parser, analyzer, optimizer and physical planning strategy rules in Spark.
To enable CarbonExtensions, we need to add the following configuration.

|Key|Value|
|---|---|
|spark.sql.extensions|org.apache.spark.sql.CarbonExtensions| 

Start Spark SQL CLI by running the following command in the Spark directory:

```
./bin/spark-sql --conf spark.sql.extensions=org.apache.spark.sql.CarbonExtensions --jars <carbondata assembly jar path>
```
###### Creating a Table

```
CREATE TABLE IF NOT EXISTS test_table (
  id string,
  name string,
  city string,
  age Int)
STORED AS carbondata;
```
**NOTE**: CarbonExtensions only support "STORED AS carbondata" and "USING carbondata"

###### Loading Data to a Table

```
LOAD DATA INPATH '/local-path/sample.csv' INTO TABLE test_table;

LOAD DATA INPATH 'hdfs://hdfs-path/sample.csv' INTO TABLE test_table;
```

```
insert into table test_table select '1', 'name1', 'city1', 1;
```

**NOTE**: Please provide the real file path of `sample.csv` for the above script. 
If you get "tablestatus.lock" issue, please refer to [FAQ](faq.md)

###### Query Data from a Table

```
SELECT * FROM test_table;
```

```
SELECT city, avg(age), sum(age)
FROM test_table
GROUP BY city;
```

## Installing and Configuring CarbonData to run locally with Spark Shell

Apache Spark Shell provides a simple way to learn the API, as well as a powerful tool to analyze data interactively. Please visit [Apache Spark Documentation](http://spark.apache.org/docs/latest/) for more details on the Spark shell.

#### Basics

###### Option 1: Using CarbonSession (deprecated since 2.0)
Start Spark shell by running the following command in the Spark directory:

```
./bin/spark-shell --jars <carbondata assembly jar path>
```
**NOTE**: Path where packaged release of CarbonData was downloaded or assembly jar will be available after [building CarbonData](https://github.com/apache/carbondata/blob/master/build/README.md) and can be copied from `./assembly/target/scala-2.1x/apache-carbondata_xxx.jar`

In this shell, SparkSession is readily available as `spark` and Spark context is readily available as `sc`.

In order to create a CarbonSession we will have to configure it explicitly in the following manner :

* Import the following :

```
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.CarbonSession._
```

* Create a CarbonSession :

```
val carbon = SparkSession.builder().config(sc.getConf).getOrCreateCarbonSession("<carbon_store_path>")
```
**NOTE** 
 - By default metastore location points to `../carbon.metastore`, user can provide own metastore location to CarbonSession like 
   `SparkSession.builder().config(sc.getConf).getOrCreateCarbonSession("<carbon_store_path>", "<local metastore path>")`.
 - Data storage location can be specified by `<carbon_store_path>`, like `/carbon/data/store`, `hdfs://localhost:9000/carbon/data/store` or `s3a://carbon/data/store`.

###### Option 2: Using SparkSession with CarbonExtensions(since 2.0)

Start Spark shell by running the following command in the Spark directory:

```
./bin/spark-shell --conf spark.sql.extensions=org.apache.spark.sql.CarbonExtensions --jars <carbondata assembly jar path>
```

In this shell, SparkSession is readily available as `spark` and Spark context is readily available as `sc`.

In order to create a SparkSession we will have to configure it explicitly in the following manner :

* Import the following :

```
import org.apache.spark.sql.SparkSession
```

**NOTE** 
 - In this flow, we can use the built-in SparkSession `spark` instead of `carbon`.
   We also can create a new SparkSession instead of the built-in SparkSession `spark` if need. 
   It need to add "org.apache.spark.sql.CarbonExtensions" into spark configuration "spark.sql.extensions". 
   ```
   val spark = SparkSession
     .builder()
     .config(sc.getConf)
     .enableHiveSupport
     .config("spark.sql.extensions","org.apache.spark.sql.CarbonExtensions")
     .getOrCreate()
   ```
 - Data storage location can be specified by "spark.sql.warehouse.dir".

#### Executing Queries

###### Creating a Table
**NOTE** :
We use the built-in SparkSession `spark` in the following

```
carbon.sql(
           s"""
              | CREATE TABLE IF NOT EXISTS test_table(
              |   id string,
              |   name string,
              |   city string,
              |   age Int)
              | STORED AS carbondata
           """.stripMargin)
```
**NOTE**: 
The following table list all supported syntax:

|create table |SparkSession with CarbonExtensions | CarbonSession|
|---|---|---|
| STORED AS carbondata|yes|yes|
| USING carbondata|yes|yes|
| STORED BY 'carbondata'|no|yes|
| STORED BY 'org.apache.carbondata.format'|no|yes|

We suggest to use CarbonExtensions instead of CarbonSession.

###### Loading Data to a Table

```
carbon.sql("LOAD DATA INPATH '/local-path/sample.csv' INTO TABLE test_table")

carbon.sql("LOAD DATA INPATH 'hdfs://hdfs-path/sample.csv' INTO TABLE test_table")
```

**NOTE**: Please provide the real file path of `sample.csv` for the above script. 
If you get "tablestatus.lock" issue, please refer to [FAQ](faq.md)

###### Query Data from a Table

```
carbon.sql("SELECT * FROM test_table").show()

carbon.sql(
           s"""
              | SELECT city, avg(age), sum(age)
              | FROM test_table
              | GROUP BY city
           """.stripMargin).show()
```



## Installing and Configuring CarbonData on Standalone Spark Cluster

### Prerequisites

- Hadoop HDFS and Yarn should be installed and running.
- Spark should be installed and running on all the cluster nodes.
- CarbonData user should have permission to access HDFS.

### Procedure

1. [Build the CarbonData](https://github.com/apache/carbondata/blob/master/build/README.md) project and get the assembly jar from `./assembly/target/scala-2.1x/apache-carbondata_xxx.jar`. 

2. Copy `./assembly/target/scala-2.1x/apache-carbondata_xxx.jar` to `$SPARK_HOME/carbonlib` folder.

   **NOTE**: Create the carbonlib folder if it does not exist inside `$SPARK_HOME` path.

3. Add the carbonlib folder path in the Spark classpath. (Edit `$SPARK_HOME/conf/spark-env.sh` file and modify the value of `SPARK_CLASSPATH` by appending `$SPARK_HOME/carbonlib/*` to the existing value)

4. Copy the `./conf/carbon.properties.template` file from CarbonData repository to `$SPARK_HOME/conf/` folder and rename the file to `carbon.properties`. All the carbondata related properties are configured in this file.

5. Repeat Step 2 to Step 5 in all the nodes of the cluster.

6. In Spark node[master], configure the properties mentioned in the following table in `$SPARK_HOME/conf/spark-defaults.conf` file.

| Property                   | Value                                                        | Description                                                  |
| -------------------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| spark.driver.extraJavaOptions   | `-Dcarbon.properties.filepath = $SPARK_HOME/conf/carbon.properties` | A string of extra JVM options to pass to the driver. For instance, GC settings or other logging. |
| spark.executor.extraJavaOptions | `-Dcarbon.properties.filepath = $SPARK_HOME/conf/carbon.properties` | A string of extra JVM options to pass to executors. For instance, GC settings or other logging. **NOTE**: You can enter multiple values separated by space. |


**NOTE**: Please provide the real directory file path of "SPARK_HOME" instead of the "$SPARK_HOME" for the above script and there is no space on both sides of `=` in the 'Value' column.

7. Verify the installation. For example:

```
./bin/spark-shell \
--master spark://HOSTNAME:PORT \
--total-executor-cores 2 \
--executor-memory 2G
```

**NOTE**: 
 - property "carbon.storelocation" is deprecated in carbondata 2.0 version. Only the users who used this property in previous versions can still use it in carbon 2.0 version.
 - Make sure you have permissions for CarbonData JARs and files through which driver and executor will start.

## Installing and Configuring CarbonData on Spark on YARN Cluster

   This section provides the procedure to install CarbonData on "Spark on YARN" cluster.

### Prerequisites

- Hadoop HDFS and Yarn should be installed and running.
- Spark should be installed and running in all the clients.
- CarbonData user should have permission to access HDFS.

### Procedure

   The following steps are only for Driver Nodes. (Driver nodes are the one which starts the spark context.)

1. [Build the CarbonData](https://github.com/apache/carbondata/blob/master/build/README.md) project and get the assembly jar from `./assembly/target/scala-2.1x/apache-carbondata_xxx.jar` and copy to `$SPARK_HOME/carbonlib` folder.

   **NOTE**: Create the carbonlib folder if it does not exists inside `$SPARK_HOME` path.

2. Copy the `./conf/carbon.properties.template` file from CarbonData repository to `$SPARK_HOME/conf/` folder and rename the file to `carbon.properties`. All the carbondata related properties are configured in this file.

3. Create `tar.gz` file of carbonlib folder and move it inside the carbonlib folder.

```
cd $SPARK_HOME
tar -zcvf carbondata.tar.gz carbonlib/
mv carbondata.tar.gz carbonlib/
```

4. Configure the properties mentioned in the following table in `$SPARK_HOME/conf/spark-defaults.conf` file.

| Property                        | Description                                                                                      | Value                                                        |
| ------------------------------- | ------------------------------------------------------------------------------------------------ | ------------------------------------------------------------ |
| spark.master                    | Set this value to run the Spark in yarn cluster mode.        | Set yarn-client to run the Spark in yarn cluster mode.       |
| spark.yarn.dist.files           | Comma-separated list of files to be placed in the working directory of each executor. | `$SPARK_HOME/conf/carbon.properties`                         |
| spark.yarn.dist.archives        | Comma-separated list of archives to be extracted into the working directory of each executor. | `$SPARK_HOME/carbonlib/carbondata.tar.gz`                    |
| spark.executor.extraJavaOptions | A string of extra JVM options to pass to executors. For instance  **NOTE**: You can enter multiple values separated by space. | `-Dcarbon.properties.filepath = carbon.properties`           |
| spark.executor.extraClassPath   | Extra classpath entries to prepend to the classpath of executors. **NOTE**: If SPARK_CLASSPATH is defined in spark-env.sh, then comment it and append the values in below parameter spark.driver.extraClassPath | `carbondata.tar.gz/carbonlib/*`                              |
| spark.driver.extraClassPath     | Extra classpath entries to prepend to the classpath of the driver. **NOTE**: If SPARK_CLASSPATH is defined in spark-env.sh, then comment it and append the value in below parameter spark.driver.extraClassPath. | `$SPARK_HOME/carbonlib/*`                                    |
| spark.driver.extraJavaOptions   | A string of extra JVM options to pass to the driver. For instance, GC settings or other logging. | `-Dcarbon.properties.filepath = $SPARK_HOME/conf/carbon.properties` |

**NOTE**: Please provide the real directory file path of "SPARK_HOME" instead of the "$SPARK_HOME" for the above script and there is no space on both sides of `=` in the 'Value' column.

5. Verify the installation.

```
./bin/spark-shell \
--master yarn-client \
--driver-memory 1G \
--executor-memory 2G \
--executor-cores 2
```

**NOTE**:
 - property "carbon.storelocation" is deprecated in carbondata 2.0 version. Only the users who used this property in previous versions can still use it in carbon 2.0 version.
 - Make sure you have permissions for CarbonData JARs and files through which driver and executor will start.
 - If use Spark + Hive 1.1.X, it needs to add carbondata assembly jar and carbondata-hive jar into parameter 'spark.sql.hive.metastore.jars' in spark-default.conf file.



## Query Execution Using the Thrift Server

### Option 1: Starting Thrift Server with CarbonExtensions(since 2.0)
```
cd $SPARK_HOME
./sbin/start-thriftserver.sh \
--conf spark.sql.extensions=org.apache.spark.sql.CarbonExtensions \
$SPARK_HOME/carbonlib/apache-carbondata-xxx.jar
```

### Option 2: Starting CarbonData Thrift Server

a. cd `$SPARK_HOME`

b. Run the following command to start the CarbonData thrift server.

```
./bin/spark-submit \
--class org.apache.carbondata.spark.thriftserver.CarbonThriftServer \
$SPARK_HOME/carbonlib/$CARBON_ASSEMBLY_JAR
```

| Parameter           | Description                                                  | Example                                                    |
| ------------------- | ------------------------------------------------------------ | ---------------------------------------------------------- |
| CARBON_ASSEMBLY_JAR | CarbonData assembly jar name present in the `$SPARK_HOME/carbonlib/` folder. | apache-carbondata-xx.jar       |

c. Run the following command to work with S3 storage.

```
./bin/spark-submit \
--class org.apache.carbondata.spark.thriftserver.CarbonThriftServer \
$SPARK_HOME/carbonlib/$CARBON_ASSEMBLY_JAR <access_key> <secret_key> <endpoint>
```

| Parameter           | Description                                                  | Example                                                    |
| ------------------- | ------------------------------------------------------------ | ---------------------------------------------------------- |
| CARBON_ASSEMBLY_JAR | CarbonData assembly jar name present in the `$SPARK_HOME/carbonlib/` folder. | apache-carbondata-xx.jar       |
| access_key   | Access key for S3 storage |
| secret_key   | Secret key for S3 storage |
| endpoint   | Endpoint for connecting to S3 storage |

**NOTE**: From Spark 1.6, by default the Thrift server runs in multi-session mode. Which means each JDBC/ODBC connection owns a copy of their own SQL configuration and temporary function registry. Cached tables are still shared though. If you prefer to run the Thrift server in single-session mode and share all SQL configuration and temporary function registry, please set option `spark.sql.hive.thriftServer.singleSession` to `true`. You may either add this option to `spark-defaults.conf`, or pass it to `spark-submit.sh` via `--conf`:

```
./bin/spark-submit \
--conf spark.sql.hive.thriftServer.singleSession=true \
--class org.apache.carbondata.spark.thriftserver.CarbonThriftServer \
$SPARK_HOME/carbonlib/$CARBON_ASSEMBLY_JAR
```

**But** in single-session mode, if one user changes the database from one connection, the database of the other connections will be changed too.

**Examples**

- Start with default memory and executors.

```
./bin/spark-submit \
--class org.apache.carbondata.spark.thriftserver.CarbonThriftServer \
$SPARK_HOME/carbonlib/apache-carbondata-xxx.jar
```

- Start with Fixed executors and resources.

```
./bin/spark-submit \
--class org.apache.carbondata.spark.thriftserver.CarbonThriftServer \
--num-executors 3 \
--driver-memory 20G \
--executor-memory 250G \
--executor-cores 32 \
$SPARK_HOME/carbonlib/apache-carbondata-xxx.jar
```

### Connecting to Thrift Server Using Beeline.

```
cd $SPARK_HOME
./bin/beeline -u jdbc:hive2://<thriftserver_host>:port

Example
./bin/beeline -u jdbc:hive2://10.10.10.10:10000
```



## Installing and Configuring CarbonData on Presto

**NOTE:** **CarbonData tables cannot be created nor loaded from Presto. User needs to create CarbonData Table and load data into it
either with [Spark](#installing-and-configuring-carbondata-to-run-locally-with-spark-shell) or [SDK](./sdk-guide.md) or [C++ SDK](./csdk-guide.md).
Once the table is created, it can be queried from Presto.**

Please refer the presto guide linked below.

prestodb guide  - [prestodb](./prestodb-guide.md)

prestosql guide - [prestosql](./prestosql-guide.md)

Once installed the presto with carbonData as per the above guide,
you can use the Presto CLI on the coordinator to query data sources in the catalog using the Presto workers.

List the schemas(databases) available

```
show schemas;
```

Selected the schema where CarbonData table resides

```
use carbonschema;
```

List the available tables

```
show tables;
```

Query from the available tables

```
select * from carbon_table;
```

**Note:** Create Tables and data loads should be done before executing queries as we can not create carbon table from this interface.
