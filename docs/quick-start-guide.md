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
This tutorial provides a quick introduction to using CarbonData. To follow along with this guide, first download a packaged release of CarbonData from the [CarbonData website](https://dist.apache.org/repos/dist/release/carbondata/).Alternatively it can be created following [Building CarbonData](https://github.com/apache/carbondata/tree/master/build) steps.

##  Prerequisites
* CarbonData supports Spark versions upto 2.2.1.Please download Spark package from [Spark website](https://spark.apache.org/downloads.html)

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
CarbonData can be integrated with Spark,Presto and Hive execution engines. The below documentation guides on Installing and Configuring with these execution engines.

#### Spark

[Installing and Configuring CarbonData to run locally with Spark Shell](#installing-and-configuring-carbondata-to-run-locally-with-spark-shell)

[Installing and Configuring CarbonData on Standalone Spark Cluster](#installing-and-configuring-carbondata-on-standalone-spark-cluster)

[Installing and Configuring CarbonData on Spark on YARN Cluster](#installing-and-configuring-carbondata-on-spark-on-yarn-cluster)

[Installing and Configuring CarbonData Thrift Server for Query Execution](#query-execution-using-carbondata-thrift-server)


#### Presto
[Installing and Configuring CarbonData on Presto](#installing-and-configuring-carbondata-on-presto)

#### Hive
[Installing and Configuring CarbonData on Hive](https://github.com/apache/carbondata/blob/master/docs/hive-guide.md)

### Integration with Storage Engines
#### HDFS
[CarbonData supports read and write with HDFS](https://github.com/apache/carbondata/blob/master/docs/quick-start-guide.md#installing-and-configuring-carbondata-on-standalone-spark-cluster)

#### S3
[CarbonData supports read and write with S3](https://github.com/apache/carbondata/blob/master/docs/s3-guide.md) 

#### Alluxio
[CarbonData supports read and write with Alluxio](https://github.com/apache/carbondata/blob/master/docs/alluxio-guide.md)

## Installing and Configuring CarbonData to run locally with Spark Shell

Apache Spark Shell provides a simple way to learn the API, as well as a powerful tool to analyze data interactively. Please visit [Apache Spark Documentation](http://spark.apache.org/docs/latest/) for more details on Spark shell.

#### Basics

Start Spark shell by running the following command in the Spark directory:

```
./bin/spark-shell --jars <carbondata assembly jar path>
```
**NOTE**: Path where packaged release of CarbonData was downloaded or assembly jar will be available after [building CarbonData](https://github.com/apache/carbondata/blob/master/build/README.md) and can be copied from `./assembly/target/scala-2.1x/carbondata_xxx.jar`

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

#### Executing Queries

###### Creating a Table

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

###### Loading Data to a Table

```
carbon.sql("LOAD DATA INPATH '/path/to/sample.csv' INTO TABLE test_table")
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

1. [Build the CarbonData](https://github.com/apache/carbondata/blob/master/build/README.md) project and get the assembly jar from `./assembly/target/scala-2.1x/carbondata_xxx.jar`. 

2. Copy `./assembly/target/scala-2.1x/carbondata_xxx.jar` to `$SPARK_HOME/carbonlib` folder.

   **NOTE**: Create the carbonlib folder if it does not exist inside `$SPARK_HOME` path.

3. Add the carbonlib folder path in the Spark classpath. (Edit `$SPARK_HOME/conf/spark-env.sh` file and modify the value of `SPARK_CLASSPATH` by appending `$SPARK_HOME/carbonlib/*` to the existing value)

4. Copy the `./conf/carbon.properties.template` file from CarbonData repository to `$SPARK_HOME/conf/` folder and rename the file to `carbon.properties`.

5. Repeat Step 2 to Step 5 in all the nodes of the cluster.

6. In Spark node[master], configure the properties mentioned in the following table in `$SPARK_HOME/conf/spark-defaults.conf` file.

| Property                        | Value                                                        | Description                                                  |
| ------------------------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| spark.driver.extraJavaOptions   | `-Dcarbon.properties.filepath = $SPARK_HOME/conf/carbon.properties` | A string of extra JVM options to pass to the driver. For instance, GC settings or other logging. |
| spark.executor.extraJavaOptions | `-Dcarbon.properties.filepath = $SPARK_HOME/conf/carbon.properties` | A string of extra JVM options to pass to executors. For instance, GC settings or other logging. **NOTE**: You can enter multiple values separated by space. |

7. Add the following properties in `$SPARK_HOME/conf/carbon.properties` file:

| Property             | Required | Description                                                  | Example                              | Remark                        |
| -------------------- | -------- | ------------------------------------------------------------ | ------------------------------------ | ----------------------------- |
| carbon.storelocation | NO       | Location where data CarbonData will create the store and write the data in its own format. If not specified then it takes spark.sql.warehouse.dir path. | hdfs://HOSTNAME:PORT/Opt/CarbonStore | Propose to set HDFS directory |

8. Verify the installation. For example:

```
./bin/spark-shell \
--master spark://HOSTNAME:PORT \
--total-executor-cores 2 \
--executor-memory 2G
```

**NOTE**: Make sure you have permissions for CarbonData JARs and files through which driver and executor will start.

## Installing and Configuring CarbonData on Spark on YARN Cluster

   This section provides the procedure to install CarbonData on "Spark on YARN" cluster.

### Prerequisites

- Hadoop HDFS and Yarn should be installed and running.
- Spark should be installed and running in all the clients.
- CarbonData user should have permission to access HDFS.

### Procedure

   The following steps are only for Driver Nodes. (Driver nodes are the one which starts the spark context.)

1. [Build the CarbonData](https://github.com/apache/carbondata/blob/master/build/README.md) project and get the assembly jar from `./assembly/target/scala-2.1x/carbondata_xxx.jar` and copy to `$SPARK_HOME/carbonlib` folder.

   **NOTE**: Create the carbonlib folder if it does not exists inside `$SPARK_HOME` path.

2. Copy the `./conf/carbon.properties.template` file from CarbonData repository to `$SPARK_HOME/conf/` folder and rename the file to `carbon.properties`.

3. Create `tar.gz` file of carbonlib folder and move it inside the carbonlib folder.

```
cd $SPARK_HOME
tar -zcvf carbondata.tar.gz carbonlib/
mv carbondata.tar.gz carbonlib/
```

4. Configure the properties mentioned in the following table in `$SPARK_HOME/conf/spark-defaults.conf` file.

| Property                        | Description                                                  | Value                                                        |
| ------------------------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| spark.master                    | Set this value to run the Spark in yarn cluster mode.        | Set yarn-client to run the Spark in yarn cluster mode.       |
| spark.yarn.dist.files           | Comma-separated list of files to be placed in the working directory of each executor. | `$SPARK_HOME/conf/carbon.properties`                         |
| spark.yarn.dist.archives        | Comma-separated list of archives to be extracted into the working directory of each executor. | `$SPARK_HOME/carbonlib/carbondata.tar.gz`                    |
| spark.executor.extraJavaOptions | A string of extra JVM options to pass to executors. For instance  **NOTE**: You can enter multiple values separated by space. | `-Dcarbon.properties.filepath = carbon.properties`           |
| spark.executor.extraClassPath   | Extra classpath entries to prepend to the classpath of executors. **NOTE**: If SPARK_CLASSPATH is defined in spark-env.sh, then comment it and append the values in below parameter spark.driver.extraClassPath | `carbondata.tar.gz/carbonlib/*`                              |
| spark.driver.extraClassPath     | Extra classpath entries to prepend to the classpath of the driver. **NOTE**: If SPARK_CLASSPATH is defined in spark-env.sh, then comment it and append the value in below parameter spark.driver.extraClassPath. | `$SPARK_HOME/carbonlib/*`                                    |
| spark.driver.extraJavaOptions   | A string of extra JVM options to pass to the driver. For instance, GC settings or other logging. | `-Dcarbon.properties.filepath = $SPARK_HOME/conf/carbon.properties` |

5. Add the following properties in `$SPARK_HOME/conf/carbon.properties`:

| Property             | Required | Description                                                  | Example                              | Default Value                 |
| -------------------- | -------- | ------------------------------------------------------------ | ------------------------------------ | ----------------------------- |
| carbon.storelocation | NO       | Location where CarbonData will create the store and write the data in its own format. If not specified then it takes spark.sql.warehouse.dir path. | hdfs://HOSTNAME:PORT/Opt/CarbonStore | Propose to set HDFS directory |

6. Verify the installation.

```
./bin/spark-shell \
--master yarn-client \
--driver-memory 1G \
--executor-memory 2G \
--executor-cores 2
```

**NOTE**: Make sure you have permissions for CarbonData JARs and files through which driver and executor will start.



## Query Execution Using CarbonData Thrift Server

### Starting CarbonData Thrift Server.

a. cd `$SPARK_HOME`

b. Run the following command to start the CarbonData thrift server.

```
./bin/spark-submit \
--class org.apache.carbondata.spark.thriftserver.CarbonThriftServer \
$SPARK_HOME/carbonlib/$CARBON_ASSEMBLY_JAR <carbon_store_path>
```

| Parameter           | Description                                                  | Example                                                    |
| ------------------- | ------------------------------------------------------------ | ---------------------------------------------------------- |
| CARBON_ASSEMBLY_JAR | CarbonData assembly jar name present in the `$SPARK_HOME/carbonlib/` folder. | carbondata_2.xx-x.x.x-SNAPSHOT-shade-hadoop2.7.2.jar       |
| carbon_store_path   | This is a parameter to the CarbonThriftServer class. This a HDFS path where CarbonData files will be kept. Strongly Recommended to put same as carbon.storelocation parameter of carbon.properties. If not specified then it takes spark.sql.warehouse.dir path. | `hdfs://<host_name>:port/user/hive/warehouse/carbon.store` |

**NOTE**: From Spark 1.6, by default the Thrift server runs in multi-session mode. Which means each JDBC/ODBC connection owns a copy of their own SQL configuration and temporary function registry. Cached tables are still shared though. If you prefer to run the Thrift server in single-session mode and share all SQL configuration and temporary function registry, please set option `spark.sql.hive.thriftServer.singleSession` to `true`. You may either add this option to `spark-defaults.conf`, or pass it to `spark-submit.sh` via `--conf`:

```
./bin/spark-submit \
--conf spark.sql.hive.thriftServer.singleSession=true \
--class org.apache.carbondata.spark.thriftserver.CarbonThriftServer \
$SPARK_HOME/carbonlib/$CARBON_ASSEMBLY_JAR <carbon_store_path>
```

**But** in single-session mode, if one user changes the database from one connection, the database of the other connections will be changed too.

**Examples**

- Start with default memory and executors.

```
./bin/spark-submit \
--class org.apache.carbondata.spark.thriftserver.CarbonThriftServer \
$SPARK_HOME/carbonlib/carbondata_2.xx-x.x.x-SNAPSHOT-shade-hadoop2.7.2.jar \
hdfs://<host_name>:port/user/hive/warehouse/carbon.store
```

- Start with Fixed executors and resources.

```
./bin/spark-submit \
--class org.apache.carbondata.spark.thriftserver.CarbonThriftServer \
--num-executors 3 \
--driver-memory 20G \
--executor-memory 250G \
--executor-cores 32 \
$SPARK_HOME/carbonlib/carbondata_2.xx-x.x.x-SNAPSHOT-shade-hadoop2.7.2.jar \
hdfs://<host_name>:port/user/hive/warehouse/carbon.store
```

### Connecting to CarbonData Thrift Server Using Beeline.

```
cd $SPARK_HOME
./sbin/start-thriftserver.sh
./bin/beeline -u jdbc:hive2://<thriftserver_host>:port

Example
./bin/beeline -u jdbc:hive2://10.10.10.10:10000
```



## Installing and Configuring CarbonData on Presto

**NOTE:** **CarbonData tables cannot be created nor loaded from Presto. User need to create CarbonData Table and load data into it
either with [Spark](#installing-and-configuring-carbondata-to-run-locally-with-spark-shell) or [SDK](./sdk-guide.md) or [C++ SDK](./csdk-guide.md).
Once the table is created,it can be queried from Presto.**


### Installing Presto

1. Download the 0.210 version of Presto using:
`wget https://repo1.maven.org/maven2/com/facebook/presto/presto-server/0.210/presto-server-0.210.tar.gz`

2. Extract Presto tar file: `tar zxvf presto-server-0.210.tar.gz`.

3. Download the Presto CLI for the coordinator and name it presto.

```
wget https://repo1.maven.org/maven2/com/facebook/presto/presto-cli/0.210/presto-cli-0.210-executable.jar

mv presto-cli-0.210-executable.jar presto

chmod +x presto
```

### Create Configuration Files

1. Create `etc` folder in presto-server-0.210 directory.
2. Create `config.properties`, `jvm.config`, `log.properties`, and `node.properties` files.
3. Install uuid to generate a node.id.

  ```
  sudo apt-get install uuid

  uuid
  ```


##### Contents of your node.properties file

```
node.environment=production
node.id=<generated uuid>
node.data-dir=/home/ubuntu/data
```

##### Contents of your jvm.config file

```
-server
-Xmx16G
-XX:+UseG1GC
-XX:G1HeapRegionSize=32M
-XX:+UseGCOverheadLimit
-XX:+ExplicitGCInvokesConcurrent
-XX:+HeapDumpOnOutOfMemoryError
-XX:OnOutOfMemoryError=kill -9 %p
```

##### Contents of your log.properties file

```
com.facebook.presto=INFO
```

 The default minimum level is `INFO`. There are four levels: `DEBUG`, `INFO`, `WARN` and `ERROR`.

### Coordinator Configurations

##### Contents of your config.properties

```
coordinator=true
node-scheduler.include-coordinator=false
http-server.http.port=8086
query.max-memory=5GB
query.max-total-memory-per-node=5GB
query.max-memory-per-node=3GB
memory.heap-headroom-per-node=1GB
discovery-server.enabled=true
discovery.uri=http://localhost:8086
task.max-worker-threads=4
optimizer.dictionary-aggregation=true
optimizer.optimize-hash-generation = false
```
The options `node-scheduler.include-coordinator=false` and `coordinator=true` indicate that the node is the coordinator and tells the coordinator not to do any of the computation work itself and to use the workers.

**Note**: It is recommended to set `query.max-memory-per-node` to half of the JVM config max memory, though the workload is highly concurrent, lower value for `query.max-memory-per-node` is to be used.

Also relation between below two configuration-properties should be like:
If, `query.max-memory-per-node=30GB`
Then, `query.max-memory=<30GB * number of nodes>`.

### Worker Configurations

##### Contents of your config.properties

```
coordinator=false
http-server.http.port=8086
query.max-memory=5GB
query.max-memory-per-node=2GB
discovery.uri=<coordinator_ip>:8086
```

**Note**: `jvm.config` and `node.properties` files are same for all the nodes (worker + coordinator). All the nodes should have different `node.id`.(generated by uuid command).

### Catalog Configurations

1. Create a folder named `catalog` in etc directory of presto on all the nodes of the cluster including the coordinator.

##### Configuring Carbondata in Presto
1. Create a file named `carbondata.properties` in the `catalog` folder and set the required properties on all the nodes.

### Add Plugins

1. Create a directory named `carbondata` in plugin directory of presto.
2. Copy `carbondata` jars to `plugin/carbondata` directory on all nodes.

### Start Presto Server on all nodes

```
./presto-server-0.210/bin/launcher start
```
To run it as a background process.

```
./presto-server-0.210/bin/launcher run
```
To run it in foreground.

### Start Presto CLI

```
./presto
```
To connect to carbondata catalog use the following command:

```
./presto --server <coordinator_ip>:8086 --catalog carbondata --schema <schema_name>
```
Execute the following command to ensure the workers are connected.

```
select * from system.runtime.nodes;
```
Now you can use the Presto CLI on the coordinator to query data sources in the catalog using the Presto workers.

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

**Note :** Create Tables and data loads should be done before executing queries as we can not create carbon table from this interface.

```