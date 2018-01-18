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

# Installation Guide
This tutorial guides you through the installation and configuration of CarbonData in the following two modes :

* [Installing and Configuring CarbonData on Standalone Spark Cluster](#installing-and-configuring-carbondata-on-standalone-spark-cluster)
* [Installing and Configuring CarbonData on Spark on YARN Cluster](#installing-and-configuring-carbondata-on-spark-on-yarn-cluster)

followed by :

* [Query Execution using CarbonData Thrift Server](#query-execution-using-carbondata-thrift-server)

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

| Property | Value | Description |
|---------------------------------|-----------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------|
| spark.driver.extraJavaOptions | `-Dcarbon.properties.filepath = $SPARK_HOME/conf/carbon.properties` | A string of extra JVM options to pass to the driver. For instance, GC settings or other logging. |
| spark.executor.extraJavaOptions | `-Dcarbon.properties.filepath = $SPARK_HOME/conf/carbon.properties` | A string of extra JVM options to pass to executors. For instance, GC settings or other logging. **NOTE**: You can enter multiple values separated by space. |

7. Add the following properties in `$SPARK_HOME/conf/carbon.properties` file:

| Property             | Required | Description                                                                            | Example                             | Remark  |
|----------------------|----------|----------------------------------------------------------------------------------------|-------------------------------------|---------|
| carbon.storelocation | NO       | Location where data CarbonData will create the store and write the data in its own format. If not specified then it takes spark.sql.warehouse.dir path. | hdfs://HOSTNAME:PORT/Opt/CarbonStore      | Propose to set HDFS directory |


8. Verify the installation. For example:

```
./spark-shell --master spark://HOSTNAME:PORT --total-executor-cores 2
--executor-memory 2G
```

**NOTE**: Make sure you have permissions for CarbonData JARs and files through which driver and executor will start.

To get started with CarbonData : [Quick Start](quick-start-guide.md), [DDL Operations on CarbonData](ddl-operation-on-carbondata.md)

## Installing and Configuring CarbonData on Spark on YARN Cluster

   This section provides the procedure to install CarbonData on "Spark on YARN" cluster.

### Prerequisites
   * Hadoop HDFS and Yarn should be installed and running.
   * Spark should be installed and running in all the clients.
   * CarbonData user should have permission to access HDFS.

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

| Property | Description | Value |
|---------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------|
| spark.master | Set this value to run the Spark in yarn cluster mode. | Set yarn-client to run the Spark in yarn cluster mode. |
| spark.yarn.dist.files | Comma-separated list of files to be placed in the working directory of each executor. |`$SPARK_HOME/conf/carbon.properties` |
| spark.yarn.dist.archives | Comma-separated list of archives to be extracted into the working directory of each executor. |`$SPARK_HOME/carbonlib/carbondata.tar.gz` |
| spark.executor.extraJavaOptions | A string of extra JVM options to pass to executors. For instance  **NOTE**: You can enter multiple values separated by space. |`-Dcarbon.properties.filepath = carbon.properties` |
| spark.executor.extraClassPath | Extra classpath entries to prepend to the classpath of executors. **NOTE**: If SPARK_CLASSPATH is defined in spark-env.sh, then comment it and append the values in below parameter spark.driver.extraClassPath |`carbondata.tar.gz/carbonlib/*` |
| spark.driver.extraClassPath | Extra classpath entries to prepend to the classpath of the driver. **NOTE**: If SPARK_CLASSPATH is defined in spark-env.sh, then comment it and append the value in below parameter spark.driver.extraClassPath. |`$SPARK_HOME/carbonlib/*` |
| spark.driver.extraJavaOptions | A string of extra JVM options to pass to the driver. For instance, GC settings or other logging. |`-Dcarbon.properties.filepath = $SPARK_HOME/conf/carbon.properties` |


5. Add the following properties in `$SPARK_HOME/conf/carbon.properties`:

| Property | Required | Description | Example | Default Value |
|----------------------|----------|----------------------------------------------------------------------------------------|-------------------------------------|---------------|
| carbon.storelocation | NO | Location where CarbonData will create the store and write the data in its own format. If not specified then it takes spark.sql.warehouse.dir path.| hdfs://HOSTNAME:PORT/Opt/CarbonStore | Propose to set HDFS directory|

6. Verify the installation.

```
 ./bin/spark-shell --master yarn-client --driver-memory 1g
 --executor-cores 2 --executor-memory 2G
```
  **NOTE**: Make sure you have permissions for CarbonData JARs and files through which driver and executor will start.

  Getting started with CarbonData : [Quick Start](quick-start-guide.md), [DDL Operations on CarbonData](ddl-operation-on-carbondata.md)

## Query Execution Using CarbonData Thrift Server

### Starting CarbonData Thrift Server.

   a. cd `$SPARK_HOME`

   b. Run the following command to start the CarbonData thrift server.

```
./bin/spark-submit
--class org.apache.carbondata.spark.thriftserver.CarbonThriftServer
$SPARK_HOME/carbonlib/$CARBON_ASSEMBLY_JAR <carbon_store_path>
```

| Parameter | Description | Example |
|---------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------|
| CARBON_ASSEMBLY_JAR | CarbonData assembly jar name present in the `$SPARK_HOME/carbonlib/` folder. | carbondata_2.xx-x.x.x-SNAPSHOT-shade-hadoop2.7.2.jar |
| carbon_store_path | This is a parameter to the CarbonThriftServer class. This a HDFS path where CarbonData files will be kept. Strongly Recommended to put same as carbon.storelocation parameter of carbon.properties. If not specified then it takes spark.sql.warehouse.dir path. | `hdfs://<host_name>:port/user/hive/warehouse/carbon.store` |

**NOTE**: From Spark 1.6, by default the Thrift server runs in multi-session mode. Which means each JDBC/ODBC connection owns a copy of their own SQL configuration and temporary function registry. Cached tables are still shared though. If you prefer to run the Thrift server in single-session mode and share all SQL configuration and temporary function registry, please set option `spark.sql.hive.thriftServer.singleSession` to `true`. You may either add this option to `spark-defaults.conf`, or pass it to `spark-submit.sh` via `--conf`:

```
./bin/spark-submit
--conf spark.sql.hive.thriftServer.singleSession=true
--class org.apache.carbondata.spark.thriftserver.CarbonThriftServer
$SPARK_HOME/carbonlib/$CARBON_ASSEMBLY_JAR <carbon_store_path>
```

**But** in single-session mode, if one user changes the database from one connection, the database of the other connections will be changed too.

**Examples**
   
   * Start with default memory and executors.

```
./bin/spark-submit
--class org.apache.carbondata.spark.thriftserver.CarbonThriftServer 
$SPARK_HOME/carbonlib
/carbondata_2.xx-x.x.x-SNAPSHOT-shade-hadoop2.7.2.jar
hdfs://<host_name>:port/user/hive/warehouse/carbon.store
```
   
   * Start with Fixed executors and resources.

```
./bin/spark-submit
--class org.apache.carbondata.spark.thriftserver.CarbonThriftServer 
--num-executors 3 --driver-memory 20g --executor-memory 250g 
--executor-cores 32 
/srv/OSCON/BigData/HACluster/install/spark/sparkJdbc/lib
/carbondata_2.xx-x.x.x-SNAPSHOT-shade-hadoop2.7.2.jar
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

