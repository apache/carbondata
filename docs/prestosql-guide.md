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


# Prestosql guide
This tutorial provides a quick introduction to using current integration/presto module.


[Presto Multinode Cluster Setup for Carbondata](#presto-multinode-cluster-setup-for-carbondata)

[Presto Single Node Setup for Carbondata](#presto-single-node-setup-for-carbondata)

[Presto Setup with CarbonData Distributed IndexServer](#presto-setup-with-carbondata-distributed-indexserver)

## Presto SQL is now Trino

⚠️ PrestoSQL is rename as Trino. The software and the community you have come to love and depend on aren’t going anywhere, we are simply renaming. Trino is the new name for PrestoSQL,
the project supported by the founders and creators of Presto® along with the major contributors – just under a shiny new name. And now you can find us here:

GitHub: https://github.com/trinodb/trino.


## Presto Multinode Cluster Setup for Carbondata
### Installing Presto

To know about which version of presto is supported by this version of carbon, visit
https://github.com/apache/carbondata/blob/master/pom.xml
and look for ```<presto.version>``` inside `prestosql` profile.

_Example:_
`<presto.version>333</presto.version>`
This means current version of carbon supports presto 333 version.

_Note:_
Currently carbondata supports only one version of presto, cannot handle multiple versions at same time. If user wish to use older version of presto, then need to use older version of carbon (other old branches, say branch-1.5 and check the supported presto version in it's pom.xml file in integration/presto/)

1. Download that version of Presto (say 333) using below command:
  ```
  wget https://repo1.maven.org/maven2/io/prestosql/presto-server/333/presto-server-333.tar.gz
  ```

2. Extract Presto tar file: `tar zxvf presto-server-333.tar.gz`.

3. Download the Presto CLI of the same presto server version (say 333) for the coordinator and name it presto.

  ```
    wget https://repo1.maven.org/maven2/io/prestosql/presto-cli/333/presto-cli-333-executable.jar

    mv presto-cli-333-executable.jar presto

    chmod +x presto
  ```

### Create Configuration Files

1. Create `etc` folder in presto-server-333 directory.
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
  io.prestosql=INFO
  ```

The default minimum level is `INFO`. There are four levels: `DEBUG`, `INFO`, `WARN` and `ERROR`.

## Coordinator Configurations

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
  discovery.uri=http://<coordinator_ip>:8086
  ```
The options `node-scheduler.include-coordinator=false` and `coordinator=true` indicate that the node is the coordinator and tells the coordinator not to do any of the computation work itself and to use the workers.


**Note**: We recommend setting `query.max-memory-per-node` to half of the JVM config max memory, though if your workload is highly concurrent, you may want to use a lower value for `query.max-memory-per-node`.

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
  discovery.uri=http://<coordinator_ip>:8086
  ```

**Note**: `jvm.config` and `node.properties` files are same for all the nodes (worker + coordinator). All the nodes should have different `node.id`.

### Catalog Configurations

1. Create a folder named `catalog` in etc directory of presto on all the nodes of the cluster including the coordinator.

##### Configuring Carbondata in Presto
1. Create a file named `carbondata.properties` in the `catalog` folder and set the required properties on all the nodes.
2. As carbondata connector extends hive connector all the configurations(including S3) is same as hive connector.
   Just replace the connector name in hive configuration and copy same to carbondata.properties
   `connector.name = carbondata`

### Add Plugins

1. Create a directory named `carbondata` in plugin directory of presto.
2. Copy all the jars from ../integration/presto/target/carbondata-presto-X.Y.Z-SNAPSHOT to `plugin/carbondata` directory on all nodes.


### Start Presto Server on all nodes

```
./presto-server-333/bin/launcher start
```
To run it as a background process.

```
./presto-server-333/bin/launcher run
```
To run it in foreground.

### Start Presto CLI

To connect to carbondata catalog use the following command:

```
./presto --server <coordinator_ip>:8086 --catalog carbondata --schema <schema_name>
```
Execute the following command to ensure the workers are connected.

```
select * from system.runtime.nodes;
```
Now you can use the Presto CLI on the coordinator to query data sources in the catalog using the Presto workers.



## Presto Single Node Setup for Carbondata

### Config presto server
* Download presto server (333 is suggested and supported) : https://repo1.maven.org/maven2/io/prestosql/presto-server/
* Finish presto configuration following https://github.com/trinodb/trino/blob/333/presto-docs/src/main/sphinx/installation/deployment.rst.
  </br> A configuration example:


**config.properties**

  ```
  coordinator=true
  node-scheduler.include-coordinator=true
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


**jvm.config**

  ```
  -server
  -Xmx4G
  -XX:+UseG1GC
  -XX:G1HeapRegionSize=32M
  -XX:+UseGCOverheadLimit
  -XX:+ExplicitGCInvokesConcurrent
  -XX:+HeapDumpOnOutOfMemoryError
  -XX:OnOutOfMemoryError=kill -9 %p
  -XX:+TraceClassLoading
  -Dcarbon.properties.filepath=<path>/carbon.properties
  
  ```
`carbon.properties.filepath` property is used to set the carbon.properties file path and it is recommended to set otherwise some features may not work. Please check the above example.


**log.properties**
  ```
  io.prestosql=DEBUG
  io.prestosql.server.PluginManager=DEBUG
  ```

**node.properties**
  ```
  node.environment=carbondata
  node.id=ffffffff-ffff-ffff-ffff-ffffffffffff
  node.data-dir=/Users/apple/DEMO/presto_test/data
  ```
* Config carbondata-connector for presto

  Firstly: Compile carbondata, including carbondata-presto integration module
  ```
  $ git clone https://github.com/apache/carbondata
  $ cd carbondata
  $ mvn -DskipTests -P{spark-version} -P{prestodb/prestosql} clean package
  ```
  For prestosql-333 and above, please use spark2.3 profile.
  Because spark2.4 and higher profiles will bring hadoop3 dependencies and it will cause presto333 server launch failure.
  So, compile using:
  ```
  mvn -DskipTests -Pspark-2.3 -Pprestosql clean package
  ```

  Secondly: Create a folder named 'carbondata' under $PRESTO_HOME$/plugin and
  copy all jars from carbondata/integration/presto/target/carbondata-presto-x.x.x-SNAPSHOT
  to $PRESTO_HOME$/plugin/carbondata

  **NOTE:**  Copying assemble jar alone will not work, need to copy all jars from integration/presto/target/carbondata-presto-x.x.x-SNAPSHOT

  Thirdly: Create a carbondata.properties file under $PRESTO_HOME$/etc/catalog/ containing the following contents:
  ```
  connector.name=carbondata
  hive.metastore.uri=thrift://<host>:<port>
  ```
  Carbondata becomes one of the supported format of presto hive plugin, so the configurations and setup is similar to hive connector of presto.
  Please refer <a>https://github.com/trinodb/trino/blob/333/presto-docs/src/main/sphinx/connector/hive.rst</a> for more details.

  **Note**: Since carbon can work only with hive metastore, it is necessary that spark also connects to same metastore db for creating tables and updating tables.
  All the operations done on spark will be reflected in presto immediately.
  It is mandatory to create Carbon tables from spark using CarbonData 1.5.2 or greater version since input/output formats are updated in carbon table properly from this version.
#### Connecting to carbondata store on s3
* In case you want to query carbonstore on S3 using S3A api put following additional properties inside $PRESTO_HOME$/etc/catalog/carbondata.properties
  ```
   Required properties
  
   hive.s3.aws-access-key={value}
   hive.s3.aws-secret-key={value}
   
   Optional properties
   
   hive.s3.endpoint={value}
  ```

  Please refer <a>https://github.com/trinodb/trino/blob/333/presto-docs/src/main/sphinx/connector/hive.rst</a> for more details on S3 integration.

### Generate CarbonData file

There are two ways to create the carbondata files to query from presto.
1. Please refer to quick start: https://github.com/apache/carbondata/blob/master/docs/quick-start-guide.md.
   Load data statement in Spark can be used to create carbondata tables and then you can easily find the created
   carbondata files.
2. Carbondata supports writing the transactional data to table using the insert command. The folder
   structure will be similar to what we have for spark. Table can be created from spark, then data can
   be inserted from presto and queried from presto.

Data can be inserted in two ways.
1. ``` insert into target_carbon_table select values('a,b,c');```
2. ```insert into target_carbon_table select * from source_table;```

Note: Load Carbon properties are not yet supported. The insert will work with all the default
configurations.

### Query carbondata in CLI of presto
* Download presto cli client of version 333 : https://repo1.maven.org/maven2/io/prestosql/presto-cli/

* Start CLI:

  ```
  $ ./presto --server localhost:8086 --catalog carbondata --schema default
  ```
  Replace the hostname, port and schema name with your own.

### Supported features of presto carbon
Presto carbon only supports reading the carbon table which is written by spark carbon or carbon SDK.
Regarding complex datatypes- currently reading of only Array and Struct datatypes are supported,
while Map datatype is not yet supported.
During reading, it supports the non-distributed index like block index and bloom index.
It doesn't support Materialized View as it needs query plan to be changed and presto does not allow it.
Also, Presto carbon supports streaming segment read from streaming table created by spark.

## Presto Setup with CarbonData Distributed IndexServer

### Dependency jars
After copying all the jars from ../integration/presto/target/carbondata-presto-X.Y.Z-SNAPSHOT
to `plugin/carbondata` directory on all nodes, ensure copying the following jars as well.
1. Copy ../integration/spark/target/carbondata-spark_X.Y.Z-SNAPSHOT.jar
2. Copy corresponding Spark dependency jars to the location.

### Configure properties
Configure IndexServer configurations in carbon.properties file. Refer
[Configuring IndexServer](https://github.com/apache/carbondata/blob/master/docs/index-server.md#Configurations) for more info.
Add  `-Dcarbon.properties.filepath=<path>/carbon.properties` in jvm.config file.

### Presto with IndexServer
Start distributed index server. Launch presto CLI and fire SELECT query and check if the corresponding job
is fired in the index server application.  Users can use spark to view the cache loaded by using
show metacache command. Refer: [MetaCacheDDL](./ddl-of-carbondata.md#cache)