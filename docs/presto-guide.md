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


# Presto guide
This tutorial provides a quick introduction to using current integration/presto module.


[Presto Multinode Cluster Setup for Carbondata](#presto-multinode-cluster-setup-for-carbondata)

[Presto Single Node Setup for Carbondata](#presto-single-node-setup-for-carbondata)

## Presto Multinode Cluster Setup for Carbondata
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
  discovery.uri=<coordinator_ip>:8086
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
  discovery.uri=<coordinator_ip>:8086
  ```

**Note**: `jvm.config` and `node.properties` files are same for all the nodes (worker + coordinator). All the nodes should have different `node.id`.

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



## Presto Single Node Setup for Carbondata

### Config presto server
* Download presto server (0.210 is suggested and supported) : https://repo1.maven.org/maven2/com/facebook/presto/presto-server/
* Finish presto configuration following https://prestodb.io/docs/current/installation/deployment.html.
  A configuration example:
  ```
  config.properties:
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
 
  
  jvm.config:
  -server
  -Xmx4G
  -XX:+UseG1GC
  -XX:G1HeapRegionSize=32M
  -XX:+UseGCOverheadLimit
  -XX:+ExplicitGCInvokesConcurrent
  -XX:+HeapDumpOnOutOfMemoryError
  -XX:OnOutOfMemoryError=kill -9 %p
  -XX:+TraceClassLoading
  
  log.properties:
  com.facebook.presto=DEBUG
  com.facebook.presto.server.PluginManager=DEBUG
  
  node.properties:
  node.environment=carbondata
  node.id=ffffffff-ffff-ffff-ffff-ffffffffffff
  node.data-dir=/Users/apple/DEMO/presto_test/data
  ```
* Config carbondata-connector for presto
  
  Firstly: Compile carbondata, including carbondata-presto integration module
  ```
  $ git clone https://github.com/apache/carbondata
  $ cd carbondata
  $ mvn -DskipTests -P{spark-version} -Dspark.version={spark-version-number} -Dhadoop.version={hadoop-version-number} clean package
  ```
  Replace the spark and hadoop version with the version used in your cluster.
  For example, if you are using Spark 2.2.1 and Hadoop 2.7.2, you would like to compile using:
  ```
  mvn -DskipTests -Pspark-2.2 -Dspark.version=2.2.1 -Dhadoop.version=2.7.2 clean package
  ```

  Secondly: Create a folder named 'carbondata' under $PRESTO_HOME$/plugin and
  copy all jars from carbondata/integration/presto/target/carbondata-presto-x.x.x-SNAPSHOT
        to $PRESTO_HOME$/plugin/carbondata

  Thirdly: Create a carbondata.properties file under $PRESTO_HOME$/etc/catalog/ containing the following contents:
  ```
  connector.name=carbondata
  carbondata-store={schema-store-path}
  enable.unsafe.in.query.processing=false
  carbon.unsafe.working.memory.in.mb={value}
  enable.unsafe.columnpage=false
  enable.unsafe.sort=false

  ```
  Replace the schema-store-path with the absolute path of the parent directory of the schema.
  For example, if you have a schema named 'default' stored in hdfs://namenode:9000/test/carbondata/,
  Then set carbondata-store=hdfs://namenode:9000/test/carbondata
  
#### Connecting to carbondata store on s3
 * In case you want to query carbonstore on S3 using S3A api put following additional properties inside $PRESTO_HOME$/etc/catalog/carbondata.properties 
   ```
    Required properties

    fs.s3a.access.key={value}
    fs.s3a.secret.key={value}
    
    Optional properties
    
    fs.s3a.endpoint={value}
   ```
 * In case you want to query carbonstore on s3 using S3 api put following additional properties inside $PRESTO_HOME$/etc/catalog/carbondata.properties 
    ```
      fs.s3.awsAccessKeyId={value}
      fs.s3.awsSecretAccessKey={value}
    ```
  * In case You want to query carbonstore on s3 using S3N api put following additional properties inside $PRESTO_HOME$/etc/catalog/carbondata.properties 
    ```
        fs.s3n.awsAccessKeyId={value}
        fs.s3n.awsSecretAccessKey={value}
     ```
     
    Replace the schema-store-path with the absolute path of the parent directory of the schema.
    For example, if you have a schema named 'default' stored in a bucket s3a://s3-carbon/store,
    Then set carbondata-store=s3a://s3-carbon/store
    
####  Unsafe Properties    
  enable.unsafe.in.query.processing property by default is true in CarbonData system, the carbon.unsafe.working.memory.in.mb 
  property defines the limit for Unsafe Memory usage in Mega Bytes, the default value is 512 MB.
  Currently Presto does not support Unsafe Memory so we have to disable the unsafe feature by setting below properties to false.

  enable.unsafe.in.query.processing=false.
  enable.unsafe.columnpage=false
  enable.unsafe.sort=false

  If you updated the jar balls or configuration files, make sure you have dispatched them
   to all the presto nodes and restarted the presto servers on the nodes. The updates will not take effect before restarting.
  
### Generate CarbonData file

Please refer to quick start: https://github.com/apache/carbondata/blob/master/docs/quick-start-guide.md.
Load data statement in Spark can be used to create carbondata tables. And then you can easily find the created
carbondata files.

### Query carbondata in CLI of presto
* Download presto cli client of version 0.210 : https://repo1.maven.org/maven2/com/facebook/presto/presto-cli

* Start CLI:
  
  ```
  $ ./presto --server localhost:8086 --catalog carbondata --schema default
  ```
  Replace the hostname, port and schema name with your own.

