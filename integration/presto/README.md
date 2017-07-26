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

Please follow the below steps to query carbondata in presto

### Config presto server
* Download presto server (0.166 is suggested and supported) : https://repo1.maven.org/maven2/com/facebook/presto/presto-server/
* Finish presto configuration following https://prestodb.io/docs/current/installation/deployment.html.
  A configuration example:
  ```
  config.properties:
  coordinator=true
  node-scheduler.include-coordinator=true
  http-server.http.port=8086
  query.max-memory=5GB
  query.max-memory-per-node=1GB
  discovery-server.enabled=true
  discovery.uri=http://localhost:8086
  
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
  For example, if you are using Spark 2.1.0 and Hadoop 2.7.2, you would like to compile using:
  ```
  mvn -DskipTests -Pspark-2.1 -Dspark.version=2.1.0 -Dhadoop.version=2.7.2 clean package
  ```

  Secondly: Create a folder named 'carbondata' under $PRESTO_HOME$/plugin and
  copy all jars from carbondata/integration/presto/target/carbondata-presto-x.x.x-SNAPSHOT
        to $PRESTO_HOME$/plugin/carbondata

  Thirdly: Create a carbondata.properties file under $PRESTO_HOME$/etc/catalog/ containing the following contents:
  ```
  connector.name=carbondata
  carbondata-store={schema-store-path}
  ```
  Replace the schema-store-path with the absolute path of the parent directory of the schema.
  For example, if you have a schema named 'default' stored in hdfs://namenode:9000/test/carbondata/,
  Then set carbondata-store=hdfs://namenode:9000/test/carbondata

  If you updated the jar balls or configuration files, make sure you have dispatched them
   to all the presto nodes and restarted the presto servers on the nodes. The updates will not take effect before restarting.
  
### Generate CarbonData file

Please refer to quick start: https://github.com/apache/carbondata/blob/master/docs/quick-start-guide.md.
Load data statement in Spark can be used to create carbondata tables. And then you can easily find the created
carbondata files.

### Query carbondata in CLI of presto
* Download presto cli client following: https://prestodb.io/docs/current/installation/cli.html

* Start CLI:
  
  ```
  $ ./presto --server localhost:8086 --catalog carbondata --schema default
  ```
  Replace the hostname, port and schema name with your own.

### Running Presto Example

Please follow the steps given below in the sequential order to run the presto example with CarbonData

* Execute the following class : CarbonDataFactory

  (located at examples/spark2/src/main/scala/org/apache/carbondata/examples/CarbonDataFactory.scala )

* Execute the example by running : PrestoClientRunner

  (located at integration/presto/src/main/scala/org/apache/carbondata/presto/PrestoClientRunner.scala )





