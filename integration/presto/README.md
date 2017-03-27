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
* Download presto server 0.166 : https://repo1.maven.org/maven2/com/facebook/presto/presto-server/
* Finish configuration as per https://prestodb.io/docs/current/installation/deployment.html
  for example:
  ```
  carbondata.properties:
  connector.name=carbondata
  carbondata-store=/Users/apple/DEMO/presto_test/data
  
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
* config carbondata-connector for presto
  
  First:compile carbondata-presto integration module
  ```
  $ git clone https://github.com/apache/incubator-carbondata
  $ cd incubator-carbondata/integration/presto
  $ mvn clean package
  ```
  Second:create one folder "carbondata" under ./presto-server-0.166/plugin
  Third:copy all jar from ./incubator-carbondata/integration/presto/target/carbondata-presto-1.1.0-incubating-SNAPSHOT
        to ./presto-server-0.166/plugin/carbondata
  
### Generate CarbonData file

Please refer to quick start : https://github.com/apache/incubator-carbondata/blob/master/docs/quick-start-guide.md

### Query carbondata in CLI of presto
* Download presto-cli-0.166-executable.jar

* Start CLI:
  
  ```
  $ ./presto-cli-0.166-executable.jar --server localhost:8086 --catalog carbondata --schema default
  ```



