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

# Distributed Index Server

## Background

Carbon currently prunes and caches all block/blocklet datamap index information into the driver for
normal table, for Bloom/Index datamaps the JDBC driver will launch a job to prune and cache the
datamaps in executors.

This causes the driver to become a bottleneck in the following ways:
1. If the cache size becomes huge(70-80% of the driver memory) then there can be excessive GC in
the driver which can slow down the query and the driver may even go OutOfMemory.
2. LRU has to evict a lot of elements from the cache to accommodate the new objects which would
in turn slow down the queries.
3. For bloom there is no guarantee that the next query goes to the same executor to reuse the cache
and hence cache could be duplicated in multiple executors.
4. Multiple JDBC drivers need to maintain their own copy of the cache.

Distributed Index Cache Server aims to solve the above mentioned problems.

## Distribution
When enabled, any query on a carbon table will be routed to the index server service in form of
a request. The request will consist of the table name, segments, filter expression and other
information used for pruning.

In IndexServer service a pruning RDD is fired which will take care of the pruning for that
request. This RDD will be creating tasks based on the number of segments that are applicable for 
pruning. It can happen that the user has specified segments to access for that table, so only the
specified segments would be applicable for pruning. Refer: [query-data-with-specified-segments](./segment-management-on-carbondata.md#query-data-with-specified-segments).
IndexServer driver would have 2 important tasks, distributing the segments equally among the
available executors and keeping track of the executor where the segment is cached.

To achieve this 2 separate mappings would be maintained as follows.
1. segment to executor location:
This mapping will be maintained for each table and will enable the index server to track the 
cache location for each segment.

2. Cache size held by each executor: 
    This mapping will be used to distribute the segments equally(on the basis of size) among the 
    executors.
  
Once a request is received each segment would be iterated over and
checked against tableToExecutorMapping to find if a executor is already
assigned. If a mapping already exists then it means that most
probably(if not evicted by LRU) the segment is already cached in that
executor and the task for that segment has to be fired on this executor.

If mapping is not found then first check executorToCacheMapping against
the available executor list to find if any unassigned executor is
present and use that executor for the current segment. If all the
executors are assigned with some segment then find the least loaded
executor on the basis of size.

Initially the segment index size would be used to distribute the
segments fairly among the executor because the actual cache size would
be known to the driver only when the segments are cached and appropriate
information is returned to the driver.

**NOTE:** In case of legacy segment(version: 1.1) the index size is not available
therefore all the legacy segments would be processed in a round robin
fashion.

After the job is completed the tasks would return the cache size held by
each executor which would be updated to the executorToCacheMapping and
the pruned blocklets which would be further used for result fetching.

**Note:** Multiple JDBC drivers can connect to the index server to use the cache.

## Reallocation of executor
In case executor(s) become dead/unavailable then the segments that were
earlier being handled by those would be reassigned to some other
executor using the distribution logic.

**Note:** Cache loading would be done again in the new executor for the
current query.

## MetaCache DDL
The show metacache DDL has a new column called cache location will indicate whether the cache is
from executor or driver. To drop cache the user has to enable/disable the index server using the
dynamic configuration to clear the cache of the desired location.

Refer: [MetaCacheDDL](./ddl-of-carbondata.md#cache)

## Fallback
In case of any failure the index server would fallback to embedded mode
which means that the JDBCServer would take care of distributed pruning.
A similar job would be fired by the JDBCServer which would take care of
pruning using its own executors. If for any reason the embedded mode
also fails to prune the datamaps then the job would be passed on to
driver.

**NOTE:** In case of embedded mode a job would be fired after pruning to clear the
cache as data cached in JDBCServer executors would be of no use.

## Writing splits to a file
If the response is too huge then it is better to write the splits to a file so that the driver can
read this file and create the splits. This can be controlled using the property 'carbon.index.server
.inmemory.serialization.threshold.inKB'. By default, the minimum value for this property is 0,
meaning that no matter how small the splits are they would be written to the file. Maximum is
102400KB which will mean if the size of the splits for a executor cross this value then they would
be written to file.

The user can set the location for these files by using 'carbon.indexserver.temp.path'. By default
the files are written in the path /tmp/indexservertmp.

## Prepriming
As each query is responsible for caching the pruned datamaps, thus a lot of execution time is wasted in reading the 
files and caching the datmaps for the first query.
To avoid this problem we have introduced Pre-Priming which allows each data manipulation command like load, insert etc 
to fire a request to the index server to load the corresponding segments into the index server.
When index server receives a request it checks whether the request is for pre-priming, if it is then the request is 
processed in a new thread, and a dummy response is immediately returned to the client.
Since pre-priming acts as an async call, it does not have any negative performance impacts. 

The user can enable prepriming by using 'carbon.indexserver.enable.prepriming' = 'true/false'. By default this is set as false.

## Configurations

##### carbon.properties(JDBCServer) 

| Name     |      Default Value    |  Description |
|:----------:|:-------------:|:------:       |
| carbon.enable.index.server       |  false | Enable the use of index server for pruning for the whole application.       |
| carbon.index.server.ip |    NA   |   Specify the IP/HOST on which the server is started. Better to specify the private IP. |
| carbon.index.server.port | NA | The port on which the index server is started. |
| carbon.disable.index.server.fallback | false | Whether to enable/disable fallback for index server. Should be used for testing purposes only. Refer: [Fallback](#fallback)|
|carbon.index.server.max.jobname.length|NA|The max length of the job to show in the index server service UI. For bigger queries this may impact performance as the whole string would be sent from JDBCServer to IndexServer.|
|carbon.indexserver.enable.prepriming|false|Enable the use of prepriming in the Index Server to improve the performance of first time query.|



##### carbon.properties(IndexServer) 

| Name     |      Default Value    |  Description |
|:----------:|:-------------:|:------:       |
| carbon.index.server.ip |    NA   |   Specify the IP/HOST on which the server would be started. Better to specify the private IP. | 
| carbon.index.server.port | NA | The port on which the index server has to be started. |
|carbon.index.server.max.worker.threads| 500 | Number of RPC handlers to open for accepting the requests from JDBC driver. Max accepted value is Integer.Max. Refer: [Hive configuration](https://github.com/apache/hive/blob/master/common/src/java/org/apache/hadoop/hive/conf/HiveConf.java#L3441) |
|carbon.max.executor.lru.cache.size|  NA | Maximum memory **(in MB)** upto which the executor process can cache the data (DataMaps and reverse dictionary values). Only integer values greater than 0 are accepted. **NOTE:** Mandatory for the user to set. |
|carbon.index.server.max.jobname.length|NA|The max length of the job to show in the index server application UI. For bigger queries this may impact performance as the whole string would be sent from JDBCServer to IndexServer.|
|carbon.max.executor.threads.for.block.pruning|4| max executor threads used for block pruning. |
|carbon.index.server.inmemory.serialization.threshold.inKB|300|Max in memory serialization size after reaching threshold data will be written to file. Min value that the user can set is 0KB and max is 102400KB. |
|carbon.indexserver.temp.path|tablePath| The folder to write the split files if in memory datamap size for network transfers crossed the 'carbon.index.server.inmemory.serialization.threshold.inKB' limit.|


##### spark-defaults.conf(only for secure mode)

| Name     |      Default Value    |  Description |
|:----------:|:-------------:|:------:       |
| spark.dynamicAllocation.enabled | true | Set to false, so that spark does not kill the executor, If executors are killed, cache would be lost. Applicable only for Index Server. |
| spark.yarn.principal | NA | Should be set to the same user used for JDBCServer. Required only for IndexServer.   |
|spark.yarn.keytab| NA | Should be set to the same as JDBCServer.   |

##### spark-defaults.conf(non-secure mode)
| Name     |      Default Value    |  Description |
|:----------:|:-------------:|:------:       |
| spark.dynamicAllocation.enabled | true | Set to false, so that spark does not kill the executor, If executors are killed, cache would be lost. Applicable only for Index Server. |


**NOTE:** Its better to create a new user for indexserver principal,
that will authenticate the user to access the index server and no other service.

##### core-site.xml

| Name     |      Default Value    |  Description |
|:----------:|:-------------:|:------:       |
| ipc.client.rpc-timeout.ms |  NA | Set the above property to some appropriate value based on your estimated query time. The best option is to set this to the same value as spark.network.timeout. |
| hadoop.security.authorization |  false | Property to enable the hadoop security which is required only on the server side. |
| hadoop.proxyuser.<indexserver_user>.users |  NA | Property to set Proxy User list for which IndexServer permission were to be given ,check https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/Superusers.html|
| hadoop.proxyuser.<indexserver_user>.hosts |  NA | Property to set hosts list for which IndexServer permission were to be given ,check https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/Superusers.html|
| hadoop.proxyuser.<indexserver_user>.groups |  NA | Property to set groups list for which IndexServer permission to be given ,check https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/Superusers.html|
| security.indexserver.protocol.acl |  * | Property to set List of User to be Authorized for Other than proxy Spark Application |


##### dynamic-properties(set command)

| Name     |      Default Value    |  Description |
|:----------:|:-------------:|:------:       |
| carbon.enable.index.server |  false | Enable the use of index server for pruning for the current session. |
| carbon.enable.index.server.dbName.tableName |  false | Enable the use of index server for the specified table in the current session. |
  
  
## Starting the Server
``` 
./bin/spark-submit --master [yarn/local] --[optional parameters] --class org.apache.carbondata.indexserver.IndexServer [path to carbondata-spark-<version>.jar]
```
Or 
``` 
./sbin/start-indexserver.sh --master yarn --num-executors 2 /<absolute path>/carbondata-spark-<version>.jar
```

## FAQ

Q. **Index Server is throwing Large response size exception.** 

A. The exception would show the size of response it is trying to send over the
network. Use ipc.maximum.response.length to a value bigger than the
response size.

Q. **Unable to connect to index server**

A. Check whether the carbon.properties configurations are set in JDBCServer as well as the index
server.

Q. **IndexServer is throwing FileNotFoundException for index files.**

A. Check whether the Index server and JDBCServer are connected to the
same namenode or not. And the store should be shared by both

Q. **OutOfMemoryException in DirectMemoryBuffer**

A. Increase -XX:MaxDirectMemorySize in driver.extraJavaOptions to
accommodate the large response in driver.