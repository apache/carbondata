# Presto Multinode Cluster setup For Carbondata

## Installing Presto

  1. Download the 0.210 version of Presto using:
  `wget https://repo1.maven.org/maven2/com/facebook/presto/presto-server/0.210/presto-server-0.210.tar.gz`

  2. Extract Presto tar file: `tar zxvf presto-server-0.210.tar.gz`.

  3. Download the Presto CLI for the coordinator and name it presto.

  ```
    wget https://repo1.maven.org/maven2/com/facebook/presto/presto-cli/0.210/presto-cli-0.210-executable.jar

    mv presto-cli-0.210-executable.jar presto

    chmod +x presto
  ```

 ## Create Configuration Files

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

## Worker Configurations

##### Contents of your config.properties

  ```
  coordinator=false
  http-server.http.port=8086
  query.max-memory=5GB
  query.max-memory-per-node=2GB
  discovery.uri=<coordinator_ip>:8086
  ```

**Note**: `jvm.config` and `node.properties` files are same for all the nodes (worker + coordinator). All the nodes should have different `node.id`.

## Catalog Configurations

1. Create a folder named `catalog` in etc directory of presto on all the nodes of the cluster including the coordinator.

##### Configuring Carbondata in Presto
1. Create a file named `carbondata.properties` in the `catalog` folder and set the required properties on all the nodes.

## Add Plugins

1. Create a directory named `carbondata` in plugin directory of presto.
2. Copy `carbondata` jars to `plugin/carbondata` directory on all nodes.

## Start Presto Server on all nodes

```
./presto-server-0.210/bin/launcher start
```
To run it as a background process.

```
./presto-server-0.210/bin/launcher run
```
To run it in foreground.

## Start Presto CLI
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
