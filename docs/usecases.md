# Use Cases

CarbonData is useful in various analytical work loads.Some of the most typical usecases where CarbonData is being used in production is documented here.

CarbonData is used for but not limited to

- ### Bank

  -  fraud detection analysis
  - risk profile analysis
  - As a zip table to update the daily balance of customers

- ### Telecom

  - Detection of signal anamolies for VIP customers for providing improved customer experience
  - Analysis of MR,CHR records of GSM data to determine the tower load at a particular time period and rebalance the tower configuration
  - Analysis of access sites, video, screen size, streaming bandwidth, quality to determine the network quality,routing configuration

- ### Web/Internet

  - Analysis of page or video being accessed,server loads, streaming quality, screen size

- ### Smart City

  - Vehicle tracking analysis
  - Unusual behaviour analysis



These use cases can be broadly classified into below categories:

- Full scan/Detailed/Interactive queries
- Aggregation queries
- Multi-dimensional queries
- Real time Ingestion and queries



## Detailed Queries in the Telecom scenario

### Scenario

User wants to analyse all the CHR(Call History Record) and MR(Measurement Records) of the mobile subscribers in order to identify the service failures within 10 secs.Also user wants to run machine learning models on the data to fairly estimate the reasons and time of probable failures and take action ahead to meet the SLA(Service Level Agreements) of VIP customers. 

### Challenges

- Data incoming rate might vary based on the user concentration at a particular period of time.Hence higher data load speeds are required
- Cluster needs to be well utilised and share the cluster among various applications for better resource consumption and savings
- Queries needs to be interactive.ie., the queries fetch small data and need to be returned in seconds

### Solution

Setup a Hadoop + Spark + CarbonData cluster managed by YARN.

Proposed the following configurations for CarbonData.(These tunings were proposed before CarbonData introduced SORT_COLUMNS parameter using which the sort order and schema order could be different.)

Add the frequently used columns to the left of the table definition.Add it in the increasing order of cardinality.It was suggested to keep msisdn,imsi columns in the beginning of the schema.With latest CarbonData, SORT_COLUMNS needs to be configured msisdn,imsi in the beginning.

Add timestamp column to the right of the schema as it is naturally increasing.

Create two separate YARN queues for Query and Data Loading.

Apart from these, the following CarbonData configuration was suggested to be configured in the cluster.



| Parameter                               | Value  |
| --------------------------------------- | ------ |
| carbon.graph.rowset.size                | 100000 |
| carbon.compaction.level.threshold       | 6,6    |
| carbon.enable.auto.load.merge           | true   |
| carbon.number.of.cores.while.compacting | 4      |
| carbon.number.of.cores.while.loading    | 12     |
| carbon.sort.size                        | 100000 |
| carbon.major.compaction.size            | 921600 |
| table_blocksize                         | 256MB  |
| spark.scheduler.mode                    | FIFO   |
| spark.shuffle.manager                   | SORT   |
| spark.sql.shuffle.partitions            | 500    |
| spark.dynamicAllocation.enabled         | FALSE  |
| dfs.datanode.drop.cache.behind.reads    | FALSE  |
| dfs.datanode.drop.cache.behind.reads    | FALSE  |
| dfs.datanode.sync.behind.writes         | FALSE  |
| carbon.sort.intermediate.files.limit    | 100    |
| carbon.use.local.dir                    | TRUE   |
| carbon.use.multiple.temp.dir            | TRUE   |



### Results Achieved

| Parameter                                 | Results          |
| ----------------------------------------- | ---------------- |
| Query                                     | < 3 Sec          |
| Data Loading Speed                        | 40 MB/s Per Node |
| Concurrent query performance (20 queries) | < 10 Sec         |



## Interactive Queries in the Smart City scenario

### Scenario

User wants to analyse the person/vehicle movement and behavior during a certain time period.This output data needs to be joined with a external table for Human details extraction.The query will be run with different time period as filter to identify potential behavior mismatch.

### Challenges

Data generated per day is very huge.Data needs to be loaded multiple times per day to accomodate the incoming data size.

Queries needs to be fetched within 3 Seconds.

### Solution

Setup a Hadoop + Spark + CarbonData cluster managed by YARN.

Since data needs to be queried for a time period, it was recommended to keep the time column at the beginning of schema.

Use table block size as 512MB.

Use local sort mode.

Apart from these, the following CarbonData configuration was suggested to be configured in the cluster.



| Parameter                               | Value                   |
| --------------------------------------- | ----------------------- |
| carbon.graph.rowset.size                | 100000                  |
| enable.unsafe.sort                      | TRUE                    |
| enable.offheap.sort                     | TRUE                    |
| offheap.sort.chunk.size.in.mb           | 128                     |
| carbon.number.of.cores.while.loading    | 12                      |
| carbon.sort.size                        | 100000                  |
| carbon.major.compaction.size            | 921600                  |
| table_blocksize                         | 512                     |
| carbon.number.of.chunk.in.blocklet      | 5                       |
| carbon.enable.vector.reader             | true                    |
| enable.unsafe.in.query.procressing      | true                    |
| use.offheap.in.query.processing         | true                    |
| carbon.sort.intermediate.files.limit    | 100                     |
| carbon.use.local.dir                    | TRUE                    |
| carbon.use.multiple.temp.dir            | TRUE                    |
| sort.inmemory.size.in.mb                | 92160(load)15360(query) |
| carbon.sort.file.buffer.size            | 20                      |
| carbon.number.of.cores.while.compacting | 12                      |
| carbon.enable.auto.load.merge           | FALSE                   |
| enable.unsafe.columnpage                | TRUE                    |
| carbon.unsafe.working.memory.in.mb      | 10240                   |



### Results Achieved

| Parameter                              | Results          |
| -------------------------------------- | ---------------- |
| Query (Time Period spanning 1 segment) | < 3 Sec          |
| Data Loading Speed                     | 45 MB/s Per Node |

