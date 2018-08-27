## What is CarbonData

CarbonData is a fully indexed columnar and Hadoop native data-store for processing heavy analytical workloads and detailed queries on big data with Spark SQL. CarbonData allows faster interactive queries over PetaBytes of data.



## What does this mean

CarbonData has specially engineered optimizations like multi level indexing, compression and encoding techniques targeted to improve performance of analytical queries which can include filters, aggregation and distinct counts where users expect sub second response time for queries on TB level data on commodity hardware clusters with just a few nodes.

CarbonData has 

- **Unique data organisation** for faster retrievals and minimise amount of data retrieved

- **Advanced push down optimisations** for deep integration with Spark so as to improvise the Spark DataSource API and other experimental features thereby ensure computing is performed close to the data to minimise amount of data read, processed, converted and transmitted(shuffled) 

- **Multi level indexing** to efficiently prune the files and data to be scanned and hence reduce I/O scans and CPU processing



## Architecture

![](/Users/aditi_advith/Documents/code/carbondata/docs/images/carbondata_architecture.png)



#### Spark Interface Layer: 

CarbonData has deep integration with Apache Spark.CarbonData integrates custom Parser,Strategies,Optimization rules into Spark to take advantage of computing performed closer to data.

![](/Users/aditi_advith/Documents/code/carbondata/docs/images/carbondata_spark_integration.png)

1. **Carbon parser** Enhances Spark’s SQL parser to support Carbon specific DDL and DML commands to create carbon table, create aggregate tables, manage data loading, data retention and cleanup.
2. **Carbon Strategies**:- Modify Spark SQL’s physical query execution plan to push down possible operations to Carbon for example:- Grouping, Distinct Count, Top N etc.. for improving query performance.
3. **Carbon Data RDD**:- Makes the data present in Carbon tables visible to Spark as a RDD which enables spark to perform distributed computation on Carbon tables.



#### Carbon Processor: 

Receives a query execution fragment from spark and executes the same on the Carbon storage. This involves Scanning the carbon store files for matching record, using the indices to directly locate the row sets and even the rows that may containing the data being searched for. The Carbon processor also performs all pushed down operations such as 

Aggregation/Group By

Distinct Count

Top N

Expression Evaluation

And many more…

#### Carbon Storage:

Custom columnar data store which is heavily compressed, binary, dictionary encoded and heavily indexed.Usaually stored in HDFS.

## CarbonData Features & Functions

CarbonData has rich set of featues to support various use cases in Big Data analytics.The below table lists the major features supported by CarbonData.

 

<table style="width: 777px;">
<tbody>
<tr style="height: 63px;">
<td style="width: 143px; height: 215px;" rowspan="7">&nbsp;Table Management&nbsp;&nbsp;&nbsp;&nbsp;</td>
<td style="width: 148.125px; height: 63px;">&nbsp;DDL (Create, Alter,Drop,CTAS)</td>
<td style="width: 481.875px; height: 63px;">CarbonData provides its own DDL to create and manage carbon tables.These DDL conform to Hive,Spark SQL format and support additional properties and configuration to take advantages of CarbonData functionalities&nbsp;</td>
</tr>
<tr style="height: 43px;">
<td style="width: 148.125px; height: 43px;">&nbsp;DML(Load,Insert)</td>
<td style="width: 481.875px; height: 43px;">&nbsp;CarbonData provides its own DML to manage data in carbondata tables.It adds many customizations through configurations to completely customize the behavior as per user needs</td>
</tr>
<tr style="height: 43px;">
<td style="width: 148.125px; height: 43px;">Update and Delete&nbsp;</td>
<td style="width: 481.875px; height: 43px;">CarbonData supports Update and Delete on Big Data.CarbonData provides the syntax similar to Hive to support IUD operations on CarbonData tables</td>
</tr>
<tr style="height: 43px;">
<td style="width: 148.125px; height: 43px;">Segment Management&nbsp;</td>
<td style="width: 481.875px; height: 43px;">&nbsp;CarbonData has unique concept of segments to manage incremental loads to CarbonData tables effectively.Segment management helps to easily control the table, perform easy retention, and is also used to provide transaction capability for operations being performed.</td>
</tr>
<tr style="height: 23px;">
<td style="width: 148.125px; height: 23px;">Partition&nbsp;</td>
<td style="width: 481.875px; height: 23px;">&nbsp;CarbonData supports 2 kinds of partitions.1.partition similar to hive partition.2.CarbonData partition supporting hash,list,range partitioning</td>
</tr>
<tr style="height: 23px;">
<td style="width: 148.125px; height: 23px;">Compaction</td>
<td style="width: 481.875px; height: 23px;">&nbsp;CarbonData manages incremental loads as segments.Compaction help to compact the growing number of segments and also to improve query filter pruning.</td>
</tr>
<tr style="height: 23px;">
<td style="width: 148.125px; height: 23px;">&nbsp;External Tables</td>
<td style="width: 481.875px; height: 23px;">&nbsp;CarbonData can read any carbondata file and automatically infer schema from the file and provide a relational table view to perform sql queries using Spark or any other applicaion.</td>
</tr>
<tr style="height: 23px;">
<td style="width: 143px; height: 92px;" rowspan="5">&nbsp;DataMaps&nbsp;&nbsp;&nbsp;</td>
<td style="width: 148.125px; height: 23px;">Pre-Aggregate&nbsp;</td>
<td style="width: 481.875px; height: 23px;">&nbsp;CarbonData has concept of datamaps to assist in pruning of data while querying so that performance is faster.Pre Aggregate tables are kind of datamaps which can improve the query performance by order of magnitude.CarbonData will automatically pre-aggregae the incremental data and re-write the query to automatically fetch from the most appropriate pre-aggregate table to serve the query faster.</td>
</tr>
<tr style="height: 23px;">
<td style="width: 148.125px; height: 23px;">Time Series&nbsp;</td>
<td style="width: 481.875px; height: 23px;">&nbsp;CarbonData has built in understanding of time order(Year, month,day,hour, minute,second).Time series is a pre-aggregate table which can automatically roll-up the data to the desired level during incremental load and serve the query from the most appropriate pre-aggregate table.</td>
</tr>
<tr style="height: 23px;">
<td style="width: 148.125px; height: 23px;">Bloom filter&nbsp;</td>
<td style="width: 481.875px; height: 23px;">&nbsp;CarbonData supports bloom filter as a datamap in order to quickly and efficiently prune the data for scanning and acheive faster query performance.</td>
</tr>
<tr style="height: 23px;">
<td style="width: 148.125px; height: 23px;">Lucene&nbsp;</td>
<td style="width: 481.875px; height: 23px;">&nbsp;Lucene is popular for indexing text data which are long.CarbonData provides a lucene datamap so that text columns can be indexed using lucene and use the index result for efficient pruning of data to be retrieved during query</td>
</tr>
<tr style="height: 23px;">
<td style="width: 148.125px; height: 23px;">MV (Materialized Views)</td>
<td style="width: 481.875px; height: 23px;">&nbsp;MVs are kind of pre-aggregate tables which can support efficent query re-write and processing.CarbonData provides MV which can rewrite query to fetch from any table(including non-carbondata tables).Typical usecase is to store the aggregated data of a non-carbondata fact table into carbondata and use mv to rewrite the query to fetch from carbondata</td>
</tr>
<tr style="height: 23px;">
<td style="width: 143px; height: 23px;">&nbsp;Streaming</td>
<td style="width: 148.125px; height: 23px;">&nbsp;Spark Streaming</td>
<td style="width: 481.875px; height: 23px;">&nbsp;CarbonData supports streaming of data into carbondata in near-realtime and make it immediately available for query</td>
</tr>
<tr style="height: 23px;">
<td style="width: 143px; height: 23px;" rowspan="3">SDK</td>
<td style="width: 148.125px; height: 23px;">Streaming DSL</td>
<td style="width: 481.875px; height: 23px;">&nbsp;CarbonData provides a DSL to easily write an streaming application and feed the data into carbondata tables</td>
</tr>
<tr style="height: 23px;">
<td style="width: 148.125px; height: 23px;">CarbonData writer</td>
<td style="width: 481.875px; height: 23px;">&nbsp;CarbonData supports writing data from non-spark application using SDK.Users can use SDK to generate carbondata files from custom applications.Typical usecase is to write the streaming application plugged in to kafka and use carbondata as sink(target) table for storing</td>
</tr>
<tr style="height: 23px;">
<td style="width: 148.125px; height: 23px;">CarbonData Reader</td>
<td style="width: 481.875px; height: 23px;">&nbsp;CarbonData supports reading of&nbsp;data from non-spark application using SDK.Users can use the SDK to read the carbondata files from their application and do custom processing</td>
</tr>
<tr style="height: 23px;">
<td style="width: 143px; height: 23px;" rowspan="2">&nbsp;Storage</td>
<td style="width: 148.125px; height: 23px;">S3</td>
<td style="width: 481.875px; height: 23px;">&nbsp;CarbonData can write to S3, OBS or any cloud storage confirming to S3 protocol.CarbonData uses the HDFS api to write to cloud object stores</td>
</tr>
<tr style="height: 23px;">
<td style="width: 148.125px; height: 23px;">HDFS</td>
<td style="width: 481.875px; height: 23px;">CarbonData uses HDFS api to write and read data from HDFS.CarbonData can take advantage of the locality information to efficiently suggest spark to run tasks near to the data.</td>
</tr>
</tbody>
</table>

## Design

- ### Dictionary Encoding

CarbonData supports encoding of data with suggogate values to reduce storage space and speed up processing.Most databases and big data SQL data stores adopt dictionary encoding(integer surrogate numbers) to achieve data compression.Unlike other column store databases where the dictionary is local to each data block, CarbonData maintains a global dictionary which provides opportunity for lazy conversion to actual values enabling all computation to be performed on the lightweight surrogate values.

##### Dictionary generation

![](/Users/aditi_advith/Documents/code/carbondata/docs/images/carbondata_dict_encoding.png)



##### MDK Indexing

All the surrogate keys are byte packed to generate an MDK (Multi Dimensional Key) Index.

Any non surrogate columns of String data types are compressed using one of the configured compression algorithms and stored.For those numeric columns where surrogates are not generated, such data is stored as it is after compression.

![image-20180903212418381](/Users/aditi_advith/Documents/code/carbondata/docs/images/carbondata_mdk.png)

##### Sorted MDK

The data is sorted based on the MDK Index.Sorting helps for logical grouping of similar data and there by aids in faster look up during query.

#### ![image-20180903212525214](/Users/aditi_advith/Documents/code/carbondata/docs/images/carbondata_mdk_sort.png)

##### Custom Columnar Encoding

The Sorted MDK Index is split into each column.Unlike other stores where the column is compressed and stored as it is, CarbonData sorts this column data so that Binary Search can be performed on individual column data based on the filter conditions.This aids in magnitude increase in query performance and also in better compression.Since the individual column's data gets sorted, it is necessary to maintain the row mapping with the sorted MDK Index data in order to retrieve data from other columns which are not participating in filter.This row mapping is termed as **Inverted Index** and is stored along with the column data.The below picture depicts the logical column view.User has the option to **turn off** Inverted Index for such columns where filters are never applied or is very rare.In such cases, scanning would be sequential, but can aid in reducing the storage size(occupied due to inverted index data).

#### ![](/Users/aditi_advith/Documents/code/carbondata/docs/images/carbondata_blocklet_view.png)

- ### CarbonData Storage Format

  CarbonData has a unique storage structure which aids in efficient storage and retrieval of data.Please refer to [File Structure of CarbonData](#./file-structure-of-carbondata.md) for detailed information on the format.

- ### Indexing

  CarbonData maintains multiple indexes at multiple levels to assist in efficient pruning of unwanted data from scan during query.Also CarbonData has support for plugging in external indexing solutions to speed up the query process.

  ##### Min-Max Indexing

  Storing data along with index significantly accelerates query performance and reduces the I/O scans and CPU resources in case of filters in the query. CarbonData index consists of multiple levels of indices, a processing framework can leverage this index to reduce the number of tasks it needs to schedule and process. It can also do skip scan in more fine grained units (called blocklet) in task side scanning instead of scanning the whole file.  **CarbonData maintains Min-Max Index for all the columns.**

  CarbonData maintains a separate index file which contains the footer information for efficient IO reads.

  Using the Min-Max info in these index files, two levels of filtering can be achieved.

  Min-Max at the carbondata file level,to efficiently prune the files when the filter condition doesn't fall in the range.This information when maintained at the Spark Driver, will help to efficiently schedule the tasks for scanning

  Min-Max at the blocklet level, to efficiently prune the blocklets when the filter condition doesn't fall in the range.This information when maintained at the executor can significantly reduce the amount unnecessary data processed by the executor tasks. 



  ![](/Users/aditi_advith/Documents/code/carbondata/docs/images/carbondata-minmax-blocklet.png)

- #### DataMaps

  DataMap is a framework for indexing and also for statistics that can be used to add primary index (Blocklet Index) , secondary index type and statistical type to CarbonData.

  DataMap is a standardized general interface which CarbonData uses to prune data blocks for scanning.

  DataMaps are of 2 types:

  **CG(Coarse Grained) DataMaps** Can prune data to the blocklet or to Page level.ie., Holds information for deciding which blocks/blocklets to be scanned.This DataMap is used in Spark Driver to decide the number of tasks to be scheduled.

  **FG(Fine Grained) DataMaps** Can prune data to row level.This DataMap is used in Spark executor for scanning an fetching the data much faster.

  Since DataMap interfaces are generalised, We can write a thin adaptor called as **DataMap Providers** to interface between CarbonData and other external Indexing engines. For eg., Lucene, Solr,ES,...

  CarbonData has its own DSL to create and manage DataMaps.Please refer to [CarbonData DSL](#./datamap/datamap-management.md#overview) for more information.

  The below diagram explains about the DataMap execution in CarbonData.

  ![](/Users/aditi_advith/Documents/code/carbondata/docs/images/carbondata-datamap.png)

- #### Update & Delete


CarbonData supports Update and delete operations over big data.This functionality is not targetted for OLTP scenarios where high concurrent update/delete are required.Following are the assumptions considered when this feature is designed.

1. Updates or Deletes are periodic and in Bulk
2. Updates or Deletes are atomic
3. Data is immediately visible
4. Concurrent query to be allowed during an update or delete operation
5. Single statement auto-commit support (not OLTP-style transaction)

Since data stored in HDFS are immutable,data blocks cannot be updated in-place.Re-write of entire data block is not efficient for IO and also is a slow process.

To over come these limitations, CarbonData adopts methodology of writing a delta file containing the rows to be deleted and another delta file containing the values to be updated with.During processing, These two delta files are merged with the main carbondata file and the correct result is returned for the query.

The below diagram describes the process.

![](/Users/aditi_advith/Documents/code/carbondata/docs/images/carbondata_update_delete.png)



## Integration with Big Data ecosystem

Refer to Integration with [Spark](#./quick-start-guide.md#spark), [Presto](#./quick-start-guide.md#presto) for detailed information on integrating CarbonData with these execution engines.

## Scenarios where CarbonData is suitable

CarbonData is useful in various analytical work loads.Some of the most typical usecases where CarbonData is being used in production is [documented here](./usecases.md).


<script>
// Show selected style on nav item
$(function() { $('.b-nav__intro').addClass('selected'); });
</script>