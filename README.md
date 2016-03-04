CarbonData is a fully indexed columnar and hadoop native data-store for processing heavy analytical workloads and detailed queries on big data. In customer benchmarks, CarbonData has been shown to manage Petabyte of data running on extraordinarily low-cost hardware and answers queries around 10 times faster than the current open source solutions (column-oriented SQL on Hadoop data-stores). 

### Why CarbonData
For big data interactive analysis scenarios, many of our customers expect sub-second response to query TB-PB level data on general hardware clusters with just a few nodes. 

In the current big data ecosystem, there are few columnar storage formats such as ORC and Parquet that are designed for SQL on Big Data.
Apache Hive’s ORC format is a columnar storage format with basic indexing capability. However, ORC cannot meet the sub-second query response expectation on TB level data, because ORC format performs only stride level dictionary encoding and all analytical operations such as filtering and aggregation is done on the actual data. 
Apache Parquet is columnar storage can improve performance in comparison to ORC, because of more efficient storage organization. Though Parquet can provide query response on TB level data in a few seconds, it is still far from the sub-second expectation of interactive analysis users.
Cloudera Kudu can effectively solve some query performance issues, but kudu is not hadoop native, can’t seamlessly integrate historic HDFS data into new kudu system.

However, CarbonData uses specially engineered optimizations targeted to improve performance of analytical queries which can include filters, aggregation and distinct counts, the required data to be stored in an indexed, well organized, read-optimized format, CarbonData’s query performance can achieve sub-second response.

