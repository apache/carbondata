# DataMap Developer Guide

### Introduction
DataMap is a data structure that can be used to accelerate certain query of the table. Different DataMap can be implemented by developers. 
Currently, there are two 2 types of DataMap supported:
1. IndexDataMap: DataMap that leveraging index to accelerate filter query
2. MVDataMap: DataMap that leveraging Materialized View to accelerate olap style query, like SPJG query (select, predicate, join, groupby)

### DataMap provider
When user issues `CREATE DATAMAP dm ON TABLE main USING 'provider'`, the corresponding DataMapProvider implementation will be created and initialized. 
Currently, the provider string can be:
1. preaggregate: one type of MVDataMap that do pre-aggregate of single table
2. timeseries: one type of MVDataMap that do pre-aggregate based on time dimension of the table
3. class name IndexDataMapFactory  implementation: Developer can implement new type of IndexDataMap by extending IndexDataMapFactory

When user issues `DROP DATAMAP dm ON TABLE main`, the corresponding DataMapProvider interface will be called.