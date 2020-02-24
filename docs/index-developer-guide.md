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

# Index Developer Guide

### Introduction
DataMap is a data structure that can be used to accelerate certain query of the table. Different DataMap can be implemented by developers. 
Currently, there are two types of DataMap supported:
1. IndexDataMap: DataMap that leverages index to accelerate filter query. Lucene DataMap and BloomFiler DataMap belong to this type of DataMaps.
2. MVDataMap: DataMap that leverages Materialized View to accelerate olap style query, like SPJG query (select, predicate, join, groupby). Preaggregate, timeseries and mv DataMap belong to this type of DataMaps.

### Index Provider
When user issues `CREATE DATAMAP dm ON TABLE main USING 'provider'`, the corresponding DataMapProvider implementation will be created and initialized. 
Currently, the provider string can be:
1. class name IndexDataMapFactory implementation: Developer can implement new type of IndexDataMap by extending IndexDataMapFactory

When user issues `DROP DATAMAP dm ON TABLE main`, the corresponding DataMapProvider interface will be called.

Click for more details about [DataMap Management](./index/index-management.md#index-management) and supported [DSL](./index/index-management.md#overview).

