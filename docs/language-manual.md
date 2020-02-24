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

# Overview



CarbonData has its own parser, in addition to Spark's SQL Parser, to parse and process certain Commands related to CarbonData table handling. You can interact with the SQL interface using the [command-line](https://spark.apache.org/docs/latest/sql-programming-guide.html#running-the-spark-sql-cli) or over [JDBC/ODBC](https://spark.apache.org/docs/latest/sql-programming-guide.html#running-the-thrift-jdbcodbc-server).

- [Data Types](./supported-data-types-in-carbondata.md)
- Data Definition Statements
  - [DDL:](./ddl-of-carbondata.md)[Create](./ddl-of-carbondata.md#create-table),[Drop](./ddl-of-carbondata.md#drop-table),[Partition](./ddl-of-carbondata.md#partition),[Bucketing](./ddl-of-carbondata.md#bucketing),[Alter](./ddl-of-carbondata.md#alter-table),[CTAS](./ddl-of-carbondata.md#create-table-as-select),[External Table](./ddl-of-carbondata.md#create-external-table)
  - [Index](./index/index-management.md)
    - [Bloom](./index/bloomfilter-index-guide.md)
    - [Lucene](./index/lucene-index-guide.md)
  - Materialized Views (MV)
  - [Streaming](./streaming-guide.md)
- Data Manipulation Statements
  - [DML:](./dml-of-carbondata.md) [Load](./dml-of-carbondata.md#load-data), [Insert](./dml-of-carbondata.md#insert-data-into-carbondata-table), [Update](./dml-of-carbondata.md#update), [Delete](./dml-of-carbondata.md#delete)
  - [Segment Management](./segment-management-on-carbondata.md)
- [CarbonData as Spark's Datasource](./carbon-as-spark-datasource-guide.md)
- [Configuration Properties](./configuration-parameters.md)



