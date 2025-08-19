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

# PRESTO INTEGRATION IN CARBONDATA

1. [Document Purpose](#document-purpose)
    1. [Purpose](#purpose)
    1. [Scope](#scope)
    1. [Definitions and Acronyms](#definitions-and-acronyms)
1. [Requirements addressed](#requirements-addressed)
1. [Design Considerations](#design-considerations)
    1. [Row Iterator Implementation](#row-iterator-implementation)
    1. [ColumnarReaders or StreamReaders approach](#columnarreaders-or-streamreaders-approach)
1. [Module Structure](#module-structure)
1. [Detailed design](#detailed-design)
    1. [Modules](#modules)
    1. [Functions Developed](#functions-developed)
1. [Integration Tests](#integration-tests)
1. [Tools and languages used](#tools-and-languages-used)
1. [References](#references)

## Document Purpose

 * #### _Purpose_
 The purpose of this document is to outline the technical design of the Presto Integration in CarbonData.

 Its main purpose is to -
   *  Provide the link between the Functional Requirement and the detailed Technical Design documents.
   *  Detail the functionality which will be provided by each component or group of components and show how the various components interact in the design.

 This document is not intended to address installation and configuration details of the actual implementation. Installation and configuration details are provided in [presto integration Technical note](presto-integration-technical-note.md).As is true with any high level design, this document will be updated and refined based on changing requirements.
 * #### _Scope_
 CarbonData Integration with Presto will allow execution of queries on CarbonData through Presto CLI.  CarbonData can be added easily as a Data Source among the multiple heterogeneous data sources for Presto.
 * #### _Definitions and Acronyms_
  **CarbonData :** CarbonData is a fully indexed columnar and Hadoop native data-store for processing heavy analytical workloads and detailed queries on big data. In customer benchmarks, CarbonData has proven to manage Petabyte of data running on extraordinarily low-cost hardware and answers queries around 10 times faster than the current open source solutions (column-oriented SQL on Hadoop data-stores).

 **Presto :** Presto is a distributed SQL query engine designed to query large data sets distributed over one or more heterogeneous data sources.

## Requirements addressed
This integration of Presto mainly serves two purpose:
 * Support of Apache CarbonData as Data Source in Presto.
 * Execution of Apache CarbonData Queries on Presto.

## Design Considerations

Presto was designed as an alternative to tools that query HDFS using pipelines of MapReduce jobs such as Hive or Pig, but Presto is not limited to accessing HDFS. Presto can be and has been extended to operate over different kinds of data sources including traditional relational databases and other data sources such as Cassandra.

![presto-integration-design](../presto/images/presto-integration-design.png?raw=true)

Following are the design considerations for the Presto Integration with the Carbondata.

#### Row Iterator Implementation

   Presto provides a way to iterate the records through a RecordSetProvider which creates a RecordCursor so we have to extend this class to create a CarbondataRecordSetProvider and CarbondataRecordCursor to read data from Carbondata core module. The CarbondataRecordCursor will utilize the DictionaryBasedResultCollector class of Core module to read data row by row. This approach has two drawbacks.
   * The Presto converts this row data into columnar data again since carbondata itself store data in columnar format we are adding an additional conversion to row to column instead of directly using the column.
   * The cursor reads the data row by row instead of a batch of data , so this is a costly operation as we are already storing the data in pages or batches we can directly read the batches of data.

#### ColumnarReaders or StreamReaders approach

   In this design we can create StreamReaders that can read data from the Carbondata Column based on DataType and directly convert it into Presto Block. This approach saves us the row by row processing as well as reduce the transition and conversion of data . By this approach we can achieve the fastest read from Presto and create a Presto Page by extending PageSourceProvider and PageSource class. This design will be discussed in detail in the next sections of this document.

## Module Structure


![module structure](../presto/images/module-structure.jpg?raw=true)



## Detailed design

#### Modules

Based on the above functionality, Presto Integration is implemented as following module:

1. **Presto**

Integration of CarbonData with Presto includes implementation of connector Api of the Presto.
#### Functions developed

![functionas developed](../presto/images/functions-developed-diagram.png?raw=true)

### List of developed functions:
```
1.  CarbonDataPlugin : It implements the Plugin Interface of the Presto.
2.  CarbonDataConnectorFactory : It implements the ConnectorFactory Interface of the Presto. The connector factory is a simple interface responsible for creating an instance of a Connector object that returns instances of the following services:
      1. ConnectorMetadata

      2.  ConnectorSplitManager

      3. ConnectorHandleResolver

3.  CarbonDataConnector : It implements the Connector Interface of the Presto.
4.  CarbonDataMetadata  : It implements the ConnectorMetadata Interface of the Presto.  The connector metadata interface has a large number of important methods that are responsible for allowing Presto to look at lists of schemas, lists of tables, lists of columns, and other metadata about a particular data source.
5.  CarbonDataSplitManager  : It implements the ConnectorSplitManager Interface of the Presto. The split manager partitions the data for a table into the individual chunks or splits that Presto will distribute to workers for processing.
6.  CarbonDataHandleResolver : It implements the ConnectorHandleResolver Interface of the Presto. It is mainly to get some of the connector in the handler type information.
7.  CarbonDataTableLayoutHandle :  It implements the ConnectorTableLayoutHandle Interface of the Presto.
8.  CarbonDataTableHandle : It implements the ConnectorTableHandle Interface of the Presto.
9.  CarbonDataColumnHandle : It implements the ConnectorTableHandle Interface of the Presto.
10. CarbonDataSplit : It implements the ConnectorSplit Interface of the Presto.
11. CarbonDataTransactionHandle : It implements the ConnectorTransactionHandle Interface of the Presto.
12. CarbondataPageSourceProvider: It implements the ConnectorPageSourceProvider and creates a new CarbondataPageSource object.
13. CarbondataPageSource : It implements the ConnectorPagesource and creates a new Page by reading from PrestoVectorizedRecordReader.
14. PrestoVectorizedRecordReader : It reads CarbonColumnar batches to fetch the data.
15. CarbonVectorBatch : This Class Creates A VectorizedRowBatch which is a set of rows, organized with each column as a CarbonVector. It is the unit of query execution, organized to minimize the cost per row and achieve high cycles-per-instruction. The major fields are public by design to allow fast and convenient access by the vectorized query execution code.
16. CarbonColumnVectorImpl: This Class Implements the Interface CarbonColumnVector and provides the methods to store the data in a Vector and to retrieved the data from it.
17. CarbonTableReader :  This class is located in Impl package and is responsible for reading the carbon tables and providing schema and tables lists.
18. CarbonDictionaryDecodeReadSupport : This class decodes the dictionary values and creates either the slice array or dictionary array on the basis of datatype.
19. Stream Readers :  This package includes all the stream readers responsible for creating blocks of data of the particular data type.
```
## Integration Tests
We have created a **PrestoAllDataTypeTest** file to cover all the scenarios for integration tests.

## Tools and languages used
Presto Integration will use the following open source tools:


| Sr. No. |    Name    |
|:-------:|:----------:|
|    1    | CarbonData |
|    2    |    Java    |
|    3    |   Presto   |
|    4    |    Scala   |

## References

https://prestodb.io/docs/current/
