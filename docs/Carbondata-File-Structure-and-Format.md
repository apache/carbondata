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

## Use Case & Motivation :  Why introducing a new file format?
The motivation behind CarbonData is to create a single file format for all kind of query and analysis on Big Data. Existing data storage formats in Hadoop address only specific use cases requiring users to use multiple file formats for various types of queries resulting in unnecessary duplication of data. 

### Sequential Access / Big Scan
Such queries select only a few columns with a group by clause but do not contain any filters. This results in full scan over the complete store for the selected columns.  
![Full Scan Query](/docs/images/format/carbon_data_full_scan.png?raw=true)

### OLAP Style Query / Multi-dimensional Analysis
These are queries which are typically fired from Interactive Analysis tools. Such queries often select a few columns but involve filters and group by on a column or a grouping expression.  
![OLAP Scan Query](/docs/images/format/carbon_data_olap_scan.png?raw=true)


### Random Access / Narrow Scan
These are queries used from operational applications and usually select all or most of the columns but do involve a large number of filters which reduce the result to a small size. Such queries generally do not involve any aggregation or group by clause.  
![Random Scan Query](/docs/images/format/carbon_data_random_scan.png?raw=true)

### Single Format to provide low latency response for all usecases
The main motivation behind CarbonData is to provide a single storage format for all the usecases of querying big data on Hadoop. Thus CarbonData is able to cover all use-cases into a single storage format.
![Motivation](/docs/images/format/carbon_data_motivation.png?raw=true)


## CarbonData File Structure
CarbonData file contains groups of data called blocklet, along with all required information like schema, offsets and indices, etc, in a file footer.

The file footer can be read once to build the indices in memory, which can be utilized for optimizing the scans and processing for all subsequent queries.

Each blocklet in the file is further divided into chunks of data called Data Chunks. Each data chunk is organized either in columnar format or row format, and stores the data of either a single column or a set of columns. All blocklets in one file contain the same number and type of Data Chunks.

![Carbon File Structure](/docs/images/format/carbon_data_file_structure_new.png?raw=true)

Each Data Chunk contains multiple groups of data called as Pages. There are three types of pages.
* Data Page: Contains the encoded data of a column/group of columns.
* Row ID Page (optional): Contains the row id mappings used when the Data Page is stored as an inverted index.
* RLE Page (optional): Contains additional metadata used when the Data Page in RLE coded.

![Carbon File Format](/docs/images/format/carbon_data_format_new.png?raw=true)
