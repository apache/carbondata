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

# CarbonData File Structure

CarbonData files contain groups of data called blocklets, along with all required information like schema, offsets and indices etc, in a file header and footer, co-located in HDFS.

The file footer can be read once to build the indices in memory, which can be utilized for optimizing the scans and processing for all subsequent queries.

### Understanding CarbonData File Structure
* Block : It would be as same as HDFS block, CarbonData creates one file for each data block, user can specify TABLE_BLOCKSIZE during creation table. Each file contains File Header, Blocklets and File Footer. 

![CarbonData File Structure](../docs/images/carbon_data_file_structure_new.png?raw=true)

* File Header : It contains CarbonData file version number, list of column schema and schema updation timestamp.
* File Footer : it contains Number of rows, segmentinfo ,all blocklets’ info and index, you can find the detail from the below diagram.
* Blocklet : Rows are grouped to form a blocklet, the size of the blocklet is configurable and default size is 64MB, Blocklet contains Column Page groups for each column.
* Column Page Group : Data of one column and it is further divided to pages, it is guaranteed to be contiguous in file.
* Page : It has the data of one column and the number of row is fixed to 32000 size. 

![CarbonData File Format](../docs/images/carbon_data_format_new.png?raw=true)

### Each page contains three types of data
* Data Page: Contains the encoded data of a column of columns.
* Row ID Page (optional): Contains the row ID mappings used when the data page is stored as an inverted index.
* RLE Page (optional): Contains additional metadata used when the data page is RLE coded.



