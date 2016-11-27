/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/**
 * File format description for the carbon file format
 */
namespace java org.apache.carbondata.format

include "schema.thrift"
include "carbondata.thrift"

/**
 * header information stored in index file
 */
struct IndexHeader{
  1: required i32 version; // version used for data compatibility
  2: required list<schema.ColumnSchema> table_columns;	// Description of columns in this file
  3: required carbondata.SegmentInfo segment_info;	// Segment info (will be same/repeated for all files in this segment)
  4: optional i32 bucket_id; //bucket number in which file contains
}

/**
 * block index information stored in index file for every block
 */
struct BlockIndex{
  1: required i64 num_rows; // Total number of rows in this file
  2: required string file_name; // Block file name
  3: required i64 offset; // Offset of block
  4: required carbondata.BlockletIndex block_index;	// Block index
}