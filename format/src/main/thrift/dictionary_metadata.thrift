/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * File format description for CarbonData dictionary metadata file
 */
namespace java org.apache.carbondata.format

struct ColumnDictionaryChunkMeta {
	1: required i32 min_surrogate_key; //The least surrogate key in this dictionary, in most cases min will be 0, but after history data deletion, min can be non-zero
	2: required i32 max_surrogate_key; //The Max surrogate key to be stored , so that next load can continue ID generation from this number.
	3: required i64 start_offset; // The start offset of this column dictionary chunk in the dictionary file.
	4: required i64 end_offset; // The end offset of this column dictionary chunk in the dictionary file.
	5: required i32 chunk_count; // The count of total dictionary objects for one segment.
	6: optional i64 segment_id; // the mapping of this dictionary chunk to the corresponding segment of the table, not useful in case of shared dimensions.
}
