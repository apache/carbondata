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
 * File format description for carbon dictionary file
 */
namespace java org.carbondata.format

struct ColumnDictionaryChunk {
	1: required i32 min_surrogate_key //The least surrogate key in this dictionary, in most cases min will be 0, but after history data deletion, min can be non-zero
	2: list<binary> values // the values in dictionary order, each value is represented by a list of bytes, The values can be of any supported data type
	3: required i32 max_surrogate_key //The least surrogate key in this dictionary, in most cases min will be 0, but after history data deletion, min can be non-zero
}

struct ColumnDictionary{
	1: required list<ColumnDictionaryChunk> dictionary_chunks
}