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
 * File format description for the CarbonData file format
 */
namespace java org.apache.carbondata.format

include "schema.thrift"
include "dictionary.thrift"

/**
 * Information about a segment, that represents one data load
 */
struct SegmentInfo{
    1: required i32 num_cols; // Number of columns in this load, because schema can evolve, different segments may have different columns
    2: required list<i32> column_cardinalities; // Cardinality of columns
}

/**
 * Btree index of one blocklet
 */
struct BlockletBTreeIndex{
    1: required binary start_key; // Bit-packed start key of one blocklet
    2: required binary end_key;	// Bit-packed end key of one blocklet
}

/**
 * Min-max index of one blocklet
 */
struct BlockletMinMaxIndex{
    1: required list<binary> min_values; //Min value of all columns of one blocklet Bit-Packed
    2: required list<binary> max_values; //Max value of all columns of one blocklet Bit-Packed
}

/**
 * Index of one blocklet
 */
struct BlockletIndex{
    1: optional BlockletMinMaxIndex min_max_index;
    2: optional BlockletBTreeIndex b_tree_index;
}

/**
 * Sort state of one column
 */
enum SortState{
    SORT_NONE = 0; // Data is not sorted
    SORT_NATIVE = 1; //Source data was sorted
    SORT_EXPLICIT = 2;	// Sorted (ascending) when loading
}

/**
 * Compressions supported by CarbonData.
 */
enum CompressionCodec{
    SNAPPY = 0;
}

/**
 * Represents the data of one column page or one column page group inside blocklet.
 * Currently CarbonData using snappy for compression directly , user can specify the different compression by ChunkCompressionMeta.
 * Currently not using it , keep it as for future extend ability.
 */
struct ChunkCompressionMeta{
    1: required CompressionCodec compression_codec; // The compressor used
    /** Total byte size of all uncompressed pages in this column chunk (including the headers) **/
    2: required i64 total_uncompressed_size;
    /** Total byte size of all compressed pages in this column chunk (including the headers) **/
    3: required i64 total_compressed_size;
}

/**
 * To handle space data with nulls
 */
struct PresenceMeta{
    1: required bool represents_presence; // If true, ones in the bit stream reprents presence. otherwise represents absence
    2: required binary present_bit_stream; // Compressed bit stream representing the presence of null values
}

/**
 * Represents a chunk of data. The chunk can be a single column stored in Column Major format or a group of columns stored in Row Major Format.
 * For V1 format.
 */
struct DataChunk{
    1: required ChunkCompressionMeta chunk_meta; // The metadata of a chunk
    2: required bool rowMajor; // Whether this chunk is a row chunk or column chunk
	/** The column IDs in this chunk, in the order in which the data is physically stored, will have atleast one column ID for columnar format, many column ID for row major format**/
    3: required list<i32> column_ids;
    4: required i64 data_page_offset; // Offset of data page
    5: required i32 data_page_length; // Length of data page
    6: optional i64 rowid_page_offset; // Offset of row id page, only if encoded using inverted index
    7: optional i32 rowid_page_length; // Length of row id page, only if encoded using inverted index
    8: optional i64 rle_page_offset;	// Offset of rle page, only if RLE coded.
    9: optional i32 rle_page_length;	// Length of rle page, only if RLE coded.
    10: optional PresenceMeta presence; // Information about presence of values in each row of this column chunk
    11: optional SortState sort_state;
    12: optional list<schema.Encoding> encoders; // The List of encoders overriden at node level
    13: optional list<binary> encoder_meta; // Extra information required by encoders
}

/**
 * Represents the metadata of a data chunk.
 * The chunk can be a single column stored in Column Major format or a group of columns stored
 * in Row Major format.
 *
 * For V3, one data chunk is one page data of 32K rows.
 * For V2, one data chunk is one blocklet data.
 */
struct DataChunk2{
    1: required ChunkCompressionMeta chunk_meta; // The metadata of a chunk
    2: required bool rowMajor; // Whether this chunk is a row chunk or column chunk
	/** The column IDs in this chunk, in the order in which the data is physically stored, will have atleast one column ID for columnar format, many column ID for row major format**/
    3: required i32 data_page_length; // Length of data page
    4: optional i32 rowid_page_length; // Length of row id page, only if encoded using inverted index
    5: optional i32 rle_page_length;	// Length of rle page, only if RLE coded.
    6: optional PresenceMeta presence; // Information about presence of values in each row of this column chunk
    7: optional SortState sort_state;
    8: optional list<schema.Encoding> encoders; // The List of encoders overriden at node level
    9: optional list<binary> encoder_meta; // Extra information required by encoders
    10: optional BlockletMinMaxIndex min_max; 
    11: optional i32 numberOfRowsInpage;
 }


/**
 * Represents a chunk of data. The chunk can be a single column stored in Column Major format or a group of columns stored in Row Major Format.
 * For V3 format
 */
struct DataChunk3{
    1: required list<DataChunk2> data_chunk_list; // List of data chunk
    2: optional list<i32> page_offset; // Offset of each chunk
    3: optional list<i32> page_length; // Length of each chunk
   
 }
/**
 * Information about a blocklet for V1 format
 */
struct BlockletInfo{
    1: required i32 num_rows;	// Number of rows in this blocklet
    2: required list<DataChunk> column_data_chunks;	// Information about all column chunks in this blocklet
}

/**
 * Information about a blocklet for V2 format
 */
struct BlockletInfo2{
    1: required i32 num_rows;	// Number of rows in this blocklet
    2: required list<i64> column_data_chunks_offsets;	// Information about offsets all column chunks in this blocklet
    3: required list<i16> column_data_chunks_length;	// Information about length all column chunks in this blocklet
}

/**
 * Information about a blocklet for V3 format
 */
struct BlockletInfo3{
    1: required i32 num_rows;	// Number of rows in this blocklet
    2: required list<i64> column_data_chunks_offsets;	// Information about offsets all column chunks in this blocklet
    3: required list<i32> column_data_chunks_length;	// Information about length all column chunks in this blocklet
    4: required i64 dimension_offsets;
    5: required i64 measure_offsets;
    6: required i32 number_number_of_pages; // This is rquired for alter table, in case of alter table when filter is only selected on new added column this will help
  }

/**
 * Footer for indexed carbon file
 */
struct FileFooter{
    1: required i32 version; // Version used for data compatibility
    2: required i64 num_rows; // Total number of rows in this file
    3: required list<schema.ColumnSchema> table_columns;	// Description of columns in this file
    4: required SegmentInfo segment_info;	// Segment info (will be same/repeated for all files in this segment)
    5: required list<BlockletIndex> blocklet_index_list;	// Blocklet index of all blocklets in this file
    6: optional list<BlockletInfo> blocklet_info_list;	// Information about blocklets of all columns in this file for V1 format
    7: optional list<BlockletInfo2> blocklet_info_list2;	// Information about blocklets of all columns in this file for V2 format
    8: optional dictionary.ColumnDictionaryChunk dictionary; // Blocklet local dictionary
}

/**
 * Footer for indexed carbon file for V3 format
 */
struct FileFooter3{
    1: required i64 num_rows; // Total number of rows in this file
    2: required SegmentInfo segment_info;	// Segment info (will be same/repeated for all files in this segment)
    3: required list<BlockletIndex> blocklet_index_list;	// Blocklet index of all blocklets in this file
    4: optional list<BlockletInfo3> blocklet_info_list3;	// Information about blocklets of all columns in this file for V3 format
    5: optional dictionary.ColumnDictionaryChunk dictionary; // Blocklet local dictionary
}

/**
 * Header for appendable carbon file
 */
struct FileHeader{
	1: required i32 version; // Version used for data compatibility
	2: required list<schema.ColumnSchema> column_schema;  // Description of columns in this file
	3: optional bool is_footer_present; //  To check whether footer is present or not
	4: optional i64 time_stamp; // Timestamp to compare column schema against master schema
}

/**
 * Mutation type
 */
enum MutationType {
    INSERT = 0; // Used for inserting data
    DELETE = 1; // Used for deleting data
}

/**
 * Header for one blocklet in appendable carbon file
 */
struct BlockletHeader{
	1: required i32 blocklet_length; // Length of blocklet data
	2: required MutationType mutation; // Mutation type of this blocklet
	3: required BlockletIndex blocklet_index;  // Index for the following blocklet
	4: required BlockletInfo blocklet_info;  // Info for the following blocklet
	5: optional dictionary.ColumnDictionaryChunk dictionary; // Blocklet local dictionary
}