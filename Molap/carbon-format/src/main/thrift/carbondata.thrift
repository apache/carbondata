
include schema_file.thrift

/**
* Information about a segment, that represents one data load
*/
struct SegmentInfo{
	1: required i32 num_cols // Number of columns in this load, because schema can evolve . TODO: Check whether this is really required
	2: required list<i32> column_cardanilities // Cardinality of columns

}

/**
*	Btree index of one node.
*/
struct LeafNodeBtreeIndex{
	1: required list<byte> start_key // Bit-packed start key of one leaf node
	2: required list<byte> end_key	// Bit-packed start key of one leaf node
}

/**
*	Min-max index of one complete file
*/
struct LeafNodeMinMaxIndex{
	1: required list<list<byte>> min //Min value of all columns of one leaf node Bit-Packed
	2: required list<list<byte>> max //Max value of all columns of one leaf node Bit-Packed
}

/**
*	Index of all leaf nodes in one file
*/
struct LeafNodeIndex{
	1: optional list<LeafNodeMinMaxIndex> min_max_index
	2: optional list<LeafNodeBtreeIndex> b_tree_index
}


/**
* Sort state of one column
*/
enum SortState{
	SORT_NONE=0; // Data is not sorted
	SORT_NATIVE=1 //Source data was sorted
	SORT_EXPLICIT=2;	// Sorted (ascending) when loading
}

/**
 * Wrapper struct to specify sort order
 */
struct SortingColumn {
  /** The column index (in this leaf node) **/
  1: required i32 column_idx

  /** If true, indicates this column is sorted in descending order. **/
  2: required SortState sort_state

}


/**
*	Compressions supported by Carbon Data. 
*/
enum Compression{
	SNAPPY=0; 
	LZO=1;	
	GZIP=2;	
	CUSTOM=3; 
}

/**
*	Wrapper for the encoder and the cutstom class name for custom encoder.
*/
struct Compressor{
	1: required Compression compression;
	2: optional string custom_class_name; // Custom class name if Compression is custom.

}

/**
* Represents the data of one dimension one dimension group in one leaf node
*/
// add a innger level placeholder for further I/O granulatity
struct ChunkCompressionMeta{
	1: required Compressor compressor // the compressor used
	/** total byte size of all uncompressed pages in this column chunk (including the headers) **/
	2: required i64 total_uncompressed_size
	/** total byte size of all compressed pages in this column chunk (including the headers) **/
	3: required i64 total_compressed_size
}
/**
* To handle space data with nulls
*/
struct PresenceMeta{
	1: required bool represents_presence // if true, ones in the bit stream reprents presence. otherwise represents absence
	2: required list<byte> present_bit_stream // Compressed bit stream representing the presence of null values
}


struct DimensionDataChunk{
	1: required ChunkCompressionMeta chunk_meta // the metadata of a chunk
	2: required boolean isRowChunk // whether this chunk is a row chunk or column chunk ? Decide whethe rthis can be replace with counting od columnIDs
	3: required list<i32> columnIDs // the column IDs in this chunk, will have atleast one column ID for columnar format, many column ID for row major format
	4: required i64 data_page_offset // Offset of data page
	5: required i32 data_page_length // length of data page
	6: optional i64 rowid_page_offset //offset of row id page, only if encoded using inverted index
	7: optional i32 rowid_page_length //length of row id page, only if encoded using inverted index
	8: optional i64 rle_page_offset	// offset of rle page, only if RLE coded.
	9: optional i32 rle_page_length	// length of rle page, only if RLE coded.
	10: optional PresenceMeta presence // information about presence of values in each row of this column chunk
}

/**
*	Represents data of a measure in one leaf node
*/
struct MeasureDataChunk{
	1: required ChunkCompressionMeta chunk_meta // the metadata of a chunk
	2: required boolean isRowChunk // whether this chunk is a row chunk or column chunk
	3: required list<i32> columnIDs // the column IDs in this chunk, will have atleast one column ID for columnar format, many column ID for row major format
	4: required i64 data_page_offset	// The offset of data page
	5: required i32 data_page_length	// The length of data page
	/**The data type that is used to store the data physically, 
	*Higher types can be stored using lower types to save space 
	*if the data can fit the precision of nower type
	*/
	6: required list<DataType> encoded_type 
	7: optional PresenceMeta presence // information about presence of values in each row of this column chunk
}

/**
*	Information about a leaf node
*/
struct LeafNodeInfo{
1: required i64 num_rows	// Number of rows in this leaf node
2: optional list<SortingColumn> sorting_columns	// The sort information of all columns in this leaf node
3: required list<DimensionDataChunk> dimensions	// Information about dimension chunk of all dimensions in this leaf node
4: required list<MeasureDataChunk> measures	// Information about measure chunk of all measures in this leaf node
}

/**
* Description of one data file
*/
struct FileMeta{
	1: required i32 version // version used for data compatibility
	2: required i64 num_rows // Total number of rows in this file
	3: required SegmentInfo segment_info	// Segment info (will be same/repeated for all files in this segment)
	4: required LeafNodeIndex index	// Leaf node index of all leaf nodes in this file
	5: required list<ColumnSchema> table_columns	// Description of columns in this file
	6: required list<LeafNodeInfo> leaf_node_info	// Information about leaf nodes of all columns in this file
}
