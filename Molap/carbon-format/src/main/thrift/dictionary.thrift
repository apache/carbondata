struct ColumnDictionaryChunk {
	1: required i32 min_surrogate_key //The least surrogate key in this dictionary, in most cases min will be 0, but after history data deletion, min can be non-zero
	2: list <list<byte>> values // the values in dictionary order, each value is represented by a list of bytes, The values can be of any supported data type
	3: required i32 max_surrogate_key //The least surrogate key in this dictionary, in most cases min will be 0, but after history data deletion, min can be non-zero
}

struct ColumnDictionary{
	1: required list<ColumnDictionaryChunk> dictionary_chunks
}