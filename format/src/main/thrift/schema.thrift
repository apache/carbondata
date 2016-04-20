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
 * File format description for carbon schema file
 */
namespace java org.carbondata.format

/**
*	The types supported by Carbon Data.
*/
enum DataType {
	STRING = 0,
	INT = 1,
	LONG = 2,
	DOUBLE = 3,
	DECIMAL = 4,
	TIMESTAMP = 5,
	ARRAY = 20,
	STRUCT = 21,
}

/**
 * Common types used by frameworks(e.g. hive, pig, spark) using carbondata.  This helps map
 * between types in those frameworks to the base types in carbondata.  This is only
 * metadata and not needed to read or write the data.
 */
enum ConvertedType {
  /** a BYTE_ARRAY actually contains UTF8 encoded chars */
  UTF8 = 0,

  /** a map is converted as an optional field containing a repeated key/value pair */
  MAP = 1,

  /** a key/value pair is converted into a group of two fields */
  MAP_KEY_VALUE = 2,

  /** a list is converted into an optional field containing a repeated field for its
   * values */
  LIST = 3,

  /** an enum is converted into a binary field */
  ENUM = 4,

  /**
   * A decimal value.
   *
   * This may be used to annotate binary or fixed primitive types. The
   * underlying byte array stores the unscaled value encoded as two's
   * complement using big-endian byte order (the most significant byte is the
   * zeroth element). The value of the decimal is the value * 10^{-scale}.
   *
   * This must be accompanied by a (maximum) precision and a scale in the
   * SchemaElement. The precision specifies the number of digits in the decimal
   * and the scale stores the location of the decimal point. For example 1.23
   * would have precision 3 (3 total digits) and scale 2 (the decimal point is
   * 2 digits over).
   */
  DECIMAL = 5,

  /**
   * A Date
   *
   * Stored as days since Unix epoch, encoded as the INT32 physical type.
   *
   */
  DATE = 6,

  /** 
   * A time 
   *
   * The total number of milliseconds since midnight.  The value is stored 
   * as an INT32 physical type.
   */
  TIME_MILLIS = 7,

  /**
   * A date/time combination
   * 
   * Date and time recorded as milliseconds since the Unix epoch.  Recorded as
   * a physical type of INT64.
   */
  TIMESTAMP_MILLIS = 8,

  RESERVED = 10,

  /** 
   * An unsigned integer value.  
   * 
   * The number describes the maximum number of meainful data bits in 
   * the stored value. 8, 16 and 32 bit values are stored using the 
   * INT32 physical type.  64 bit values are stored using the INT64
   * physical type.
   *
   */
  UINT_8 = 11,
  UINT_16 = 12,
  UINT_32 = 13,
  UINT_64 = 14,

  /**
   * A signed integer value.
   *
   * The number describes the maximum number of meainful data bits in
   * the stored value. 8, 16 and 32 bit values are stored using the
   * INT32 physical type.  64 bit values are stored using the INT64
   * physical type.
   *
   */
  INT_8 = 15,
  INT_16 = 16,
  INT_32 = 17,
  INT_64 = 18,

  /** 
   * An embedded JSON document
   * 
   * A JSON document embedded within a single UTF8 column.
   */
  JSON = 19,

  /** 
   * An embedded BSON document
   * 
   * A BSON document embedded within a single BINARY column. 
   */
  BSON = 20,

  /**
   * An interval of time
   * 
   * This type annotates data stored as a FIXED_LEN_BYTE_ARRAY of length 12
   * This data is composed of three separate little endian unsigned
   * integers.  Each stores a component of a duration of time.  The first
   * integer identifies the number of months associated with the duration,
   * the second identifies the number of days associated with the duration
   * and the third identifies the number of milliseconds associated with 
   * the provided duration.  This duration of time is independent of any
   * particular timezone or date.
   */
  INTERVAL = 21
  
}

/**
*	Encodings supported by Carbon Data.  Not all encodings are valid for all types.
*	Certain Encodings can be chained.
*/
enum Encoding{
	DICTIONARY = 0; // Identified that a column is dictionary encoded
	DELTA = 1;	// Identifies that a column delta encoded
	RLE = 2;		// Indetifies that a column is run length encoded
	INVERTED_INDEX = 3; // identifies that a column is encoded using inverted index, can be used only along with dictionary encoding
	BIT_PACKED = 4;	// identifies that a column is encoded using bit packing, can be used only along with dictionary encoding
}


/**
* Description of a Column for both dimension and measure
*/
//TODO:where to put the CSV column name and carbon table column name mapping? should not keep in schema
struct ColumnSchema{ 
	1: required DataType data_type;
	/**
	* Name of the column. If it is a complex data type, we follow a naming rule grand_parent_column.parent_column.child_column
	* For Array types, two columns will be stored one for the array type and one for the primitive type with the name parent_column.value
	*/
	2: required string column_name;  //
	3: required string column_id;  // Unique ID for a column. if this is dimension, it is an unique ID that used in dictionary
	4: required bool columnar; // wether it is stored as columnar format or row format
	5: required list<Encoding> encoders; // List of encoders that are chained to encode the data for this column
	6: required bool dimension;  // Whether the column is a dimension or measure
	7: optional i32 column_group_id; // The group ID for column used for row format columns, where in columns in each group are chunked together.
	
	/** When the schema is the result of a conversion from another model
   * Used to record the original type to help with cross conversion.
   */
	8: optional ConvertedType converted_type;
	/** 
	* Used when this column contains decimal data.
	*/
	9: optional i32 scale;
	10: optional i32 precision;
	
	/** Nested fields.  Since thrift does not support nested fields,
	* the nesting is flattened to a single list by a depth-first traversal.
	* The children count is used to construct the nested relationship.
	* This field is not set when the element is a primitive type
	*/
	11: optional i32 num_child;
	
	/** 
	* Used when this column is part of an aggregate table.
	*/
	12: optional string aggregate_function;

	13: optional binary default_value;
}

/**
* Description of One Schema Change, contains list of added columns and deleted columns
*/
struct SchemaEvolutionEntry{
	1: required i64 time_stamp;
	2: optional list<ColumnSchema> added;
	3: optional list<ColumnSchema> removed;
}

/**
* History of schema evolution
*/
struct SchemaEvolution{
1: required list<SchemaEvolutionEntry> schema_evolution_history;
}

/**
* The description of table schema
*/
struct TableSchema{
	1: required i32 table_id;  // ID used to
	2: required list<ColumnSchema> table_columns; // Columns in the table
	3: required SchemaEvolution schema_evolution; // History of schema evolution of this table
}

struct TableInfo{
	1: required TableSchema fact_table;
	2: required list<TableSchema> aggregate_table_list;

}
