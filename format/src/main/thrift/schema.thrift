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
 * File format description for CarbonData schema file
 */
namespace java org.apache.carbondata.format

/**
 * The types supported by Carbon Data.
 */
enum DataType {
	STRING = 0,
	SHORT = 1,
	INT = 2,
	LONG = 3,
	DOUBLE = 4,
	DECIMAL = 5,
	TIMESTAMP = 6,
	DATE = 7,
	BOOLEAN = 8,
	ARRAY = 20,
	STRUCT = 21,
	VARCHAR = 22,
	MAP = 23,
	FLOAT = 24,
	BYTE = 25
}

/**
 *	Encodings supported by Carbon Data.  Not all encodings are valid for all types.
 *	Certain Encodings can be chained.
 */
enum Encoding{
	DICTIONARY = 0; // Identified that a column is dictionary encoded
	DELTA = 1;	// Identifies that a column delta encoded
	RLE = 2;		// Indetifies that a column is run length encoded
	INVERTED_INDEX = 3; // Identifies that a column is encoded using inverted index, can be used only along with dictionary encoding
	BIT_PACKED = 4;	// Identifies that a column is encoded using bit packing, can be used only along with dictionary encoding
	DIRECT_DICTIONARY = 5; // Identifies that a column is direct dictionary encoded
	DIRECT_COMPRESS = 6;  // Identifies that a columm is encoded using DirectCompressCodec
	ADAPTIVE_INTEGRAL = 7; // Identifies that a column is encoded using AdaptiveIntegralCodec
	ADAPTIVE_DELTA_INTEGRAL = 8; // Identifies that a column is encoded using AdaptiveDeltaIntegralCodec
	RLE_INTEGRAL = 9;     // Identifies that a column is encoded using RLECodec
	DIRECT_STRING = 10;   // Stores string value and string length separately in page data
	ADAPTIVE_FLOATING = 11; // Identifies that a column is encoded using AdaptiveFloatingCodec
	BOOL_BYTE = 12;   // Identifies that a column is encoded using BooleanPageCodec
	ADAPTIVE_DELTA_FLOATING = 13; // Identifies that a column is encoded using AdaptiveDeltaFloatingCodec
	DIRECT_COMPRESS_VARCHAR = 14;  // Identifies that a columm is encoded using DirectCompressCodec, it is used for long string columns
}

enum PartitionType{
  RANGE = 0;
  RANGE_INTERVAL = 1;
  LIST = 2;
  HASH = 3;
  NATIVE_HIVE = 4; // Uses the standard partition features of spark/hive
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
	4: required bool columnar; // Whether it is stored as columnar format or row format
	5: required list<Encoding> encoders; // List of encoders that are chained to encode the data for this column
	6: required bool dimension;  // Whether the column is a dimension or measure
	7: optional i32 column_group_id; // The group ID for column used for row format columns, where in columns in each group are chunked together.
	/**
	 * Used when this column contains mantissa data.
	 */
	8: optional i32 scale;
	9: optional i32 precision;
	
	/** Nested fields.  Since thrift does not support nested fields,
	 * the nesting is flattened to a single list by a depth-first traversal.
	 * The children count is used to construct the nested relationship.
	 * This field is not set when the element is a primitive type
	 */
	10: optional i32 num_child;
	
	/** 
	 * Used when this column is part of an aggregate table.
	 */
	11: optional string aggregate_function;

	12: optional binary default_value;
	
	13: optional map<string,string> columnProperties;
	
  /**
	 * To specify the visibily of the column by default its false
	 */
	14: optional bool invisible;

	/**
	 * Column reference id
	 */
	15: optional string columnReferenceId;
	/**
	 * It will have column order which user has provided
	 */	
	16: optional i32 schemaOrdinal

  /**
  *  to maintain the column relation with parent table.
  *  will be usefull in case of pre-aggregate
  **/
	17: optional list<ParentColumnTableRelation> parentColumnTableRelations;
}

/**
 * Description of One Schema Change, contains list of added columns and deleted columns
 */
struct SchemaEvolutionEntry{
	1: required i64 time_stamp;
	2: optional list<ColumnSchema> added;
	3: optional list<ColumnSchema> removed;
	4: optional string tableName;
}

/**
 * History of schema evolution
 */
struct SchemaEvolution{
    1: required list<SchemaEvolutionEntry> schema_evolution_history;
}

/**
 * Partition information of table
 */
struct PartitionInfo{
    1: required list<ColumnSchema> partition_columns;
    2: required PartitionType partition_type;
    3: optional list<list<string>> list_info; // value list of list partition table
    4: optional list<string> range_info;  // range value list of range partition table
    5: optional list<i32> partition_ids; // partition id list
    6: optional i32 num_partitions;  // total partition count
    7: optional i32 max_partition;  // max partition id for now
}

/**
 * Bucketing information of fields on table
 */
struct BucketingInfo{
  1: required list<ColumnSchema> table_columns;
  2: required i32 number_of_buckets;
}

/**
 * The description of table schema
 */
struct TableSchema{
	1: required string table_id;  // ID used to
	2: required list<ColumnSchema> table_columns; // Columns in the table
	3: required SchemaEvolution schema_evolution; // History of schema evolution of this table
  4: optional map<string,string> tableProperties; // Table properties configured by the user
  5: optional BucketingInfo bucketingInfo; // Bucketing information
  6: optional PartitionInfo partitionInfo; // Partition information
  7: optional list<string> long_string_columns // long string columns in the table
}

struct RelationIdentifier {
   1: optional string databaseName;
   2: required string tableName;
   3: required string tableId;
}

struct ParentColumnTableRelation {
   1: required RelationIdentifier relationIdentifier;
   2: required string columnId;
   3: required string columnName
}

struct DataMapSchema  {
    // DataMap name
    1: required string dataMapName;
    // class name
    2: required string className;
    // to maintain properties which are mentioned in DMPROPERTIES of DDL and also it
    // stores properties of select query, query type like groupby, join in
    // case of preaggregate/timeseries
    3: optional map<string, string> properties;
    // relation identifier of a table which stores data of datamaps like preaggregate/timeseries.
    4: optional RelationIdentifier childTableIdentifier;
    // in case of preaggregate/timeseries datamap it will be used to maintain the child schema
    // which will be usefull in case of query and data load
    5: optional TableSchema childTableSchema;
}

struct TableInfo{
	1: required TableSchema fact_table;
	2: required list<TableSchema> aggregate_table_list;
	3: optional list<DataMapSchema> dataMapSchemas; // childSchema information
}
