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

package org.apache.carbondata.core.metadata.datatype;

import java.util.ArrayList;
import java.util.List;

/**
 * Holds all singleton object for all data type used in carbon
 */
public class DataTypes {

  // singleton for each data type
  public static final DataType STRING = StringType.STRING;
  public static final DataType DATE = DateType.DATE;
  public static final DataType TIMESTAMP = TimestampType.TIMESTAMP;
  public static final DataType BOOLEAN = BooleanType.BOOLEAN;
  public static final DataType SHORT = ShortType.SHORT;
  public static final DataType INT = IntType.INT;
  public static final DataType FLOAT = FloatType.FLOAT;
  public static final DataType LONG = LongType.LONG;
  public static final DataType DOUBLE = DoubleType.DOUBLE;
  public static final DataType NULL = NullType.NULL;
  public static final DataType BYTE = ByteType.BYTE;
  public static final DataType BINARY = BinaryType.BINARY;

  // internal use only, for variable length data type
  public static final DataType BYTE_ARRAY = ByteArrayType.BYTE_ARRAY;

  // internal use only, for value compression from integer/long to 3 bytes value
  public static final DataType SHORT_INT = ShortIntType.SHORT_INT;

  public static final DataType VARCHAR = VarcharType.VARCHAR;

  // these IDs are used within this package only
  static final int STRING_TYPE_ID = 0;
  static final int DATE_TYPE_ID = 1;
  static final int TIMESTAMP_TYPE_ID = 2;
  static final int BOOLEAN_TYPE_ID = 3;
  static final int SHORT_TYPE_ID = 4;
  static final int INT_TYPE_ID = 5;
  static final int FLOAT_TYPE_ID = 6;
  static final int LONG_TYPE_ID = 7;
  static final int DOUBLE_TYPE_ID = 8;
  static final int NULL_TYPE_ID = 9;
  static final int BYTE_TYPE_ID = 14;
  static final int BYTE_ARRAY_TYPE_ID = 15;
  static final int SHORT_INT_TYPE_ID = 16;
  static final int LEGACY_LONG_TYPE_ID = 17;
  public static final int DECIMAL_TYPE_ID = 10;
  public static final int ARRAY_TYPE_ID = 11;
  public static final int STRUCT_TYPE_ID = 12;
  public static final int MAP_TYPE_ID = 13;
  public static final int VARCHAR_TYPE_ID = 18;
  public static final int BINARY_TYPE_ID = 19;

  /**
   * create a DataType instance from uniqueId of the DataType
   */
  public static DataType valueOf(int id) {
    if (id == STRING.getId()) {
      return STRING;
    } else if (id == DATE.getId()) {
      return DATE;
    } else if (id == TIMESTAMP.getId()) {
      return TIMESTAMP;
    } else if (id == BOOLEAN.getId()) {
      return BOOLEAN;
    } else if (id == BYTE.getId()) {
      return BYTE;
    } else if (id == SHORT.getId()) {
      return SHORT;
    } else if (id == SHORT_INT.getId()) {
      return SHORT_INT;
    } else if (id == INT.getId()) {
      return INT;
    } else if (id == LONG.getId()) {
      return LONG;
    } else if (id == FLOAT.getId()) {
      return FLOAT;
    } else if (id == DOUBLE.getId()) {
      return DOUBLE;
    } else if (id == NULL.getId()) {
      return NULL;
    } else if (id == DECIMAL_TYPE_ID) {
      return createDefaultDecimalType();
    } else if (id == BINARY.getId()) {
      return BINARY;
    } else if (id == ARRAY_TYPE_ID) {
      return createDefaultArrayType();
    } else if (id == STRUCT_TYPE_ID) {
      return createDefaultStructType();
    } else if (id == MAP_TYPE_ID) {
      return createDefaultMapType();
    } else if (id == BYTE_ARRAY.getId()) {
      return BYTE_ARRAY;
    } else if (id == VARCHAR.getId()) {
      return VARCHAR;
    } else {
      throw new RuntimeException("create DataType with invalid id: " + id);
    }
  }

  /**
   * create a decimal type object with specified precision and scale
   */
  public static DecimalType createDecimalType(int precision, int scale) {
    return new DecimalType(precision, scale);
  }

  /**
   * create a decimal type object with default precision = 10 and scale = 2
   */
  public static DecimalType createDefaultDecimalType() {
    return new DecimalType(10, 2);
  }

  public static boolean isDecimal(DataType dataType) {
    return dataType.getId() == DECIMAL_TYPE_ID;
  }

  /**
   * create array type with specified element type
   */
  public static ArrayType createArrayType(DataType elementType) {
    return new ArrayType(elementType);
  }

  /**
   * create array type with specified element type and name
   */
  public static ArrayType createArrayType(DataType elementType, String elementName) {
    return new ArrayType(elementType, elementName);
  }

  /**
   * create a array type object with no child
   */
  public static ArrayType createDefaultArrayType() {
    return new ArrayType(STRING);
  }

  public static boolean isArrayType(DataType dataType) {
    return dataType.getId() == ARRAY_TYPE_ID;
  }

  /**
   * create struct type with specified fields
   */
  public static StructType createStructType(List<StructField> fields) {
    return new StructType(fields);
  }

  /**
   * create a struct type object with no field
   */
  public static StructType createDefaultStructType() {
    return new StructType(new ArrayList<StructField>());
  }

  public static boolean isStructType(DataType dataType) {
    return dataType.getId() == STRUCT_TYPE_ID;
  }

  /**
   * create map type with specified key type and value type
   */
  public static MapType createMapType(DataType keyType, DataType valueType) {
    return new MapType(keyType, valueType);
  }

  /**
   * create a map type object with no child
   */
  public static MapType createDefaultMapType() {
    return new MapType(STRING, STRING);
  }

  public static boolean isMapType(DataType dataType) {
    return dataType.getId() == MAP_TYPE_ID;
  }

}
