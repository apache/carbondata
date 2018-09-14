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

import java.io.Serializable;

public class DataType implements Serializable {

  private static final long serialVersionUID = 19371726L;

  // id is used for comparison and serialization/deserialization
  private int id;
  private int precedenceOrder;
  private String name;

  // size of the value of this data type, negative value means variable length
  private int sizeInBytes;

  DataType(int id, int precedenceOrder, String name, int sizeInBytes) {
    this.id = id;
    this.precedenceOrder = precedenceOrder;
    this.name = name;
    this.sizeInBytes = sizeInBytes;
  }

  public int getPrecedenceOrder() {
    return precedenceOrder;
  }

  public String getName() {
    return name;
  }

  public int getSizeInBytes() {
    return sizeInBytes;
  }

  public int getSizeBits() {
    return (int) (Math.log(getSizeInBytes()) / Math.log(2));
  }

  public int getId() {
    return id;
  }

  public boolean isComplexType() {
    return false;
  }

  @Override
  public String toString() {
    return getName();
  }

  public static final char DOUBLE_MEASURE_CHAR = 'n';
  public static final char STRING_CHAR = 's';
  public static final char VARCHAR_CHAR = 'v';
  public static final char TIMESTAMP_CHAR = 't';
  public static final char DATE_CHAR = 'x';
  public static final char BYTE_ARRAY_CHAR = 'y';
  public static final char BYTE_VALUE_MEASURE_CHAR = 'c';
  public static final char BIG_DECIMAL_MEASURE_CHAR = 'b';
  public static final char BIG_INT_MEASURE_CHAR = 'd';

  public static char convertType(DataType dataType) {
    if (dataType == DataTypes.BYTE ||
        dataType == DataTypes.BOOLEAN ||
        dataType == DataTypes.SHORT ||
        dataType == DataTypes.SHORT_INT ||
        dataType == DataTypes.INT ||
        dataType == DataTypes.LONG) {
      return BIG_INT_MEASURE_CHAR;
    } else if (dataType == DataTypes.DOUBLE || dataType == DataTypes.FLOAT) {
      return DOUBLE_MEASURE_CHAR;
    } else if (DataTypes.isDecimal(dataType)) {
      return BIG_DECIMAL_MEASURE_CHAR;
    } else if (dataType == DataTypes.STRING) {
      return STRING_CHAR;
    } else if (dataType == DataTypes.VARCHAR) {
      return VARCHAR_CHAR;
    } else if (dataType == DataTypes.TIMESTAMP) {
      return TIMESTAMP_CHAR;
    } else if (dataType == DataTypes.DATE) {
      return DATE_CHAR;
    } else if (dataType == DataTypes.BYTE_ARRAY) {
      return BYTE_ARRAY_CHAR;
    } else {
      throw new RuntimeException("Unexpected type: " + dataType);
    }
  }

  public static DataType getDataType(char type) {
    switch (type) {
      case BIG_INT_MEASURE_CHAR:
        return DataTypes.LONG;
      case DOUBLE_MEASURE_CHAR:
        return DataTypes.DOUBLE;
      case BIG_DECIMAL_MEASURE_CHAR:
        return DataTypes.createDefaultDecimalType();
      case 'l':
        return DataTypes.LEGACY_LONG;
      default:
        throw new RuntimeException("Unexpected type: " + type);
    }
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + getName().hashCode();
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    if (!this.getName().equalsIgnoreCase(((DataType) obj).getName())) {
      return false;
    }
    return true;
  }
}
