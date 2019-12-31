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

package org.apache.carbondata.core.util.comparator;

import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;

public final class Comparator {

  //Comparators are made static so that only one instance is generated
  private static final SerializableComparator BOOLEAN  = new BooleanSerializableComparator();
  private static final SerializableComparator INT = new IntSerializableComparator();
  private static final SerializableComparator SHORT = new ShortSerializableComparator();
  private static final SerializableComparator DOUBLE = new DoubleSerializableComparator();
  private static final SerializableComparator FLOAT = new FloatSerializableComparator();
  private static final SerializableComparator LONG = new LongSerializableComparator();
  private static final SerializableComparator DECIMAL  = new BigDecimalSerializableComparator();
  private static final SerializableComparator BYTE = new ByteArraySerializableComparator();

  public static SerializableComparator getComparator(DataType dataType) {
    if (dataType == DataTypes.DATE || dataType == DataTypes.TIMESTAMP) {
      return LONG;
    } else if (dataType == DataTypes.STRING) {
      return BYTE;
    } else {
      return getComparatorByDataTypeForMeasure(dataType);
    }
  }

  /**
   * create Comparator for Measure Datatype
   *
   * @param dataType
   * @return
   */
  public static SerializableComparator getComparatorByDataTypeForMeasure(DataType dataType) {
    if (dataType == DataTypes.BOOLEAN) {
      return BOOLEAN;
    } else if (dataType == DataTypes.INT) {
      return INT;
    } else if (dataType == DataTypes.SHORT) {
      return SHORT;
    } else if (dataType == DataTypes.LONG) {
      return LONG;
    } else if (dataType == DataTypes.DOUBLE) {
      return DOUBLE;
    } else if (dataType == DataTypes.FLOAT) {
      return FLOAT;
    } else if (DataTypes.isDecimal(dataType)) {
      return DECIMAL;
    } else if (dataType == DataTypes.BYTE) {
      return BYTE;
    } else {
      throw new IllegalArgumentException("Unsupported data type: " + dataType.getName());
    }
  }
}
