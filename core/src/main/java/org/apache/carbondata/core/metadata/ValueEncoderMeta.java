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

package org.apache.carbondata.core.metadata;

import java.io.Serializable;
import java.nio.ByteBuffer;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.page.statistics.SimpleStatsResult;
import org.apache.carbondata.core.metadata.datatype.DataType;

/**
 * DO NOT MODIFY THIS CLASS AND PACKAGE NAME, BECAUSE
 * IT IS SERIALIZE TO STORE
 * It holds Value compression metadata for one data column
 */
public class ValueEncoderMeta implements Serializable {

  /**
   * maxValue
   */
  protected Object maxValue;
  /**
   * minValue.
   */
  protected Object minValue;

  /**
   * uniqueValue
   */
  private Object uniqueValue;

  protected int decimal;

  private char type;

  private byte dataTypeSelected;

  static ValueEncoderMeta newInstance() {
    return new ValueEncoderMeta();
  }

  static ValueEncoderMeta newInstance(
      SimpleStatsResult stats, DataType targetDataType) {
    ValueEncoderMeta meta = new ValueEncoderMeta();
    meta.setSrcDataType(stats.getDataType());
    meta.maxValue = stats.getMax();
    meta.minValue = stats.getMin();
    meta.decimal = stats.getDecimalPoint();
    return meta;
  }

  public Object getMaxValue() {
    return maxValue;
  }

  public void setMaxValue(Object maxValue) {
    this.maxValue = maxValue;
  }

  public Object getMinValue() {
    return minValue;
  }

  public void setMinValue(Object minValue) {
    this.minValue = minValue;
  }

  public Object getUniqueValue() {
    return uniqueValue;
  }

  public void setUniqueValue(Object uniqueValue) {
    this.uniqueValue = uniqueValue;
  }

  public int getDecimalPoint() {
    return decimal;
  }

  public void setDecimalPoint(int decimalPoint) {
    this.decimal = decimalPoint;
  }

  public DataType getSrcDataType() {
    switch (type) {
      case CarbonCommonConstants.BIG_INT_MEASURE:
        return DataType.LONG;
      case CarbonCommonConstants.DOUBLE_MEASURE:
        return DataType.DOUBLE;
      case CarbonCommonConstants.BIG_DECIMAL_MEASURE:
        return DataType.DECIMAL;
      default:
        throw new RuntimeException("Unexpected type: " + type);
    }
  }

  public char getTypeInChar() {
    return type;
  }

  public void setSrcDataType(char type) {
    this.type = type;
  }


  public void setSrcDataType(DataType type) {
    switch (type) {
      case BYTE:
      case SHORT:
      case INT:
      case LONG:
        this.type = CarbonCommonConstants.BIG_INT_MEASURE;
        break;
      case DOUBLE:
        this.type = CarbonCommonConstants.DOUBLE_MEASURE;
        break;
      case DECIMAL:
        this.type = CarbonCommonConstants.BIG_DECIMAL_MEASURE;
        break;
      default:
        throw new RuntimeException("Unexpected type: " + type);
    }
  }

  public byte getDataTypeSelected() {
    return dataTypeSelected;
  }

  public void setDataTypeSelected(byte dataTypeSelected) {
    this.dataTypeSelected = dataTypeSelected;
  }

  public byte[] getMaxAsBytes() {
    return getValueAsBytes(maxValue);
  }

  public byte[] getMinAsBytes() {
    return getValueAsBytes(minValue);
  }

  /**
   * convert value to byte array
   */
  private byte[] getValueAsBytes(Object value) {
    ByteBuffer b;
    switch (getSrcDataType()) {
      case BYTE:
        b = ByteBuffer.allocate(8);
        b.putLong((byte) value);
        b.flip();
        return b.array();
      case SHORT:
        b = ByteBuffer.allocate(8);
        b.putLong((short) value);
        b.flip();
        return b.array();
      case INT:
        b = ByteBuffer.allocate(8);
        b.putLong((int) value);
        b.flip();
        return b.array();
      case LONG:
        b = ByteBuffer.allocate(8);
        b.putLong((long) value);
        b.flip();
        return b.array();
      case DOUBLE:
        b = ByteBuffer.allocate(8);
        b.putDouble((double) value);
        b.flip();
        return b.array();
      case DECIMAL:
      case BYTE_ARRAY:
        return new byte[8];
      default:
        throw new IllegalArgumentException("Invalid data type: " + getDataTypeSelected());
    }
  }

  public byte[] serialize() {
    return null;
  }

  public void deserialize(byte[] encodeMeta) {

  }
}
