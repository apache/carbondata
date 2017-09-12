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

package org.apache.carbondata.core.datastore.page;

import java.math.BigDecimal;

import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.util.ByteUtil;

/**
 * Represent a columnar data in one page for one column.
 */
public class SafeFixLengthColumnPage extends ColumnPage {

  // Only one of following fields will be used
  private byte[] byteData;
  private short[] shortData;
  private int[] intData;
  private long[] longData;
  private float[] floatData;
  private double[] doubleData;
  private byte[] shortIntData;

  SafeFixLengthColumnPage(DataType dataType, int pageSize, int scale, int precision) {
    super(dataType, pageSize, scale, precision);
  }

  /**
   * Set byte value at rowId
   */
  @Override
  public void putByte(int rowId, byte value) {
    byteData[rowId] = value;
  }

  /**
   * Set short value at rowId
   */
  @Override
  public void putShort(int rowId, short value) {
    shortData[rowId] = value;
  }

  /**
   * Set integer value at rowId
   */
  @Override
  public void putInt(int rowId, int value) {
    intData[rowId] = value;
  }

  /**
   * Set long value at rowId
   */
  @Override
  public void putLong(int rowId, long value) {
    longData[rowId] = value;
  }

  /**
   * Set double value at rowId
   */
  @Override
  public void putDouble(int rowId, double value) {
    doubleData[rowId] = value;
  }

  /**
   * Set string value at rowId
   */
  @Override
  public void putBytes(int rowId, byte[] bytes) {
    throw new UnsupportedOperationException("invalid data type: " + dataType);
  }

  @Override
  public void putShortInt(int rowId, int value) {
    byte[] converted = ByteUtil.to3Bytes(value);
    System.arraycopy(converted, 0, shortIntData, rowId * 3, 3);
  }

  @Override
  public void putBytes(int rowId, byte[] bytes, int offset, int length) {
    throw new UnsupportedOperationException("invalid data type: " + dataType);
  }

  @Override
  public void putDecimal(int rowId, BigDecimal decimal) {
    throw new UnsupportedOperationException("invalid data type: " + dataType);
  }

  @Override
  public byte[] getDecimalPage() {
    throw new UnsupportedOperationException("invalid data type: " + dataType);
  }

  /**
   * Get byte value at rowId
   */
  @Override
  public byte getByte(int rowId) {
    return byteData[rowId];
  }

  /**
   * Get short value at rowId
   */
  @Override
  public short getShort(int rowId) {
    return shortData[rowId];
  }

  /**
   * Get short int value at rowId
   */
  @Override
  public int getShortInt(int rowId) {
    return ByteUtil.valueOf3Bytes(shortIntData, rowId * 3);
  }

  /**
   * Get int value at rowId
   */
  @Override
  public int getInt(int rowId) {
    return intData[rowId];
  }

  /**
   * Get long value at rowId
   */
  @Override
  public long getLong(int rowId) {
    return longData[rowId];
  }

  /**
   * Get float value at rowId
   */
  @Override
  public float getFloat(int rowId) {
    return floatData[rowId];
  }

  /**
   * Get double value at rowId
   */
  @Override
  public double getDouble(int rowId) {
    return doubleData[rowId];
  }

  @Override
  public BigDecimal getDecimal(int rowId) {
    throw new UnsupportedOperationException("invalid data type: " + dataType);
  }

  @Override
  public byte[] getBytes(int rowId) {
    throw new UnsupportedOperationException("invalid data type: " + dataType);
  }

  /**
   * Get byte value page
   */
  @Override
  public byte[] getBytePage() {
    return byteData;
  }

  /**
   * Get short value page
   */
  @Override
  public short[] getShortPage() {
    return shortData;
  }

  /**
   * Get short value page
   */
  @Override
  public byte[] getShortIntPage() {
    return shortIntData;
  }

  /**
   * Get int value page
   */
  @Override
  public int[] getIntPage() {
    return intData;
  }

  /**
   * Get long value page
   */
  @Override
  public long[] getLongPage() {
    return longData;
  }

  /**
   * Get float value page
   */
  @Override
  public float[] getFloatPage() {
    return floatData;
  }

  /**
   * Get double value page
   */
  @Override
  public double[] getDoublePage() {
    return doubleData;
  }

  /**
   * Get string page
   */
  @Override
  public byte[][] getByteArrayPage() {
    throw new UnsupportedOperationException("invalid data type: " + dataType);
  }

  @Override
  public byte[] getLVFlattenedBytePage() {
    throw new UnsupportedOperationException("invalid data type: " + dataType);
  }

  /**
   * Set byte values to page
   */
  @Override
  public void setBytePage(byte[] byteData) {
    this.byteData = byteData;
  }

  /**
   * Set short values to page
   */
  @Override
  public void setShortPage(short[] shortData) {
    this.shortData = shortData;
  }

  /**
   * Set short values to page
   */
  @Override
  public void setShortIntPage(byte[] shortIntData) {
    this.shortIntData = shortIntData;
  }

  /**
   * Set int values to page
   */
  @Override
  public void setIntPage(int[] intData) {
    this.intData = intData;
  }

  /**
   * Set long values to page
   */
  @Override
  public void setLongPage(long[] longData) {
    this.longData = longData;
  }

  /**
   * Set float values to page
   */
  @Override
  public void setFloatPage(float[] floatData) {
    this.floatData = floatData;
  }

  /**
   * Set double value to page
   */
  @Override
  public void setDoublePage(double[] doubleData) {
    this.doubleData = doubleData;
  }

  /**
   * Set decimal values to page
   */
  @Override
  public void setByteArrayPage(byte[][] byteArray) {
    throw new UnsupportedOperationException("invalid data type: " + dataType);
  }

  @Override
  public void freeMemory() {
  }

  /**
   * apply encoding to page data
   * @param codec type of transformation
   */
  @Override
  public void convertValue(ColumnPageValueConverter codec) {
    switch (dataType) {
      case BYTE:
        for (int i = 0; i < pageSize; i++) {
          codec.encode(i, byteData[i]);
        }
        break;
      case SHORT:
        for (int i = 0; i < pageSize; i++) {
          codec.encode(i, shortData[i]);
        }
        break;
      case INT:
        for (int i = 0; i < pageSize; i++) {
          codec.encode(i, intData[i]);
        }
        break;
      case LONG:
        for (int i = 0; i < pageSize; i++) {
          codec.encode(i, longData[i]);
        }
        break;
      case FLOAT:
        for (int i = 0; i < pageSize; i++) {
          codec.encode(i, floatData[i]);
        }
        break;
      case DOUBLE:
        for (int i = 0; i < pageSize; i++) {
          codec.encode(i, doubleData[i]);
        }
        break;
      default:
        throw new UnsupportedOperationException("not support value conversion on " +
            dataType + " page");
    }
  }

}
