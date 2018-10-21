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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigDecimal;

import org.apache.carbondata.core.datastore.page.encoding.ColumnPageEncoderMeta;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
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
  private byte[][] fixedLengthdata;

  // total number of entries in array
  private int arrayElementCount = 0;

  SafeFixLengthColumnPage(ColumnPageEncoderMeta columnPageEncoderMeta, int pageSize) {
    super(columnPageEncoderMeta, pageSize);
    this.fixedLengthdata = new byte[pageSize][];
  }

  /**
   * Set byte value at rowId
   */
  @Override
  public void putByte(int rowId, byte value) {
    ensureArraySize(rowId, DataTypes.BYTE);
    byteData[rowId] = value;
    arrayElementCount++;
  }

  /**
   * Set short value at rowId
   */
  @Override
  public void putShort(int rowId, short value) {
    ensureArraySize(rowId, DataTypes.SHORT);
    shortData[rowId] = value;
    arrayElementCount++;
  }

  /**
   * Set integer value at rowId
   */
  @Override
  public void putInt(int rowId, int value) {
    ensureArraySize(rowId, DataTypes.INT);
    intData[rowId] = value;
    arrayElementCount++;
  }

  /**
   * Set long value at rowId
   */
  @Override
  public void putLong(int rowId, long value) {
    ensureArraySize(rowId, DataTypes.LONG);
    longData[rowId] = value;
    arrayElementCount++;
  }

  /**
   * Set double value at rowId
   */
  @Override
  public void putDouble(int rowId, double value) {
    ensureArraySize(rowId, DataTypes.DOUBLE);
    doubleData[rowId] = value;
    arrayElementCount++;
  }

  /**
   * Set float value at rowId
   */
  @Override
  public void putFloat(int rowId, float value) {
    ensureArraySize(rowId, DataTypes.FLOAT);
    floatData[rowId] = value;
    arrayElementCount++;
  }

  /**
   * Set string value at rowId
   */
  @Override
  public void putBytes(int rowId, byte[] bytes) {
    ensureArraySize(rowId, DataTypes.BYTE_ARRAY);
    this.fixedLengthdata[rowId] = bytes;
    arrayElementCount++;
  }

  @Override
  public void putShortInt(int rowId, int value) {
    ensureArraySize(rowId, DataTypes.SHORT_INT);
    byte[] converted = ByteUtil.to3Bytes(value);
    System.arraycopy(converted, 0, shortIntData, rowId * 3, 3);
    arrayElementCount++;
  }

  @Override
  public void putBytes(int rowId, byte[] bytes, int offset, int length) {
    throw new UnsupportedOperationException(
        "invalid data type: " + columnPageEncoderMeta.getStoreDataType());
  }

  @Override
  public void putDecimal(int rowId, BigDecimal decimal) {
    throw new UnsupportedOperationException(
        "invalid data type: " + columnPageEncoderMeta.getStoreDataType());
  }

  @Override
  public byte[] getDecimalPage() {
    throw new UnsupportedOperationException(
        "invalid data type: " + columnPageEncoderMeta.getStoreDataType());
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

  @Override public BigDecimal getDecimal(int rowId) {
    throw new UnsupportedOperationException(
        "invalid data type: " + columnPageEncoderMeta.getStoreDataType());
  }

  @Override
  public byte[] getBytes(int rowId) {
    return this.fixedLengthdata[rowId];
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
  @Override public byte[][] getByteArrayPage() {
    byte[][] data = new byte[arrayElementCount][];
    for (int i = 0; i < arrayElementCount; i++) {
      data[i] = fixedLengthdata[i];
    }
    return data;
  }

  @Override
  public byte[] getLVFlattenedBytePage() throws IOException {
    throw new UnsupportedOperationException(
        "invalid data type: " + columnPageEncoderMeta.getStoreDataType());
  }

  @Override
  public byte[] getComplexChildrenLVFlattenedBytePage() throws IOException {
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(stream);
    for (int i = 0; i < arrayElementCount; i++) {
      out.write(fixedLengthdata[i]);
    }
    return stream.toByteArray();
  }

  @Override
  public byte[] getComplexParentFlattenedBytePage() throws IOException {
    throw new UnsupportedOperationException("internal error");
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
  @Override public void setIntPage(int[] intData) {
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
    throw new UnsupportedOperationException(
        "invalid data type: " + columnPageEncoderMeta.getStoreDataType());
  }

  @Override
  public void freeMemory() {
    byteData = null;
    shortData = null;
    intData = null;
    longData = null;
    floatData = null;
    doubleData = null;
    shortIntData = null;
    fixedLengthdata = null;
  }

  /**
   * apply encoding to page data
   * @param codec type of transformation
   */
  @Override
  public void convertValue(ColumnPageValueConverter codec) {
    if (columnPageEncoderMeta.getStoreDataType() == DataTypes.BYTE) {
      for (int i = 0; i < arrayElementCount; i++) {
        codec.encode(i, byteData[i]);
      }
    } else if (columnPageEncoderMeta.getStoreDataType() == DataTypes.SHORT) {
      for (int i = 0; i < arrayElementCount; i++) {
        codec.encode(i, shortData[i]);
      }
    } else if (columnPageEncoderMeta.getStoreDataType() == DataTypes.INT) {
      for (int i = 0; i < arrayElementCount; i++) {
        codec.encode(i, intData[i]);
      }
    } else if (columnPageEncoderMeta.getStoreDataType() == DataTypes.LONG) {
      for (int i = 0; i < arrayElementCount; i++) {
        codec.encode(i, longData[i]);
      }
    } else if (columnPageEncoderMeta.getStoreDataType() == DataTypes.FLOAT) {
      for (int i = 0; i < arrayElementCount; i++) {
        codec.encode(i, floatData[i]);
      }
    } else if (columnPageEncoderMeta.getStoreDataType() == DataTypes.DOUBLE) {
      for (int i = 0; i < arrayElementCount; i++) {
        codec.encode(i, doubleData[i]);
      }
    } else {
      throw new UnsupportedOperationException("not support value conversion on "
          + columnPageEncoderMeta.getStoreDataType() + " page");
    }
  }

  private void ensureArraySize(int requestSize, DataType dataType) {
    if (dataType == DataTypes.BYTE) {
      if (requestSize >= byteData.length) {
        byte[] newArray = new byte[arrayElementCount * 2];
        System.arraycopy(byteData, 0, newArray, 0, arrayElementCount);
        byteData = newArray;
      }
    } else if (dataType == DataTypes.SHORT) {
      if (requestSize >= shortData.length) {
        short[] newArray = new short[arrayElementCount * 2];
        System.arraycopy(shortData, 0, newArray, 0, arrayElementCount);
        shortData = newArray;
      }
    } else if (dataType == DataTypes.SHORT_INT) {
      if (requestSize >= shortIntData.length / 3) {
        byte[] newArray = new byte[arrayElementCount * 6];
        System.arraycopy(shortIntData, 0, newArray, 0, arrayElementCount * 3);
        shortIntData = newArray;
      }
    } else if (dataType == DataTypes.INT) {
      if (requestSize >= intData.length) {
        int[] newArray = new int[arrayElementCount * 2];
        System.arraycopy(intData, 0, newArray, 0, arrayElementCount);
        intData = newArray;
      }
    } else if (dataType == DataTypes.LONG) {
      if (requestSize >= longData.length) {
        long[] newArray = new long[arrayElementCount * 2];
        System.arraycopy(longData, 0, newArray, 0, arrayElementCount);
        longData = newArray;
      }
    } else if (dataType == DataTypes.FLOAT) {
      if (requestSize >= floatData.length) {
        float[] newArray = new float[arrayElementCount * 2];
        System.arraycopy(floatData, 0, newArray, 0, arrayElementCount);
        floatData = newArray;
      }
    } else if (dataType == DataTypes.DOUBLE) {
      if (requestSize >= doubleData.length) {
        double[] newArray = new double[arrayElementCount * 2];
        System.arraycopy(doubleData, 0, newArray, 0, arrayElementCount);
        doubleData = newArray;
      }
    } else if (dataType == DataTypes.BYTE_ARRAY) {
      if (requestSize >= fixedLengthdata.length) {
        byte[][] newArray = new byte[arrayElementCount * 2][];
        int index = 0;
        for (byte[] data : fixedLengthdata) {
          newArray[index++] = data;
        }
        fixedLengthdata = newArray;
      }
    } else {
      throw new UnsupportedOperationException(
          "not support value conversion on " + dataType + " page");
    }
  }
  public int getActualRowCount() {
    return arrayElementCount;
  }

}
