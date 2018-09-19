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

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;

import org.apache.carbondata.core.datastore.page.encoding.ColumnPageEncoderMeta;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.util.ByteUtil;

/**
 * Represent a columnar data in one page for one column.
 */
public class SafeFixLengthColumnPage extends ColumnPage {

  // a buffer to hold all the records in flatten bytes
  private ByteBuffer flattenBytesBuffer;
  private byte[] flattenContentInBytes;

  private int eachRowSize;
  // total number of entries in array
  private int arrayElementCount = 0;

  SafeFixLengthColumnPage(ColumnPageEncoderMeta columnPageEncoderMeta, int pageSize,
      int eachRowSize) {
    super(columnPageEncoderMeta, pageSize);
    this.eachRowSize = eachRowSize;
    this.flattenContentInBytes = new byte[pageSize * this.eachRowSize];
    this.flattenBytesBuffer = ByteBuffer.wrap(flattenContentInBytes);
  }

  /**
   * Set byte value at rowId
   */
  @Override
  public void putByte(int rowId, byte value) {
    flattenBytesBuffer.put(value);
    arrayElementCount++;
  }

  /**
   * Set short value at rowId
   */
  @Override
  public void putShort(int rowId, short value) {
    flattenBytesBuffer.putShort(value);
    arrayElementCount++;
  }

  /**
   * Set integer value at rowId
   */
  @Override
  public void putInt(int rowId, int value) {
    flattenBytesBuffer.putInt(value);
    arrayElementCount++;
  }

  /**
   * Set long value at rowId
   */
  @Override
  public void putLong(int rowId, long value) {
    flattenBytesBuffer.putLong(value);
    arrayElementCount++;
  }

  /**
   * Set double value at rowId
   */
  @Override
  public void putDouble(int rowId, double value) {
    flattenBytesBuffer.putDouble(value);
    arrayElementCount++;
  }

  /**
   * Set float value at rowId
   */
  @Override
  public void putFloat(int rowId, float value) {
    flattenBytesBuffer.putFloat(value);
    arrayElementCount++;
  }

  /**
   * Set string value at rowId
   */
  @Override
  public void putBytes(int rowId, byte[] bytes) {
    // todo: to handle varchar
    flattenBytesBuffer.putShort((short) bytes.length);
    flattenBytesBuffer.put(bytes);
    arrayElementCount++;
  }

  @Override
  public void putShortInt(int rowId, int value) {
    byte[] converted = ByteUtil.to3Bytes(value);
    flattenBytesBuffer.put(converted);
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
    return flattenBytesBuffer.get(rowId);
  }

  /**
   * Get short value at rowId
   */
  @Override
  public short getShort(int rowId) {
    return flattenBytesBuffer.getShort(rowId * eachRowSize);
  }

  /**
   * Get short int value at rowId
   */
  @Override
  public int getShortInt(int rowId) {
    return ByteUtil.valueOf3Bytes(flattenBytesBuffer.array(), rowId * eachRowSize);
  }

  /**
   * Get int value at rowId
   */
  @Override
  public int getInt(int rowId) {
    return flattenBytesBuffer.getInt(rowId * eachRowSize);
  }

  /**
   * Get long value at rowId
   */
  @Override
  public long getLong(int rowId) {
    return flattenBytesBuffer.getLong(rowId * eachRowSize);
  }

  /**
   * Get float value at rowId
   */
  @Override
  public float getFloat(int rowId) {
    return flattenBytesBuffer.getFloat(rowId * eachRowSize);
  }

  /**
   * Get double value at rowId
   */
  @Override
  public double getDouble(int rowId) {
    return flattenBytesBuffer.getDouble(rowId * eachRowSize);
  }

  @Override public BigDecimal getDecimal(int rowId) {
    throw new UnsupportedOperationException(
        "invalid data type: " + columnPageEncoderMeta.getStoreDataType());
  }

  @Override
  public byte[] getBytes(int rowId) {
    for (int i = 0; i < rowId; i++) {
      short length = flattenBytesBuffer.getShort();
      flattenBytesBuffer.position(flattenBytesBuffer.position() + length);
    }
    short length = flattenBytesBuffer.getShort();
    // todo: try to avoid this allocation
    byte[] rtn = new byte[length];
    flattenBytesBuffer.get(rtn);
    return rtn;
  }

  /**
   * Get string page
   */
  @Override public byte[][] getByteArrayPage() {
    byte[][] data = new byte[arrayElementCount][];
    for (int i = 0; i < arrayElementCount; i++) {
      short length = flattenBytesBuffer.getShort();
      data[i] = new byte[length];
      flattenBytesBuffer.get(data[i]);
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
    return flattenBytesBuffer.array();
  }

  @Override
  public byte[] getComplexParentFlattenedBytePage() throws IOException {
    throw new UnsupportedOperationException("internal error");
  }

  @Override
  public void setFlattenContentInBytes(byte[] flattenContentInBytes) {
    this.flattenContentInBytes = flattenContentInBytes;
    this.flattenBytesBuffer = ByteBuffer.wrap(flattenContentInBytes);
    this.arrayElementCount = flattenContentInBytes.length / eachRowSize;
  }

  @Override
  public byte[] getFlattenContentInBytes() {
    return flattenContentInBytes;
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
    flattenBytesBuffer.clear();
    flattenContentInBytes = null;
  }

  /**
   * apply encoding to page data
   * @param codec type of transformation
   */
  @Override
  public void convertValue(ColumnPageValueConverter codec) {
    if (columnPageEncoderMeta.getStoreDataType() == DataTypes.BYTE) {
      for (int i = 0; i < arrayElementCount; i++) {
        codec.encode(i, getByte(i));
      }
    } else if (columnPageEncoderMeta.getStoreDataType() == DataTypes.SHORT) {
      for (int i = 0; i < arrayElementCount; i++) {
        codec.encode(i, getShort(i));
      }
    } else if (columnPageEncoderMeta.getStoreDataType() == DataTypes.INT) {
      for (int i = 0; i < arrayElementCount; i++) {
        codec.encode(i, getInt(i));
      }
    } else if (columnPageEncoderMeta.getStoreDataType() == DataTypes.LONG) {
      for (int i = 0; i < arrayElementCount; i++) {
        codec.encode(i, getLong(i));
      }
    } else if (columnPageEncoderMeta.getStoreDataType() == DataTypes.FLOAT) {
      for (int i = 0; i < arrayElementCount; i++) {
        codec.encode(i, getFloat(i));
      }
    } else if (columnPageEncoderMeta.getStoreDataType() == DataTypes.DOUBLE) {
      for (int i = 0; i < arrayElementCount; i++) {
        codec.encode(i, getDouble(i));
      }
    } else {
      throw new UnsupportedOperationException("not support value conversion on "
          + columnPageEncoderMeta.getStoreDataType() + " page");
    }
  }

  public int getActualRowCount() {
    return arrayElementCount;
  }

}
