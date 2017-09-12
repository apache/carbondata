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
import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DecimalConverterFactory;
import org.apache.carbondata.core.util.ByteUtil;

import static org.apache.carbondata.core.metadata.datatype.DataType.DECIMAL;

public abstract class VarLengthColumnPageBase extends ColumnPage {

  // the offset of row in the unsafe memory, its size is pageSize + 1
  int[] rowOffset;

  // the length of bytes added in the page
  int totalLength;

  VarLengthColumnPageBase(DataType dataType, int pageSize, int scale, int precision) {
    super(dataType, pageSize, scale, precision);
    rowOffset = new int[pageSize + 1];
    totalLength = 0;
  }

  @Override
  public void setBytePage(byte[] byteData) {
    throw new UnsupportedOperationException("invalid data type: " + dataType);
  }

  @Override
  public void setShortPage(short[] shortData) {
    throw new UnsupportedOperationException("invalid data type: " + dataType);
  }

  @Override
  public void setShortIntPage(byte[] shortIntData) {
    throw new UnsupportedOperationException("invalid data type: " + dataType);
  }

  @Override
  public void setIntPage(int[] intData) {
    throw new UnsupportedOperationException("invalid data type: " + dataType);
  }

  @Override
  public void setLongPage(long[] longData) {
    throw new UnsupportedOperationException("invalid data type: " + dataType);
  }

  @Override
  public void setFloatPage(float[] floatData) {
    throw new UnsupportedOperationException("invalid data type: " + dataType);
  }

  @Override
  public void setDoublePage(double[] doubleData) {
    throw new UnsupportedOperationException("invalid data type: " + dataType);
  }

  /**
   * Create a new column page for decimal page
   */
  static ColumnPage newDecimalColumnPage(byte[] lvEncodedBytes, int scale, int precision)
      throws MemoryException {
    DecimalConverterFactory.DecimalConverter decimalConverter =
        DecimalConverterFactory.INSTANCE.getDecimalConverter(precision, scale);
    int size = decimalConverter.getSize();
    if (size < 0) {
      return getLVBytesColumnPage(lvEncodedBytes, DataType.DECIMAL);
    } else {
      // Here the size is always fixed.
      return getDecimalColumnPage(lvEncodedBytes, scale, precision, size);
    }
  }

  /**
   * Create a new column page based on the LV (Length Value) encoded bytes
   */
  static ColumnPage newLVBytesColumnPage(byte[] lvEncodedBytes)
      throws MemoryException {
    return getLVBytesColumnPage(lvEncodedBytes, DataType.BYTE_ARRAY);
  }

  private static ColumnPage getDecimalColumnPage(byte[] lvEncodedBytes, int scale, int precision,
      int size) throws MemoryException {
    List<Integer> rowOffset = new ArrayList<>();
    int offset;
    int rowId = 0;
    for (offset = 0; offset < lvEncodedBytes.length; offset += size) {
      rowOffset.add(offset);
      rowId++;
    }
    rowOffset.add(offset);

    VarLengthColumnPageBase page;
    if (unsafe) {
      page = new UnsafeVarLengthColumnPage(DECIMAL, rowId, scale, precision);
    } else {
      page = new SafeVarLengthColumnPage(DECIMAL, rowId, scale, precision);
    }

    // set total length and rowOffset in page
    page.totalLength = offset;
    page.rowOffset = new int[rowId + 1];
    for (int i = 0; i < rowId + 1; i++) {
      page.rowOffset[i] = rowOffset.get(i);
    }
    for (int i = 0; i < rowId; i++) {
      page.putBytes(i, lvEncodedBytes, i * size, size);
    }
    return page;
  }

  private static ColumnPage getLVBytesColumnPage(byte[] lvEncodedBytes, DataType dataType)
      throws MemoryException {
    // extract length and data, set them to rowOffset and unsafe memory correspondingly
    int rowId = 0;
    List<Integer> rowOffset = new ArrayList<>();
    List<Integer> rowLength = new ArrayList<>();
    int length;
    int offset;
    int lvEncodedOffset = 0;

    // extract Length field in input and calculate total length
    for (offset = 0; lvEncodedOffset < lvEncodedBytes.length; offset += length) {
      length = ByteUtil.toInt(lvEncodedBytes, lvEncodedOffset);
      rowOffset.add(offset);
      rowLength.add(length);
      lvEncodedOffset += 4 + length;
      rowId++;
    }
    rowOffset.add(offset);

    int numRows = rowId;

    VarLengthColumnPageBase page;
    int inputDataLength = offset;
    if (unsafe) {
      page = new UnsafeVarLengthColumnPage(DECIMAL, numRows, inputDataLength, -1, -1);
    } else {
      page = new SafeVarLengthColumnPage(dataType, numRows, -1, -1);
    }

    // set total length and rowOffset in page
    page.totalLength = offset;
    page.rowOffset = new int[rowId + 1];
    for (int i = 0; i < rowId + 1; i++) {
      page.rowOffset[i] = rowOffset.get(i);
    }

    // set data in page
    lvEncodedOffset = 0;
    for (int i = 0; i < numRows; i++) {
      length = rowLength.get(i);
      page.putBytes(i, lvEncodedBytes, lvEncodedOffset + 4, length);
      lvEncodedOffset += 4 + length;
    }

    return page;
  }

  @Override
  public void putByte(int rowId, byte value) {
    throw new UnsupportedOperationException("invalid data type: " + dataType);
  }

  @Override
  public void putShort(int rowId, short value) {
    throw new UnsupportedOperationException("invalid data type: " + dataType);
  }

  @Override
  public void putShortInt(int rowId, int value) {
    throw new UnsupportedOperationException("invalid data type: " + dataType);
  }

  @Override
  public void putInt(int rowId, int value) {
    throw new UnsupportedOperationException("invalid data type: " + dataType);
  }

  @Override
  public void putLong(int rowId, long value) {
    throw new UnsupportedOperationException("invalid data type: " + dataType);
  }

  @Override
  public void putDouble(int rowId, double value) {
    throw new UnsupportedOperationException("invalid data type: " + dataType);
  }

  abstract void putBytesAtRow(int rowId, byte[] bytes);

  @Override
  public void putBytes(int rowId, byte[] bytes) {
    if (rowId == 0) {
      rowOffset[0] = 0;
    }
    rowOffset[rowId + 1] = rowOffset[rowId] + bytes.length;
    putBytesAtRow(rowId, bytes);
    totalLength += bytes.length;
  }

  @Override
  public byte getByte(int rowId) {
    throw new UnsupportedOperationException("invalid data type: " + dataType);
  }

  @Override
  public short getShort(int rowId) {
    throw new UnsupportedOperationException("invalid data type: " + dataType);
  }

  @Override
  public int getShortInt(int rowId) {
    throw new UnsupportedOperationException("invalid data type: " + dataType);
  }

  @Override
  public int getInt(int rowId) {
    throw new UnsupportedOperationException("invalid data type: " + dataType);
  }

  @Override
  public long getLong(int rowId) {
    throw new UnsupportedOperationException("invalid data type: " + dataType);
  }

  @Override
  public float getFloat(int rowId) {
    throw new UnsupportedOperationException("invalid data type: " + dataType);
  }

  @Override
  public double getDouble(int rowId) {
    throw new UnsupportedOperationException("invalid data type: " + dataType);
  }

  @Override
  public byte[] getBytePage() {
    throw new UnsupportedOperationException("invalid data type: " + dataType);
  }

  @Override
  public short[] getShortPage() {
    throw new UnsupportedOperationException("invalid data type: " + dataType);
  }

  @Override
  public byte[] getShortIntPage() {
    throw new UnsupportedOperationException("invalid data type: " + dataType);
  }

  @Override
  public int[] getIntPage() {
    throw new UnsupportedOperationException("invalid data type: " + dataType);
  }

  @Override
  public long[] getLongPage() {
    throw new UnsupportedOperationException("invalid data type: " + dataType);
  }

  @Override
  public float[] getFloatPage() {
    throw new UnsupportedOperationException("invalid data type: " + dataType);
  }

  @Override
  public double[] getDoublePage() {
    throw new UnsupportedOperationException("invalid data type: " + dataType);
  }

  @Override
  public byte[] getDecimalPage() {
    // output LV encoded byte array
    int offset = 0;
    byte[] data = new byte[totalLength];
    for (int rowId = 0; rowId < pageSize; rowId++) {
      int length = rowOffset[rowId + 1] - rowOffset[rowId];
      copyBytes(rowId, data, offset, length);
      offset += length;
    }
    return data;
  }

  /**
   * Copy `length` bytes from data at rowId to dest start from destOffset
   */
  abstract void copyBytes(int rowId, byte[] dest, int destOffset, int length);

  @Override
  public byte[] getLVFlattenedBytePage() throws IOException {
    // output LV encoded byte array
    int offset = 0;
    byte[] data = new byte[totalLength + pageSize * 4];
    for (int rowId = 0; rowId < pageSize; rowId++) {
      int length = rowOffset[rowId + 1] - rowOffset[rowId];
      ByteUtil.setInt(data, offset, length);
      copyBytes(rowId, data, offset + 4, length);
      offset += 4 + length;
    }
    return data;
  }

  @Override
  public void convertValue(ColumnPageValueConverter codec) {
    throw new UnsupportedOperationException("invalid data type: " + dataType);
  }
}
