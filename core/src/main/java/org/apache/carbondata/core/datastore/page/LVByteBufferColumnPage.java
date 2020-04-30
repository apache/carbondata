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
import java.nio.ByteBuffer;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.ColumnType;
import org.apache.carbondata.core.datastore.TableSpec;
import org.apache.carbondata.core.datastore.page.encoding.ColumnPageEncoderMeta;
import org.apache.carbondata.core.memory.CarbonUnsafe;
import org.apache.carbondata.core.memory.UnsafeMemoryManager;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;

import static org.apache.carbondata.core.datastore.page.VarLengthColumnPageBase.DEFAULT_ROW_SIZE;
import static org.apache.carbondata.core.datastore.page.VarLengthColumnPageBase.FACTOR;

import sun.nio.ch.DirectBuffer;

/**
 * ColumnPage implementation backed by a ByteBuffer
 * All data added in this page is encoded as LV (Length-Value)
 */
public class LVByteBufferColumnPage extends ColumnPage {

  // data of this page
  private ByteBuffer byteBuffer;

  // the offset of row in the unsafe memory, its size is pageSize + 1
  protected ColumnPage rowOffset;

  // number of rows in this page
  private int numRows;

  // the length of bytes added in the page
  protected int totalLength;

  LVByteBufferColumnPage(ColumnPageEncoderMeta columnPageEncoderMeta, int pageSize) {
    super(columnPageEncoderMeta, pageSize);
    checkDataType(columnPageEncoderMeta);
    TableSpec.ColumnSpec spec = TableSpec.ColumnSpec.newInstance(
        columnPageEncoderMeta.getColumnSpec().getFieldName(), DataTypes.INT, ColumnType.MEASURE);
    rowOffset = ColumnPage.newPage(
        new ColumnPageEncoderMeta(spec, DataTypes.INT, columnPageEncoderMeta.getCompressorName()),
        pageSize + 1);
    byteBuffer = ByteBuffer.allocateDirect((int)(pageSize * DEFAULT_ROW_SIZE * FACTOR));
    numRows = 0;
    totalLength = 0;
  }

  private void checkDataType(ColumnPageEncoderMeta columnPageEncoderMeta) {
    DataType dataType = columnPageEncoderMeta.getStoreDataType();
    if (dataType != DataTypes.STRING &&
        dataType != DataTypes.VARCHAR &&
        dataType != DataTypes.BINARY) {
      throw new UnsupportedOperationException("Unsupported data type: " + dataType);
    }
  }

  /**
   * This putBytes different from others in that it writes LV (Length-Value) encoded data
   * The L part is based on the data type
   */
  @Override
  public void putBytes(int rowId, byte[] bytes) {
    // since it is variable length, we need to prepare each
    // element as LV result byte array (first two/four bytes are the length of the array)
    int requiredLength;
    DataType dataType = getDataType();
    if (dataType == DataTypes.STRING) {
      requiredLength = bytes.length + CarbonCommonConstants.SHORT_SIZE_IN_BYTE;
      ensureMaxLengthForString(requiredLength);
    } else if (dataType == DataTypes.VARCHAR || dataType == DataTypes.BINARY) {
      requiredLength = bytes.length + CarbonCommonConstants.INT_SIZE_IN_BYTE;
    } else {
      throw new UnsupportedOperationException("unsupported data type: " + dataType);
    }

    // ensure the byte buffer has enough capacity
    ensureMemory(requiredLength);

    if (dataType == DataTypes.STRING) {
      byteBuffer.putShort((short)bytes.length);
    } else {
      byteBuffer.putInt(bytes.length);
    }
    byteBuffer.put(bytes);

    if (rowId == 0) {
      rowOffset.putInt(0, 0);
    }
    rowOffset.putInt(rowId + 1, rowOffset.getInt(rowId) + requiredLength);
    totalLength += requiredLength;
    numRows++;
  }

  /**
   * reallocate byte buffer if capacity length than current size + request size
   */
  protected void ensureMemory(int requestSize) {
    int capacity = byteBuffer.capacity();
    if (totalLength + requestSize > capacity) {
      int newSize = Math.max(2 * capacity, totalLength + requestSize);
      ByteBuffer newBuffer = ByteBuffer.allocateDirect(newSize);
      CarbonUnsafe.getUnsafe().copyMemory(((DirectBuffer)byteBuffer).address(),
          ((DirectBuffer)newBuffer).address(), capacity);
      newBuffer.position(byteBuffer.position());
      UnsafeMemoryManager.destroyDirectByteBuffer(byteBuffer);
      byteBuffer = newBuffer;
    }
  }

  // since we later store a column page in a byte array, so its maximum size is 2GB
  private void ensureMaxLengthForString(int requiredLength) {
    if (requiredLength > Short.MAX_VALUE) {
      throw new RuntimeException("input data length " + requiredLength +
          " bytes too long, maximum length supported is " + Short.MAX_VALUE + " bytes");
    }
  }

  @Override
  public ByteBuffer getByteBuffer() {
    return byteBuffer;
  }

  @Override
  public byte[][] getByteArrayPage() {
    byte[][] output = new byte[numRows][];
    ByteBuffer buffer = byteBuffer.asReadOnlyBuffer();
    for (int rowId = 0; rowId < numRows; rowId++) {
      int offset = rowOffset.getInt(rowId);
      int length = rowOffset.getInt(rowId + 1) - offset;
      buffer.position(offset);
      output[rowId] = new byte[length];
      buffer.get(output[rowId]);
    }
    return output;
  }

  @Override
  public byte[] getLVFlattenedBytePage() {
    // output LV encoded byte array
    byte[] data = new byte[byteBuffer.position()];
    ByteBuffer buffer = byteBuffer.asReadOnlyBuffer();
    buffer.flip();
    buffer.get(data);
    return data;
  }

  @Override
  public byte[] getComplexChildrenLVFlattenedBytePage(DataType dataType) {
    // output LV encoded byte array
    byte[] data = new byte[byteBuffer.position()];
    ByteBuffer buffer = byteBuffer.asReadOnlyBuffer();
    buffer.flip();
    buffer.get(data);
    return data;
  }

  @Override
  public byte[] getComplexParentFlattenedBytePage() {
    int outputOffset = 0;
    byte[] output = new byte[totalLength];
    ByteBuffer buffer = byteBuffer.asReadOnlyBuffer();
    buffer.flip();
    for (int rowId = 0; rowId < numRows; rowId++) {
      int offset = rowOffset.getInt(rowId);
      int length =  (rowOffset.getInt(rowId + 1) - rowOffset.getInt(rowId));
      int readLength;
      DataType dataType = getDataType();
      if (dataType == DataTypes.STRING) {
        buffer.position(offset + CarbonCommonConstants.SHORT_SIZE_IN_BYTE);
        readLength = length - CarbonCommonConstants.SHORT_SIZE_IN_BYTE;
      } else {
        buffer.position(offset + CarbonCommonConstants.INT_SIZE_IN_BYTE);
        readLength = length - CarbonCommonConstants.INT_SIZE_IN_BYTE;
      }
      buffer.get(output, outputOffset, readLength);
      outputOffset += readLength;
    }
    return output;
  }

  @Override
  public long getPageLengthInBytes() {
    return totalLength;
  }

  @Override
  public void convertValue(ColumnPageValueConverter codec) {
    throw new UnsupportedOperationException(
        "invalid data type: " + columnPageEncoderMeta.getStoreDataType());
  }

  @Override
  public void setBytePage(byte[] byteData) {
    throw new UnsupportedOperationException(
        "invalid data type: " + columnPageEncoderMeta.getStoreDataType());
  }

  @Override
  public void setShortPage(short[] shortData) {
    throw new UnsupportedOperationException(
        "invalid data type: " + columnPageEncoderMeta.getStoreDataType());
  }

  @Override
  public void setShortIntPage(byte[] shortIntData) {
    throw new UnsupportedOperationException(
        "invalid data type: " + columnPageEncoderMeta.getStoreDataType());
  }

  @Override
  public void setIntPage(int[] intData) {
    throw new UnsupportedOperationException(
        "invalid data type: " + columnPageEncoderMeta.getStoreDataType());
  }

  @Override
  public void setLongPage(long[] longData) {
    throw new UnsupportedOperationException(
        "invalid data type: " + columnPageEncoderMeta.getStoreDataType());
  }

  @Override
  public void setFloatPage(float[] floatData) {
    throw new UnsupportedOperationException(
        "invalid data type: " + columnPageEncoderMeta.getStoreDataType());
  }

  @Override
  public void setDoublePage(double[] doubleData) {
    throw new UnsupportedOperationException(
        "invalid data type: " + columnPageEncoderMeta.getStoreDataType());
  }

  @Override
  public void putBytes(int rowId, byte[] bytes, int offset, int length) {
    throw new UnsupportedOperationException("Operation not supported");
  }

  @Override
  public byte getByte(int rowId) {
    throw new UnsupportedOperationException(
        "invalid data type: " + columnPageEncoderMeta.getStoreDataType());
  }

  @Override
  public short getShort(int rowId) {
    throw new UnsupportedOperationException(
        "invalid data type: " + columnPageEncoderMeta.getStoreDataType());
  }

  @Override
  public int getShortInt(int rowId) {
    throw new UnsupportedOperationException(
        "invalid data type: " + columnPageEncoderMeta.getStoreDataType());
  }

  @Override
  public int getInt(int rowId) {
    throw new UnsupportedOperationException(
        "invalid data type: " + columnPageEncoderMeta.getStoreDataType());
  }

  @Override
  public long getLong(int rowId) {
    throw new UnsupportedOperationException(
        "invalid data type: " + columnPageEncoderMeta.getStoreDataType());
  }

  @Override
  public float getFloat(int rowId) {
    throw new UnsupportedOperationException(
        "invalid data type: " + columnPageEncoderMeta.getStoreDataType());
  }

  @Override
  public double getDouble(int rowId) {
    throw new UnsupportedOperationException(
        "invalid data type: " + columnPageEncoderMeta.getStoreDataType());
  }

  @Override
  public byte[] getBytePage() {
    throw new UnsupportedOperationException(
        "invalid data type: " + columnPageEncoderMeta.getStoreDataType());
  }

  @Override
  public short[] getShortPage() {
    throw new UnsupportedOperationException(
        "invalid data type: " + columnPageEncoderMeta.getStoreDataType());
  }

  @Override
  public byte[] getShortIntPage() {
    throw new UnsupportedOperationException(
        "invalid data type: " + columnPageEncoderMeta.getStoreDataType());
  }

  @Override
  public int[] getIntPage() {
    throw new UnsupportedOperationException(
        "invalid data type: " + columnPageEncoderMeta.getStoreDataType());
  }

  @Override
  public long[] getLongPage() {
    throw new UnsupportedOperationException(
        "invalid data type: " + columnPageEncoderMeta.getStoreDataType());
  }

  @Override
  public float[] getFloatPage() {
    throw new UnsupportedOperationException(
        "invalid data type: " + columnPageEncoderMeta.getStoreDataType());
  }

  @Override
  public double[] getDoublePage() {
    throw new UnsupportedOperationException(
        "invalid data type: " + columnPageEncoderMeta.getStoreDataType());
  }

  @Override
  public byte[] getDecimalPage() {
    throw new UnsupportedOperationException(
        "invalid data type: " + columnPageEncoderMeta.getStoreDataType());
  }

  @Override
  public byte[] getBytes(int rowId) {
    int offset = rowOffset.getInt(rowId);
    int length = rowOffset.getInt(rowId + 1) - rowOffset.getInt(rowId);
    byte[] data = new byte[length];
    ByteBuffer duplicated = byteBuffer.asReadOnlyBuffer();
    duplicated.position(offset);
    duplicated.limit(offset + length);
    duplicated.get(data);
    return data;
  }

  @Override
  public void putDecimal(int rowId, BigDecimal decimal) {
    throw new UnsupportedOperationException("Operation not supported");
  }

  @Override
  public BigDecimal getDecimal(int rowId) {
    throw new UnsupportedOperationException("Operation not supported");
  }

  @Override
  public void setByteArrayPage(byte[][] byteArray) {
    throw new UnsupportedOperationException("Operation not supported");
  }

  @Override
  public void freeMemory() {
    if (null != rowOffset) {
      rowOffset.freeMemory();
      rowOffset = null;
    }
    if (null != byteBuffer) {
      UnsafeMemoryManager.destroyDirectByteBuffer(byteBuffer);
    }
  }

  @Override
  public void putByte(int rowId, byte value) {
    throw new UnsupportedOperationException(
        "invalid data type: " + columnPageEncoderMeta.getStoreDataType());
  }

  @Override
  public void putShort(int rowId, short value) {
    throw new UnsupportedOperationException(
        "invalid data type: " + columnPageEncoderMeta.getStoreDataType());
  }

  @Override
  public void putShortInt(int rowId, int value) {
    throw new UnsupportedOperationException(
        "invalid data type: " + columnPageEncoderMeta.getStoreDataType());
  }

  @Override
  public void putInt(int rowId, int value) {
    throw new UnsupportedOperationException(
        "invalid data type: " + columnPageEncoderMeta.getStoreDataType());
  }

  @Override
  public void putLong(int rowId, long value) {
    throw new UnsupportedOperationException(
        "invalid data type: " + columnPageEncoderMeta.getStoreDataType());
  }

  @Override
  public void putDouble(int rowId, double value) {
    throw new UnsupportedOperationException(
        "invalid data type: " + columnPageEncoderMeta.getStoreDataType());
  }

  @Override
  public void putFloat(int rowId, float value) {
    throw new UnsupportedOperationException(
        "invalid data type: " + columnPageEncoderMeta.getStoreDataType());
  }
}
