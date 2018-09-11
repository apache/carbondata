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

import org.apache.carbondata.core.datastore.page.encoding.ColumnPageEncoderMeta;
import org.apache.carbondata.core.memory.CarbonUnsafe;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.memory.UnsafeMemoryManager;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.util.ByteUtil;

/**
 * Represents a columnar data for decimal data type column for one page
 */
public class UnsafeDecimalColumnPage extends DecimalColumnPage {

  UnsafeDecimalColumnPage(ColumnPageEncoderMeta columnPageEncoderMeta, int pageSize)
      throws MemoryException {
    this(columnPageEncoderMeta, pageSize, (int) (pageSize * DEFAULT_ROW_SIZE * FACTOR));
  }

  UnsafeDecimalColumnPage(ColumnPageEncoderMeta columnPageEncoderMeta, int pageSize, int capacity)
      throws MemoryException {
    super(columnPageEncoderMeta, pageSize);
    this.capacity = capacity;
    initMemory();
  }

  private void initMemory() throws MemoryException {
    if (columnPageEncoderMeta.getStoreDataType() == DataTypes.BYTE ||
        columnPageEncoderMeta.getStoreDataType() == DataTypes.SHORT ||
        columnPageEncoderMeta.getStoreDataType() == DataTypes.INT ||
        columnPageEncoderMeta.getStoreDataType() == DataTypes.LONG) {
      int size = pageSize << columnPageEncoderMeta.getStoreDataType().getSizeBits();
      memoryBlock = UnsafeMemoryManager.allocateMemoryWithRetry(taskId, size);
    } else if (columnPageEncoderMeta.getStoreDataType() == DataTypes.SHORT_INT) {
      int size = pageSize * 3;
      memoryBlock = UnsafeMemoryManager.allocateMemoryWithRetry(taskId, size);
    } else if (DataTypes.isDecimal(columnPageEncoderMeta.getStoreDataType())) {
      memoryBlock = UnsafeMemoryManager.allocateMemoryWithRetry(taskId, (long) (capacity));
    } else if (columnPageEncoderMeta.getStoreDataType() == DataTypes.BYTE_ARRAY) {
      memoryBlock = UnsafeMemoryManager.allocateMemoryWithRetry(taskId, (long) (capacity));
    } else {
      throw new UnsupportedOperationException(
          "invalid data type: " + columnPageEncoderMeta.getStoreDataType());
    }
    baseAddress = memoryBlock.getBaseObject();
    baseOffset = memoryBlock.getBaseOffset();
  }

  @Override
  public void setBytePage(byte[] byteData) {
    CarbonUnsafe.getUnsafe()
        .copyMemory(byteData, CarbonUnsafe.BYTE_ARRAY_OFFSET, baseAddress, baseOffset,
            byteData.length << byteBits);
  }

  @Override
  public void setShortPage(short[] shortData) {
    CarbonUnsafe.getUnsafe()
        .copyMemory(shortData, CarbonUnsafe.SHORT_ARRAY_OFFSET, baseAddress, baseOffset,
            shortData.length << shortBits);
  }

  @Override
  public void setShortIntPage(byte[] shortIntData) {
    CarbonUnsafe.getUnsafe()
        .copyMemory(shortIntData, CarbonUnsafe.BYTE_ARRAY_OFFSET, baseAddress, baseOffset,
            shortIntData.length);
  }

  @Override
  public void setIntPage(int[] intData) {
    CarbonUnsafe.getUnsafe()
        .copyMemory(intData, CarbonUnsafe.INT_ARRAY_OFFSET, baseAddress, baseOffset,
            intData.length << intBits);
  }

  @Override
  public void setLongPage(long[] longData) {
    CarbonUnsafe.getUnsafe()
        .copyMemory(longData, CarbonUnsafe.LONG_ARRAY_OFFSET, baseAddress, baseOffset,
            longData.length << longBits);
  }

  @Override
  public void setByteArrayPage(byte[][] byteArray) {
    if (totalLength != 0) {
      throw new IllegalStateException("page is not empty");
    }
    for (int i = 0; i < byteArray.length; i++) {
      putBytes(i, byteArray[i]);
    }
  }

  @Override
  public void freeMemory() {
    if (memoryBlock != null) {
      UnsafeMemoryManager.INSTANCE.freeMemory(taskId, memoryBlock);
      memoryBlock = null;
      baseAddress = null;
      baseOffset = 0;
      super.freeMemory();
    }
  }

  @Override
  public void putByte(int rowId, byte value) {
    long offset = (long)rowId << byteBits;
    CarbonUnsafe.getUnsafe().putByte(baseAddress, baseOffset + offset, value);
  }

  @Override
  public void putShort(int rowId, short value) {
    long offset = (long)rowId << shortBits;
    CarbonUnsafe.getUnsafe().putShort(baseAddress, baseOffset + offset, value);
  }

  @Override
  public void putShortInt(int rowId, int value) {
    byte[] data = ByteUtil.to3Bytes(value);
    long offset = rowId * 3L;
    CarbonUnsafe.getUnsafe().putByte(baseAddress, baseOffset + offset, data[0]);
    CarbonUnsafe.getUnsafe().putByte(baseAddress, baseOffset + offset + 1, data[1]);
    CarbonUnsafe.getUnsafe().putByte(baseAddress, baseOffset + offset + 2, data[2]);
  }

  @Override
  public void putInt(int rowId, int value) {
    long offset = (long)rowId << intBits;
    CarbonUnsafe.getUnsafe().putInt(baseAddress, baseOffset + offset, value);
  }

  @Override
  public void putLong(int rowId, long value) {
    long offset = (long)rowId << longBits;
    CarbonUnsafe.getUnsafe().putLong(baseAddress, baseOffset + offset, value);
  }

  @Override
  public void putBytesAtRow(int rowId, byte[] bytes) {
    putBytes(rowId, bytes, 0, bytes.length);
  }

  @Override
  public void putBytes(int rowId, byte[] bytes, int offset, int length) {
    try {
      ensureMemory(length);
    } catch (MemoryException e) {
      throw new RuntimeException(e);
    }
    CarbonUnsafe.getUnsafe().copyMemory(bytes, CarbonUnsafe.BYTE_ARRAY_OFFSET + offset, baseAddress,
        baseOffset + rowOffset.getInt(rowId), length);
  }

  @Override
  public void putDecimal(int rowId, BigDecimal decimal) {
    switch (decimalConverter.getDecimalConverterType()) {
      case DECIMAL_INT:
        putInt(rowId, (int) decimalConverter.convert(decimal));
        break;
      case DECIMAL_LONG:
        putLong(rowId, (long) decimalConverter.convert(decimal));
        break;
      default:
        putBytes(rowId, (byte[]) decimalConverter.convert(decimal));
    }
  }

  @Override
  public byte getByte(int rowId) {
    long offset = (long)rowId << byteBits;
    return CarbonUnsafe.getUnsafe().getByte(baseAddress, baseOffset + offset);
  }

  @Override
  public byte[] getBytes(int rowId) {
    int length = rowOffset.getInt(rowId + 1) - rowOffset.getInt(rowId);
    byte[] bytes = new byte[length];
    CarbonUnsafe.getUnsafe().copyMemory(baseAddress, baseOffset + rowOffset.getInt(rowId),
        bytes, CarbonUnsafe.BYTE_ARRAY_OFFSET, length);
    return bytes;
  }

  @Override
  public short getShort(int rowId) {
    long offset = (long) rowId << shortBits;
    return CarbonUnsafe.getUnsafe().getShort(baseAddress, baseOffset + offset);
  }

  @Override
  public int getShortInt(int rowId) {
    long offset = rowId * 3L;
    byte[] data = new byte[3];
    data[0] = CarbonUnsafe.getUnsafe().getByte(baseAddress, baseOffset + offset);
    data[1] = CarbonUnsafe.getUnsafe().getByte(baseAddress, baseOffset + offset + 1);
    data[2] = CarbonUnsafe.getUnsafe().getByte(baseAddress, baseOffset + offset + 2);
    return ByteUtil.valueOf3Bytes(data, 0);
  }

  @Override
  public int getInt(int rowId) {
    long offset = (long)rowId << intBits;
    return CarbonUnsafe.getUnsafe().getInt(baseAddress, baseOffset + offset);
  }

  @Override
  public long getLong(int rowId) {
    long offset = (long) rowId << longBits;
    return CarbonUnsafe.getUnsafe().getLong(baseAddress, baseOffset + offset);
  }

  @Override
  void copyBytes(int rowId, byte[] dest, int destOffset, int length) {
    CarbonUnsafe.getUnsafe().copyMemory(baseAddress, baseOffset + rowOffset.getInt(rowId), dest,
        CarbonUnsafe.BYTE_ARRAY_OFFSET + destOffset, length);
  }

  @Override
  public void convertValue(ColumnPageValueConverter codec) {
    convertValueForDecimalType(codec);
  }

  private void convertValueForDecimalType(ColumnPageValueConverter codec) {
    switch (decimalConverter.getDecimalConverterType()) {
      case DECIMAL_INT:
        for (int i = 0; i < pageSize; i++) {
          long offset = (long)i << intBits;
          codec.encode(i, CarbonUnsafe.getUnsafe().getInt(baseAddress, baseOffset + offset));
        }
        break;
      case DECIMAL_LONG:
        for (int i = 0; i < pageSize; i++) {
          long offset = (long)i << longBits;
          codec.encode(i, CarbonUnsafe.getUnsafe().getLong(baseAddress, baseOffset + offset));
        }
        break;
      default:
        throw new UnsupportedOperationException("not support value conversion on "
            + columnPageEncoderMeta.getStoreDataType() + " page");
    }
  }

}
