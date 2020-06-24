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

import org.apache.carbondata.core.datastore.page.encoding.ColumnPageEncoderMeta;
import org.apache.carbondata.core.memory.CarbonUnsafe;
import org.apache.carbondata.core.memory.MemoryBlock;
import org.apache.carbondata.core.memory.UnsafeMemoryManager;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.util.ByteUtil;
import org.apache.carbondata.core.util.ThreadLocalTaskInfo;

import sun.nio.ch.DirectBuffer;

// This extension uses unsafe memory to store page data, for fix length data type only (byte,
// short, integer, long, float, double)
public class UnsafeFixLengthColumnPage extends ColumnPage {
  // memory allocated by Unsafe
  private MemoryBlock memoryBlock;

  // base address of memoryBlock
  private Object baseAddress;

  // base offset of memoryBlock
  private long baseOffset;

  private int eachRowSize;

  // the length of the bytes added in the page
  private int totalLength;

  // size of the allocated memory, in bytes
  private int capacity;

  private final String taskId = ThreadLocalTaskInfo.getCarbonTaskInfo().getTaskId();

  private static final int byteBits = DataTypes.BYTE.getSizeBits();
  private static final int shortBits = DataTypes.SHORT.getSizeBits();
  private static final int intBits = DataTypes.INT.getSizeBits();
  private static final int longBits = DataTypes.LONG.getSizeBits();
  private static final int floatBits = DataTypes.FLOAT.getSizeBits();
  private static final int doubleBits = DataTypes.DOUBLE.getSizeBits();

//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3575
  UnsafeFixLengthColumnPage(ColumnPageEncoderMeta columnPageEncoderMeta, int pageSize) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2851
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2852
    super(columnPageEncoderMeta, pageSize);
    if (columnPageEncoderMeta.getStoreDataType() == DataTypes.BOOLEAN ||
        columnPageEncoderMeta.getStoreDataType() == DataTypes.BYTE ||
        columnPageEncoderMeta.getStoreDataType() == DataTypes.SHORT ||
        columnPageEncoderMeta.getStoreDataType() == DataTypes.INT ||
        columnPageEncoderMeta.getStoreDataType() == DataTypes.LONG ||
        columnPageEncoderMeta.getStoreDataType() == DataTypes.FLOAT ||
        columnPageEncoderMeta.getStoreDataType() == DataTypes.DOUBLE) {
      int size = pageSize << columnPageEncoderMeta.getStoreDataType().getSizeBits();
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1318
      memoryBlock = UnsafeMemoryManager.allocateMemoryWithRetry(taskId, size);
      baseAddress = memoryBlock.getBaseObject();
      baseOffset = memoryBlock.getBaseOffset();
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2607
      capacity = size;
    } else if (columnPageEncoderMeta.getStoreDataType() == DataTypes.SHORT_INT) {
      int size = pageSize * 3;
      memoryBlock = UnsafeMemoryManager.allocateMemoryWithRetry(taskId, size);
      baseAddress = memoryBlock.getBaseObject();
      baseOffset = memoryBlock.getBaseOffset();
      capacity = size;
    } else if (DataTypes.isDecimal(columnPageEncoderMeta.getStoreDataType()) ||
        columnPageEncoderMeta.getStoreDataType() == DataTypes.STRING) {
      throw new UnsupportedOperationException(
          "invalid data type: " + columnPageEncoderMeta.getStoreDataType());
    }
    totalLength = 0;
  }

  UnsafeFixLengthColumnPage(ColumnPageEncoderMeta columnPageEncoderMeta, int pageSize,
      int eachRowSize) {
    this(columnPageEncoderMeta, pageSize);
    this.eachRowSize = eachRowSize;
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2607
    totalLength = 0;
    if (columnPageEncoderMeta.getStoreDataType() == DataTypes.BYTE_ARRAY) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3519
      capacity = pageSize * eachRowSize;
      memoryBlock = UnsafeMemoryManager.allocateMemoryWithRetry(taskId, capacity);
      baseAddress = memoryBlock.getBaseObject();
      baseOffset = memoryBlock.getBaseOffset();
    }
  }

  private void checkDataFileSize() {
    // 16 is a Watermark in order to stop from overflowing.
    if (totalLength > (Integer.MAX_VALUE - 16)) {
      // since we later store a column page in a byte array, so its maximum size is 2GB
      throw new RuntimeException("Carbondata only support maximum 2GB size for one column page");
    }
  }

  private void updatePageSize(int rowId) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2735
    if (pageSize < rowId) {
      // update the actual number of rows
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3233
      pageSize = rowId + 1;
    }
  }

  @Override
  public void putByte(int rowId, byte value) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3575
    ensureMemory(ByteUtil.SIZEOF_BYTE);
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2489
    long offset = ((long)rowId) << byteBits;
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1386
    CarbonUnsafe.getUnsafe().putByte(baseAddress, baseOffset + offset, value);
    totalLength += ByteUtil.SIZEOF_BYTE;
    updatePageSize(rowId);
  }

  @Override
  public void putShort(int rowId, short value) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3575
    ensureMemory(shortBits);
    long offset = ((long)rowId) << shortBits;
    CarbonUnsafe.getUnsafe().putShort(baseAddress, baseOffset + offset, value);
    totalLength += ByteUtil.SIZEOF_SHORT;
    updatePageSize(rowId);
  }

  @Override
  public void putShortInt(int rowId, int value) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3575
    ensureMemory(ByteUtil.SIZEOF_SHORT_INT);
    byte[] data = ByteUtil.to3Bytes(value);
    long offset = rowId * 3L;
    CarbonUnsafe.getUnsafe().putByte(baseAddress, baseOffset + offset, data[0]);
    CarbonUnsafe.getUnsafe().putByte(baseAddress, baseOffset + offset + 1, data[1]);
    CarbonUnsafe.getUnsafe().putByte(baseAddress, baseOffset + offset + 2, data[2]);
    totalLength += ByteUtil.SIZEOF_SHORT_INT;
    updatePageSize(rowId);
  }

  @Override
  public void putInt(int rowId, int value) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3575
    ensureMemory(ByteUtil.SIZEOF_INT);
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2489
    long offset = ((long)rowId) << intBits;
    CarbonUnsafe.getUnsafe().putInt(baseAddress, baseOffset + offset, value);
    totalLength += ByteUtil.SIZEOF_INT;
    updatePageSize(rowId);
  }

  @Override
  public void putLong(int rowId, long value) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3575
    ensureMemory(ByteUtil.SIZEOF_LONG);
    long offset = ((long)rowId) << longBits;
    CarbonUnsafe.getUnsafe().putLong(baseAddress, baseOffset + offset, value);
    totalLength += ByteUtil.SIZEOF_LONG;
    updatePageSize(rowId);
  }

  @Override
  public void putDouble(int rowId, double value) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3575
    ensureMemory(ByteUtil.SIZEOF_DOUBLE);
    long offset = ((long)rowId) << doubleBits;
    CarbonUnsafe.getUnsafe().putDouble(baseAddress, baseOffset + offset, value);
    totalLength += ByteUtil.SIZEOF_DOUBLE;
    updatePageSize(rowId);
  }

  @Override
  public void putFloat(int rowId, float value) {
    ensureMemory(ByteUtil.SIZEOF_FLOAT);
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3575

    long offset = ((long) rowId) << floatBits;
    CarbonUnsafe.getUnsafe().putFloat(baseAddress, baseOffset + offset, value);
    totalLength += ByteUtil.SIZEOF_FLOAT;
    updatePageSize(rowId);
  }

  @Override
  public void putBytes(int rowId, byte[] bytes) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3575
    ensureMemory(eachRowSize);
    // copy the data to memory
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2477
    long offset = (long)rowId * eachRowSize;
    CarbonUnsafe.getUnsafe()
        .copyMemory(bytes, CarbonUnsafe.BYTE_ARRAY_OFFSET, memoryBlock.getBaseObject(),
            baseOffset + offset, bytes.length);
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2735
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2735
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2735
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2735
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2735
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2735
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2735
    updatePageSize(rowId);
    totalLength += eachRowSize;
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
  public byte getByte(int rowId) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2489
    long offset = ((long)rowId) << byteBits;
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1386
    return CarbonUnsafe.getUnsafe().getByte(baseAddress, baseOffset + offset);
  }

  @Override
  public short getShort(int rowId) {
    long offset = ((long)rowId) << shortBits;
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
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2489
    long offset = ((long)rowId) << intBits;
    return CarbonUnsafe.getUnsafe().getInt(baseAddress, baseOffset + offset);
  }

  @Override
  public long getLong(int rowId) {
    long offset = ((long)rowId) << longBits;
    return CarbonUnsafe.getUnsafe().getLong(baseAddress, baseOffset + offset);
  }

  @Override
  public float getFloat(int rowId) {
    long offset = ((long)rowId) << floatBits;
    return CarbonUnsafe.getUnsafe().getFloat(baseAddress, baseOffset + offset);
  }

  @Override
  public double getDouble(int rowId) {
    long offset = ((long)rowId) << doubleBits;
    return CarbonUnsafe.getUnsafe().getDouble(baseAddress, baseOffset + offset);
  }

  @Override
  public BigDecimal getDecimal(int rowId) {
    throw new UnsupportedOperationException(
        "invalid data type: " + columnPageEncoderMeta.getStoreDataType());
  }

  @Override
  public byte[] getBytes(int rowId) {
    // creating a row
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2477
    byte[] data = new byte[eachRowSize];
    //copy the row from memory block based on offset
    // offset position will be index * each column value length
    CarbonUnsafe.getUnsafe().copyMemory(memoryBlock.getBaseObject(),
        baseOffset + ((long)rowId * eachRowSize), data,
        CarbonUnsafe.BYTE_ARRAY_OFFSET, eachRowSize);
    return data;
  }

  @Override
  public byte[] getDecimalPage() {
    throw new UnsupportedOperationException(
        "invalid data type: " + columnPageEncoderMeta.getStoreDataType());
  }

  @Override
  public byte[] getBytePage() {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2735
    byte[] data = new byte[getEndLoop()];
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2489
    for (long i = 0; i < data.length; i++) {
      long offset = i << byteBits;
      data[(int)i] = CarbonUnsafe.getUnsafe().getByte(baseAddress, baseOffset + offset);
    }
    return data;
  }

  @Override
  public short[] getShortPage() {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2735
    short[] data = new short[getEndLoop()];
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2489
    for (long i = 0; i < data.length; i++) {
      long offset = i << shortBits;
      data[(int)i] = CarbonUnsafe.getUnsafe().getShort(baseAddress, baseOffset + offset);
    }
    return data;
  }

  @Override
  public byte[] getShortIntPage() {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2735
    byte[] data = new byte[getEndLoop() * 3];
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1386
    CarbonUnsafe.getUnsafe().copyMemory(baseAddress, baseOffset,
        data, CarbonUnsafe.BYTE_ARRAY_OFFSET, data.length);
    return data;
  }

  @Override
  public int[] getIntPage() {
    int[] data = new int[getEndLoop()];
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2489
    for (long i = 0; i < data.length; i++) {
      long offset = i << intBits;
      data[(int)i] = CarbonUnsafe.getUnsafe().getInt(baseAddress, baseOffset + offset);
    }
    return data;
  }

  @Override
  public long[] getLongPage() {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2735
    long[] data = new long[getEndLoop()];
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2489
    for (long i = 0; i < data.length; i++) {
      long offset = i << longBits;
      data[(int)i] = CarbonUnsafe.getUnsafe().getLong(baseAddress, baseOffset + offset);
    }
    return data;
  }

  @Override
  public float[] getFloatPage() {
    float[] data = new float[getEndLoop()];
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2489
    for (long i = 0; i < data.length; i++) {
      long offset = i << floatBits;
      data[(int)i] = CarbonUnsafe.getUnsafe().getFloat(baseAddress, baseOffset + offset);
    }
    return data;
  }

  @Override
  public double[] getDoublePage() {
    double[] data = new double[getEndLoop()];
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2489
    for (long i = 0; i < data.length; i++) {
      long offset = i << doubleBits;
      data[(int)i] = CarbonUnsafe.getUnsafe().getDouble(baseAddress, baseOffset + offset);
    }
    return data;
  }

  @Override
  public byte[][] getByteArrayPage() {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2760
    byte[][] data = new byte[getEndLoop()][eachRowSize];
    long offset = baseOffset;
    for (int i = 0; i < data.length; i++) {
      //copy the row from memory block based on offset
      // offset position will be index * each column value length
      CarbonUnsafe.getUnsafe().copyMemory(memoryBlock.getBaseObject(), offset, data[i],
          CarbonUnsafe.BYTE_ARRAY_OFFSET, eachRowSize);
      offset += eachRowSize;
    }
    return data;
  }

  @Override
  public ByteBuffer getByteBuffer() {
    int numRow = getEndLoop();
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3731
    ByteBuffer out = ByteBuffer.allocateDirect(numRow * eachRowSize);
    CarbonUnsafe.getUnsafe().copyMemory(
        memoryBlock.getBaseOffset(), ((DirectBuffer)out).address(), numRow * eachRowSize);
    return out;
  }

  @Override
  public byte[] getLVFlattenedBytePage() {
    throw new UnsupportedOperationException(
        "invalid data type: " + columnPageEncoderMeta.getStoreDataType());
  }

  @Override
  public byte[] getComplexChildrenLVFlattenedBytePage(DataType dataType) {
    byte[] data = new byte[totalLength];
    CarbonUnsafe.getUnsafe()
        .copyMemory(baseAddress, baseOffset, data, CarbonUnsafe.BYTE_ARRAY_OFFSET, totalLength);
    return data;
  }

  @Override
  public byte[] getComplexParentFlattenedBytePage() {
    throw new UnsupportedOperationException("internal error");
  }

  @Override
  public void setBytePage(byte[] byteData) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1386
    CarbonUnsafe.getUnsafe().copyMemory(byteData, CarbonUnsafe.BYTE_ARRAY_OFFSET,
        baseAddress, baseOffset, byteData.length << byteBits);
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2607
    capacity = byteData.length;
  }

  @Override
  public void setShortPage(short[] shortData) {
    CarbonUnsafe.getUnsafe().copyMemory(shortData, CarbonUnsafe.SHORT_ARRAY_OFFSET,
        baseAddress, baseOffset, shortData.length << shortBits);
    capacity = shortData.length;
  }

  @Override
  public void setShortIntPage(byte[] shortIntData) {
    CarbonUnsafe.getUnsafe().copyMemory(shortIntData, CarbonUnsafe.BYTE_ARRAY_OFFSET,
        baseAddress, baseOffset, shortIntData.length);
    capacity = shortIntData.length;
  }

  @Override
  public void setIntPage(int[] intData) {
    CarbonUnsafe.getUnsafe().copyMemory(intData, CarbonUnsafe.INT_ARRAY_OFFSET,
        baseAddress, baseOffset, intData.length << intBits);
    capacity = intData.length;
  }

  @Override
  public void setLongPage(long[] longData) {
    CarbonUnsafe.getUnsafe().copyMemory(longData, CarbonUnsafe.LONG_ARRAY_OFFSET,
        baseAddress, baseOffset, longData.length << longBits);
    capacity = longData.length;
  }

  @Override
  public void setFloatPage(float[] floatData) {
    CarbonUnsafe.getUnsafe().copyMemory(floatData, CarbonUnsafe.FLOAT_ARRAY_OFFSET,
        baseAddress, baseOffset, floatData.length << floatBits);
    capacity = floatData.length;
  }

  @Override
  public void setDoublePage(double[] doubleData) {
    CarbonUnsafe.getUnsafe().copyMemory(doubleData, CarbonUnsafe.DOUBLE_ARRAY_OFFSET,
        baseAddress, baseOffset, doubleData.length << doubleBits);
    capacity = doubleData.length;
  }

  @Override
  public void setByteArrayPage(byte[][] byteArray) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2851
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2852
    throw new UnsupportedOperationException(
        "invalid data type: " + columnPageEncoderMeta.getStoreDataType());
  }

  public void freeMemory() {
    if (memoryBlock != null) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1318
      UnsafeMemoryManager.INSTANCE.freeMemory(taskId, memoryBlock);
      memoryBlock = null;
      baseAddress = null;
      baseOffset = 0;
    }
  }

  @Override
  public void convertValue(ColumnPageValueConverter codec) {
    int endLoop = getEndLoop();
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2851
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2852
    if (columnPageEncoderMeta.getStoreDataType() == DataTypes.BYTE) {
      for (long i = 0; i < endLoop; i++) {
        long offset = i << byteBits;
        codec.encode((int) i, CarbonUnsafe.getUnsafe().getByte(baseAddress, baseOffset + offset));
      }
    } else if (columnPageEncoderMeta.getStoreDataType() == DataTypes.SHORT) {
      for (long i = 0; i < endLoop; i++) {
        long offset = i << shortBits;
        codec.encode((int) i, CarbonUnsafe.getUnsafe().getShort(baseAddress, baseOffset + offset));
      }
    } else if (columnPageEncoderMeta.getStoreDataType() == DataTypes.INT) {
      for (long i = 0; i < endLoop; i++) {
        long offset = i << intBits;
        codec.encode((int) i, CarbonUnsafe.getUnsafe().getInt(baseAddress, baseOffset + offset));
      }
    } else if (columnPageEncoderMeta.getStoreDataType() == DataTypes.LONG) {
      for (long i = 0; i < endLoop; i++) {
        long offset = i << longBits;
        codec.encode((int) i, CarbonUnsafe.getUnsafe().getLong(baseAddress, baseOffset + offset));
      }
    } else if (columnPageEncoderMeta.getStoreDataType() == DataTypes.FLOAT) {
      for (long i = 0; i < endLoop; i++) {
        long offset = i << floatBits;
        codec.encode((int) i, CarbonUnsafe.getUnsafe().getFloat(baseAddress, baseOffset + offset));
      }
    } else if (columnPageEncoderMeta.getStoreDataType() == DataTypes.DOUBLE) {
      for (long i = 0; i < endLoop; i++) {
        long offset = i << doubleBits;
        codec.encode((int) i, CarbonUnsafe.getUnsafe().getDouble(baseAddress, baseOffset + offset));
      }
    } else {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2851
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2852
      throw new UnsupportedOperationException(
          "invalid data type: " + columnPageEncoderMeta.getStoreDataType());
    }
  }

  private int getEndLoop() {
    if (columnPageEncoderMeta.getStoreDataType() == DataTypes.BYTE) {
      return totalLength / ByteUtil.SIZEOF_BYTE;
    } else if (columnPageEncoderMeta.getStoreDataType() == DataTypes.SHORT) {
      return totalLength / ByteUtil.SIZEOF_SHORT;
    } else if (columnPageEncoderMeta.getStoreDataType() == DataTypes.SHORT_INT) {
      return totalLength / ByteUtil.SIZEOF_SHORT_INT;
    } else if (columnPageEncoderMeta.getStoreDataType() == DataTypes.INT) {
      return totalLength / ByteUtil.SIZEOF_INT;
    } else if (columnPageEncoderMeta.getStoreDataType() == DataTypes.LONG) {
      return totalLength / ByteUtil.SIZEOF_LONG;
    } else if (columnPageEncoderMeta.getStoreDataType() == DataTypes.FLOAT) {
      return totalLength / DataTypes.FLOAT.getSizeInBytes();
    } else if (columnPageEncoderMeta.getStoreDataType() == DataTypes.DOUBLE) {
      return totalLength / DataTypes.DOUBLE.getSizeInBytes();
    } else if (columnPageEncoderMeta.getStoreDataType() == DataTypes.BYTE_ARRAY) {
      return totalLength / eachRowSize;
    } else {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2851
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2852
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2851
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2852
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2851
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2852
      throw new UnsupportedOperationException(
          "invalid data type: " + columnPageEncoderMeta.getStoreDataType());
    }
  }

  @Override
  public long getPageLengthInBytes() {
    // For unsafe column page, we are always tracking the total length
    // so return it directly instead of calculate it again (super class implementation)
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2977
    return totalLength;
  }

  /**
   * reallocate memory if capacity length than current size + request size
   */
  protected void ensureMemory(int requestSize) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2607
    checkDataFileSize();
    if (totalLength + requestSize > capacity) {
      int newSize = Math.max(2 * capacity, totalLength + requestSize);
      MemoryBlock newBlock = UnsafeMemoryManager.allocateMemoryWithRetry(taskId, newSize);
      CarbonUnsafe.getUnsafe().copyMemory(baseAddress, baseOffset,
          newBlock.getBaseObject(), newBlock.getBaseOffset(), totalLength);
      UnsafeMemoryManager.INSTANCE.freeMemory(taskId, memoryBlock);
      memoryBlock = newBlock;
      baseAddress = newBlock.getBaseObject();
      baseOffset = newBlock.getBaseOffset();
      capacity = newSize;
    }
  }

  public int getActualRowCount() {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2735
    return getEndLoop();
  }
}
