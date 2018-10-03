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

import org.apache.carbondata.core.datastore.compression.Compressor;
import org.apache.carbondata.core.datastore.page.encoding.ColumnPageEncoderMeta;
import org.apache.carbondata.core.memory.CarbonUnsafe;
import org.apache.carbondata.core.memory.MemoryBlock;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.memory.UnsafeMemoryManager;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.util.ByteUtil;
import org.apache.carbondata.core.util.ThreadLocalTaskInfo;


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

  UnsafeFixLengthColumnPage(ColumnPageEncoderMeta columnPageEncoderMeta, int pageSize)
      throws MemoryException {
    super(columnPageEncoderMeta, pageSize);
    if (columnPageEncoderMeta.getStoreDataType() == DataTypes.BOOLEAN ||
        columnPageEncoderMeta.getStoreDataType() == DataTypes.BYTE ||
        columnPageEncoderMeta.getStoreDataType() == DataTypes.SHORT ||
        columnPageEncoderMeta.getStoreDataType() == DataTypes.INT ||
        columnPageEncoderMeta.getStoreDataType() == DataTypes.LONG ||
        columnPageEncoderMeta.getStoreDataType() == DataTypes.FLOAT ||
        columnPageEncoderMeta.getStoreDataType() == DataTypes.DOUBLE) {
      int size = pageSize << columnPageEncoderMeta.getStoreDataType().getSizeBits();
      memoryBlock = UnsafeMemoryManager.allocateMemoryWithRetry(taskId, size);
      baseAddress = memoryBlock.getBaseObject();
      baseOffset = memoryBlock.getBaseOffset();
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
      int eachRowSize) throws MemoryException {
    this(columnPageEncoderMeta, pageSize);
    this.eachRowSize = eachRowSize;
    totalLength = 0;
    if (columnPageEncoderMeta.getStoreDataType() == DataTypes.BYTE_ARRAY) {
      memoryBlock =
          UnsafeMemoryManager.allocateMemoryWithRetry(taskId, (long) pageSize * eachRowSize);
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
    if (pageSize < rowId) {
      pageSize = rowId;
    }
  }

  @Override
  public void putByte(int rowId, byte value) {
    try {
      ensureMemory(ByteUtil.SIZEOF_BYTE);
    } catch (MemoryException e) {
      throw new RuntimeException(e);
    }
    long offset = ((long)rowId) << byteBits;
    CarbonUnsafe.getUnsafe().putByte(baseAddress, baseOffset + offset, value);
    totalLength += ByteUtil.SIZEOF_BYTE;
    updatePageSize(rowId);
  }



  @Override
  public void putShort(int rowId, short value) {
    try {
      ensureMemory(shortBits);
    } catch (MemoryException e) {
      throw new RuntimeException(e);
    }
    long offset = ((long)rowId) << shortBits;
    CarbonUnsafe.getUnsafe().putShort(baseAddress, baseOffset + offset, value);
    totalLength += ByteUtil.SIZEOF_SHORT;
    updatePageSize(rowId);
  }

  @Override
  public void putShortInt(int rowId, int value) {
    try {
      ensureMemory(ByteUtil.SIZEOF_SHORT_INT);
    } catch (MemoryException e) {
      throw new RuntimeException(e);
    }
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
    try {
      ensureMemory(ByteUtil.SIZEOF_INT);
    } catch (MemoryException e) {
      throw new RuntimeException(e);
    }
    long offset = ((long)rowId) << intBits;
    CarbonUnsafe.getUnsafe().putInt(baseAddress, baseOffset + offset, value);
    totalLength += ByteUtil.SIZEOF_INT;
    updatePageSize(rowId);
  }

  @Override
  public void putLong(int rowId, long value) {
    try {
      ensureMemory(ByteUtil.SIZEOF_LONG);
    } catch (MemoryException e) {
      throw new RuntimeException(e);
    }
    long offset = ((long)rowId) << longBits;
    CarbonUnsafe.getUnsafe().putLong(baseAddress, baseOffset + offset, value);
    totalLength += ByteUtil.SIZEOF_LONG;
    updatePageSize(rowId);
  }

  @Override
  public void putDouble(int rowId, double value) {
    try {
      ensureMemory(ByteUtil.SIZEOF_DOUBLE);
    } catch (MemoryException e) {
      throw new RuntimeException(e);
    }
    long offset = ((long)rowId) << doubleBits;
    CarbonUnsafe.getUnsafe().putDouble(baseAddress, baseOffset + offset, value);
    totalLength += ByteUtil.SIZEOF_DOUBLE;
    updatePageSize(rowId);
  }

  @Override
  public void putFloat(int rowId, float value) {
    try {
      ensureMemory(ByteUtil.SIZEOF_FLOAT);
    } catch (MemoryException e) {
      throw new RuntimeException(e);
    }

    long offset = ((long) rowId) << floatBits;
    CarbonUnsafe.getUnsafe().putFloat(baseAddress, baseOffset + offset, value);
    totalLength += ByteUtil.SIZEOF_FLOAT;
    updatePageSize(rowId);
  }

  @Override
  public void putBytes(int rowId, byte[] bytes) {
    try {
      ensureMemory(eachRowSize);
    } catch (MemoryException e) {
      throw new RuntimeException(e);
    }
    // copy the data to memory
    long offset = (long)rowId * eachRowSize;
    CarbonUnsafe.getUnsafe()
        .copyMemory(bytes, CarbonUnsafe.BYTE_ARRAY_OFFSET, memoryBlock.getBaseObject(),
            baseOffset + offset, bytes.length);
    updatePageSize(rowId);
    totalLength += eachRowSize;
  }

  @Override
  public void putBytes(int rowId, byte[] bytes, int offset, int length) {
    throw new UnsupportedOperationException(
        "invalid data type: " + columnPageEncoderMeta.getStoreDataType());
  }

  @Override public void putDecimal(int rowId, BigDecimal decimal) {
    throw new UnsupportedOperationException(
        "invalid data type: " + columnPageEncoderMeta.getStoreDataType());
  }

  @Override
  public byte getByte(int rowId) {
    long offset = ((long)rowId) << byteBits;
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
    byte[] data = new byte[eachRowSize];
    //copy the row from memory block based on offset
    // offset position will be index * each column value length
    CarbonUnsafe.getUnsafe().copyMemory(memoryBlock.getBaseObject(),
        baseOffset + ((long)rowId * eachRowSize), data,
        CarbonUnsafe.BYTE_ARRAY_OFFSET, eachRowSize);
    return data;
  }

  @Override public byte[] getDecimalPage() {
    throw new UnsupportedOperationException(
        "invalid data type: " + columnPageEncoderMeta.getStoreDataType());
  }

  @Override
  public byte[] getBytePage() {
    byte[] data = new byte[getEndLoop()];
    for (long i = 0; i < data.length; i++) {
      long offset = i << byteBits;
      data[(int)i] = CarbonUnsafe.getUnsafe().getByte(baseAddress, baseOffset + offset);
    }
    return data;
  }

  @Override
  public short[] getShortPage() {
    short[] data = new short[getEndLoop()];
    for (long i = 0; i < data.length; i++) {
      long offset = i << shortBits;
      data[(int)i] = CarbonUnsafe.getUnsafe().getShort(baseAddress, baseOffset + offset);
    }
    return data;
  }

  @Override
  public byte[] getShortIntPage() {
    byte[] data = new byte[getEndLoop() * 3];
    CarbonUnsafe.getUnsafe().copyMemory(baseAddress, baseOffset,
        data, CarbonUnsafe.BYTE_ARRAY_OFFSET, data.length);
    return data;
  }

  @Override
  public int[] getIntPage() {
    int[] data = new int[getEndLoop()];
    for (long i = 0; i < data.length; i++) {
      long offset = i << intBits;
      data[(int)i] = CarbonUnsafe.getUnsafe().getInt(baseAddress, baseOffset + offset);
    }
    return data;
  }

  @Override
  public long[] getLongPage() {
    long[] data = new long[getEndLoop()];
    for (long i = 0; i < data.length; i++) {
      long offset = i << longBits;
      data[(int)i] = CarbonUnsafe.getUnsafe().getLong(baseAddress, baseOffset + offset);
    }
    return data;
  }

  @Override
  public float[] getFloatPage() {
    float[] data = new float[getPageSize()];
    for (long i = 0; i < data.length; i++) {
      long offset = i << floatBits;
      data[(int)i] = CarbonUnsafe.getUnsafe().getFloat(baseAddress, baseOffset + offset);
    }
    return data;
  }

  @Override
  public double[] getDoublePage() {
    double[] data = new double[getPageSize()];
    for (long i = 0; i < data.length; i++) {
      long offset = i << doubleBits;
      data[(int)i] = CarbonUnsafe.getUnsafe().getDouble(baseAddress, baseOffset + offset);
    }
    return data;
  }

  @Override
  public byte[][] getByteArrayPage() {
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
  public byte[] getLVFlattenedBytePage() {
    throw new UnsupportedOperationException(
        "invalid data type: " + columnPageEncoderMeta.getStoreDataType());
  }

  @Override public byte[] getComplexChildrenLVFlattenedBytePage() {
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
    CarbonUnsafe.getUnsafe().copyMemory(byteData, CarbonUnsafe.BYTE_ARRAY_OFFSET,
        baseAddress, baseOffset, byteData.length << byteBits);
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
    throw new UnsupportedOperationException(
        "invalid data type: " + columnPageEncoderMeta.getStoreDataType());
  }

  public void freeMemory() {
    if (memoryBlock != null) {
      UnsafeMemoryManager.INSTANCE.freeMemory(taskId, memoryBlock);
      memoryBlock = null;
      baseAddress = null;
      baseOffset = 0;
    }
  }

  @Override public void convertValue(ColumnPageValueConverter codec) {
    int endLoop = getEndLoop();
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
      throw new UnsupportedOperationException(
          "invalid data type: " + columnPageEncoderMeta.getStoreDataType());
    }
  }

  @Override public byte[] compress(Compressor compressor) throws MemoryException, IOException {
    if (UnsafeMemoryManager.isOffHeap() && compressor.supportUnsafe()) {
      // use raw compression and copy to byte[]
      int inputSize = totalLength;
      long compressedMaxSize = compressor.maxCompressedLength(inputSize);
      MemoryBlock compressed =
          UnsafeMemoryManager.allocateMemoryWithRetry(taskId, compressedMaxSize);
      long outSize = compressor.rawCompress(baseOffset, inputSize, compressed.getBaseOffset());
      assert outSize < Integer.MAX_VALUE;
      byte[] output = new byte[(int) outSize];
      CarbonUnsafe.getUnsafe()
          .copyMemory(compressed.getBaseObject(), compressed.getBaseOffset(), output,
              CarbonUnsafe.BYTE_ARRAY_OFFSET, outSize);
      UnsafeMemoryManager.INSTANCE.freeMemory(taskId, compressed);
      return output;
    } else {
      return super.compress(compressor);
    }
  }

  /**
   * reallocate memory if capacity length than current size + request size
   */
  protected void ensureMemory(int requestSize) throws MemoryException {
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
    return getEndLoop();
  }
}