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
import org.apache.carbondata.core.memory.CarbonUnsafe;
import org.apache.carbondata.core.memory.MemoryBlock;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.memory.UnsafeMemoryManager;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.util.ByteUtil;
import org.apache.carbondata.core.util.ThreadLocalTaskInfo;

import static org.apache.carbondata.core.metadata.datatype.DataType.BYTE;

// This extension uses unsafe memory to store page data, for fix length data type only (byte,
// short, integer, long, float, double)
public class UnsafeFixLengthColumnPage extends ColumnPage {
  // memory allocated by Unsafe
  private MemoryBlock memoryBlock;

  // base address of memoryBlock
  private Object baseAddress;

  // base offset of memoryBlock
  private long baseOffset;

  private final long taskId = ThreadLocalTaskInfo.getCarbonTaskInfo().getTaskId();

  private static final int byteBits = BYTE.getSizeBits();
  private static final int shortBits = DataType.SHORT.getSizeBits();
  private static final int intBits = DataType.INT.getSizeBits();
  private static final int longBits = DataType.LONG.getSizeBits();
  private static final int floatBits = DataType.FLOAT.getSizeBits();
  private static final int doubleBits = DataType.DOUBLE.getSizeBits();

  UnsafeFixLengthColumnPage(DataType dataType, int pageSize, int scale, int precision)
      throws MemoryException {
    super(dataType, pageSize, scale, precision);
    switch (dataType) {
      case BYTE:
      case SHORT:
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
        int size = pageSize << dataType.getSizeBits();
        memoryBlock = UnsafeMemoryManager.allocateMemoryWithRetry(taskId, size);
        baseAddress = memoryBlock.getBaseObject();
        baseOffset = memoryBlock.getBaseOffset();
        break;
      case SHORT_INT:
        size = pageSize * 3;
        memoryBlock = UnsafeMemoryManager.allocateMemoryWithRetry(taskId, size);
        baseAddress = memoryBlock.getBaseObject();
        baseOffset = memoryBlock.getBaseOffset();
        break;
      case DECIMAL:
      case STRING:
        throw new UnsupportedOperationException("invalid data type: " + dataType);
    }
  }

  @Override
  public void putByte(int rowId, byte value) {
    long offset = rowId << byteBits;
    CarbonUnsafe.getUnsafe().putByte(baseAddress, baseOffset + offset, value);
  }

  @Override
  public void putShort(int rowId, short value) {
    long offset = rowId << shortBits;
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
    long offset = rowId << intBits;
    CarbonUnsafe.getUnsafe().putInt(baseAddress, baseOffset + offset, value);
  }

  @Override
  public void putLong(int rowId, long value) {
    long offset = rowId << longBits;
    CarbonUnsafe.getUnsafe().putLong(baseAddress, baseOffset + offset, value);
  }

  @Override
  public void putDouble(int rowId, double value) {
    long offset = rowId << doubleBits;
    CarbonUnsafe.getUnsafe().putDouble(baseAddress, baseOffset + offset, value);
  }

  @Override
  public void putBytes(int rowId, byte[] bytes) {
    throw new UnsupportedOperationException("invalid data type: " + dataType);
  }

  @Override
  public void putBytes(int rowId, byte[] bytes, int offset, int length) {
    throw new UnsupportedOperationException("invalid data type: " + dataType);
  }

  @Override public void putDecimal(int rowId, BigDecimal decimal) {
    throw new UnsupportedOperationException("invalid data type: " + dataType);
  }

  @Override
  public byte getByte(int rowId) {
    long offset = rowId << byteBits;
    return CarbonUnsafe.getUnsafe().getByte(baseAddress, baseOffset + offset);
  }

  @Override
  public short getShort(int rowId) {
    long offset = rowId << shortBits;
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
    long offset = rowId << intBits;
    return CarbonUnsafe.getUnsafe().getInt(baseAddress, baseOffset + offset);
  }

  @Override
  public long getLong(int rowId) {
    long offset = rowId << longBits;
    return CarbonUnsafe.getUnsafe().getLong(baseAddress, baseOffset + offset);
  }

  @Override
  public float getFloat(int rowId) {
    long offset = rowId << floatBits;
    return CarbonUnsafe.getUnsafe().getFloat(baseAddress, baseOffset + offset);
  }

  @Override
  public double getDouble(int rowId) {
    long offset = rowId << doubleBits;
    return CarbonUnsafe.getUnsafe().getDouble(baseAddress, baseOffset + offset);
  }

  @Override
  public BigDecimal getDecimal(int rowId) {
    throw new UnsupportedOperationException("invalid data type: " + dataType);
  }

  @Override
  public byte[] getBytes(int rowId) {
    throw new UnsupportedOperationException("invalid data type: " + dataType);
  }

  @Override public byte[] getDecimalPage() {
    throw new UnsupportedOperationException("invalid data type: " + dataType);
  }

  @Override
  public byte[] getBytePage() {
    byte[] data = new byte[getPageSize()];
    for (int i = 0; i < data.length; i++) {
      long offset = i << byteBits;
      data[i] = CarbonUnsafe.getUnsafe().getByte(baseAddress, baseOffset + offset);
    }
    return data;
  }

  @Override
  public short[] getShortPage() {
    short[] data = new short[getPageSize()];
    for (int i = 0; i < data.length; i++) {
      long offset = i << shortBits;
      data[i] = CarbonUnsafe.getUnsafe().getShort(baseAddress, baseOffset + offset);
    }
    return data;
  }

  @Override
  public byte[] getShortIntPage() {
    byte[] data = new byte[pageSize * 3];
    CarbonUnsafe.getUnsafe().copyMemory(baseAddress, baseOffset,
        data, CarbonUnsafe.BYTE_ARRAY_OFFSET, data.length);
    return data;
  }

  @Override
  public int[] getIntPage() {
    int[] data = new int[getPageSize()];
    for (int i = 0; i < data.length; i++) {
      long offset = i << intBits;
      data[i] = CarbonUnsafe.getUnsafe().getInt(baseAddress, baseOffset + offset);
    }
    return data;
  }

  @Override
  public long[] getLongPage() {
    long[] data = new long[getPageSize()];
    for (int i = 0; i < data.length; i++) {
      long offset = i << longBits;
      data[i] = CarbonUnsafe.getUnsafe().getLong(baseAddress, baseOffset + offset);
    }
    return data;
  }

  @Override
  public float[] getFloatPage() {
    float[] data = new float[getPageSize()];
    for (int i = 0; i < data.length; i++) {
      long offset = i << floatBits;
      data[i] = CarbonUnsafe.getUnsafe().getFloat(baseAddress, baseOffset + offset);
    }
    return data;
  }

  @Override
  public double[] getDoublePage() {
    double[] data = new double[getPageSize()];
    for (int i = 0; i < data.length; i++) {
      long offset = i << doubleBits;
      data[i] = CarbonUnsafe.getUnsafe().getDouble(baseAddress, baseOffset + offset);
    }
    return data;
  }

  @Override
  public byte[][] getByteArrayPage() {
    throw new UnsupportedOperationException("invalid data type: " + dataType);
  }

  @Override
  public byte[] getLVFlattenedBytePage() {
    throw new UnsupportedOperationException("invalid data type: " + dataType);
  }

  @Override
  public void setBytePage(byte[] byteData) {
    CarbonUnsafe.getUnsafe().copyMemory(byteData, CarbonUnsafe.BYTE_ARRAY_OFFSET,
        baseAddress, baseOffset, byteData.length << byteBits);
  }

  @Override
  public void setShortPage(short[] shortData) {
    CarbonUnsafe.getUnsafe().copyMemory(shortData, CarbonUnsafe.SHORT_ARRAY_OFFSET,
        baseAddress, baseOffset, shortData.length << shortBits);
  }

  @Override
  public void setShortIntPage(byte[] shortIntData) {
    CarbonUnsafe.getUnsafe().copyMemory(shortIntData, CarbonUnsafe.BYTE_ARRAY_OFFSET,
        baseAddress, baseOffset, shortIntData.length);
  }

  @Override
  public void setIntPage(int[] intData) {
    CarbonUnsafe.getUnsafe().copyMemory(intData, CarbonUnsafe.INT_ARRAY_OFFSET,
        baseAddress, baseOffset, intData.length << intBits);
  }

  @Override
  public void setLongPage(long[] longData) {
    CarbonUnsafe.getUnsafe().copyMemory(longData, CarbonUnsafe.LONG_ARRAY_OFFSET,
        baseAddress, baseOffset, longData.length << longBits);
  }

  @Override
  public void setFloatPage(float[] floatData) {
    CarbonUnsafe.getUnsafe().copyMemory(floatData, CarbonUnsafe.FLOAT_ARRAY_OFFSET,
        baseAddress, baseOffset, floatData.length << floatBits);
  }

  @Override
  public void setDoublePage(double[] doubleData) {
    CarbonUnsafe.getUnsafe().copyMemory(doubleData, CarbonUnsafe.DOUBLE_ARRAY_OFFSET,
        baseAddress, baseOffset, doubleData.length << doubleBits);
  }

  @Override
  public void setByteArrayPage(byte[][] byteArray) {
    throw new UnsupportedOperationException("invalid data type: " + dataType);
  }

  public void freeMemory() {
    if (memoryBlock != null) {
      UnsafeMemoryManager.INSTANCE.freeMemory(taskId, memoryBlock);
      memoryBlock = null;
      baseAddress = null;
      baseOffset = 0;
    }
  }

  @Override
  public void convertValue(ColumnPageValueConverter codec) {
    int pageSize = getPageSize();
    switch (dataType) {
      case BYTE:
        for (int i = 0; i < pageSize; i++) {
          long offset = i << byteBits;
          codec.encode(i, CarbonUnsafe.getUnsafe().getByte(baseAddress, baseOffset + offset));
        }
        break;
      case SHORT:
        for (int i = 0; i < pageSize; i++) {
          long offset = i << shortBits;
          codec.encode(i, CarbonUnsafe.getUnsafe().getShort(baseAddress, baseOffset + offset));
        }
        break;
      case INT:
        for (int i = 0; i < pageSize; i++) {
          long offset = i << intBits;
          codec.encode(i, CarbonUnsafe.getUnsafe().getInt(baseAddress, baseOffset + offset));
        }
        break;
      case LONG:
        for (int i = 0; i < pageSize; i++) {
          long offset = i << longBits;
          codec.encode(i, CarbonUnsafe.getUnsafe().getLong(baseAddress, baseOffset + offset));
        }
        break;
      case FLOAT:
        for (int i = 0; i < pageSize; i++) {
          long offset = i << floatBits;
          codec.encode(i, CarbonUnsafe.getUnsafe().getFloat(baseAddress, baseOffset + offset));
        }
        break;
      case DOUBLE:
        for (int i = 0; i < pageSize; i++) {
          long offset = i << doubleBits;
          codec.encode(i, CarbonUnsafe.getUnsafe().getDouble(baseAddress, baseOffset + offset));
        }
        break;
      default:
        throw new UnsupportedOperationException("invalid data type: " + dataType);
    }
  }

  @Override public byte[] compress(Compressor compressor) throws MemoryException, IOException {
    if (UnsafeMemoryManager.isOffHeap()) {
      // use raw compression and copy to byte[]
      int inputSize = pageSize * dataType.getSizeInBytes();
      int compressedMaxSize = compressor.maxCompressedLength(inputSize);
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
}