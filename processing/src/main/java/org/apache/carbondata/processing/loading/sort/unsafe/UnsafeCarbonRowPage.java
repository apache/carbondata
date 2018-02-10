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

package org.apache.carbondata.processing.loading.sort.unsafe;

import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Arrays;

import org.apache.carbondata.core.memory.CarbonUnsafe;
import org.apache.carbondata.core.memory.IntPointerBuffer;
import org.apache.carbondata.core.memory.MemoryBlock;
import org.apache.carbondata.core.memory.UnsafeMemoryManager;
import org.apache.carbondata.core.memory.UnsafeSortMemoryManager;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.util.DataTypeUtil;

/**
 * It can keep the data of prescribed size data in offheap/onheap memory and returns it when needed
 */
public class UnsafeCarbonRowPage {

  private boolean[] noDictionaryDimensionMapping;

  private boolean[] noDictionarySortColumnMapping;

  private int dimensionSize;

  private int measureSize;

  private DataType[] measureDataType;

  private long[] nullSetWords;

  private IntPointerBuffer buffer;

  private int lastSize;

  private long sizeToBeUsed;

  private MemoryBlock dataBlock;

  private boolean saveToDisk;

  private MemoryManagerType managerType;

  private long taskId;

  public UnsafeCarbonRowPage(boolean[] noDictionaryDimensionMapping,
      boolean[] noDictionarySortColumnMapping, int dimensionSize, int measureSize, DataType[] type,
      MemoryBlock memoryBlock, boolean saveToDisk, long taskId) {
    this.noDictionaryDimensionMapping = noDictionaryDimensionMapping;
    this.noDictionarySortColumnMapping = noDictionarySortColumnMapping;
    this.dimensionSize = dimensionSize;
    this.measureSize = measureSize;
    this.measureDataType = type;
    this.saveToDisk = saveToDisk;
    this.nullSetWords = new long[((measureSize - 1) >> 6) + 1];
    this.taskId = taskId;
    buffer = new IntPointerBuffer(this.taskId);
    this.dataBlock = memoryBlock;
    // TODO Only using 98% of space for safe side.May be we can have different logic.
    sizeToBeUsed = dataBlock.size() - (dataBlock.size() * 5) / 100;
    this.managerType = MemoryManagerType.UNSAFE_MEMORY_MANAGER;
  }

  public int addRow(Object[] row) {
    int size = addRow(row, dataBlock.getBaseOffset() + lastSize);
    buffer.set(lastSize);
    lastSize = lastSize + size;
    return size;
  }

  private int addRow(Object[] row, long address) {
    if (row == null) {
      throw new RuntimeException("Row is null ??");
    }
    int dimCount = 0;
    int size = 0;
    Object baseObject = dataBlock.getBaseObject();
    for (; dimCount < noDictionaryDimensionMapping.length; dimCount++) {
      if (noDictionaryDimensionMapping[dimCount]) {
        byte[] col = (byte[]) row[dimCount];
        CarbonUnsafe.getUnsafe()
            .putShort(baseObject, address + size, (short) col.length);
        size += 2;
        CarbonUnsafe.getUnsafe().copyMemory(col, CarbonUnsafe.BYTE_ARRAY_OFFSET, baseObject,
            address + size, col.length);
        size += col.length;
      } else {
        int value = (int) row[dimCount];
        CarbonUnsafe.getUnsafe().putInt(baseObject, address + size, value);
        size += 4;
      }
    }

    // write complex dimensions here.
    for (; dimCount < dimensionSize; dimCount++) {
      byte[] col = (byte[]) row[dimCount];
      CarbonUnsafe.getUnsafe().putShort(baseObject, address + size, (short) col.length);
      size += 2;
      CarbonUnsafe.getUnsafe().copyMemory(col, CarbonUnsafe.BYTE_ARRAY_OFFSET, baseObject,
          address + size, col.length);
      size += col.length;
    }
    Arrays.fill(nullSetWords, 0);
    int nullSetSize = nullSetWords.length * 8;
    int nullWordLoc = size;
    size += nullSetSize;
    for (int mesCount = 0; mesCount < measureSize; mesCount++) {
      Object value = row[mesCount + dimensionSize];
      if (null != value) {
        DataType dataType = measureDataType[mesCount];
        if (dataType == DataTypes.BOOLEAN) {
          Boolean bval = (Boolean) value;
          CarbonUnsafe.getUnsafe().putBoolean(baseObject, address + size, bval);
          size += 1;
        } else if (dataType == DataTypes.SHORT) {
          Short sval = (Short) value;
          CarbonUnsafe.getUnsafe().putShort(baseObject, address + size, sval);
          size += 2;
        } else if (dataType == DataTypes.INT) {
          Integer ival = (Integer) value;
          CarbonUnsafe.getUnsafe().putInt(baseObject, address + size, ival);
          size += 4;
        } else if (dataType == DataTypes.LONG) {
          Long val = (Long) value;
          CarbonUnsafe.getUnsafe().putLong(baseObject, address + size, val);
          size += 8;
        } else if (dataType == DataTypes.DOUBLE) {
          Double doubleVal = (Double) value;
          CarbonUnsafe.getUnsafe().putDouble(baseObject, address + size, doubleVal);
          size += 8;
        } else if (DataTypes.isDecimal(dataType)) {
          BigDecimal decimalVal = (BigDecimal) value;
          byte[] bigDecimalInBytes = DataTypeUtil.bigDecimalToByte(decimalVal);
          CarbonUnsafe.getUnsafe()
              .putShort(baseObject, address + size, (short) bigDecimalInBytes.length);
          size += 2;
          CarbonUnsafe.getUnsafe()
              .copyMemory(bigDecimalInBytes, CarbonUnsafe.BYTE_ARRAY_OFFSET, baseObject,
                  address + size, bigDecimalInBytes.length);
          size += bigDecimalInBytes.length;
        } else {
          throw new IllegalArgumentException("unsupported data type:" + measureDataType[mesCount]);
        }
        set(nullSetWords, mesCount);
      } else {
        unset(nullSetWords, mesCount);
      }
    }
    CarbonUnsafe.getUnsafe().copyMemory(nullSetWords, CarbonUnsafe.LONG_ARRAY_OFFSET, baseObject,
        address + nullWordLoc, nullSetSize);
    return size;
  }

  public Object[] getRow(long address, Object[] rowToFill) {
    int dimCount = 0;
    int size = 0;

    Object baseObject = dataBlock.getBaseObject();
    for (; dimCount < noDictionaryDimensionMapping.length; dimCount++) {
      if (noDictionaryDimensionMapping[dimCount]) {
        short aShort = CarbonUnsafe.getUnsafe().getShort(baseObject, address + size);
        byte[] col = new byte[aShort];
        size += 2;
        CarbonUnsafe.getUnsafe()
            .copyMemory(baseObject, address + size, col, CarbonUnsafe.BYTE_ARRAY_OFFSET,
                col.length);
        size += col.length;
        rowToFill[dimCount] = col;
      } else {
        int anInt = CarbonUnsafe.getUnsafe().getInt(baseObject, address + size);
        size += 4;
        rowToFill[dimCount] = anInt;
      }
    }

    // write complex dimensions here.
    for (; dimCount < dimensionSize; dimCount++) {
      short aShort = CarbonUnsafe.getUnsafe().getShort(baseObject, address + size);
      byte[] col = new byte[aShort];
      size += 2;
      CarbonUnsafe.getUnsafe()
          .copyMemory(baseObject, address + size, col, CarbonUnsafe.BYTE_ARRAY_OFFSET, col.length);
      size += col.length;
      rowToFill[dimCount] = col;
    }

    int nullSetSize = nullSetWords.length * 8;
    Arrays.fill(nullSetWords, 0);
    CarbonUnsafe.getUnsafe()
        .copyMemory(baseObject, address + size, nullSetWords, CarbonUnsafe.LONG_ARRAY_OFFSET,
            nullSetSize);
    size += nullSetSize;

    for (int mesCount = 0; mesCount < measureSize; mesCount++) {
      if (isSet(nullSetWords, mesCount)) {
        DataType dataType = measureDataType[mesCount];
        if (dataType == DataTypes.BOOLEAN) {
          Boolean bval = CarbonUnsafe.getUnsafe().getBoolean(baseObject, address + size);
          size += 1;
          rowToFill[dimensionSize + mesCount] = bval;
        } else if (dataType == DataTypes.SHORT) {
          Short sval = CarbonUnsafe.getUnsafe().getShort(baseObject, address + size);
          size += 2;
          rowToFill[dimensionSize + mesCount] = sval;
        } else if (dataType == DataTypes.INT) {
          Integer ival = CarbonUnsafe.getUnsafe().getInt(baseObject, address + size);
          size += 4;
          rowToFill[dimensionSize + mesCount] = ival;
        } else if (dataType == DataTypes.LONG) {
          Long val = CarbonUnsafe.getUnsafe().getLong(baseObject, address + size);
          size += 8;
          rowToFill[dimensionSize + mesCount] = val;
        } else if (dataType == DataTypes.DOUBLE) {
          Double doubleVal = CarbonUnsafe.getUnsafe().getDouble(baseObject, address + size);
          size += 8;
          rowToFill[dimensionSize + mesCount] = doubleVal;
        } else if (DataTypes.isDecimal(dataType)) {
          short aShort = CarbonUnsafe.getUnsafe().getShort(baseObject, address + size);
          byte[] bigDecimalInBytes = new byte[aShort];
          size += 2;
          CarbonUnsafe.getUnsafe().copyMemory(baseObject, address + size, bigDecimalInBytes,
              CarbonUnsafe.BYTE_ARRAY_OFFSET, bigDecimalInBytes.length);
          size += bigDecimalInBytes.length;
          rowToFill[dimensionSize + mesCount] = DataTypeUtil.byteToBigDecimal(bigDecimalInBytes);
        } else {
          throw new IllegalArgumentException("unsupported data type:" + measureDataType[mesCount]);
        }
      } else {
        rowToFill[dimensionSize + mesCount] = null;
      }
    }
    return rowToFill;
  }

  public void fillRow(long address, DataOutputStream stream) throws IOException {
    int dimCount = 0;
    int size = 0;

    Object baseObject = dataBlock.getBaseObject();
    for (; dimCount < noDictionaryDimensionMapping.length; dimCount++) {
      if (noDictionaryDimensionMapping[dimCount]) {
        short aShort = CarbonUnsafe.getUnsafe().getShort(baseObject, address + size);
        byte[] col = new byte[aShort];
        size += 2;
        CarbonUnsafe.getUnsafe()
            .copyMemory(baseObject, address + size, col, CarbonUnsafe.BYTE_ARRAY_OFFSET,
                col.length);
        size += col.length;
        stream.writeShort(aShort);
        stream.write(col);
      } else {
        int anInt = CarbonUnsafe.getUnsafe().getInt(baseObject, address + size);
        size += 4;
        stream.writeInt(anInt);
      }
    }

    // write complex dimensions here.
    for (; dimCount < dimensionSize; dimCount++) {
      short aShort = CarbonUnsafe.getUnsafe().getShort(baseObject, address + size);
      byte[] col = new byte[aShort];
      size += 2;
      CarbonUnsafe.getUnsafe()
          .copyMemory(baseObject, address + size, col, CarbonUnsafe.BYTE_ARRAY_OFFSET, col.length);
      size += col.length;
      stream.writeShort(aShort);
      stream.write(col);
    }

    int nullSetSize = nullSetWords.length * 8;
    Arrays.fill(nullSetWords, 0);
    CarbonUnsafe.getUnsafe()
        .copyMemory(baseObject, address + size, nullSetWords, CarbonUnsafe.LONG_ARRAY_OFFSET,
            nullSetSize);
    size += nullSetSize;
    for (int i = 0; i < nullSetWords.length; i++) {
      stream.writeLong(nullSetWords[i]);
    }

    for (int mesCount = 0; mesCount < measureSize; mesCount++) {
      if (isSet(nullSetWords, mesCount)) {
        DataType dataType = measureDataType[mesCount];
        if (dataType == DataTypes.SHORT) {
          short sval = CarbonUnsafe.getUnsafe().getShort(baseObject, address + size);
          size += 2;
          stream.writeShort(sval);
        } else if (dataType == DataTypes.INT) {
          int ival = CarbonUnsafe.getUnsafe().getInt(baseObject, address + size);
          size += 4;
          stream.writeInt(ival);
        } else if (dataType == DataTypes.LONG) {
          long val = CarbonUnsafe.getUnsafe().getLong(baseObject, address + size);
          size += 8;
          stream.writeLong(val);
        } else if (dataType == DataTypes.DOUBLE) {
          double doubleVal = CarbonUnsafe.getUnsafe().getDouble(baseObject, address + size);
          size += 8;
          stream.writeDouble(doubleVal);
        } else if (DataTypes.isDecimal(dataType)) {
          short aShort = CarbonUnsafe.getUnsafe().getShort(baseObject, address + size);
          byte[] bigDecimalInBytes = new byte[aShort];
          size += 2;
          CarbonUnsafe.getUnsafe().copyMemory(baseObject, address + size, bigDecimalInBytes,
              CarbonUnsafe.BYTE_ARRAY_OFFSET, bigDecimalInBytes.length);
          size += bigDecimalInBytes.length;
          stream.writeShort(aShort);
          stream.write(bigDecimalInBytes);
        } else {
          throw new IllegalArgumentException("unsupported data type:" + measureDataType[mesCount]);
        }
      }
    }
  }

  public void freeMemory() {
    switch (managerType) {
      case UNSAFE_MEMORY_MANAGER:
        UnsafeMemoryManager.INSTANCE.freeMemory(taskId, dataBlock);
        break;
      default:
        UnsafeSortMemoryManager.INSTANCE.freeMemory(taskId, dataBlock);
        buffer.freeMemory();
    }
  }

  public boolean isSaveToDisk() {
    return saveToDisk;
  }

  public IntPointerBuffer getBuffer() {
    return buffer;
  }

  public int getUsedSize() {
    return lastSize;
  }

  public boolean canAdd() {
    return lastSize < sizeToBeUsed;
  }

  public MemoryBlock getDataBlock() {
    return dataBlock;
  }

  public static void set(long[] words, int index) {
    int wordOffset = (index >> 6);
    words[wordOffset] |= (1L << index);
  }

  public static void unset(long[] words, int index) {
    int wordOffset = (index >> 6);
    words[wordOffset] &= ~(1L << index);
  }

  public static boolean isSet(long[] words, int index) {
    int wordOffset = (index >> 6);
    return ((words[wordOffset] & (1L << index)) != 0);
  }

  public boolean[] getNoDictionaryDimensionMapping() {
    return noDictionaryDimensionMapping;
  }

  public boolean[] getNoDictionarySortColumnMapping() {
    return noDictionarySortColumnMapping;
  }

  public void setNewDataBlock(MemoryBlock newMemoryBlock) {
    this.dataBlock = newMemoryBlock;
    this.managerType = MemoryManagerType.UNSAFE_SORT_MEMORY_MANAGER;
  }

  public enum MemoryManagerType {
    UNSAFE_MEMORY_MANAGER, UNSAFE_SORT_MEMORY_MANAGER
  }
}
