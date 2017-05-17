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

package org.apache.carbondata.processing.newflow.sort.unsafe;

import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Arrays;

import org.apache.carbondata.core.memory.CarbonUnsafe;
import org.apache.carbondata.core.memory.MemoryBlock;
import org.apache.carbondata.core.metadata.datatype.DataType;
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

  public UnsafeCarbonRowPage(boolean[] noDictionaryDimensionMapping,
      boolean[] noDictionarySortColumnMapping, int dimensionSize, int measureSize, DataType[] type,
      MemoryBlock memoryBlock, boolean saveToDisk) {
    this.noDictionaryDimensionMapping = noDictionaryDimensionMapping;
    this.noDictionarySortColumnMapping = noDictionarySortColumnMapping;
    this.dimensionSize = dimensionSize;
    this.measureSize = measureSize;
    this.measureDataType = type;
    this.saveToDisk = saveToDisk;
    this.nullSetWords = new long[((measureSize - 1) >> 6) + 1];
    buffer = new IntPointerBuffer(memoryBlock);
    this.dataBlock = buffer.getBaseBlock();
    // TODO Only using 98% of space for safe side.May be we can have different logic.
    sizeToBeUsed = dataBlock.size() - (dataBlock.size() * 5) / 100;
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
        CarbonUnsafe.unsafe
            .putShort(baseObject, address + size, (short) col.length);
        size += 2;
        CarbonUnsafe.unsafe.copyMemory(col, CarbonUnsafe.BYTE_ARRAY_OFFSET, baseObject,
            address + size, col.length);
        size += col.length;
      } else {
        int value = (int) row[dimCount];
        CarbonUnsafe.unsafe.putInt(baseObject, address + size, value);
        size += 4;
      }
    }

    // write complex dimensions here.
    for (; dimCount < dimensionSize; dimCount++) {
      byte[] col = (byte[]) row[dimCount];
      CarbonUnsafe.unsafe.putShort(baseObject, address + size, (short) col.length);
      size += 2;
      CarbonUnsafe.unsafe.copyMemory(col, CarbonUnsafe.BYTE_ARRAY_OFFSET, baseObject,
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
        switch (measureDataType[mesCount]) {
          case SHORT:
            Short sval = (Short) value;
            CarbonUnsafe.unsafe.putShort(baseObject, address + size, sval);
            size += 2;
            break;
          case INT:
            Integer ival = (Integer) value;
            CarbonUnsafe.unsafe.putInt(baseObject, address + size, ival);
            size += 4;
            break;
          case LONG:
            Long val = (Long) value;
            CarbonUnsafe.unsafe.putLong(baseObject, address + size, val);
            size += 8;
            break;
          case DOUBLE:
            Double doubleVal = (Double) value;
            CarbonUnsafe.unsafe.putDouble(baseObject, address + size, doubleVal);
            size += 8;
            break;
          case DECIMAL:
            BigDecimal decimalVal = (BigDecimal) value;
            byte[] bigDecimalInBytes = DataTypeUtil.bigDecimalToByte(decimalVal);
            CarbonUnsafe.unsafe.putShort(baseObject, address + size,
                (short) bigDecimalInBytes.length);
            size += 2;
            CarbonUnsafe.unsafe
                .copyMemory(bigDecimalInBytes, CarbonUnsafe.BYTE_ARRAY_OFFSET, baseObject,
                    address + size, bigDecimalInBytes.length);
            size += bigDecimalInBytes.length;
            break;
        }
        set(nullSetWords, mesCount);
      } else {
        unset(nullSetWords, mesCount);
      }
    }
    CarbonUnsafe.unsafe.copyMemory(nullSetWords, CarbonUnsafe.LONG_ARRAY_OFFSET, baseObject,
        address + nullWordLoc, nullSetSize);
    return size;
  }

  public Object[] getRow(long address, Object[] rowToFill) {
    int dimCount = 0;
    int size = 0;

    Object baseObject = dataBlock.getBaseObject();
    for (; dimCount < noDictionaryDimensionMapping.length; dimCount++) {
      if (noDictionaryDimensionMapping[dimCount]) {
        short aShort = CarbonUnsafe.unsafe.getShort(baseObject, address + size);
        byte[] col = new byte[aShort];
        size += 2;
        CarbonUnsafe.unsafe
            .copyMemory(baseObject, address + size, col, CarbonUnsafe.BYTE_ARRAY_OFFSET,
                col.length);
        size += col.length;
        rowToFill[dimCount] = col;
      } else {
        int anInt = CarbonUnsafe.unsafe.getInt(baseObject, address + size);
        size += 4;
        rowToFill[dimCount] = anInt;
      }
    }

    // write complex dimensions here.
    for (; dimCount < dimensionSize; dimCount++) {
      short aShort = CarbonUnsafe.unsafe.getShort(baseObject, address + size);
      byte[] col = new byte[aShort];
      size += 2;
      CarbonUnsafe.unsafe
          .copyMemory(baseObject, address + size, col, CarbonUnsafe.BYTE_ARRAY_OFFSET, col.length);
      size += col.length;
      rowToFill[dimCount] = col;
    }

    int nullSetSize = nullSetWords.length * 8;
    Arrays.fill(nullSetWords, 0);
    CarbonUnsafe.unsafe
        .copyMemory(baseObject, address + size, nullSetWords, CarbonUnsafe.LONG_ARRAY_OFFSET,
            nullSetSize);
    size += nullSetSize;

    for (int mesCount = 0; mesCount < measureSize; mesCount++) {
      if (isSet(nullSetWords, mesCount)) {
        switch (measureDataType[mesCount]) {
          case SHORT:
            Short sval = CarbonUnsafe.unsafe.getShort(baseObject, address + size);
            size += 2;
            rowToFill[dimensionSize + mesCount] = sval;
            break;
          case INT:
            Integer ival = CarbonUnsafe.unsafe.getInt(baseObject, address + size);
            size += 4;
            rowToFill[dimensionSize + mesCount] = ival;
            break;
          case LONG:
            Long val = CarbonUnsafe.unsafe.getLong(baseObject, address + size);
            size += 8;
            rowToFill[dimensionSize + mesCount] = val;
            break;
          case DOUBLE:
            Double doubleVal = CarbonUnsafe.unsafe.getDouble(baseObject, address + size);
            size += 8;
            rowToFill[dimensionSize + mesCount] = doubleVal;
            break;
          case DECIMAL:
            short aShort = CarbonUnsafe.unsafe.getShort(baseObject, address + size);
            byte[] bigDecimalInBytes = new byte[aShort];
            size += 2;
            CarbonUnsafe.unsafe.copyMemory(baseObject, address + size, bigDecimalInBytes,
                CarbonUnsafe.BYTE_ARRAY_OFFSET, bigDecimalInBytes.length);
            size += bigDecimalInBytes.length;
            rowToFill[dimensionSize + mesCount] = bigDecimalInBytes;
            break;
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
        short aShort = CarbonUnsafe.unsafe.getShort(baseObject, address + size);
        byte[] col = new byte[aShort];
        size += 2;
        CarbonUnsafe.unsafe
            .copyMemory(baseObject, address + size, col, CarbonUnsafe.BYTE_ARRAY_OFFSET,
                col.length);
        size += col.length;
        stream.writeShort(aShort);
        stream.write(col);
      } else {
        int anInt = CarbonUnsafe.unsafe.getInt(baseObject, address + size);
        size += 4;
        stream.writeInt(anInt);
      }
    }

    // write complex dimensions here.
    for (; dimCount < dimensionSize; dimCount++) {
      short aShort = CarbonUnsafe.unsafe.getShort(baseObject, address + size);
      byte[] col = new byte[aShort];
      size += 2;
      CarbonUnsafe.unsafe
          .copyMemory(baseObject, address + size, col, CarbonUnsafe.BYTE_ARRAY_OFFSET, col.length);
      size += col.length;
      stream.writeShort(aShort);
      stream.write(col);
    }

    int nullSetSize = nullSetWords.length * 8;
    Arrays.fill(nullSetWords, 0);
    CarbonUnsafe.unsafe
        .copyMemory(baseObject, address + size, nullSetWords, CarbonUnsafe.LONG_ARRAY_OFFSET,
            nullSetSize);
    size += nullSetSize;
    for (int i = 0; i < nullSetWords.length; i++) {
      stream.writeLong(nullSetWords[i]);
    }

    for (int mesCount = 0; mesCount < measureSize; mesCount++) {
      if (isSet(nullSetWords, mesCount)) {
        switch (measureDataType[mesCount]) {
          case SHORT:
            short sval = CarbonUnsafe.unsafe.getShort(baseObject, address + size);
            size += 2;
            stream.writeShort(sval);
            break;
          case INT:
            int ival = CarbonUnsafe.unsafe.getInt(baseObject, address + size);
            size += 4;
            stream.writeInt(ival);
            break;
          case LONG:
            long val = CarbonUnsafe.unsafe.getLong(baseObject, address + size);
            size += 8;
            stream.writeLong(val);
            break;
          case DOUBLE:
            double doubleVal = CarbonUnsafe.unsafe.getDouble(baseObject, address + size);
            size += 8;
            stream.writeDouble(doubleVal);
            break;
          case DECIMAL:
            short aShort = CarbonUnsafe.unsafe.getShort(baseObject, address + size);
            byte[] bigDecimalInBytes = new byte[aShort];
            size += 2;
            CarbonUnsafe.unsafe.copyMemory(baseObject, address + size, bigDecimalInBytes,
                CarbonUnsafe.BYTE_ARRAY_OFFSET, bigDecimalInBytes.length);
            size += bigDecimalInBytes.length;
            stream.writeShort(aShort);
            stream.write(bigDecimalInBytes);
            break;
        }
      }
    }
  }

  public void freeMemory() {
    buffer.freeMemory();
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
}
