/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.carbondata.core.carbon.datastore.chunk.store.impl;

import java.nio.ByteBuffer;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.memory.MemoryAllocatorFactory;
import org.apache.carbondata.core.memory.MemoryBlock;
import org.apache.carbondata.core.unsafe.CarbonUnsafe;

/**
 * Below class is responsible to store fixed length dimension data chunk in memory
 * Memory occupied can be on heap or offheap
 */
public class VariableLengthDimesionDataChunkStore extends AbstractDimensionDataChunkStore {

  /**
   * pointer of the memory data block
   */
  private MemoryBlock dataOffsetMemoryBlock;

  /**
   * total number of rows
   */
  private int numberOfRows;

  public VariableLengthDimesionDataChunkStore(int totalSize, boolean isInvertedIdex,
      int numberOfRows) {
    super(totalSize, isInvertedIdex, numberOfRows);
    // creating a memory block for storing pointers
    dataOffsetMemoryBlock = MemoryAllocatorFactory.INSATANCE.getMemoryAllocator()
        .allocate(CarbonCommonConstants.LONG_SIZE_IN_BYTE * numberOfRows);
    this.numberOfRows = numberOfRows;
  }

  /**
   * Below method will be used to put the rows and its metadata in offheap
   *
   * @param invertedIndex        inverted index to be stored
   * @param invertedIndexReverse inverted index reverse to be stored
   * @param data                 data to be stored
   */
  @Override public void putArray(final int[] invertedIndex, final int[] invertedIndexReverse,
      byte[] data) {
    // first put the data, inverted index and reverse inverted index to memory
    super.putArray(invertedIndex, invertedIndexReverse, data);
    // As data is of variable length and data format is
    //<length in short><data><length in short><data>
    // we need to store offset of each data so data can be accessed directly
    long startOffset = dataPageMemoryBlock.getBaseOffset();
    CarbonUnsafe.unsafe
        .putLong(dataOffsetMemoryBlock.getBaseObject(), dataOffsetMemoryBlock.getBaseOffset(),
            startOffset);
    ByteBuffer buffer = ByteBuffer.allocate(CarbonCommonConstants.SHORT_SIZE_IN_BYTE);
    byte[] length = new byte[CarbonCommonConstants.SHORT_SIZE_IN_BYTE];
    for (int i = 1; i < numberOfRows; i++) {
      CarbonUnsafe.unsafe.copyMemory(dataPageMemoryBlock.getBaseObject(), startOffset, length,
          CarbonUnsafe.BYTE_ARRAY_OFFSET, CarbonCommonConstants.SHORT_SIZE_IN_BYTE);
      buffer.put(length);
      buffer.flip();
      startOffset = startOffset + buffer.getShort() + CarbonCommonConstants.SHORT_SIZE_IN_BYTE;
      buffer.clear();
      CarbonUnsafe.unsafe.putLong(dataOffsetMemoryBlock.getBaseObject(),
          dataOffsetMemoryBlock.getBaseOffset() + (i * CarbonCommonConstants.LONG_SIZE_IN_BYTE),
          startOffset);
    }
  }

  /**
   * Below method will be used to get the row
   * based on row id passed
   *
   * @param index
   * @return row
   */
  @Override public byte[] getRow(int rowId) {
    if (isExplictSorted) {
      rowId = CarbonUnsafe.unsafe.getInt(invertedIndexReverseMemoryBlock.getBaseObject(),
          invertedIndexReverseMemoryBlock.getBaseOffset() + (rowId
              * CarbonCommonConstants.INT_SIZE_IN_BYTE));
    }
    // get the offset of set of data
    long currentDataOffset = CarbonUnsafe.unsafe.getLong(dataOffsetMemoryBlock.getBaseObject(),
        dataOffsetMemoryBlock.getBaseOffset() + ((long) rowId
            * CarbonCommonConstants.LONG_SIZE_IN_BYTE)) + CarbonCommonConstants.SHORT_SIZE_IN_BYTE;
    short length = 0;
    // calculating the length of data
    if (rowId < numberOfRows - 1) {
      long OffsetOfNextdata = CarbonUnsafe.unsafe.getLong(dataOffsetMemoryBlock.getBaseObject(),
          dataOffsetMemoryBlock.getBaseOffset() + ((rowId + 1)
              * CarbonCommonConstants.LONG_SIZE_IN_BYTE));
      length = (short) (OffsetOfNextdata - currentDataOffset);
    } else {
      // for last record
      length = (short) (dataPageMemoryBlock.getBaseOffset() + dataPageMemoryBlock.size()
          - currentDataOffset);
    }
    byte[] data = new byte[length];
    CarbonUnsafe.unsafe.copyMemory(dataPageMemoryBlock.getBaseObject(), currentDataOffset, data,
        CarbonUnsafe.BYTE_ARRAY_OFFSET, length);
    return data;
  }

  /**
   * Below method will be used to free the memory occupied by
   * the column chunk
   */
  @Override public void freeMemory() {
    if (isMemoryReleased) {
      return;
    }
    // first free the data page memory
    super.freeMemory();
    // freeing the offset memory
    MemoryAllocatorFactory.INSATANCE.getMemoryAllocator().free(dataOffsetMemoryBlock);
  }

  /**
   * to compare the two byte array
   *
   * @param index        index of first byte array
   * @param compareValue value of to be compared
   * @return compare result
   */
  @Override public int compareTo(int index, byte[] compareValue) {
    // get the offset of set of data
    long currentDataOffset = CarbonUnsafe.unsafe.getLong(dataOffsetMemoryBlock.getBaseObject(),
        dataOffsetMemoryBlock.getBaseOffset() + ((long) index
            * CarbonCommonConstants.LONG_SIZE_IN_BYTE)) + CarbonCommonConstants.SHORT_SIZE_IN_BYTE;
    short length = 0;
    // calculating the length of data
    if (index < numberOfRows - 1) {
      long OffsetOfNextdata = CarbonUnsafe.unsafe.getLong(dataOffsetMemoryBlock.getBaseObject(),
          dataOffsetMemoryBlock.getBaseOffset() + ((index + 1)
              * CarbonCommonConstants.LONG_SIZE_IN_BYTE));
      length = (short) (OffsetOfNextdata - currentDataOffset);
    } else {
      // for last record
      length = (short) (dataPageMemoryBlock.getBaseOffset() + dataPageMemoryBlock.size()
          - currentDataOffset);
    }

    int compareResult = 0;
    for (int i = 0; i < compareValue.length; i++) {
      compareResult =
          (CarbonUnsafe.unsafe.getByte(dataPageMemoryBlock.getBaseObject(), currentDataOffset)
              & 0xff) - (compareValue[i] & 0xff);
      if (compareResult != 0) {
        break;
      }
      currentDataOffset++;
    }
    return compareResult;
  }

}
