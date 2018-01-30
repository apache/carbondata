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

package org.apache.carbondata.core.datastore.chunk.store.impl.unsafe;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.memory.CarbonUnsafe;

/**
 * Below class is responsible to store fixed length dimension data chunk in
 * memory Memory occupied can be on heap or offheap using unsafe interface
 */
public class UnsafeFixedLengthDimensionDataChunkStore
    extends UnsafeAbstractDimensionDataChunkStore {

  /**
   * Size of each value
   */
  private int columnValueSize;

  /**
   * Constructor
   *
   * @param columnValueSize value of each column
   * @param isInvertedIdex  is inverted index present
   * @param numberOfRows    total number of rows
   */
  public UnsafeFixedLengthDimensionDataChunkStore(long totalDataSize, int columnValueSize,
      boolean isInvertedIdex, int numberOfRows) {
    super(totalDataSize, isInvertedIdex, numberOfRows);
    this.columnValueSize = columnValueSize;
  }

  /**
   * Below method will be used to get the row based inverted index
   *
   * @param rowId Inverted index
   */
  @Override public byte[] getRow(int rowId) {
    // if column was explicitly sorted we need to get the rowid based inverted index reverse
    if (isExplicitSorted) {
      rowId = CarbonUnsafe.getUnsafe().getInt(dataPageMemoryBlock.getBaseObject(),
          dataPageMemoryBlock.getBaseOffset() + this.invertedIndexReverseOffset + ((long)rowId
              * CarbonCommonConstants.INT_SIZE_IN_BYTE));
    }
    // creating a row
    byte[] data = new byte[columnValueSize];
    //copy the row from memory block based on offset
    // offset position will be index * each column value length
    CarbonUnsafe.getUnsafe().copyMemory(dataPageMemoryBlock.getBaseObject(),
        dataPageMemoryBlock.getBaseOffset() + ((long)rowId * columnValueSize), data,
        CarbonUnsafe.BYTE_ARRAY_OFFSET, columnValueSize);
    return data;
  }

  /**
   * Below method will be used to get the surrogate key of the based on the row
   * id passed
   *
   * @param index row id
   * @return surrogate key
   */
  @Override public int getSurrogate(int index) {
    // if column was explicitly sorted we need to get the rowid based inverted index reverse
    if (isExplicitSorted) {
      index = CarbonUnsafe.getUnsafe().getInt(dataPageMemoryBlock.getBaseObject(),
          dataPageMemoryBlock.getBaseOffset() + this.invertedIndexReverseOffset + ((long)index
              * CarbonCommonConstants.INT_SIZE_IN_BYTE));
    }
    // below part is to convert the byte array to surrogate value
    int startOffsetOfData = index * columnValueSize;
    int surrogate = 0;
    for (int i = 0; i < columnValueSize; i++) {
      surrogate <<= 8;
      surrogate ^= CarbonUnsafe.getUnsafe().getByte(dataPageMemoryBlock.getBaseObject(),
          dataPageMemoryBlock.getBaseOffset() + startOffsetOfData) & 0xFF;
      startOffsetOfData++;
    }
    return surrogate;
  }

  /**
   * Below method will be used to fill the row values to buffer array
   *
   * @param rowId  row id of the data to be filled
   * @param buffer   buffer in which data will be filled
   * @param offset off the of the buffer
   */
  @Override public void fillRow(int rowId, byte[] buffer, int offset) {
    // if column was explicitly sorted we need to get the rowid based inverted index reverse
    if (isExplicitSorted) {
      rowId = CarbonUnsafe.getUnsafe().getInt(dataPageMemoryBlock.getBaseObject(),
          dataPageMemoryBlock.getBaseOffset() + this.invertedIndexReverseOffset + ((long)rowId
              * CarbonCommonConstants.INT_SIZE_IN_BYTE));
    }
    //copy the row from memory block based on offset
    // offset position will be index * each column value length
    CarbonUnsafe.getUnsafe().copyMemory(dataPageMemoryBlock.getBaseObject(),
        dataPageMemoryBlock.getBaseOffset() + ((long)rowId * columnValueSize), buffer,
        CarbonUnsafe.BYTE_ARRAY_OFFSET + offset, columnValueSize);
  }

  /**
   * @return size of each column value
   */
  @Override public int getColumnValueSize() {
    return columnValueSize;
  }

  /**
   * to compare the two byte array
   *
   * @param rowId        index of first byte array
   * @param compareValue value of to be compared
   * @return compare result
   */
  @Override public int compareTo(int rowId, byte[] compareValue) {
    // based on index we need to calculate the actual position in memory block
    rowId = rowId * columnValueSize;
    int compareResult = 0;
    for (int i = 0; i < compareValue.length; i++) {
      compareResult = (CarbonUnsafe.getUnsafe()
          .getByte(dataPageMemoryBlock.getBaseObject(), dataPageMemoryBlock.getBaseOffset() + rowId)
          & 0xff) - (compareValue[i] & 0xff);
      if (compareResult != 0) {
        break;
      }
      rowId++;
    }
    return compareResult;
  }
}