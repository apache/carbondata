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
import org.apache.carbondata.core.datastore.chunk.store.DimensionDataChunkStore;
import org.apache.carbondata.core.memory.CarbonUnsafe;
import org.apache.carbondata.core.memory.MemoryBlock;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.memory.UnsafeMemoryManager;
import org.apache.carbondata.core.scan.result.vector.CarbonColumnVector;
import org.apache.carbondata.core.scan.result.vector.ColumnVectorInfo;
import org.apache.carbondata.core.util.ThreadLocalTaskInfo;

/**
 * Responsibility is to store dimension data in memory. storage can be on heap
 * or offheap.
 */
public abstract class UnsafeAbstractDimensionDataChunkStore implements DimensionDataChunkStore {

  /**
   * memory block for data page
   */
  protected MemoryBlock dataPageMemoryBlock;

  /**
   * to check whether dimension column was explicitly sorted or not
   */
  protected boolean isExplicitSorted;

  /**
   * is memory released
   */
  protected boolean isMemoryReleased;

  /**
   * length of the actual data
   */
  protected int dataLength;

  /**
   * offset of the inverted index reverse
   */
  protected long invertedIndexReverseOffset;

  /**
   * to validate whether data is already kept in memory or not
   */
  protected boolean isMemoryOccupied;

  private final String taskId = ThreadLocalTaskInfo.getCarbonTaskInfo().getTaskId();

  /**
   * Constructor
   *
   * @param totalSize      total size of the data to be kept
   * @param isInvertedIdex is inverted index present
   * @param numberOfRows   total number of rows
   */
  public UnsafeAbstractDimensionDataChunkStore(long totalSize, boolean isInvertedIdex,
      int numberOfRows) {
    try {
      // allocating the data page
      this.dataPageMemoryBlock = UnsafeMemoryManager.allocateMemoryWithRetry(taskId, totalSize);
    } catch (MemoryException e) {
      throw new RuntimeException(e);
    }
    this.isExplicitSorted = isInvertedIdex;
  }

  /**
   * Below method will be used to put the rows and its metadata in offheap
   *
   * @param invertedIndex        inverted index to be stored
   * @param invertedIndexReverse inverted index reverse to be stored
   * @param data                 data to be stored
   */
  @Override public void putArray(final int[] invertedIndex, final int[] invertedIndexReverse,
      final byte[] data) {
    assert (!isMemoryOccupied);
    this.dataLength = data.length;
    this.invertedIndexReverseOffset = dataLength;
    if (isExplicitSorted) {
      this.invertedIndexReverseOffset +=
          invertedIndex.length * CarbonCommonConstants.INT_SIZE_IN_BYTE;
    }
    // copy the data to memory
    CarbonUnsafe.getUnsafe()
        .copyMemory(data, CarbonUnsafe.BYTE_ARRAY_OFFSET, dataPageMemoryBlock.getBaseObject(),
            dataPageMemoryBlock.getBaseOffset(), this.dataLength);
    // if inverted index is present then copy the inverted index
    // and reverse inverted index to memory
    if (isExplicitSorted) {
      CarbonUnsafe.getUnsafe().copyMemory(invertedIndex, CarbonUnsafe.INT_ARRAY_OFFSET,
          dataPageMemoryBlock.getBaseObject(), dataPageMemoryBlock.getBaseOffset() + dataLength,
          invertedIndex.length * CarbonCommonConstants.INT_SIZE_IN_BYTE);
      CarbonUnsafe.getUnsafe().copyMemory(invertedIndexReverse, CarbonUnsafe.INT_ARRAY_OFFSET,
          dataPageMemoryBlock.getBaseObject(),
          dataPageMemoryBlock.getBaseOffset() + this.invertedIndexReverseOffset,
          invertedIndexReverse.length * CarbonCommonConstants.INT_SIZE_IN_BYTE);
    }
  }

  @Override public void fillVector(int[] invertedIndex, int[] invertedIndexReverse, byte[] data,
      ColumnVectorInfo vectorInfo) {
    throw new UnsupportedOperationException("This method not supposed to be called here");
  }

  /**
   * Below method will be used to free the memory occupied by the column chunk
   */
  @Override public void freeMemory() {
    if (isMemoryReleased) {
      return;
    }
    // free data page memory
    UnsafeMemoryManager.INSTANCE.freeMemory(taskId, dataPageMemoryBlock);
    isMemoryReleased = true;
    this.dataPageMemoryBlock = null;
    this.isMemoryOccupied = false;
  }

  /**
   * Below method will be used to get the inverted index
   *
   * @param rowId row id
   * @return inverted index based on row id passed
   */
  @Override public int getInvertedIndex(int rowId) {
    return CarbonUnsafe.getUnsafe().getInt(dataPageMemoryBlock.getBaseObject(),
        dataPageMemoryBlock.getBaseOffset() + dataLength + ((long)rowId
            * CarbonCommonConstants.INT_SIZE_IN_BYTE));
  }

  /**
   * Below method will be used to get the reverse inverted index
   *
   * @param rowId row id
   * @return inverted index based on row id passed
   */
  @Override public int getInvertedReverseIndex(int rowId) {
    return CarbonUnsafe.getUnsafe().getInt(dataPageMemoryBlock.getBaseObject(),
        dataPageMemoryBlock.getBaseOffset() + this.invertedIndexReverseOffset + ((long)rowId
            * CarbonCommonConstants.INT_SIZE_IN_BYTE));
  }

  /**
   * Below method will be used to get the surrogate key of the based on the row
   * id passed
   *
   * @param rowId row id
   * @return surrogate key
   */
  @Override public int getSurrogate(int rowId) {
    throw new UnsupportedOperationException("Operation not supported");
  }

  /**
   * @return size of each column value
   */
  @Override public int getColumnValueSize() {
    throw new UnsupportedOperationException("Operation not supported");
  }

  /**
   * @return whether column was explicitly sorted or not
   */
  @Override public boolean isExplicitSorted() {
    return isExplicitSorted;
  }

  /**
   * Below method will be used to fill the row values to data array
   *
   * @param rowId  row id of the data to be filled
   * @param data   buffer in which data will be filled
   * @param offset off the of the buffer
   */
  @Override public void fillRow(int rowId, byte[] data, int offset) {
    throw new UnsupportedOperationException("Operation not supported");
  }

  @Override public void fillRow(int rowId, CarbonColumnVector vector, int vectorRow) {
    throw new UnsupportedOperationException("Operation not supported");
  }
}
