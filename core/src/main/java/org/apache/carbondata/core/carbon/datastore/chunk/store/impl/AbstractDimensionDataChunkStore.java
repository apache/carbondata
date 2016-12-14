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

import org.apache.carbondata.core.carbon.datastore.chunk.store.DimensionDataChunkStore;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.memory.MemoryAllocatorFactory;
import org.apache.carbondata.core.memory.MemoryBlock;
import org.apache.carbondata.core.unsafe.CarbonUnsafe;

/**
 * Responsibility is to store dimension data in memory.
 * storage can be on heap or offheap.
 */
public abstract class AbstractDimensionDataChunkStore implements DimensionDataChunkStore {

  /**
   * memory block for data page
   */
  protected MemoryBlock dataPageMemoryBlock;

  /**
   * memory block for inverted index
   */
  protected MemoryBlock invertedIndexMemoryBlock;

  /**
   * memory block for inverted index reverse
   */
  protected MemoryBlock invertedIndexReverseMemoryBlock;

  /**
   * to check whether dimension column was explicitly sorted or not
   */
  protected boolean isExplictSorted;

  /**
   * is memory released
   */
  protected boolean isMemoryReleased;

  /**
   * Constructor
   *
   * @param totalSize      total size of the data to be kept
   * @param isInvertedIdex is inverted index present
   * @param numberOfRows   total number of rows
   */
  public AbstractDimensionDataChunkStore(int totalSize, boolean isInvertedIdex, int numberOfRows) {
    // allocating the data page
    this.dataPageMemoryBlock =
        MemoryAllocatorFactory.INSATANCE.getMemoryAllocator().allocate(totalSize);
    this.isExplictSorted = isInvertedIdex;
    if (isInvertedIdex) {
      // allocating the inverted index page memory
      invertedIndexMemoryBlock = MemoryAllocatorFactory.INSATANCE.getMemoryAllocator()
          .allocate(CarbonCommonConstants.INT_SIZE_IN_BYTE * numberOfRows);
      // allocating the inverted index reverese page memory
      invertedIndexReverseMemoryBlock = MemoryAllocatorFactory.INSATANCE.getMemoryAllocator()
          .allocate(CarbonCommonConstants.INT_SIZE_IN_BYTE * numberOfRows);
    }
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
    // copy the data to memory
    CarbonUnsafe.unsafe
        .copyMemory(data, CarbonUnsafe.BYTE_ARRAY_OFFSET, dataPageMemoryBlock.getBaseObject(),
            dataPageMemoryBlock.getBaseOffset(), dataPageMemoryBlock.size());
    // if inverted index is present then copy the inverted index
    // and reverse inverted index to memory
    if (isExplictSorted) {
      CarbonUnsafe.unsafe.copyMemory(invertedIndex, CarbonUnsafe.INT_ARRAY_OFFSET,
          invertedIndexMemoryBlock.getBaseObject(), invertedIndexMemoryBlock.getBaseOffset(),
          invertedIndexReverseMemoryBlock.size());
      CarbonUnsafe.unsafe.copyMemory(invertedIndexReverse, CarbonUnsafe.INT_ARRAY_OFFSET,
          invertedIndexReverseMemoryBlock.getBaseObject(),
          invertedIndexReverseMemoryBlock.getBaseOffset(), invertedIndexReverseMemoryBlock.size());
    }
  }

  /**
   * Below method will be used to free the memory occupied by
   * the column chunk
   */
  @Override public void freeMemory() {
    if (isMemoryReleased) {
      return;
    }
    // free data page memory
    MemoryAllocatorFactory.INSATANCE.getMemoryAllocator().free(dataPageMemoryBlock);
    if (isExplictSorted) {
      MemoryAllocatorFactory.INSATANCE.getMemoryAllocator().free(invertedIndexMemoryBlock);
      MemoryAllocatorFactory.INSATANCE.getMemoryAllocator().free(invertedIndexReverseMemoryBlock);
    }
    isMemoryReleased = true;
  }

  /**
   * Below method will be used to get the inverted index
   *
   * @param rowId row id
   * @return inverted index based on row id passed
   */
  @Override public int getInvertedIndex(int rowId) {
    return CarbonUnsafe.unsafe.getInt(invertedIndexMemoryBlock.getBaseObject(),
        invertedIndexMemoryBlock.getBaseOffset() + (rowId
            * CarbonCommonConstants.INT_SIZE_IN_BYTE));
  }

  /**
   * Below method will be used to get the surrogate key of the
   * based on the row id passed
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
  @Override public boolean isExplictSorted() {
    return isExplictSorted;
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

}
