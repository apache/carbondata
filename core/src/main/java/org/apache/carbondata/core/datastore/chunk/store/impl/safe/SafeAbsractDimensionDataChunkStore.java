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

package org.apache.carbondata.core.datastore.chunk.store.impl.safe;

import org.apache.carbondata.core.datastore.chunk.store.DimensionDataChunkStore;
import org.apache.carbondata.core.scan.result.vector.CarbonColumnVector;

/**
 * Responsibility is to store dimension data
 */
public abstract class SafeAbsractDimensionDataChunkStore implements DimensionDataChunkStore {

  /**
   * data chunk for dimension column
   */
  protected byte[] data;

  /**
   * inverted index
   */
  protected int[] invertedIndex;

  /**
   * inverted index reverser
   */
  protected int[] invertedIndexReverse;

  /**
   * to check whether dimension column was explicitly sorted or not
   */
  protected boolean isExplictSorted;

  /**
   * Constructor
   *
   * @param isInvertedIdex is inverted index present
   */
  public SafeAbsractDimensionDataChunkStore(boolean isInvertedIdex) {
    this.isExplictSorted = isInvertedIdex;
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
    this.data = data;
    this.invertedIndex = invertedIndex;
    this.invertedIndexReverse = invertedIndexReverse;
  }

  /**
   * Below method will be used to free the memory occupied by the column chunk
   */
  @Override public void freeMemory() {
    // do nothing as GC will take care of freeing memory
  }

  /**
   * Below method will be used to get the inverted index
   *
   * @param rowId row id
   * @return inverted index based on row id passed
   */
  @Override public int getInvertedIndex(int rowId) {
    return invertedIndex[rowId];
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

  @Override public void fillRow(int rowId, CarbonColumnVector vector, int vectorRowId) {
    throw new UnsupportedOperationException("Operation not supported");
  }
}
