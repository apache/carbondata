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

package org.apache.carbondata.core.datastore.chunk.store.impl;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.chunk.store.DimensionDataChunkStore;
import org.apache.carbondata.core.scan.result.vector.CarbonColumnVector;
import org.apache.carbondata.core.scan.result.vector.CarbonDictionary;
import org.apache.carbondata.core.scan.result.vector.ColumnVectorInfo;
import org.apache.carbondata.core.util.CarbonUtil;

/**
 * Dimension chunk store for local dictionary encoded data.
 * It's a decorator over dimension chunk store
 */
public class LocalDictDimensionDataChunkStore implements DimensionDataChunkStore {

  private DimensionDataChunkStore dimensionDataChunkStore;

  private CarbonDictionary dictionary;

  public LocalDictDimensionDataChunkStore(DimensionDataChunkStore dimensionDataChunkStore,
      CarbonDictionary dictionary) {
    this.dimensionDataChunkStore = dimensionDataChunkStore;
    this.dictionary = dictionary;
  }

  /**
   * Below method will be used to put the rows and its metadata in offheap
   *
   * @param invertedIndex        inverted index to be stored
   * @param invertedIndexReverse inverted index reverse to be stored
   * @param data                 data to be stored
   */
  public void putArray(int[] invertedIndex, int[] invertedIndexReverse, byte[] data) {
    this.dimensionDataChunkStore.putArray(invertedIndex, invertedIndexReverse, data);
  }

  @Override
  public void fillVector(int[] invertedIndex, int[] invertedIndexReverse, byte[] data,
      ColumnVectorInfo vectorInfo) {
    int columnValueSize = dimensionDataChunkStore.getColumnValueSize();
    int rowsNum = data.length / columnValueSize;
    CarbonColumnVector vector = vectorInfo.vector;
    if (!dictionary.isDictionaryUsed()) {
      vector.setDictionary(dictionary);
      dictionary.setDictionaryUsed();
    }
    for (int i = 0; i < rowsNum; i++) {
      int surrogate = CarbonUtil.getSurrogateInternal(data, i * columnValueSize, columnValueSize);
      if (surrogate == CarbonCommonConstants.MEMBER_DEFAULT_VAL_SURROGATE_KEY) {
        vector.putNull(i);
        vector.getDictionaryVector().putNull(i);
      } else {
        vector.putNotNull(i);
        vector.getDictionaryVector().putInt(i, surrogate);
      }

    }
  }

  @Override public byte[] getRow(int rowId) {
    return dictionary.getDictionaryValue(dimensionDataChunkStore.getSurrogate(rowId));
  }

  @Override public void fillRow(int rowId, CarbonColumnVector vector, int vectorRow) {
    if (!dictionary.isDictionaryUsed()) {
      vector.setDictionary(dictionary);
      dictionary.setDictionaryUsed();
    }
    int surrogate = dimensionDataChunkStore.getSurrogate(rowId);
    if (surrogate == CarbonCommonConstants.MEMBER_DEFAULT_VAL_SURROGATE_KEY) {
      vector.putNull(vectorRow);
      vector.getDictionaryVector().putNull(vectorRow);
      return;
    }
    vector.putNotNull(vectorRow);
    vector.getDictionaryVector().putInt(vectorRow, dimensionDataChunkStore.getSurrogate(rowId));
  }

  @Override public void fillRow(int rowId, byte[] buffer, int offset) {
    throw new UnsupportedOperationException("Operation not supported");
  }

  @Override public int getInvertedIndex(int rowId) {
    return this.dimensionDataChunkStore.getInvertedIndex(rowId);
  }

  @Override public int getInvertedReverseIndex(int rowId) {
    return this.dimensionDataChunkStore.getInvertedReverseIndex(rowId);
  }

  @Override public int getSurrogate(int rowId) {
    throw new UnsupportedOperationException("Operation not supported");
  }

  @Override public int getColumnValueSize() {
    throw new UnsupportedOperationException("Operation not supported");
  }

  @Override public boolean isExplicitSorted() {
    return this.dimensionDataChunkStore.isExplicitSorted();
  }

  @Override public int compareTo(int rowId, byte[] compareValue) {
    return dimensionDataChunkStore.compareTo(rowId, compareValue);
  }

  /**
   * Below method will be used to free the memory occupied by the column chunk
   */
  @Override public void freeMemory() {
    if (null != dimensionDataChunkStore) {
      this.dimensionDataChunkStore.freeMemory();
      this.dictionary = null;
      this.dimensionDataChunkStore = null;
    }
  }
}
