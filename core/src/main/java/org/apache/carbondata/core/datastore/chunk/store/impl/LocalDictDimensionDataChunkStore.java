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

import java.util.Arrays;
import java.util.BitSet;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.chunk.store.DimensionDataChunkStore;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.scan.result.vector.CarbonColumnVector;
import org.apache.carbondata.core.scan.result.vector.CarbonDictionary;
import org.apache.carbondata.core.scan.result.vector.ColumnVectorInfo;
import org.apache.carbondata.core.scan.result.vector.impl.CarbonColumnVectorImpl;
import org.apache.carbondata.core.scan.result.vector.impl.directread.ColumnarVectorWrapperDirectFactory;
import org.apache.carbondata.core.scan.result.vector.impl.directread.ConvertibleVector;
import org.apache.carbondata.core.util.CarbonUtil;

/**
 * Dimension chunk store for local dictionary encoded data.
 * It's a decorator over dimension chunk store
 */
public class LocalDictDimensionDataChunkStore implements DimensionDataChunkStore {

  private DimensionDataChunkStore dimensionDataChunkStore;

  private CarbonDictionary dictionary;

  private int dataLength;

  public LocalDictDimensionDataChunkStore(DimensionDataChunkStore dimensionDataChunkStore,
      CarbonDictionary dictionary, int dataLength) {
    this.dimensionDataChunkStore = dimensionDataChunkStore;
    this.dictionary = dictionary;
    this.dataLength = dataLength;
  }

  /**
   * Below method will be used to put the rows and its metadata in off-heap
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
    int rowsNum = dataLength / columnValueSize;
    CarbonColumnVector vector = vectorInfo.vector;
    if (vector.getType().isComplexType()) {
      if (DataTypes.isStructType(vector.getType())) {
        int deletedRow = vectorInfo.deletedRows != null ? vectorInfo.deletedRows.cardinality() : 0;
        rowsNum = dataLength - deletedRow;
      } else {
        // this is not required to be changed in the case of the array because
        // datalength of the array already taking care of deleted rows in
        // ColumnVectorInfo.getUpdatedPageSizeForChildVector
        rowsNum = dataLength;
      }
      vector = vectorInfo.vectorStack.peek();
      CarbonColumnVector sliceVector = vector.getColumnVector();
      // use rowsNum as positionCount in order to create dictionary block
      sliceVector.setPositionCount(rowsNum);
      sliceVector.setIsLocalDictEnabledForComplextype(true);
    }
    if (!dictionary.isDictionaryUsed()) {
      vector.setDictionary(dictionary);
      dictionary.setDictionaryUsed();
    }
    BitSet nullBitset = new BitSet();
    CarbonColumnVector dictionaryVector = ColumnarVectorWrapperDirectFactory
        .getDirectVectorWrapperFactory(vectorInfo, vector.getDictionaryVector(), invertedIndex,
            nullBitset, vectorInfo.deletedRows, false, true);
    vector = ColumnarVectorWrapperDirectFactory
        .getDirectVectorWrapperFactory(vectorInfo, vector, invertedIndex, nullBitset,
            vectorInfo.deletedRows, false, false);
    // this check is in case of array of string type
    if (vectorInfo.vector.getType().isComplexType()
        && dictionaryVector instanceof CarbonColumnVectorImpl
        && ((CarbonColumnVectorImpl) dictionaryVector).getIntArraySize() < rowsNum) {
      ((CarbonColumnVectorImpl) dictionaryVector).increaseIntArraySize(rowsNum);
    }
    for (int i = 0; i < rowsNum; i++) {
      int surrogate = CarbonUtil.getSurrogateInternal(data, i * columnValueSize, columnValueSize);
      // If complex string primitive value is null then surrogate will be unequal to
      // MEMBER_DEFAULT_VAL_SURROGATE_KEY. Therefore check should be using MEMBER_DEFAULT_VAL_ARRAY
      if (surrogate == CarbonCommonConstants.MEMBER_DEFAULT_VAL_SURROGATE_KEY || Arrays
          .equals(CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY,
              dictionary.getDictionaryValue(surrogate))) {
        vector.putNull(i);
        dictionaryVector.putNull(i);
      } else {
        // if vector is 'ColumnarVectorWrapperDirectWithDeleteDelta', it needs to call 'putNotNull'
        // to increase 'counter', otherwise it will set the null value to the wrong index.
        vector.putNotNull(i);
        dictionaryVector.putInt(i, surrogate);
      }
    }
    if (dictionaryVector instanceof ConvertibleVector) {
      ((ConvertibleVector) dictionaryVector).convert();
    }
  }

  @Override
  public byte[] getRow(int rowId) {
    return dictionary.getDictionaryValue(dimensionDataChunkStore.getSurrogate(rowId));
  }

  @Override
  public void fillRow(int rowId, CarbonColumnVector vector, int vectorRow) {
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

  @Override
  public void fillRow(int rowId, byte[] buffer, int offset) {
    throw new UnsupportedOperationException("Operation not supported");
  }

  @Override
  public int getInvertedIndex(int rowId) {
    return this.dimensionDataChunkStore.getInvertedIndex(rowId);
  }

  @Override
  public int getInvertedReverseIndex(int rowId) {
    return this.dimensionDataChunkStore.getInvertedReverseIndex(rowId);
  }

  @Override
  public int getSurrogate(int rowId) {
    throw new UnsupportedOperationException("Operation not supported");
  }

  @Override
  public int getColumnValueSize() {
    throw new UnsupportedOperationException("Operation not supported");
  }

  @Override
  public boolean isExplicitSorted() {
    return this.dimensionDataChunkStore.isExplicitSorted();
  }

  @Override
  public int compareTo(int rowId, byte[] compareValue) {
    return dimensionDataChunkStore.compareTo(rowId, compareValue);
  }

  /**
   * Below method will be used to free the memory occupied by the column chunk
   */
  @Override
  public void freeMemory() {
    if (null != dimensionDataChunkStore) {
      this.dimensionDataChunkStore.freeMemory();
      this.dictionary = null;
      this.dimensionDataChunkStore = null;
    }
  }
}
