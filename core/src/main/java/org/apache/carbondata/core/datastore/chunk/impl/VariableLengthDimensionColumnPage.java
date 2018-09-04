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
package org.apache.carbondata.core.datastore.chunk.impl;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.chunk.store.DimensionChunkStoreFactory;
import org.apache.carbondata.core.datastore.chunk.store.DimensionChunkStoreFactory.DimensionStoreType;
import org.apache.carbondata.core.scan.result.vector.CarbonColumnVector;
import org.apache.carbondata.core.scan.result.vector.CarbonDictionary;
import org.apache.carbondata.core.scan.result.vector.ColumnVectorInfo;

/**
 * This class is gives access to variable length dimension data chunk store
 */
public class VariableLengthDimensionColumnPage extends AbstractDimensionColumnPage {

  /**
   * Constructor for this class
   */
  public VariableLengthDimensionColumnPage(byte[] dataChunks, int[] invertedIndex,
      int[] invertedIndexReverse, int numberOfRows, DimensionStoreType dimStoreType,
      CarbonDictionary dictionary, ColumnVectorInfo vectorInfo) {
    boolean isExplicitSorted = isExplicitSorted(invertedIndex);
    long totalSize = 0;
    switch (dimStoreType) {
      case LOCAL_DICT:
        totalSize = null != invertedIndex ?
            (dataChunks.length + (2 * numberOfRows * CarbonCommonConstants.INT_SIZE_IN_BYTE)) :
            dataChunks.length;
        break;
      case VARIABLE_INT_LENGTH:
      case VARIABLE_SHORT_LENGTH:
        totalSize = null != invertedIndex ?
            (dataChunks.length + (2 * numberOfRows * CarbonCommonConstants.INT_SIZE_IN_BYTE) + (
                numberOfRows * CarbonCommonConstants.INT_SIZE_IN_BYTE)) :
            (dataChunks.length + (numberOfRows * CarbonCommonConstants.INT_SIZE_IN_BYTE));
        break;
      default:
        throw new UnsupportedOperationException("Invalidate dimension store type");
    }
    dataChunkStore = DimensionChunkStoreFactory.INSTANCE
        .getDimensionChunkStore(0, isExplicitSorted, numberOfRows, totalSize, dimStoreType,
            dictionary);
    if (vectorInfo != null) {
      dataChunkStore.putArray(invertedIndex, invertedIndexReverse, dataChunks, vectorInfo);
    } else {
      dataChunkStore.putArray(invertedIndex, invertedIndexReverse, dataChunks);
    }
  }


  /**
   * Below method will be used to fill the data based on offset and row id
   *
   * @param rowId             row id of the chunk
   * @param offset            offset from which data need to be filed
   * @param data              data to filed
   * @return how many bytes was copied
   */
  @Override public int fillRawData(int rowId, int offset, byte[] data) {
    // no required in this case because this column chunk is not the part if
    // mdkey
    return 0;
  }

  /**
   * Converts to column dictionary integer value
   *
   * @param rowId
   * @param chunkIndex
   * @param outputSurrogateKey
   * @return
   */
  @Override public int fillSurrogateKey(int rowId, int chunkIndex, int[] outputSurrogateKey) {
    return chunkIndex + 1;
  }

  /**
   * @return whether column is dictionary column or not
   */
  @Override public boolean isNoDicitionaryColumn() {
    return true;
  }

  /**
   * Fill the data to vector
   *
   * @param vectorInfo
   * @param chunkIndex
   * @return next column index
   */
  @Override public int fillVector(ColumnVectorInfo[] vectorInfo, int chunkIndex) {
    ColumnVectorInfo columnVectorInfo = vectorInfo[chunkIndex];
    CarbonColumnVector vector = columnVectorInfo.vector;
    int offset = columnVectorInfo.offset;
    int vectorOffset = columnVectorInfo.vectorOffset;
    int len = offset + columnVectorInfo.size;
    for (int i = offset; i < len; i++) {
      // Considering only String case now as we support only
      // string in no dictionary case at present.
      dataChunkStore.fillRow(i, vector, vectorOffset++);
    }
    return chunkIndex + 1;
  }

  /**
   * Fill the data to vector
   *
   * @param filteredRowId
   * @param vectorInfo
   * @param chunkIndex
   * @return next column index
   */
  @Override public int fillVector(int[] filteredRowId, ColumnVectorInfo[] vectorInfo,
      int chunkIndex) {
    ColumnVectorInfo columnVectorInfo = vectorInfo[chunkIndex];
    CarbonColumnVector vector = columnVectorInfo.vector;
    int offset = columnVectorInfo.offset;
    int vectorOffset = columnVectorInfo.vectorOffset;
    int len = offset + columnVectorInfo.size;
    for (int i = offset; i < len; i++) {
      // Considering only String case now as we support only
      // string in no dictionary case at present.
      dataChunkStore.fillRow(filteredRowId[i], vector, vectorOffset++);
    }
    return chunkIndex + 1;
  }
}
