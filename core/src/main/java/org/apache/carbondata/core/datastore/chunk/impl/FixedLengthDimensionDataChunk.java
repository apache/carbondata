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
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.scan.executor.infos.KeyStructureInfo;
import org.apache.carbondata.core.scan.result.vector.CarbonColumnVector;
import org.apache.carbondata.core.scan.result.vector.ColumnVectorInfo;

/**
 * This class is gives access to fixed length dimension data chunk store
 */
public class FixedLengthDimensionDataChunk extends AbstractDimensionDataChunk {

  /**
   * Constructor
   *
   * @param dataChunk            data chunk
   * @param invertedIndex        inverted index
   * @param invertedIndexReverse reverse inverted index
   * @param numberOfRows         number of rows
   * @param columnValueSize      size of each column value
   */
  public FixedLengthDimensionDataChunk(byte[] dataChunk, int[] invertedIndex,
      int[] invertedIndexReverse, int numberOfRows, int columnValueSize) {
    long totalSize = null != invertedIndex ?
        dataChunk.length + (2 * numberOfRows * CarbonCommonConstants.INT_SIZE_IN_BYTE) :
        dataChunk.length;
    dataChunkStore = DimensionChunkStoreFactory.INSTANCE
        .getDimensionChunkStore(columnValueSize, null != invertedIndex, numberOfRows, totalSize,
            DimensionStoreType.FIXEDLENGTH);
    dataChunkStore.putArray(invertedIndex, invertedIndexReverse, dataChunk);
  }

  /**
   * Below method will be used to fill the data based on offset and row id
   *
   * @param data             data to filed
   * @param offset           offset from which data need to be filed
   * @param index            row id of the chunk
   * @param keyStructureInfo define the structure of the key
   * @return how many bytes was copied
   */
  @Override public int fillChunkData(byte[] data, int offset, int index,
      KeyStructureInfo keyStructureInfo) {
    dataChunkStore.fillRow(index, data, offset);
    return dataChunkStore.getColumnValueSize();
  }

  /**
   * Converts to column dictionary integer value
   *
   * @param rowId
   * @param columnIndex
   * @param row
   * @param restructuringInfo
   * @return
   */
  @Override public int fillConvertedChunkData(int rowId, int columnIndex, int[] row,
      KeyStructureInfo restructuringInfo) {
    row[columnIndex] = dataChunkStore.getSurrogate(rowId);
    return columnIndex + 1;
  }

  /**
   * Fill the data to vector
   *
   * @param vectorInfo
   * @param column
   * @param restructuringInfo
   * @return next column index
   */
  @Override public int fillConvertedChunkData(ColumnVectorInfo[] vectorInfo, int column,
      KeyStructureInfo restructuringInfo) {
    ColumnVectorInfo columnVectorInfo = vectorInfo[column];
    int offset = columnVectorInfo.offset;
    int vectorOffset = columnVectorInfo.vectorOffset;
    int len = columnVectorInfo.size + offset;
    CarbonColumnVector vector = columnVectorInfo.vector;
    for (int j = offset; j < len; j++) {
      int dict = dataChunkStore.getSurrogate(j);
      if (columnVectorInfo.directDictionaryGenerator == null) {
        vector.putInt(vectorOffset++, dict);
      } else {
        Object valueFromSurrogate =
            columnVectorInfo.directDictionaryGenerator.getValueFromSurrogate(dict);
        if (valueFromSurrogate == null) {
          vector.putNull(vectorOffset++);
        } else {
          DataType dataType = columnVectorInfo.directDictionaryGenerator.getReturnType();
          if (dataType == DataTypes.INT) {
            vector.putInt(vectorOffset++, (int) valueFromSurrogate);
          } else if (dataType == DataTypes.LONG) {
            vector.putLong(vectorOffset++, (long) valueFromSurrogate);
          } else {
            throw new IllegalArgumentException("unsupported data type: " +
                columnVectorInfo.directDictionaryGenerator.getReturnType());
          }
        }
      }
    }
    return column + 1;
  }

  /**
   * Fill the data to vector
   *
   * @param rowMapping
   * @param vectorInfo
   * @param column
   * @param restructuringInfo
   * @return next column index
   */
  @Override public int fillConvertedChunkData(int[] rowMapping, ColumnVectorInfo[] vectorInfo,
      int column, KeyStructureInfo restructuringInfo) {
    ColumnVectorInfo columnVectorInfo = vectorInfo[column];
    int offset = columnVectorInfo.offset;
    int vectorOffset = columnVectorInfo.vectorOffset;
    int len = columnVectorInfo.size + offset;
    CarbonColumnVector vector = columnVectorInfo.vector;
    for (int j = offset; j < len; j++) {
      int dict = dataChunkStore.getSurrogate(rowMapping[j]);
      if (columnVectorInfo.directDictionaryGenerator == null) {
        vector.putInt(vectorOffset++, dict);
      } else {
        Object valueFromSurrogate =
            columnVectorInfo.directDictionaryGenerator.getValueFromSurrogate(dict);
        if (valueFromSurrogate == null) {
          vector.putNull(vectorOffset++);
        } else {
          DataType dataType = columnVectorInfo.directDictionaryGenerator.getReturnType();
          if (dataType == DataTypes.INT) {
            vector.putInt(vectorOffset++, (int) valueFromSurrogate);
          } else if (dataType == DataTypes.LONG) {
            vector.putLong(vectorOffset++, (long) valueFromSurrogate);
          } else {
            throw new IllegalArgumentException("unsupported data type: " +
                columnVectorInfo.directDictionaryGenerator.getReturnType());
          }
        }
      }
    }
    return column + 1;
  }
}
