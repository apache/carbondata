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
package org.apache.carbondata.core.carbon.datastore.chunk.impl;

import org.apache.carbondata.core.carbon.datastore.chunk.DimensionChunkAttributes;
import org.apache.carbondata.core.carbon.datastore.chunk.DimensionColumnDataChunk;
import org.apache.carbondata.scan.executor.infos.KeyStructureInfo;
import org.apache.carbondata.scan.result.vector.CarbonColumnVector;
import org.apache.carbondata.scan.result.vector.ColumnVectorInfo;

/**
 * This class is holder of the dimension column chunk data of the fixed length
 * key size
 */
public class FixedLengthDimensionDataChunk implements DimensionColumnDataChunk<byte[]> {

  /**
   * dimension chunk attributes
   */
  private DimensionChunkAttributes chunkAttributes;

  /**
   * data chunks
   */
  private byte[] dataChunk;

  /**
   * Constructor for this class
   *
   * @param dataChunk       data chunk
   * @param chunkAttributes chunk attributes
   */
  public FixedLengthDimensionDataChunk(byte[] dataChunk, DimensionChunkAttributes chunkAttributes) {
    this.chunkAttributes = chunkAttributes;
    this.dataChunk = dataChunk;
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
    if (chunkAttributes.getInvertedIndexes() != null) {
      index = chunkAttributes.getInvertedIndexesReverse()[index];
    }
    System.arraycopy(dataChunk, index * chunkAttributes.getColumnValueSize(), data, offset,
        chunkAttributes.getColumnValueSize());
    return chunkAttributes.getColumnValueSize();
  }

  /**
   * Converts to column dictionary integer value
   */
  @Override public int fillConvertedChunkData(int rowId, int columnIndex, int[] row,
      KeyStructureInfo restructuringInfo) {
    if (chunkAttributes.getInvertedIndexes() != null) {
      rowId = chunkAttributes.getInvertedIndexesReverse()[rowId];
    }
    int start = rowId * chunkAttributes.getColumnValueSize();
    int dict = getInt(chunkAttributes.getColumnValueSize(), start);
    row[columnIndex] = dict;
    return columnIndex + 1;
  }

  @Override public int fillConvertedChunkData(ColumnVectorInfo[] vectorInfo, int column,
      KeyStructureInfo restructuringInfo) {
    ColumnVectorInfo columnVectorInfo = vectorInfo[column];
    int offset = columnVectorInfo.offset;
    int vectorOffset = columnVectorInfo.vectorOffset;
    int len = columnVectorInfo.size + offset;
    int[] indexesReverse = chunkAttributes.getInvertedIndexesReverse();
    int columnValueSize = chunkAttributes.getColumnValueSize();
    CarbonColumnVector vector = columnVectorInfo.vector;
    for (int j = offset; j < len; j++) {
      int start =
          indexesReverse == null ? j * columnValueSize : indexesReverse[j] * columnValueSize;
      int dict = getInt(columnValueSize, start);
      if (columnVectorInfo.directDictionaryGenerator == null) {
        vector.putInt(vectorOffset++, dict);
      } else {
        Object valueFromSurrogate =
            columnVectorInfo.directDictionaryGenerator.getValueFromSurrogate(dict);
        if (valueFromSurrogate == null) {
          vector.putNull(vectorOffset++);
        } else {
          switch (columnVectorInfo.directDictionaryGenerator.getReturnType()) {
            case INT:
              vector.putInt(vectorOffset++, (int) valueFromSurrogate);
              break;
            case LONG:
              vector.putLong(vectorOffset++, (long) valueFromSurrogate);
              break;
          }
        }
      }
    }
    return column + 1;
  }

  @Override
  public int fillConvertedChunkData(int[] rowMapping, ColumnVectorInfo[] vectorInfo, int column,
      KeyStructureInfo restructuringInfo) {
    ColumnVectorInfo columnVectorInfo = vectorInfo[column];
    int offset = columnVectorInfo.offset;
    int vectorOffset = columnVectorInfo.vectorOffset;
    int len = columnVectorInfo.size + offset;
    int[] indexesReverse = chunkAttributes.getInvertedIndexesReverse();
    int columnValueSize = chunkAttributes.getColumnValueSize();
    CarbonColumnVector vector = columnVectorInfo.vector;
    for (int j = offset; j < len; j++) {
      // calculate the start index to take the dictionary data. Here dictionary data size is fixed
      // so calculate using fixed size of it.
      int start = indexesReverse == null ?
          rowMapping[j] * columnValueSize :
          indexesReverse[rowMapping[j]] * columnValueSize;
      int dict = getInt(columnValueSize, start);
      if (columnVectorInfo.directDictionaryGenerator == null) {
        vector.putInt(vectorOffset++, dict);
      } else {
        Object valueFromSurrogate =
            columnVectorInfo.directDictionaryGenerator.getValueFromSurrogate(dict);
        if (valueFromSurrogate == null) {
          vector.putNull(vectorOffset++);
        } else {
          switch (columnVectorInfo.directDictionaryGenerator.getReturnType()) {
            case INT:
              vector.putInt(vectorOffset++, (int) valueFromSurrogate);
              break;
            case LONG:
              vector.putLong(vectorOffset++, (long) valueFromSurrogate);
              break;
          }
        }
      }
    }
    return column + 1;
  }

  /**
   * Create the integer from fixed column size and start index
   * @param columnValueSize
   * @param start
   * @return
   */
  private int getInt(int columnValueSize, int start) {
    int dict = 0;
    for (int i = start; i < start + columnValueSize; i++) {
      dict <<= 8;
      dict ^= dataChunk[i] & 0xFF;
    }
    return dict;
  }

  /**
   * Below method to get the data based in row id
   *
   * @param index row id of the data
   * @return chunk
   */
  @Override public byte[] getChunkData(int index) {
    byte[] data = new byte[chunkAttributes.getColumnValueSize()];
    if (chunkAttributes.getInvertedIndexes() != null) {
      index = chunkAttributes.getInvertedIndexesReverse()[index];
    }
    System.arraycopy(dataChunk, index * chunkAttributes.getColumnValueSize(), data, 0,
        chunkAttributes.getColumnValueSize());
    return data;
  }

  /**
   * Below method will be used get the chunk attributes
   *
   * @return chunk attributes
   */
  @Override public DimensionChunkAttributes getAttributes() {
    return chunkAttributes;
  }

  /**
   * Below method will be used to return the complete data chunk
   * This will be required during filter query
   *
   * @return complete chunk
   */
  @Override public byte[] getCompleteDataChunk() {
    return dataChunk;
  }
}
