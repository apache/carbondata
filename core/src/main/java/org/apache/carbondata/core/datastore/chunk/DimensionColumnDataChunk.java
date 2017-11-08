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
package org.apache.carbondata.core.datastore.chunk;

import org.apache.carbondata.core.scan.executor.infos.KeyStructureInfo;
import org.apache.carbondata.core.scan.result.vector.ColumnVectorInfo;

/**
 * Interface for dimension column chunk.
 */
public interface DimensionColumnDataChunk {

  /**
   * Below method will be used to fill the data based on offset and row id
   *
   * @param data   data to filed
   * @param offset offset from which data need to be filed
   * @return how many bytes was copied
   */
  int fillChunkData(byte[] data, int offset, int columnIndex, KeyStructureInfo restructuringInfo);

  /**
   * It uses to convert column data to dictionary integer value
   *
   * @param rowId
   * @param columnIndex
   * @param row
   * @param restructuringInfo @return
   */
  int fillConvertedChunkData(int rowId, int columnIndex, int[] row,
      KeyStructureInfo restructuringInfo);

  /**
   * Fill the data to vector
   * @param vectorInfo
   * @param column
   * @param restructuringInfo
   * @return next column index
   */
  int fillConvertedChunkData(ColumnVectorInfo[] vectorInfo, int column,
      KeyStructureInfo restructuringInfo);

  /**
   * Fill the data to vector
   * @param rowMapping
   * @param vectorInfo
   * @param column
   * @param restructuringInfo
   * @return next column index
   */
  int fillConvertedChunkData(int[] rowMapping, ColumnVectorInfo[] vectorInfo, int column,
      KeyStructureInfo restructuringInfo);

  /**
   * Below method to get  the data based in row id
   *
   * @return chunk
   */
  byte[] getChunkData(int columnIndex);

  /**
   * @return inverted index
   */
  int getInvertedIndex(int index);

  /**
   *
   * @param invertedIndex
   * @return index reverse index
   */
  int getInvertedReverseIndex(int invertedIndex);

  /**
   * @return whether column is dictionary column or not
   */
  boolean isNoDicitionaryColumn();

  /**
   * @return whether columns where explictly sorted or not
   */
  boolean isExplicitSorted();

  /**
   * to compare the data
   *
   * @param index        row index to be compared
   * @param compareValue value to compare
   * @return compare result
   */
  int compareTo(int index, byte[] compareValue);

  /**
   * below method will be used to free the allocated memory
   */
  void freeMemory();

}
