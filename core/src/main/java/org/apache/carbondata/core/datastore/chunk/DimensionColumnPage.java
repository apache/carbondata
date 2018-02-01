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
public interface DimensionColumnPage {

  /**
   * Below method will be used to fill the data based on offset and row id
   *
   * @param offset offset from which data need to be filed
   * @param data   data to filed
   * @return how many bytes was copied
   */
  int fillRawData(int rowId, int offset, byte[] data, KeyStructureInfo restructuringInfo);

  /**
   * It uses to convert column data to dictionary integer value
   *
   * @param rowId
   * @param chunkIndex
   * @param outputSurrogateKey
   * @param restructuringInfo @return
   */
  int fillSurrogateKey(int rowId, int chunkIndex, int[] outputSurrogateKey,
      KeyStructureInfo restructuringInfo);

  /**
   * Fill the data to vector
   * @param vectorInfo
   * @param chunkIndex
   * @param restructuringInfo
   * @return next column index
   */
  int fillVector(ColumnVectorInfo[] vectorInfo, int chunkIndex,
      KeyStructureInfo restructuringInfo);

  /**
   * Fill the data to vector
   * @param filteredRowId
   * @param vectorInfo
   * @param chunkIndex
   * @param restructuringInfo
   * @return next column index
   */
  int fillVector(int[] filteredRowId, ColumnVectorInfo[] vectorInfo, int chunkIndex,
      KeyStructureInfo restructuringInfo);

  /**
   * Below method to get  the data based in row id
   *
   * @return chunk
   */
  byte[] getChunkData(int rowId);

  /**
   * @return inverted index
   */
  int getInvertedIndex(int rowId);

  /**
   *
   * @param rowId
   * @return index reverse index
   */
  int getInvertedReverseIndex(int rowId);

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
   * @param rowId        row index to be compared
   * @param compareValue value to compare
   * @return compare result
   */
  int compareTo(int rowId, byte[] compareValue);

  /**
   * below method will be used to free the allocated memory
   */
  void freeMemory();

}
