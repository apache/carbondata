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
package org.apache.carbondata.core.carbon.datastore.chunk;

import org.apache.carbondata.scan.executor.infos.KeyStructureInfo;
import org.apache.carbondata.scan.result.vector.ColumnVectorInfo;

/**
 * Interface for dimension column chunk.
 */
public interface DimensionColumnDataChunk<T> {

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
   * @param rowId
   * @param columnIndex
   * @param row
   * @param restructuringInfo  @return
   */
  int fillConvertedChunkData(int rowId, int columnIndex, int[] row,
      KeyStructureInfo restructuringInfo);

  /**
   * Fill the data to vector
   */
  int fillConvertedChunkData(ColumnVectorInfo[] vectorInfo, int column,
      KeyStructureInfo restructuringInfo);

  /**
   * Fill the data to vector
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
   * Below method will be used get the chunk attributes
   *
   * @return chunk attributes
   */
  DimensionChunkAttributes getAttributes();

  /**
   * Below method will be used to return the complete data chunk
   * This will be required during filter query
   *
   * @return complete chunk
   */
  T getCompleteDataChunk();
}
