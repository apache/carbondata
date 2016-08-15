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

import java.util.List;

import org.apache.carbondata.core.carbon.datastore.chunk.DimensionChunkAttributes;
import org.apache.carbondata.core.carbon.datastore.chunk.DimensionColumnDataChunk;
import org.apache.carbondata.scan.executor.infos.KeyStructureInfo;

/**
 * This class is holder of the dimension column chunk data of the variable
 * length key size
 */
public class VariableLengthDimensionDataChunk implements DimensionColumnDataChunk<List<byte[]>> {

  /**
   * dimension chunk attributes
   */
  private DimensionChunkAttributes chunkAttributes;

  /**
   * data chunk
   */
  private List<byte[]> dataChunk;

  /**
   * Constructor for this class
   *
   * @param dataChunk       data chunk
   * @param chunkAttributes chunk attributes
   */
  public VariableLengthDimensionDataChunk(List<byte[]> dataChunk,
      DimensionChunkAttributes chunkAttributes) {
    this.chunkAttributes = chunkAttributes;
    this.dataChunk = dataChunk;
  }

  /**
   * Below method will be used to fill the data based on offset and row id
   *
   * @param data             data to filed
   * @param offset           offset from which data need to be filed
   * @param index            row id of the chunk
   * @param restructuringInfo define the structure of the key
   * @return how many bytes was copied
   */
  @Override public int fillChunkData(byte[] data, int offset, int index,
      KeyStructureInfo restructuringInfo) {
    // no required in this case because this column chunk is not the part if
    // mdkey
    return 0;
  }

  /**
   * Converts to column dictionary integer value
   * @param rowId
   * @param columnIndex
   * @param row
   * @param restructuringInfo  @return
   */
  @Override public int fillConvertedChunkData(int rowId, int columnIndex, int[] row,
      KeyStructureInfo restructuringInfo) {
    return columnIndex + 1;
  }

  /**
   * Below method to get the data based in row id
   *
   * @param index row id of the data
   * @return chunk
   */
  @Override public byte[] getChunkData(int index) {
    if (null != chunkAttributes.getInvertedIndexes()) {
      index = chunkAttributes.getInvertedIndexesReverse()[index];
    }
    return dataChunk.get(index);
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
  @Override public List<byte[]> getCompleteDataChunk() {
    return dataChunk;
  }
}
