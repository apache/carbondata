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
package org.carbondata.core.carbon.datastore.chunk.impl;

import org.carbondata.core.carbon.datastore.chunk.DimensionChunkAttributes;
import org.carbondata.core.carbon.datastore.chunk.DimensionColumnDataChunk;
import org.carbondata.query.carbon.executor.infos.KeyStructureInfo;

/**
 * This class is holder of the dimension column chunk data of the fixed length
 * key size
 */
public class ColumnGroupDimensionDataChunk implements DimensionColumnDataChunk<byte[]> {

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
  public ColumnGroupDimensionDataChunk(byte[] dataChunk, DimensionChunkAttributes chunkAttributes) {
    this.chunkAttributes = chunkAttributes;
    this.dataChunk = dataChunk;
  }

  /**
   * Below method will be used to fill the data based on offset and row id
   *
   * @param data             data to filed
   * @param offset           offset from which data need to be filed
   * @param rowId            row id of the chunk
   * @param keyStructureInfo define the structure of the key
   * @return how many bytes was copied
   */
  @Override public int fillChunkData(byte[] data, int offset, int rowId,
      KeyStructureInfo restructuringInfo) {
    byte[] maskedKey =
        getMaskedKey(dataChunk, rowId * chunkAttributes.getColumnValueSize(), restructuringInfo);
    System.arraycopy(maskedKey, 0, data, offset, maskedKey.length);
    return maskedKey.length;
  }

  /**
   * Below method to get the data based in row id
   *
   * @param row id row id of the data
   * @return chunk
   */
  public byte[] getMaskedKey(byte[] data, int offset, KeyStructureInfo info) {
    byte[] maskedKey = new byte[info.getMaskByteRanges().length];
    int counter = 0;
    int byteRange = 0;
    for (int i = 0; i < info.getMaskByteRanges().length; i++) {
      byteRange = info.getMaskByteRanges()[i];
      maskedKey[counter++] = (byte) (data[byteRange + offset] & info.getMaxKey()[byteRange]);
    }
    return maskedKey;
  }

  /**
   * Below method to get the data based in row id
   *
   * @param row id row id of the data
   * @return chunk
   */
  @Override public byte[] getChunkData(int rowId) {
    byte[] data = new byte[chunkAttributes.getColumnValueSize()];
    System.arraycopy(dataChunk, rowId * data.length, data, 0, data.length);
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
