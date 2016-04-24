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
package org.carbondata.core.carbon.datastore.chunk;

import org.carbondata.query.carbon.executor.infos.KeyStructureInfo;

/**
 * Interface for dimension column chunk.
 */
public interface DimensionColumnDataChunk<T> {

  /**
   * Below method will be used to fill the data based on offset and row id
   *
   * @param data   data to filed
   * @param offset offset from which data need to be filed
   * @param rowId  row id of the chunk
   * @return how many bytes was copied
   */
  int fillChunkData(byte[] data, int offset, int columnIndex, KeyStructureInfo restructuringInfo);

  /**
   * Below method to get  the data based in row id
   *
   * @param row id
   *            row id of the data
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
