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

package org.apache.carbondata.core.scan.complextypes;

import java.io.IOException;

import org.apache.carbondata.core.datastore.chunk.DimensionColumnDataChunk;
import org.apache.carbondata.core.scan.filter.GenericQueryType;
import org.apache.carbondata.core.scan.processor.BlocksChunkHolder;

public class ComplexQueryType {
  protected GenericQueryType children;

  protected String name;

  protected String parentname;

  protected int blockIndex;

  public ComplexQueryType(String name, String parentname, int blockIndex) {
    this.name = name;
    this.parentname = parentname;
    this.blockIndex = blockIndex;
  }

  /**
   * Method will copy the block chunk holder data to the passed
   * byte[], this method is also used by child
   *
   * @param rowNumber
   * @param input
   */
  protected void copyBlockDataChunk(DimensionColumnDataChunk[] dimensionColumnDataChunks,
      int rowNumber, byte[] input) {
    byte[] data = dimensionColumnDataChunks[blockIndex].getChunkData(rowNumber);
    System.arraycopy(data, 0, input, 0, data.length);
  }

  /*
   * This method will read the block data chunk from the respective block
   */
  protected void readBlockDataChunk(BlocksChunkHolder blockChunkHolder) throws IOException {
    if (null == blockChunkHolder.getDimensionDataChunk()[blockIndex]) {
      blockChunkHolder.getDimensionDataChunk()[blockIndex] = blockChunkHolder.getDataBlock()
          .getDimensionChunk(blockChunkHolder.getFileReader(), blockIndex);
    }
  }
}
