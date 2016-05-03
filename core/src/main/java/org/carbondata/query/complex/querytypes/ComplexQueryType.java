package org.carbondata.query.complex.querytypes;

import org.carbondata.core.datastorage.store.columnar.ColumnarKeyStoreDataHolder;
import org.carbondata.query.carbon.processor.BlocksChunkHolder;

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

  public void fillRequiredBlockData(BlocksChunkHolder blockChunkHolder) {
    if (null == blockChunkHolder.getDimensionDataChunk()[blockIndex]) {
      blockChunkHolder.getDimensionDataChunk()[blockIndex] = blockChunkHolder.getDataBlock()
          .getDimensionChunk(blockChunkHolder.getFileReader(), blockIndex);
    }
    children.fillRequiredBlockData(blockChunkHolder);
  }

  /**
   * Method will copy the block chunk holder data to the passed
   * byte[], this method is also used by child
   *
   * @param columnarKeyStoreDataHolder
   * @param rowNumber
   * @param input
   */
  protected void copyBlockDataChunk(ColumnarKeyStoreDataHolder[] columnarKeyStoreDataHolder,
      int rowNumber, byte[] input) {
    if (!columnarKeyStoreDataHolder[blockIndex].getColumnarKeyStoreMetadata().isSorted()) {
      System.arraycopy(columnarKeyStoreDataHolder[blockIndex].getKeyBlockData(),
          columnarKeyStoreDataHolder[blockIndex].getColumnarKeyStoreMetadata()
              .getColumnReverseIndex()[rowNumber] * columnarKeyStoreDataHolder[blockIndex]
              .getColumnarKeyStoreMetadata().getEachRowSize(), input, 0,
          columnarKeyStoreDataHolder[blockIndex].getColumnarKeyStoreMetadata().getEachRowSize());
    } else {

      System.arraycopy(columnarKeyStoreDataHolder[blockIndex].getKeyBlockData(),
          rowNumber * columnarKeyStoreDataHolder[blockIndex].getColumnarKeyStoreMetadata()
              .getEachRowSize(), input, 0,
          columnarKeyStoreDataHolder[blockIndex].getColumnarKeyStoreMetadata().getEachRowSize());
    }
  }

  /*
   * This method will read the block data chunk from the respective block
   */
  protected void readBlockDataChunk(BlocksChunkHolder blockChunkHolder) {
    if (null == blockChunkHolder.getDimensionDataChunk()[blockIndex]) {
      blockChunkHolder.getDimensionDataChunk()[blockIndex] = blockChunkHolder.getDataBlock()
          .getDimensionChunk(blockChunkHolder.getFileReader(), blockIndex);
    }
  }
}
