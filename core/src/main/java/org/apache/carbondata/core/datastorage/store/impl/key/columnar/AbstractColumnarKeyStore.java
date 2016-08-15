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

package org.apache.carbondata.core.datastorage.store.impl.key.columnar;

import java.util.HashMap;
import java.util.Map;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastorage.store.FileHolder;
import org.apache.carbondata.core.datastorage.store.columnar.ColumnarKeyStore;
import org.apache.carbondata.core.datastorage.store.columnar.ColumnarKeyStoreInfo;
import org.apache.carbondata.core.datastorage.store.compression.Compressor;
import org.apache.carbondata.core.datastorage.store.compression.SnappyCompression;

public abstract class AbstractColumnarKeyStore implements ColumnarKeyStore {

  /**
   * compressor will be used to compress the data
   */
  protected static final Compressor<byte[]> COMPRESSOR =
      SnappyCompression.SnappyByteCompression.INSTANCE;

  protected ColumnarKeyStoreInfo columnarStoreInfo;

  protected byte[][] columnarKeyBlockDataIndex;

  protected byte[][] columnarKeyBlockData;

  protected Map<Integer, Integer> mapOfColumnIndexAndColumnBlockIndex;

  protected Map<Integer, Integer> mapOfAggDataIndex;

  protected byte[][] columnarUniqueblockKeyBlockIndex;

  public AbstractColumnarKeyStore(ColumnarKeyStoreInfo columnarStoreInfo, boolean isInMemory,
      FileHolder fileHolder) {
    this.columnarStoreInfo = columnarStoreInfo;
    this.mapOfColumnIndexAndColumnBlockIndex =
        new HashMap<Integer, Integer>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    this.mapOfAggDataIndex =
        new HashMap<Integer, Integer>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    int index = 0;
    for (int i = 0; i < this.columnarStoreInfo.getIsSorted().length; i++) {
      if (!this.columnarStoreInfo.getIsSorted()[i]) {
        this.mapOfColumnIndexAndColumnBlockIndex.put(i, index++);
      }
    }
    index = 0;
    for (int i = 0; i < this.columnarStoreInfo.getAggKeyBlock().length; i++) {
      if (this.columnarStoreInfo.getAggKeyBlock()[i]) {
        mapOfAggDataIndex.put(i, index++);
      }
    }
    if (isInMemory) {
      this.columnarKeyBlockData = new byte[this.columnarStoreInfo.getIsSorted().length][];
      this.columnarKeyBlockDataIndex = new byte[this.mapOfColumnIndexAndColumnBlockIndex.size()][];
      this.columnarUniqueblockKeyBlockIndex = new byte[this.mapOfAggDataIndex.size()][];
      for (int i = 0; i < columnarStoreInfo.getSizeOfEachBlock().length; i++) {
        columnarKeyBlockData[i] = fileHolder.readByteArray(columnarStoreInfo.getFilePath(),
            columnarStoreInfo.getKeyBlockOffsets()[i], columnarStoreInfo.getKeyBlockLengths()[i]);

        if (!this.columnarStoreInfo.getIsSorted()[i]) {
          this.columnarKeyBlockDataIndex[mapOfColumnIndexAndColumnBlockIndex.get(i)] = fileHolder
              .readByteArray(columnarStoreInfo.getFilePath(),
                  columnarStoreInfo.getKeyBlockIndexOffsets()[mapOfColumnIndexAndColumnBlockIndex
                      .get(i)],
                  columnarStoreInfo.getKeyBlockIndexLength()[mapOfColumnIndexAndColumnBlockIndex
                      .get(i)]);
        }

        if (this.columnarStoreInfo.getAggKeyBlock()[i]) {
          this.columnarUniqueblockKeyBlockIndex[mapOfAggDataIndex.get(i)] = fileHolder
              .readByteArray(columnarStoreInfo.getFilePath(),
                  columnarStoreInfo.getDataIndexMapOffsets()[mapOfAggDataIndex.get(i)],
                  columnarStoreInfo.getDataIndexMapLength()[mapOfAggDataIndex.get(i)]);
        }
      }
    }
  }

  protected int[] getColumnIndexForNonFilter(int[] columnIndex) {
    int[] columnIndexTemp = new int[columnIndex.length];

    for (int i = 0; i < columnIndex.length; i++) {
      columnIndexTemp[columnIndex[i]] = i;
    }
    return columnIndexTemp;
  }
}
