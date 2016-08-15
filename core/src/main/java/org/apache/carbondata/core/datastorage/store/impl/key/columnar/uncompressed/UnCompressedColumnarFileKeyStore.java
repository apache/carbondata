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

package org.apache.carbondata.core.datastorage.store.impl.key.columnar.uncompressed;

import org.apache.carbondata.core.datastorage.store.FileHolder;
import org.apache.carbondata.core.datastorage.store.columnar.ColumnarKeyStoreDataHolder;
import org.apache.carbondata.core.datastorage.store.columnar.ColumnarKeyStoreInfo;
import org.apache.carbondata.core.datastorage.store.columnar.ColumnarKeyStoreMetadata;
import org.apache.carbondata.core.datastorage.store.columnar.UnBlockIndexer;
import org.apache.carbondata.core.datastorage.store.impl.key.columnar.AbstractColumnarKeyStore;
import org.apache.carbondata.core.util.CarbonUtil;

public class UnCompressedColumnarFileKeyStore extends AbstractColumnarKeyStore {

  public UnCompressedColumnarFileKeyStore(ColumnarKeyStoreInfo columnarStoreInfo) {
    super(columnarStoreInfo, false, null);
  }

  @Override public ColumnarKeyStoreDataHolder[] getUnCompressedKeyArray(FileHolder fileHolder,
      int[] blockIndex, boolean[] needCompressedData, int[] noDictionaryColIndexes) {
    ColumnarKeyStoreDataHolder[] columnarKeyStoreDataHolders =
        new ColumnarKeyStoreDataHolder[blockIndex.length];
    byte[] columnarKeyBlockData = null;
    int[] columnKeyBlockIndex = null;
    ColumnarKeyStoreMetadata columnarKeyStoreMetadata = null;
    int columnarKeyBlockIndex = 0;
    int[] dataIndex = null;
    int[] columnKeyBlockReverseIndex = null;
    for (int j = 0; j < columnarKeyStoreDataHolders.length; j++) {
      columnarKeyBlockData = fileHolder.readByteArray(columnarStoreInfo.getFilePath(),
          columnarStoreInfo.getKeyBlockOffsets()[blockIndex[j]],
          columnarStoreInfo.getKeyBlockLengths()[blockIndex[j]]);
      if (this.columnarStoreInfo.getAggKeyBlock()[blockIndex[j]]) {
        dataIndex = columnarStoreInfo.getNumberCompressor().unCompress(fileHolder
            .readByteArray(columnarStoreInfo.getFilePath(),
                columnarStoreInfo.getDataIndexMapOffsets()[mapOfAggDataIndex.get(blockIndex[j])],
                columnarStoreInfo.getDataIndexMapLength()[mapOfAggDataIndex.get(blockIndex[j])]));
        if (!needCompressedData[j]) {
          columnarKeyBlockData = UnBlockIndexer.uncompressData(columnarKeyBlockData, dataIndex,
              columnarStoreInfo.getSizeOfEachBlock()[blockIndex[j]]);
          dataIndex = null;
        }
      }
      if (!columnarStoreInfo.getIsSorted()[blockIndex[j]]) {
        columnarKeyBlockIndex = mapOfColumnIndexAndColumnBlockIndex.get(blockIndex[j]);
        columnKeyBlockIndex = CarbonUtil.getUnCompressColumnIndex(
            columnarStoreInfo.getKeyBlockIndexLength()[columnarKeyBlockIndex], fileHolder
                .readByteArray(columnarStoreInfo.getFilePath(),
                    columnarStoreInfo.getKeyBlockIndexOffsets()[columnarKeyBlockIndex],
                    columnarStoreInfo.getKeyBlockIndexLength()[columnarKeyBlockIndex]),
            columnarStoreInfo.getNumberCompressor());
        columnKeyBlockReverseIndex = getColumnIndexForNonFilter(columnKeyBlockIndex);
      }
      columnarKeyStoreMetadata =
          new ColumnarKeyStoreMetadata(columnarStoreInfo.getSizeOfEachBlock()[blockIndex[j]]);
      columnarKeyStoreMetadata.setSorted(columnarStoreInfo.getIsSorted()[blockIndex[j]]);
      columnarKeyStoreMetadata.setColumnIndex(columnKeyBlockIndex);
      columnarKeyStoreMetadata.setDataIndex(dataIndex);
      columnarKeyStoreMetadata.setColumnReverseIndex(columnKeyBlockReverseIndex);
      columnarKeyStoreDataHolders[j] =
          new ColumnarKeyStoreDataHolder(columnarKeyBlockData, columnarKeyStoreMetadata);
    }
    return columnarKeyStoreDataHolders;
  }

  @Override
  public ColumnarKeyStoreDataHolder getUnCompressedKeyArray(FileHolder fileHolder, int blockIndex,
      boolean needCompressedData, int[] noDictionaryColIndexes) {
    return null;
  }
}
