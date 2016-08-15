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
import org.apache.carbondata.core.datastorage.store.impl.key.columnar.AbstractColumnarKeyStore;
import org.apache.carbondata.core.util.CarbonUtil;

public class UnCompressedColumnarInMemoryStore extends AbstractColumnarKeyStore {

  public UnCompressedColumnarInMemoryStore(ColumnarKeyStoreInfo columnarStoreInfo,
      FileHolder fileHolder) {
    super(columnarStoreInfo, true, fileHolder);
  }

  @Override public ColumnarKeyStoreDataHolder[] getUnCompressedKeyArray(FileHolder fileHolder,
      int[] blockIndex, boolean[] needCompressedData, int[] noDictionaryColIndexes) {
    int columnarKeyBlockIndex = 0;
    int[] columnIndex = null;
    ColumnarKeyStoreDataHolder[] columnarKeyStoreDataHolders =
        new ColumnarKeyStoreDataHolder[blockIndex.length];
    ColumnarKeyStoreMetadata columnarKeyStoreMetadataTemp = null;
    for (int i = 0; i < columnarKeyStoreDataHolders.length; i++) {
      columnarKeyStoreMetadataTemp = new ColumnarKeyStoreMetadata(0);
      if (!columnarStoreInfo.getIsSorted()[blockIndex[i]]) {
        columnarKeyBlockIndex = mapOfColumnIndexAndColumnBlockIndex.get(blockIndex[i]);
        columnIndex = CarbonUtil.getUnCompressColumnIndex(
            columnarStoreInfo.getKeyBlockIndexLength()[columnarKeyBlockIndex], fileHolder
                .readByteArray(columnarStoreInfo.getFilePath(),
                    columnarStoreInfo.getKeyBlockIndexOffsets()[columnarKeyBlockIndex],
                    columnarStoreInfo.getKeyBlockIndexLength()[columnarKeyBlockIndex]),
            columnarStoreInfo.getNumberCompressor());
        columnIndex = getColumnIndexForNonFilter(columnIndex);
        columnarKeyStoreMetadataTemp.setColumnIndex(columnIndex);
      }
      columnarKeyStoreMetadataTemp.setSorted(columnarStoreInfo.getIsSorted()[blockIndex[i]]);
      columnarKeyStoreDataHolders[i] =
          new ColumnarKeyStoreDataHolder(columnarKeyBlockData[blockIndex[i]],
              columnarKeyStoreMetadataTemp);
    }
    return columnarKeyStoreDataHolders;
  }

  @Override
  public ColumnarKeyStoreDataHolder getUnCompressedKeyArray(FileHolder fileHolder, int blockIndex,
      boolean needCompressedData, int[] noDictionaryVals) {
    return null;
  }

}
