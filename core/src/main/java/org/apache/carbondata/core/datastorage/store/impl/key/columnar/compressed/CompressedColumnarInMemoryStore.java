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

package org.apache.carbondata.core.datastorage.store.impl.key.columnar.compressed;

import java.util.List;

import org.apache.carbondata.core.datastorage.store.FileHolder;
import org.apache.carbondata.core.datastorage.store.columnar.ColumnarKeyStoreDataHolder;
import org.apache.carbondata.core.datastorage.store.columnar.ColumnarKeyStoreInfo;
import org.apache.carbondata.core.datastorage.store.columnar.ColumnarKeyStoreMetadata;
import org.apache.carbondata.core.datastorage.store.columnar.UnBlockIndexer;
import org.apache.carbondata.core.datastorage.store.impl.key.columnar.AbstractColumnarKeyStore;
import org.apache.carbondata.core.util.CarbonUtil;

public class CompressedColumnarInMemoryStore extends AbstractColumnarKeyStore {

  public CompressedColumnarInMemoryStore(ColumnarKeyStoreInfo columnarStoreInfo,
      FileHolder fileHolder) {
    super(columnarStoreInfo, true, fileHolder);
  }

  @Override public ColumnarKeyStoreDataHolder[] getUnCompressedKeyArray(FileHolder fileHolder,
      int[] blockIndex, boolean[] needCompressedData, int[] noDictionaryColIndexes) {
    ColumnarKeyStoreDataHolder[] columnarKeyStoreDataHolders =
        new ColumnarKeyStoreDataHolder[blockIndex.length];
    for (int i = 0; i < columnarKeyStoreDataHolders.length; i++) {
      byte[] columnarKeyBlockDataTemp = null;
      int[] columnKeyBlockIndex = null;
      int[] columnKeyBlockReverseIndexes = null;
      ColumnarKeyStoreMetadata columnarKeyStoreMetadata = null;
      int columnarKeyBlockIndex = 0;
      int[] dataIndex = null;
      boolean isUnCompressed = true;
      columnarKeyBlockDataTemp = COMPRESSOR.unCompress(columnarKeyBlockData[blockIndex[i]]);
      boolean isNoDictionaryBlock =
          CompressedColumnarKeyStoreUtil.isNoDictionaryBlock(noDictionaryColIndexes, blockIndex[i]);
      if (!isNoDictionaryBlock && this.columnarStoreInfo.getAggKeyBlock()[blockIndex[i]]) {
        dataIndex = columnarStoreInfo.getNumberCompressor()
            .unCompress(columnarUniqueblockKeyBlockIndex[mapOfAggDataIndex.get(blockIndex[i])]);
        if (!needCompressedData[i]) {
          columnarKeyBlockDataTemp = UnBlockIndexer
              .uncompressData(columnarKeyBlockDataTemp, dataIndex,
                  columnarStoreInfo.getSizeOfEachBlock()[blockIndex[i]]);
          dataIndex = null;
        } else {
          isUnCompressed = false;
        }
      }
      if (!columnarStoreInfo.getIsSorted()[blockIndex[i]]) {
        columnarKeyBlockIndex = mapOfColumnIndexAndColumnBlockIndex.get(blockIndex[i]);
        columnKeyBlockIndex = CarbonUtil.getUnCompressColumnIndex(
            columnarStoreInfo.getKeyBlockIndexLength()[columnarKeyBlockIndex],
            columnarKeyBlockDataIndex[columnarKeyBlockIndex],
            columnarStoreInfo.getNumberCompressor());
        columnKeyBlockReverseIndexes = getColumnIndexForNonFilter(columnKeyBlockIndex);
      }
      if (isNoDictionaryBlock) {
        columnarKeyStoreMetadata = new ColumnarKeyStoreMetadata(0);
        columnarKeyStoreMetadata.setColumnIndex(columnKeyBlockIndex);
        columnarKeyStoreMetadata.setColumnReverseIndex(columnKeyBlockReverseIndexes);
        columnarKeyStoreMetadata.setNoDictionaryValColumn(true);
        columnarKeyStoreMetadata.setUnCompressed(true);
        columnarKeyStoreMetadata.setSorted(columnarStoreInfo.getIsSorted()[blockIndex[i]]);
        //System is reading the direct surrogates data from byte array which contains both
        // length and the direct surrogates data
        List<byte[]> noDictionaryValBasedKeyBlockData = CompressedColumnarKeyStoreUtil
            .readColumnarKeyBlockDataForNoDictionaryCols(columnarKeyBlockDataTemp);
        columnarKeyStoreDataHolders[i] =
            new ColumnarKeyStoreDataHolder(noDictionaryValBasedKeyBlockData,
                columnarKeyStoreMetadata);
      }
      columnarKeyStoreMetadata =
          new ColumnarKeyStoreMetadata(columnarStoreInfo.getSizeOfEachBlock()[blockIndex[i]]);
      columnarKeyStoreMetadata.setColumnIndex(columnKeyBlockIndex);
      columnarKeyStoreMetadata.setSorted(columnarStoreInfo.getIsSorted()[blockIndex[i]]);
      columnarKeyStoreMetadata.setDataIndex(dataIndex);
      columnarKeyStoreMetadata.setColumnReverseIndex(columnKeyBlockReverseIndexes);
      columnarKeyStoreMetadata.setUnCompressed(isUnCompressed);
      columnarKeyStoreDataHolders[i] =
          new ColumnarKeyStoreDataHolder(columnarKeyBlockDataTemp, columnarKeyStoreMetadata);
    }
    return columnarKeyStoreDataHolders;
  }

  @Override
  public ColumnarKeyStoreDataHolder getUnCompressedKeyArray(FileHolder fileHolder, int blockIndex,
      boolean needCompressedData, int[] noDictionaryVals) {

    byte[] columnarKeyBlockDataTemp = null;
    int[] columnKeyBlockIndex = null;
    int[] columnKeyBlockReverseIndex = null;
    ColumnarKeyStoreMetadata columnarKeyStoreMetadata = null;
    int columnarKeyBlockIndex = 0;
    int[] dataIndex = null;
    boolean isUnCompressed = true;
    columnarKeyBlockDataTemp = COMPRESSOR.unCompress(columnarKeyBlockData[blockIndex]);
    boolean isNoDictionaryBlock =
        CompressedColumnarKeyStoreUtil.isNoDictionaryBlock(noDictionaryVals, blockIndex);
    if (!isNoDictionaryBlock && this.columnarStoreInfo.getAggKeyBlock()[blockIndex]) {
      dataIndex = columnarStoreInfo.getNumberCompressor()
          .unCompress(columnarUniqueblockKeyBlockIndex[mapOfAggDataIndex.get(blockIndex)]);
      if (!needCompressedData) {
        columnarKeyBlockDataTemp = UnBlockIndexer
            .uncompressData(columnarKeyBlockDataTemp, dataIndex,
                columnarStoreInfo.getSizeOfEachBlock()[blockIndex]);
        dataIndex = null;
      } else {
        isUnCompressed = false;
      }
    }
    if (!columnarStoreInfo.getIsSorted()[blockIndex]) {
      columnarKeyBlockIndex = mapOfColumnIndexAndColumnBlockIndex.get(blockIndex);
      columnKeyBlockIndex = CarbonUtil.getUnCompressColumnIndex(
          columnarStoreInfo.getKeyBlockIndexLength()[columnarKeyBlockIndex],
          columnarKeyBlockDataIndex[columnarKeyBlockIndex],
          columnarStoreInfo.getNumberCompressor());
      columnKeyBlockReverseIndex = getColumnIndexForNonFilter(columnKeyBlockIndex);
    }
    if (isNoDictionaryBlock) {
      ColumnarKeyStoreDataHolder colKeystoreDataHolders = CompressedColumnarKeyStoreUtil
          .createColumnarKeyStoreMetadataForHCDims(blockIndex, columnarKeyBlockDataTemp,
              columnKeyBlockIndex, columnKeyBlockReverseIndex, columnarStoreInfo);
      return colKeystoreDataHolders;
    }
    columnarKeyStoreMetadata =
        new ColumnarKeyStoreMetadata(columnarStoreInfo.getSizeOfEachBlock()[blockIndex]);
    columnarKeyStoreMetadata.setColumnIndex(columnKeyBlockIndex);
    columnarKeyStoreMetadata.setSorted(columnarStoreInfo.getIsSorted()[blockIndex]);
    columnarKeyStoreMetadata.setDataIndex(dataIndex);
    columnarKeyStoreMetadata.setColumnReverseIndex(columnKeyBlockReverseIndex);
    columnarKeyStoreMetadata.setUnCompressed(isUnCompressed);
    ColumnarKeyStoreDataHolder columnarKeyStoreDataHolders =
        new ColumnarKeyStoreDataHolder(columnarKeyBlockDataTemp, columnarKeyStoreMetadata);
    return columnarKeyStoreDataHolders;

  }

}
