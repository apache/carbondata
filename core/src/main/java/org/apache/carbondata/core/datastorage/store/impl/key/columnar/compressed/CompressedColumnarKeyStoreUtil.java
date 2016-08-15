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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.core.datastorage.store.columnar.ColumnarKeyStoreDataHolder;
import org.apache.carbondata.core.datastorage.store.columnar.ColumnarKeyStoreInfo;
import org.apache.carbondata.core.datastorage.store.columnar.ColumnarKeyStoreMetadata;

/**
 * Utility helper class for managing the processing of columnar key store block.
 */
public final class CompressedColumnarKeyStoreUtil {

  private CompressedColumnarKeyStoreUtil() {

  }

  /**
   * @param columnarKeyBlockData
   * @param columnarKeyStoreMetadata
   * @return
   * @author s71955 The high cardinality dimensions rows will be send in byte
   * array with its data length appended in the
   * ColumnarKeyStoreDataHolder byte array since high cardinality dim
   * data will not be part of MDKey/Surrogate keys. In this method the
   * byte array will be scanned and the length which is stored in
   * short will be removed.
   */
  public static List<byte[]> readColumnarKeyBlockDataForNoDictionaryCols(
      byte[] columnarKeyBlockData) {
    List<byte[]> columnarKeyBlockDataList = new ArrayList<byte[]>(50);
    ByteBuffer noDictionaryValKeyStoreDataHolder = ByteBuffer.allocate(columnarKeyBlockData.length);
    noDictionaryValKeyStoreDataHolder.put(columnarKeyBlockData);
    noDictionaryValKeyStoreDataHolder.flip();
    while (noDictionaryValKeyStoreDataHolder.hasRemaining()) {
      short dataLength = noDictionaryValKeyStoreDataHolder.getShort();
      byte[] noDictionaryValKeyData = new byte[dataLength];
      noDictionaryValKeyStoreDataHolder.get(noDictionaryValKeyData);
      columnarKeyBlockDataList.add(noDictionaryValKeyData);
    }
    return columnarKeyBlockDataList;

  }

  /**
   * @param blockIndex
   * @param columnarKeyBlockData
   * @param columnKeyBlockIndex
   * @param columnKeyBlockReverseIndex
   * @param columnarStoreInfo
   * @return
   */
  public static ColumnarKeyStoreDataHolder createColumnarKeyStoreMetadataForHCDims(int blockIndex,
      byte[] columnarKeyBlockData, int[] columnKeyBlockIndex, int[] columnKeyBlockReverseIndex,
      ColumnarKeyStoreInfo columnarStoreInfo) {
    ColumnarKeyStoreMetadata columnarKeyStoreMetadata;
    columnarKeyStoreMetadata = new ColumnarKeyStoreMetadata(0);
    columnarKeyStoreMetadata.setNoDictionaryValColumn(true);
    columnarKeyStoreMetadata.setColumnIndex(columnKeyBlockIndex);
    columnarKeyStoreMetadata.setColumnReverseIndex(columnKeyBlockReverseIndex);
    columnarKeyStoreMetadata.setSorted(columnarStoreInfo.getIsSorted()[blockIndex]);
    columnarKeyStoreMetadata.setUnCompressed(true);
    List<byte[]> noDictionaryValBasedKeyBlockData = CompressedColumnarKeyStoreUtil
        .readColumnarKeyBlockDataForNoDictionaryCols(columnarKeyBlockData);
    ColumnarKeyStoreDataHolder columnarKeyStoreDataHolders =
        new ColumnarKeyStoreDataHolder(noDictionaryValBasedKeyBlockData, columnarKeyStoreMetadata);
    return columnarKeyStoreDataHolders;
  }

  /**
   * This API will determine whether the requested block index is a  No dictionary
   * column index.
   *
   * @param noDictionaryColIndexes
   * @param blockIndex
   * @return
   */
  public static boolean isNoDictionaryBlock(int[] noDictionaryColIndexes, int blockIndex) {
    if (null != noDictionaryColIndexes) {
      for (int noDictionaryValIndex : noDictionaryColIndexes) {
        if (noDictionaryValIndex == blockIndex) {
          return true;
        }
      }
    }
    return false;
  }
}
