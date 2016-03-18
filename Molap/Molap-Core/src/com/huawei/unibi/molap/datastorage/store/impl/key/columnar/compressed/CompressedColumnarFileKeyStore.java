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

package com.huawei.unibi.molap.datastorage.store.impl.key.columnar.compressed;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import com.huawei.unibi.molap.datastorage.store.FileHolder;
import com.huawei.unibi.molap.datastorage.store.columnar.ColumnarKeyStoreDataHolder;
import com.huawei.unibi.molap.datastorage.store.columnar.ColumnarKeyStoreInfo;
import com.huawei.unibi.molap.datastorage.store.columnar.ColumnarKeyStoreMetadata;
import com.huawei.unibi.molap.datastorage.store.columnar.UnBlockIndexer;
import com.huawei.unibi.molap.datastorage.store.impl.key.columnar.AbstractColumnarKeyStore;
import com.huawei.unibi.molap.util.MolapUtil;

public class CompressedColumnarFileKeyStore extends AbstractColumnarKeyStore {

    public CompressedColumnarFileKeyStore(ColumnarKeyStoreInfo columnarStoreInfo) {
        super(columnarStoreInfo, false, null);
    }

    @Override public ColumnarKeyStoreDataHolder[] getUnCompressedKeyArray(FileHolder fileHolder,
            int[] blockIndex, boolean[] needCompressedData) {
        ColumnarKeyStoreDataHolder[] columnarKeyStoreDataHolders =
                new ColumnarKeyStoreDataHolder[blockIndex.length];

        for (int i = 0; i < columnarKeyStoreDataHolders.length; i++) {
            byte[] columnarKeyBlockData = null;
            int[] columnKeyBlockIndex = null;
            int[] columnKeyBlockReverseIndexes = null;
            ColumnarKeyStoreMetadata columnarKeyStoreMetadata = null;
            int columnarKeyBlockIndex = 0;
            int[] dataIndex = null;
            boolean isUnCompressed = true;
            columnarKeyBlockData = COMPRESSOR.unCompress(fileHolder
                    .readByteArray(columnarStoreInfo.getFilePath(),
                            columnarStoreInfo.getKeyBlockOffsets()[blockIndex[i]],
                            columnarStoreInfo.getKeyBlockLengths()[blockIndex[i]]));
            if (blockIndex[i] <= this.columnarStoreInfo.getAggKeyBlock().length - 1
                    && this.columnarStoreInfo.getAggKeyBlock()[blockIndex[i]]) {
                dataIndex = columnarStoreInfo.getNumberCompressor().unCompress(fileHolder
                        .readByteArray(columnarStoreInfo.getFilePath(),
                                columnarStoreInfo.getDataIndexMapOffsets()[mapOfAggDataIndex
                                        .get(blockIndex[i])],
                                columnarStoreInfo.getDataIndexMapLength()[mapOfAggDataIndex
                                        .get(blockIndex[i])]));
                if (!needCompressedData[i]) {
                    columnarKeyBlockData = UnBlockIndexer
                            .uncompressData(columnarKeyBlockData, dataIndex,
                                    columnarStoreInfo.getSizeOfEachBlock()[blockIndex[i]]);
                    dataIndex = null;
                } else {
                    isUnCompressed = false;
                }
            }
            if (!columnarStoreInfo.getIsSorted()[blockIndex[i]]) {
                columnarKeyBlockIndex = mapOfColumnIndexAndColumnBlockIndex.get(blockIndex[i]);
                columnKeyBlockIndex = MolapUtil.getUnCompressColumnIndex(
                        columnarStoreInfo.getKeyBlockIndexLength()[columnarKeyBlockIndex],
                        fileHolder.readByteArray(columnarStoreInfo.getFilePath(),
                                columnarStoreInfo.getKeyBlockIndexOffsets()[columnarKeyBlockIndex],
                                columnarStoreInfo.getKeyBlockIndexLength()[columnarKeyBlockIndex]),
                        columnarStoreInfo.getNumberCompressor());
                columnKeyBlockReverseIndexes = getColumnIndexForNonFilter(columnKeyBlockIndex);
            }
            //Since its anhigh cardinality dimension
            if (blockIndex[i] > this.columnarStoreInfo.getSizeOfEachBlock().length - 1) {
                columnarKeyStoreMetadata = new ColumnarKeyStoreMetadata(0);
                columnarKeyStoreMetadata.setColumnIndex(columnKeyBlockIndex);
                columnarKeyStoreMetadata.setColumnReverseIndex(columnKeyBlockReverseIndexes);
                columnarKeyStoreMetadata.setDirectSurrogateColumn(true);
                columnarKeyStoreMetadata.setSorted(columnarStoreInfo.getIsSorted()[blockIndex[i]]);
                //System is reading the direct surrogates data from byte array which contains both length and the
                //direct surrogates data
                mapColumnIndexWithKeyColumnarKeyBlockData(columnarKeyBlockData,
                        columnarKeyStoreMetadata);
                columnarKeyStoreDataHolders[i] =
                        new ColumnarKeyStoreDataHolder(columnarKeyBlockData,
                                columnarKeyStoreMetadata);
            } else {
                columnarKeyStoreMetadata = new ColumnarKeyStoreMetadata(
                        columnarStoreInfo.getSizeOfEachBlock()[blockIndex[i]]);
                columnarKeyStoreMetadata.setColumnIndex(columnKeyBlockIndex);
                columnarKeyStoreMetadata.setSorted(columnarStoreInfo.getIsSorted()[blockIndex[i]]);
                columnarKeyStoreMetadata.setDataIndex(dataIndex);
                columnarKeyStoreMetadata.setColumnReverseIndex(columnKeyBlockReverseIndexes);
                columnarKeyStoreMetadata.setUnCompressed(isUnCompressed);
                columnarKeyStoreDataHolders[i] =
                        new ColumnarKeyStoreDataHolder(columnarKeyBlockData,
                                columnarKeyStoreMetadata);
            }
        }
        return columnarKeyStoreDataHolders;
    }

    /**
     * The high cardinality dimensions rows will be send in byte array with its data length
     * appended in the ColumnarKeyStoreDataHolder byte array since high cardinality dim data will not be
     * part of MDKey/Surrogate keys. In this method the byte array will be scanned and the length which is
     * stored in short will be removed.
     */
    private void mapColumnIndexWithKeyColumnarKeyBlockData(byte[] columnarKeyBlockData,
            ColumnarKeyStoreMetadata columnarKeyStoreMetadata) {
        Map<Integer, byte[]> mapOfColumnarKeyBlockData = new HashMap<Integer, byte[]>(50);
        ByteBuffer directSurrogateKeyStoreDataHolder =
                ByteBuffer.allocate(columnarKeyBlockData.length);
        directSurrogateKeyStoreDataHolder.put(columnarKeyBlockData);
        directSurrogateKeyStoreDataHolder.flip();
        int row = -1;
        while (directSurrogateKeyStoreDataHolder.hasRemaining()) {
            short dataLength = directSurrogateKeyStoreDataHolder.getShort();
            byte[] directSurrKeyData = new byte[dataLength];
            directSurrogateKeyStoreDataHolder.get(directSurrKeyData);
            mapOfColumnarKeyBlockData.put(++row, directSurrKeyData);
        }
        columnarKeyStoreMetadata.setDirectSurrogateKeyMembers(mapOfColumnarKeyBlockData);

    }

    @Override
    public ColumnarKeyStoreDataHolder getUnCompressedKeyArray(FileHolder fileHolder, int blockIndex,
            boolean needCompressedData) {
        byte[] columnarKeyBlockData = null;
        int[] columnKeyBlockIndex = null;
        int[] columnKeyBlockReverseIndex = null;
        ColumnarKeyStoreMetadata columnarKeyStoreMetadata = null;
        int columnarKeyBlockIndex = 0;
        int[] dataIndex = null;
        boolean isUnCompressed = true;
        columnarKeyBlockData = COMPRESSOR.unCompress(fileHolder
                .readByteArray(columnarStoreInfo.getFilePath(),
                        columnarStoreInfo.getKeyBlockOffsets()[blockIndex],
                        columnarStoreInfo.getKeyBlockLengths()[blockIndex]));
        if (blockIndex <= this.columnarStoreInfo.getAggKeyBlock().length - 1
                && this.columnarStoreInfo.getAggKeyBlock()[blockIndex]) {
            dataIndex = columnarStoreInfo.getNumberCompressor().unCompress(fileHolder
                    .readByteArray(columnarStoreInfo.getFilePath(),
                            columnarStoreInfo.getDataIndexMapOffsets()[mapOfAggDataIndex
                                    .get(blockIndex)],
                            columnarStoreInfo.getDataIndexMapLength()[mapOfAggDataIndex
                                    .get(blockIndex)]));
            if (!needCompressedData) {
                columnarKeyBlockData = UnBlockIndexer
                        .uncompressData(columnarKeyBlockData, dataIndex,
                                columnarStoreInfo.getSizeOfEachBlock()[blockIndex]);
                dataIndex = null;
            } else {
                isUnCompressed = false;
            }
        }
        if (!columnarStoreInfo.getIsSorted()[blockIndex]) {
            columnarKeyBlockIndex = mapOfColumnIndexAndColumnBlockIndex.get(blockIndex);
            columnKeyBlockIndex = MolapUtil.getUnCompressColumnIndex(
                    columnarStoreInfo.getKeyBlockIndexLength()[columnarKeyBlockIndex], fileHolder
                            .readByteArray(columnarStoreInfo.getFilePath(), columnarStoreInfo
                                    .getKeyBlockIndexOffsets()[columnarKeyBlockIndex],
                                    columnarStoreInfo
                                            .getKeyBlockIndexLength()[columnarKeyBlockIndex]),
                    columnarStoreInfo.getNumberCompressor());
            columnKeyBlockReverseIndex = getColumnIndexForNonFilter(columnKeyBlockIndex);
        }

        //Since its an high cardinality dimension, For filter queries.
        if (blockIndex > this.columnarStoreInfo.getSizeOfEachBlock().length - 1) {
            columnarKeyStoreMetadata = new ColumnarKeyStoreMetadata(0);
            columnarKeyStoreMetadata.setDirectSurrogateColumn(true);
            columnarKeyStoreMetadata.setColumnIndex(columnKeyBlockIndex);
            columnarKeyStoreMetadata.setColumnReverseIndex(columnKeyBlockReverseIndex);
            columnarKeyStoreMetadata.setSorted(columnarStoreInfo.getIsSorted()[blockIndex]);
            columnarKeyStoreMetadata.setUnCompressed(true);
            mapColumnIndexWithKeyColumnarKeyBlockData(columnarKeyBlockData,
                    columnarKeyStoreMetadata);
            ColumnarKeyStoreDataHolder columnarKeyStoreDataHolders =
                    new ColumnarKeyStoreDataHolder(columnarKeyBlockData, columnarKeyStoreMetadata);
            return columnarKeyStoreDataHolders;
        }
        columnarKeyStoreMetadata =
                new ColumnarKeyStoreMetadata(columnarStoreInfo.getSizeOfEachBlock()[blockIndex]);
        columnarKeyStoreMetadata.setColumnIndex(columnKeyBlockIndex);
        columnarKeyStoreMetadata.setSorted(columnarStoreInfo.getIsSorted()[blockIndex]);
        columnarKeyStoreMetadata.setDataIndex(dataIndex);
        columnarKeyStoreMetadata.setColumnReverseIndex(columnKeyBlockReverseIndex);
        columnarKeyStoreMetadata.setUnCompressed(isUnCompressed);
        if(columnarStoreInfo.getHybridStoreModel().isHybridStore() && blockIndex==0)
        {
            columnarKeyStoreMetadata.setRowStore(true);
        }
        ColumnarKeyStoreDataHolder columnarKeyStoreDataHolders = new ColumnarKeyStoreDataHolder(columnarKeyBlockData,
                columnarKeyStoreMetadata);
        return columnarKeyStoreDataHolders;
    }

}
