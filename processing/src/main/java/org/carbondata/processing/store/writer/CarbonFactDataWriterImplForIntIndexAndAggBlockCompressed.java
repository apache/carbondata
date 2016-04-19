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

package org.carbondata.processing.store.writer;

import org.carbondata.core.datastorage.store.columnar.IndexStorage;
import org.carbondata.core.datastorage.store.compression.SnappyCompression.SnappyByteCompression;
import org.carbondata.core.file.manager.composite.IFileManagerComposite;
import org.carbondata.core.keygenerator.mdkey.NumberCompressor;
import org.carbondata.core.util.CarbonUtil;

public class CarbonFactDataWriterImplForIntIndexAndAggBlockCompressed
        extends CarbonFactDataWriterImplForIntIndexAndAggBlock {

    private NumberCompressor[] keyBlockCompressor;

    public CarbonFactDataWriterImplForIntIndexAndAggBlockCompressed(String storeLocation,
            int measureCount, int mdKeyLength, String tableName, boolean isNodeHolder,
            IFileManagerComposite fileManager, int[] keyBlockSize, boolean[] aggBlocks,
            int[] cardinality, boolean isUpdateFact) {
        super(storeLocation, measureCount, mdKeyLength, tableName, isNodeHolder, fileManager,
                keyBlockSize, aggBlocks, isUpdateFact, null);
        this.keyBlockCompressor = new NumberCompressor[cardinality.length];
        for (int i = 0; i < cardinality.length; i++) {
            this.keyBlockCompressor[i] =
                    new NumberCompressor(Long.toBinaryString(cardinality[i]).length(), CarbonUtil
                            .getIncrementedFullyFilledRCDCardinalityFullyFilled(cardinality[i]));
        }
    }

    protected byte[][] fillAndCompressedKeyBlockData(IndexStorage<int[]>[] keyStorageArray,
            int entryCount) {
        byte[][] keyBlockData = new byte[keyStorageArray.length][];
        int destPos = 0;

        for (int i = 0; i < keyStorageArray.length; i++) {
            destPos = 0;
            if (aggBlocks[i]) {
                keyBlockData[i] = new byte[keyStorageArray[i].getTotalSize()];
                for (int m = 0; m < keyStorageArray[i].getKeyBlock().length; m++) {
                    System.arraycopy(keyStorageArray[i].getKeyBlock()[m], 0, keyBlockData[i],
                            destPos, keyStorageArray[i].getKeyBlock()[m].length);
                    destPos += keyStorageArray[i].getKeyBlock()[m].length;
                }
                keyBlockData[i] = this.keyBlockCompressor[i].compressBytes(keyBlockData[i]);
            } else {
                keyBlockData[i] = new byte[entryCount * keyBlockSize[i]];
                for (int j = 0; j < keyStorageArray[i].getKeyBlock().length; j++) {
                    System.arraycopy(keyStorageArray[i].getKeyBlock()[j], 0, keyBlockData[i],
                            destPos, keyBlockSize[i]);
                    destPos += keyBlockSize[i];
                }
                keyBlockData[i] = this.keyBlockCompressor[i].compressBytes(keyBlockData[i]);
                keyBlockData[i] = SnappyByteCompression.INSTANCE.compress(keyBlockData[i]);
            }

        }
        return keyBlockData;
    }

}
