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

package org.carbondata.processing.suggest.datastats.load;

import org.carbondata.core.datastorage.store.FileHolder;
import org.carbondata.core.datastorage.store.columnar.ColumnarKeyStore;
import org.carbondata.core.datastorage.store.columnar.ColumnarKeyStoreDataHolder;
import org.carbondata.core.datastorage.store.columnar.ColumnarKeyStoreInfo;
import org.carbondata.core.datastorage.store.compression.ValueCompressionModel;
import org.carbondata.core.datastorage.util.StoreFactory;
import org.carbondata.core.metadata.LeafNodeInfoColumnar;
import org.carbondata.core.util.CarbonUtil;

/**
 * This class contains compressed Columnkey block of store and Measure value
 *
 * @author A00902717
 */
public class FactDataNode {

    /**
     * Compressed keyblocks
     */
    private ColumnarKeyStore keyStore;

    private int maxKeys;

    public FactDataNode(int maxKeys, int[] eachBlockSize, boolean isFileStore,
            FileHolder fileHolder, LeafNodeInfoColumnar leafNodeInfo,
            ValueCompressionModel compressionModel) {

        this.maxKeys = maxKeys;

        ColumnarKeyStoreInfo columnarStoreInfo =
                CarbonUtil.getColumnarKeyStoreInfo(leafNodeInfo, eachBlockSize, null);
        keyStore = StoreFactory.createColumnarKeyStore(columnarStoreInfo, fileHolder, isFileStore);

    }

    public ColumnarKeyStoreDataHolder[] getColumnData(FileHolder fileHolder, int[] dimensions,
            boolean[] needCompression,int[] noDictionaryColIndexes) {

        ColumnarKeyStoreDataHolder[] keyDataHolderUncompressed =
                keyStore.getUnCompressedKeyArray(fileHolder, dimensions, needCompression,noDictionaryColIndexes);

        return keyDataHolderUncompressed;

    }
    
    /**
     * getColumnData.
     * @param fileHolder
     * @param dimensions
     * @param needCompression
     * @param noDictionaryColIndexes
     * @return
     */
    public ColumnarKeyStoreDataHolder getColumnData(FileHolder fileHolder, int dimensions,
            boolean needCompression,int[] noDictionaryColIndexes) {

        ColumnarKeyStoreDataHolder keyDataHolderUncompressed =
                keyStore.getUnCompressedKeyArray(fileHolder, dimensions, needCompression,noDictionaryColIndexes);

        return keyDataHolderUncompressed;

    }

    public int getMaxKeys() {
        return maxKeys;
    }

}
