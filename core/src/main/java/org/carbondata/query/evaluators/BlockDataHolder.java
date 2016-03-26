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

package org.carbondata.query.evaluators;

import org.carbondata.core.datastorage.store.FileHolder;
import org.carbondata.core.datastorage.store.columnar.ColumnarKeyStoreDataHolder;
import org.carbondata.core.datastorage.store.dataholder.MolapReadDataHolder;
import org.carbondata.query.datastorage.storeInterfaces.DataStoreBlock;

public class BlockDataHolder {
    private MolapReadDataHolder[] measureBlocks;

    private ColumnarKeyStoreDataHolder[] columnarKeyStore;

    private DataStoreBlock leafDataBlock;

    private FileHolder fileHolder;

    public BlockDataHolder(int dimColumnCount, int msrColumnCount) {
        this.measureBlocks = new MolapReadDataHolder[msrColumnCount];
        this.columnarKeyStore = new ColumnarKeyStoreDataHolder[dimColumnCount];
    }

    public MolapReadDataHolder[] getMeasureBlocks() {
        return measureBlocks;
    }

    public void setMeasureBlocks(final MolapReadDataHolder[] measureBlocks) {
        this.measureBlocks = measureBlocks;
    }

    public ColumnarKeyStoreDataHolder[] getColumnarKeyStore() {
        return columnarKeyStore;
    }

    public void setColumnarKeyStore(final ColumnarKeyStoreDataHolder[] columnarKeyStore) {
        this.columnarKeyStore = columnarKeyStore;
    }

    public DataStoreBlock getLeafDataBlock() {
        return leafDataBlock;
    }

    public void setLeafDataBlock(final DataStoreBlock dataBlock) {
        this.leafDataBlock = dataBlock;
    }

    public FileHolder getFileHolder() {
        return fileHolder;
    }

    public void setFileHolder(final FileHolder fileHolder) {
        this.fileHolder = fileHolder;
    }

    public void reset() {
        for (int i = 0; i < measureBlocks.length; i++) {
            this.measureBlocks[i] = null;
        }
        for (int i = 0; i < columnarKeyStore.length; i++) {
            this.columnarKeyStore[i] = null;
        }
    }
}
