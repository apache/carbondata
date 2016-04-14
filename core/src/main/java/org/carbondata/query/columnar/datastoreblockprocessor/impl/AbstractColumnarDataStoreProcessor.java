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

package org.carbondata.query.columnar.datastoreblockprocessor.impl;

import org.carbondata.core.datastorage.store.columnar.ColumnarKeyStoreDataHolder;
import org.carbondata.query.columnar.datastoreblockprocessor.ColumnarDataStoreBlockProcessorInfo;
import org.carbondata.query.columnar.datastoreblockprocessor.DataStoreBlockProcessor;
import org.carbondata.query.columnar.keyvalue.AbstractColumnarScanResult;
import org.carbondata.query.evaluators.BlockDataHolder;

public abstract class AbstractColumnarDataStoreProcessor implements DataStoreBlockProcessor {
    protected AbstractColumnarScanResult keyValue;

    protected ColumnarDataStoreBlockProcessorInfo columnarDataStoreBlockInfo;

    public AbstractColumnarDataStoreProcessor(
            ColumnarDataStoreBlockProcessorInfo columnarDataStoreBlockInfo) {
        this.columnarDataStoreBlockInfo = columnarDataStoreBlockInfo;
    }

    protected void fillKeyValue(BlockDataHolder blockDataHolder,int[] noDictionaryColIndexes) {
        keyValue.reset();
        keyValue.setMeasureBlock(blockDataHolder.getLeafDataBlock()
                .getNodeMsrDataWrapper(columnarDataStoreBlockInfo.getAllSelectedMeasures(),
                        columnarDataStoreBlockInfo.getFileHolder()).getValues());
        keyValue.setNumberOfRows(blockDataHolder.getLeafDataBlock().getnKeys());
        ColumnarKeyStoreDataHolder[] columnarKeyStore = blockDataHolder.getLeafDataBlock()
                .getColumnarKeyStore(columnarDataStoreBlockInfo.getFileHolder(),
                        columnarDataStoreBlockInfo.getAllSelectedDimensions(),
                        new boolean[columnarDataStoreBlockInfo.getAllSelectedDimensions().length],noDictionaryColIndexes);
        ColumnarKeyStoreDataHolder[] temp =
                new ColumnarKeyStoreDataHolder[columnarDataStoreBlockInfo
                        .getTotalNumberOfDimension()];
        for (int i = 0; i < columnarDataStoreBlockInfo.getAllSelectedDimensions().length; i++) {
            temp[columnarDataStoreBlockInfo.getAllSelectedDimensions()[i]] = columnarKeyStore[i];
        }
        keyValue.setKeyBlock(temp);
    }

    @Override
    public AbstractColumnarScanResult getScannedData(BlockDataHolder blockDataHolder,int[] noDictionaryColIndexes) {
        fillKeyValue(blockDataHolder,noDictionaryColIndexes);
        return keyValue;
    }
}
