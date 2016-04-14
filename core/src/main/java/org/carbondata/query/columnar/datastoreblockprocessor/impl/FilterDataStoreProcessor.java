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

import java.util.BitSet;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.datastorage.store.columnar.ColumnarKeyStoreDataHolder;
import org.carbondata.core.datastorage.store.dataholder.CarbonReadDataHolder;
import org.carbondata.core.util.CarbonProperties;
import org.carbondata.query.columnar.datastoreblockprocessor.ColumnarDataStoreBlockProcessorInfo;
import org.carbondata.query.columnar.keyvalue.AbstractColumnarScanResult;
import org.carbondata.query.columnar.keyvalue.FilterScanResult;
import org.carbondata.query.datastorage.storeInterfaces.DataStoreBlock;
import org.carbondata.query.datastorage.tree.CSBTreeColumnarLeafNode;
import org.carbondata.query.evaluators.BlockDataHolder;
import org.carbondata.query.evaluators.FilterEvaluator;
import org.carbondata.query.util.CarbonEngineLogEvent;

public class FilterDataStoreProcessor extends AbstractColumnarDataStoreProcessor {

    private static final LogService LOGGER =
            LogServiceFactory.getLogService(FilterDataStoreProcessor.class.getName());

    private FilterEvaluator filterEvaluatorTree;

    public FilterDataStoreProcessor(ColumnarDataStoreBlockProcessorInfo columnarDataStoreBlockInfo,
            FilterEvaluator filterEvaluatorTree) {
        super(columnarDataStoreBlockInfo);
        this.filterEvaluatorTree = filterEvaluatorTree;
        this.keyValue = new FilterScanResult(this.columnarDataStoreBlockInfo.getKeySize(),
                columnarDataStoreBlockInfo.getDimensionIndexes());
    }

    public AbstractColumnarScanResult getScannedData(BlockDataHolder blockDataHolder,int[] noDictionaryColIndexes) {
        fillKeyValue(blockDataHolder,noDictionaryColIndexes);
        return keyValue;
    }

    protected void fillKeyValue(BlockDataHolder blockDataHolder,int[] noDictionaryColIndexes) {
        keyValue.reset();
        boolean isMinMaxEnabled = true;
        String minMaxEnableValue = CarbonProperties.getInstance().getProperty("carbon.enableMinMax");
        if (null != minMaxEnableValue) {
            isMinMaxEnabled = Boolean.parseBoolean(minMaxEnableValue);
        }
        if (isMinMaxEnabled) {
            BitSet bitSet = filterEvaluatorTree
                    .isScanRequired(blockDataHolder.getLeafDataBlock().getBlockMaxData(),
                            blockDataHolder.getLeafDataBlock().getBlockMinData());
            
            

            if (bitSet.isEmpty()) {
                keyValue.setNumberOfRows(0);
                keyValue.setIndexes(new int[0]);
                DataStoreBlock dataStoreBlock = blockDataHolder.getLeafDataBlock();
                if (dataStoreBlock instanceof CSBTreeColumnarLeafNode) {
                    String factFile = ((CSBTreeColumnarLeafNode) dataStoreBlock).getFactFile();
                    LOGGER.debug(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG,
                            "Skipping fact file because it is not required to "
                                    + "scan based on filter condtion:"
                                    + factFile);
                }
                return;
            }
        }

        BitSet bitSet = filterEvaluatorTree.applyFilter(blockDataHolder, null,noDictionaryColIndexes);

        if (bitSet.isEmpty()) {
            keyValue.setNumberOfRows(0);
            keyValue.setIndexes(new int[0]);
            return;
        }
        int[] indexes = new int[bitSet.cardinality()];
        int index = 0;
        for (int i = bitSet.nextSetBit(0); i >= 0; i = bitSet.nextSetBit(i + 1)) {
            indexes[index++] = i;
        }

        ColumnarKeyStoreDataHolder[] keyBlocks =
                new ColumnarKeyStoreDataHolder[columnarDataStoreBlockInfo
                        .getAllSelectedDimensions().length];

        for (int i = 0; i < columnarDataStoreBlockInfo.getAllSelectedDimensions().length; i++) {
            if (null == blockDataHolder.getColumnarKeyStore()[columnarDataStoreBlockInfo
                    .getAllSelectedDimensions()[i]]) {
                keyBlocks[i] = blockDataHolder.getLeafDataBlock()
                        .getColumnarKeyStore(columnarDataStoreBlockInfo.getFileHolder(),
                                columnarDataStoreBlockInfo.getAllSelectedDimensions()[i], false,noDictionaryColIndexes);
            } else {
                keyBlocks[i] = blockDataHolder.getColumnarKeyStore()[columnarDataStoreBlockInfo
                        .getAllSelectedDimensions()[i]];
                if (null != keyBlocks[i].getColumnarKeyStoreMetadata().getDataIndex()) {
                    keyBlocks[i].unCompress();
                }
            }
        }

        ColumnarKeyStoreDataHolder[] temp =
                new ColumnarKeyStoreDataHolder[columnarDataStoreBlockInfo
                        .getTotalNumberOfDimension()];
        for (int i = 0; i < columnarDataStoreBlockInfo.getAllSelectedDimensions().length; i++) {
            temp[columnarDataStoreBlockInfo.getAllSelectedDimensions()[i]] = keyBlocks[i];
        }

        CarbonReadDataHolder[] msrBlocks =
                new CarbonReadDataHolder[columnarDataStoreBlockInfo.getTotalNumberOfMeasures()];
        for (int i = 0; i < columnarDataStoreBlockInfo.getAllSelectedMeasures().length; i++) {
            if (null == blockDataHolder.getMeasureBlocks()[columnarDataStoreBlockInfo
                    .getAllSelectedMeasures()[i]]) {
                msrBlocks[columnarDataStoreBlockInfo.getAllSelectedMeasures()[i]] =
                        blockDataHolder.getLeafDataBlock().getNodeMsrDataWrapper(
                                columnarDataStoreBlockInfo.getAllSelectedMeasures()[i],
                                columnarDataStoreBlockInfo.getFileHolder())
                                .getValues()[columnarDataStoreBlockInfo
                                .getAllSelectedMeasures()[i]];
            } else {
                msrBlocks[columnarDataStoreBlockInfo.getAllSelectedMeasures()[i]] =
                        blockDataHolder.getMeasureBlocks()[columnarDataStoreBlockInfo
                                .getAllSelectedMeasures()[i]];
            }
        }
        keyValue.setMeasureBlock(msrBlocks);
        keyValue.setNumberOfRows(indexes.length);
        keyValue.setKeyBlock(temp);
        keyValue.setIndexes(indexes);
    }

}
