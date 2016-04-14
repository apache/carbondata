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

package org.carbondata.processing.factreader.columnar;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.datastorage.store.FileHolder;
import org.carbondata.core.datastorage.store.MeasureDataWrapper;
import org.carbondata.core.datastorage.store.columnar.ColumnarKeyStore;
import org.carbondata.core.datastorage.store.columnar.ColumnarKeyStoreDataHolder;
import org.carbondata.core.datastorage.store.compression.ValueCompressionModel;
import org.carbondata.core.datastorage.store.filesystem.CarbonFile;
import org.carbondata.core.datastorage.store.impl.FileFactory;
import org.carbondata.core.datastorage.util.StoreFactory;
import org.carbondata.core.keygenerator.columnar.ColumnarSplitter;
import org.carbondata.core.keygenerator.columnar.impl.MultiDimKeyVarLengthEquiSplitGenerator;
import org.carbondata.core.metadata.LeafNodeInfoColumnar;
import org.carbondata.core.util.CarbonProperties;
import org.carbondata.core.util.CarbonUtil;
import org.carbondata.processing.factreader.FactReaderInfo;
import org.carbondata.processing.iterator.CarbonIterator;
import org.carbondata.query.columnar.keyvalue.AbstractColumnarScanResult;
import org.carbondata.query.columnar.keyvalue.NonFilterScanResult;

public class CarbonColumnarLeafNodeIterator implements CarbonIterator<AbstractColumnarScanResult> {
    /**
     * entryCountList
     */
    private int entryCount;

    /**
     * data store which will hold the measure data
     */
    private MeasureDataWrapper dataStore;

    /**
     * fileHolder
     */
    private FileHolder fileHolder;

    /**
     * leafSize
     */
    private int leafSize;

    /**
     * currentCount
     */
    private int currentCount;

    /**
     * leafNodeInfo
     */
    private List<LeafNodeInfoColumnar> leafNodeInfoList;

    /**
     * mdKeyLength
     */
    private int mdKeyLength;

    /**
     * measureCount
     */
    private int measureCount;

    /**
     * compressionModel
     */
    private ValueCompressionModel compressionModel;

    /**
     * isUniqueBlock
     */
    private boolean[] isUniqueBlock;

    /**
     * blockKeySize
     */
    private int[] blockKeySize;

    /**
     * Key array
     */
    private ColumnarKeyStore keyStore;

    /**
     * keyValue
     */
    private AbstractColumnarScanResult keyValue;

    /**
     * blockIndexes
     */
    private int[] blockIndexes;

    /**
     * needCompressedData
     */
    private boolean[] needCompressedData;

    /**
     * CarbonLeafNodeIterator constructor to initialise iterator
     *
     * @param factFiles        fact files
     * @param mdkeyLength
     * @param compressionModel
     */
    public CarbonColumnarLeafNodeIterator(CarbonFile[] factFiles, int mdkeyLength,
            ValueCompressionModel compressionModel, FactReaderInfo iteratorInfo) {
        intialiseColumnarLeafNodeIterator(factFiles, mdkeyLength, compressionModel, iteratorInfo);
        initialise(factFiles, null);
        this.needCompressedData = new boolean[blockIndexes.length];
        Arrays.fill(needCompressedData, true);
    }

    public CarbonColumnarLeafNodeIterator(CarbonFile[] factFiles, int mdKeySize,
            ValueCompressionModel compressionModel, FactReaderInfo factReaderInfo,
            LeafNodeInfoColumnar leafNodeInfoColumnar) {
        intialiseColumnarLeafNodeIterator(factFiles, mdKeySize, compressionModel, factReaderInfo);
        initialise(factFiles, leafNodeInfoColumnar);
        this.needCompressedData = new boolean[blockIndexes.length];
        Arrays.fill(needCompressedData, true);
    }

    private void intialiseColumnarLeafNodeIterator(CarbonFile[] factFiles, int mdkeyLength,
            ValueCompressionModel compressionModel, FactReaderInfo iteratorInfo) {
        this.fileHolder =
                FileFactory.getFileHolder(FileFactory.getFileType(factFiles[0].getAbsolutePath()));
        this.mdKeyLength = mdkeyLength;
        this.measureCount = iteratorInfo.getMeasureCount();
        this.compressionModel = compressionModel;
        blockIndexes = iteratorInfo.getBlockIndex();
        this.isUniqueBlock = new boolean[iteratorInfo.getDimLens().length];
        boolean isAggKeyBlock = Boolean.parseBoolean(
                CarbonCommonConstants.AGGREAGATE_COLUMNAR_KEY_BLOCK_DEFAULTVALUE);
        if (isAggKeyBlock) {
            int noDictionaryValue = Integer.parseInt(CarbonProperties.getInstance()
                    .getProperty(CarbonCommonConstants.HIGH_CARDINALITY_VALUE,
                            CarbonCommonConstants.HIGH_CARDINALITY_VALUE_DEFAULTVALUE));
            for (int i = 0; i < iteratorInfo.getDimLens().length; i++) {
                if (iteratorInfo.getDimLens()[i] < noDictionaryValue) {
                    this.isUniqueBlock[i] = true;
                }
            }
        }

        int dimSet = Integer.parseInt(
                CarbonCommonConstants.DIMENSION_SPLIT_VALUE_IN_COLUMNAR_DEFAULTVALUE);
        ColumnarSplitter columnarSplitter = new MultiDimKeyVarLengthEquiSplitGenerator(
                CarbonUtil.getIncrementedCardinalityFullyFilled(iteratorInfo.getDimLens()),
                (byte) dimSet);
        blockKeySize = columnarSplitter.getBlockKeySize();
        int keySize = 0;
        for (int i = 0; i < blockIndexes.length; i++) {
            keySize += blockKeySize[blockIndexes[i]];
        }
        this.keyValue = new NonFilterScanResult(keySize, blockIndexes);
    }

    /**
     * below method will be used to initialise the iterator
     *
     * @param factFiles            fact files
     * @param leafNodeInfoColumnar
     */
    private void initialise(CarbonFile[] factFiles, LeafNodeInfoColumnar leafNodeInfoColumnar) {
        this.leafNodeInfoList =
                new ArrayList<LeafNodeInfoColumnar>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
        if (null != leafNodeInfoColumnar) {
            leafNodeInfoColumnar.setAggKeyBlock(isUniqueBlock);
            leafNodeInfoList.add(leafNodeInfoColumnar);
        } else {
            List<LeafNodeInfoColumnar> leafNodeInfo = null;
            for (int i = 0; i < factFiles.length; i++) {
                leafNodeInfo =
                        CarbonUtil.getLeafNodeInfoColumnar(factFiles[i], measureCount, mdKeyLength);
                for (LeafNodeInfoColumnar leafInfo : leafNodeInfo) {
                    leafInfo.setAggKeyBlock(isUniqueBlock);
                }
                leafNodeInfoList.addAll(leafNodeInfo);
            }
        }
        leafSize = leafNodeInfoList.size();
    }

    private void getNewLeafData() {
        LeafNodeInfoColumnar leafNodeInfo = leafNodeInfoList.get(currentCount++);
        keyStore = StoreFactory.createColumnarKeyStore(
                CarbonUtil.getColumnarKeyStoreInfo(leafNodeInfo, blockKeySize, null), fileHolder,
                true);
        this.dataStore = StoreFactory
                .createDataStore(true, compressionModel, leafNodeInfo.getMeasureOffset(),
                        leafNodeInfo.getMeasureLength(), leafNodeInfo.getFileName(), fileHolder)
                .getBackData(null, fileHolder);
        this.entryCount = leafNodeInfo.getNumberOfKeys();
        this.keyValue.reset();
        this.keyValue.setNumberOfRows(this.entryCount);
        this.keyValue.setMeasureBlock(this.dataStore.getValues());
        ColumnarKeyStoreDataHolder[] unCompressedKeyArray =
                keyStore.getUnCompressedKeyArray(fileHolder, blockIndexes, needCompressedData,null);

        for (int i = 0; i < unCompressedKeyArray.length; i++) {
            if (this.isUniqueBlock[blockIndexes[i]]) {
                unCompressedKeyArray[i].unCompress();
            }
        }
        this.keyValue.setKeyBlock(unCompressedKeyArray);
    }

    /**
     * check some more leaf are present in the b tree
     */
    @Override
    public boolean hasNext() {
        if (currentCount < leafSize) {
            return true;
        } else {
            fileHolder.finish();
        }
        return false;
    }

    /**
     * below method will be used to get the leaf node
     */
    @Override
    public AbstractColumnarScanResult next() {
        getNewLeafData();
        return keyValue;
    }
}
