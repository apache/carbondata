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

package com.huawei.unibi.molap.factreader.columnar;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.datastorage.store.FileHolder;
import com.huawei.unibi.molap.datastorage.store.MeasureDataWrapper;
import com.huawei.unibi.molap.datastorage.store.columnar.ColumnarKeyStore;
import com.huawei.unibi.molap.datastorage.store.columnar.ColumnarKeyStoreDataHolder;
import com.huawei.unibi.molap.datastorage.store.compression.ValueCompressionModel;
import com.huawei.unibi.molap.datastorage.store.filesystem.MolapFile;
import com.huawei.unibi.molap.datastorage.store.impl.FileFactory;
import com.huawei.unibi.molap.datastorage.util.StoreFactory;
import com.huawei.unibi.molap.engine.columnar.keyvalue.AbstractColumnarScanResult;
import com.huawei.unibi.molap.engine.columnar.keyvalue.NonFilterScanResult;
import com.huawei.unibi.molap.factreader.FactReaderInfo;
import com.huawei.unibi.molap.iterator.MolapIterator;
import com.huawei.unibi.molap.keygenerator.columnar.ColumnarSplitter;
import com.huawei.unibi.molap.keygenerator.columnar.impl.MultiDimKeyVarLengthEquiSplitGenerator;
import com.huawei.unibi.molap.metadata.LeafNodeInfoColumnar;
import com.huawei.unibi.molap.util.MolapProperties;
import com.huawei.unibi.molap.util.MolapUtil;

public class MolapColumnarLeafNodeIterator implements
                    MolapIterator<AbstractColumnarScanResult>
{
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
     * MolapLeafNodeIterator constructor to initialise iterator
     * @param factFiles
     *          fact files 
     * @param mdkeyLength
     * @param compressionModel
     */
    public MolapColumnarLeafNodeIterator(MolapFile[] factFiles,
            int mdkeyLength,ValueCompressionModel compressionModel, FactReaderInfo iteratorInfo)
    {
        intialiseColumnarLeafNodeIterator(factFiles, mdkeyLength, compressionModel, iteratorInfo);
        initialise(factFiles,null);
        this.needCompressedData= new boolean[blockIndexes.length];
        Arrays.fill(needCompressedData, true);
    }
    
    public MolapColumnarLeafNodeIterator(MolapFile[] factFiles, int mdKeySize,
			ValueCompressionModel compressionModel,
			FactReaderInfo factReaderInfo,
			LeafNodeInfoColumnar leafNodeInfoColumnar) {
    	intialiseColumnarLeafNodeIterator(factFiles, mdKeySize, compressionModel, factReaderInfo);
    	initialise(factFiles,leafNodeInfoColumnar);
        this.needCompressedData= new boolean[blockIndexes.length];
        Arrays.fill(needCompressedData, true);
	}

	private void intialiseColumnarLeafNodeIterator(MolapFile[] factFiles, int mdkeyLength,
			ValueCompressionModel compressionModel, FactReaderInfo iteratorInfo) {
		this.fileHolder = FileFactory.getFileHolder(FileFactory.getFileType(factFiles[0].getAbsolutePath()));
        this.mdKeyLength=mdkeyLength;
        this.measureCount=iteratorInfo.getMeasureCount();
        this.compressionModel=compressionModel;
        blockIndexes = iteratorInfo.getBlockIndex();
        this.isUniqueBlock= new boolean[iteratorInfo.getDimLens().length];
        boolean isAggKeyBlock = Boolean
                .parseBoolean(MolapCommonConstants.AGGREAGATE_COLUMNAR_KEY_BLOCK_DEFAULTVALUE);
        if(isAggKeyBlock)
        {
            int highCardinalityValue = Integer
                    .parseInt(MolapProperties
                            .getInstance()
                            .getProperty(
                                    MolapCommonConstants.HIGH_CARDINALITY_VALUE,
                                    MolapCommonConstants.HIGH_CARDINALITY_VALUE_DEFAULTVALUE));
            for(int i = 0;i < iteratorInfo.getDimLens().length;i++)
            {
                if(iteratorInfo.getDimLens()[i] < highCardinalityValue)
                {
                    this.isUniqueBlock[i] = true;
                }
            }
        }
        
        int dimSet=Integer.parseInt(MolapCommonConstants.DIMENSION_SPLIT_VALUE_IN_COLUMNAR_DEFAULTVALUE);
        ColumnarSplitter columnarSplitter = new MultiDimKeyVarLengthEquiSplitGenerator(
                MolapUtil.getIncrementedCardinalityFullyFilled(iteratorInfo
                        .getDimLens()), (byte)dimSet);
        blockKeySize = columnarSplitter.getBlockKeySize();
        int keySize=0;
        for(int i = 0;i < blockIndexes.length;i++)
        {
            keySize+=blockKeySize[blockIndexes[i]];
        }
        this.keyValue= new NonFilterScanResult(keySize,blockIndexes);
	}



	/**
     * below method will be used to initialise the iterator
     * @param factFiles
     *          fact files
	 * @param leafNodeInfoColumnar 
     */
	private void initialise(MolapFile[] factFiles,
			LeafNodeInfoColumnar leafNodeInfoColumnar) {
		this.leafNodeInfoList = new ArrayList<LeafNodeInfoColumnar>(
				MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
		if (null != leafNodeInfoColumnar) {
			leafNodeInfoColumnar.setAggKeyBlock(isUniqueBlock);
			leafNodeInfoList.add(leafNodeInfoColumnar);
		} else {
			List<LeafNodeInfoColumnar> leafNodeInfo = null;
			for (int i = 0; i < factFiles.length; i++) {
				leafNodeInfo = MolapUtil.getLeafNodeInfoColumnar(factFiles[i],
						measureCount, mdKeyLength);
				for (LeafNodeInfoColumnar leafInfo : leafNodeInfo) {
					leafInfo.setAggKeyBlock(isUniqueBlock);
				}
				leafNodeInfoList.addAll(leafNodeInfo);
			}
		}
		leafSize = leafNodeInfoList.size();
	}
    
    private void getNewLeafData()
    {
        LeafNodeInfoColumnar leafNodeInfo = leafNodeInfoList.get(currentCount++);
        keyStore = StoreFactory.createColumnarKeyStore(MolapUtil.getColumnarKeyStoreInfo(leafNodeInfo, blockKeySize), fileHolder,true);
        this.dataStore = StoreFactory.createDataStore(true, compressionModel, leafNodeInfo.getMeasureOffset(),
                leafNodeInfo.getMeasureLength(), leafNodeInfo.getFileName(), fileHolder).getBackData(null, fileHolder);
        this.entryCount=leafNodeInfo.getNumberOfKeys();
        this.keyValue.reset();
        this.keyValue.setNumberOfRows(this.entryCount);
        this.keyValue.setMeasureBlock(this.dataStore.getValues());
        ColumnarKeyStoreDataHolder[] unCompressedKeyArray = keyStore.getUnCompressedKeyArray(fileHolder, blockIndexes, needCompressedData);
        
        for(int i = 0;i < unCompressedKeyArray.length;i++)
        {
            if(this.isUniqueBlock[blockIndexes[i]])
            {
                unCompressedKeyArray[i].unCompress();
            }
        }
        this.keyValue.setKeyBlock(unCompressedKeyArray);
    }
    
    /**
     * check some more leaf are present in the b tree 
     */
    @Override
    public boolean hasNext()
    {
        if(currentCount<leafSize)
        {
            return true;
        }
        else
        {
            fileHolder.finish();
        }
        return false;
    }

    /**
     * below method will be used to get the leaf node
     */
    @Override
    public AbstractColumnarScanResult next()
    {
        getNewLeafData();
        return keyValue;
    }
}
