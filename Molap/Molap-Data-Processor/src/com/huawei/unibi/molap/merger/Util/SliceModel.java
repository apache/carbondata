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

/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwe/owl+XpObKvwejIomJrN10iZBX17jBC5vj/zP
61+XaclqAO9d103KRwWYi0UGOOvZ8/h4W92oYB8y2/tthAMlXAmNI97jcdh3Ea9O+zyziCf3
XthCJhyPxf2zjCdQRQ//BTyRDJxBpstnMIEk4NZXvPzCFrx8jDorwHypI25S/w==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
/**
 *
 * Copyright Notice
 * =====================================
 * This file contains proprietary information of
 * Huawei Technologies India Pvt Ltd.
 * Copying or reproduction without prior written approval is prohibited.
 * Copyright (c) 2013
 * =====================================
 *
 */
package com.huawei.unibi.molap.merger.Util;

import java.util.ArrayList;
import java.util.List;

import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.datastorage.store.FileHolder;
import com.huawei.unibi.molap.datastorage.store.MeasureDataWrapper;
import com.huawei.unibi.molap.datastorage.store.NodeKeyStore;
import com.huawei.unibi.molap.datastorage.store.NodeMeasureDataStore;
import com.huawei.unibi.molap.datastorage.store.compression.ValueCompressionModel;
import com.huawei.unibi.molap.datastorage.store.impl.FileFactory;
import com.huawei.unibi.molap.datastorage.util.StoreFactory;
import com.huawei.unibi.molap.metadata.LeafNodeInfo;
import com.huawei.unibi.molap.util.MolapProperties;

/**
 * 
 * Project Name NSE V3R7C00 
 * Module Name : Molap Data Processor
 * Author K00900841
 * Created Date :21-May-2013 6:42:29 PM
 * FileName : SliceModel.java
 * Class Description : Slice model class 
 * Version 1.0
 */
public class SliceModel
{
    /**
     * compressionModel
     */
    private ValueCompressionModel compressionModel;

    /**
     * createKeyStore object which will hold the mdkey
     */
    private List<NodeKeyStore> keyStoreList;
    
    /**
     * entryCountList
     */
    private List<Integer> entryCountList;

    /**
     * data store which will hold the measure data
     */
    private List<NodeMeasureDataStore> dataStoreList;

    /**
     * file holder
     */
    private FileHolder fileHolder;
    
    /**
     * uniqueValue
     */
    private double[]uniqueValue;
   /**
    * SliceModel
    * 
    * @param sliceInfo 
    *           slice info
    *
    */
    public SliceModel(SliceInfo sliceInfo)
    {
        this.compressionModel = sliceInfo.getCompressionModel();
        this.uniqueValue=this.compressionModel.getUniqueValue();
        initialize(sliceInfo);
    }

    /**
     * This method will create the key and data store for slice It will read all
     * the leaf node info and create the key store and data store and add to key
     * store and data store list
     * 
     * @param sliceInfo
     *          slice info
     * 
     */
    private void initialize(SliceInfo sliceInfo)
    {
        // get leaf node size
        List<LeafNodeInfo> leafNodeInfoList = sliceInfo.getLeafNodeInfoList();
        int mdKeyLength = leafNodeInfoList.get(0).getStartKey().length;
        int leafNodeSize = Integer.parseInt(MolapProperties.getInstance().getProperty(
                MolapCommonConstants.LEAFNODE_SIZE, MolapCommonConstants.LEAFNODE_SIZE_DEFAULT_VAL));
        this.fileHolder = FileFactory.getFileHolder(FileFactory.getFileType());
        keyStoreList = new ArrayList<NodeKeyStore>(MolapCommonConstants.CONSTANT_SIZE_TEN);
        dataStoreList = new ArrayList<NodeMeasureDataStore>(MolapCommonConstants.CONSTANT_SIZE_TEN);
        entryCountList = new ArrayList<Integer>(MolapCommonConstants.CONSTANT_SIZE_TEN);
        for(LeafNodeInfo info : leafNodeInfoList)
        {
            NodeKeyStore keyStore = StoreFactory.createKeyStore(leafNodeSize, mdKeyLength, true, true,
                    info.getKeyOffset(), info.getFileName(), info.getKeyLength(),
                    fileHolder);
            keyStoreList.add(keyStore);
            NodeMeasureDataStore dataStore = StoreFactory.createDataStore(true, compressionModel, info.getMeasureOffset(),
                    info.getMeasureLength(), info.getFileName(), fileHolder);
            dataStoreList.add(dataStore);
            entryCountList.add(info.getNumberOfKeys());
        }
    }
    
    /**
     * getEnrtyCount
     * @return int
     */
    public int getEnrtyCount()
    {
        return entryCountList.remove(0);
    }
    /**
     * This method will remove the lead node from key store list and will return
     * the back array
     * 
     * @return back array
     * 
     */
    public byte[] getKeyData()
    {
        return keyStoreList.remove(0).getBackArray(fileHolder);
    }

    /**
     * This method will be used to get the measures value
     * 
     * @param index
     *          index
     * @return measures
     *
     */
    public MeasureDataWrapper getMeasureData()
    {
        return dataStoreList.remove(0).getBackData(null, fileHolder);
    }

    /**
     * This method will check whether any more leaf is present in the key store
     * list or not
     * 
     * @return isLeaf present 
     * 
     */
    public boolean hasRemaning()
    {
        return keyStoreList.size() > 0 && dataStoreList.size()>0;
    }

    /**
     * This method will be used to close the file channel
     */
    public void close()
    {
        fileHolder.finish();
    }

    /**
     * getUniqueValue
     * @return double[]
     */
    public double[] getUniqueValue()
    {
        return uniqueValue;
    }
}
