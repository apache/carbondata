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

package org.carbondata.processing.factreader;

import java.util.ArrayList;
import java.util.List;

import org.carbondata.core.constants.MolapCommonConstants;
import org.carbondata.core.datastorage.store.FileHolder;
import org.carbondata.core.datastorage.store.MeasureDataWrapper;
import org.carbondata.core.datastorage.store.compression.ValueCompressionModel;
import org.carbondata.core.datastorage.store.filesystem.MolapFile;
import org.carbondata.core.datastorage.store.impl.FileFactory;
import org.carbondata.core.datastorage.util.StoreFactory;
import org.carbondata.processing.iterator.MolapIterator;
import org.carbondata.core.metadata.LeafNodeInfo;
import org.carbondata.core.util.MolapProperties;
import org.carbondata.core.util.MolapUtil;

public class MolapLeafNodeIterator implements MolapIterator<MolapLeafNodeTuplesHolder> {
    /**
     * createKeyStore object which will hold the mdkey
     */
    private byte[] keyArray;

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
    private List<LeafNodeInfo> leafNodeInfoList;

    /**
     * leafNodeSize
     */
    private int leafNodeSize;

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
     * MolapLeafNodeIterator constructor to initialise iterator
     *
     * @param factFiles        fact files
     * @param measureCount
     * @param mdkeyLength
     * @param compressionModel
     */
    public MolapLeafNodeIterator(MolapFile[] factFiles, int measureCount, int mdkeyLength,
            ValueCompressionModel compressionModel) {
        this.fileHolder =
                FileFactory.getFileHolder(FileFactory.getFileType(factFiles[0].getAbsolutePath()));
        this.mdKeyLength = mdkeyLength;
        this.measureCount = measureCount;
        this.compressionModel = compressionModel;
        initialise(factFiles);
    }

    /**
     * below method will be used to initialise the iterator
     *
     * @param factFiles fact files
     */
    private void initialise(MolapFile[] factFiles) {
        this.leafNodeInfoList =
                new ArrayList<LeafNodeInfo>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
        // get the count of number of tuples present in leaf node
        this.leafNodeSize = Integer.parseInt(MolapProperties.getInstance()
                .getProperty(MolapCommonConstants.LEAFNODE_SIZE,
                        MolapCommonConstants.LEAFNODE_SIZE_DEFAULT_VAL));
        /**
         * below method 
         */
        List<LeafNodeInfo> leafNodeInfo = null;
        for (int i = 0; i < factFiles.length; i++) {
            leafNodeInfo = MolapUtil.getLeafNodeInfo(factFiles[i], measureCount, mdKeyLength);
            leafNodeInfoList.addAll(leafNodeInfo);
        }
        leafSize = leafNodeInfoList.size();
    }

    private void getNewLeafData() {
        LeafNodeInfo leafNodeInfo = leafNodeInfoList.get(currentCount++);
        this.keyArray = StoreFactory
                .createKeyStore(leafNodeSize, mdKeyLength, true, true, leafNodeInfo.getKeyOffset(),
                        leafNodeInfo.getFileName(), leafNodeInfo.getKeyLength(), fileHolder)
                .getBackArray(fileHolder);
        this.dataStore = StoreFactory
                .createDataStore(true, compressionModel, leafNodeInfo.getMeasureOffset(),
                        leafNodeInfo.getMeasureLength(), leafNodeInfo.getFileName(), fileHolder)
                .getBackData(null, fileHolder);
        this.entryCount = leafNodeInfo.getNumberOfKeys();
    }

    /**
     * check some more leaf are present in the b tree
     */
    @Override public boolean hasNext() {
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
    @Override public MolapLeafNodeTuplesHolder next() {
        MolapLeafNodeTuplesHolder holder = new MolapLeafNodeTuplesHolder();
        getNewLeafData();
        holder.setEntryCount(this.entryCount);
        holder.setMdKey(this.keyArray);
        holder.setMeasureDataWrapper(this.dataStore);
        return holder;
    }
}
