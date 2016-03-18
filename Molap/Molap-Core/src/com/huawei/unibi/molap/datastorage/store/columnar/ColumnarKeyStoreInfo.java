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

package com.huawei.unibi.molap.datastorage.store.columnar;

import com.huawei.unibi.molap.keygenerator.mdkey.NumberCompressor;
import com.huawei.unibi.molap.vo.HybridStoreModel;

public class ColumnarKeyStoreInfo {
    private int numberOfKeys;

    private int[] sizeOfEachBlock;

    private int[] keyBlockLengths;

    private long[] keyBlockOffsets;

    private int[] keyBlockIndexLength;

    private long[] keyBlockIndexOffsets;

    private String filePath;

    private boolean[] isSorted;

    private int[] cardinality;

    private NumberCompressor numberCompressor;

    private NumberCompressor[] keyBlockUnCompressor;

    private HybridStoreModel hybridStoreModel;
    
    /**
     * dataIndexMap
     */
    private int[] dataIndexMapLength;

    /**
     * dataIndexMap
     */
    private long[] dataIndexMapOffsets;

    /**
     * aggKeyBlock
     */
    private boolean[] aggKeyBlock;

    /**
     * @return the numberOfKeys
     */
    public int getNumberOfKeys() {
        return numberOfKeys;
    }

    /**
     * @param numberOfKeys the numberOfKeys to set
     */
    public void setNumberOfKeys(int numberOfKeys) {
        this.numberOfKeys = numberOfKeys;
    }

    /**
     * @return the sizeOfEachBlock
     */
    public int[] getSizeOfEachBlock() {
        return sizeOfEachBlock;
    }

    /**
     * @param sizeOfEachBlock the sizeOfEachBlock to set
     */
    public void setSizeOfEachBlock(int[] sizeOfEachBlock) {
        this.sizeOfEachBlock = sizeOfEachBlock;
    }

    /**
     * @return the keyBlockLengths
     */
    public int[] getKeyBlockLengths() {
        return keyBlockLengths;
    }

    /**
     * @param keyBlockLengths the keyBlockLengths to set
     */
    public void setKeyBlockLengths(int[] keyBlockLengths) {
        this.keyBlockLengths = keyBlockLengths;
    }

    /**
     * @return the keyBlockOffsets
     */
    public long[] getKeyBlockOffsets() {
        return keyBlockOffsets;
    }

    /**
     * @param keyBlockOffsets the keyBlockOffsets to set
     */
    public void setKeyBlockOffsets(long[] keyBlockOffsets) {
        this.keyBlockOffsets = keyBlockOffsets;
    }

    /**
     * @return the keyBlockIndexLength
     */
    public int[] getKeyBlockIndexLength() {
        return keyBlockIndexLength;
    }

    /**
     * @param keyBlockIndexLength the keyBlockIndexLength to set
     */
    public void setKeyBlockIndexLength(int[] keyBlockIndexLength) {
        this.keyBlockIndexLength = keyBlockIndexLength;
    }

    /**
     * @return the keyBlockIndexOffsets
     */
    public long[] getKeyBlockIndexOffsets() {
        return keyBlockIndexOffsets;
    }

    /**
     * @param keyBlockIndexOffsets the keyBlockIndexOffsets to set
     */
    public void setKeyBlockIndexOffsets(long[] keyBlockIndexOffsets) {
        this.keyBlockIndexOffsets = keyBlockIndexOffsets;
    }

    /**
     * @return the filePath
     */
    public String getFilePath() {
        return filePath;
    }

    /**
     * @param filePath the filePath to set
     */
    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    /**
     * @return the isSorted
     */
    public boolean[] getIsSorted() {
        return isSorted;
    }

    /**
     * @param isSorted the isSorted to set
     */
    public void setIsSorted(boolean[] isSorted) {
        this.isSorted = isSorted;
    }

    /**
     * @return the numberCompressor
     */
    public NumberCompressor getNumberCompressor() {
        return numberCompressor;
    }

    /**
     * @param numberCompressor the numberCompressor to set
     */
    public void setNumberCompressor(NumberCompressor numberCompressor) {
        this.numberCompressor = numberCompressor;
    }

    /**
     * @return the dataIndexMapLength
     */
    public int[] getDataIndexMapLength() {
        return dataIndexMapLength;
    }

    /**
     * @param dataIndexMapLength the dataIndexMapLength to set
     */
    public void setDataIndexMapLength(int[] dataIndexMapLength) {
        this.dataIndexMapLength = dataIndexMapLength;
    }

    /**
     * @return the dataIndexMapOffsets
     */
    public long[] getDataIndexMapOffsets() {
        return dataIndexMapOffsets;
    }

    /**
     * @param dataIndexMapOffsets the dataIndexMapOffsets to set
     */
    public void setDataIndexMapOffsets(long[] dataIndexMapOffsets) {
        this.dataIndexMapOffsets = dataIndexMapOffsets;
    }

    /**
     * @return the aggKeyBlock
     */
    public boolean[] getAggKeyBlock() {
        return aggKeyBlock;
    }

    /**
     * @param aggKeyBlock the aggKeyBlock to set
     */
    public void setAggKeyBlock(boolean[] aggKeyBlock) {
        this.aggKeyBlock = aggKeyBlock;
    }

    /**
     * @return the keyBlockUnCompressor
     */
    public NumberCompressor[] getKeyBlockUnCompressor() {
        return keyBlockUnCompressor;
    }

    /**
     * @param keyBlockUnCompressor the keyBlockUnCompressor to set
     */
    public void setKeyBlockUnCompressor(NumberCompressor[] keyBlockUnCompressor) {
        this.keyBlockUnCompressor = keyBlockUnCompressor;
    }

    public int[] getCardinality() {
        return cardinality;
    }

    public void setCardinality(int[] cardinality) {
        this.cardinality = cardinality;
    }

    public HybridStoreModel getHybridStoreModel()
    {
        return hybridStoreModel;
    }

    public void setHybridStoreModel(HybridStoreModel hybridStoreModel)
    {
        this.hybridStoreModel = hybridStoreModel;
    }
    
}
