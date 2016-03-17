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

package com.huawei.unibi.molap.metadata;

import com.huawei.unibi.molap.keygenerator.mdkey.NumberCompressor;

/**
 * 
 * Project Name NSE V3R7C00 
 * Module Name : Molap Data Processor 
 * Author K00900841
 * Created Date :23-May-2013 5:46:57 PM
 * FileName : LeafNodeInfo.java
 * Class Description : This class will hold the MDKey details as leaf node.
 * Version 1.0
 */
public class LeafNodeInfoColumnar
{
    /**
     * fileName.
     */
    private String fileName;

    /**
     * measureOffset.
     */
    private long[] measureOffset;

    /**
     * measureLength.
     */
    private int[] measureLength;
    
    /**
     * numberOfKeys.
     */
    private int numberOfKeys;
    
    /**
     * startKey.
     */
    private byte[] startKey;
    
    /**
     * endKey.
     */
    private byte[] endKey;
    
    /**
     * keyOffSets
     */
    private long[] keyOffSets;
    
    /**
     * keyLengths
     */
    private int[] keyLengths;
    
    /**
     * isSortedKeyColumn
     */
    private boolean[] isSortedKeyColumn;
    
    /**
     * keyBlockIndexOffSets
     */
    private long[] keyBlockIndexOffSets;
    
    /**
     * keyBlockIndexLength
     */
    private int[] keyBlockIndexLength;
    
    /**
     * dataIndexMap
     */
    private int[] dataIndexMapLength;
    
    
    /**
     * dataIndexMap
     */
    private long[] dataIndexMapOffsets;
    
    private boolean[] aggKeyBlock;
    /**
     * leafNodeMetaSize
     */
    private int leafNodeMetaSize;
    
    private NumberCompressor[] keyBlockUnCompressor;

    /**
     * column min max array
     */
    private byte[][] columnMinMaxData;
    
    /**
     * getFileName().
     * @return String.
     */
    public String getFileName()
    {
        return fileName;
    }

    /**
     * setFileName.
     * @param fileName.
     */
    public void setFileName(String fileName)
    {
        this.fileName = fileName;
    }

    /**
     * setMeasureOffset.
     * @param measureOffset
     */
    public void setMeasureOffset(long[] measureOffset)
    {
        this.measureOffset = measureOffset;
    }

    /**
     * getMeasureLength 
     * @return int[].
     */
    public int[] getMeasureLength()
    {
        return measureLength;
    }

    /**
     * setMeasureLength.
     * @param measureLength
     */
    public void setMeasureLength(int[] measureLength)
    {
        this.measureLength = measureLength;
    }

    /**
     * getMeasureOffset.
     * @return long[].
     */
    public long[] getMeasureOffset()
    {
        return measureOffset;
    }

    /**
     * setNumberOfKeys.
     * @param numberOfKeys
     */
    public void setNumberOfKeys(int numberOfKeys)
    {
        this.numberOfKeys = numberOfKeys;
    }

    /**
     * getStartKey().
     * @return byte[].
     */
    public byte[] getStartKey()
    {
        return startKey;
    }

    /**
     * setStartKey.
     * @param startKey
     */
    public void setStartKey(byte[] startKey)
    {
        this.startKey = startKey;
    }

    /**
     * getEndKey().
     * @return byte[].
     */
    public byte[] getEndKey()
    {
        return endKey;
    }

    /**
     * @return the keyOffSets
     */
    public long[] getKeyOffSets()
    {
        return keyOffSets;
    }

    /**
     * @param keyOffSets the keyOffSets to set
     */
    public void setKeyOffSets(long[] keyOffSets)
    {
        this.keyOffSets = keyOffSets;
    }

    /**
     * @return the keyLengths
     */
    public int[] getKeyLengths()
    {
        return keyLengths;
    }
    
    //TODO SIMIAN
    /**
     * getNumberOfKeys()
     * @return int.
     */
    public int getNumberOfKeys()
    {
        return numberOfKeys;
    }

    /**
     * @param keyLengths the keyLengths to set
     */
    public void setKeyLengths(int[] keyLengths)
    {
        this.keyLengths = keyLengths;
    }

    /**
     * @return the isSortedKeyColumn
     */
    public boolean[] getIsSortedKeyColumn()
    {
        return isSortedKeyColumn;
    }

    /**
     * @param isSortedKeyColumn the isSortedKeyColumn to set
     */
    public void setIsSortedKeyColumn(boolean[] isSortedKeyColumn)
    {
        this.isSortedKeyColumn = isSortedKeyColumn;
    }

    /**
     * @return the keyBlockIndexOffSets
     */
    public long[] getKeyBlockIndexOffSets()
    {
        return keyBlockIndexOffSets;
    }

    /**
     * @param keyBlockIndexOffSets the keyBlockIndexOffSets to set
     */
    public void setKeyBlockIndexOffSets(long[] keyBlockIndexOffSets)
    {
        this.keyBlockIndexOffSets = keyBlockIndexOffSets;
    }

    /**
     * @return the keyBlockIndexLength
     */
    public int[] getKeyBlockIndexLength()
    {
        return keyBlockIndexLength;
    }
    
    
    //TODO SIMIAN
    /**
     * setEndKey.
     * @param endKey
     */
    public void setEndKey(byte[] endKey)
    {
        this.endKey = endKey;
    }

    /**
     * @param keyBlockIndexLength the keyBlockIndexLength to set
     */
    public void setKeyBlockIndexLength(int[] keyBlockIndexLength)
    {
        this.keyBlockIndexLength = keyBlockIndexLength;
    }

    /**
     * @return the leafNodeMetaSize
     */
    public int getLeafNodeMetaSize()
    {
        return leafNodeMetaSize;
    }

    /**
     * @param leafNodeMetaSize the leafNodeMetaSize to set
     */
    public void setLeafNodeMetaSize(int leafNodeMetaSize)
    {
        this.leafNodeMetaSize = leafNodeMetaSize;
    }

    /**
     * @return the dataIndexMapLenght
     */
    public int[] getDataIndexMapLength()
    {
        return dataIndexMapLength;
    }

    /**
     * @return the dataIndexMapOffsets
     */
    public long[] getDataIndexMapOffsets()
    {
        return dataIndexMapOffsets;
    }

    /**
     * @param dataIndexMapLenght the dataIndexMapLenght to set
     */
    public void setDataIndexMapLength(int[] dataIndexMapLength)
    {
        this.dataIndexMapLength = dataIndexMapLength;
    }

    /**
     * @param dataIndexMapOffsets the dataIndexMapOffsets to set
     */
    public void setDataIndexMapOffsets(long[] dataIndexMapOffsets)
    {
        this.dataIndexMapOffsets = dataIndexMapOffsets;
    }

    /**
     * @return the aggKeyBlock
     */
    public boolean[] getAggKeyBlock()
    {
        return aggKeyBlock;
    }

    /**
     * @param aggKeyBlock the aggKeyBlock to set
     */
    public void setAggKeyBlock(boolean[] aggKeyBlock)
    {
        this.aggKeyBlock = aggKeyBlock;
    }

    /**
     * @return the keyBlockUnCompressor
     */
    public NumberCompressor[] getKeyBlockUnCompressor()
    {
        return keyBlockUnCompressor;
    }

    /**
     * @param keyBlockUnCompressor the keyBlockUnCompressor to set
     */
    public void setKeyBlockUnCompressor(NumberCompressor[] keyBlockUnCompressor)
    {
        this.keyBlockUnCompressor = keyBlockUnCompressor;
    }

    /**
     * for each column min max data
     * @return
     */
    public void setColumnMinMaxData(byte[][] columnMinMaxData)
    {
       this.columnMinMaxData=columnMinMaxData;
        
    }

    /**
     * for each column min max data
     * @return
     */
    public byte[][] getColumnMinMaxData()
    {
        return this.columnMinMaxData;
    }
    
}
