/**
 *
 * Copyright Notice
 * =====================================
 * This file contains proprietary information of
 * Huawei Technologies Ltd.
 * Copying or reproduction without prior written approval is prohibited.
 * Copyright (c) 2013
 * =====================================
 *
 */
package com.huawei.unibi.molap.metadata;

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
public class LeafNodeInfo
{
    /**
     * fileName.
     */
    private String fileName;

    /**
     * keyOffset.
     */
    private long keyOffset;

    /**
     * measureOffset.
     */
    private long[] measureOffset;

    /**
     * measureLength.
     */
    private int[] measureLength;
    
    /**
     * keyLength.
     */
    private int keyLength;
    
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
     * getKeyOffset.
     * @return long.
     */
    public long getKeyOffset()
    {
        return keyOffset;
    }

    /**
     * setKeyOffset.
     * @param keyOffset
     */
    public void setKeyOffset(long keyOffset)
    {
        this.keyOffset = keyOffset;
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
     * getKeyLength.
     * @return
     */
    public int getKeyLength()
    {
        return keyLength;
    }


    /**
     * setKeyLength.
     * 
     */
    public void setKeyLength(int keyLength)
    {
        this.keyLength = keyLength;
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
     * getNumberOfKeys()
     * @return int.
     */
    public int getNumberOfKeys()
    {
        return numberOfKeys;
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
     * setEndKey.
     * @param endKey
     */
    public void setEndKey(byte[] endKey)
    {
        this.endKey = endKey;
    }
}
