package com.huawei.unibi.molap.store.writer;

import com.huawei.unibi.molap.datastorage.store.columnar.BlockIndexerStorage;

public class NodeHolderColumnar 
{
    /**
     * keyArray
     */
    private BlockIndexerStorage[] keyStorageArray;
    
    /**
     * dataArray
     */
    private byte[] dataArray;
    
    /**
     * measureLenght
     */
    private int[] measureLenght;
    
    /**
     * startKey
     */
    private byte[] startKey;
    
    /**
     * endKey
     */
    private byte[] endKey;

    /**
     * entryCount
     */
    private int entryCount;
    /**
     * @return the keyArray
     */
    public BlockIndexerStorage[] getKeyArray()
    {
        return keyStorageArray;
    }

    /**
     * @param keyArray the keyArray to set
     */
    public void setKeyArray(BlockIndexerStorage[] keyStorageArray)
    {
        this.keyStorageArray = keyStorageArray;
    }

    /**
     * @return the startKey
     */
    public byte[] getStartKey()
    {
        return startKey;
    }

    /**
     * @param startKey the startKey to set
     */
    public void setStartKey(byte[] startKey)
    {
        this.startKey = startKey;
    }
    
    
    /**
     * @return the dataArray
     */
    public byte[] getDataArray()
    {
        return dataArray;
    }

    /**
     * @param dataArray the dataArray to set
     */
    public void setDataArray(byte[] dataArray)
    {
        this.dataArray = dataArray;
    }

    /**
     * @return the endKey
     */
    public byte[] getEndKey()
    {
        return endKey;
    }

    /**
     * @param endKey the endKey to set
     */
    public void setEndKey(byte[] endKey)
    {
        this.endKey = endKey;
    }

    /**
     * @return the entryCount
     */
    public int getEntryCount()
    {
        return entryCount;
    }

    /**
     * @param entryCount the entryCount to set
     */
    public void setEntryCount(int entryCount)
    {
        this.entryCount = entryCount;
    }
    
    /**
     * @return the measureLenght
     */
    public int[] getMeasureLenght()
    {
        return measureLenght;
    }

    /**
     * @param measureLenght the measureLenght to set
     */
    public void setMeasureLenght(int[] measureLenght)
    {
        this.measureLenght = measureLenght;
    }
}
