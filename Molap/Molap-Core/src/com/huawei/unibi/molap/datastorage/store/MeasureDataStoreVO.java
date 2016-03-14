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
package com.huawei.unibi.molap.datastorage.store;

import com.huawei.unibi.molap.datastorage.store.compression.ValueCompressionModel;

/**
 * 
 * Project Name NSE V3R7C00 
 * Module Name : Molap Data Processor 
 * Author K00900841
 * Created Date :23-May-2013 5:46:57 PM
 * FileName : KeyStoreVO.java
 * Class Description : This class will holding all the properties required for getting the data store instance
 * Version 1.0
 */
public class MeasureDataStoreVO
{
    /**
     * totalSize.
     */
    private int totalSize;

    /**
     * elementSize.
     */
    private int elementSize;

    /**
     * isFileStore.
     */
    private boolean isFileStore;

    /**
     * compressionModel.
     */
    private ValueCompressionModel compressionModel;

    /**
     * offset.
     */
    private long[] offset;

    /**
     * length.
     */
    private int[] length;

    /**
     * filePath.
     */
    private String filePath;

    /**
     * fileHolder.
     */
    private FileHolder fileHolder;
    
    /**
     * getTotalSize.
     * @return int
     */
    public int getTotalSize()
    {
        return totalSize;
    }

    /**
     * setTotalSize.
     * @param totalSize
     */
    public void setTotalSize(int totalSize)
    {
        this.totalSize = totalSize;
    }

    /**
     * getElementSize.
     * @return int
     */
    public int getElementSize()
    {
        return elementSize;
    }

    /**
     * setElementSize.
     * @param elementSize
     */
    public void setElementSize(int elementSize)
    {
        this.elementSize = elementSize;
    }

    /**
     * isFileStore.
     * @return boolean.
     */
    public boolean isFileStore()
    {
        return isFileStore;
    }

    /**
     * setFileStore.
     * @param isFileStore
     */
    public void setFileStore(boolean isFileStore)
    {
        this.isFileStore = isFileStore;
    }

    /**
     * getCompressionModel.
     * @return ValueCompressionModel.
     */
    public ValueCompressionModel getCompressionModel()
    {
        return compressionModel;
    }

    /**
     * setCompressionModel.
     * @param compressionModel
     */
    public void setCompressionModel(ValueCompressionModel compressionModel)
    {
        this.compressionModel = compressionModel;
    }

    /**
     * getOffset.
     * @return long[].
     */
    public long[] getOffset()
    {
        return offset;
    }

    /**
     * setOffset.
     * @param offset
     */
    public void setOffset(long[] offset)
    {
        this.offset = offset;
    }

    /**
     * getLength().
     * @return int[].
     */
    public int[] getLength()
    {
        return length;
    }

    /**
     * setLength.
     * @param length
     */
    public void setLength(int[] length)
    {
        this.length = length;
    }

    /**
     * getFilePath()
     * @return String.
     */
    public String getFilePath()
    {
        return filePath;
    }

    /**
     * setFilePath.
     * @param filePath
     */
    public void setFilePath(String filePath)
    {
        this.filePath = filePath;
    }

    /**
     * getFileHolder.
     * @return FileHolder.
     */
    public FileHolder getFileHolder()
    {
        return fileHolder;
    }

    /**
     * setFileHolder.
     * @param fileHolder
     */
    public void setFileHolder(FileHolder fileHolder)
    {
        this.fileHolder = fileHolder;
    }
}
