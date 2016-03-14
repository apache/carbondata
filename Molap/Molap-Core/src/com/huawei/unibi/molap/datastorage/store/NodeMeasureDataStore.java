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

import com.huawei.unibi.molap.datastorage.store.dataholder.MolapWriteDataHolder;

/**
 * 
 * Project Name NSE V3R7C00 
 * Module Name : Molap Commons 
 * Author K00900841
 * Created Date :21-May-2013 7:21:49 PM
 * FileName : NodeMeasureDataStore.java
 * Class Description : 
 * This class is responsible for storing and holding the node's keys
 * Version 1.0
 *
 * @param <T>
 */
public interface NodeMeasureDataStore //<T>
{
//    /**
//     * This method will be used to insert measure data to store 
//     * 
//     * @param index
//     *          index of key
//     * @param value
//     *          key
//     *
//     */
//    public void put(int index, T  value);

//    /**
//     * This method will be used to get the writable key array.
//     * 
//     * writable measure data array will hold below information:
//     * <size of measure data array><measure data array>
//     * total length will be 4 bytes for size + measure data array length
//     * @return writable array (compressed or normal)
//     *
//     */
//    public byte[][] getWritableMeasureDataArray();
    
    /**
     * This method will be used to get the writable key array.
     * 
     * writable measure data array will hold below information:
     * <size of measure data array><measure data array>
     * total length will be 4 bytes for size + measure data array length
     * @return writable array (compressed or normal)
     *
     */
     byte[][] getWritableMeasureDataArray(MolapWriteDataHolder[] dataHolderArray);

    /**
     * 
     * 
     * @param cols
     * @param fileHolder
     * @return
     *
     */
     MeasureDataWrapper getBackData(int[] cols,FileHolder fileHolder);
    
    /**
     * 
     * 
     * @param cols
     * @param fileHolder
     * @return
     *
     */
    MeasureDataWrapper getBackData(int cols,FileHolder fileHolder);

    /**
     * 
     * 
     * @return
     *
     */
     short getLength();
    

//    /**
//     * This method will clear the store and create the new empty store
//     * 
//     */
//    public void clear();

}