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

/**
 * 
 * Project Name NSE V3R7C00 
 * Module Name : Molap Commons 
 * Author K00900841
 * Created Date :21-May-2013 7:21:49 PM
 * FileName : NodeKeyStore.java
 * Class Description : 
 * This class is responsible for storing and holding the mdkey of nodes  
 * Version 1.0
 */

public interface NodeKeyStore
{
    /**
     * This method will be used to get the actual mdkeys array present in the
     * store store
     * 
     * @param fileHolder
     * @return mdkey
     * 
     * 
     */
     byte[] getBackArray(FileHolder fileHolder);

    /**
     * This method will be used to insert mdkey to store 
     * 
     * @param index
     *          index of mdkey
     * @param value
     *          mdkey
     *
     */
     void put(int index, byte[] value);

    /**
     * This method will be used to get the writable key array.
     * 
     * writable key array will hold below information:
     * <size of key array><key array>
     * total length will be 4 bytes for size + key array length
     * @return writable array (compressed or normal)
     *
     */
     byte[] getWritableKeyArray();
    
    /**
     * This method will be used to get the mdkkey array based on index
     * 
     * @param index
     *          index in store
     * @param fileHolder
     *          file holder will be used to read the file
     * @return mdkey
     * 
     */
     byte[] get(int index, FileHolder fileHolder);
    
    /**
     * This method will clear the store and create the new empty store
     * 
     */
     void clear();

}
