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

package com.huawei.unibi.molap.datastorage.store.impl.key.uncompressed;

import com.huawei.unibi.molap.datastorage.store.FileHolder;

/**
 * 
 * Project Name NSE V3R7C00 
 * Module Name : Molap Commons 
 * Author K00900841
 * Created Date :21-May-2013 7:21:49 PM
 * FileName : SingleArrayKeyInMemoryStore.java
 * Class Description : 
 * 
 * Version 1.0
 */
public class SingleArrayKeyInMemoryStore extends AbstractSingleArrayKeyStore
{

    /**
     * 
     * 
     * @param size
     * @param elementSize
     *
     */
    public SingleArrayKeyInMemoryStore(int size, int elementSize)
    {
        super(size, elementSize);
    }

    /**
     * 
     * 
     * @param size
     * @param elementSize
     * @param offset
     * @param filePath
     * @param fileHolder
     * @param length
     *
     */
    public SingleArrayKeyInMemoryStore(int size, int elementSize, long offset, String filePath, FileHolder fileHolder, int length)
    {
        this(size, elementSize);
        datastore=fileHolder.readByteArray(filePath, offset, length);
    }
    
}
