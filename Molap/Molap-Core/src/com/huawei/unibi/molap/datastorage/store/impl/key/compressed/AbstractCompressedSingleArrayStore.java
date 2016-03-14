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

package com.huawei.unibi.molap.datastorage.store.impl.key.compressed;

import com.huawei.unibi.molap.datastorage.store.FileHolder;
import com.huawei.unibi.molap.datastorage.store.NodeKeyStore;
import com.huawei.unibi.molap.datastorage.store.compression.Compressor;
import com.huawei.unibi.molap.datastorage.store.compression.SnappyCompression;

/**
 * 
 * Project Name NSE V3R7C00 
 * Module Name : Molap Commons 
 * Author K00900841
 * Created Date :21-May-2013 7:21:49 PM
 * FileName : AbstractCompressedSingleArrayStore.java
 * Class Description : 
 * 
 * Version 1.0
 */

public abstract class AbstractCompressedSingleArrayStore implements NodeKeyStore
{
    
    /**
     * data store which will hold the data
     */
    protected byte[] datastore;

    /**
     * compressor will be used to compress the data 
     */
    protected static final Compressor<byte[]> COMPRESSOR = SnappyCompression.SnappyByteCompression.INSTANCE;
    
    /**
     * size of each element 
     */
    protected final int sizeOfEachElement;
    
    /**
     * total number of elements;
     */
    protected int totalNumberOfElements;

    /**
     * 
     * 
     * @param size
     * @param elementSize
     *
     */
    public AbstractCompressedSingleArrayStore(int size, int elementSize)
    {
        this(size, elementSize, true);
    }
    
    /**
     * 
     * 
     * @param size
     * @param elementSize
     *
     */
    public AbstractCompressedSingleArrayStore(int size, int elementSize,boolean createDataStore)
    {
        this.sizeOfEachElement = elementSize;
        this.totalNumberOfElements = size;
        if(createDataStore)
        {
            datastore = new byte[this.totalNumberOfElements * this.sizeOfEachElement];
        }
    }

    /**
     * This method will be used to insert key to store 
     * 
     * @param index
     *          index of key
     * @param value
     *          key
     *
     */
    @Override
    public void put(int index, byte[] value)
    {
        System.arraycopy(value, 0, datastore, ((index) * sizeOfEachElement), sizeOfEachElement);
    }

    /**
     * This method will be used to get the writable key array.
     * 
     * writable key array will hold below information:
     * <size of key array><key array>
     * total length will be stored in 4 bytes+ key array length for key array
     * @return writable array (compressed or normal)
     *
     */
    @Override
    public byte[] getWritableKeyArray()
    {
        // compress the data store
         byte[] compressedKeys = COMPRESSOR.compress(datastore);
         
//         // create allocate size for byte buffer
//         //  4 bytes for size of array(for array length) + size of array(for array)
//         ByteBuffer byteBuffer = ByteBuffer.allocate(compressedKeys.length+MolapCommonConstants.INT_SIZE_IN_BYTE);
//         // add array length
//         byteBuffer.putInt(compressedKeys.length);
//         // add key array
//         byteBuffer.put(compressedKeys);
         // return the array
         return compressedKeys;
    }

    /**
     * This method will be used to get the actual key array present in the
     * store . 
     * Here back array will be uncompress array
     * 
     * @param fileHolder
     *          file holder will be used to read the file
     * @return uncompressed keys
     *          will return uncompressed key
     * 
     * 
     */
    @Override
    public byte[] getBackArray(FileHolder fileHolder)
    {
        return COMPRESSOR.unCompress(datastore);
    }
    
    /**
     * This method will be used to get the key array based on index
     * 
     * @param index
     *          index in store
     * @param fileHolder
     *          file holder will be used to read the file
     * @return key 
     * 
     */
    @Override
    public byte[] get(int index,FileHolder fileHolder)
    {
        // uncompress the store data
        byte[] unCompress = COMPRESSOR.unCompress(datastore);
        // create new array of size of each element
        byte[] copy = new byte[sizeOfEachElement];
        // copy array for given index
        // copy will done based on below calculation
        // eg: index is 4 and size of each key is 6 then copy from 6*4= 24th
        // index till 29th index 
        System.arraycopy(unCompress, ((index) * sizeOfEachElement), copy, 0,
                sizeOfEachElement);
        return copy;
    }
    
    /**
     * This method will clear the store and create the new empty store
     * 
     */
    @Override
    public void clear()
    {
        datastore = new byte[this.totalNumberOfElements * this.sizeOfEachElement];
    }
}
