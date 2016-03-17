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

/**
 *
 * Copyright Notice
 * =====================================
 * This file contains proprietary information of
 * Huawei Technologies India Pvt Ltd.
 * Copying or reproduction without prior written approval is prohibited.
 * Copyright (c) 2013
 * =====================================
 *
 */
package com.huawei.unibi.molap.datastorage.util;

import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.datastorage.store.FileHolder;
import com.huawei.unibi.molap.datastorage.store.NodeKeyStore;
import com.huawei.unibi.molap.datastorage.store.NodeMeasureDataStore;
import com.huawei.unibi.molap.datastorage.store.columnar.ColumnarKeyStore;
import com.huawei.unibi.molap.datastorage.store.columnar.ColumnarKeyStoreInfo;
import com.huawei.unibi.molap.datastorage.store.compression.ValueCompressionModel;
import com.huawei.unibi.molap.datastorage.store.impl.data.compressed.HeavyCompressedDoubleArrayDataFileStore;
import com.huawei.unibi.molap.datastorage.store.impl.data.compressed.HeavyCompressedDoubleArrayDataInMemoryStore;
import com.huawei.unibi.molap.datastorage.store.impl.data.uncompressed.DoubleArrayDataFileStore;
import com.huawei.unibi.molap.datastorage.store.impl.data.uncompressed.DoubleArrayDataInMemoryStore;
import com.huawei.unibi.molap.datastorage.store.impl.key.columnar.compressed.CompressedColumnarFileKeyStore;
import com.huawei.unibi.molap.datastorage.store.impl.key.columnar.compressed.CompressedColumnarInMemoryStore;
import com.huawei.unibi.molap.datastorage.store.impl.key.columnar.uncompressed.UnCompressedColumnarFileKeyStore;
import com.huawei.unibi.molap.datastorage.store.impl.key.columnar.uncompressed.UnCompressedColumnarInMemoryStore;
import com.huawei.unibi.molap.datastorage.store.impl.key.compressed.CompressedSingleArrayKeyFileStore;
import com.huawei.unibi.molap.datastorage.store.impl.key.compressed.CompressedSingleArrayKeyInMemoryStore;
import com.huawei.unibi.molap.datastorage.store.impl.key.uncompressed.SingleArrayKeyFileStore;
import com.huawei.unibi.molap.datastorage.store.impl.key.uncompressed.SingleArrayKeyInMemoryStore;
import com.huawei.unibi.molap.util.MolapProperties;

/**
 * 
 * Project Name NSE V3R7C00 Module Name : Molap Data Processor Author K00900841
 * Created Date :23-May-2013 5:01:50 PM FileName : StoreFactory.java Class
 * Description : This class will be used to get the store based on store type;
 * Version 1.0
 */
public final class StoreFactory
{
    private StoreFactory()
    {
        
    }
    /**
     * Single Array Key store.
     */
    private static final String SINGLE_ARRAY = "SINGLE_ARRAY";

    /**
     * Compressed single array key store.
     */
    private static final String COMPRESSED_SINGLE_ARRAY = "COMPRESSED_SINGLE_ARRAY";

    /**
     * Double array data store.
     */
    private static final String COMPRESSED_DOUBLE_ARRAY = "COMPRESSED_DOUBLE_ARRAY";

    /**
     * Compressed double array data store.
     */
    private static final String HEAVY_VALUE_COMPRESSION = "HEAVY_VALUE_COMPRESSION";

    /**
     * key type.
     */
    private static StoreType keyType;

    /**
     * value type.
     */
    private static StoreType valueType;

    /**
     * enum defined.
     * @author S71955
     *
     */
    public enum StoreType 
    {
        /**
         * 
         */
        SINGLE_ARRAY, /**
         * 
         */
        COMPRESSED_SINGLE_ARRAY, /**
         * 
         */
        COMPRESSED_DOUBLE_ARRAY, /**
         * 
         */
        HEAVY_VALUE_COMPRESSION
    }

    static
    {
        String keytype = MolapProperties.getInstance().getProperty(
                MolapCommonConstants.KEYSTORE_TYPE,
                MolapCommonConstants.KEYSTORE_TYPE_DEFAULT_VAL);
        String valuetype = MolapProperties.getInstance().getProperty(
                MolapCommonConstants.VALUESTORE_TYPE,
                MolapCommonConstants.VALUESTORE_TYPE_DEFAULT_VAL);

        // set key type
        if(COMPRESSED_SINGLE_ARRAY.equals(keytype))
        {
            keyType = StoreType.COMPRESSED_SINGLE_ARRAY;
        }
        else if(SINGLE_ARRAY.equals(keytype))
        {
            keyType = StoreType.SINGLE_ARRAY;
        }
        else
        {
            keyType = StoreType.COMPRESSED_SINGLE_ARRAY;
        }
        // set value type
        if(COMPRESSED_DOUBLE_ARRAY.equals(valuetype))
        {
            valueType = StoreType.COMPRESSED_DOUBLE_ARRAY;
        }
        //findbugs fix -
        /*else if(HEAVY_VALUE_COMPRESSION.equals(valuetype))
        {
            valueType = StoreType.HEAVY_VALUE_COMPRESSION;
        }*/
        else
        {
            valueType = StoreType.HEAVY_VALUE_COMPRESSION;
        }
    }

    /**
     * 
     * createKeyStore.
     * @param size
     * @param elementSize
     * @param isLeaf
     * @param isFileStore
     * @param offset
     * @param fileName
     * @param length
     * @param fileHolder
     * @return
     *
     */
    public static NodeKeyStore createKeyStore(int size, int elementSize,
            boolean isLeaf, boolean isFileStore, long offset, String fileName,
            int length, FileHolder fileHolder)
    {
        switch(keyType)
        {
        //
            case SINGLE_ARRAY:
            
                if(isFileStore)
                {
                    return new SingleArrayKeyFileStore(size, elementSize,
                            offset, fileName, length);
                }
                else
                {
                    return new SingleArrayKeyInMemoryStore(size, elementSize,
                            offset, fileName, fileHolder, length);
                }
            //
            default:
            
                if(isLeaf)
                {
                    if(isFileStore)
                    {
                        return new CompressedSingleArrayKeyFileStore(size,
                                elementSize, offset, fileName, length);
                    }
                    else
                    {
                        return new CompressedSingleArrayKeyInMemoryStore(size,
                                elementSize, offset, fileName, fileHolder,
                                length);
                    }
                }
            //
                else
                {
                    if(isFileStore)
                    {
                        return new SingleArrayKeyFileStore(size, elementSize,
                                offset, fileName, length);
                    }
                    else
                    {
                        return new SingleArrayKeyInMemoryStore(size,
                                elementSize, offset, fileName, fileHolder,
                                length);
                    }
                }
            }
        }
    

    /**
     * createKeyStore.
     * @param size
     * @param elementSize
     * @param isLeaf
     * @return NodeKeyStore.
     */
    public static NodeKeyStore createKeyStore(int size, int elementSize,
            boolean isLeaf)
    {
        switch(keyType)
        {
            case SINGLE_ARRAY:
            
                return new SingleArrayKeyInMemoryStore(size, elementSize);
            
            default:
            
                if(isLeaf)
                {
                    return new CompressedSingleArrayKeyInMemoryStore(size,
                            elementSize);
                }
                else
                {
                    return new SingleArrayKeyInMemoryStore(size, elementSize);
                }
            
        }
    }
    
    public static ColumnarKeyStore createColumnarKeyStore(ColumnarKeyStoreInfo columnarKeyStoreInfo, FileHolder fileHolder, boolean isFileStore)
    {
        switch(keyType)
        {
        case SINGLE_ARRAY:

            if(isFileStore)
            {
                return new UnCompressedColumnarFileKeyStore(
                        columnarKeyStoreInfo);
            }
            else
            {
                return new UnCompressedColumnarInMemoryStore(
                        columnarKeyStoreInfo, fileHolder);
            }
        default:

            if(isFileStore)
            {
                return new CompressedColumnarFileKeyStore(columnarKeyStoreInfo);
            }
            else
            {
                return new CompressedColumnarInMemoryStore(
                        columnarKeyStoreInfo, fileHolder);
            }
        }
    }
    /**
     * NodeMeasureDataStore.
     * @param size
     * @param elementSize
     * @param isFileStore
     * @param compressionModel
     * @param offset
     * @param length
     * @param filePath
     * @param fileHolder
     * @return NodeMeasureDataStore.
     */
    public static NodeMeasureDataStore createDataStore(boolean isFileStore,
            ValueCompressionModel compressionModel, long[] offset,
            int[] length, String filePath, FileHolder fileHolder)
    {
        switch(valueType)
        {

        //
            case COMPRESSED_DOUBLE_ARRAY:
            
                if(isFileStore)
                {
                    return new DoubleArrayDataFileStore(compressionModel, offset, filePath, length);
                }
                else
                {
                    return new DoubleArrayDataInMemoryStore(compressionModel, offset, length, filePath,
                            fileHolder);
                }
            
            //
            case HEAVY_VALUE_COMPRESSION:
            
                if(isFileStore)
                {
                    return new HeavyCompressedDoubleArrayDataFileStore(compressionModel, offset, length,
                            filePath);
                }
                else
                {
                    return new HeavyCompressedDoubleArrayDataInMemoryStore(compressionModel, offset,
                            length, filePath, fileHolder);
                }
            //
            default:
            
                if(isFileStore)
                {
                    return new HeavyCompressedDoubleArrayDataFileStore(compressionModel, offset, length,
                            filePath);
                }
                else
                {
                    return new HeavyCompressedDoubleArrayDataInMemoryStore(compressionModel, offset,
                            length, filePath, fileHolder);
                }
            
        }
    }

    /**
     * createDataStore.
     * @param size
     * @param elementSize
     * @param compressionModel
     * @return NodeMeasureDataStore.
     */
    public static NodeMeasureDataStore createDataStore(ValueCompressionModel compressionModel)
    {
        switch(valueType)
        {
            case COMPRESSED_DOUBLE_ARRAY:
                return new DoubleArrayDataInMemoryStore(compressionModel);

            case HEAVY_VALUE_COMPRESSION:
                return new HeavyCompressedDoubleArrayDataInMemoryStore(compressionModel);
            default:
                return new HeavyCompressedDoubleArrayDataInMemoryStore(compressionModel);
        }
    }
    
}
