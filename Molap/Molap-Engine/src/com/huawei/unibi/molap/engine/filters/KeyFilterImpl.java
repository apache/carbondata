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

/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwcz8AOhvEHjQfa55oxvUSJWRQCwLl+VwWEHaV7n
0eFj3Q8d7m0cWhPqJpWXCXqFhQo4IRr9ZbIZHyWGqm0IAd8WGUWZrZwl2FirE9IvwhWSGY4F
FMNPWkL884Pq7vluWzB7TMULRUwIiTdETt2ErnK7Es6i/nZg/tlRWz6ZpBmrCg==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
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
package com.huawei.unibi.molap.engine.filters;

//import org.apache.log4j.Logger;
import com.huawei.iweb.platform.logging.LogService;
import com.huawei.iweb.platform.logging.LogServiceFactory;
import com.huawei.unibi.molap.engine.datastorage.storeInterfaces.KeyValue;
import com.huawei.unibi.molap.engine.filters.metadata.InMemFilterModel;
import com.huawei.unibi.molap.engine.util.MolapEngineLogEvent;
import com.huawei.unibi.molap.keygenerator.KeyGenerator;
import com.huawei.unibi.molap.util.ByteUtil.UnsafeComparer;

/**
 * Filter implementation to scan the data store based on include filter applied in the dimensions.
 * 
 * @author R00900208
 * 
 */
public class KeyFilterImpl implements InMemoryFilter
{

    /**
     * 
     */
    protected InMemFilterModel filterModel;

    /**
     * 
     */
    protected KeyGenerator keyGenerator;

//    /**
//     * 
//     */
//    protected ScanOptimizer optimizer;
    
    /**
     * 
     */
    private static final LogService LOGGER = LogServiceFactory.getLogService(KeyFilterImpl.class.getName());
    
    protected int[] dimensionOffset;
    
    protected  byte[][][] includeFilters = null;
    
    protected byte[][] maxKeyBytes;
    
    protected int[][] ranges;
    
    protected int[] lengths;
    
    protected byte[][] temps = null;
    

    /**
     * subclass needs to ensure filters,keyGenerator & optimizer is set by the
     * subclass constructor. Its protected as this constructor is only to
     * support subclass
     */
    protected KeyFilterImpl()
    {

    }

    public KeyFilterImpl(InMemFilterModel filterModel, KeyGenerator keyGenerator, long[] maxKey)
    {
        this.filterModel = filterModel;
        this.keyGenerator = keyGenerator;
        dimensionOffset = filterModel.getColIncludeDimOffset();
        includeFilters = filterModel.getFilter();
        maxKeyBytes = filterModel.getMaxKey();
        ranges = filterModel.getMaskedByteRanges();
        temps = new byte[dimensionOffset.length][];
        lengths = new int[dimensionOffset.length];
        createTempByteAndRangeLengths(temps,lengths,dimensionOffset,ranges);
//        optimizer = new ScanOptimizerImpl(maxKey, filterModel.getIncludePredicateKeys(), keyGenerator);
    }

    /**
     * createTempByteAndRangeLengths
     */
    protected void createTempByteAndRangeLengths(byte[][] temps,int[] lengths,int[] dimensionOffset,int[][] ranges)
    {
        if(lengths.length > 0)
        {
            for(int i = 0;i < dimensionOffset.length;i++)
            {
                lengths[i] = ranges[dimensionOffset[i]] == null?0:ranges[dimensionOffset[i]].length;
                temps[i] = new byte[lengths[i]];
            }
        }
    }
    /**
     * Filters the keys
     */
    @Override
    public boolean filterKey(KeyValue key)
    {

        byte[][] allowedValuesForDim;
        for(int i = 0;i < dimensionOffset.length;i++)
        {
            allowedValuesForDim = includeFilters[dimensionOffset[i]];

            int searchresult = binarySearch(allowedValuesForDim, key.backKeyArray, key.keyOffset,
                    maxKeyBytes[dimensionOffset[i]], ranges[dimensionOffset[i]],temps[i],lengths[i]);
            if(searchresult < 0)
            {
                return false;
            }
        }
        return true;
    }
    
    /**
     * Filter the keys
     * @param key
     * @return
     */
    public boolean filterKey(byte[] key)
    {

        byte[][] allowedValuesForDim;
        int []searchresult = new int[dimensionOffset.length];
        for(int i = 0;i < dimensionOffset.length;i++)
        {
            allowedValuesForDim = includeFilters[dimensionOffset[i]];

            searchresult[i] = binarySearch(allowedValuesForDim, key, 0,
                    maxKeyBytes[dimensionOffset[i]], ranges[dimensionOffset[i]],temps[i],lengths[i]);
        }
        if(dimensionOffset.length<1)
        {
            return true;
        }
        for(int i = 0;i < searchresult.length;i++)
        {
            if(searchresult[i] < 0)
            {
                return false;
            }
        }
        return true;
    }

    /**
     * @param array
     * @param key
     * @param offset
     * @param length
     * @param maxKey
     * @param byteSize
     * @return
     */
    protected int binarySearch(byte[][] array, byte[] key, int offset, byte[] maxKey,int[] range,byte[] temp,int length)
    {
        try
        {
            for(int j = 0;j < length;j++)
            {
                temp[j] = (byte)(key[offset+range[j]] & maxKey[j]);
            }
            //
            int low = 0;
            int high = array.length - 1;
            //
            while(low <= high)
            {
                int mid = (low + high) >>> 1;
                byte[] midVal = array[mid];
                int cmp = UnsafeComparer.INSTANCE.compareTo(midVal, temp);//ByteUtil.compare(midVal, temp);
                //
                if(cmp < 0)
                {
                    low = mid + 1;
                }
                else if(cmp > 0)
                {
                    high = mid - 1;
                }
                else
                {
                    return mid; // key found
                }
            }
            return -(low + 1);
        }
        catch(Exception e)
        {
            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, e, "Execption occured while searching");
        }
        return 1;
    }

    @Override
    public byte[] getNextJump(KeyValue key)
    {
        byte[] bKey = new byte[key.getKeyLength()];
        System.arraycopy(key.getArray(), key.getKeyOffset(), bKey, 0, bKey.length);
        return bKey;
//        return optimizer.getNextKey(keyGenerator.getKeyArray(bKey), bKey);
    }

}
