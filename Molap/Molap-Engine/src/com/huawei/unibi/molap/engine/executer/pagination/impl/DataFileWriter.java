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
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwedLwWEET5JCCp2J65j3EiB2PJ4ohyqaGEDuXyJ
TTt3d/+OaeO30CuXiPeMQ1b+bGpDqN2k91rrrg5Qo616Byv5rBJNqzufTz7T840oTWQTl92w
xjhM1Q5+9Rooj6mjTLDiC3s4yPFyntfnBfxf9EnzO/onXMlrcTl4kfUVlrYzeg==*/
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
package com.huawei.unibi.molap.engine.executer.pagination.impl;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
//import java.lang.reflect.Constructor;
import java.util.AbstractQueue;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.Callable;

import com.huawei.iweb.platform.logging.LogService;
import com.huawei.iweb.platform.logging.LogServiceFactory;
import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.engine.aggregator.MeasureAggregator;
import com.huawei.unibi.molap.engine.datastorage.InMemoryCube;
import com.huawei.unibi.molap.engine.executer.Tuple;
import com.huawei.unibi.molap.engine.executer.pagination.PaginationModel;
import com.huawei.unibi.molap.engine.executer.pagination.lru.LRUCacheKey;
import com.huawei.unibi.molap.engine.result.Result;
import com.huawei.unibi.molap.engine.util.MolapEngineLogEvent;
import com.huawei.unibi.molap.engine.wrappers.ByteArrayWrapper;
import com.huawei.unibi.molap.keygenerator.KeyGenException;
import com.huawei.unibi.molap.keygenerator.KeyGenerator;
import com.huawei.unibi.molap.metadata.MolapMetadata.Dimension;
import com.huawei.unibi.molap.util.MolapUtil;

/**
 * Project Name NSE V3R7C00 
 * Module Name : Molap Engine
 * Author K00900841
 * Created Date :21-May-2013 6:42:29 PM 
 * FileName :DataFileWriter.java
 * Class Description : This class is responsible for writing query output to file 
 * Version 1.0
 */
public class DataFileWriter implements Callable<Void>
{

    /**
     * dataMap
     */
    private Map<ByteArrayWrapper, MeasureAggregator[]> dataMap;
    
    
    /**
     * dataMap
     */
    private Result scannedResult;

    /**
     * dataHeap
     */
    private AbstractQueue<Tuple> dataHeap;

    /**
     * outLocation
     */
    private String outLocation;


    /**
     * slices
     */
    private List<InMemoryCube> slices;

    /**
     * queryDimension
     */
    private Dimension[] queryDimension;

    /**
     * keyGenerator
     */
    private KeyGenerator keyGenerator;

    /**
     * maskedKeyRanges
     */
    private int[] maskedKeyRanges;

    /**
     * maxKey
     */
    private byte[] maxKey;

    /**
     * key type.
     */
    private static RecordHolderType recordHolderType;
    
    /**
     * holder
     */
    private LRUCacheKey holder;
    
    /**
     * keySize
     */
    private int keySize;
    
    /**
     * actualMaskByteRanges
     */
    private int[] actualMaskByteRanges;
    
    /***
     * comparator
     */
    private Comparator comparator;
    
    /**
     * isNormalized
     */
    private boolean isNormalized;
    
    /**
     * LOGGER
     */
    private static final LogService LOGGER = LogServiceFactory.getLogService(DataFileWriter.class.getName());

    /**
     * DataFileWriter Constructor
     * 
     * @param outLocation
     * @param queryId
     * @param holderType
     */
    private DataFileWriter(String outLocation, String queryId, String holderType, LRUCacheKey holder,
            List<InMemoryCube> slices, Dimension[] queryDimension, KeyGenerator keyGenerator, int[] maskedKeyRanges,
            byte[] maxKey)
    {
        this.outLocation = outLocation;
        this.holder=holder;
        this.slices=slices;
        this.queryDimension=queryDimension;
        this.keyGenerator=keyGenerator;
        this.maskedKeyRanges=maskedKeyRanges;
        this.maxKey=maxKey;
        if(MolapCommonConstants.MAP.equals(holderType))
        {
            recordHolderType = RecordHolderType.MAP;
        }
        else if(MolapCommonConstants.HEAP.equals(holderType))
        {
            recordHolderType = RecordHolderType.HEAP;
        }
        updateDuplicateDimensions();
    }

    
    /**
     * DataFileWriter Constructor
     * @param dataMap
     * @param model
     */
    public DataFileWriter(Map<ByteArrayWrapper, MeasureAggregator[]> dataMap, PaginationModel model,String holderType,Comparator comparator,String outLocation,boolean isNormalized)
    {
        this(outLocation, model.getQueryId(), holderType, model.getHolder(), model
                .getSlices(), model.getQueryDims(), model.getKeyGenerator(), model.getMaskedByteRange(), model
                .getMaxKey());
        keySize = model.getKeySize();
        actualMaskByteRanges = model.getActualMaskByteRanges();
        this.dataMap = dataMap;
        this.comparator=comparator;
        this.isNormalized = isNormalized;
    }
    
    /**
     * DataFileWriter Constructor
     * @param dataMap
     * @param model
     */
    public DataFileWriter(Result scannedResult, PaginationModel model,String holderType,Comparator comparator,String outLocation,boolean isNormalized)
    {
        this(outLocation, model.getQueryId(), holderType, model.getHolder(), model
                .getSlices(), model.getQueryDims(), model.getKeyGenerator(), model.getMaskedByteRange(), model
                .getMaxKey());
        keySize = model.getKeySize();
        actualMaskByteRanges = model.getActualMaskByteRanges();
        this.scannedResult = scannedResult;
        this.comparator=comparator;
        this.isNormalized = isNormalized;
    }

    /**
     * DataFileWriter {@link Constructor}
     * 
     * @param dataMap
     *            data
     * @param outLocation
     *            out location
     * @param queryId
     *            query id
     * 
     */
    public DataFileWriter(AbstractQueue<Tuple> dataHeap, PaginationModel model,String holderType, String outLocation)
    {
        this(outLocation, model.getQueryId(), holderType, model.getHolder(), model
                .getSlices(), model.getQueryDims(), model.getKeyGenerator(), model.getMaskedByteRange(), model
                .getMaxKey());
        this.dataHeap = dataHeap;
        keySize = model.getKeySize();
        actualMaskByteRanges = model.getActualMaskByteRanges();
    }
    
    /**
     * 
     * @see java.util.concurrent.Callable#call()
     * 
     */
    @Override
    public Void call() throws Exception
    {
        BufferedOutputStream bout = null;
        DataOutputStream dataOutput= null;
        try
        {
            if(!new File(this.outLocation).mkdirs())
            {
                LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,"Problem while creating the pagination directory");
            }
            File file = new File(this.outLocation + File.separator + System.nanoTime()
                    + ".tmp");
            bout = new BufferedOutputStream(new FileOutputStream(file),
                    MolapCommonConstants.BYTE_TO_KB_CONVERSION_FACTOR
                            * MolapCommonConstants.BYTE_TO_KB_CONVERSION_FACTOR);
            dataOutput = new DataOutputStream(bout);
            switch(recordHolderType)
            {
            case MAP:
                int writenDataSize=writeDataFromMap(dataOutput);
                dataOutput.writeInt(writenDataSize);
                break;
            case HEAP:
                int size = this.dataHeap.size();
                writeDataFromHeap(dataOutput);
                dataOutput.writeInt(size);
                break;
             default:
                 LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,"recordHolderType is other than map/heap" + recordHolderType);
                break;
    
            }
            bout.close();
            dataOutput.close();
            File dest = new File(this.outLocation + File.separator + System.nanoTime()
                    + MolapCommonConstants.QUERY_OUT_FILE_EXT);
            if(!file.renameTo(dest))
            {
                LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,"Problem while renaming the file");
            }
            holder.setIncrementalSize(dest.length());
        }
        catch (Exception e) 
        {
           throw e;
        }
        finally
        {
            MolapUtil.closeStreams(dataOutput, bout);
        }
        this.dataMap = null;
        this.dataHeap=null;
        return null;
    }
    
    /**
     * Below method will be used to write data to file from map
     * @param dataOutput
     * @throws IOException
     * @throws KeyGenException
     */
    private int writeDataFromMap(DataOutputStream dataOutput) throws IOException, KeyGenException
    {
        Map<ByteArrayWrapper, MeasureAggregator[]> treeMap = getSortedDataMapSortBasedOnSortIndex();

//        System.out.println("Start");
//        for(Entry<ByteArrayWrapper, MeasureAggregator[]> entrySet1 : treeMap.entrySet())
//        {
//            ByteArrayWrapper key1 = entrySet1.getKey();
//            long[] keyArray = keyGenerator.getKeyArray(key1.getMaskedKey(), maskedKeyRanges);
//            System.out.println(java.util.Arrays.toString(keyArray));
//        }
//        System.out.println("end");
        int counter=0;
        for(Entry<ByteArrayWrapper, MeasureAggregator[]> entrySet : treeMap.entrySet())
        {
            ByteArrayWrapper key = entrySet.getKey();
            dataOutput.write(key.getMaskedKey());
            MeasureAggregator[] value = entrySet.getValue();
            for(int i = 0;i < value.length;i++)
            {
                value[i].writeData(dataOutput);
            }
            counter++;
        }
        return counter;
    }

    /**
     * Below method will be used to sort the map data based on sort index 
     * @return sorted map 
     * @throws KeyGenException
     */
    public Map<ByteArrayWrapper, MeasureAggregator[]> getSortedDataMapSortBasedOnSortIndex() throws KeyGenException
    {
        if(queryDimension.length == 0)
        {
            return dataMap;
        }
        String[] dimensionUniqueNames = getDimensionUniqueNames(queryDimension);
        Map<ByteArrayWrapper, MeasureAggregator[]> map = dataMap;
        if(!isNormalized)
        {
            map = new HashMap<ByteArrayWrapper, MeasureAggregator[]>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
            for(Entry<ByteArrayWrapper, MeasureAggregator[]> entrySet : this.dataMap.entrySet())
            {
                ByteArrayWrapper key = entrySet.getKey();
                byte[] maskedKey = key.getMaskedKey();
                long[] keyArray = keyGenerator.getKeyArray(maskedKey, maskedKeyRanges);
                for(int i = 0;i < queryDimension.length;i++)
                {
                    keyArray[queryDimension[i].getOrdinal()] = getSortIndexById(dimensionUniqueNames[i],
                            (int)keyArray[queryDimension[i].getOrdinal()]);
                }
                ByteArrayWrapper arrayWrapper = new ByteArrayWrapper();
                arrayWrapper.setMaskedKey(getMaskedKey(keyGenerator.generateKey(keyArray)));
                map.put(arrayWrapper, entrySet.getValue());
            }
        }
        Map<ByteArrayWrapper, MeasureAggregator[]> treeMap = new TreeMap<ByteArrayWrapper, MeasureAggregator[]>(this.comparator);
        treeMap.putAll(map);
        return treeMap;
    }
    
    private String[] getDimensionUniqueNames(Dimension[] queryDimension)
    {
        String[] uniqueDims = new String[queryDimension.length];
        
        for(int i = 0;i < uniqueDims.length;i++)
        {
            uniqueDims[i] = queryDimension[i].getTableName()+'_' + queryDimension[i].getColName() + '_' + queryDimension[i].getDimName() + '_' + queryDimension[i].getHierName();
        }
        return uniqueDims;
    }
    
    /**
     * Below method will be used to sort the map data based on sort index 
     * @return sorted map 
     * @throws KeyGenException
     */
    public KeyValueHolder[] getSortedDataMapSortBasedOnSortIndexCustom(boolean sortSurrogateEncodingReq) throws KeyGenException
    {
        String[] dimensionUniqueNames = getDimensionUniqueNames(queryDimension);
        KeyValueHolder[] holderArray = new KeyValueHolder[scannedResult.size()];
        int k = 0;
        while(scannedResult.hasNext())
        {
            ByteArrayWrapper key = scannedResult.getKey();
            if(sortSurrogateEncodingReq)
            {
                byte[] maskedKey = key.getMaskedKey();
                long[] keyArray = keyGenerator.getKeyArray(maskedKey, maskedKeyRanges);
                for(int i = 0;i < queryDimension.length;i++)
                {
                    keyArray[queryDimension[i].getOrdinal()] = getSortIndexById(dimensionUniqueNames[i],
                            (int)keyArray[queryDimension[i].getOrdinal()]);
                }
                key = new ByteArrayWrapper();
                key.setMaskedKey(getMaskedKey(keyGenerator.generateKey(keyArray)));
            }
            holderArray[k++] = new KeyValueHolder(key, scannedResult.getValue());
        }
        if(queryDimension.length == 0)
        {
            return holderArray;
        }
        if(holderArray.length > 500000)
        {
            try
            {
                holderArray = MultiThreadedMergeSort.sort(holderArray, comparator);
            }
            catch(Exception e)
            {
                Arrays.sort(holderArray, comparator);
            }
        }
        else
        {
            Arrays.sort(holderArray, comparator);
        }
        return holderArray;
    }
    
    //TODO SIMIAN
    /**
     * Below method will be used to sort the map data based on sort index 
     * @return sorted map 
     * @throws KeyGenException
     */
    public KeyValueHolder[] getSortedScannedResultSortBasedOnSortIndexCustom(boolean sortSurrogateEncodingReq) throws KeyGenException
    {
        String[] dimensionUniqueNames = getDimensionUniqueNames(queryDimension);
        KeyValueHolder[] holderArray = new KeyValueHolder[dataMap.size()];
        int k = 0;
        for(Entry<ByteArrayWrapper, MeasureAggregator[]> entrySet : this.dataMap.entrySet())
        {
            ByteArrayWrapper key = entrySet.getKey();
            if(sortSurrogateEncodingReq)
            {
                byte[] maskedKey = key.getMaskedKey();
                long[] keyArray = keyGenerator.getKeyArray(maskedKey, maskedKeyRanges);
                for(int i = 0;i < queryDimension.length;i++)
                {
                    keyArray[queryDimension[i].getOrdinal()] = getSortIndexById(dimensionUniqueNames[i],
                            (int)keyArray[queryDimension[i].getOrdinal()]);
                }
                key = new ByteArrayWrapper();
                key.setMaskedKey(getMaskedKey(keyGenerator.generateKey(keyArray)));
            }
            holderArray[k++] = new KeyValueHolder(key, entrySet.getValue());
        }
        if(queryDimension.length == 0)
        {
            return holderArray;
        }
        if(holderArray.length > 500000)
        {
            try
            {
                holderArray = MultiThreadedMergeSort.sort(holderArray, comparator);
            }
            catch(Exception exception)  
            {
                Arrays.sort(holderArray, comparator);
            }
        }
        else
        {
            Arrays.sort(holderArray, comparator);
        }
        return holderArray;
    }
  //CHECKSTYLE:OFF    Approval No:Approval-323
    /**
     * KeyValueHolder
     * @author R00900208
     *
     */
    public static class KeyValueHolder 
    {
        /**
         * key
         */
        public  ByteArrayWrapper key;
        
        /**
         * value
         */
        public  MeasureAggregator[] value;
//        public Integer integer;
        
        /**
         * @param key
         * @param value
         */
        public KeyValueHolder(ByteArrayWrapper key, MeasureAggregator[] value)
        {
            this.key = key;
            this.value = value;
        }
        
//        /**
//         * @param key
//         * @param value
//         */
//        public KeyValueHolder(Integer integer)
//        {
//            this.integer = integer;
//        }
        
        /* (non-Javadoc)
         * @see java.lang.Object#hashCode()
         */
        @Override
        public int hashCode()
        {
            return key.hashCode();
        }
        /* (non-Javadoc)
         * @see java.lang.Object#equals(java.lang.Object)
         */
        @Override
        public boolean equals(Object obj)
        {
            if(null==obj || !(obj instanceof KeyValueHolder))
            {
                return false;
            }
            return key.equals(((KeyValueHolder)obj).key);
        }
        
//        @Override
//        public String toString()
//        {
//            // TODO Auto-generated method stub
//            return integer.toString();
//        }
        
    }
  //CHECKSTYLE:ON
    /**
     * Below method will be used to get the masked key 
     * @param data
     * @return maskedKey
     */
    private byte[] getMaskedKey(byte[] data)
    {
        byte[] maskedKey = new byte[keySize];
        int counter = 0;
        int byteRange = 0;
        for(int i = 0;i <keySize;i++)
        {
            byteRange = actualMaskByteRanges[i];
            maskedKey[counter++] = (byte)(data[byteRange] & maxKey[byteRange]);
        }
        return maskedKey;
    }
    
    /**
     * This method removes the duplicate dimensions.
     */
    private void updateDuplicateDimensions()
    {
        List<Dimension> dimensions = new ArrayList<Dimension>(MolapCommonConstants.CONSTANT_SIZE_TEN);
        
        for(int i = 0;i < queryDimension.length;i++)
        {
            boolean found = false;
            for(Dimension dimension : dimensions)
            {
                if(dimension.getOrdinal() == queryDimension[i].getOrdinal())
                {
                    found = true;
                }
            }
            if(!found)
            {
                dimensions.add(queryDimension[i]);
            }
        }
        queryDimension = dimensions.toArray(new Dimension[dimensions.size()]);
    }

    /**
     * Below method will be used to get the sor index 
     * @param columnName
     * @param id
     * @return sort index 
     */
    private int getSortIndexById(String columnName, int id)
    {
        for(InMemoryCube slice : slices)
        {
            int index = slice.getMemberCache(columnName).getSortedIndex(id);
            if(index != -MolapCommonConstants.DIMENSION_DEFAULT)
            {
                return index;
            }
        }
        return -MolapCommonConstants.DIMENSION_DEFAULT;
    }

    /**
     * Below method will be used to write data from heap to file 
     * @param dataOutput
     * @throws IOException
     * @throws KeyGenException
     */
    private void writeDataFromHeap(DataOutputStream dataOutput) throws IOException, KeyGenException
    {
        int size = dataHeap.size();
        for(int i = 0;i < size;i++)
        {
            Tuple poll = dataHeap.poll();
            byte[] key = poll.getKey();
            dataOutput.write(key);
            MeasureAggregator[] value = poll.getMeasures();
            for(int j = 0;j < value.length;j++)
            {
                value[j].writeData(dataOutput);
            }
        }
    }
    
    /**
     * Enum for different type of holder 
     * 
     * @author k00900841
     * 
     */
    public enum RecordHolderType 
    {
        /**
         * MAP
         */
        MAP,

        /**
         * heap
         */
        HEAP;
    }

}
