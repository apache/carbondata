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

package com.huawei.unibi.molap.engine.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import org.apache.commons.collections.comparators.ComparatorChain;

import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.datastorage.store.filesystem.MolapFile;
import com.huawei.unibi.molap.datastorage.store.filesystem.MolapFileFilter;
import com.huawei.unibi.molap.datastorage.store.impl.FileFactory;
import com.huawei.unibi.molap.engine.datastorage.InMemoryCube;
import com.huawei.unibi.molap.engine.datastorage.MemberStore;
import com.huawei.unibi.molap.engine.executer.exception.QueryExecutionException;
import com.huawei.unibi.molap.engine.executer.impl.comparator.MaksedByteComparatorBAW;
import com.huawei.unibi.molap.engine.executer.pagination.impl.MultiThreadedMergeSort;
import com.huawei.unibi.molap.engine.executer.pagination.impl.DataFileWriter.KeyValueHolder;
import com.huawei.unibi.molap.engine.reader.MaksedByteComparatorForReader;
import com.huawei.unibi.molap.engine.reader.ResultTempFileReader;
import com.huawei.unibi.molap.engine.result.Result;
import com.huawei.unibi.molap.engine.schema.metadata.DataProcessorInfo;
import com.huawei.unibi.molap.engine.schema.metadata.SliceExecutionInfo;
import com.huawei.unibi.molap.engine.wrappers.ByteArrayWrapper;
import com.huawei.unibi.molap.keygenerator.KeyGenException;
import com.huawei.unibi.molap.keygenerator.KeyGenerator;
import com.huawei.unibi.molap.metadata.MolapMetadata.Dimension;
import com.huawei.unibi.molap.util.MolapProperties;

/**
 * 
 * Project Name : Carbon Module Name : MOLAP Data Processor Author :
 * R00903928,k00900841 Created Date : 27-Aug-2015 FileName :
 * ScannedResultProcessorUtil.java Description : Utility class for the data
 * processing. Class Version : 1.0
 */
public final class ScannedResultProcessorUtil
{
    private ScannedResultProcessorUtil()
    {

    }

    /**
     * 
     * @param maskedByteRangeForSorting
     * @param dimensionSortOrder
     * @param dimensionMasks
     * @return
     */
    public static ComparatorChain getMergerChainComparator(int[][] maskedByteRangeForSorting,
            byte[] dimensionSortOrder, byte[][] dimensionMasks)
    {
        if(dimensionSortOrder.length < 1)
        {
            return null;
        }
        List<Comparator<KeyValueHolder>> compratorList = new ArrayList<Comparator<KeyValueHolder>>(
                MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
        MaksedByteComparatorBAW keyComparator = null;
        int length = maskedByteRangeForSorting.length;
        for(int i = 0;i < length;i++)
        {
            if(null==maskedByteRangeForSorting[i])
            {
                keyComparator=new MaksedByteComparatorBAW(dimensionSortOrder[i]);
                compratorList.add(keyComparator);
                continue;
            }
            keyComparator = new MaksedByteComparatorBAW(maskedByteRangeForSorting[i], dimensionSortOrder[i],
                    dimensionMasks[i]);

            compratorList.add(keyComparator);
        }
        return new ComparatorChain(compratorList);
    }

    /**
     * For getting the unique name of the dimensions.
     * 
     * @param queryDimension
     * @return
     */
    public static String[] getDimensionUniqueNames(Dimension[] queryDimension)
    {
        String[] uniqueDims = new String[queryDimension.length];

        for(int i = 0;i < uniqueDims.length;i++)
        {
            uniqueDims[i] = queryDimension[i].getTableName() + '_' + queryDimension[i].getColName() + '_'
                    + queryDimension[i].getDimName() + '_' + queryDimension[i].getHierName();
        }
        return uniqueDims;
    }

    /**
     * Below method will be used to sort the map data based on sort index
     * 
     * @return sorted map
     * @throws QueryExecutionException
     * @throws KeyGenException
     */
    public static KeyValueHolder[] getSortedResult(DataProcessorInfo dataProcessorInfo, Result scannedResult,
            Comparator comparator) throws QueryExecutionException
    {
        Dimension[] queryDimension = dataProcessorInfo.getQueryDims();
        KeyGenerator keyGenerator = dataProcessorInfo.getKeyGenerator();
        String[] dimensionUniqueNames = getDimensionUniqueNames(queryDimension);
        KeyValueHolder[] holderArray = new KeyValueHolder[scannedResult.size()];
        int k = 0;
        byte[] sortedDimensionIndex = dataProcessorInfo.getSortedDimensionIndex();
        try
        {
            while(scannedResult.hasNext())
            {
                ByteArrayWrapper key = scannedResult.getKey();
                if(null != comparator)
                {
                    byte[] maskedKey = key.getMaskedKey();
                    long[] keyArray = keyGenerator.getKeyArray(maskedKey, dataProcessorInfo.getMaskedByteRange());
                    for(int i = 0;i < queryDimension.length;i++)
                    { // CHECKSTYLE:OFF Approval No:Approval-V1R2C10_006
                     
                        if(queryDimension[i].isHighCardinalityDim())
                        {
                            continue;
                        }
                        if(sortedDimensionIndex[i] == 1)
                        {
                            keyArray[queryDimension[i].getOrdinal()] = getSortIndexById(dimensionUniqueNames[i],
                                    (int)keyArray[queryDimension[i].getOrdinal()], dataProcessorInfo.getSlices());
                        }
                    }// CHECKSTYLE:ON
                    List<byte[]> listOfDirectSurrogates=key.getDirectSurrogateKeyList();
                    key = new ByteArrayWrapper();
                    key.addToDirectSurrogateKeyList(listOfDirectSurrogates);
                    key.setMaskedKey(getMaskedKey(keyGenerator.generateKey(keyArray), dataProcessorInfo));
                }
                
                
                holderArray[k++] = new KeyValueHolder(key, scannedResult.getValue());
            }
        }
        catch(KeyGenException e)
        {
            throw new QueryExecutionException(e);
        }
        if(dataProcessorInfo.getDimensionSortOrder().length == 0)
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
            if(null != comparator)
            {
                Arrays.sort(holderArray, comparator);
            }
        }
        return holderArray;
    }

    /**
     * Below method will be used to get the masked key
     * 
     * @param data
     * @return maskedKey
     */
    private static byte[] getMaskedKey(byte[] data, DataProcessorInfo dataProcessorInfo)
    {
        int keySize = dataProcessorInfo.getKeySize();
        int[] actualMaskByteRanges = dataProcessorInfo.getActualMaskByteRanges();
        byte[] maxKey = dataProcessorInfo.getMaxKey();
        byte[] maskedKey = new byte[keySize];
        int counter = 0;
        int byteRange = 0;
        for(int i = 0;i < keySize;i++)
        {
            byteRange = actualMaskByteRanges[i];
            maskedKey[counter++] = (byte)(data[byteRange] & maxKey[byteRange]);
        }
        return maskedKey;
    }

    /**
     * Below method will be used to get the sort index
     * 
     * @param columnName
     * @param id
     * @return sort index
     */
    private static int getSortIndexById(String columnName, int id, List<InMemoryCube> slices)
    {
        MemberStore memberCache = null;
        for(InMemoryCube slice : slices)
        {
            memberCache = slice.getMemberCache(columnName);
            if(null != memberCache)
            {
                if(null != memberCache.getAllMembers() && memberCache.getAllMembers().length > 0)
                {
                    int index = memberCache.getSortedIndex(id);
                    if(index != -MolapCommonConstants.DIMENSION_DEFAULT)
                    {
                        return index;
                    }
                }
            }
        }
        return -MolapCommonConstants.DIMENSION_DEFAULT;
    }

    /**
     * For initializing the DataProcessorInfo object.
     * 
     * @param info
     * @param heapcomparator
     * @param sortComparator
     * @return
     */
    public static DataProcessorInfo getDataProcessorInfo(SliceExecutionInfo info,
            Comparator<ResultTempFileReader> heapcomparator, Comparator sortComparator)
    {
        DataProcessorInfo dataProcessorInfo = new DataProcessorInfo();
        dataProcessorInfo.setKeySize(info.getActualMaskedKeyByteSize());
        dataProcessorInfo.setKeyGenerator(info.getActualKeyGenerator());
        dataProcessorInfo.setActualMaskByteRanges(info.getActalMaskedByteRanges());
        dataProcessorInfo.setMaxKey(info.getActualMaxKeyBasedOnDimensions());
        dataProcessorInfo.setQueryDims(info.getQueryDimensions());
        dataProcessorInfo.setSlices(info.getSlices());
        dataProcessorInfo.setMaskedByteRange(info.getMaskedBytePositions());
        dataProcessorInfo.setSortComparator(sortComparator);
        dataProcessorInfo.setCubeUniqueName(info.getSlice().getCubeUniqueName());
        dataProcessorInfo.setHeapComparator(heapcomparator);
        dataProcessorInfo.setSortedData(info.getDimensionSortOrder().length > 0);
        dataProcessorInfo.setFileBufferSize(120000);
        dataProcessorInfo.setBlockSize(120000);
        dataProcessorInfo.setLimit(info.getLimit());
        dataProcessorInfo.setMaskedByteRangeForSorting(info.getMaskedByteRangeForSorting());
        dataProcessorInfo.setHolderSize(Integer.parseInt(MolapProperties.getInstance().getProperty(
                MolapCommonConstants.PAGINATED_INTERNAL_FILE_ROW_LIMIT,
                MolapCommonConstants.PAGINATED_INTERNAL_FILE_ROW_LIMIT_DEFAULT)));

        dataProcessorInfo.setDimensionSortOrder(info.getDimensionSortOrder());
        dataProcessorInfo.setDimensionMasks(info.getDimensionMaskKeys());
        dataProcessorInfo.setQueryId(info.getQueryId());
        dataProcessorInfo.setAggType(info.getAggType());
        dataProcessorInfo.setHighCardinalityTypes(info.getHighCardinalityTypes());
        dataProcessorInfo.setMsrMinValue(info.getMsrMinValue());
        dataProcessorInfo.setSortedDimensionIndex(info.getSortedDimensionsIndex());
        return dataProcessorInfo;
    }

    /**
     * @param model
     * @return
     */
    public static ComparatorChain getResultTempFileReaderComprator(int[][] maskedByteRangeForSorting,
            byte[] dimensionSortOrder, byte[][] dimensionMasks)
    {
        if(dimensionSortOrder.length < 1)
        {
            return null;
        }
        List<Comparator<ResultTempFileReader>> compratorList = new ArrayList<Comparator<ResultTempFileReader>>(
                MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
        MaksedByteComparatorForReader keyComparator = null;
        for(int i = 0;i < maskedByteRangeForSorting.length;i++)
        {
            if(null==maskedByteRangeForSorting[i])
            {
                continue;
            }
            keyComparator = new MaksedByteComparatorForReader(maskedByteRangeForSorting[i], dimensionSortOrder[i],
                    dimensionMasks[i]);
            compratorList.add(keyComparator);
        }
        return new ComparatorChain(compratorList);
    }

    /**
     * Returns the list of Molap files with specific extension at a specified
     * location.
     * 
     * @param location
     * @param extension
     * @return
     */
    public static MolapFile[] getFiles(final String location, final String[] extension)
    {
        MolapFile molapFile = FileFactory.getMolapFile(location, FileFactory.getFileType(location));
        MolapFile[] list = molapFile.listFiles(new MolapFileFilter()
        {
            @Override
            public boolean accept(MolapFile file)
            {
                for(int i = 0;i < extension.length;i++)
                {
                    if(file.getName().endsWith(extension[i]))
                    {
                        return true;
                    }
                }
                return false;
            }
        });
        return list;
    }
}
