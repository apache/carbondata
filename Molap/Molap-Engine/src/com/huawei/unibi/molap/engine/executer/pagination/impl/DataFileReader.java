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

package com.huawei.unibi.molap.engine.executer.pagination.impl;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.huawei.iweb.platform.logging.LogService;
import com.huawei.iweb.platform.logging.LogServiceFactory;
import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.datastorage.store.FileHolder;
import com.huawei.unibi.molap.datastorage.store.compression.SnappyCompression.SnappyByteCompression;
import com.huawei.unibi.molap.datastorage.store.impl.FileFactory;
//import com.huawei.unibi.molap.datastorage.store.impl.FileHolderImpl;
import com.huawei.unibi.molap.engine.aggregator.MeasureAggregator;
import com.huawei.unibi.molap.engine.aggregator.util.AggUtil;
import com.huawei.unibi.molap.engine.cache.MolapSegmentHeader;
import com.huawei.unibi.molap.engine.cache.QueryExecutorUtil;
import com.huawei.unibi.molap.engine.datastorage.InMemoryCube;
import com.huawei.unibi.molap.engine.executer.impl.InMemoryQueryExecutor;
import com.huawei.unibi.molap.engine.util.MolapEngineLogEvent;
import com.huawei.unibi.molap.engine.wrappers.ByteArrayWrapper;
import com.huawei.unibi.molap.engine.wrappers.PostQueryAggregatorUtil.ByteArrayWrapperExtended;
import com.huawei.unibi.molap.keygenerator.KeyGenException;
import com.huawei.unibi.molap.keygenerator.KeyGenerator;
import com.huawei.unibi.molap.metadata.CalculatedMeasure;
import com.huawei.unibi.molap.metadata.LeafNodeInfo;
import com.huawei.unibi.molap.metadata.MolapMetadata.Dimension;
import com.huawei.unibi.molap.metadata.MolapMetadata.Measure;
import com.huawei.unibi.molap.util.MolapUtil;

/**
 * It reads the data from file which already created during pagination scanner.
 *
 */
public class DataFileReader
{
    /**
     * LOGGER
     */
    private static final LogService LOGGER = LogServiceFactory.getLogService(InMemoryQueryExecutor.class.getName());
    /**
     * keySize
     */
    private int keySize;
    
    /**
     * leafNodeInfos
     */
    private List<LeafNodeInfo> leafNodeInfos;
    
    
    private File filePath;
    
    private KeyGenerator generator;
    
    private Measure[] measures;
    
    private CalculatedMeasure[] calculatedMeasures;
    
    private int[] measureIndexToRead;
    
    private InMemoryCube slice;
    
    
    public DataFileReader(int keySize, String outLocation, String queryId,KeyGenerator generator,Measure[] measures,CalculatedMeasure[] calculatedMeasures, int[] measureIndexToRead,InMemoryCube slice)
    {
        this.keySize = keySize;
        this.generator = generator;
        this.measures = measures;
        filePath = new File(outLocation + File.separator + queryId+"_0");
        this.calculatedMeasures = calculatedMeasures;
        if(filePath.exists())
        {
            leafNodeInfos = MolapUtil.getLeafNodeInfo(filePath, measures.length+calculatedMeasures.length, keySize);
        }
        this.measureIndexToRead=measureIndexToRead;
        this.slice = slice;
    }

    public Map<ByteArrayWrapper, MeasureAggregator[]> readData(int fromRow, int toRow) throws IOException
    {
        if(!filePath.exists())
        {
            return null;
        }
        FileHolder fileHolder = FileFactory.getFileHolder(FileFactory.getFileType(filePath.getCanonicalPath()));
        Map<ByteArrayWrapper, MeasureAggregator[]> data = new LinkedHashMap<ByteArrayWrapper, MeasureAggregator[]>();
        try
        {
            MeasureAggregator[] measureAggregators = AggUtil.getAggregators(measures, calculatedMeasures, false,
                    generator, slice.getCubeUniqueName());
            MeasureAggregator[] measureAggregatorsToRead = new MeasureAggregator[measureIndexToRead.length];
            for(int i = 0;i < measureIndexToRead.length;i++)
            {
                measureAggregatorsToRead[i] = measureAggregators[measureIndexToRead[i]];
            }
            int rowCount = 0;
            for(int i = 0;i < leafNodeInfos.size();i++)
            {
                LeafNodeInfo leafNodeInfo = leafNodeInfos.get(i);

                if(fromRow <= (rowCount + leafNodeInfo.getNumberOfKeys()))
                {
                    byte[] keyArray = fileHolder.readByteArray(filePath.getAbsolutePath(), leafNodeInfo.getKeyOffset(),
                            leafNodeInfo.getKeyLength());
                    DataInputStream[] msrStreams = new DataInputStream[this.measureIndexToRead.length];
                    for(int j = 0;j < measureAggregatorsToRead.length;j++)
                    {
                        msrStreams[j] = new DataInputStream(new ByteArrayInputStream(
                                SnappyByteCompression.INSTANCE.unCompress(fileHolder.readByteArray(
                                        filePath.getAbsolutePath(), leafNodeInfo.getMeasureOffset()[measureIndexToRead[j]],
                                        leafNodeInfo.getMeasureLength()[measureIndexToRead[j]]))));
                    }

                    DataInputStream keyStream = new DataInputStream(new ByteArrayInputStream(
                            SnappyByteCompression.INSTANCE.unCompress(keyArray)));
                    int counter = rowCount;
                    for(int j = 0;j < leafNodeInfo.getNumberOfKeys();j++)
                    {
                        byte[] key = new byte[keySize];
                        if(keyStream.read(key)<0)
                        {
                            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, "Problem while reading the pagination file");
                        }
                        for(int k = 0;k < measureAggregatorsToRead.length;k++)
                        {
                            measureAggregatorsToRead[k].readData(msrStreams[k]);
                        }
                        counter++;
                        if(counter >= fromRow)
                        {
                            ByteArrayWrapper wrapper = new ByteArrayWrapper();
                            wrapper.setMaskedKey(key);
                            data.put(wrapper, measureAggregatorsToRead);
                            // measureAggregators =
                            // AggUtil.getAggregators(measures,calculatedMeasures,
                            // false, generator, null);
                            measureAggregators = AggUtil.getAggregators(measures, calculatedMeasures, false,
                                    generator, slice.getCubeUniqueName());
                            measureAggregatorsToRead = new MeasureAggregator[measureIndexToRead.length];
                            for(int k1 = 0;k1 < measureIndexToRead.length;k1++)
                            {
                                measureAggregatorsToRead[k1] = measureAggregators[measureIndexToRead[k1]];
                            }
                        }
                        if(counter >= toRow)
                        {
                            fileHolder.finish();
                            return data;
                        }
                    }
                }
                rowCount += leafNodeInfo.getNumberOfKeys();
            }
        }
        finally
        {
            fileHolder.finish();
        }
        return data;
    }
    
    public Map<ByteArrayWrapper, MeasureAggregator[]> readDataAndAggregate(int fromRow, int toRow,MolapSegmentHeader finalHeader,KeyGenerator keyGenerator,Dimension[] requireDims,boolean actual) throws IOException, KeyGenException
    {
        
        int[] dims = new int[requireDims.length];
        for(int i = 0;i < requireDims.length;i++)
        {
            dims[i] = requireDims[i].getOrdinal();
        }
        List<Integer> bytePos = new ArrayList<Integer>(MolapCommonConstants.CONSTANT_SIZE_TEN);
        byte[] maskedBytesForRollUp = QueryExecutorUtil.getMaskedBytesForRollUp(dims, keyGenerator,
                QueryExecutorUtil.getRangesForMaskedByte(finalHeader.getDims(), keyGenerator), bytePos);
        int[] bytePosArray = QueryExecutorUtil.convertIntegerListToIntArray(bytePos);
        
        int mainCounter=0;
        ByteArrayWrapperExtended extended = new ByteArrayWrapperExtended();
        ByteArrayWrapperExtended extendedPrev = new ByteArrayWrapperExtended();
        
        MeasureAggregator[] prevAggregators = null;
        byte[] prevKey = null;
        
        
        if(!filePath.exists())
        {
            return null;
        }
        FileHolder fileHolder = FileFactory.getFileHolder(FileFactory.getFileType(filePath.getCanonicalPath()));
        Map<ByteArrayWrapper, MeasureAggregator[]> data = new LinkedHashMap<ByteArrayWrapper, MeasureAggregator[]>();
        try
        {
            MeasureAggregator[] measureAggregators = AggUtil.getAggregators(measures, calculatedMeasures, false,
                    generator, slice.getCubeUniqueName());
            MeasureAggregator[] measureAggregatorsToRe = new MeasureAggregator[measureIndexToRead.length];
            for(int i = 0;i < measureIndexToRead.length;i++)
            {
                measureAggregatorsToRe[i] = measureAggregators[measureIndexToRead[i]];
            }
            int counter = 0;
//            int rowCount = 0;
            for(int i = 0;i < leafNodeInfos.size();i++)
            {
                LeafNodeInfo leafNodeInfo = leafNodeInfos.get(i);

//                if(fromRow <= (rowCount + leafNodeInfo.getNumberOfKeys()))
//                {
                    byte[] keyArray = fileHolder.readByteArray(filePath.getAbsolutePath(), leafNodeInfo.getKeyOffset(),
                            leafNodeInfo.getKeyLength());
                    DataInputStream[] msrStreams = new DataInputStream[this.measureIndexToRead.length];
                    for(int j = 0;j < measureAggregatorsToRe.length;j++)
                    {
                        msrStreams[j] = new DataInputStream(new ByteArrayInputStream(
                                SnappyByteCompression.INSTANCE.unCompress(fileHolder.readByteArray(
                                        filePath.getAbsolutePath(), leafNodeInfo.getMeasureOffset()[measureIndexToRead[j]],
                                        leafNodeInfo.getMeasureLength()[measureIndexToRead[j]]))));
                    }

                    DataInputStream keyStream = new DataInputStream(new ByteArrayInputStream(
                            SnappyByteCompression.INSTANCE.unCompress(keyArray)));
                  
                    for(int j = 0;j < leafNodeInfo.getNumberOfKeys();j++)
                    {
                        byte[] key = new byte[keySize];
                        if(keyStream.read(key)<0)
                        {
                            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, "Problem while reading the pagination file");
                        }
                        measureAggregators = AggUtil.getAggregators(measures, calculatedMeasures, false,
                                generator, slice.getCubeUniqueName());
                        MeasureAggregator[] measureAggregatorsToRead = new MeasureAggregator[measureIndexToRead.length];
                        for(int l = 0;l < measureIndexToRead.length;l++)
                        {
                            measureAggregatorsToRead[l] = measureAggregators[measureIndexToRead[l]];
                        }
                        
                        for(int k = 0;k < measureAggregatorsToRead.length;k++)
                        {
                            measureAggregatorsToRead[k].readData(msrStreams[k]);
                        }
                        if(mainCounter >= toRow)
                        {
                            fileHolder.finish();
                            fillData(fromRow, toRow, prevAggregators, prevKey, data, counter);
                            return data;
                        }
                        
                        if(mainCounter >= fromRow-1)
                        {
                            
                            if(prevKey != null && !actual)
                            {
                                extended.setData(key, bytePosArray, maskedBytesForRollUp);
                                extendedPrev.setData(prevKey, bytePosArray, maskedBytesForRollUp);
                                if(extended.equals(extendedPrev))
                                {
                                    for(int k = 0;k < measureAggregatorsToRead.length;k++)
                                    {
                                        measureAggregatorsToRead[k].merge(prevAggregators[k]);
                                    }
                                }
                                else
                                {
                                    ByteArrayWrapper wrapper = new ByteArrayWrapper();
                                    wrapper.setMaskedKey(prevKey);
                                    data.put(wrapper, prevAggregators);
                                }
                            }
                            
                            if(actual)
                            {
                                if(prevKey != null)
                                {
                                    ByteArrayWrapper wrapper = new ByteArrayWrapper();
                                    wrapper.setMaskedKey(prevKey);
                                    data.put(wrapper, prevAggregators);
                                }
                            }
                            
                        }
                        
                        if(prevKey != null)
                        {
                            extended.setData(key, bytePosArray, maskedBytesForRollUp);
                            extendedPrev.setData(prevKey, bytePosArray, maskedBytesForRollUp);
                            if(!extended.equals(extendedPrev))
                            {
                                counter++;
                            }
                        }

                        prevKey = key;
                        prevAggregators = measureAggregatorsToRead;
                        mainCounter++;
                    }
                    if(prevKey != null && counter >= fromRow-1 && counter < toRow)
                    {
                        ByteArrayWrapper wrapper = new ByteArrayWrapper();
                        wrapper.setMaskedKey(prevKey);
                        data.put(wrapper, prevAggregators);
                    }
                }
//                rowCount += leafNodeInfo.getNumberOfKeys();
//            }
        }
        finally
        {
            fileHolder.finish();
        }
        return data;
    }

    private void fillData(int fromRow, int toRow, MeasureAggregator[] prevAggregators, byte[] prevKey,
            Map<ByteArrayWrapper, MeasureAggregator[]> data, int counter)
    {
        if(prevKey != null && counter >= fromRow-1 && counter < toRow && data.isEmpty())
        {
            ByteArrayWrapper wrapper = new ByteArrayWrapper();
            wrapper.setMaskedKey(prevKey);
            data.put(wrapper, prevAggregators);
        }
    }
    
    public int getSizeAfterAggregate(int fromRow, int toRow,MolapSegmentHeader finalHeader,KeyGenerator keyGenerator,Dimension[] requireDims) throws IOException, KeyGenException
    {
        
        int[] dims = new int[requireDims.length];
        for(int i = 0;i < requireDims.length;i++)
        {
            dims[i] = requireDims[i].getOrdinal();
        }
        List<Integer> bytePos = new ArrayList<Integer>();
        byte[] maskedBytesForRollUp = QueryExecutorUtil.getMaskedBytesForRollUp(dims, keyGenerator,
                QueryExecutorUtil.getRangesForMaskedByte(finalHeader.getDims(), keyGenerator), bytePos);
        int[] bytePosArray = QueryExecutorUtil.convertIntegerListToIntArray(bytePos);
        
        int size = 0;
        
        ByteArrayWrapperExtended extended = new ByteArrayWrapperExtended();
        ByteArrayWrapperExtended extendedPrev = new ByteArrayWrapperExtended();
        
        byte[] prevKey = null;
        
        
        if(!filePath.exists())
        {
            return 0;
        }
        FileHolder fileHolder = FileFactory.getFileHolder(FileFactory.getFileType(filePath.getCanonicalPath()));
        try
        {
            MeasureAggregator[] measureAggregators = AggUtil.getAggregators(measures, calculatedMeasures, false,
                    generator, null);
            MeasureAggregator[] measureAggregatorsToRe = new MeasureAggregator[measureIndexToRead.length];
            for(int i = 0;i < measureIndexToRead.length;i++)
            {
                measureAggregatorsToRe[i] = measureAggregators[measureIndexToRead[i]];
            }

            for(int i = 0;i < leafNodeInfos.size();i++)
            {
                LeafNodeInfo leafNodeInfo = leafNodeInfos.get(i);

//                if(fromRow <= (rowCount + leafNodeInfo.getNumberOfKeys()))
//                {
                    byte[] keyArry = fileHolder.readByteArray(filePath.getAbsolutePath(), leafNodeInfo.getKeyOffset(),
                            leafNodeInfo.getKeyLength());
                    DataInputStream[] msrStreams = new DataInputStream[this.measureIndexToRead.length];
                    for(int j = 0;j < measureAggregatorsToRe.length;j++)
                    {
                        msrStreams[j] = new DataInputStream(new ByteArrayInputStream(
                                SnappyByteCompression.INSTANCE.unCompress(fileHolder.readByteArray(
                                        filePath.getAbsolutePath(), leafNodeInfo.getMeasureOffset()[measureIndexToRead[j]],
                                        leafNodeInfo.getMeasureLength()[measureIndexToRead[j]]))));
                    }

                    DataInputStream keyStream = new DataInputStream(new ByteArrayInputStream(
                            SnappyByteCompression.INSTANCE.unCompress(keyArry)));
                    for(int j = 0;j < leafNodeInfo.getNumberOfKeys();j++)
                    {
                        byte[] key = new byte[keySize];
                        if(keyStream.read(key)<0)
                        {
                            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, "Problem while reading the pagination file");
                        }
                        measureAggregators = AggUtil.getAggregators(measures, calculatedMeasures, false,
                                generator, null);
                        MeasureAggregator[] measureAggregatorsToRead = new MeasureAggregator[measureIndexToRead.length];
                        for(int l = 0;l < measureIndexToRead.length;l++)
                        {
                            measureAggregatorsToRead[l] = measureAggregators[measureIndexToRead[l]];
                        }
                        
                        for(int k = 0;k < measureAggregatorsToRead.length;k++)
                        {
                            measureAggregatorsToRead[k].readData(msrStreams[k]);
                        }
                        
                        if(prevKey != null)
                        {
                            extended.setData(key, bytePosArray, maskedBytesForRollUp);
                            extendedPrev.setData(prevKey, bytePosArray, maskedBytesForRollUp);
                            if(!extended.equals(extendedPrev))
                            {
                                size++;
                            }
                        }
                        
                        prevKey = key;
                    }
                    if(prevKey != null)
                    {
                        size++;
                    }
//                }
//                rowCount += leafNodeInfo.getNumberOfKeys();
            }
        }
        finally
        {
            fileHolder.finish();
        }
        return size;
    }
    
}
