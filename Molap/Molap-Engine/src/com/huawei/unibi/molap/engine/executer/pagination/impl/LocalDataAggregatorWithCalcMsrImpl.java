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
TTt3d/QcyJrVzuX9j0ejnAqxCuiGxDS9cRmFja/Lr432Jx96MGu7GPRWrcFhCYyBc8YWtJZl
qmza/9W80UufcWjDRsS9xCTJJuRUtVSrabHxdlb9GjS+fiXXqmOTkHIhcDrDcA==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
/**
 * 
 */
package com.huawei.unibi.molap.engine.executer.pagination.impl;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import com.huawei.iweb.platform.logging.LogService;
import com.huawei.iweb.platform.logging.LogServiceFactory;
import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.engine.aggregator.MeasureAggregator;
import com.huawei.unibi.molap.engine.aggregator.util.AggUtil;
import com.huawei.unibi.molap.engine.cache.QueryExecutorUtil;
import com.huawei.unibi.molap.engine.datastorage.InMemoryCube;
import com.huawei.unibi.molap.engine.datastorage.storeInterfaces.KeyValue;
import com.huawei.unibi.molap.engine.executer.calcexp.MolapCalcFunction;
import com.huawei.unibi.molap.engine.executer.pagination.DataAggregator;
import com.huawei.unibi.molap.engine.executer.pagination.GlobalPaginatedAggregator;
import com.huawei.unibi.molap.engine.scanner.Scanner;
import com.huawei.unibi.molap.engine.schema.metadata.SliceExecutionInfo;
import com.huawei.unibi.molap.engine.util.MolapEngineLogEvent;
import com.huawei.unibi.molap.engine.wrappers.ByteArrayWrapper;
import com.huawei.unibi.molap.keygenerator.KeyGenerator;
import com.huawei.unibi.molap.metadata.MolapMetadata.Measure;
import com.huawei.unibi.molap.util.MolapProperties;

/**
 * @author R00900208
 *
 */
public class LocalDataAggregatorWithCalcMsrImpl implements DataAggregator
{


    /**
     * 
     */
    protected byte[] maxKeyBasedOnDim;

    /**
     * 
     */
    protected int[] maskedByteRanges;

    /**
     * 
     */
    protected int maskedKeyByteSize;
    
    
    /**
     * Measures
     */
    protected Measure[] queryMsrs;
    
    /**
     * Key generator
     */
    protected KeyGenerator generator;
    
    /**
     * slice
     */
    protected InMemoryCube slice;
    
    /**
     * msrExists
     */
    protected boolean[] msrExists;
    
    /**
     * msrDft
     */
    protected double[] msrDft;
    
    /**
     * Row counter to interrupt;
     */
    protected int rowLimit;
    
    /**
     * Internal row counter;
     */
    protected int counter;
    
    /**
     * Unique values represents null values of measure.
     */
    protected double[] uniqueValues;
    
    
    protected ByteArrayWrapper dimensionsRowWrapper = new ByteArrayWrapper();
    
    
    /**
     * Measures ordinal
     */
    protected int[] measureOrdinal;
    
    /**
     * Data Map 
     */
    protected Map<ByteArrayWrapper, MeasureAggregator[]> data;
    
    /**
     * paginatedAggregator
     */
    protected GlobalPaginatedAggregator paginatedAggregator;
    
    protected SliceExecutionInfo info;
    
    private MolapCalcFunction[] calFunctions;
    
    private int msrLength;
    
//    private int calcMsrLength;
    
    private boolean interrupted;
    
    /**
     * ID
     */
    private String id;
    
    private long rowCount;
    
    private int[] avgMsrIndexes;
    
    private int countMsrIndex;
    
    private boolean aggTable;
    
    private int[] otherMsrIndexes;
    
    /**
     * LOGGER.
     */
    private static final LogService LOGGER = LogServiceFactory.getLogService(LocalDataAggregatorWithCalcMsrImpl.class.getName());
    
    
    public LocalDataAggregatorWithCalcMsrImpl(SliceExecutionInfo info,GlobalPaginatedAggregator paginatedAggregator,int rowLimit, String id)
    {
//        queryMsrs = info.getQueryMsrs();
        measureOrdinal = info.getMeasureOrdinal();
//        maxKeyBasedOnDim = info.getMaxKeyBasedOnDimensions();
//        maskedByteRanges = info.getMaskedByteRanges();
        maskedKeyByteSize = info.getMaskedKeyByteSize();
        this.generator = info.getKeyGenerator();
        this.slice = info.getSlice();
//        this.msrExists = info.getMsrsExists();
//        this.msrDft = info.getNewMeasureDftValue();
        this.uniqueValues = info.getUniqueValues();
        String mapSize = MolapProperties.getInstance().getProperty("molap.intial.agg.mapsize", "5000");
        int size = Integer.parseInt(mapSize);
        data = new HashMap<ByteArrayWrapper, MeasureAggregator[]>(size);
       
        this.paginatedAggregator = paginatedAggregator;
        this.info = info;
        this.rowLimit = rowLimit;
//        setCalcFunctions(info.getCalculatedMeasures(), Arrays.asList(info.getQueryMsrs()));
//        msrLength = queryMsrs.length;
        this.avgMsrIndexes = QueryExecutorUtil.convertIntegerListToIntArray(info.getAvgIndexes());
        Arrays.sort(avgMsrIndexes);
        this.countMsrIndex = info.getCountMsrsIndex();
        aggTable = countMsrIndex>-1;
        if(aggTable)
        {
            otherMsrIndexes = getOtherMsrIndexes();
        }
//        calcMsrLength = calFunctions.length;
        this.id = id;
    }
    
    
    /*private void setCalcFunctions(CalculatedMeasure[] calculatedMeasures,List<Measure> msrs)
    {
        List<MolapCalcFunction> calFunList = new ArrayList<MolapCalcFunction>();
        
        for(int i = 0;i < calculatedMeasures.length;i++)
        {
            
            MolapCalcFunction calExpr = MolapCalcExpressionResolverUtil.createCalcExpressions(calculatedMeasures[i].getExp(), msrs);
            if(calExpr != null)
            {
                calFunList.add(calExpr);
            }
        }
        calFunctions = calFunList.toArray(new MolapCalcFunction[calFunList.size()]);
    }*/
    
   //TODO SIMIAN 
    /* (non-Javadoc)
     * @see com.huawei.unibi.molap.engine.executer.pagination.DataAggregator#aggregate(byte[], com.huawei.unibi.molap.engine.aggregator.MeasureAggregator[])
     */
    @Override
    public void aggregate(Scanner scanner)
    {
        long startTime = System.currentTimeMillis();
        try
        {
            

            // for aggregating without decomposition using masking

            int countVal = 0;   
            // Search till end of the data
            while(!scanner.isDone() && !interrupted)
            {
                KeyValue available = scanner.getNext();
                // Set the data into the wrapper
                dimensionsRowWrapper.setData(available.backKeyArray, available.keyOffset, maxKeyBasedOnDim,
                        maskedByteRanges, maskedKeyByteSize);

                // 2) Extract required measures
                MeasureAggregator[] currentMsrRowData = data.get(dimensionsRowWrapper);

                if(currentMsrRowData == null)
                {
                    currentMsrRowData = AggUtil.getAggregators(queryMsrs, calFunctions, aggTable, generator, slice.getCubeUniqueName());
                    // dimensionsRowWrapper.setActualData(available.getBackKeyArray(),
                    // available.getKeyOffset(), available.getKeyLength());
                    data.put(dimensionsRowWrapper, currentMsrRowData);
                    dimensionsRowWrapper = new ByteArrayWrapper();
                    counter++;
                    if(counter > rowLimit)
                    {
                        aggregateMsrs(available, currentMsrRowData);
                        rowLimitExceeded();
                        counter = 0;
                        countVal++;
                        continue;
                    }
                }
                aggregateMsrs(available, currentMsrRowData);
//                for(int i = 0;i < calcMsrLength;i++)
//                {
//                    ((CalculatedMeasureAggregator)currentMsrRowData[i + msrLength])
//                            .calculateCalcMeasure(currentMsrRowData);
//                }
                countVal++;
            }
            rowCount += data.size();
            LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, "Time taken for scan for range " + id + " : "
                    + (System.currentTimeMillis() - startTime) + " && Scanned Count : " + countVal + "  && Map Size : "
                    + rowCount);
            finish();

        }
//        catch (ResourceLimitExceededException e) 
//        {
//            throw e;
//        }
//        
        catch(Exception e)
        {
            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, e);
        }

    }

    private int[] getOtherMsrIndexes()
    {
        int[] indexes = new int[queryMsrs.length-(avgMsrIndexes.length)];
        int k = 0;
        for(int i = 0;i < queryMsrs.length;i++)
        {
            if(Arrays.binarySearch(avgMsrIndexes, i) < 0)
            {
                indexes[k++] = i;
            }
        }
        return indexes;
    }

    /**
     * @param available
     * @param currentMsrRowData
     */
    private void aggregateMsrs(KeyValue available, MeasureAggregator[] currentMsrRowData)
    {
        if(aggTable)
        {
            aggregateMsrsForAggTable(available, currentMsrRowData);
            return;
        }
        for(int i = 0;i < msrLength;i++)
        {
            double value = available.getValue(measureOrdinal[i]);
            if(uniqueValues[measureOrdinal[i]] != value)
            {
                currentMsrRowData[i].agg(value, available.backKeyArray, available.keyOffset,
                        available.keyLength);
            }
        }
    }
    
    /**
     * aggregateMsrs
     * @param available
     * @param currentMsrRowData
     */
    
    private void aggregateMsrsForAggTable(KeyValue available, MeasureAggregator[] currentMsrRowData)
    {
        double countValue = available.getValue(measureOrdinal[countMsrIndex]);
        
        for(int k = 0;k < avgMsrIndexes.length;k++)
        {
            double value = available.getValue(measureOrdinal[avgMsrIndexes[k]]);
            if(uniqueValues[measureOrdinal[avgMsrIndexes[k]]] != value)
            {
                currentMsrRowData[avgMsrIndexes[k]].agg(value,countValue);
            } 
        }
        
        for(int i = 0;i < otherMsrIndexes.length;i++)
        {
            double value = available.getValue(measureOrdinal[otherMsrIndexes[i]]);
            if(uniqueValues[measureOrdinal[otherMsrIndexes[i]]] != value)
            {
                currentMsrRowData[otherMsrIndexes[i]].agg(value, available.backKeyArray,
                    available.keyOffset, available.keyLength);
            }
        }
    }
    
    


    public void rowLimitExceeded() throws Exception//,ResourceLimitExceededException
    {
//        if(!info.isPaginationRequired())
//        {
//           throw MondrianResource.instance().MemberFetchLimitExceeded.ex(rowLimit);
//        }
        rowCount += data.size();
        paginatedAggregator.writeToDisk(data,info.getRestructureHolder());
        data = new HashMap<ByteArrayWrapper, MeasureAggregator[]>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
    }

    @Override
    public void finish() throws Exception
    {
//        rowCount += data.size();
        paginatedAggregator.writeToDisk(data,info.getRestructureHolder());
    }


    @Override
    public void interrupt()
    {
       interrupted = true;
    }

}
