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

import com.huawei.iweb.platform.logging.LogService;
import com.huawei.iweb.platform.logging.LogServiceFactory;
import com.huawei.unibi.molap.engine.aggregator.MeasureAggregator;
import com.huawei.unibi.molap.engine.datastorage.storeInterfaces.KeyValue;
//import com.huawei.unibi.molap.engine.aggregator.util.AggUtil;
import com.huawei.unibi.molap.engine.executer.pagination.GlobalPaginatedAggregator;
import com.huawei.unibi.molap.engine.scanner.Scanner;
import com.huawei.unibi.molap.engine.schema.metadata.SliceExecutionInfo;
import com.huawei.unibi.molap.engine.util.MolapEngineLogEvent;
import com.huawei.unibi.molap.engine.wrappers.ByteArrayWrapper;

public class LocalDataAggregatorRSImpl extends LocalDataAggregatorImpl
{
    
    /**
     * LOGGER.
     */
    private static final LogService LOGGER = LogServiceFactory.getLogService(LocalDataAggregatorRSImpl.class.getName());

    public LocalDataAggregatorRSImpl(SliceExecutionInfo info, GlobalPaginatedAggregator paginatedAggregator,int rowLimit, String id)
    {
        super(info, paginatedAggregator,rowLimit,id);
    }

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

            int count = 0;
            // Search till end of the data
            while(!scanner.isDone() && !interrupted)
            {
                KeyValue available = scanner.getNext();
                // Set the data into the wrapper
                dimensionsRowWrapper.setData(available.getBackKeyArray(), available.getKeyOffset(), maxKeyBasedOnDim,
                        maskedByteRanges, maskedKeyByteSize);

                // 2) Extract required measures
                MeasureAggregator[] currentMsrRowData = data.get(dimensionsRowWrapper);

                if(currentMsrRowData == null)
                {
//                    currentMsrRowData = AggUtil.getAggregators(queryMsrs, aggTable, generator, slice.getCubeUniqueName());
                    // dimensionsRowWrapper.setActualData(available.getBackKeyArray(),
                    // available.getKeyOffset(), available.getKeyLength());
                    data.put(dimensionsRowWrapper, currentMsrRowData);
                    dimensionsRowWrapper = new ByteArrayWrapper();
                    counter++;
                    if(counter > rowLimit)
                    {
                        aggregateMsrsRS(available, currentMsrRowData);
                        rowLimitExceeded();
                        counter = 0;
                        count++;
                        continue;
                    }
                }
                aggregateMsrsRS(available, currentMsrRowData);
                count++;
            }

            finish();
            LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, "Time taken for scan for range " + id + " : "
                    + (System.currentTimeMillis() - startTime) + " && Scanned Count : " + count + "  && Map Size : "
                    + rowCount);
        }
        catch(Exception e)
        {
            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, e.getMessage());
        }
    }

    /**
     * @param available
     * @param currentMsrRowData
     */
    private void aggregateMsrsRS(KeyValue available, MeasureAggregator[] currentMsrRowData)
    {
        if(aggTable)
        {
            aggregateMsrsForAggTable(available, currentMsrRowData);
            return;
        }
        for(int i = 0;i < queryMsrs.length;i++)
        {
            double value = msrExists[i] ? available.getValue(measureOrdinal[i]) : msrDft[i];
            if(msrExists[i] && uniqueValues[measureOrdinal[i]] != value)
            {
                currentMsrRowData[i].agg(value, available.getBackKeyArray(), available.getKeyOffset(),
                        available.getKeyLength());
            }
            else if(!msrExists[i])
            {
                currentMsrRowData[i].agg(value, available.getBackKeyArray(), available.getKeyOffset(),
                        available.getKeyLength());
            }
        }
    }
    
    /**
     * aggregateMsrs
     * @param available
     * @param currentMsrRowData
     */
    protected void aggregateMsrsForAggTable(KeyValue available, MeasureAggregator[] currentMsrRowData)
    {
        double countValue = available.getValue(measureOrdinal[countMsrIndex]);
        
        for(int i = 0;i < avgMsrIndexes.length;i++)
        {
            int index = avgMsrIndexes[i];
            double value = msrExists[i] ? available.getValue(measureOrdinal[index]) : msrDft[index];
            if(msrExists[index] && uniqueValues[measureOrdinal[index]] != value)
            {
                currentMsrRowData[index].agg(value, countValue);
            }
            else if(!msrExists[index])
            {
                currentMsrRowData[index].agg(value, countValue);
            }
        }
        
        for(int i = 0;i < otherMsrIndexes.length;i++)
        {
            int index = otherMsrIndexes[i];
            double value = msrExists[i] ? available.getValue(measureOrdinal[index]) : msrDft[index];
            if(msrExists[index] && uniqueValues[measureOrdinal[index]] != value)
            {
                currentMsrRowData[index].agg(value, available.getBackKeyArray(), available.getKeyOffset(),
                        available.getKeyLength());
            }
            else if(!msrExists[index])
            {
                currentMsrRowData[index].agg(value, available.getBackKeyArray(), available.getKeyOffset(),
                        available.getKeyLength());
            }
        }
    }
}
