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

package com.huawei.unibi.molap.engine.executer.impl;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.huawei.iweb.platform.logging.LogService;
import com.huawei.iweb.platform.logging.LogServiceFactory;
import com.huawei.unibi.molap.engine.executer.SliceExecuter;
import com.huawei.unibi.molap.engine.executer.exception.QueryExecutionException;
import com.huawei.unibi.molap.engine.executer.pagination.impl.QueryResult;
import com.huawei.unibi.molap.engine.executer.processor.ScannedResultProcessor;
import com.huawei.unibi.molap.engine.executer.processor.ScannedResultProcessorImpl;
import com.huawei.unibi.molap.engine.querystats.PartitionDetail;
import com.huawei.unibi.molap.engine.querystats.PartitionStatsCollector;
import com.huawei.unibi.molap.engine.schema.metadata.SliceExecutionInfo;
import com.huawei.unibi.molap.engine.util.MolapEngineLogEvent;
import com.huawei.unibi.molap.iterator.MolapIterator;

public class ColumnarDetailQueryParallelSliceExecutor implements SliceExecuter
{
 
    /**
     * LOGGER.
     */
    private static final LogService LOGGER = LogServiceFactory.getLogService(ColumnarDetailQueryParallelSliceExecutor.class.getName());
    
    
    private int numberOfCores;
    
    private SliceExecutionInfo latestInfo;
    
    public ColumnarDetailQueryParallelSliceExecutor(SliceExecutionInfo latestInfo, int numberOfCores)
    {
        this.numberOfCores=numberOfCores;
        this.latestInfo=latestInfo;
    }
    
    @Override
    public MolapIterator<QueryResult> executeSlices(List<SliceExecutionInfo> infos,int[] sliceIndex) throws QueryExecutionException
    {
        long startTime = System.currentTimeMillis();
        ColumnarSliceExecuter task = null;
        ScannedResultProcessor scannedResultProcessor = new ScannedResultProcessorImpl(latestInfo);
        ExecutorService execService=Executors.newFixedThreadPool(numberOfCores);
        try
        {
            for(int currentSliceIndex : sliceIndex)
            {
                if(currentSliceIndex==-1 || !infos.get(currentSliceIndex).isExecutionRequired())
                {
                    continue;
                }
                //Add this information to QueryDetail
                // queryDetail will be there only when user do <dataframe>.collect
                //TO-DO need to check for all queries
                PartitionStatsCollector partitionStatsCollector=PartitionStatsCollector.getInstance();
                PartitionDetail partitionDetail=partitionStatsCollector.getPartionDetail(infos.get(currentSliceIndex).getQueryId());
                if(null!=partitionDetail)
                {
                    partitionDetail.addNumberOfNodesScanned(infos.get(currentSliceIndex).getNumberOfNodeToScan());    
                }
                task = new ColumnarSliceExecuter(infos.get(currentSliceIndex), scannedResultProcessor, infos.get(
                        currentSliceIndex).getStartNode(), infos.get(currentSliceIndex).getNumberOfNodeToScan());
                execService.submit(task);
            }
            execService.shutdown();
            execService.awaitTermination(2, TimeUnit.DAYS);
            LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,
                    "Total time taken for scan " + (System.currentTimeMillis() - startTime));
            return scannedResultProcessor.getQueryResultIterator();
        }
        catch(QueryExecutionException exception)
        {
            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, exception, exception.getMessage());
            throw new QueryExecutionException(exception);
        }
        catch(InterruptedException e)
        {
            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, e, e.getMessage());
            throw new QueryExecutionException(e);
        }
        finally
        {
            execService= null;
        }
    }
}
