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
TTt3d1L7W42ZUyEL2HruC0JIv4bUd47w/FjvyhcuisEK8hD7X+v2Hfm3EevSLol9ZS6lqSX9
HlHDBsjbfO3Xru/uKgyXijwamUk1pDcwznB46Jh8sCYaMJUEOsVI01LVtivrNg==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
/**
 * 
 */
package com.huawei.unibi.molap.engine.executer.impl.topn;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.engine.aggregator.MeasureAggregator;
import com.huawei.unibi.molap.engine.executer.groupby.GroupByHolder;
import com.huawei.unibi.molap.engine.executer.pagination.DataProcessor;
import com.huawei.unibi.molap.engine.executer.pagination.PaginationModel;
import com.huawei.unibi.molap.engine.executer.pagination.exception.MolapPaginationException;
import com.huawei.unibi.molap.engine.wrappers.ByteArrayWrapper;


/**
 * @author R00900208
 *
 */
public class TopNProcessorMerger implements DataProcessor
{
    
    private List<byte[]> keys = new ArrayList<byte[]>(MolapCommonConstants.CONSTANT_SIZE_TEN);
    
    private List<MeasureAggregator[]> aggregators = new ArrayList<MeasureAggregator[]>(MolapCommonConstants.CONSTANT_SIZE_TEN);
    
    private ExecutorService executorService;
    
    private PaginationModel model;
    
    private DataProcessor processor;
    
    private Comparator comparator;
    
    private int count;
    
    private List<Future< Map<ByteArrayWrapper, MeasureAggregator[]>>> futures; 
    
    public TopNProcessorMerger(DataProcessor processor,Comparator comparator)
    {
        this.processor = processor;
        this.comparator = comparator;
    }

    @Override
    public void initModel(PaginationModel model) throws MolapPaginationException
    {
       this.model = model;
       this.executorService = Executors.newFixedThreadPool(4);
       futures = new ArrayList<Future< Map<ByteArrayWrapper, MeasureAggregator[]>>>(MolapCommonConstants.CONSTANT_SIZE_TEN);
    }

    @Override
    public void processRow(byte[] key, MeasureAggregator[] measures) throws MolapPaginationException
    {
        keys.add(key);
        aggregators.add(measures);
        count++;
        if(count > 100000)
        {
            submit();
        }
    }

    /**
     * @throws MolapPaginationException 
     * 
     */
    private void submit() throws MolapPaginationException
    {
        count = 0;
//        List<byte[]> keysL = keys;
//        List<MeasureAggregator[]> aggregatorsL = aggregators;
        keys = new ArrayList<byte[]>(MolapCommonConstants.CONSTANT_SIZE_TEN);
        aggregators = new ArrayList<MeasureAggregator[]>(MolapCommonConstants.CONSTANT_SIZE_TEN);
        
//        TopNProcessorBytes task = new TopNProcessorBytes(keysL, aggregatorsL);
//        task.initModel(model);
//        Future<Map<ByteArrayWrapper, MeasureAggregator[]>> future = executorService.submit(task);
//        futures.add(future);
    }

    @Override
    public void finish() throws MolapPaginationException
    {
        if(count > 0)
        {
            submit();
        }
        executorService.shutdown();
        try
        {
            executorService.awaitTermination(2, TimeUnit.DAYS);
            Map<ByteArrayWrapper, MeasureAggregator[]> treeMap = null;
            if(model.getQueryDims().length > 0)
            {
                treeMap = new TreeMap<ByteArrayWrapper, MeasureAggregator[]>(this.comparator);
            }
            else
            {
                treeMap = new HashMap<ByteArrayWrapper, MeasureAggregator[]>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
            }
           
        
            for(Future< Map<ByteArrayWrapper, MeasureAggregator[]>> future : futures)
            {
                Map<ByteArrayWrapper, MeasureAggregator[]> data = future.get();
                treeMap.putAll(data);
            }
            TopNProcessorBytes processorBytes = new TopNProcessorBytes(processor);
            processorBytes.initModel(model);
            for(Entry<ByteArrayWrapper, MeasureAggregator[]> entry : treeMap.entrySet())
            {
                processorBytes.processRow(entry.getKey().getMaskedKey(), entry.getValue());
            }
            processorBytes.finish();
        }
        catch(Exception e)
        {
           throw new MolapPaginationException(e);
        }
    }

    @Override
    public void processGroup(GroupByHolder groupByHolder)
    {
        
    }

}
