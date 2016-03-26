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

package org.carbondata.query.executer.impl.topn;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.carbondata.core.constants.MolapCommonConstants;
import org.carbondata.query.aggregator.MeasureAggregator;
import org.carbondata.query.executer.groupby.GroupByHolder;
import org.carbondata.query.executer.pagination.DataProcessor;
import org.carbondata.query.executer.pagination.PaginationModel;
import org.carbondata.query.executer.pagination.exception.MolapPaginationException;
import org.carbondata.query.wrappers.ByteArrayWrapper;

public class TopNProcessorMerger implements DataProcessor {

    private List<byte[]> keys = new ArrayList<byte[]>(MolapCommonConstants.CONSTANT_SIZE_TEN);

    private List<MeasureAggregator[]> aggregators =
            new ArrayList<MeasureAggregator[]>(MolapCommonConstants.CONSTANT_SIZE_TEN);

    private ExecutorService executorService;

    private PaginationModel model;

    private DataProcessor processor;

    private Comparator comparator;

    private int count;

    private List<Future<Map<ByteArrayWrapper, MeasureAggregator[]>>> futures;

    public TopNProcessorMerger(DataProcessor processor, Comparator comparator) {
        this.processor = processor;
        this.comparator = comparator;
    }

    @Override
    public void initModel(PaginationModel model) throws MolapPaginationException {
        this.model = model;
        this.executorService = Executors.newFixedThreadPool(4);
        futures = new ArrayList<Future<Map<ByteArrayWrapper, MeasureAggregator[]>>>(
                MolapCommonConstants.CONSTANT_SIZE_TEN);
    }

    @Override
    public void processRow(byte[] key, MeasureAggregator[] measures)
            throws MolapPaginationException {
        keys.add(key);
        aggregators.add(measures);
        count++;
        if (count > 100000) {
            submit();
        }
    }

    /**
     * @throws MolapPaginationException
     */
    private void submit() throws MolapPaginationException {
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
    public void finish() throws MolapPaginationException {
        if (count > 0) {
            submit();
        }
        executorService.shutdown();
        try {
            executorService.awaitTermination(2, TimeUnit.DAYS);
            Map<ByteArrayWrapper, MeasureAggregator[]> treeMap = null;
            if (model.getQueryDims().length > 0) {
                treeMap = new TreeMap<ByteArrayWrapper, MeasureAggregator[]>(this.comparator);
            } else {
                treeMap = new HashMap<ByteArrayWrapper, MeasureAggregator[]>(
                        MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
            }

            for (Future<Map<ByteArrayWrapper, MeasureAggregator[]>> future : futures) {
                Map<ByteArrayWrapper, MeasureAggregator[]> data = future.get();
                treeMap.putAll(data);
            }
            TopNProcessorBytes processorBytes = new TopNProcessorBytes(processor);
            processorBytes.initModel(model);
            for (Entry<ByteArrayWrapper, MeasureAggregator[]> entry : treeMap.entrySet()) {
                processorBytes.processRow(entry.getKey().getMaskedKey(), entry.getValue());
            }
            processorBytes.finish();
        } catch (Exception e) {
            throw new MolapPaginationException(e);
        }
    }

    @Override
    public void processGroup(GroupByHolder groupByHolder) {

    }

}
