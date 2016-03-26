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

package org.carbondata.query.result.iterator;

import java.util.Arrays;
import java.util.List;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.constants.MolapCommonConstants;
import org.carbondata.core.iterator.MolapIterator;
import org.carbondata.core.keygenerator.KeyGenException;
import org.carbondata.core.util.MolapProperties;
import org.carbondata.query.datastorage.storeInterfaces.DataStoreBlock;
import org.carbondata.query.executer.MolapQueryExecutorModel;
import org.carbondata.query.executer.SliceExecuter;
import org.carbondata.query.executer.exception.QueryExecutionException;
import org.carbondata.query.executer.impl.ColumnarDetailQueryParallelSliceExecutor;
import org.carbondata.query.executer.impl.QueryExecuterProperties;
import org.carbondata.query.executer.impl.QueryResultPreparator;
import org.carbondata.query.executer.pagination.impl.QueryResult;
import org.carbondata.query.result.ChunkResult;
import org.carbondata.query.schema.metadata.SliceExecutionInfo;
import org.carbondata.query.util.MolapEngineLogEvent;

public class DetailQueryResultIterator implements MolapIterator<ChunkResult> {

    /**
     * LOGGER.
     */
    private static final LogService LOGGER =
            LogServiceFactory.getLogService(DetailQueryResultIterator.class.getName());

    private QueryResultPreparator queryResultPreparator;

    private List<SliceExecutionInfo> infos;

    private SliceExecuter executor;

    private long numberOfCores;

    private long[] totalNumberOfNodesPerSlice;

    private long totalNumberOfNode;

    private long currentCounter;

    private long[] numberOfExecutedNodesPerSlice;

    private int[] sliceIndexToBeExecuted;

    public DetailQueryResultIterator(List<SliceExecutionInfo> infos,
            QueryExecuterProperties executerProperties, MolapQueryExecutorModel queryModel) {
        this.queryResultPreparator = new QueryResultPreparator(executerProperties, queryModel);
        this.numberOfCores = infos.get(0).getNumberOfRecordsInMemory() / Integer.parseInt(
                MolapProperties.getInstance().getProperty(MolapCommonConstants.LEAFNODE_SIZE,
                        MolapCommonConstants.LEAFNODE_SIZE_DEFAULT_VAL));
        if (numberOfCores == 0) {
            numberOfCores++;
        }
        executor = new ColumnarDetailQueryParallelSliceExecutor(infos.get(infos.size() - 1),
                (int) numberOfCores);
        this.infos = infos;
        this.sliceIndexToBeExecuted = new int[(int) numberOfCores];
        intialiseInfos();
    }

    private void intialiseInfos() {
        this.totalNumberOfNodesPerSlice = new long[infos.size()];
        this.numberOfExecutedNodesPerSlice = new long[infos.size()];
        int index = -1;
        for (SliceExecutionInfo info : infos) {
            ++index;
            if (!info.isExecutionRequired()) {
                continue;
            }
            try {
                DataStoreBlock startNode = info.getSlice().getDataCache(info.getTableName())
                        .getDataStoreBlock(info.getKeyGenerator().generateKey(info.getStartKey()),
                                null, true);
                DataStoreBlock lastNode = info.getSlice().getDataCache(info.getTableName())
                        .getDataStoreBlock(info.getKeyGenerator().generateKey(info.getEndKey()),
                                null, false);
                this.totalNumberOfNodesPerSlice[index] =
                        lastNode.getNodeNumber() - startNode.getNodeNumber() + 1;
                totalNumberOfNode += this.totalNumberOfNodesPerSlice[index];
                info.setStartNode(startNode);
                info.setNumberOfNodeToScan(1);
            } catch (KeyGenException e) {
                LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, e, e.getMessage());
            }
        }

    }

    @Override
    public boolean hasNext() {
        return currentCounter < totalNumberOfNode;
    }

    @Override
    public ChunkResult next() {
        updateSliceIndexToBeExecuted();
        MolapIterator<QueryResult> executeSlices = null;
        try {
            executeSlices = executor.executeSlices(infos, sliceIndexToBeExecuted);
        } catch (QueryExecutionException e) {
            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, e, e.getMessage());
        }
        for (int i = 0; i < sliceIndexToBeExecuted.length; i++) {
            if (sliceIndexToBeExecuted[i] != -1) {
                if (infos.get(sliceIndexToBeExecuted[i]).isExecutionRequired()) {
                    infos.get(sliceIndexToBeExecuted[i]).setStartNode(
                            infos.get(sliceIndexToBeExecuted[i]).getStartNode().getNext());
                }
            }
        }
        if (null != executeSlices) {
            QueryResult next = executeSlices.next();
            if (next.size() > 0) {
                return queryResultPreparator.prepareQueryOutputResult(next);
            } else {
                return new ChunkResult();
            }
        } else {
            return new ChunkResult();
        }
    }

    private void updateSliceIndexToBeExecuted() {
        Arrays.fill(sliceIndexToBeExecuted, -1);
        int currentSliceIndex = 0;
        int i = 0;
        for (; i < (int) numberOfCores; ) {
            if (this.totalNumberOfNodesPerSlice[currentSliceIndex]
                    > this.numberOfExecutedNodesPerSlice[currentSliceIndex]) {
                this.numberOfExecutedNodesPerSlice[currentSliceIndex]++;
                sliceIndexToBeExecuted[i] = currentSliceIndex;
                i++;
            }
            currentSliceIndex++;
            if (currentSliceIndex >= totalNumberOfNodesPerSlice.length) {
                break;
            }
        }
        currentCounter += i;
    }
}
