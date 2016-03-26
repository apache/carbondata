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

package org.carbondata.query.executer.impl;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.constants.MolapCommonConstants;
import org.carbondata.core.iterator.MolapIterator;
import org.carbondata.core.keygenerator.KeyGenException;
import org.carbondata.core.util.MolapProperties;
import org.carbondata.query.datastorage.storeInterfaces.DataStoreBlock;
import org.carbondata.query.executer.SliceExecuter;
import org.carbondata.query.executer.exception.QueryExecutionException;
import org.carbondata.query.executer.pagination.impl.QueryResult;
import org.carbondata.query.executer.processor.ScannedResultProcessor;
import org.carbondata.query.executer.processor.ScannedResultProcessorImpl;
import org.carbondata.query.querystats.PartitionDetail;
import org.carbondata.query.querystats.PartitionStatsCollector;
import org.carbondata.query.schema.metadata.SliceExecutionInfo;
import org.carbondata.query.util.MolapEngineLogEvent;

public class ColumnarParallelSliceExecutor implements SliceExecuter {

    /**
     * LOGGER.
     */
    private static final LogService LOGGER =
            LogServiceFactory.getLogService(ParallelSliceExecutorImpl.class.getName());

    /**
     * Executor Service.
     */
    private ExecutorService execService;

    @Override
    public MolapIterator<QueryResult> executeSlices(List<SliceExecutionInfo> infos,
            int[] sliceIndex) throws QueryExecutionException {
        ColumnarSliceExecuter task = null;
        SliceExecutionInfo latestInfo = infos.get(infos.size() - 1);
        long startTime = System.currentTimeMillis();
        int numberOfCores = 0;
        try {
            numberOfCores = Integer.parseInt(MolapProperties.getInstance()
                    .getProperty(MolapCommonConstants.NUM_CORES,
                            MolapCommonConstants.NUM_CORES_DEFAULT_VAL));
        } catch (NumberFormatException e) {
            numberOfCores = 1;
        }

        if (latestInfo.isDetailQuery()) {
            int numberOfRecordsInMemory = latestInfo.getNumberOfRecordsInMemory();
            numberOfCores = numberOfRecordsInMemory / Integer.parseInt(MolapProperties.getInstance()
                    .getProperty(MolapCommonConstants.LEAFNODE_SIZE,
                            MolapCommonConstants.LEAFNODE_SIZE_DEFAULT_VAL));
            if (numberOfCores < 1) {
                numberOfCores = 1;
            }
        }
        execService = Executors.newFixedThreadPool(numberOfCores);
        ScannedResultProcessor scannedResultProcessor = new ScannedResultProcessorImpl(latestInfo);
        try {
            for (SliceExecutionInfo info : infos) {
                if (!info.isExecutionRequired()) {
                    continue;
                }
                DataStoreBlock startNode = info.getSlice().getDataCache(info.getTableName())
                        .getDataStoreBlock(info.getKeyGenerator().generateKey(info.getStartKey()),
                                null, true);

                DataStoreBlock lastNode = info.getSlice().getDataCache(info.getTableName())
                        .getDataStoreBlock(info.getKeyGenerator().generateKey(info.getEndKey()),
                                null, false);
                long numberOfNodesToScan = lastNode.getNodeNumber() - startNode.getNodeNumber() + 1;

                //Add this information to QueryDetail
                // queryDetail will be there only when user do <dataframe>.collect
                //TO-DO need to check for all queries
                PartitionStatsCollector partitionStatsCollector =
                        PartitionStatsCollector.getInstance();
                PartitionDetail partitionDetail =
                        partitionStatsCollector.getPartionDetail(info.getQueryId());
                if (null != partitionDetail) {
                    partitionDetail.addNumberOfNodesScanned(numberOfNodesToScan);
                }
                task = new ColumnarSliceExecuter(info, scannedResultProcessor, startNode,
                        numberOfNodesToScan);
                execService.submit(task);
            }
            execService.shutdown();
            execService.awaitTermination(2, TimeUnit.DAYS);
            LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,
                    "Total time taken for scan " + (System.currentTimeMillis() - startTime));
            return scannedResultProcessor.getQueryResultIterator();
        } catch (QueryExecutionException e) {
            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, e, e.getMessage());
            throw new QueryExecutionException(e);
        } catch (InterruptedException e) {
            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, e, e.getMessage());
            throw new QueryExecutionException(e);
        } catch (KeyGenException e) {
            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, e, e.getMessage());
            throw new QueryExecutionException(e);
        } finally {
            execService = null;
            latestInfo = null;
            latestInfo = null;
        }
    }

}
