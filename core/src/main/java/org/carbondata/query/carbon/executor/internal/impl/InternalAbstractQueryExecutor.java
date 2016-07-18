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
package org.carbondata.query.carbon.executor.internal.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.carbon.datastore.DataRefNode;
import org.carbondata.core.carbon.datastore.DataRefNodeFinder;
import org.carbondata.core.carbon.datastore.impl.btree.BTreeDataRefNodeFinder;
import org.carbondata.core.carbon.querystatistics.QueryStatistic;
import org.carbondata.core.iterator.CarbonIterator;
import org.carbondata.query.carbon.executor.exception.QueryExecutionException;
import org.carbondata.query.carbon.executor.infos.BlockExecutionInfo;
import org.carbondata.query.carbon.executor.internal.InternalQueryExecutor;
import org.carbondata.query.carbon.merger.ScannedResultMerger;
import org.carbondata.query.carbon.merger.impl.SortedScannedResultMerger;
import org.carbondata.query.carbon.merger.impl.UnSortedScannedResultMerger;
import org.carbondata.query.carbon.result.Result;

/**
 * Query executor class which will have common implementation
 * among all type of executor
 */
public abstract class InternalAbstractQueryExecutor implements InternalQueryExecutor {

  /**
   * LOGGER.
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(InternalAbstractQueryExecutor.class.getName());

  /**
   * Executor Service.
   */
  protected ExecutorService execService;

  /**
   * number of cores to be used
   */
  protected int numberOfCores;

  /**
   * Below method will be used to used to execute the detail query
   * and it will return iterator over result
   *
   * @param executionInfos block execution info which will have all the properties
   *                       required for query execution
   * @param sliceIndexes   slice indexes to be executed in this case it w
   * @return query result
   */
  @Override public CarbonIterator<Result> executeQuery(
      List<BlockExecutionInfo> tableBlockExecutionInfosList, int[] sliceIndex)
      throws QueryExecutionException {

    long startTime = System.currentTimeMillis();
    BlockExecutionInfo latestInfo =
        tableBlockExecutionInfosList.get(tableBlockExecutionInfosList.size() - 1);
    execService = Executors.newFixedThreadPool(numberOfCores);

    QueryStatistic statistic = new QueryStatistic();

    ScannedResultMerger scannedResultProcessor = null;
    if (null != latestInfo.getSortInfo()
        && latestInfo.getSortInfo().getSortDimensionIndex().length > 0) {
      scannedResultProcessor = new SortedScannedResultMerger(latestInfo, numberOfCores);
    } else {
      scannedResultProcessor = new UnSortedScannedResultMerger(latestInfo, numberOfCores);
    }
    try {
      List<Future> listFutureObjects = new ArrayList<Future>();
      for (BlockExecutionInfo blockInfo : tableBlockExecutionInfosList) {
        DataRefNodeFinder finder = new BTreeDataRefNodeFinder(blockInfo.getEachColumnValueSize());
        DataRefNode startDataBlock = finder
            .findFirstDataBlock(blockInfo.getDataBlock().getDataRefNode(), blockInfo.getStartKey());
        DataRefNode endDataBlock = finder
            .findLastDataBlock(blockInfo.getDataBlock().getDataRefNode(), blockInfo.getEndKey());
        long numberOfBlockToScan = endDataBlock.nodeNumber() - startDataBlock.nodeNumber() + 1;
        blockInfo.setFirstDataBlock(startDataBlock);
        blockInfo.setNumberOfBlockToScan(numberOfBlockToScan);
        blockInfo.setScannedResultProcessor(scannedResultProcessor);
        listFutureObjects.add(execService.submit(new QueryRunner(blockInfo)));
      }
      execService.shutdown();
      execService.awaitTermination(2, TimeUnit.DAYS);
      for (Future future : listFutureObjects) {
        try {
          future.get();
        } catch (ExecutionException e) {
          throw new QueryExecutionException(e.getMessage());
        }
      }
      CarbonIterator<Result> queryResultIterator = scannedResultProcessor.getQueryResultIterator();
      statistic
          .addStatistics("Time taken to scan " + tableBlockExecutionInfosList.size() + " block(s) ",
              System.currentTimeMillis());
      latestInfo.getStatisticsRecorder().recordStatistics(statistic);
      return queryResultIterator;
    } catch (QueryExecutionException e) {
      LOGGER.error(e, e.getMessage());
      throw new QueryExecutionException(e);
    } catch (InterruptedException e) {
      LOGGER.error(e, e.getMessage());
      throw new QueryExecutionException(e);
    } finally {
      execService = null;
      latestInfo = null;
    }
  }

}
