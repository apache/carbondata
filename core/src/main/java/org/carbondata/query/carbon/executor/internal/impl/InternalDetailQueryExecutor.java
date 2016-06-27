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
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.iterator.CarbonIterator;
import org.carbondata.core.util.CarbonProperties;
import org.carbondata.query.carbon.executor.exception.QueryExecutionException;
import org.carbondata.query.carbon.executor.infos.BlockExecutionInfo;
import org.carbondata.query.carbon.executor.internal.InternalQueryExecutor;
import org.carbondata.query.carbon.merger.ScannedResultMerger;
import org.carbondata.query.carbon.merger.impl.UnSortedScannedResultMerger;
import org.carbondata.query.carbon.result.Result;

/**
 * Below Class will be used to execute the detail query
 */
public class InternalDetailQueryExecutor implements InternalQueryExecutor {

  /**
   * LOGGER.
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(InternalDetailQueryExecutor.class.getName());

  /**
   * number of cores can be used to execute the query
   */
  private int numberOfCores;

  public InternalDetailQueryExecutor() {

    // below code will be used to update the number of cores based on number
    // records we
    // can keep in memory while executing the query execution
    int recordSize = 0;
    String defaultInMemoryRecordsSize =
        CarbonProperties.getInstance().getProperty(CarbonCommonConstants.INMEMORY_REOCRD_SIZE);
    if (null != defaultInMemoryRecordsSize) {
      try {
        recordSize = Integer.parseInt(defaultInMemoryRecordsSize);
      } catch (NumberFormatException ne) {
        LOGGER.error("Invalid inmemory records size. Using default value");
        recordSize = CarbonCommonConstants.INMEMORY_REOCRD_SIZE_DEFAULT;
      }
    }
    this.numberOfCores = recordSize / Integer.parseInt(CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.BLOCKLET_SIZE,
            CarbonCommonConstants.BLOCKLET_SIZE_DEFAULT_VAL));
    if (numberOfCores == 0) {
      numberOfCores++;
    }
  }

  /**
   * Below method will be used to used to execute the detail query
   * and it will return iterator over result
   *
   * @param executionInfos block execution info which will have all the properties
   *                       required for query execution
   * @param sliceIndexes   slice indexes to be executed
   * @return query result
   */
  @Override public CarbonIterator<Result> executeQuery(List<BlockExecutionInfo> executionInfos,
      int[] sliceIndexes) throws QueryExecutionException {
    long startTime = System.currentTimeMillis();
    QueryRunner task = null;
    ScannedResultMerger scannedResultProcessor =
        new UnSortedScannedResultMerger(executionInfos.get(executionInfos.size() - 1),
            sliceIndexes.length);
    ExecutorService execService = Executors.newFixedThreadPool(numberOfCores);
    List<Future> listFutureObjects = new ArrayList<Future>();
    try {
      for (int currentSliceIndex : sliceIndexes) {
        if (currentSliceIndex == -1) {
          continue;
        }
        executionInfos.get(currentSliceIndex).setScannedResultProcessor(scannedResultProcessor);
        task = new QueryRunner(executionInfos.get(currentSliceIndex));
        listFutureObjects.add(execService.submit(task));
      }
      execService.shutdown();
      execService.awaitTermination(2, TimeUnit.DAYS);
      LOGGER.info("Total time taken for scan " + (System.currentTimeMillis() - startTime));
      for (Future future : listFutureObjects) {
        try {
          future.get();
        } catch (ExecutionException e) {
          throw new QueryExecutionException(e.getMessage());
        }
      }
      return scannedResultProcessor.getQueryResultIterator();
    } catch (QueryExecutionException exception) {
      throw new QueryExecutionException(exception);
    } catch (InterruptedException e) {
      LOGGER.error(e, e.getMessage());
      throw new QueryExecutionException(e);
    } finally {
      execService = null;
      for (int currentSliceIndex : sliceIndexes) {
        if (currentSliceIndex == -1) {
          continue;
        }
        executionInfos.get(currentSliceIndex).setScannedResultProcessor(null);
      }
    }
  }

}
