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
package org.carbondata.query.carbon.merger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.iterator.CarbonIterator;
import org.carbondata.query.carbon.executor.exception.QueryExecutionException;
import org.carbondata.query.carbon.executor.infos.BlockExecutionInfo;
import org.carbondata.query.carbon.result.Result;
import org.carbondata.query.carbon.result.impl.ListBasedResult;
import org.carbondata.query.carbon.result.impl.MapBasedResult;
import org.carbondata.query.carbon.result.iterator.MemoryBasedResultIterator;

/**
 * Class which processed the scanned result
 * Processing can be merging sorting
 */
public abstract class AbstractScannedResultMerger implements ScannedResultMerger {

  /**
   * LOGGER
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(AbstractScannedResultMerger.class.getName());
  /**
   * merging will done using thread
   */
  protected ExecutorService execService;

  /**
   * Merged scanned result which will merge all the result from all the blocks
   * executor
   */
  protected Result mergedScannedResult;

  /**
   * scanned result list
   */
  protected List<Result> scannedResultList;

  /**
   * tableBlockExecutionInfo
   */
  protected BlockExecutionInfo blockExecutionInfo;

  /**
   * max number of scanned result can keep in memory
   */
  private int maxNumberOfScannedResultList;

  /**
   * lockObject
   */
  private Object lockObject;

  public AbstractScannedResultMerger(BlockExecutionInfo blockExecutionInfo,
      int maxNumberOfScannedresultList) {

    this.lockObject = new Object();
    this.maxNumberOfScannedResultList = maxNumberOfScannedresultList;
    execService = Executors.newFixedThreadPool(1);
    scannedResultList = new ArrayList<Result>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    this.blockExecutionInfo = blockExecutionInfo;
    initialiseResult();
  }

  /**
   * for initializing the map based or list based result.
   */
  protected void initialiseResult() {
    if (!blockExecutionInfo.isDetailQuery() && !blockExecutionInfo.isRawRecordDetailQuery()) {
      mergedScannedResult = new MapBasedResult();
    } else {
      mergedScannedResult = new ListBasedResult();
    }
  }

  /**
   * Below method will be used to add the scanned result
   * If number of scanned result in the list of more than
   * the maxNumberOfScannedResultList than results present in the
   * list will be merged to merged scanned result
   *
   * @param scannedResult
   */
  @Override public void addScannedResult(Result scannedResult) throws QueryExecutionException {
    synchronized (this.lockObject) {
      scannedResultList.add(scannedResult);
      if ((scannedResultList.size() > maxNumberOfScannedResultList)) {
        List<Result> localResult = scannedResultList;
        scannedResultList = new ArrayList<Result>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
        execService.submit(new MergerThread(localResult));
      }
    }
  }

  /**
   * Below method will be used to merge the scanned result
   *
   * @param scannedResultList scanned result list
   */
  protected void mergeScannedResults(List<Result> scannedResultList) {
    long start = System.currentTimeMillis();
    LOGGER.debug("Started a slice result merging");

    for (int i = 0; i < scannedResultList.size(); i++) {
      mergedScannedResult.merge(scannedResultList.get(i));
    }
    LOGGER.debug("Finished current slice result merging in time (MS) " + (System.currentTimeMillis()
            - start));
  }

  /**
   * Below method will be used to get the final query
   * return
   *
   * @return iterator over result
   */
  @Override public CarbonIterator<Result> getQueryResultIterator() throws QueryExecutionException {
    execService.shutdown();
    try {
      execService.awaitTermination(1, TimeUnit.DAYS);
    } catch (InterruptedException e1) {
      LOGGER.error("Problem in thread termination" + e1.getMessage());
    }
    if (scannedResultList.size() > 0) {
      mergeScannedResults(scannedResultList);
      scannedResultList = null;
    }
    LOGGER.debug("Finished result merging from all slices");
    return new MemoryBasedResultIterator(mergedScannedResult);
  }

  /**
   * Thread class to merge the scanned result
   */
  private final class MergerThread implements Callable<Void> {
    private List<Result> scannedResult;

    private MergerThread(List<Result> scannedResult) {
      this.scannedResult = scannedResult;
    }

    @Override public Void call() throws Exception {
      mergeScannedResults(scannedResult);
      return null;
    }
  }
}