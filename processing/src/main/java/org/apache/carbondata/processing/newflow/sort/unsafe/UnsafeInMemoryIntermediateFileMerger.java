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
package org.apache.carbondata.processing.newflow.sort.unsafe;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.processing.sortandgroupby.exception.CarbonSortKeyAndGroupByException;
import org.apache.carbondata.processing.sortandgroupby.sortdata.SortParameters;

/**
 * It does mergesort intermediate files to big file.
 */
public class UnsafeInMemoryIntermediateFileMerger {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(UnsafeInMemoryIntermediateFileMerger.class.getName());

  /**
   * executorService
   */
  private ExecutorService executorService;
  /**
   * rowPages
   */
  private List<UnsafeCarbonRowPage> rowPages;

  private List<UnsafeCarbonRowPage> mergedPages;

  private SortParameters parameters;

  private final Object lockObject = new Object();

  private boolean offHeap;

  public UnsafeInMemoryIntermediateFileMerger(SortParameters parameters) {
    this.parameters = parameters;
    // processed file list
    this.rowPages = new ArrayList<UnsafeCarbonRowPage>(CarbonCommonConstants.CONSTANT_SIZE_TEN);
    this.mergedPages = new ArrayList<>();
    this.executorService = Executors.newFixedThreadPool(parameters.getNumberOfCores());
    this.offHeap = Boolean.parseBoolean(CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.ENABLE_OFFHEAP_SORT,
            CarbonCommonConstants.ENABLE_OFFHEAP_SORT_DEFAULT));
  }

  public void addDataChunkToMerge(UnsafeCarbonRowPage rowPage) {
    // add sort temp filename to and arrayList. When the list size reaches 20 then
    // intermediate merging of sort temp files will be triggered
    synchronized (lockObject) {
      rowPages.add(rowPage);
    }
  }

  public void startMergingIfPossible() {
    UnsafeCarbonRowPage[] localRowPages;
    if (rowPages.size() >= parameters.getNumberOfIntermediateFileToBeMerged()) {
      synchronized (lockObject) {
        localRowPages = rowPages.toArray(new UnsafeCarbonRowPage[rowPages.size()]);
        this.rowPages = new ArrayList<UnsafeCarbonRowPage>();
      }
      LOGGER
          .debug("Sumitting request for intermediate merging no of files: " + localRowPages.length);
      startIntermediateMerging(localRowPages);
    }
  }

  /**
   * Below method will be used to start the intermediate file merging
   *
   * @param rowPages
   */
  private void startIntermediateMerging(UnsafeCarbonRowPage[] rowPages) {
    UnsafeCarbonRowPage outRowPage = createOutUnsafeCarbonRowPage(rowPages);
    mergedPages.add(outRowPage);
    UnsafeIntermediateDataMerger merger =
        new UnsafeIntermediateDataMerger(parameters, rowPages, outRowPage);
    executorService.submit(merger);
  }

  private UnsafeCarbonRowPage createOutUnsafeCarbonRowPage(
      UnsafeCarbonRowPage[] unsafeCarbonRowPages) {
    int totalSize = 0;
    for (int i = 0; i < unsafeCarbonRowPages.length; i++) {
      totalSize += unsafeCarbonRowPages[i].getBuffer().getTotalSize();
    }
    return new UnsafeCarbonRowPage(unsafeCarbonRowPages[0].getNoDictionaryDimensionMapping(),
        parameters.getDimColCount(), parameters.getMeasureColCount(), parameters.getAggType(),
        totalSize, offHeap);
  }

  public void finish() throws CarbonSortKeyAndGroupByException {
    try {
      executorService.shutdown();
      executorService.awaitTermination(2, TimeUnit.DAYS);
    } catch (InterruptedException e) {
      throw new CarbonSortKeyAndGroupByException("Problem while shutdown the server ", e);
    }
  }

  public void close() {
    if (executorService.isShutdown()) {
      executorService.shutdownNow();
    }
    rowPages.clear();
    rowPages = null;
  }

  public List<UnsafeCarbonRowPage> getRowPages() {
    return rowPages;
  }

  public List<UnsafeCarbonRowPage> getMergedPages() {
    return mergedPages;
  }
}
