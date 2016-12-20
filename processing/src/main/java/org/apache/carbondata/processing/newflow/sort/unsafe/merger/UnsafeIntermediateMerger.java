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
package org.apache.carbondata.processing.newflow.sort.unsafe.merger;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.processing.newflow.sort.unsafe.UnsafeCarbonRowPage;
import org.apache.carbondata.processing.sortandgroupby.exception.CarbonSortKeyAndGroupByException;
import org.apache.carbondata.processing.sortandgroupby.sortdata.SortParameters;

/**
 * It does mergesort intermediate files to big file.
 */
public class UnsafeIntermediateMerger {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(UnsafeIntermediateMerger.class.getName());

  /**
   * executorService
   */
  private ExecutorService executorService;
  /**
   * rowPages
   */
  private List<UnsafeCarbonRowPage> rowPages;

  private List<UnsafeInMemoryIntermediateDataMerger> mergedPages;

  private SortParameters parameters;

  private final Object lockObject = new Object();

  private boolean offHeap;

  private List<File> procFiles;

  public UnsafeIntermediateMerger(SortParameters parameters) {
    this.parameters = parameters;
    // processed file list
    this.rowPages = new ArrayList<UnsafeCarbonRowPage>(CarbonCommonConstants.CONSTANT_SIZE_TEN);
    this.mergedPages = new ArrayList<>();
    this.executorService = Executors.newFixedThreadPool(parameters.getNumberOfCores());
    this.offHeap = Boolean.parseBoolean(CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.ENABLE_OFFHEAP_SORT,
            CarbonCommonConstants.ENABLE_OFFHEAP_SORT_DEFAULT));
    this.procFiles = new ArrayList<File>(CarbonCommonConstants.CONSTANT_SIZE_TEN);
  }

  public void addDataChunkToMerge(UnsafeCarbonRowPage rowPage) {
    // add sort temp filename to and arrayList. When the list size reaches 20 then
    // intermediate merging of sort temp files will be triggered
    synchronized (lockObject) {
      rowPages.add(rowPage);
    }
  }

  public void addFileToMerge(File sortTempFile) {
    // add sort temp filename to and arrayList. When the list size reaches 20 then
    // intermediate merging of sort temp files will be triggered
    synchronized (lockObject) {
      procFiles.add(sortTempFile);
    }
  }

  public void startFileMergingIfPossible() {
    File[] fileList;
    if (procFiles.size() >= parameters.getNumberOfIntermediateFileToBeMerged()) {
      synchronized (lockObject) {
        fileList = procFiles.toArray(new File[procFiles.size()]);
        this.procFiles = new ArrayList<File>();
      }
      LOGGER.debug("Sumitting request for intermediate merging no of files: " + fileList.length);
      startIntermediateMerging(fileList);
    }
  }

  /**
   * Below method will be used to start the intermediate file merging
   *
   * @param intermediateFiles
   */
  private void startIntermediateMerging(File[] intermediateFiles) {
    File file = new File(
        parameters.getTempFileLocation() + File.separator + parameters.getTableName() + System
            .nanoTime() + CarbonCommonConstants.MERGERD_EXTENSION);
    UnsafeIntermediateFileMerger merger =
        new UnsafeIntermediateFileMerger(parameters, intermediateFiles, file);
    executorService.submit(merger);
  }

  public void startInmemoryMergingIfPossible() throws CarbonSortKeyAndGroupByException {
    UnsafeCarbonRowPage[] localRowPages;
    if (rowPages.size() >= parameters.getNumberOfIntermediateFileToBeMerged()) {
      int totalRows = 0;
      synchronized (lockObject) {
        totalRows = getTotalNumberOfRows(rowPages);
        if (totalRows <= 0) {
          return;
        }
        localRowPages = rowPages.toArray(new UnsafeCarbonRowPage[rowPages.size()]);
        this.rowPages = new ArrayList<>();
      }
      LOGGER.debug("Sumitting request for intermediate merging of in-memory pages : "
          + localRowPages.length);
      startIntermediateMerging(localRowPages, totalRows);
    }
  }

  /**
   * Below method will be used to start the intermediate file merging
   *
   * @param rowPages
   */
  private void startIntermediateMerging(UnsafeCarbonRowPage[] rowPages, int totalRows)
      throws CarbonSortKeyAndGroupByException {
    UnsafeInMemoryIntermediateDataMerger merger =
        new UnsafeInMemoryIntermediateDataMerger(rowPages, totalRows);
    mergedPages.add(merger);
    executorService.submit(merger);
  }

  private int getTotalNumberOfRows(List<UnsafeCarbonRowPage> unsafeCarbonRowPages) {
    int totalSize = 0;
    for (UnsafeCarbonRowPage unsafeCarbonRowPage : unsafeCarbonRowPages) {
      totalSize += unsafeCarbonRowPage.getBuffer().getActualSize();
    }
    return totalSize;
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

  public List<UnsafeInMemoryIntermediateDataMerger> getMergedPages() {
    return mergedPages;
  }
}
