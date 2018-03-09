/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.carbondata.processing.loading.sort.impl;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.row.CarbonRow;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.metadata.schema.ColumnRangeInfo;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonTimeStatisticsFactory;
import org.apache.carbondata.processing.loading.exception.CarbonDataLoadingException;
import org.apache.carbondata.processing.loading.row.CarbonRowBatch;
import org.apache.carbondata.processing.loading.sort.AbstractMergeSorter;
import org.apache.carbondata.processing.loading.sort.unsafe.UnsafeCarbonRowPage;
import org.apache.carbondata.processing.loading.sort.unsafe.UnsafeSortDataRows;
import org.apache.carbondata.processing.loading.sort.unsafe.merger.UnsafeIntermediateMerger;
import org.apache.carbondata.processing.loading.sort.unsafe.merger.UnsafeSingleThreadFinalSortFilesMerger;
import org.apache.carbondata.processing.sort.sortdata.SortParameters;
import org.apache.carbondata.processing.util.CarbonDataProcessorUtil;

import org.apache.commons.lang3.StringUtils;

/**
 * It parallely reads data from array of iterates and do merge sort.
 * First it sorts the data and write to temp files. These temp files will be merge sorted to get
 * final merge sort result.
 * This step is specifically for the data loading with specifying column value range, such as
 * bucketing, sort_column_bounds, it sorts each range of data separately and write to temp files.
 */
public class UnsafeParallelReadMergeSorterWithColumnRangeImpl extends AbstractMergeSorter {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(
          UnsafeParallelReadMergeSorterWithColumnRangeImpl.class.getName());

  private SortParameters originSortParameters;
  private UnsafeIntermediateMerger[] intermediateFileMergers;
  private int inMemoryChunkSizeInMB;
  private AtomicLong rowCounter;
  private ColumnRangeInfo columnRangeInfo;
  /**
   * counters to collect information about rows processed by each range
   */
  private List<AtomicLong> insideRowCounterList;

  public UnsafeParallelReadMergeSorterWithColumnRangeImpl(AtomicLong rowCounter,
      ColumnRangeInfo columnRangeInfo) {
    this.rowCounter = rowCounter;
    this.columnRangeInfo = columnRangeInfo;
  }

  @Override public void initialize(SortParameters sortParameters) {
    this.originSortParameters = sortParameters;
    int totalInMemoryChunkSizeInMB = CarbonProperties.getInstance().getSortMemoryChunkSizeInMB();
    inMemoryChunkSizeInMB = totalInMemoryChunkSizeInMB / columnRangeInfo.getNumOfRanges();
    if (inMemoryChunkSizeInMB < 5) {
      inMemoryChunkSizeInMB = 5;
    }
    this.insideRowCounterList = new ArrayList<>(columnRangeInfo.getNumOfRanges());
    for (int i = 0; i < columnRangeInfo.getNumOfRanges(); i++) {
      insideRowCounterList.add(new AtomicLong(0));
    }
  }

  @Override public Iterator<CarbonRowBatch>[] sort(Iterator<CarbonRowBatch>[] iterators)
      throws CarbonDataLoadingException {
    UnsafeSortDataRows[] sortDataRows = new UnsafeSortDataRows[columnRangeInfo.getNumOfRanges()];
    intermediateFileMergers = new UnsafeIntermediateMerger[columnRangeInfo.getNumOfRanges()];
    SortParameters[] sortParameterArray = new SortParameters[columnRangeInfo.getNumOfRanges()];
    try {
      for (int i = 0; i < columnRangeInfo.getNumOfRanges(); i++) {
        SortParameters parameters = originSortParameters.getCopy();
        parameters.setPartitionID(i + "");
        parameters.setRangeId(i);
        sortParameterArray[i] = parameters;
        setTempLocation(parameters);
        intermediateFileMergers[i] = new UnsafeIntermediateMerger(parameters);
        sortDataRows[i] =
            new UnsafeSortDataRows(parameters, intermediateFileMergers[i], inMemoryChunkSizeInMB);
        sortDataRows[i].initialize();
      }
    } catch (MemoryException e) {
      throw new CarbonDataLoadingException(e);
    }
    ExecutorService executorService = Executors.newFixedThreadPool(iterators.length);
    this.threadStatusObserver = new ThreadStatusObserver(executorService);
    final int batchSize = CarbonProperties.getInstance().getBatchSize();
    try {
      for (int i = 0; i < iterators.length; i++) {
        executorService.execute(new SortIteratorThread(iterators[i], sortDataRows, rowCounter,
            this.insideRowCounterList, this.threadStatusObserver));
      }
      executorService.shutdown();
      executorService.awaitTermination(2, TimeUnit.DAYS);
      processRowToNextStep(sortDataRows, originSortParameters);
    } catch (Exception e) {
      checkError();
      throw new CarbonDataLoadingException("Problem while shutdown the server ", e);
    }
    checkError();
    try {
      for (int i = 0; i < intermediateFileMergers.length; i++) {
        intermediateFileMergers[i].finish();
      }
    } catch (Exception e) {
      throw new CarbonDataLoadingException(e);
    }

    Iterator<CarbonRowBatch>[] batchIterator = new Iterator[columnRangeInfo.getNumOfRanges()];
    for (int i = 0; i < sortDataRows.length; i++) {
      batchIterator[i] =
          new MergedDataIterator(sortParameterArray[i], batchSize, intermediateFileMergers[i]);
    }

    return batchIterator;
  }

  private UnsafeSingleThreadFinalSortFilesMerger getFinalMerger(SortParameters sortParameters) {
    String[] storeLocation = CarbonDataProcessorUtil
        .getLocalDataFolderLocation(sortParameters.getDatabaseName(), sortParameters.getTableName(),
            String.valueOf(sortParameters.getTaskNo()),
            sortParameters.getSegmentId() + "", false, false);
    // Set the data file location
    String[] dataFolderLocation = CarbonDataProcessorUtil.arrayAppend(storeLocation,
        File.separator, CarbonCommonConstants.SORT_TEMP_FILE_LOCATION);
    return new UnsafeSingleThreadFinalSortFilesMerger(sortParameters, dataFolderLocation);
  }

  @Override public void close() {
    for (int i = 0; i < intermediateFileMergers.length; i++) {
      intermediateFileMergers[i].close();
    }
  }

  /**
   * Below method will be used to process data to next step
   */
  private boolean processRowToNextStep(UnsafeSortDataRows[] sortDataRows, SortParameters parameters)
      throws CarbonDataLoadingException {
    if (null == sortDataRows || sortDataRows.length == 0) {
      LOGGER.info("Record Processed For table: " + parameters.getTableName());
      LOGGER.info("Number of Records was Zero");
      String logMessage = "Summary: Carbon Sort Key Step: Read: " + 0 + ": Write: " + 0;
      LOGGER.info(logMessage);
      return false;
    }

    try {
      for (int i = 0; i < sortDataRows.length; i++) {
        // start sorting
        sortDataRows[i].startSorting();
      }
      // check any more rows are present
      LOGGER.info("Record Processed For table: " + parameters.getTableName());
      CarbonTimeStatisticsFactory.getLoadStatisticsInstance()
          .recordSortRowsStepTotalTime(parameters.getPartitionID(), System.currentTimeMillis());
      CarbonTimeStatisticsFactory.getLoadStatisticsInstance()
          .recordDictionaryValuesTotalTime(parameters.getPartitionID(), System.currentTimeMillis());
      return false;
    } catch (Exception e) {
      throw new CarbonDataLoadingException(e);
    }
  }

  private void setTempLocation(SortParameters parameters) {
    String[] carbonDataDirectoryPath = CarbonDataProcessorUtil
        .getLocalDataFolderLocation(parameters.getDatabaseName(), parameters.getTableName(),
            parameters.getTaskNo(), parameters.getSegmentId(),
            false, false);
    String[] tmpLoc = CarbonDataProcessorUtil.arrayAppend(carbonDataDirectoryPath, File.separator,
        CarbonCommonConstants.SORT_TEMP_FILE_LOCATION);
    LOGGER.error("set temp location: " + StringUtils.join(tmpLoc, ", "));
    parameters.setTempFileLocation(tmpLoc);
  }

  /**
   * This thread iterates the iterator and adds the rows to @{@link UnsafeSortDataRows}
   */
  private static class SortIteratorThread implements Runnable {

    private Iterator<CarbonRowBatch> iterator;

    private UnsafeSortDataRows[] sortDataRows;
    private AtomicLong rowCounter;
    private List<AtomicLong> insideRowCounterList;
    private ThreadStatusObserver threadStatusObserver;

    public SortIteratorThread(Iterator<CarbonRowBatch> iterator,
        UnsafeSortDataRows[] sortDataRows, AtomicLong rowCounter,
        List<AtomicLong> insideRowCounterList,
        ThreadStatusObserver threadStatusObserver) {
      this.iterator = iterator;
      this.sortDataRows = sortDataRows;
      this.rowCounter = rowCounter;
      this.insideRowCounterList = insideRowCounterList;
      this.threadStatusObserver = threadStatusObserver;
    }

    @Override
    public void run() {
      try {
        while (iterator.hasNext()) {
          CarbonRowBatch batch = iterator.next();
          int i = 0;
          while (batch.hasNext()) {
            CarbonRow row = batch.next();
            if (row != null) {
              UnsafeSortDataRows sortDataRow = sortDataRows[row.getRangeId()];
              synchronized (sortDataRow) {
                rowCounter.getAndIncrement();
                insideRowCounterList.get(row.getRangeId()).getAndIncrement();
                sortDataRow.addRow(row.getData());
              }
            }
          }
        }
        LOGGER.info("Rows processed by each range: " + insideRowCounterList);
      } catch (Exception e) {
        LOGGER.error(e);
        this.threadStatusObserver.notifyFailed(e);
      }
    }
  }

  private class MergedDataIterator extends CarbonIterator<CarbonRowBatch> {

    private SortParameters sortParameters;

    private int batchSize;

    private boolean firstRow;

    private UnsafeIntermediateMerger intermediateMerger;

    public MergedDataIterator(SortParameters sortParameters, int batchSize,
        UnsafeIntermediateMerger intermediateMerger) {
      this.sortParameters = sortParameters;
      this.batchSize = batchSize;
      this.intermediateMerger = intermediateMerger;
      this.firstRow = true;
    }

    private UnsafeSingleThreadFinalSortFilesMerger finalMerger;

    @Override public boolean hasNext() {
      if (firstRow) {
        firstRow = false;
        finalMerger = getFinalMerger(sortParameters);
        List<UnsafeCarbonRowPage> rowPages = intermediateMerger.getRowPages();
        finalMerger.startFinalMerge(rowPages.toArray(new UnsafeCarbonRowPage[rowPages.size()]),
            intermediateMerger.getMergedPages());
      }
      return finalMerger.hasNext();
    }

    @Override public CarbonRowBatch next() {
      int counter = 0;
      CarbonRowBatch rowBatch = new CarbonRowBatch(batchSize);
      while (finalMerger.hasNext() && counter < batchSize) {
        rowBatch.addRow(new CarbonRow(finalMerger.next()));
        counter++;
      }
      return rowBatch;
    }
  }
}