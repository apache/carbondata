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
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.exception.CarbonDataWriterException;
import org.apache.carbondata.core.datastore.row.CarbonRow;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonThreadFactory;
import org.apache.carbondata.core.util.CarbonTimeStatisticsFactory;
import org.apache.carbondata.processing.loading.exception.CarbonDataLoadingException;
import org.apache.carbondata.processing.loading.row.CarbonRowBatch;
import org.apache.carbondata.processing.loading.sort.AbstractMergeSorter;
import org.apache.carbondata.processing.sort.exception.CarbonSortKeyAndGroupByException;
import org.apache.carbondata.processing.sort.sortdata.SingleThreadFinalSortFilesMerger;
import org.apache.carbondata.processing.sort.sortdata.SortDataRows;
import org.apache.carbondata.processing.sort.sortdata.SortIntermediateFileMerger;
import org.apache.carbondata.processing.sort.sortdata.SortParameters;
import org.apache.carbondata.processing.util.CarbonDataProcessorUtil;

/**
 * It parallely reads data from array of iterates and do merge sort.
 * First it sorts the data and write to temp files. These temp files will be merge sorted to get
 * final merge sort result.
 */
public class ParallelReadMergeSorterImpl extends AbstractMergeSorter {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(ParallelReadMergeSorterImpl.class.getName());

  private SortParameters sortParameters;

  private SortIntermediateFileMerger intermediateFileMerger;

  private SingleThreadFinalSortFilesMerger finalMerger;

  private AtomicLong rowCounter;

  private ExecutorService executorService;

  public ParallelReadMergeSorterImpl(AtomicLong rowCounter) {
    this.rowCounter = rowCounter;
  }

  @Override
  public void initialize(SortParameters sortParameters) {
    this.sortParameters = sortParameters;
    intermediateFileMerger = new SortIntermediateFileMerger(sortParameters);
    String[] storeLocations =
        CarbonDataProcessorUtil.getLocalDataFolderLocation(
            sortParameters.getDatabaseName(), sortParameters.getTableName(),
            String.valueOf(sortParameters.getTaskNo()), sortParameters.getSegmentId(),
            false, false);
    // Set the data file location
    String[] dataFolderLocations = CarbonDataProcessorUtil.arrayAppend(storeLocations,
        File.separator, CarbonCommonConstants.SORT_TEMP_FILE_LOCATION);
    finalMerger =
        new SingleThreadFinalSortFilesMerger(dataFolderLocations, sortParameters.getTableName(),
            sortParameters);
  }

  @Override
  public Iterator<CarbonRowBatch>[] sort(Iterator<CarbonRowBatch>[] iterators)
      throws CarbonDataLoadingException {
    SortDataRows sortDataRow = new SortDataRows(sortParameters, intermediateFileMerger);
    final int batchSize = CarbonProperties.getInstance().getBatchSize();
    try {
      sortDataRow.initialize();
    } catch (CarbonSortKeyAndGroupByException e) {
      throw new CarbonDataLoadingException(e);
    }
    this.executorService = Executors.newFixedThreadPool(iterators.length,
        new CarbonThreadFactory("SafeParallelSorterPool:" + sortParameters.getTableName()));
    this.threadStatusObserver = new ThreadStatusObserver(executorService);

    try {
      for (int i = 0; i < iterators.length; i++) {
        executorService.execute(
            new SortIteratorThread(iterators[i], sortDataRow, batchSize, rowCounter,
                threadStatusObserver));
      }
      executorService.shutdown();
      executorService.awaitTermination(2, TimeUnit.DAYS);
      processRowToNextStep(sortDataRow, sortParameters);
    } catch (Exception e) {
      checkError();
      throw new CarbonDataLoadingException("Problem while shutdown the server ", e);
    }
    checkError();
    try {
      intermediateFileMerger.finish();
      intermediateFileMerger = null;
      finalMerger.startFinalMerge();
    } catch (CarbonDataWriterException e) {
      throw new CarbonDataLoadingException(e);
    } catch (CarbonSortKeyAndGroupByException e) {
      throw new CarbonDataLoadingException(e);
    }

    // Creates the iterator to read from merge sorter.
    Iterator<CarbonRowBatch> batchIterator = new CarbonIterator<CarbonRowBatch>() {

      @Override
      public boolean hasNext() {
        return finalMerger.hasNext();
      }

      @Override
      public CarbonRowBatch next() {
        int counter = 0;
        CarbonRowBatch rowBatch = new CarbonRowBatch(batchSize);
        while (finalMerger.hasNext() && counter < batchSize) {
          rowBatch.addRow(new CarbonRow(finalMerger.next()));
          counter++;
        }
        return rowBatch;
      }
    };
    return new Iterator[] { batchIterator };
  }

  @Override public void close() {
    if (intermediateFileMerger != null) {
      intermediateFileMerger.close();
    }
    if (null != executorService && !executorService.isShutdown()) {
      executorService.shutdownNow();
    }
  }

  /**
   * Below method will be used to process data to next step
   */
  private boolean processRowToNextStep(SortDataRows sortDataRows, SortParameters parameters)
      throws CarbonDataLoadingException {
    if (null == sortDataRows) {
      LOGGER.info("Record Processed For table: " + parameters.getTableName());
      LOGGER.info("Number of Records was Zero");
      String logMessage = "Summary: Carbon Sort Key Step: Read: " + 0 + ": Write: " + 0;
      LOGGER.info(logMessage);
      return false;
    }

    try {
      // start sorting
      sortDataRows.startSorting();

      // check any more rows are present
      LOGGER.info("Record Processed For table: " + parameters.getTableName());
      CarbonTimeStatisticsFactory.getLoadStatisticsInstance()
          .recordSortRowsStepTotalTime(parameters.getPartitionID(), System.currentTimeMillis());
      CarbonTimeStatisticsFactory.getLoadStatisticsInstance()
          .recordDictionaryValuesTotalTime(parameters.getPartitionID(),
              System.currentTimeMillis());
      return false;
    } catch (CarbonSortKeyAndGroupByException e) {
      throw new CarbonDataLoadingException(e);
    }
  }

  /**
   * This thread iterates the iterator and adds the rows to @{@link SortDataRows}
   */
  private static class SortIteratorThread implements Runnable {

    private Iterator<CarbonRowBatch> iterator;

    private SortDataRows sortDataRows;

    private Object[][] buffer;

    private AtomicLong rowCounter;

    private ThreadStatusObserver observer;

    public SortIteratorThread(Iterator<CarbonRowBatch> iterator, SortDataRows sortDataRows,
        int batchSize, AtomicLong rowCounter, ThreadStatusObserver observer) {
      this.iterator = iterator;
      this.sortDataRows = sortDataRows;
      this.buffer = new Object[batchSize][];
      this.rowCounter = rowCounter;
      this.observer = observer;

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
              buffer[i++] = row.getData();
            }
          }
          if (i > 0) {
            sortDataRows.addRowBatch(buffer, i);
            rowCounter.getAndAdd(i);
          }
        }
      } catch (Exception e) {
        LOGGER.error(e);
        observer.notifyFailed(e);
      }
    }

  }
}
