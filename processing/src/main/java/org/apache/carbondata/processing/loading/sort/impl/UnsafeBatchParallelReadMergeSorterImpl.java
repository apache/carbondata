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
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.exception.CarbonDataWriterException;
import org.apache.carbondata.core.datastore.row.CarbonRow;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonTimeStatisticsFactory;
import org.apache.carbondata.processing.loading.exception.CarbonDataLoadingException;
import org.apache.carbondata.processing.loading.row.CarbonRowBatch;
import org.apache.carbondata.processing.loading.row.CarbonSortBatch;
import org.apache.carbondata.processing.loading.sort.AbstractMergeSorter;
import org.apache.carbondata.processing.loading.sort.unsafe.UnsafeCarbonRowPage;
import org.apache.carbondata.processing.loading.sort.unsafe.UnsafeSortDataRows;
import org.apache.carbondata.processing.loading.sort.unsafe.merger.UnsafeIntermediateMerger;
import org.apache.carbondata.processing.loading.sort.unsafe.merger.UnsafeSingleThreadFinalSortFilesMerger;
import org.apache.carbondata.processing.sort.exception.CarbonSortKeyAndGroupByException;
import org.apache.carbondata.processing.sort.sortdata.SortParameters;
import org.apache.carbondata.processing.util.CarbonDataProcessorUtil;

/**
 * It parallely reads data from array of iterates and do merge sort.
 * It sorts data in batches and send to the next step.
 */
public class UnsafeBatchParallelReadMergeSorterImpl extends AbstractMergeSorter {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(UnsafeBatchParallelReadMergeSorterImpl.class.getName());

  private SortParameters sortParameters;

  private ExecutorService executorService;

  private AtomicLong rowCounter;

  public UnsafeBatchParallelReadMergeSorterImpl(AtomicLong rowCounter) {
    this.rowCounter = rowCounter;
  }

  @Override public void initialize(SortParameters sortParameters) {
    this.sortParameters = sortParameters;

  }

  @Override public Iterator<CarbonRowBatch>[] sort(Iterator<CarbonRowBatch>[] iterators)
      throws CarbonDataLoadingException {
    this.executorService = Executors.newFixedThreadPool(iterators.length);
    this.threadStatusObserver = new ThreadStatusObserver(this.executorService);
    int batchSize = CarbonProperties.getInstance().getBatchSize();
    final SortBatchHolder sortBatchHolder = new SortBatchHolder(sortParameters, iterators.length,
        this.threadStatusObserver);

    try {
      for (int i = 0; i < iterators.length; i++) {
        executorService.execute(
            new SortIteratorThread(iterators[i], sortBatchHolder, batchSize, rowCounter,
                this.threadStatusObserver));
      }
    } catch (Exception e) {
      checkError();
      throw new CarbonDataLoadingException("Problem while shutdown the server ", e);
    }
    checkError();
    // Creates the iterator to read from merge sorter.
    Iterator<CarbonSortBatch> batchIterator = new CarbonIterator<CarbonSortBatch>() {

      @Override public boolean hasNext() {
        return sortBatchHolder.hasNext();
      }

      @Override public CarbonSortBatch next() {
        return new CarbonSortBatch(sortBatchHolder.next());
      }
    };
    return new Iterator[] { batchIterator };
  }

  @Override public void close() {
    executorService.shutdown();
    try {
      executorService.awaitTermination(2, TimeUnit.DAYS);
    } catch (InterruptedException e) {
      LOGGER.error(e);
    }
  }

  /**
   * This thread iterates the iterator and adds the rows
   */
  private static class SortIteratorThread implements Runnable {

    private Iterator<CarbonRowBatch> iterator;

    private SortBatchHolder sortDataRows;

    private Object[][] buffer;

    private AtomicLong rowCounter;

    private ThreadStatusObserver threadStatusObserver;

    public SortIteratorThread(Iterator<CarbonRowBatch> iterator, SortBatchHolder sortDataRows,
        int batchSize, AtomicLong rowCounter, ThreadStatusObserver threadStatusObserver) {
      this.iterator = iterator;
      this.sortDataRows = sortDataRows;
      this.buffer = new Object[batchSize][];
      this.rowCounter = rowCounter;
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
              buffer[i++] = row.getData();
            }
          }
          if (i > 0) {
            synchronized (sortDataRows) {
              sortDataRows.getSortDataRow().addRowBatchWithOutSync(buffer, i);
              rowCounter.getAndAdd(i);
              if (!sortDataRows.getSortDataRow().canAdd()) {
                sortDataRows.finish(false);
                sortDataRows.createSortDataRows();
              }
            }
          }
        }
      } catch (Exception e) {
        LOGGER.error(e);
        this.threadStatusObserver.notifyFailed(e);
      } finally {
        sortDataRows.finishThread();
      }
    }

  }

  private static class SortBatchHolder
      extends CarbonIterator<UnsafeSingleThreadFinalSortFilesMerger> {

    private SortParameters sortParameters;

    private UnsafeSingleThreadFinalSortFilesMerger finalMerger;

    private UnsafeIntermediateMerger unsafeIntermediateFileMerger;

    private UnsafeSortDataRows sortDataRow;

    private final BlockingQueue<UnsafeSingleThreadFinalSortFilesMerger> mergerQueue;

    private AtomicInteger iteratorCount;

    private int batchCount;

    private ThreadStatusObserver threadStatusObserver;

    private final Object lock = new Object();

    public SortBatchHolder(SortParameters sortParameters, int numberOfThreads,
        ThreadStatusObserver threadStatusObserver) {
      this.sortParameters = sortParameters.getCopy();
      this.iteratorCount = new AtomicInteger(numberOfThreads);
      this.mergerQueue = new LinkedBlockingQueue<>(1);
      this.threadStatusObserver = threadStatusObserver;
      createSortDataRows();
    }

    private void createSortDataRows() {
      int inMemoryChunkSizeInMB = CarbonProperties.getInstance().getSortMemoryChunkSizeInMB();
      setTempLocation(sortParameters);
      this.finalMerger = new UnsafeSingleThreadFinalSortFilesMerger(sortParameters,
          sortParameters.getTempFileLocation());
      unsafeIntermediateFileMerger = new UnsafeIntermediateMerger(sortParameters);
      sortDataRow = new UnsafeSortDataRows(sortParameters, unsafeIntermediateFileMerger,
          inMemoryChunkSizeInMB);

      try {
        sortDataRow.initialize();
      } catch (MemoryException e) {
        throw new CarbonDataLoadingException(e);
      }
      batchCount++;
    }

    private void setTempLocation(SortParameters parameters) {
      String[] carbonDataDirectoryPath = CarbonDataProcessorUtil.getLocalDataFolderLocation(
          parameters.getDatabaseName(), parameters.getTableName(), parameters.getTaskNo(),
          parameters.getSegmentId(), false, false);
      String[] tempDirs = CarbonDataProcessorUtil.arrayAppend(carbonDataDirectoryPath,
          File.separator, CarbonCommonConstants.SORT_TEMP_FILE_LOCATION);
      parameters.setTempFileLocation(tempDirs);
    }

    @Override public UnsafeSingleThreadFinalSortFilesMerger next() {
      try {
        UnsafeSingleThreadFinalSortFilesMerger unsafeSingleThreadFinalSortFilesMerger =
            mergerQueue.take();
        if (unsafeSingleThreadFinalSortFilesMerger.isStopProcess()) {
          throw new RuntimeException(threadStatusObserver.getThrowable());
        }
        return unsafeSingleThreadFinalSortFilesMerger;
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }

    public UnsafeSortDataRows getSortDataRow() {
      return sortDataRow;
    }

    public void finish(boolean isFinalAttempt) {
      try {
        // if the mergerQue is empty and some CarbonDataLoadingException exception has occurred
        // then set stop process to true in the finalmerger instance
        if (mergerQueue.isEmpty() && threadStatusObserver != null
            && threadStatusObserver.getThrowable() != null && threadStatusObserver
            .getThrowable() instanceof CarbonDataLoadingException) {
          finalMerger.setStopProcess(true);
          if (isFinalAttempt) {
            iteratorCount.decrementAndGet();
          }
          mergerQueue.put(finalMerger);
          return;
        }
        processRowToNextStep(sortDataRow, sortParameters);
        unsafeIntermediateFileMerger.finish();
        List<UnsafeCarbonRowPage> rowPages = unsafeIntermediateFileMerger.getRowPages();
        finalMerger.startFinalMerge(rowPages.toArray(new UnsafeCarbonRowPage[rowPages.size()]),
            unsafeIntermediateFileMerger.getMergedPages());
        unsafeIntermediateFileMerger.close();
        if (isFinalAttempt) {
          iteratorCount.decrementAndGet();
        }
        mergerQueue.put(finalMerger);
        sortDataRow = null;
        unsafeIntermediateFileMerger = null;
        finalMerger = null;
      } catch (CarbonDataWriterException e) {
        throw new CarbonDataLoadingException(e);
      } catch (CarbonSortKeyAndGroupByException e) {
        throw new CarbonDataLoadingException(e);
      } catch (InterruptedException e) {
        // if fails to put in queue because of interrupted exception, we can offer to free the main
        // thread from waiting.
        if (finalMerger != null) {
          finalMerger.setStopProcess(true);
          boolean offered = mergerQueue.offer(finalMerger);
          if (!offered) {
            throw new CarbonDataLoadingException(e);
          }
        }
        throw new CarbonDataLoadingException(e);
      }
    }

    public void finishThread() {
      synchronized (lock) {
        if (iteratorCount.get() <= 1) {
          finish(true);
        } else {
          iteratorCount.decrementAndGet();
        }
      }
    }

    public boolean hasNext() {
      return iteratorCount.get() > 0 || !mergerQueue.isEmpty();
    }

    /**
     * Below method will be used to process data to next step
     */
    private boolean processRowToNextStep(UnsafeSortDataRows sortDataRows, SortParameters parameters)
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
      } catch (InterruptedException e) {
        throw new CarbonDataLoadingException(e);
      }
    }

  }
}
