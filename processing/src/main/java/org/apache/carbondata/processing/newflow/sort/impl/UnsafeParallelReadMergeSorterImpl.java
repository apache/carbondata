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
package org.apache.carbondata.processing.newflow.sort.impl;

import java.io.File;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonTimeStatisticsFactory;
import org.apache.carbondata.processing.newflow.DataField;
import org.apache.carbondata.processing.newflow.exception.CarbonDataLoadingException;
import org.apache.carbondata.processing.newflow.row.CarbonRow;
import org.apache.carbondata.processing.newflow.row.CarbonRowBatch;
import org.apache.carbondata.processing.newflow.sort.Sorter;
import org.apache.carbondata.processing.newflow.sort.unsafe.UnsafeCarbonRowPage;
import org.apache.carbondata.processing.newflow.sort.unsafe.UnsafeSortDataRows;
import org.apache.carbondata.processing.newflow.sort.unsafe.merger.UnsafeIntermediateMerger;
import org.apache.carbondata.processing.newflow.sort.unsafe.merger.UnsafeSingleThreadFinalSortFilesMerger;
import org.apache.carbondata.processing.sortandgroupby.exception.CarbonSortKeyAndGroupByException;
import org.apache.carbondata.processing.sortandgroupby.sortdata.SortParameters;
import org.apache.carbondata.processing.store.writer.exception.CarbonDataWriterException;
import org.apache.carbondata.processing.util.CarbonDataProcessorUtil;

/**
 * It parallely reads data from array of iterates and do merge sort.
 * First it sorts the data and write to temp files. These temp files will be merge sorted to get
 * final merge sort result.
 */
public class UnsafeParallelReadMergeSorterImpl implements Sorter {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(UnsafeParallelReadMergeSorterImpl.class.getName());

  private SortParameters sortParameters;

  private UnsafeIntermediateMerger unsafeIntermediateFileMerger;

  private ExecutorService executorService;

  private UnsafeSingleThreadFinalSortFilesMerger finalMerger;

  private DataField[] inputDataFields;

  public UnsafeParallelReadMergeSorterImpl(DataField[] inputDataFields) {
    this.inputDataFields = inputDataFields;
  }

  @Override public void initialize(SortParameters sortParameters) {
    this.sortParameters = sortParameters;
    unsafeIntermediateFileMerger = new UnsafeIntermediateMerger(sortParameters);
    String storeLocation = CarbonDataProcessorUtil
        .getLocalDataFolderLocation(sortParameters.getDatabaseName(), sortParameters.getTableName(),
            String.valueOf(sortParameters.getTaskNo()), sortParameters.getPartitionID(),
            sortParameters.getSegmentId() + "", false);
    // Set the data file location
    String dataFolderLocation =
        storeLocation + File.separator + CarbonCommonConstants.SORT_TEMP_FILE_LOCATION;
    finalMerger = new UnsafeSingleThreadFinalSortFilesMerger(sortParameters);
  }

  @Override public Iterator<CarbonRowBatch>[] sort(Iterator<CarbonRowBatch>[] iterators)
      throws CarbonDataLoadingException {
    UnsafeSortDataRows sortDataRow =
        new UnsafeSortDataRows(sortParameters, unsafeIntermediateFileMerger);
    final int batchSize = CarbonProperties.getInstance().getBatchSize();
    try {
      sortDataRow.initialize();
    } catch (CarbonSortKeyAndGroupByException e) {
      throw new CarbonDataLoadingException(e);
    }
    this.executorService = Executors.newFixedThreadPool(iterators.length);

    try {
      for (int i = 0; i < iterators.length; i++) {
        executorService
            .submit(new SortIteratorThread(iterators[i], sortDataRow, sortParameters, batchSize));
      }
      executorService.shutdown();
      executorService.awaitTermination(2, TimeUnit.DAYS);
      processRowToNextStep(sortDataRow, sortParameters);
    } catch (Exception e) {
      throw new CarbonDataLoadingException("Problem while shutdown the server ", e);
    }
    try {
      unsafeIntermediateFileMerger.finish();
      List<UnsafeCarbonRowPage> rowPages = unsafeIntermediateFileMerger.getRowPages();
      finalMerger.startFinalMerge(rowPages.toArray(new UnsafeCarbonRowPage[rowPages.size()]),
          unsafeIntermediateFileMerger.getMergedPages());
    } catch (CarbonDataWriterException e) {
      throw new CarbonDataLoadingException(e);
    } catch (CarbonSortKeyAndGroupByException e) {
      throw new CarbonDataLoadingException(e);
    }

    // Creates the iterator to read from merge sorter.
    Iterator<CarbonRowBatch> batchIterator = new CarbonIterator<CarbonRowBatch>() {

      @Override public boolean hasNext() {
        return finalMerger.hasNext();
      }

      @Override public CarbonRowBatch next() {
        int counter = 0;
        CarbonRowBatch rowBatch = new CarbonRowBatch();
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
    unsafeIntermediateFileMerger.close();
    finalMerger.clear();
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
          .recordDictionaryValuesTotalTime(parameters.getPartitionID(), System.currentTimeMillis());
      return false;
    } catch (CarbonSortKeyAndGroupByException e) {
      throw new CarbonDataLoadingException(e);
    }
  }

  /**
   * This thread iterates the iterator and adds the rows
   */
  private static class SortIteratorThread implements Callable<Void> {

    private Iterator<CarbonRowBatch> iterator;

    private UnsafeSortDataRows sortDataRows;

    private SortParameters parameters;

    private Object[][] buffer;

    public SortIteratorThread(Iterator<CarbonRowBatch> iterator, UnsafeSortDataRows sortDataRows,
        SortParameters parameters, int batchSize) {
      this.iterator = iterator;
      this.sortDataRows = sortDataRows;
      this.parameters = parameters;
      this.buffer = new Object[batchSize][];
    }

    @Override public Void call() throws CarbonDataLoadingException {
      try {
        while (iterator.hasNext()) {
          CarbonRowBatch batch = iterator.next();
          Iterator<CarbonRow> batchIterator = batch.getBatchIterator();
          int i = 0;
          while (batchIterator.hasNext()) {
            CarbonRow row = batchIterator.next();
            if (row != null) {
              buffer[i++] = row.getData();
            }
          }
          if (i > 0) {
            sortDataRows.addRowBatch(buffer, i);
          }
        }
      } catch (Exception e) {
        LOGGER.error(e);
        throw new CarbonDataLoadingException(e);
      }
      return null;
    }

  }
}
