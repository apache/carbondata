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
import org.apache.carbondata.core.datastore.row.CarbonRow;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.metadata.schema.BucketingInfo;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonTimeStatisticsFactory;
import org.apache.carbondata.processing.newflow.DataField;
import org.apache.carbondata.processing.newflow.exception.CarbonDataLoadingException;
import org.apache.carbondata.processing.newflow.row.CarbonRowBatch;
import org.apache.carbondata.processing.newflow.sort.Sorter;
import org.apache.carbondata.processing.newflow.sort.unsafe.UnsafeCarbonRowPage;
import org.apache.carbondata.processing.newflow.sort.unsafe.UnsafeSortDataRows;
import org.apache.carbondata.processing.newflow.sort.unsafe.merger.UnsafeIntermediateMerger;
import org.apache.carbondata.processing.newflow.sort.unsafe.merger.UnsafeSingleThreadFinalSortFilesMerger;
import org.apache.carbondata.processing.sortandgroupby.sortdata.SortParameters;
import org.apache.carbondata.processing.util.CarbonDataProcessorUtil;

/**
 * It parallely reads data from array of iterates and do merge sort.
 * First it sorts the data and write to temp files. These temp files will be merge sorted to get
 * final merge sort result.
 * This step is specifically for bucketing, it sorts each bucket data separately and write to
 * temp files.
 */
public class UnsafeParallelReadMergeSorterWithBucketingImpl implements Sorter {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(ParallelReadMergeSorterImpl.class.getName());

  private SortParameters sortParameters;

  private BucketingInfo bucketingInfo;

  public UnsafeParallelReadMergeSorterWithBucketingImpl(DataField[] inputDataFields,
      BucketingInfo bucketingInfo) {
    this.bucketingInfo = bucketingInfo;
  }

  @Override public void initialize(SortParameters sortParameters) {
    this.sortParameters = sortParameters;
  }

  @Override public Iterator<CarbonRowBatch>[] sort(Iterator<CarbonRowBatch>[] iterators)
      throws CarbonDataLoadingException {
    UnsafeSortDataRows[] sortDataRows = new UnsafeSortDataRows[bucketingInfo.getNumberOfBuckets()];
    UnsafeIntermediateMerger[] intermediateFileMergers =
        new UnsafeIntermediateMerger[sortDataRows.length];
    int inMemoryChunkSizeInMB = CarbonProperties.getInstance().getSortMemoryChunkSizeInMB();
    inMemoryChunkSizeInMB = inMemoryChunkSizeInMB / bucketingInfo.getNumberOfBuckets();
    if (inMemoryChunkSizeInMB < 5) {
      inMemoryChunkSizeInMB = 5;
    }
    try {
      for (int i = 0; i < bucketingInfo.getNumberOfBuckets(); i++) {
        SortParameters parameters = sortParameters.getCopy();
        parameters.setPartitionID(i + "");
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
    final int batchSize = CarbonProperties.getInstance().getBatchSize();
    try {
      for (int i = 0; i < iterators.length; i++) {
        executorService.submit(new SortIteratorThread(iterators[i], sortDataRows));
      }
      executorService.shutdown();
      executorService.awaitTermination(2, TimeUnit.DAYS);
      processRowToNextStep(sortDataRows, sortParameters);
    } catch (Exception e) {
      throw new CarbonDataLoadingException("Problem while shutdown the server ", e);
    }
    try {
      for (int i = 0; i < intermediateFileMergers.length; i++) {
        intermediateFileMergers[i].finish();
      }
    } catch (Exception e) {
      throw new CarbonDataLoadingException(e);
    }

    Iterator<CarbonRowBatch>[] batchIterator = new Iterator[bucketingInfo.getNumberOfBuckets()];
    for (int i = 0; i < sortDataRows.length; i++) {
      batchIterator[i] =
          new MergedDataIterator(String.valueOf(i), batchSize, intermediateFileMergers[i]);
    }

    return batchIterator;
  }

  private UnsafeSingleThreadFinalSortFilesMerger getFinalMerger(String bucketId) {
    String storeLocation = CarbonDataProcessorUtil
        .getLocalDataFolderLocation(sortParameters.getDatabaseName(), sortParameters.getTableName(),
            String.valueOf(sortParameters.getTaskNo()), bucketId,
            sortParameters.getSegmentId() + "", false);
    // Set the data file location
    String dataFolderLocation =
        storeLocation + File.separator + CarbonCommonConstants.SORT_TEMP_FILE_LOCATION;
    return new UnsafeSingleThreadFinalSortFilesMerger(sortParameters, dataFolderLocation);
  }

  @Override public void close() {
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
    String carbonDataDirectoryPath = CarbonDataProcessorUtil
        .getLocalDataFolderLocation(parameters.getDatabaseName(), parameters.getTableName(),
            parameters.getTaskNo(), parameters.getPartitionID(), parameters.getSegmentId(), false);
    parameters.setTempFileLocation(
        carbonDataDirectoryPath + File.separator + CarbonCommonConstants.SORT_TEMP_FILE_LOCATION);
  }

  /**
   * This thread iterates the iterator and adds the rows to @{@link UnsafeSortDataRows}
   */
  private static class SortIteratorThread implements Callable<Void> {

    private Iterator<CarbonRowBatch> iterator;

    private UnsafeSortDataRows[] sortDataRows;

    public SortIteratorThread(Iterator<CarbonRowBatch> iterator,
        UnsafeSortDataRows[] sortDataRows) {
      this.iterator = iterator;
      this.sortDataRows = sortDataRows;
    }

    @Override public Void call() throws CarbonDataLoadingException {
      try {
        while (iterator.hasNext()) {
          CarbonRowBatch batch = iterator.next();
          int i = 0;
          while (batch.hasNext()) {
            CarbonRow row = batch.next();
            if (row != null) {
              UnsafeSortDataRows sortDataRow = sortDataRows[row.bucketNumber];
              synchronized (sortDataRow) {
                sortDataRow.addRow(row.getData());
              }
            }
          }
        }
      } catch (Exception e) {
        LOGGER.error(e);
        throw new CarbonDataLoadingException(e);
      }
      return null;
    }

  }

  private class MergedDataIterator extends CarbonIterator<CarbonRowBatch> {

    private String partitionId;

    private int batchSize;

    private boolean firstRow;

    private UnsafeIntermediateMerger intermediateMerger;

    public MergedDataIterator(String partitionId, int batchSize,
        UnsafeIntermediateMerger intermediateMerger) {
      this.partitionId = partitionId;
      this.batchSize = batchSize;
      this.intermediateMerger = intermediateMerger;
      this.firstRow = true;
    }

    private UnsafeSingleThreadFinalSortFilesMerger finalMerger;

    @Override public boolean hasNext() {
      if (firstRow) {
        firstRow = false;
        finalMerger = getFinalMerger(partitionId);
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
