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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.carbon.metadata.datatype.DataType;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.partition.Partitioner;
import org.apache.carbondata.core.partition.impl.HashPartitionerImpl;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonTimeStatisticsFactory;
import org.apache.carbondata.processing.newflow.DataField;
import org.apache.carbondata.processing.newflow.exception.CarbonDataLoadingException;
import org.apache.carbondata.processing.newflow.row.CarbonRow;
import org.apache.carbondata.processing.newflow.row.CarbonRowBatch;
import org.apache.carbondata.processing.newflow.sort.Sorter;
import org.apache.carbondata.processing.sortandgroupby.exception.CarbonSortKeyAndGroupByException;
import org.apache.carbondata.processing.sortandgroupby.sortdata.SortDataRows;
import org.apache.carbondata.processing.sortandgroupby.sortdata.SortIntermediateFileMerger;
import org.apache.carbondata.processing.sortandgroupby.sortdata.SortParameters;
import org.apache.carbondata.processing.store.SingleThreadFinalSortFilesMerger;
import org.apache.carbondata.processing.store.writer.exception.CarbonDataWriterException;
import org.apache.carbondata.processing.util.CarbonDataProcessorUtil;

/**
 * It parallely reads data from array of iterates and do merge sort.
 * First it sorts the data and write to temp files. These temp files will be merge sorted to get
 * final merge sort result.
 */
public class ParallelReadMergeSorterWithBucketingImpl implements Sorter {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(ParallelReadMergeSorterImpl.class.getName());

  private SortParameters sortParameters;

  private SortIntermediateFileMerger intermediateFileMerger;

  private ExecutorService executorService;

  private int numberOfBuckets;

  private DataField[] bucketFields;

  private Partitioner<Object[]> partitioner;

  private DataField[] inputDataFields;

  public ParallelReadMergeSorterWithBucketingImpl(DataField[] inputDataFields,
      DataField[] bucketFields, int numberOfBuckets) {
    this.inputDataFields = inputDataFields;
    this.numberOfBuckets = numberOfBuckets;
    this.bucketFields = bucketFields;
  }

  @Override public void initialize(SortParameters sortParameters) {
    this.sortParameters = sortParameters;
    intermediateFileMerger = new SortIntermediateFileMerger(sortParameters);
    List<Integer> indexes = new ArrayList<>();
    List<DataType> bucketDataTypes = new ArrayList<>();
    for (int i = 0; i < inputDataFields.length; i++) {
      for (int j = 0; j < bucketFields.length; j++) {
        if (inputDataFields[i].getColumn().getColName()
            .equals(bucketFields[j].getColumn().getColName())) {
          indexes.add(i);
          bucketDataTypes.add(inputDataFields[i].getColumn().getDataType());
          break;
        }
      }
    }
    partitioner = new HashPartitionerImpl(indexes, bucketDataTypes, numberOfBuckets);
  }

  @Override public Iterator<CarbonRowBatch>[] sort(Iterator<CarbonRowBatch>[] iterators)
      throws CarbonDataLoadingException {
    SortDataRows[] sortDataRows = new SortDataRows[numberOfBuckets];
    try {
      for (int i = 0; i < numberOfBuckets; i++) {
        SortParameters parameters = sortParameters.getCopy();
        parameters.setPartitionID(i + "");
        sortDataRows[i] = new SortDataRows(parameters, intermediateFileMerger);
        sortDataRows[i].initialize();
      }
    } catch (CarbonSortKeyAndGroupByException e) {
      throw new CarbonDataLoadingException(e);
    }
    this.executorService = Executors.newFixedThreadPool(iterators.length);
    final int batchSize = CarbonProperties.getInstance().getBatchSize();
    try {
      for (int i = 0; i < iterators.length; i++) {
        executorService.submit(new SortIteratorThread(iterators[i], sortDataRows, partitioner));
      }
      executorService.shutdown();
      executorService.awaitTermination(2, TimeUnit.DAYS);
      processRowToNextStep(sortDataRows, sortParameters);
    } catch (Exception e) {
      throw new CarbonDataLoadingException("Problem while shutdown the server ", e);
    }
    try {
      intermediateFileMerger.finish();
    } catch (CarbonDataWriterException e) {
      throw new CarbonDataLoadingException(e);
    } catch (CarbonSortKeyAndGroupByException e) {
      throw new CarbonDataLoadingException(e);
    }

    Iterator<CarbonRowBatch>[] batchIterator = new Iterator[numberOfBuckets];
    for (int i = 0; i < numberOfBuckets; i++) {
      batchIterator[i] = new MergedDataIterator(String.valueOf(i), batchSize);
    }

    return batchIterator;
  }

  private SingleThreadFinalSortFilesMerger getFinalMerger(String bucketId) {
    String storeLocation = CarbonDataProcessorUtil
        .getLocalDataFolderLocation(sortParameters.getDatabaseName(), sortParameters.getTableName(),
            String.valueOf(sortParameters.getTaskNo()), bucketId,
            sortParameters.getSegmentId() + "", false);
    // Set the data file location
    String dataFolderLocation =
        storeLocation + File.separator + CarbonCommonConstants.SORT_TEMP_FILE_LOCATION;
    SingleThreadFinalSortFilesMerger finalMerger =
        new SingleThreadFinalSortFilesMerger(dataFolderLocation, sortParameters.getTableName(),
            sortParameters.getDimColCount(), sortParameters.getComplexDimColCount(),
            sortParameters.getMeasureColCount(), sortParameters.getNoDictionaryCount(),
            sortParameters.getAggType(), sortParameters.getNoDictionaryDimnesionColumn(),
            sortParameters.isUseKettle());
    return finalMerger;
  }

  @Override public void close() {
    intermediateFileMerger.close();
  }

  /**
   * Below method will be used to process data to next step
   */
  private boolean processRowToNextStep(SortDataRows[] sortDataRows, SortParameters parameters)
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
    } catch (CarbonSortKeyAndGroupByException e) {
      throw new CarbonDataLoadingException(e);
    }
  }

  /**
   * This thread iterates the iterator and adds the rows to @{@link SortDataRows}
   */
  private static class SortIteratorThread implements Callable<Void> {

    private Iterator<CarbonRowBatch> iterator;

    private SortDataRows[] sortDataRows;

    private Partitioner partitioner;

    public SortIteratorThread(Iterator<CarbonRowBatch> iterator, SortDataRows[] sortDataRows,
        Partitioner partitioner) {
      this.iterator = iterator;
      this.sortDataRows = sortDataRows;
      this.partitioner = partitioner;
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
              int bucket = partitioner.getPartition(row.getData());
              SortDataRows sortDataRow = sortDataRows[bucket];
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

    private boolean firstRow = true;

    public MergedDataIterator(String partitionId, int batchSize) {
      this.partitionId = partitionId;
      this.batchSize = batchSize;
    }

    private SingleThreadFinalSortFilesMerger finalMerger;

    @Override public boolean hasNext() {
      if (firstRow) {
        firstRow = false;
        finalMerger = getFinalMerger(partitionId);
        finalMerger.startFinalMerge();
      }
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
  }
}
