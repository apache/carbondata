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
package org.apache.carbondata.processing.newflow.steps.sort;

import java.io.File;
import java.util.Iterator;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.util.CarbonTimeStatisticsFactory;
import org.apache.carbondata.processing.newflow.AbstractDataLoadProcessorStep;
import org.apache.carbondata.processing.newflow.CarbonDataLoadConfiguration;
import org.apache.carbondata.processing.newflow.DataField;
import org.apache.carbondata.processing.newflow.constants.DataLoadProcessorConstants;
import org.apache.carbondata.processing.newflow.exception.CarbonDataLoadingException;
import org.apache.carbondata.processing.newflow.row.CarbonRow;
import org.apache.carbondata.processing.newflow.row.CarbonRowBatch;
import org.apache.carbondata.processing.sortandgroupby.exception.CarbonSortKeyAndGroupByException;
import org.apache.carbondata.processing.sortandgroupby.sortdata.SortDataRows;
import org.apache.carbondata.processing.sortandgroupby.sortdata.SortIntermediateFileMerger;
import org.apache.carbondata.processing.sortandgroupby.sortdata.SortParameters;
import org.apache.carbondata.processing.store.SingleThreadFinalSortFilesMerger;
import org.apache.carbondata.processing.store.writer.exception.CarbonDataWriterException;
import org.apache.carbondata.processing.util.CarbonDataProcessorUtil;

/**
 * It sorts the data and write them to intermediate temp files. These files will be further read
 * by next step for writing to carbondata files.
 */
public class SortProcessorStepImpl extends AbstractDataLoadProcessorStep {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(SortProcessorStepImpl.class.getName());

  private SortParameters sortParameters;

  private SortIntermediateFileMerger intermediateFileMerger;

  private ExecutorService executorService;

  private SingleThreadFinalSortFilesMerger finalMerger;

  public SortProcessorStepImpl(CarbonDataLoadConfiguration configuration,
      AbstractDataLoadProcessorStep child) {
    super(configuration, child);
  }

  @Override public DataField[] getOutput() {
    return child.getOutput();
  }

  @Override public void intialize() throws CarbonDataLoadingException {
    sortParameters = SortParameters.createSortParameters(configuration);
    intermediateFileMerger = new SortIntermediateFileMerger(sortParameters);
    String storeLocation = CarbonDataProcessorUtil
        .getLocalDataFolderLocation(sortParameters.getDatabaseName(), sortParameters.getTableName(),
            String.valueOf(sortParameters.getTaskNo()), sortParameters.getPartitionID(),
            sortParameters.getSegmentId() + "", false);
    // Set the data file location
    String dataFolderLocation =
        storeLocation + File.separator + CarbonCommonConstants.SORT_TEMP_FILE_LOCATION;
    finalMerger =
        new SingleThreadFinalSortFilesMerger(dataFolderLocation, sortParameters.getTableName(),
            sortParameters.getDimColCount() - sortParameters.getComplexDimColCount(),
            sortParameters.getComplexDimColCount(), sortParameters.getMeasureColCount(),
            sortParameters.getNoDictionaryCount(), sortParameters.getAggType(),
            sortParameters.getNoDictionaryDimnesionColumn());
  }

  @Override public Iterator<CarbonRowBatch>[] execute() throws CarbonDataLoadingException {
    final Iterator<CarbonRowBatch>[] iterators = child.execute();
    SortDataRows[] sortDataRows = new SortDataRows[iterators.length];
    try {
      for (int i = 0; i < iterators.length; i++) {
        sortDataRows[i] = new SortDataRows(sortParameters, intermediateFileMerger);
        // initialize sort
        sortDataRows[i].initialize();
      }
    } catch (CarbonSortKeyAndGroupByException e) {
      throw new CarbonDataLoadingException(e);
    }
    this.executorService = Executors.newFixedThreadPool(iterators.length);

    for (int i = 0; i < sortDataRows.length; i++) {
      executorService.submit(new SortIteratorThread(iterators[i], sortDataRows[i], sortParameters));
    }

    try {
      executorService.shutdown();
      executorService.awaitTermination(2, TimeUnit.DAYS);
    } catch (InterruptedException e) {
      throw new CarbonDataLoadingException("Problem while shutdown the server ", e);
    }
    child.finish();

    try {
      intermediateFileMerger.finish();
      finalMerger.startFinalMerge();
    } catch (CarbonDataWriterException e) {
      throw new CarbonDataLoadingException(e);
    } catch (CarbonSortKeyAndGroupByException e) {
      throw new CarbonDataLoadingException(e);
    }

    final int batchSize = getBatchSize();

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

  private int getBatchSize() {
    int batchSize;
    try {
      batchSize = Integer.parseInt(configuration
          .getDataLoadProperty(DataLoadProcessorConstants.DATA_LOAD_BATCH_SIZE,
              DataLoadProcessorConstants.DATA_LOAD_BATCH_SIZE_DEFAULT).toString());
    } catch (NumberFormatException exc) {
      batchSize = Integer.parseInt(DataLoadProcessorConstants.DATA_LOAD_BATCH_SIZE_DEFAULT);
    }
    return batchSize;
  }

  @Override protected CarbonRow processRow(CarbonRow row) {
    return null;
  }

  @Override public void close() {
    intermediateFileMerger.close();
  }

  private static class SortIteratorThread implements Callable<Void> {

    private Iterator<CarbonRowBatch> iterator;

    private SortDataRows sortDataRows;

    private SortParameters parameters;

    public SortIteratorThread(Iterator<CarbonRowBatch> iterator, SortDataRows sortDataRows,
        SortParameters parameters) {
      this.iterator = iterator;
      this.sortDataRows = sortDataRows;
      this.parameters = parameters;
    }

    @Override public Void call() throws CarbonDataLoadingException {
      try {
        while (iterator.hasNext()) {
          CarbonRowBatch batch = iterator.next();
          Iterator<CarbonRow> batchIterator = batch.getBatchIterator();
          while (batchIterator.hasNext()) {
            sortDataRows.addRow(batchIterator.next().getData());
          }
        }

        processRowToNextStep(sortDataRows);
      } catch (CarbonSortKeyAndGroupByException e) {
        LOGGER.error(e);
        throw new CarbonDataLoadingException(e);
      }
      return null;
    }

    /**
     * Below method will be used to process data to next step
     */
    private boolean processRowToNextStep(SortDataRows sortDataRows)
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
  }

  @Override public void finish() {

  }

}
