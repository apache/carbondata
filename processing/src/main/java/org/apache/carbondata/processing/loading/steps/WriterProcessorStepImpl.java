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
package org.apache.carbondata.processing.loading.steps;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datastore.exception.CarbonDataWriterException;
import org.apache.carbondata.core.datastore.row.CarbonRow;
import org.apache.carbondata.core.datastore.row.WriteStepRowUtil;
import org.apache.carbondata.core.metadata.CarbonTableIdentifier;
import org.apache.carbondata.core.util.CarbonThreadFactory;
import org.apache.carbondata.core.util.CarbonTimeStatisticsFactory;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.processing.loading.CarbonDataLoadConfiguration;
import org.apache.carbondata.processing.loading.exception.BadRecordFoundException;
import org.apache.carbondata.processing.loading.exception.CarbonDataLoadingException;
import org.apache.carbondata.processing.loading.row.CarbonRowBatch;
import org.apache.carbondata.processing.store.CarbonFactDataHandlerModel;
import org.apache.carbondata.processing.store.CarbonFactHandler;
import org.apache.carbondata.processing.store.CarbonFactHandlerFactory;
import org.apache.carbondata.processing.util.CarbonDataProcessorUtil;

/**
 * Writer that consumes the output of child iterator and write to carbondata files.
 */
public class WriterProcessorStepImpl extends AbstractDataLoadProcessorStep {

  private final LogService LOGGER =
      LogServiceFactory.getLogService(this.getClass().getCanonicalName());

  private int dimensionWithComplexCount;

  private int noDictWithComplextCount;

  private boolean[] isNoDictionaryDimensionColumn;

  private int dimensionCount;

  private int measureCount;

  private long[] readCounter;

  private long[] writeCounter;

  private String tableName;

  /**
   * If true, this step is for NO_SORT flow.
   * it means we will launch a separate a thread to process each input iterator, if there are more
   * than 1 iterator.
   */
  private boolean noSort;

  public WriterProcessorStepImpl(CarbonDataLoadConfiguration configuration,
      AbstractDataLoadProcessorStep child, boolean noSort) {
    super(configuration, child);
    this.noSort = noSort;
  }

  @Override
  public void initialize() throws IOException {
    child.initialize();
    CarbonTableIdentifier tableIdentifier =
        configuration.getTableIdentifier().getCarbonTableIdentifier();
    tableName = tableIdentifier.getTableName();
    dimensionWithComplexCount = configuration.getDimensionCount();
    noDictWithComplextCount =
        configuration.getNoDictionaryCount() + configuration.getComplexColumnCount();
    dimensionCount = configuration.getDimensionCount() - noDictWithComplextCount;
    isNoDictionaryDimensionColumn =
        CarbonDataProcessorUtil.getNoDictionaryMapping(configuration.getDataFields());
    measureCount = configuration.getMeasureCount();
    CarbonTimeStatisticsFactory.getLoadStatisticsInstance()
        .recordDictionaryValue2MdkAdd2FileTime(CarbonTablePath.DEPRECATED_PATITION_ID,
            System.currentTimeMillis());
  }

  @Override
  public Iterator<CarbonRowBatch>[] execute() throws CarbonDataLoadingException {
    final Iterator<CarbonRowBatch>[] iterators = child.execute();
    CarbonTimeStatisticsFactory.getLoadStatisticsInstance().recordDictionaryValue2MdkAdd2FileTime(
        CarbonTablePath.DEPRECATED_PATITION_ID, System.currentTimeMillis());
    try {
      readCounter = new long[iterators.length];
      writeCounter = new long[iterators.length];

      if (noSort && iterators.length > 1) {
        doExecuteParallel(iterators);
      } else {
        doExecuteSequential(iterators);
      }
    } catch (BadRecordFoundException e) {
      throw e;
    } catch (Exception e) {
      LOGGER.error(e, "Failed for table: " + tableName + " in WriterLocalSortProcessorStepImpl");
      throw new CarbonDataLoadingException(e.getMessage(), e);
    }
    return null;
  }

  private void doExecuteParallel(Iterator<CarbonRowBatch>[] iterators)
      throws InterruptedException, java.util.concurrent.ExecutionException {
    ExecutorService executorService = Executors.newFixedThreadPool(
        iterators.length,
        new CarbonThreadFactory("NoSortDataWriterPool:" +
            configuration.getTableIdentifier().getCarbonTableIdentifier().getTableName()));
    try {
      Future[] futures = new Future[iterators.length];
      for (int i = 0; i < iterators.length; i++) {
        final Iterator<CarbonRowBatch> iterator = iterators[i];
        final int iteratorIndex = i;
        futures[i] = executorService.submit(new Runnable() {
          @Override public void run() {
            doExecute(iterator, iteratorIndex);
          }
        });
      }
      for (Future future : futures) {
        future.get();
      }
    } finally {
      executorService.shutdownNow();
    }
  }

  private void doExecute(Iterator<CarbonRowBatch> iterator, int iterateIndex) {
    int taskExtension = 0;
    while (iterator.hasNext()) {
      CarbonRowBatch next = iterator.next();
      // If no rows from merge sorter, then don't create a file in fact column handler
      if (next.hasNext()) {
        CarbonFactDataHandlerModel model =
            CarbonFactDataHandlerModel.createModelForLoading(
                configuration, 0, taskExtension++);
        CarbonFactHandler dataHandler =
            CarbonFactHandlerFactory.createCarbonFactHandler(
                model, CarbonFactHandlerFactory.FactHandlerType.COLUMNAR);
        dataHandler.initialise();
        processBatch(next, dataHandler, iterateIndex);
        finish(dataHandler, iterateIndex);
      }
    }
  }

  private void doExecuteSequential(Iterator<CarbonRowBatch>[] iterators) {
    for (int i = 0; i < iterators.length; i++) {
      doExecute(iterators[i], i);
    }
  }

  private void finish(CarbonFactHandler dataHandler, int iteratorIndex) {
    try {
      dataHandler.finish();
    } catch (Exception e) {
      LOGGER.error(e, "Failed for table: " + tableName + " in  finishing data handler");
    }
    LOGGER.info("Record Processed For table: " + tableName);
    String logMessage =
        "Finished Carbon WriterLocalSortProcessorStepImpl: Read: " + readCounter[iteratorIndex]
            + ": Write: " + readCounter[iteratorIndex];
    LOGGER.info(logMessage);
    CarbonTimeStatisticsFactory.getLoadStatisticsInstance().recordTotalRecords(rowCounter.get());
    processingComplete(dataHandler);
    CarbonTimeStatisticsFactory.getLoadStatisticsInstance()
        .recordDictionaryValue2MdkAdd2FileTime(CarbonTablePath.DEPRECATED_PATITION_ID,
            System.currentTimeMillis());
    CarbonTimeStatisticsFactory.getLoadStatisticsInstance()
        .recordMdkGenerateTotalTime(CarbonTablePath.DEPRECATED_PATITION_ID,
            System.currentTimeMillis());
  }

  private void processingComplete(CarbonFactHandler dataHandler) throws CarbonDataLoadingException {
    if (null != dataHandler) {
      try {
        dataHandler.closeHandler();
      } catch (CarbonDataWriterException e) {
        LOGGER.error(e, e.getMessage());
        throw new CarbonDataLoadingException(e.getMessage());
      } catch (Exception e) {
        LOGGER.error(e, e.getMessage());
        throw new CarbonDataLoadingException("There is an unexpected error: " + e.getMessage());
      }
    }
  }

  /**
   * convert input CarbonRow to output CarbonRow
   * e.g. There is a table as following,
   * the number of dictionary dimensions is a,
   * the number of no-dictionary dimensions is b,
   * the number of complex dimensions is c,
   * the number of measures is d.
   * input CarbonRow format:  the length of Object[] data is a+b+c+d, the number of all columns.
   * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
   * | Part                     | Object item                    | describe                 |
   * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
   * | Object[0 ~ a+b-1]        | Integer, byte[], Integer, ...  | dict + no dict dimensions|
   * ----------------------------------------------------------------------------------------
   * | Object[a+b ~ a+b+c-1]    | byte[], byte[], ...            | complex dimensions       |
   * ----------------------------------------------------------------------------------------
   * | Object[a+b+c ~ a+b+c+d-1]| int, byte[], ...               | measures                 |
   * ----------------------------------------------------------------------------------------
   * output CarbonRow format: the length of object[] data is d + (b+c>0?1:0) + 1.
   * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
   * | Part                     | Object item                    | describe                 |
   * ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
   * | Object[d+1]              | byte[]                         | mdkey                    |
   * ----------------------------------------------------------------------------------------
   * | Object[d]                | byte[b+c][]                    | no dict + complex dim    |
   * ----------------------------------------------------------------------------------------
   * | Object[0 ~ d-1]          | int, byte[], ...               | measures                 |
   * ----------------------------------------------------------------------------------------
   *
   * @param row
   * @return
   */
  private CarbonRow convertRow(CarbonRow row) {
    int dictIndex = 0;
    int nonDicIndex = 0;
    int[] dim = new int[this.dimensionCount];
    byte[][] nonDicArray = new byte[this.noDictWithComplextCount][];
    // read dimension values
    int dimCount = 0;
    for (; dimCount < isNoDictionaryDimensionColumn.length; dimCount++) {
      if (isNoDictionaryDimensionColumn[dimCount]) {
        nonDicArray[nonDicIndex++] = (byte[]) row.getObject(dimCount);
      } else {
        dim[dictIndex++] = (int) row.getObject(dimCount);
      }
    }

    for (; dimCount < this.dimensionWithComplexCount; dimCount++) {
      nonDicArray[nonDicIndex++] = (byte[]) row.getObject(dimCount);
    }

    Object[] measures = new Object[measureCount];
    for (int i = 0; i < this.measureCount; i++) {
      measures[i] = row.getObject(i + this.dimensionWithComplexCount);
    }

    return WriteStepRowUtil.fromColumnCategory(dim, nonDicArray, measures);
  }

  private void processBatch(CarbonRowBatch batch, CarbonFactHandler dataHandler, int iteratorIndex)
  {
    if (noSort) {
      while (batch.hasNext()) {
        CarbonRow row = convertRow(batch.next());
        dataHandler.addDataToStore(row);
        readCounter[iteratorIndex]++;
      }
    } else {
      while (batch.hasNext()) {
        CarbonRow row = batch.next();
        dataHandler.addDataToStore(row);
        readCounter[iteratorIndex]++;
      }
    }
    writeCounter[iteratorIndex] += batch.getSize();
    rowCounter.getAndAdd(batch.getSize());
  }
}
