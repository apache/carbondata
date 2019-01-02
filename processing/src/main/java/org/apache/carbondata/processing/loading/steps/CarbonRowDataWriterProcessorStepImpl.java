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
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datastore.exception.CarbonDataWriterException;
import org.apache.carbondata.core.datastore.row.CarbonRow;
import org.apache.carbondata.core.datastore.row.WriteStepRowUtil;
import org.apache.carbondata.core.keygenerator.KeyGenException;
import org.apache.carbondata.core.localdictionary.generator.LocalDictionaryGenerator;
import org.apache.carbondata.core.metadata.CarbonTableIdentifier;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.util.CarbonThreadFactory;
import org.apache.carbondata.core.util.CarbonTimeStatisticsFactory;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.processing.datamap.DataMapWriterListener;
import org.apache.carbondata.processing.loading.AbstractDataLoadProcessorStep;
import org.apache.carbondata.processing.loading.CarbonDataLoadConfiguration;
import org.apache.carbondata.processing.loading.exception.BadRecordFoundException;
import org.apache.carbondata.processing.loading.exception.CarbonDataLoadingException;
import org.apache.carbondata.processing.loading.row.CarbonRowBatch;
import org.apache.carbondata.processing.store.CarbonFactDataHandlerModel;
import org.apache.carbondata.processing.store.CarbonFactHandler;
import org.apache.carbondata.processing.store.CarbonFactHandlerFactory;
import org.apache.carbondata.processing.util.CarbonDataProcessorUtil;

import org.apache.log4j.Logger;

/**
 * It reads data from sorted files which are generated in previous sort step.
 * And it writes data to carbondata file. It also generates mdk key while writing to carbondata file
 */
public class CarbonRowDataWriterProcessorStepImpl extends AbstractDataLoadProcessorStep {

  private static final Logger LOGGER =
      LogServiceFactory.getLogService(CarbonRowDataWriterProcessorStepImpl.class.getName());

  private int dimensionWithComplexCount;

  private int noDictWithComplextCount;

  private boolean[] isNoDictionaryDimensionColumn;

  private DataType[] measureDataType;

  private int dimensionCount;

  private int measureCount;

  private long[] readCounter;

  private long[] writeCounter;

  private int outputLength;

  private CarbonTableIdentifier tableIdentifier;

  private String tableName;

  private Map<String, LocalDictionaryGenerator> localDictionaryGeneratorMap;

  private List<CarbonFactHandler> carbonFactHandlers;

  private ExecutorService executorService = null;

  public CarbonRowDataWriterProcessorStepImpl(CarbonDataLoadConfiguration configuration,
      AbstractDataLoadProcessorStep child) {
    super(configuration, child);
    this.localDictionaryGeneratorMap =
        CarbonUtil.getLocalDictionaryModel(configuration.getTableSpec().getCarbonTable());
    this.carbonFactHandlers = new CopyOnWriteArrayList<>();
  }

  @Override public void initialize() throws IOException {
    super.initialize();
    child.initialize();
  }

  private String[] getStoreLocation() {
    String[] storeLocation = CarbonDataProcessorUtil
        .getLocalDataFolderLocation(this.configuration.getTableSpec().getCarbonTable(),
            String.valueOf(configuration.getTaskNo()), configuration.getSegmentId(), false, false);
    CarbonDataProcessorUtil.createLocations(storeLocation);
    return storeLocation;
  }

  @Override public Iterator<CarbonRowBatch>[] execute() throws CarbonDataLoadingException {
    final Iterator<CarbonRowBatch>[] iterators = child.execute();
    tableIdentifier = configuration.getTableIdentifier().getCarbonTableIdentifier();
    tableName = tableIdentifier.getTableName();
    try {
      readCounter = new long[iterators.length];
      writeCounter = new long[iterators.length];
      dimensionWithComplexCount = configuration.getDimensionCount();
      noDictWithComplextCount =
          configuration.getNoDictionaryCount() + configuration.getComplexDictionaryColumnCount()
              + configuration.getComplexNonDictionaryColumnCount();
      dimensionCount = configuration.getDimensionCount() - noDictWithComplextCount;
      isNoDictionaryDimensionColumn =
          CarbonDataProcessorUtil.getNoDictionaryMapping(configuration.getDataFields());
      measureDataType = configuration.getMeasureDataType();
      measureCount = configuration.getMeasureCount();
      outputLength = measureCount + (this.noDictWithComplextCount > 0 ? 1 : 0) + 1;
      CarbonTimeStatisticsFactory.getLoadStatisticsInstance()
          .recordDictionaryValue2MdkAdd2FileTime(CarbonTablePath.DEPRECATED_PARTITION_ID,
              System.currentTimeMillis());

      if (iterators.length == 1) {
        doExecute(iterators[0], 0);
      } else {
        executorService = Executors.newFixedThreadPool(iterators.length,
            new CarbonThreadFactory("NoSortDataWriterPool:" + configuration.getTableIdentifier()
                .getCarbonTableIdentifier().getTableName()));
        Future[] futures = new Future[iterators.length];
        for (int i = 0; i < iterators.length; i++) {
          futures[i] = executorService.submit(new DataWriterRunnable(iterators[i], i));
        }
        for (Future future : futures) {
          future.get();
        }
      }
    } catch (CarbonDataWriterException e) {
      LOGGER.error("Failed for table: " + tableName + " in DataWriterProcessorStepImpl", e);
      throw new CarbonDataLoadingException(
          "Error while initializing data handler : " + e.getMessage());
    } catch (Exception e) {
      LOGGER.error("Failed for table: " + tableName + " in DataWriterProcessorStepImpl", e);
      if (e instanceof BadRecordFoundException) {
        throw new BadRecordFoundException(e.getMessage(), e);
      }
      throw new CarbonDataLoadingException(e.getMessage(), e);
    }
    return null;
  }

  private void doExecute(Iterator<CarbonRowBatch> iterator, int iteratorIndex) throws IOException {
    String[] storeLocation = getStoreLocation();
    DataMapWriterListener listener = getDataMapWriterListener(0);
    CarbonFactDataHandlerModel model = CarbonFactDataHandlerModel.createCarbonFactDataHandlerModel(
        configuration, storeLocation, 0, iteratorIndex, listener);
    model.setColumnLocalDictGenMap(localDictionaryGeneratorMap);
    CarbonFactHandler dataHandler = null;
    boolean rowsNotExist = true;
    while (iterator.hasNext()) {
      if (rowsNotExist) {
        rowsNotExist = false;
        dataHandler = CarbonFactHandlerFactory.createCarbonFactHandler(model);
        this.carbonFactHandlers.add(dataHandler);
        dataHandler.initialise();
      }
      processBatch(iterator.next(), dataHandler, iteratorIndex);
    }
    try {
      if (!rowsNotExist) {
        finish(dataHandler, iteratorIndex);
      }
    } finally {
      carbonFactHandlers.remove(dataHandler);
    }


  }

  @Override protected String getStepName() {
    return "Data Writer";
  }

  private void finish(CarbonFactHandler dataHandler, int iteratorIndex) {
    CarbonDataWriterException exception = null;
    try {
      dataHandler.finish();
    } catch (Exception e) {
      // if throw exception from here dataHandler will not be closed.
      // so just holding exception and later throwing exception
      LOGGER.error("Failed for table: " + tableName + " in  finishing data handler", e);
      exception = new CarbonDataWriterException(
          "Failed for table: " + tableName + " in  finishing data handler", e);
    }
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Record Processed For table: " + tableName);
      String logMessage =
          "Finished Carbon DataWriterProcessorStepImpl: Read: " + readCounter[iteratorIndex] +
              ": Write: " + readCounter[iteratorIndex];
      LOGGER.debug(logMessage);
    }
    CarbonTimeStatisticsFactory.getLoadStatisticsInstance().recordTotalRecords(rowCounter.get());
    try {
      processingComplete(dataHandler);
    } catch (CarbonDataLoadingException e) {
      // only assign when exception is null
      // else it will erase original root cause
      if (null == exception) {
        exception = new CarbonDataWriterException(e);
      }
    }
    CarbonTimeStatisticsFactory.getLoadStatisticsInstance()
        .recordDictionaryValue2MdkAdd2FileTime(CarbonTablePath.DEPRECATED_PARTITION_ID,
            System.currentTimeMillis());
    CarbonTimeStatisticsFactory.getLoadStatisticsInstance()
        .recordMdkGenerateTotalTime(CarbonTablePath.DEPRECATED_PARTITION_ID,
            System.currentTimeMillis());
    if (null != exception) {
      throw exception;
    }
  }

  private void processingComplete(CarbonFactHandler dataHandler) throws CarbonDataLoadingException {
    if (null != dataHandler) {
      try {
        dataHandler.closeHandler();
      } catch (CarbonDataWriterException e) {
        LOGGER.error(e.getMessage(), e);
        throw new CarbonDataLoadingException(e.getMessage());
      } catch (Exception e) {
        LOGGER.error(e.getMessage(), e);
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
  private CarbonRow convertRow(CarbonRow row) throws KeyGenException {
    int dictIndex = 0;
    int nonDicIndex = 0;
    int[] dim = new int[this.dimensionCount];
    Object[] nonDicArray = new Object[this.noDictWithComplextCount];
    // read dimension values
    int dimCount = 0;
    for (; dimCount < isNoDictionaryDimensionColumn.length; dimCount++) {
      if (isNoDictionaryDimensionColumn[dimCount]) {
        nonDicArray[nonDicIndex++] = row.getObject(dimCount);
      } else {
        dim[dictIndex++] = (int) row.getObject(dimCount);
      }
    }

    for (; dimCount < this.dimensionWithComplexCount; dimCount++) {
      nonDicArray[nonDicIndex++] = row.getObject(dimCount);
    }

    Object[] measures = new Object[measureCount];
    for (int i = 0; i < this.measureCount; i++) {
      measures[i] = row.getObject(i + this.dimensionWithComplexCount);
    }

    return WriteStepRowUtil.fromColumnCategory(dim, nonDicArray, measures);
  }

  private void processBatch(CarbonRowBatch batch, CarbonFactHandler dataHandler, int iteratorIndex)
      throws CarbonDataLoadingException {
    try {
      while (batch.hasNext()) {
        CarbonRow row = batch.next();
        CarbonRow converted = convertRow(row);
        dataHandler.addDataToStore(converted);
        readCounter[iteratorIndex]++;
      }
      writeCounter[iteratorIndex] += batch.getSize();
    } catch (Exception e) {
      throw new CarbonDataLoadingException(e);
    }
    rowCounter.getAndAdd(batch.getSize());
  }

  class DataWriterRunnable implements Runnable {

    private Iterator<CarbonRowBatch> iterator;
    private int iteratorIndex = 0;

    DataWriterRunnable(Iterator<CarbonRowBatch> iterator, int iteratorIndex) {
      this.iterator = iterator;
      this.iteratorIndex = iteratorIndex;
    }

    @Override public void run() {
      try {
        doExecute(this.iterator, iteratorIndex);
      } catch (IOException e) {
        LOGGER.error(e);
        throw new RuntimeException(e);
      }
    }
  }

  @Override public void close() {
    if (!closed) {
      super.close();
      if (null != executorService) {
        executorService.shutdownNow();
      }
      if (null != this.carbonFactHandlers && !this.carbonFactHandlers.isEmpty()) {
        for (CarbonFactHandler carbonFactHandler : this.carbonFactHandlers) {
          carbonFactHandler.finish();
          carbonFactHandler.closeHandler();
        }
      }
    }
  }
}
