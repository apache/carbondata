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
import org.apache.carbondata.core.keygenerator.KeyGenException;
import org.apache.carbondata.core.metadata.CarbonTableIdentifier;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.util.CarbonThreadFactory;
import org.apache.carbondata.core.util.CarbonTimeStatisticsFactory;
import org.apache.carbondata.processing.loading.AbstractDataLoadProcessorStep;
import org.apache.carbondata.processing.loading.CarbonDataLoadConfiguration;
import org.apache.carbondata.processing.loading.DataField;
import org.apache.carbondata.processing.loading.exception.BadRecordFoundException;
import org.apache.carbondata.processing.loading.exception.CarbonDataLoadingException;
import org.apache.carbondata.processing.loading.row.CarbonRowBatch;
import org.apache.carbondata.processing.store.CarbonFactDataHandlerModel;
import org.apache.carbondata.processing.store.CarbonFactHandler;
import org.apache.carbondata.processing.store.CarbonFactHandlerFactory;
import org.apache.carbondata.processing.util.CarbonDataProcessorUtil;

/**
 * It reads data from sorted files which are generated in previous sort step.
 * And it writes data to carbondata file. It also generates mdk key while writing to carbondata file
 */
public class CarbonRowDataWriterProcessorStepImpl extends AbstractDataLoadProcessorStep {

  private static final LogService LOGGER =
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

  public CarbonRowDataWriterProcessorStepImpl(CarbonDataLoadConfiguration configuration,
      AbstractDataLoadProcessorStep child) {
    super(configuration, child);
  }

  @Override public DataField[] getOutput() {
    return child.getOutput();
  }

  @Override public void initialize() throws IOException {
    super.initialize();
    child.initialize();
  }

  private String[] getStoreLocation(CarbonTableIdentifier tableIdentifier, String partitionId) {
    String[] storeLocation = CarbonDataProcessorUtil
        .getLocalDataFolderLocation(tableIdentifier.getDatabaseName(),
            tableIdentifier.getTableName(), String.valueOf(configuration.getTaskNo()), partitionId,
            configuration.getSegmentId() + "", false, false);
    CarbonDataProcessorUtil.createLocations(storeLocation);
    return storeLocation;
  }

  @Override public Iterator<CarbonRowBatch>[] execute() throws CarbonDataLoadingException {
    final Iterator<CarbonRowBatch>[] iterators = child.execute();
    tableIdentifier = configuration.getTableIdentifier().getCarbonTableIdentifier();
    tableName = tableIdentifier.getTableName();
    ExecutorService executorService = null;
    try {
      readCounter = new long[iterators.length];
      writeCounter = new long[iterators.length];
      dimensionWithComplexCount = configuration.getDimensionCount();
      noDictWithComplextCount =
          configuration.getNoDictionaryCount() + configuration.getComplexColumnCount();
      dimensionCount = configuration.getDimensionCount() - noDictWithComplextCount;
      isNoDictionaryDimensionColumn =
          CarbonDataProcessorUtil.getNoDictionaryMapping(configuration.getDataFields());
      measureDataType = configuration.getMeasureDataType();
      measureCount = configuration.getMeasureCount();
      outputLength = measureCount + (this.noDictWithComplextCount > 0 ? 1 : 0) + 1;
      CarbonTimeStatisticsFactory.getLoadStatisticsInstance()
          .recordDictionaryValue2MdkAdd2FileTime(configuration.getPartitionId(),
              System.currentTimeMillis());

      if (iterators.length == 1) {
        doExecute(iterators[0], 0, 0);
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
      LOGGER.error(e, "Failed for table: " + tableName + " in DataWriterProcessorStepImpl");
      throw new CarbonDataLoadingException(
          "Error while initializing data handler : " + e.getMessage());
    } catch (Exception e) {
      LOGGER.error(e, "Failed for table: " + tableName + " in DataWriterProcessorStepImpl");
      if (e instanceof BadRecordFoundException) {
        throw new BadRecordFoundException(e.getMessage(), e);
      }
      throw new CarbonDataLoadingException("There is an unexpected error: " + e.getMessage(), e);
    } finally {
      if (null != executorService && executorService.isShutdown()) {
        executorService.shutdownNow();
      }
    }
    return null;
  }

  private void doExecute(Iterator<CarbonRowBatch> iterator, int partitionId, int iteratorIndex) {
    String[] storeLocation = getStoreLocation(tableIdentifier, String.valueOf(partitionId));
    CarbonFactDataHandlerModel model = CarbonFactDataHandlerModel
        .createCarbonFactDataHandlerModel(configuration, storeLocation, partitionId,
            iteratorIndex);
    CarbonFactHandler dataHandler = null;
    boolean rowsNotExist = true;
    while (iterator.hasNext()) {
      if (rowsNotExist) {
        rowsNotExist = false;
        dataHandler = CarbonFactHandlerFactory
            .createCarbonFactHandler(model, CarbonFactHandlerFactory.FactHandlerType.COLUMNAR);
        dataHandler.initialise();
      }
      processBatch(iterator.next(), dataHandler, iteratorIndex);
    }
    if (!rowsNotExist) {
      finish(dataHandler, iteratorIndex);
    }
  }

  @Override protected String getStepName() {
    return "Data Writer";
  }

  private void finish(CarbonFactHandler dataHandler, int iteratorIndex) {
    try {
      dataHandler.finish();
    } catch (Exception e) {
      LOGGER.error(e, "Failed for table: " + tableName + " in  finishing data handler");
    }
    LOGGER.info("Record Processed For table: " + tableName);
    String logMessage =
        "Finished Carbon DataWriterProcessorStepImpl: Read: " + readCounter[iteratorIndex]
            + ": Write: " + readCounter[iteratorIndex];
    LOGGER.info(logMessage);
    CarbonTimeStatisticsFactory.getLoadStatisticsInstance().recordTotalRecords(rowCounter.get());
    processingComplete(dataHandler);
    CarbonTimeStatisticsFactory.getLoadStatisticsInstance()
        .recordDictionaryValue2MdkAdd2FileTime(configuration.getPartitionId(),
            System.currentTimeMillis());
    CarbonTimeStatisticsFactory.getLoadStatisticsInstance()
        .recordMdkGenerateTotalTime(configuration.getPartitionId(), System.currentTimeMillis());
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
  private CarbonRow convertRow(CarbonRow row) throws KeyGenException {
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
      throw new CarbonDataLoadingException("unable to generate the mdkey", e);
    }
    rowCounter.getAndAdd(batch.getSize());
  }

  @Override protected CarbonRow processRow(CarbonRow row) {
    return null;
  }

  class DataWriterRunnable implements Runnable {

    private Iterator<CarbonRowBatch> iterator;
    private int iteratorIndex = 0;

    DataWriterRunnable(Iterator<CarbonRowBatch> iterator, int iteratorIndex) {
      this.iterator = iterator;
      this.iteratorIndex = iteratorIndex;
    }

    @Override public void run() {
      doExecute(this.iterator, 0, iteratorIndex);
    }
  }
}
