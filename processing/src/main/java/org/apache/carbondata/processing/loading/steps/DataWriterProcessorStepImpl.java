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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datastore.exception.CarbonDataWriterException;
import org.apache.carbondata.core.datastore.row.CarbonRow;
import org.apache.carbondata.core.keygenerator.KeyGenException;
import org.apache.carbondata.core.metadata.CarbonTableIdentifier;
import org.apache.carbondata.core.util.CarbonThreadFactory;
import org.apache.carbondata.core.util.CarbonTimeStatisticsFactory;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.processing.loading.AbstractDataLoadProcessorStep;
import org.apache.carbondata.processing.loading.CarbonDataLoadConfiguration;
import org.apache.carbondata.processing.loading.DataField;
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
public class DataWriterProcessorStepImpl extends AbstractDataLoadProcessorStep {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(DataWriterProcessorStepImpl.class.getName());

  private long readCounter;

  public DataWriterProcessorStepImpl(CarbonDataLoadConfiguration configuration,
      AbstractDataLoadProcessorStep child) {
    super(configuration, child);
  }

  public DataWriterProcessorStepImpl(CarbonDataLoadConfiguration configuration) {
    super(configuration, null);
  }

  @Override public DataField[] getOutput() {
    return child.getOutput();
  }

  @Override public void initialize() throws IOException {
    super.initialize();
    child.initialize();
  }

  private String[] getStoreLocation(CarbonTableIdentifier tableIdentifier) {
    String[] storeLocation = CarbonDataProcessorUtil
        .getLocalDataFolderLocation(tableIdentifier.getDatabaseName(),
            tableIdentifier.getTableName(), String.valueOf(configuration.getTaskNo()),
            configuration.getSegmentId(), false, false);
    CarbonDataProcessorUtil.createLocations(storeLocation);
    return storeLocation;
  }

  public CarbonFactDataHandlerModel getDataHandlerModel() {
    CarbonTableIdentifier tableIdentifier =
        configuration.getTableIdentifier().getCarbonTableIdentifier();
    String[] storeLocation = getStoreLocation(tableIdentifier);
    return CarbonFactDataHandlerModel.createCarbonFactDataHandlerModel(configuration,
        storeLocation, 0, 0);
  }

  @Override public Iterator<CarbonRowBatch>[] execute() throws CarbonDataLoadingException {
    Iterator<CarbonRowBatch>[] iterators = child.execute();
    CarbonTableIdentifier tableIdentifier =
        configuration.getTableIdentifier().getCarbonTableIdentifier();
    String tableName = tableIdentifier.getTableName();
    try {
      CarbonTimeStatisticsFactory.getLoadStatisticsInstance()
          .recordDictionaryValue2MdkAdd2FileTime(CarbonTablePath.DEPRECATED_PATITION_ID,
              System.currentTimeMillis());
      ExecutorService rangeExecutorService = Executors.newFixedThreadPool(iterators.length,
          new CarbonThreadFactory("WriterForwardPool: " + tableName));
      List<Future<Void>> rangeExecutorServiceSubmitList = new ArrayList<>(iterators.length);
      int i = 0;
      // do this concurrently
      for (Iterator<CarbonRowBatch> iterator : iterators) {
        String[] storeLocation = getStoreLocation(tableIdentifier);

        CarbonFactDataHandlerModel model = CarbonFactDataHandlerModel
            .createCarbonFactDataHandlerModel(configuration, storeLocation, i, 0);
        CarbonFactHandler dataHandler = null;
        boolean rowsNotExist = true;
        while (iterator.hasNext()) {
          if (rowsNotExist) {
            rowsNotExist = false;
            dataHandler = CarbonFactHandlerFactory
                .createCarbonFactHandler(model, CarbonFactHandlerFactory.FactHandlerType.COLUMNAR);
            dataHandler.initialise();
          }
          processBatch(iterator.next(), dataHandler);
        }
        if (!rowsNotExist) {
          finish(dataHandler);
        }
        rangeExecutorServiceSubmitList.add(
            rangeExecutorService.submit(new WriterForwarder(iterator, tableIdentifier, i)));
        i++;
      }
      try {
        rangeExecutorService.shutdown();
        rangeExecutorService.awaitTermination(2, TimeUnit.DAYS);
        for (int j = 0; j < rangeExecutorServiceSubmitList.size(); j++) {
          rangeExecutorServiceSubmitList.get(j).get();
        }
      } catch (InterruptedException | ExecutionException e) {
        throw new CarbonDataWriterException(e);
      }
    } catch (CarbonDataWriterException e) {
      LOGGER.error(e, "Failed for table: " + tableName + " in DataWriterProcessorStepImpl");
      throw new CarbonDataLoadingException(
          "Error while initializing data handler : " + e.getMessage());
    } catch (Exception e) {
      LOGGER.error(e, "Failed for table: " + tableName + " in DataWriterProcessorStepImpl");
      throw new CarbonDataLoadingException("There is an unexpected error: " + e.getMessage(), e);
    }
    return null;
  }

  @Override protected String getStepName() {
    return "Data Writer";
  }

  /**
   * Used to forward rows to different ranges based on range id.
   */
  private final class WriterForwarder implements Callable<Void> {
    private Iterator<CarbonRowBatch> insideRangeIterator;
    private CarbonTableIdentifier tableIdentifier;
    private int rangeId;

    public WriterForwarder(Iterator<CarbonRowBatch> insideRangeIterator,
        CarbonTableIdentifier tableIdentifier, int rangeId) {
      this.insideRangeIterator = insideRangeIterator;
      this.tableIdentifier = tableIdentifier;
      this.rangeId = rangeId;
    }

    @Override public Void call() throws Exception {
      LOGGER.info("Process writer forward for table " + tableIdentifier.getTableName()
          + ", range: " + rangeId);
      processRange(insideRangeIterator, tableIdentifier, rangeId);
      return null;
    }
  }

  private void processRange(Iterator<CarbonRowBatch> insideRangeIterator,
      CarbonTableIdentifier tableIdentifier, int rangeId) {
    String[] storeLocation = getStoreLocation(tableIdentifier);

    CarbonFactDataHandlerModel model = CarbonFactDataHandlerModel
        .createCarbonFactDataHandlerModel(configuration, storeLocation, rangeId, 0);
    CarbonFactHandler dataHandler = null;
    boolean rowsNotExist = true;
    while (insideRangeIterator.hasNext()) {
      if (rowsNotExist) {
        rowsNotExist = false;
        dataHandler = CarbonFactHandlerFactory
            .createCarbonFactHandler(model, CarbonFactHandlerFactory.FactHandlerType.COLUMNAR);
        dataHandler.initialise();
      }
      processBatch(insideRangeIterator.next(), dataHandler);
    }
    if (!rowsNotExist) {
      finish(dataHandler);
    }
  }

  public void finish(CarbonFactHandler dataHandler) {
    CarbonTableIdentifier tableIdentifier =
        configuration.getTableIdentifier().getCarbonTableIdentifier();
    String tableName = tableIdentifier.getTableName();

    try {
      dataHandler.finish();
    } catch (Exception e) {
      LOGGER.error(e, "Failed for table: " + tableName + " in  finishing data handler");
    }
    LOGGER.info("Record Processed For table: " + tableName);
    String logMessage =
        "Finished Carbon DataWriterProcessorStepImpl: Read: " + readCounter + ": Write: "
            + rowCounter.get();
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
        throw new CarbonDataLoadingException(e.getMessage(), e);
      } catch (Exception e) {
        LOGGER.error(e, e.getMessage());
        throw new CarbonDataLoadingException("There is an unexpected error: " + e.getMessage());
      }
    }
  }

  private void processBatch(CarbonRowBatch batch, CarbonFactHandler dataHandler)
      throws CarbonDataLoadingException {
    try {
      while (batch.hasNext()) {
        CarbonRow row = batch.next();
        dataHandler.addDataToStore(row);
        readCounter++;
      }
    } catch (Exception e) {
      throw new CarbonDataLoadingException(e);
    }
    rowCounter.getAndAdd(batch.getSize());
  }

  public void processRow(CarbonRow row, CarbonFactHandler dataHandler) throws KeyGenException {
    try {
      readCounter++;
      dataHandler.addDataToStore(row);
    } catch (Exception e) {
      throw new CarbonDataLoadingException("unable to generate the mdkey", e);
    }
    rowCounter.getAndAdd(1);
  }

  @Override protected CarbonRow processRow(CarbonRow row) {
    return null;
  }

}
