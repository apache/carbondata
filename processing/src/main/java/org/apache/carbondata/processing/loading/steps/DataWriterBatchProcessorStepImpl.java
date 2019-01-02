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
import java.util.Map;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datastore.exception.CarbonDataWriterException;
import org.apache.carbondata.core.datastore.row.CarbonRow;
import org.apache.carbondata.core.localdictionary.generator.LocalDictionaryGenerator;
import org.apache.carbondata.core.metadata.CarbonTableIdentifier;
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
 * It reads data from batch of sorted files(it could be in-memory/disk based files)
 * which are generated in previous sort step. And it writes data to carbondata file.
 * It also generates mdk key while writing to carbondata file
 */
public class DataWriterBatchProcessorStepImpl extends AbstractDataLoadProcessorStep {

  private static final Logger LOGGER =
      LogServiceFactory.getLogService(DataWriterBatchProcessorStepImpl.class.getName());

  private Map<String, LocalDictionaryGenerator> localDictionaryGeneratorMap;

  private CarbonFactHandler carbonFactHandler;

  public DataWriterBatchProcessorStepImpl(CarbonDataLoadConfiguration configuration,
      AbstractDataLoadProcessorStep child) {
    super(configuration, child);
    this.localDictionaryGeneratorMap =
        CarbonUtil.getLocalDictionaryModel(configuration.getTableSpec().getCarbonTable());
  }

  @Override public void initialize() throws IOException {
    super.initialize();
    child.initialize();
  }

  private String[] getStoreLocation() {
    return CarbonDataProcessorUtil
        .getLocalDataFolderLocation(configuration.getTableSpec().getCarbonTable(),
            String.valueOf(configuration.getTaskNo()), configuration.getSegmentId(), false, false);
  }

  @Override public Iterator<CarbonRowBatch>[] execute() throws CarbonDataLoadingException {
    Iterator<CarbonRowBatch>[] iterators = child.execute();
    CarbonTableIdentifier tableIdentifier =
        configuration.getTableIdentifier().getCarbonTableIdentifier();
    String tableName = tableIdentifier.getTableName();
    try {
      CarbonTimeStatisticsFactory.getLoadStatisticsInstance()
          .recordDictionaryValue2MdkAdd2FileTime(CarbonTablePath.DEPRECATED_PARTITION_ID,
              System.currentTimeMillis());
      int i = 0;
      String[] storeLocation = getStoreLocation();
      CarbonDataProcessorUtil.createLocations(storeLocation);
      for (Iterator<CarbonRowBatch> iterator : iterators) {
        int k = 0;
        while (iterator.hasNext()) {
          CarbonRowBatch next = iterator.next();
          // If no rows from merge sorter, then don't create a file in fact column handler
          if (next.hasNext()) {
            DataMapWriterListener listener = getDataMapWriterListener(0);
            CarbonFactDataHandlerModel model = CarbonFactDataHandlerModel
                .createCarbonFactDataHandlerModel(configuration, storeLocation, i, k++, listener);
            model.setColumnLocalDictGenMap(this.localDictionaryGeneratorMap);
            this.carbonFactHandler = CarbonFactHandlerFactory
                .createCarbonFactHandler(model);
            carbonFactHandler.initialise();
            processBatch(next, carbonFactHandler);
            try {
              finish(tableName, carbonFactHandler);
            } finally {
              // we need to make carbonFactHandler =null as finish will call closehandler
              // even finish throws exception
              // otherwise close() will call finish method again for same handler.
              this.carbonFactHandler = null;
            }
          }
        }
        i++;
      }
    } catch (Exception e) {
      LOGGER.error("Failed for table: " + tableName + " in DataWriterBatchProcessorStepImpl", e);
      if (e.getCause() instanceof BadRecordFoundException) {
        throw new BadRecordFoundException(e.getCause().getMessage());
      }
      throw new CarbonDataLoadingException("There is an unexpected error: " + e.getMessage());
    }
    return null;
  }

  @Override protected String getStepName() {
    return "Data Batch Writer";
  }

  private void finish(String tableName, CarbonFactHandler dataHandler) {
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
    CarbonTimeStatisticsFactory.getLoadStatisticsInstance().recordTotalRecords(rowCounter.get());
    try {
      processingComplete(dataHandler);
    } catch (Exception e) {
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

  private void processingComplete(CarbonFactHandler dataHandler) {
    if (null != dataHandler) {
      try {
        dataHandler.closeHandler();
      } catch (Exception e) {
        LOGGER.error(e);
        throw new CarbonDataLoadingException(
            "There is an unexpected error while closing data handler", e);
      }
    }
  }

  private void processBatch(CarbonRowBatch batch, CarbonFactHandler dataHandler) throws Exception {
    int batchSize = 0;
    while (batch.hasNext()) {
      CarbonRow row = batch.next();
      dataHandler.addDataToStore(row);
      batchSize++;
    }
    batch.close();
    rowCounter.getAndAdd(batchSize);
  }

  @Override public void close() {
    if (!closed) {
      super.close();
      if (null != this.carbonFactHandler) {
        carbonFactHandler.finish();
        carbonFactHandler.closeHandler();
      }
    }
  }
}
