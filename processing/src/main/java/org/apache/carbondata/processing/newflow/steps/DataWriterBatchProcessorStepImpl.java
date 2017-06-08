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
package org.apache.carbondata.processing.newflow.steps;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datastore.row.CarbonRow;
import org.apache.carbondata.core.metadata.CarbonTableIdentifier;
import org.apache.carbondata.core.util.CarbonTimeStatisticsFactory;
import org.apache.carbondata.processing.newflow.AbstractDataLoadProcessorStep;
import org.apache.carbondata.processing.newflow.CarbonDataLoadConfiguration;
import org.apache.carbondata.processing.newflow.DataField;
import org.apache.carbondata.processing.newflow.exception.CarbonDataLoadingException;
import org.apache.carbondata.processing.newflow.row.CarbonRowBatch;
import org.apache.carbondata.processing.store.CarbonFactDataHandlerModel;
import org.apache.carbondata.processing.store.CarbonFactHandler;
import org.apache.carbondata.processing.store.CarbonFactHandlerFactory;
import org.apache.carbondata.processing.util.CarbonDataProcessorUtil;

/**
 * It reads data from batch of sorted files(it could be in-memory/disk based files)
 * which are generated in previous sort step. And it writes data to carbondata file.
 * It also generates mdk key while writing to carbondata file
 */
public class DataWriterBatchProcessorStepImpl extends AbstractDataLoadProcessorStep {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(DataWriterBatchProcessorStepImpl.class.getName());

  public DataWriterBatchProcessorStepImpl(CarbonDataLoadConfiguration configuration,
      AbstractDataLoadProcessorStep child) {
    super(configuration, child);
  }

  @Override public DataField[] getOutput() {
    return child.getOutput();
  }

  @Override public void initialize() throws IOException {
    child.initialize();
  }

  private String getStoreLocation(CarbonTableIdentifier tableIdentifier, String partitionId) {
    String storeLocation = CarbonDataProcessorUtil
        .getLocalDataFolderLocation(tableIdentifier.getDatabaseName(),
            tableIdentifier.getTableName(), String.valueOf(configuration.getTaskNo()), partitionId,
            configuration.getSegmentId() + "", false);
    new File(storeLocation).mkdirs();
    return storeLocation;
  }

  @Override public Iterator<CarbonRowBatch>[] execute() throws CarbonDataLoadingException {
    Iterator<CarbonRowBatch>[] iterators = child.execute();
    CarbonTableIdentifier tableIdentifier =
        configuration.getTableIdentifier().getCarbonTableIdentifier();
    String tableName = tableIdentifier.getTableName();
    try {
      CarbonTimeStatisticsFactory.getLoadStatisticsInstance()
          .recordDictionaryValue2MdkAdd2FileTime(configuration.getPartitionId(),
              System.currentTimeMillis());
      int i = 0;
      for (Iterator<CarbonRowBatch> iterator : iterators) {
        String storeLocation = getStoreLocation(tableIdentifier, String.valueOf(i));
        int k = 0;
        while (iterator.hasNext()) {
          CarbonRowBatch next = iterator.next();
          CarbonFactDataHandlerModel model = CarbonFactDataHandlerModel
              .createCarbonFactDataHandlerModel(configuration, storeLocation, i, k++);
          CarbonFactHandler dataHandler = CarbonFactHandlerFactory
              .createCarbonFactHandler(model, CarbonFactHandlerFactory.FactHandlerType.COLUMNAR);
          dataHandler.initialise();
          processBatch(next, dataHandler);
          finish(tableName, dataHandler);
        }
        i++;
      }
    } catch (Exception e) {
      LOGGER.error(e, "Failed for table: " + tableName + " in DataWriterBatchProcessorStepImpl");
      throw new CarbonDataLoadingException("There is an unexpected error: " + e.getMessage());
    }
    return null;
  }

  @Override protected String getStepName() {
    return "Data Batch Writer";
  }

  private void finish(String tableName, CarbonFactHandler dataHandler) {
    try {
      dataHandler.finish();
    } catch (Exception e) {
      LOGGER.error(e, "Failed for table: " + tableName + " in  finishing data handler");
    }
    CarbonTimeStatisticsFactory.getLoadStatisticsInstance().recordTotalRecords(rowCounter.get());
    processingComplete(dataHandler);
    CarbonTimeStatisticsFactory.getLoadStatisticsInstance()
        .recordDictionaryValue2MdkAdd2FileTime(configuration.getPartitionId(),
            System.currentTimeMillis());
    CarbonTimeStatisticsFactory.getLoadStatisticsInstance()
        .recordMdkGenerateTotalTime(configuration.getPartitionId(), System.currentTimeMillis());
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
    rowCounter.getAndAdd(batchSize);
  }

  @Override protected CarbonRow processRow(CarbonRow row) {
    return null;
  }

}
