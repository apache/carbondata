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
package org.apache.carbondata.processing.newflow.steps;

import java.io.File;
import java.util.Iterator;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.carbon.CarbonTableIdentifier;
import org.apache.carbondata.core.carbon.datastore.block.SegmentProperties;
import org.apache.carbondata.core.constants.IgnoreDictionary;
import org.apache.carbondata.core.keygenerator.KeyGenerator;
import org.apache.carbondata.core.util.CarbonTimeStatisticsFactory;
import org.apache.carbondata.processing.newflow.AbstractDataLoadProcessorStep;
import org.apache.carbondata.processing.newflow.CarbonDataLoadConfiguration;
import org.apache.carbondata.processing.newflow.DataField;
import org.apache.carbondata.processing.newflow.exception.CarbonDataLoadingException;
import org.apache.carbondata.processing.newflow.row.CarbonRow;
import org.apache.carbondata.processing.newflow.row.CarbonRowBatch;
import org.apache.carbondata.processing.store.CarbonFactDataHandlerModel;
import org.apache.carbondata.processing.store.CarbonFactHandler;
import org.apache.carbondata.processing.store.CarbonFactHandlerFactory;
import org.apache.carbondata.processing.store.writer.exception.CarbonDataWriterException;
import org.apache.carbondata.processing.util.CarbonDataProcessorUtil;

/**
 * It reads data from sorted files which are generated in previous sort step.
 * And it writes data to carbondata file. It also generates mdk key while writing to carbondata file
 */
public class DataWriterProcessorStepImpl extends AbstractDataLoadProcessorStep {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(DataWriterProcessorStepImpl.class.getName());

  private SegmentProperties segmentProperties;

  private KeyGenerator keyGenerator;

  private int noDictionaryCount;

  private int complexDimensionCount;

  private int measureCount;

  private long readCounter;

  private long writeCounter;

  private int measureIndex = IgnoreDictionary.MEASURES_INDEX_IN_ROW.getIndex();

  private int noDimByteArrayIndex = IgnoreDictionary.BYTE_ARRAY_INDEX_IN_ROW.getIndex();

  private int dimsArrayIndex = IgnoreDictionary.DIMENSION_INDEX_IN_ROW.getIndex();

  public DataWriterProcessorStepImpl(CarbonDataLoadConfiguration configuration,
      AbstractDataLoadProcessorStep child) {
    super(configuration, child);
  }

  @Override public DataField[] getOutput() {
    return child.getOutput();
  }

  @Override public void initialize() throws CarbonDataLoadingException {
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
      CarbonFactDataHandlerModel dataHandlerModel = CarbonFactDataHandlerModel
          .createCarbonFactDataHandlerModel(configuration,
              getStoreLocation(tableIdentifier, String.valueOf(0)), 0);
      noDictionaryCount = dataHandlerModel.getNoDictionaryCount();
      complexDimensionCount = configuration.getComplexDimensionCount();
      measureCount = dataHandlerModel.getMeasureCount();
      segmentProperties = dataHandlerModel.getSegmentProperties();
      keyGenerator = segmentProperties.getDimensionKeyGenerator();

      CarbonTimeStatisticsFactory.getLoadStatisticsInstance()
          .recordDictionaryValue2MdkAdd2FileTime(configuration.getPartitionId(),
              System.currentTimeMillis());
      int i = 0;
      for (Iterator<CarbonRowBatch> iterator : iterators) {
        String storeLocation = getStoreLocation(tableIdentifier, String.valueOf(i));
        CarbonFactDataHandlerModel model = CarbonFactDataHandlerModel
            .createCarbonFactDataHandlerModel(configuration, storeLocation, i);
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
          finish(tableName, dataHandler);
        }
        i++;
      }

    } catch (CarbonDataWriterException e) {
      LOGGER.error(e, "Failed for table: " + tableName + " in DataWriterProcessorStepImpl");
      throw new CarbonDataLoadingException(
          "Error while initializing data handler : " + e.getMessage());
    } catch (Exception e) {
      LOGGER.error(e, "Failed for table: " + tableName + " in DataWriterProcessorStepImpl");
      throw new CarbonDataLoadingException("There is an unexpected error: " + e.getMessage());
    }
    return null;
  }

  @Override public void close() {

  }

  private void finish(String tableName, CarbonFactHandler dataHandler) {
    try {
      dataHandler.finish();
    } catch (Exception e) {
      LOGGER.error(e, "Failed for table: " + tableName + " in  finishing data handler");
    }
    LOGGER.info("Record Processed For table: " + tableName);
    String logMessage =
        "Finished Carbon DataWriterProcessorStepImpl: Read: " + readCounter + ": Write: "
            + writeCounter;
    LOGGER.info(logMessage);
    CarbonTimeStatisticsFactory.getLoadStatisticsInstance().recordTotalRecords(writeCounter);
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

  private void processBatch(CarbonRowBatch batch, CarbonFactHandler dataHandler)
      throws CarbonDataLoadingException {
    Iterator<CarbonRow> iterator = batch.getBatchIterator();
    try {
      while (iterator.hasNext()) {
        CarbonRow row = iterator.next();
        readCounter++;
        Object[] outputRow;
        // adding one for the high cardinality dims byte array.
        if (noDictionaryCount > 0 || complexDimensionCount > 0) {
          outputRow = new Object[measureCount + 1 + 1];
        } else {
          outputRow = new Object[measureCount + 1];
        }

        int l = 0;
        int index = 0;
        Object[] measures = row.getObjectArray(measureIndex);
        for (int i = 0; i < measureCount; i++) {
          outputRow[l++] = measures[index++];
        }
        outputRow[l] = row.getObject(noDimByteArrayIndex);

        int[] highCardExcludedRows = new int[segmentProperties.getDimColumnsCardinality().length];
        int[] dimsArray = row.getIntArray(dimsArrayIndex);
        for (int i = 0; i < highCardExcludedRows.length; i++) {
          highCardExcludedRows[i] = dimsArray[i];
        }

        outputRow[outputRow.length - 1] = keyGenerator.generateKey(highCardExcludedRows);
        dataHandler.addDataToStore(outputRow);
        writeCounter++;
      }
    } catch (Exception e) {
      throw new CarbonDataLoadingException("unable to generate the mdkey", e);
    }
  }

  @Override protected CarbonRow processRow(CarbonRow row) {
    return null;
  }

}
