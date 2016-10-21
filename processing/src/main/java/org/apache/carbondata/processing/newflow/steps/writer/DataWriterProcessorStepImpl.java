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
package org.apache.carbondata.processing.newflow.steps.writer;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.carbon.CarbonTableIdentifier;
import org.apache.carbondata.core.carbon.datastore.block.SegmentProperties;
import org.apache.carbondata.core.carbon.metadata.CarbonMetadata;
import org.apache.carbondata.core.carbon.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.carbon.path.CarbonStorePath;
import org.apache.carbondata.core.carbon.path.CarbonTablePath;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.constants.IgnoreDictionary;
import org.apache.carbondata.core.keygenerator.KeyGenerator;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonTimeStatisticsFactory;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.CarbonUtilException;
import org.apache.carbondata.processing.datatypes.GenericDataType;
import org.apache.carbondata.processing.newflow.AbstractDataLoadProcessorStep;
import org.apache.carbondata.processing.newflow.CarbonDataLoadConfiguration;
import org.apache.carbondata.processing.newflow.DataField;
import org.apache.carbondata.processing.newflow.constants.DataLoadProcessorConstants;
import org.apache.carbondata.processing.newflow.exception.CarbonDataLoadingException;
import org.apache.carbondata.processing.newflow.row.CarbonRow;
import org.apache.carbondata.processing.newflow.row.CarbonRowBatch;
import org.apache.carbondata.processing.store.CarbonDataFileAttributes;
import org.apache.carbondata.processing.store.CarbonFactDataHandlerColumnar;
import org.apache.carbondata.processing.store.CarbonFactDataHandlerModel;
import org.apache.carbondata.processing.store.CarbonFactHandler;
import org.apache.carbondata.processing.store.writer.exception.CarbonDataWriterException;
import org.apache.carbondata.processing.util.CarbonDataProcessorUtil;

/**
 * It reads data from sorted files which are generated in previous sort step.
 * And it writes data to carbondata file. It also generates mdk key while writing to carbondata file
 */
public class DataWriterProcessorStepImpl extends AbstractDataLoadProcessorStep {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(DataWriterProcessorStepImpl.class.getName());

  private String storeLocation;

  private boolean[] isUseInvertedIndex;

  private int[] dimLens;

  private int dimensionCount;

  private List<ColumnSchema> wrapperColumnSchema;

  private int[] colCardinality;

  private SegmentProperties segmentProperties;

  private KeyGenerator keyGenerator;

  private CarbonFactHandler dataHandler;

  private Map<Integer, GenericDataType> complexIndexMap;

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
    CarbonTableIdentifier tableIdentifier =
        configuration.getTableIdentifier().getCarbonTableIdentifier();

    storeLocation = CarbonDataProcessorUtil
        .getLocalDataFolderLocation(tableIdentifier.getDatabaseName(),
            tableIdentifier.getTableName(), String.valueOf(configuration.getTaskNo()),
            configuration.getPartitionId(), configuration.getSegmentId() + "", false);
    isUseInvertedIndex =
        CarbonDataProcessorUtil.getIsUseInvertedIndex(configuration.getDataFields());

    if (!(new File(storeLocation).exists())) {
      LOGGER.error("Local data load folder location does not exist: " + storeLocation);
      return;
    }

    int[] dimLensWithComplex =
        (int[]) configuration.getDataLoadProperty(DataLoadProcessorConstants.DIMENSION_LENGTHS);
    List<Integer> dimsLenList = new ArrayList<Integer>();
    for (int eachDimLen : dimLensWithComplex) {
      if (eachDimLen != 0) dimsLenList.add(eachDimLen);
    }
    dimLens = new int[dimsLenList.size()];
    for (int i = 0; i < dimsLenList.size(); i++) {
      dimLens[i] = dimsLenList.get(i);
    }

    this.dimensionCount = configuration.getDimensionCount();
    this.noDictionaryCount = configuration.getNoDictionaryCount();
    this.complexDimensionCount = configuration.getComplexDimensionCount();
    this.measureCount = configuration.getMeasureCount();

    int simpleDimsCount = this.dimensionCount - complexDimensionCount;
    int[] simpleDimsLen = new int[simpleDimsCount];
    for (int i = 0; i < simpleDimsCount; i++) {
      simpleDimsLen[i] = dimLens[i];
    }

    CarbonTable carbonTable = CarbonMetadata.getInstance().getCarbonTable(
        tableIdentifier.getDatabaseName() + CarbonCommonConstants.UNDERSCORE + tableIdentifier
            .getTableName());
    wrapperColumnSchema = CarbonUtil
        .getColumnSchemaList(carbonTable.getDimensionByTableName(tableIdentifier.getTableName()),
            carbonTable.getMeasureByTableName(tableIdentifier.getTableName()));
    colCardinality = CarbonUtil.getFormattedCardinality(dimLensWithComplex, wrapperColumnSchema);
    segmentProperties = new SegmentProperties(wrapperColumnSchema, colCardinality);
    // Actual primitive dimension used to generate start & end key

    keyGenerator = segmentProperties.getDimensionKeyGenerator();

    //To Set MDKey Index of each primitive type in complex type
    int surrIndex = simpleDimsCount;
    Iterator<Map.Entry<String, GenericDataType>> complexMap =
        CarbonDataProcessorUtil.getComplexTypesMap(configuration.getDataFields()).entrySet()
            .iterator();
    complexIndexMap = new HashMap<Integer, GenericDataType>(complexDimensionCount);
    while (complexMap.hasNext()) {
      Map.Entry<String, GenericDataType> complexDataType = complexMap.next();
      complexDataType.getValue().setOutputArrayIndex(0);
      complexIndexMap.put(simpleDimsCount, complexDataType.getValue());
      simpleDimsCount++;
      List<GenericDataType> primitiveTypes = new ArrayList<GenericDataType>();
      complexDataType.getValue().getAllPrimitiveChildren(primitiveTypes);
      for (GenericDataType eachPrimitive : primitiveTypes) {
        eachPrimitive.setSurrogateIndex(surrIndex++);
      }
    }
  }

  private void initDataHandler() {
    int simpleDimsCount = dimensionCount - complexDimensionCount;
    int[] simpleDimsLen = new int[simpleDimsCount];
    for (int i = 0; i < simpleDimsCount; i++) {
      simpleDimsLen[i] = dimLens[i];
    }
    CarbonDataFileAttributes carbonDataFileAttributes =
        new CarbonDataFileAttributes(Integer.parseInt(configuration.getTaskNo()),
            (String) configuration.getDataLoadProperty(DataLoadProcessorConstants.FACT_TIME_STAMP));
    String carbonDataDirectoryPath = getCarbonDataFolderLocation();
    CarbonFactDataHandlerModel carbonFactDataHandlerModel = getCarbonFactDataHandlerModel();
    carbonFactDataHandlerModel.setPrimitiveDimLens(simpleDimsLen);
    carbonFactDataHandlerModel.setCarbonDataFileAttributes(carbonDataFileAttributes);
    carbonFactDataHandlerModel.setCarbonDataDirectoryPath(carbonDataDirectoryPath);
    carbonFactDataHandlerModel.setIsUseInvertedIndex(isUseInvertedIndex);
    if (noDictionaryCount > 0 || complexDimensionCount > 0) {
      carbonFactDataHandlerModel.setMdKeyIndex(measureCount + 1);
    } else {
      carbonFactDataHandlerModel.setMdKeyIndex(measureCount);
    }
    dataHandler = new CarbonFactDataHandlerColumnar(carbonFactDataHandlerModel);
  }

  /**
   * This method will create a model object for carbon fact data handler
   *
   * @return
   */
  private CarbonFactDataHandlerModel getCarbonFactDataHandlerModel() {
    CarbonFactDataHandlerModel carbonFactDataHandlerModel = new CarbonFactDataHandlerModel();
    carbonFactDataHandlerModel.setDatabaseName(
        configuration.getTableIdentifier().getCarbonTableIdentifier().getDatabaseName());
    carbonFactDataHandlerModel
        .setTableName(configuration.getTableIdentifier().getCarbonTableIdentifier().getTableName());
    carbonFactDataHandlerModel.setMeasureCount(measureCount);
    carbonFactDataHandlerModel.setMdKeyLength(keyGenerator.getKeySizeInBytes());
    carbonFactDataHandlerModel.setStoreLocation(configuration.getTableIdentifier().getStorePath());
    carbonFactDataHandlerModel.setDimLens(dimLens);
    carbonFactDataHandlerModel.setNoDictionaryCount(noDictionaryCount);
    carbonFactDataHandlerModel.setDimensionCount(configuration.getDimensionCount());
    carbonFactDataHandlerModel.setComplexIndexMap(complexIndexMap);
    carbonFactDataHandlerModel.setSegmentProperties(segmentProperties);
    carbonFactDataHandlerModel.setColCardinality(colCardinality);
    carbonFactDataHandlerModel.setDataWritingRequest(true);
    carbonFactDataHandlerModel.setAggType(null);
    carbonFactDataHandlerModel.setFactDimLens(dimLens);
    carbonFactDataHandlerModel.setWrapperColumnSchema(wrapperColumnSchema);
    return carbonFactDataHandlerModel;
  }

  /**
   * This method will get the store location for the given path, segment id and partition id
   *
   * @return data directory path
   */
  private String getCarbonDataFolderLocation() {
    String carbonStorePath =
        CarbonProperties.getInstance().getProperty(CarbonCommonConstants.STORE_LOCATION_HDFS);
    CarbonTableIdentifier tableIdentifier =
        configuration.getTableIdentifier().getCarbonTableIdentifier();
    CarbonTable carbonTable = CarbonMetadata.getInstance().getCarbonTable(
        tableIdentifier.getDatabaseName() + CarbonCommonConstants.UNDERSCORE + tableIdentifier
            .getTableName());
    CarbonTablePath carbonTablePath =
        CarbonStorePath.getCarbonTablePath(carbonStorePath, carbonTable.getCarbonTableIdentifier());
    String carbonDataDirectoryPath = carbonTablePath
        .getCarbonDataDirectoryPath(configuration.getPartitionId(),
            configuration.getSegmentId() + "");
    return carbonDataDirectoryPath;
  }

  @Override public Iterator<CarbonRowBatch>[] execute() throws CarbonDataLoadingException {
    Iterator<CarbonRowBatch>[] iterators = child.execute();
    String tableName = configuration.getTableIdentifier().getCarbonTableIdentifier().getTableName();
    try {
      initDataHandler();
      dataHandler.initialise();
      CarbonTimeStatisticsFactory.getLoadStatisticsInstance()
          .recordDictionaryValue2MdkAdd2FileTime(configuration.getPartitionId(),
              System.currentTimeMillis());
      for (int i = 0; i < iterators.length; i++) {
        Iterator<CarbonRowBatch> iterator = iterators[i];
        while (iterator.hasNext()) {
          processBatch(iterator.next());
        }
      }

    } catch (CarbonDataWriterException e) {
      LOGGER.error(e, "Failed for table: " + tableName + " in DataWriterProcessorStepImpl");
      throw new CarbonDataLoadingException(
          "Error while initializing data handler : " + e.getMessage());
    } catch (Exception e) {
      LOGGER.error(e, "Failed for table: " + tableName + " in DataWriterProcessorStepImpl");
      throw new CarbonDataLoadingException("There is an unexpected error: " + e.getMessage());
    } finally {
      try {
        dataHandler.finish();
      } catch (CarbonDataWriterException e) {
        LOGGER.error(e, "Failed for table: " + tableName + " in  finishing data handler");
      } catch (Exception e) {
        LOGGER.error(e, "Failed for table: " + tableName + " in  finishing data handler");
      }
    }
    LOGGER.info("Record Procerssed For table: " + tableName);
    String logMessage =
        "Finished Carbon DataWriterProcessorStepImpl: Read: " + readCounter + ": Write: "
            + writeCounter;
    LOGGER.info(logMessage);
    CarbonTimeStatisticsFactory.getLoadStatisticsInstance().recordTotalRecords(writeCounter);
    processingComplete();
    CarbonTimeStatisticsFactory.getLoadStatisticsInstance()
        .recordDictionaryValue2MdkAdd2FileTime(configuration.getPartitionId(),
            System.currentTimeMillis());
    CarbonTimeStatisticsFactory.getLoadStatisticsInstance()
        .recordMdkGenerateTotalTime(configuration.getPartitionId(), System.currentTimeMillis());

    return null;
  }

  @Override public void close() {

  }

  private void processingComplete() throws CarbonDataLoadingException {
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

  private void processBatch(CarbonRowBatch batch) throws CarbonDataLoadingException {
    Iterator<CarbonRow> iterator = batch.getBatchIterator();
    try {
      while (iterator.hasNext()) {
        CarbonRow row = iterator.next();
        readCounter++;
        Object[] outputRow = null;
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
        outputRow[l] = row.getBinary(noDimByteArrayIndex);

        int[] highCardExcludedRows = new int[segmentProperties.getDimColumnsCardinality().length];
        Integer[] dimsArray = row.getIntegerArray(dimsArrayIndex);
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
