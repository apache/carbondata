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

package org.apache.carbondata.processing.mdkeygen;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.common.logging.impl.StandardLogService;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.keygenerator.KeyGenException;
import org.apache.carbondata.core.metadata.CarbonMetadata;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonMeasure;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonTimeStatisticsFactory;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.DataTypeUtil;
import org.apache.carbondata.core.util.path.CarbonStorePath;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.processing.datatypes.GenericDataType;
import org.apache.carbondata.processing.mdkeygen.file.FileData;
import org.apache.carbondata.processing.mdkeygen.file.FileManager;
import org.apache.carbondata.processing.mdkeygen.file.IFileManagerComposite;
import org.apache.carbondata.processing.store.CarbonDataFileAttributes;
import org.apache.carbondata.processing.store.CarbonFactDataHandlerColumnar;
import org.apache.carbondata.processing.store.CarbonFactDataHandlerModel;
import org.apache.carbondata.processing.store.CarbonFactHandler;
import org.apache.carbondata.processing.store.SingleThreadFinalSortFilesMerger;
import org.apache.carbondata.processing.store.writer.exception.CarbonDataWriterException;
import org.apache.carbondata.processing.util.CarbonDataProcessorUtil;
import org.apache.carbondata.processing.util.NonDictionaryUtil;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

public class MDKeyGenStep extends BaseStep {
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(MDKeyGenStep.class.getName());

  /**
   * carbon mdkey generator step data class
   */
  private MDKeyGenStepData data;

  /**
   * carbon mdkey generator step meta
   */
  private MDKeyGenStepMeta meta;

  /**
   * dimension length
   */
  private int dimensionCount;

  /**
   * table name
   */
  private String tableName;

  /**
   * table blocksize in MB
   */
  private int blockSize;

  /**
   * File manager
   */
  private IFileManagerComposite fileManager;

  private Map<Integer, GenericDataType> complexIndexMap;

  private SegmentProperties segmentProperties;

  private int[] colCardinality;

  private List<ColumnSchema> wrapperColumnSchema;

  /**
   * readCounter
   */
  private long readCounter;

  /**
   * writeCounter
   */
  private long writeCounter;

  private int measureCount;

  private String dataFolderLocation;

  private SingleThreadFinalSortFilesMerger finalMerger;

  /**
   * dataHandler
   */
  private CarbonFactHandler dataHandler;

  private char[] aggType;

  private String storeLocation;

  private int[] dimLens;

  /**
   * to check whether dimension is of dictionary type
   * or not
   */
  private boolean[] isNoDictionaryDimension;

  /**
   * to check whether dimension use inverted index
   * or not
   */
  private boolean[] isUseInvertedIndex;

  /**
   * CarbonMDKeyGenStep
   *
   * @param stepMeta
   * @param stepDataInterface
   * @param copyNr
   * @param transMeta
   * @param trans
   */
  public MDKeyGenStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr,
      TransMeta transMeta, Trans trans) {
    super(stepMeta, stepDataInterface, copyNr, transMeta, trans);
  }

  /**
   * Perform the equivalent of processing one row. Typically this means
   * reading a row from input (getRow()) and passing a row to output
   * (putRow)).
   *
   * @param smi The steps metadata to work with
   * @param sdi The steps temporary working data to work with (database
   *            connections, result sets, caches, temporary variables, etc.)
   * @return false if no more rows can be processed or an error occurred.
   * @throws KettleException
   */
  public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException {
    meta = (MDKeyGenStepMeta) smi;
    StandardLogService.setThreadName(meta.getPartitionID(), null);
    data = (MDKeyGenStepData) sdi;

    meta.initialize();
    Object[] row = getRow();
    if (first) {
      CarbonTimeStatisticsFactory.getLoadStatisticsInstance().recordMdkGenerateTotalTime(
          meta.getPartitionID(), System.currentTimeMillis());
      first = false;

      data.outputRowMeta = new RowMeta();
      boolean isExecutionRequired = setStepConfiguration();

      if (!isExecutionRequired) {
        processingComplete();
        return false;
      }
      setStepOutputInterface();
    }

    if (null != row) {
      putRow(data.outputRowMeta, new Object[measureCount + 1]);
      return true;
    }

    try {
      initDataHandler();
      dataHandler.initialise();
      finalMerger.startFinalMerge();
      CarbonTimeStatisticsFactory.getLoadStatisticsInstance().recordDictionaryValue2MdkAdd2FileTime(
              meta.getPartitionID(), System.currentTimeMillis());
      while (finalMerger.hasNext()) {
        Object[] r = finalMerger.next();
        readCounter++;
        Object[] outputRow = process(r);
        dataHandler.addDataToStore(outputRow);
        writeCounter++;
      }
    } catch (CarbonDataWriterException e) {
      LOGGER.error(e, "Failed for table: " + this.tableName + " in MDKeyGenStep");
      throw new KettleException("Error while initializing data handler : " + e.getMessage());
    } catch (Exception e) {
      LOGGER.error(e, "Failed for table: " + this.tableName + " in MDKeyGenStep");
      throw new KettleException("There is an unexpected error: " + e.getMessage());
    } finally {
      try {
        dataHandler.finish();
      } catch (CarbonDataWriterException e) {
        LOGGER.error(e, "Failed for table: " + this.tableName + " in  finishing data handler");
      } catch (Exception e) {
        LOGGER.error(e, "Failed for table: " + this.tableName + " in  finishing data handler");
      }
    }
    LOGGER.info("Record Procerssed For table: " + this.tableName);
    String logMessage =
        "Finished Carbon Mdkey Generation Step: Read: " + readCounter + ": Write: " + writeCounter;
    LOGGER.info(logMessage);
    CarbonTimeStatisticsFactory.getLoadStatisticsInstance().recordTotalRecords(writeCounter);
    processingComplete();
    CarbonTimeStatisticsFactory.getLoadStatisticsInstance().recordDictionaryValue2MdkAdd2FileTime(
        meta.getPartitionID(), System.currentTimeMillis());
    CarbonTimeStatisticsFactory.getLoadStatisticsInstance().recordMdkGenerateTotalTime(
        meta.getPartitionID(), System.currentTimeMillis());
    return false;
  }

  private void processingComplete() throws KettleException {
    if (null != dataHandler) {
      try {
        dataHandler.closeHandler();
      } catch (CarbonDataWriterException e) {
        LOGGER.error(e, e.getMessage());
        throw new KettleException(e.getMessage());
      } catch (Exception e) {
        LOGGER.error(e, e.getMessage());
        throw new KettleException("There is an unexpected error: " + e.getMessage());
      }
    }
    setOutputDone();
  }

  /**
   * This method will be used to get and update the step properties which will
   * required to run this step
   */
  private boolean setStepConfiguration() {
    this.tableName = meta.getTableName();
    storeLocation = CarbonDataProcessorUtil
        .getLocalDataFolderLocation(meta.getDatabaseName(), meta.getTableName(),
            String.valueOf(meta.getTaskNo()), meta.getPartitionID(), meta.getSegmentId() + "",
            false);
    isNoDictionaryDimension =
        NonDictionaryUtil.convertStringToBooleanArr(meta.getNoDictionaryDimsMapping());
    isUseInvertedIndex =
        NonDictionaryUtil.convertStringToBooleanArr(meta.getIsUseInvertedIndex());
    fileManager = new FileManager();
    fileManager.setName(CarbonCommonConstants.LOAD_FOLDER + meta.getSegmentId()
        + CarbonCommonConstants.FILE_INPROGRESS_STATUS);

    if (!(new File(storeLocation).exists())) {
      LOGGER.error("Local data load folder location does not exist: " + storeLocation);
      return false;
    }

    this.meta.setNoDictionaryCount(
        NonDictionaryUtil.extractNoDictionaryCount(this.meta.getNoDictionaryDims()));

    String levelCardinalityFilePath = storeLocation + File.separator +
        CarbonCommonConstants.LEVEL_METADATA_FILE + meta.getTableName()
        + CarbonCommonConstants.CARBON_METADATA_EXTENSION;
    int[] dimLensWithComplex = null;
    try {
      dimLensWithComplex = CarbonUtil.getCardinalityFromLevelMetadataFile(levelCardinalityFilePath);
    } catch (IOException e) {
      LOGGER.error("Level cardinality file :: " + e.getMessage());
      return false;
    }
    if (null == dimLensWithComplex) {
      return false;
    }
    List<Integer> dimsLenList = new ArrayList<Integer>();
    for (int eachDimLen : dimLensWithComplex) {
      if (eachDimLen != 0) dimsLenList.add(eachDimLen);
    }
    dimLens = new int[dimsLenList.size()];
    for (int i = 0; i < dimsLenList.size(); i++) {
      dimLens[i] = dimsLenList.get(i);
    }

    this.dimensionCount = meta.getDimensionCount();

    int simpleDimsCount = this.dimensionCount - meta.getComplexDimsCount();
    int[] simpleDimsLen = new int[simpleDimsCount];
    System.arraycopy(dimLens, 0, simpleDimsLen, 0, simpleDimsCount);

    CarbonTable carbonTable = CarbonMetadata.getInstance()
        .getCarbonTable(meta.getDatabaseName() + CarbonCommonConstants.UNDERSCORE + tableName);
    wrapperColumnSchema = CarbonUtil
        .getColumnSchemaList(carbonTable.getDimensionByTableName(tableName),
            carbonTable.getMeasureByTableName(tableName));
    blockSize = carbonTable.getBlockSizeInMB();
    colCardinality =
        CarbonUtil.getFormattedCardinality(dimLensWithComplex, wrapperColumnSchema);
    segmentProperties = new SegmentProperties(wrapperColumnSchema, colCardinality);
    // Actual primitive dimension used to generate start & end key

    data.generator = segmentProperties.getDimensionKeyGenerator();

    //To Set MDKey Index of each primitive type in complex type
    int surrIndex = simpleDimsCount;
    Iterator<Entry<String, GenericDataType>> complexMap =
        meta.getComplexTypes().entrySet().iterator();
    complexIndexMap = new HashMap<Integer, GenericDataType>(meta.getComplexDimsCount());
    while (complexMap.hasNext()) {
      Entry<String, GenericDataType> complexDataType = complexMap.next();
      complexDataType.getValue().setOutputArrayIndex(0);
      complexIndexMap.put(simpleDimsCount, complexDataType.getValue());
      simpleDimsCount++;
      List<GenericDataType> primitiveTypes = new ArrayList<GenericDataType>();
      complexDataType.getValue().getAllPrimitiveChildren(primitiveTypes);
      for (GenericDataType eachPrimitive : primitiveTypes) {
        eachPrimitive.setSurrogateIndex(surrIndex++);
      }
    }

    this.measureCount = meta.getMeasureCount();

    String metaDataFileName = CarbonCommonConstants.MEASURE_METADATA_FILE_NAME + this.tableName
        + CarbonCommonConstants.MEASUREMETADATA_FILE_EXT
        + CarbonCommonConstants.FILE_INPROGRESS_STATUS;

    FileData fileData = new FileData(metaDataFileName, storeLocation);
    fileManager.add(fileData);
    // Set the data file location
    this.dataFolderLocation =
        storeLocation + File.separator + CarbonCommonConstants.SORT_TEMP_FILE_LOCATION;
    return true;
  }

  private void initDataHandler() {
    int simpleDimsCount = this.dimensionCount - meta.getComplexDimsCount();
    int[] simpleDimsLen = new int[simpleDimsCount];
    System.arraycopy(dimLens, 0, simpleDimsLen, 0, simpleDimsCount);
    CarbonDataFileAttributes carbonDataFileAttributes =
        new CarbonDataFileAttributes(meta.getTaskNo(), meta.getFactTimeStamp());
    initAggType();
    String carbonDataDirectoryPath = getCarbonDataFolderLocation();
    finalMerger = new SingleThreadFinalSortFilesMerger(dataFolderLocation, tableName,
        dimensionCount - meta.getComplexDimsCount(), meta.getComplexDimsCount(), measureCount,
        meta.getNoDictionaryCount(), aggType, isNoDictionaryDimension, true);
    CarbonFactDataHandlerModel carbonFactDataHandlerModel = getCarbonFactDataHandlerModel();
    carbonFactDataHandlerModel.setPrimitiveDimLens(simpleDimsLen);
    carbonFactDataHandlerModel.setCarbonDataFileAttributes(carbonDataFileAttributes);
    carbonFactDataHandlerModel.setCarbonDataDirectoryPath(carbonDataDirectoryPath);
    carbonFactDataHandlerModel.setIsUseInvertedIndex(isUseInvertedIndex);
    if (meta.getNoDictionaryCount() > 0 || meta.getComplexDimsCount() > 0) {
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
    carbonFactDataHandlerModel.setDatabaseName(meta.getDatabaseName());
    carbonFactDataHandlerModel.setTableName(tableName);
    carbonFactDataHandlerModel.setMeasureCount(measureCount);
    carbonFactDataHandlerModel.setMdKeyLength(data.generator.getKeySizeInBytes());
    carbonFactDataHandlerModel.setStoreLocation(storeLocation);
    carbonFactDataHandlerModel.setDimLens(dimLens);
    carbonFactDataHandlerModel.setNoDictionaryCount(meta.getNoDictionaryCount());
    carbonFactDataHandlerModel.setDimensionCount(dimensionCount);
    carbonFactDataHandlerModel.setComplexIndexMap(complexIndexMap);
    carbonFactDataHandlerModel.setSegmentProperties(segmentProperties);
    carbonFactDataHandlerModel.setColCardinality(colCardinality);
    carbonFactDataHandlerModel.setDataWritingRequest(true);
    carbonFactDataHandlerModel.setAggType(aggType);
    carbonFactDataHandlerModel.setFactDimLens(dimLens);
    carbonFactDataHandlerModel.setWrapperColumnSchema(wrapperColumnSchema);
    carbonFactDataHandlerModel.setBlockSizeInMB(blockSize);
    return carbonFactDataHandlerModel;
  }

  private void initAggType() {
    aggType = new char[measureCount];
    Arrays.fill(aggType, 'n');
    CarbonTable carbonTable = CarbonMetadata.getInstance().getCarbonTable(
        meta.getDatabaseName() + CarbonCommonConstants.UNDERSCORE + meta.getTableName());
    List<CarbonMeasure> measures = carbonTable.getMeasureByTableName(meta.getTableName());
    for (int i = 0; i < measureCount; i++) {
      aggType[i] = DataTypeUtil.getAggType(measures.get(i).getDataType());
    }
  }

  /**
   * This method will be used for setting the output interface.
   * Output interface is how this step will process the row to next step
   */
  private void setStepOutputInterface() {
    ValueMetaInterface[] out = new ValueMetaInterface[measureCount + 1];

    for (int i = 0; i < measureCount; i++) {
      out[i] = new ValueMeta("measure" + i, ValueMetaInterface.TYPE_NUMBER,
          ValueMetaInterface.STORAGE_TYPE_NORMAL);
      out[i].setStorageMetadata(new ValueMeta("measure" + i, ValueMetaInterface.TYPE_NUMBER,
          ValueMetaInterface.STORAGE_TYPE_NORMAL));
    }

    out[out.length - 1] = new ValueMeta("id", ValueMetaInterface.TYPE_BINARY,
        ValueMetaInterface.STORAGE_TYPE_BINARY_STRING);
    out[out.length - 1].setStorageMetadata(new ValueMeta("id", ValueMetaInterface.TYPE_STRING,
        ValueMetaInterface.STORAGE_TYPE_NORMAL));
    out[out.length - 1].setLength(256);
    out[out.length - 1].setStringEncoding(CarbonCommonConstants.BYTE_ENCODING);
    out[out.length - 1].getStorageMetadata().setStringEncoding(CarbonCommonConstants.BYTE_ENCODING);

    data.outputRowMeta.setValueMetaList(Arrays.asList(out));
  }

  /**
   * This method will be used to get the row from previous step and then it
   * will generate the mdkey and then send the mdkey to next step
   *
   * @param row input row
   * @throws KettleException
   */
  private Object[] process(Object[] row) throws KettleException {
    Object[] outputRow = null;
    // adding one for the high cardinality dims byte array.
    if (meta.getNoDictionaryCount() > 0 || meta.getComplexDimsCount() > 0) {
      outputRow = new Object[measureCount + 1 + 1];
    } else {
      outputRow = new Object[measureCount + 1];
    }

    int l = 0;
    int index = 0;
    for (int i = 0; i < measureCount; i++) {
      if (aggType[i] == CarbonCommonConstants.BIG_DECIMAL_MEASURE) {
        outputRow[l++] = NonDictionaryUtil.getMeasure(index++, row);
      } else if (aggType[i] == CarbonCommonConstants.BIG_INT_MEASURE) {
        outputRow[l++] = (Long) NonDictionaryUtil.getMeasure(index++, row);
      } else {
        outputRow[l++] = (Double) NonDictionaryUtil.getMeasure(index++, row);
      }
    }
    outputRow[l] = NonDictionaryUtil.getByteArrayForNoDictionaryCols(row);

    int[] highCardExcludedRows = new int[segmentProperties.getDimColumnsCardinality().length];
    for (int i = 0; i < highCardExcludedRows.length; i++) {
      Object key = NonDictionaryUtil.getDimension(i, row);
      highCardExcludedRows[i] = (Integer) key;
    }
    try {
      outputRow[outputRow.length - 1] = data.generator.generateKey(highCardExcludedRows);
    } catch (KeyGenException e) {
      throw new KettleException("unable to generate the mdkey", e);
    }

    return outputRow;
  }

  /**
   * This method will get the store location for the given path, segment id and partition id
   *
   * @return data directory path
   */
  private String getCarbonDataFolderLocation() {
    String carbonStorePath =
        CarbonProperties.getInstance().getProperty(CarbonCommonConstants.STORE_LOCATION_HDFS);
    CarbonTable carbonTable = CarbonMetadata.getInstance().getCarbonTable(
        meta.getDatabaseName() + CarbonCommonConstants.UNDERSCORE + meta.getTableName());
    CarbonTablePath carbonTablePath =
        CarbonStorePath.getCarbonTablePath(carbonStorePath, carbonTable.getCarbonTableIdentifier());
    String carbonDataDirectoryPath =
        carbonTablePath.getCarbonDataDirectoryPath(meta.getPartitionID(), meta.getSegmentId() + "");
    return carbonDataDirectoryPath;
  }

  /**
   * Initialize and do work where other steps need to wait for...
   *
   * @param smi The metadata to work with
   * @param sdi The data to initialize
   * @return step initialize or not
   */
  public boolean init(StepMetaInterface smi, StepDataInterface sdi) {
    meta = (MDKeyGenStepMeta) smi;
    data = (MDKeyGenStepData) sdi;

    return super.init(smi, sdi);
  }

  /**
   * Dispose of this step: close files, empty logs, etc.
   *
   * @param smi The metadata to work with
   * @param sdi The data to dispose of
   */
  public void dispose(StepMetaInterface smi, StepDataInterface sdi) {
    meta = (MDKeyGenStepMeta) smi;
    data = (MDKeyGenStepData) sdi;
    super.dispose(smi, sdi);
    dataHandler = null;
    finalMerger = null;
  }

}
