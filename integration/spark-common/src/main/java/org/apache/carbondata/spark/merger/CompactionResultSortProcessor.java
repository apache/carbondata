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
package org.apache.carbondata.spark.merger;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.keygenerator.KeyGenException;
import org.apache.carbondata.core.metadata.CarbonMetadata;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonMeasure;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.scan.result.BatchResult;
import org.apache.carbondata.core.scan.wrappers.ByteArrayWrapper;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.DataTypeUtil;
import org.apache.carbondata.processing.model.CarbonLoadModel;
import org.apache.carbondata.processing.schema.metadata.SortObserver;
import org.apache.carbondata.processing.sortandgroupby.exception.CarbonSortKeyAndGroupByException;
import org.apache.carbondata.processing.sortandgroupby.sortdata.SortDataRows;
import org.apache.carbondata.processing.sortandgroupby.sortdata.SortIntermediateFileMerger;
import org.apache.carbondata.processing.sortandgroupby.sortdata.SortParameters;
import org.apache.carbondata.processing.store.CarbonDataFileAttributes;
import org.apache.carbondata.processing.store.CarbonFactDataHandlerColumnar;
import org.apache.carbondata.processing.store.CarbonFactDataHandlerModel;
import org.apache.carbondata.processing.store.SingleThreadFinalSortFilesMerger;
import org.apache.carbondata.processing.store.writer.exception.CarbonDataWriterException;
import org.apache.carbondata.processing.util.CarbonDataProcessorUtil;

import org.pentaho.di.core.exception.KettleException;

/**
 * This class will process the query result and convert the data
 * into a format compatible for data load
 */
public class CompactionResultSortProcessor {

  /**
   * LOGGER
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(CompactionResultSortProcessor.class.getName());
  /**
   * carbon load model that contains all the required information for load
   */
  private CarbonLoadModel carbonLoadModel;
  /**
   * sortDataRows instance for sorting each row read ad writing to sort temp file
   */
  private SortDataRows sortDataRows;
  /**
   * segment proeprties which contains required information for a segment
   */
  private SegmentProperties segmentProperties;
  /**
   * segment information of parent table
   */
  private SegmentProperties srcSegmentProperties;
  /**
   * final merger for merge sort
   */
  private SingleThreadFinalSortFilesMerger finalMerger;
  /**
   * data handler VO object
   */
  private CarbonFactDataHandlerColumnar dataHandler;
  /**
   * column cardinality
   */
  private int[] columnCardinality;
  /**
   * Fact Table To Index Table Column Mapping order
   */
  private int[] factToIndexColumnMapping;
  /**
   * Fact Table Dict Column to Index Table Dict Column Mapping
   */
  private int[] factToIndexDictColumnMapping;
  /**
   * boolean mapping for no dictionary columns in schema
   */
  private boolean[] noDictionaryColMapping;
  /**
   * agg type defined for measures
   */
  private char[] aggType;
  /**
   * segment id
   */
  private String segmentId;
  /**
   * index table name
   */
  private String indexTableName;
  /**
   * temp store location to be sued during data load
   */
  private String tempStoreLocation;
  /**
   * data base name
   */
  private String databaseName;
  /**
   * no dictionary column count in schema
   */
  private int noDictionaryCount;
  /**
   * implicit column count in schema
   */
  private int implicitColumnCount;
  /**
   * total count of measures in schema
   */
  private int measureCount;
  /**
   * dimension count excluding complex dimension and no dictionary column count
   */
  private int dimensionColumnCount;
  /**
   * complex dimension count in schema
   */
  private int complexDimensionCount;
  /**
   * carbon table
   */
  private CarbonTable carbonTable;
  /**
   * whether the allocated tasks has any record
   */
  private boolean isRecordFound;

  /**
   * @param carbonLoadModel
   * @param columnCardinality
   * @param segmentId
   * @param indexTableName
   */
  public CompactionResultSortProcessor(CarbonLoadModel carbonLoadModel, int[] columnCardinality,
      String segmentId, String indexTableName, int[] factToIndexColumnMapping,
      int[] factToIndexDictColumnMapping) {
    this.carbonLoadModel = carbonLoadModel;
    this.columnCardinality = columnCardinality;
    this.segmentId = segmentId;
    this.indexTableName = indexTableName;
    this.databaseName = carbonLoadModel.getDatabaseName();
    this.factToIndexColumnMapping = factToIndexColumnMapping;
    this.factToIndexDictColumnMapping = factToIndexDictColumnMapping;
    initSegmentProperties();
  }

  /**
   * This method will iterate over the query result and convert it into a format compatible
   * for data loading
   *
   * @param detailQueryResultIteratorList
   */
  public void processQueryResult(List<CarbonIterator<BatchResult>> detailQueryResultIteratorList)
      throws Exception {
    try {
      initTempStoreLocation();
      initSortDataRows();
      processResult(detailQueryResultIteratorList);
      // After delete command, if no records are fetched from one split,
      // below steps are not required to be initialized.
      if (isRecordFound) {
        initializeFinalThreadMergerForMergeSort();
        initDataHandler();
        readAndLoadDataFromSortTempFiles();
      }
    } finally {
      // clear temp files and folders created during secondary index creation
      deleteTempStoreLocation();
    }
  }

  /**
   * This method will clean up the local folders and files created for secondary index creation
   */
  private void deleteTempStoreLocation() {
    if (null != tempStoreLocation) {
      try {
        CarbonUtil.deleteFoldersAndFiles(new File[] { new File(tempStoreLocation) });
      } catch (IOException | InterruptedException e) {
        LOGGER.error(
            "Problem deleting local folders during secondary index creation: " + e.getMessage());
      }
    }
  }

  /**
   * This method will iterate over the query result and perform row sorting operation
   *
   * @param detailQueryResultIteratorList
   */
  private void processResult(List<CarbonIterator<BatchResult>> detailQueryResultIteratorList)
      throws Exception {
    for (CarbonIterator<BatchResult> detailQueryIterator : detailQueryResultIteratorList) {
      while (detailQueryIterator.hasNext()) {
        BatchResult batchResult = detailQueryIterator.next();
        while (batchResult.hasNext()) {
          addRowForSorting(prepareRowObjectForSorting(batchResult.next()));
          isRecordFound = true;
        }
      }
    }
    try {
      sortDataRows.startSorting();
    } catch (CarbonSortKeyAndGroupByException e) {
      LOGGER.error(e);
      throw new Exception("Problem loading data while creating secondary index: " + e.getMessage());
    }
  }

  /**
   * This method will prepare the data from raw object that will take part in sorting
   *
   * @param row
   * @return
   */
  private Object[] prepareRowObjectForSorting(Object[] row) {
    ByteArrayWrapper wrapper = (ByteArrayWrapper) row[0];
    // ByteBuffer[] noDictionaryBuffer = new ByteBuffer[noDictionaryCount];

    List<CarbonDimension> dimensions = segmentProperties.getDimensions();
    Object[] preparedRow = new Object[dimensions.size() + measureCount];

    // convert the dictionary from MDKey to surrogate key
    byte[] dictionaryKey = wrapper.getDictionaryKey();
    long[] keyArray = srcSegmentProperties.getDimensionKeyGenerator().getKeyArray(dictionaryKey);
    Object[] dictionaryValues = new Object[dimensionColumnCount + measureCount];
    // Re-ordering is required as per index table column dictionary order,
    // as output dictionary Byte Array is as per parent table schema order
    for (int i = 0; i < keyArray.length; i++) {
      dictionaryValues[factToIndexDictColumnMapping[i]] = Long.valueOf(keyArray[i]).intValue();
    }

    int noDictionaryIndex = 0;
    int dictionaryIndex = 0;
    int i = 0;
    // loop excluding last dimension as last one is implicit column.
    for (; i < dimensions.size() - 1; i++) {
      CarbonDimension dims = dimensions.get(i);
      if (dims.hasEncoding(Encoding.DICTIONARY)) {
        // dictionary
        preparedRow[i] = dictionaryValues[dictionaryIndex++];
      } else {
        // no dictionary dims
        preparedRow[i] = wrapper.getNoDictionaryKeyByIndex(noDictionaryIndex++);
      }
    }

    // at last add implicit column position reference(PID)

    preparedRow[i] = wrapper.getImplicitColumnByteArray();
    return preparedRow;
  }

  /**
   * This method will read sort temp files, perform merge sort and add it to store for data loading
   */
  private void readAndLoadDataFromSortTempFiles() throws Exception {
    try {
      Object[] previousRow = null;
      finalMerger.startFinalMerge();
      while (finalMerger.hasNext()) {
        Object[] rowRead = finalMerger.next();
        // convert the row from surrogate key to MDKey
        //        Object[] outputRow = CarbonDataProcessorUtil
        //            .processNoKettle(rowRead, segmentProperties, aggType, measureCount, noDictionaryCount,
        //                complexDimensionCount);
        Object[] outputRow = null;
        dataHandler.addDataToStore(outputRow);
      }
      dataHandler.finish();
    } catch (CarbonDataWriterException e) {
      LOGGER.error(e);
      throw new Exception("Problem loading data during compaction: " + e.getMessage());
    } catch (Exception e) {
      LOGGER.error(e);
      throw new Exception("Problem loading data during compaction: " + e.getMessage());
    } finally {
      if (null != dataHandler) {
        try {
          dataHandler.closeHandler();
        } catch (CarbonDataWriterException e) {
          LOGGER.error(e);
          throw new Exception("Problem loading data during compaction: " + e.getMessage());
        }
      }
    }
  }

  /**
   * This method is used to process the row with out kettle.
   *
   * @param row               input row
   * @param segmentProperties
   * @param aggType
   * @param measureCount
   * @param noDictionaryCount
   * @param complexDimCount
   * @return
   * @throws KettleException
   */
  public static Object[] processNoKettle(Object[] row, SegmentProperties segmentProperties,
      char[] aggType, int measureCount, int noDictionaryCount, int complexDimCount)
      throws KettleException {

    //    int measureIndex = IgnoreDictionary.MEASURES_INDEX_IN_ROW.getIndex();
    //
    //    int noDimByteArrayIndex = IgnoreDictionary.BYTE_ARRAY_INDEX_IN_ROW.getIndex();
    //
    //    int dimsArrayIndex = IgnoreDictionary.DIMENSION_INDEX_IN_ROW.getIndex();

    int measureIndex = 0;

    int noDimByteArrayIndex = 0;

    int dimsArrayIndex = 0;

    Object[] outputRow;
    // adding one for the high cardinality dims byte array.
    if (noDictionaryCount > 0 || complexDimCount > 0) {
      outputRow = new Object[measureCount + 1 + 1];
    } else {
      outputRow = new Object[measureCount + 1];
    }

    int l = 0;
    int index = 0;
    Object[] measures = (Object[]) row[measureIndex];
    for (int i = 0; i < measureCount; i++) {
      outputRow[l++] = measures[index++];
    }
    outputRow[l] = row[noDimByteArrayIndex];

    int[] highCardExcludedRows = new int[segmentProperties.getDimColumnsCardinality().length];
    int[] dimsArray = (int[]) row[dimsArrayIndex];
    for (int i = 0; i < highCardExcludedRows.length; i++) {
      highCardExcludedRows[i] = dimsArray[i];
    }
    try {
      outputRow[outputRow.length - 1] =
          segmentProperties.getDimensionKeyGenerator().generateKey(highCardExcludedRows);
    } catch (KeyGenException e) {
      throw new KettleException("unable to generate the mdkey", e);
    }
    return outputRow;
  }

  /**
   * initialise segment properties
   */
  private void initSegmentProperties() {
    CarbonTable carbonTable = CarbonMetadata.getInstance()
        .getCarbonTable(databaseName + CarbonCommonConstants.UNDERSCORE + indexTableName);
    List<ColumnSchema> columnSchemaList = CarbonUtil
        .getColumnSchemaList(carbonTable.getDimensionByTableName(indexTableName),
            carbonTable.getMeasureByTableName(indexTableName));
    segmentProperties = new SegmentProperties(columnSchemaList, columnCardinality);
    srcSegmentProperties =
        new SegmentProperties(getParentColumnOrder(columnSchemaList), getParentOrderCardinality());
  }

  /**
   * Convert index table column order into parent table column order
   */
  private List<ColumnSchema> getParentColumnOrder(List<ColumnSchema> columnSchemaList) {
    List<ColumnSchema> parentColumnList = new ArrayList<ColumnSchema>(columnSchemaList.size());
    for (int i = 0; i < columnSchemaList.size(); i++) {
      // Extra cols are dummy_measure & positionId implicit column
      if (i >= columnCardinality.length) {
        parentColumnList.add(columnSchemaList.get(i));
      } else {
        parentColumnList.add(columnSchemaList.get(factToIndexColumnMapping[i]));
      }
    }
    return parentColumnList;
  }

  /**
   * Convert index table column cardinality order into parent table column order
   */
  private int[] getParentOrderCardinality() {
    int[] parentColumnCardinality = new int[columnCardinality.length];
    for (int i = 0; i < columnCardinality.length; i++) {
      parentColumnCardinality[i] = columnCardinality[factToIndexColumnMapping[i]];
    }
    return parentColumnCardinality;
  }

  /**
   * add row to a temp array which will we written to a sort temp file after sorting
   *
   * @param row
   */
  private void addRowForSorting(Object[] row) throws Exception {
    try {
      // prepare row array using RemoveDictionaryUtil class
      sortDataRows.addRow(row);
    } catch (CarbonSortKeyAndGroupByException e) {
      LOGGER.error(e);
      throw new Exception(
          "Row addition for sorting failed while creating secondary index: " + e.getMessage());
    }
  }

  /**
   * create an instance of sort data rows
   */
  private void initSortDataRows() throws Exception {
    CarbonTable indexTable = CarbonMetadata.getInstance().getCarbonTable(
        carbonLoadModel.getDatabaseName() + CarbonCommonConstants.UNDERSCORE + indexTableName);
    measureCount = indexTable.getMeasureByTableName(indexTableName).size();
    implicitColumnCount = indexTable.getImplicitDimensionByTableName(indexTableName).size();
    SortObserver observer = new SortObserver();
    List<CarbonDimension> dimensions = indexTable.getDimensionByTableName(indexTableName);
    noDictionaryColMapping = new boolean[dimensions.size()];
    int i = 0;
    for (CarbonDimension dimension : dimensions) {
      if (CarbonUtil.hasEncoding(dimension.getEncoder(), Encoding.DICTIONARY)) {
        i++;
        continue;
      }
      noDictionaryColMapping[i++] = true;
      noDictionaryCount++;
    }
    dimensionColumnCount = dimensions.size();
    SortParameters parameters = createSortParameters();
    SortIntermediateFileMerger intermediateFileMerger = new SortIntermediateFileMerger(parameters);
    this.sortDataRows = new SortDataRows(parameters, intermediateFileMerger);
    try {
      this.sortDataRows.initialize();
    } catch (CarbonSortKeyAndGroupByException e) {
      LOGGER.error(e);
      throw new Exception(
          "Error initializing sort data rows object while creating secondary index: " + e
              .getMessage());
    }
  }

  /**
   * This method will create the sort parameters VO object
   *
   * @return
   */
  private SortParameters createSortParameters() {
    boolean useKettle = false;
    SortParameters parameters = SortParameters
        .createSortParameters(databaseName, indexTableName, dimensionColumnCount,
            complexDimensionCount, measureCount, noDictionaryCount,
            carbonLoadModel.getPartitionId(), segmentId, carbonLoadModel.getTaskNo(),
            noDictionaryColMapping);
    return parameters;
  }

  /**
   * create an instance of finalThread merger which will perform merge sort on all the
   * sort temp files
   */
  private void initializeFinalThreadMergerForMergeSort() {
    String sortTempFileLocation = tempStoreLocation + CarbonCommonConstants.FILE_SEPARATOR
        + CarbonCommonConstants.SORT_TEMP_FILE_LOCATION;
    initAggType();
    // kettle will not be used
    boolean useKettle = false;
    finalMerger = new SingleThreadFinalSortFilesMerger(sortTempFileLocation, indexTableName,
        dimensionColumnCount, complexDimensionCount, measureCount, noDictionaryCount, aggType,
        noDictionaryColMapping, useKettle);
  }

  /**
   * initialise carbon data writer instance
   */
  private void initDataHandler() throws Exception {
    CarbonFactDataHandlerModel carbonFactDataHandlerModel = getCarbonFactDataHandlerModel();
    carbonFactDataHandlerModel.setPrimitiveDimLens(segmentProperties.getDimColumnsCardinality());
    CarbonDataFileAttributes carbonDataFileAttributes =
        new CarbonDataFileAttributes(Integer.parseInt(carbonLoadModel.getTaskNo()),
            carbonLoadModel.getFactTimeStamp());
    carbonFactDataHandlerModel.setCarbonDataFileAttributes(carbonDataFileAttributes);
    if (segmentProperties.getNumberOfNoDictionaryDimension() > 0
        || segmentProperties.getComplexDimensions().size() > 0) {
      carbonFactDataHandlerModel.setMdKeyIndex(measureCount + 1);
    } else {
      carbonFactDataHandlerModel.setMdKeyIndex(measureCount);
    }
    carbonFactDataHandlerModel.setColCardinality(columnCardinality);
    carbonFactDataHandlerModel.setBlockSizeInMB(carbonTable.getBlockSizeInMB());
    // NO-Kettle.
    carbonFactDataHandlerModel.setUseKettle(false);
    dataHandler = new CarbonFactDataHandlerColumnar(carbonFactDataHandlerModel);
    try {
      dataHandler.initialise();
    } catch (CarbonDataWriterException e) {
      LOGGER.error(e);
      throw new Exception(
          "Problem initialising data handler while creating secondary index: " + e.getMessage());
    }
  }

  /**
   * This method will create a model object for carbon fact data handler
   *
   * @return
   */
  private CarbonFactDataHandlerModel getCarbonFactDataHandlerModel() {
    CarbonFactDataHandlerModel carbonFactDataHandlerModel = null;
    //    CarbonFactDataHandlerModel carbonFactDataHandlerModel = CarbonLoaderUtil
    //        .getCarbonFactDataHandlerModel(carbonLoadModel, segmentProperties, databaseName,
    //            indexTableName, tempStoreLocation, carbonLoadModel.getStorePath());
    return carbonFactDataHandlerModel;
  }

  /**
   * initialise temporary store location
   */
  private void initTempStoreLocation() {
    tempStoreLocation = CarbonDataProcessorUtil
        .getLocalDataFolderLocation(databaseName, indexTableName, carbonLoadModel.getTaskNo(),
            carbonLoadModel.getPartitionId(), segmentId, false);
  }

  /**
   * initialise aggregation type for measures for their storage format
   */
  private void initAggType() {
    aggType = new char[measureCount];
    Arrays.fill(aggType, 'n');
    carbonTable = CarbonMetadata.getInstance()
        .getCarbonTable(databaseName + CarbonCommonConstants.UNDERSCORE + indexTableName);
    List<CarbonMeasure> measures = carbonTable.getMeasureByTableName(indexTableName);
    for (int i = 0; i < measureCount; i++) {
      aggType[i] = DataTypeUtil.getAggType(measures.get(i).getDataType());
    }
  }
}
