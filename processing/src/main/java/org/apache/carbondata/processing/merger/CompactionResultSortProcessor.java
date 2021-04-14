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

package org.apache.carbondata.processing.merger;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.datastore.exception.CarbonDataWriterException;
import org.apache.carbondata.core.datastore.row.CarbonRow;
import org.apache.carbondata.core.indexstore.PartitionSpec;
import org.apache.carbondata.core.metadata.SegmentFileStore;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.scan.result.iterator.RawResultIterator;
import org.apache.carbondata.core.scan.wrappers.ByteArrayWrapper;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.DataTypeUtil;
import org.apache.carbondata.processing.loading.model.CarbonLoadModel;
import org.apache.carbondata.processing.sort.exception.CarbonSortKeyAndGroupByException;
import org.apache.carbondata.processing.sort.sortdata.SingleThreadFinalSortFilesMerger;
import org.apache.carbondata.processing.sort.sortdata.SortDataRows;
import org.apache.carbondata.processing.sort.sortdata.SortIntermediateFileMerger;
import org.apache.carbondata.processing.sort.sortdata.SortParameters;
import org.apache.carbondata.processing.store.CarbonFactDataHandlerModel;
import org.apache.carbondata.processing.store.CarbonFactHandler;
import org.apache.carbondata.processing.store.CarbonFactHandlerFactory;
import org.apache.carbondata.processing.util.CarbonDataProcessorUtil;

import org.apache.log4j.Logger;

/**
 * This class will process the query result and convert the data
 * into a format compatible for data load
 */
public class CompactionResultSortProcessor extends AbstractResultProcessor {

  /**
   * LOGGER
   */
  private static final Logger LOGGER =
      LogServiceFactory.getLogService(CompactionResultSortProcessor.class.getName());
  /**
   * carbon load model that contains all the required information for load
   */
  private CarbonLoadModel carbonLoadModel;
  /**
   * carbon table
   */
  private CarbonTable carbonTable;
  /**
   * sortDataRows instance for sorting each row read ad writing to sort temp file
   */
  private SortDataRows sortDataRows;
  /**
   * final merger for merge sort
   */
  private SingleThreadFinalSortFilesMerger finalMerger;
  /**
   * data handler VO object
   */
  private CarbonFactHandler dataHandler;
  /**
   * segment properties for getting dimension cardinality and other required information of a block
   */
  private SegmentProperties segmentProperties;
  /**
   * compaction type to decide whether taskID need to be extracted from carbondata files
   */
  private CompactionType compactionType;
  /**
   * boolean mapping for no dictionary columns in schema
   */
  private boolean[] noDictionaryColMapping;

  private boolean[] sortColumnMapping;
  /**
   * boolean mapping for long string dimension
   */
  private boolean[] isVarcharDimMapping;
  /**
   * agg type defined for measures
   */
  private DataType[] dataTypes;
  /**
   * segment id
   */
  private String segmentId;
  /**
   * temp store location to be sued during data load
   */
  private String[] tempStoreLocation;
  /**
   * table name
   */
  private String tableName;
  /**
   * no dictionary column count in schema
   */
  private int noDictionaryCount;
  /**
   * total count of measures in schema
   */
  private int measureCount;
  /**
   * dimension count excluding complex dimension and no dictionary column count
   */
  private int dimensionColumnCount;
  /**
   * all allDimensions in the table
   */
  private List<CarbonDimension> dimensions;
  /**
   * whether the allocated tasks has any record
   */
  private boolean isRecordFound;
  /**
   * intermediate sort merger
   */
  private SortIntermediateFileMerger intermediateFileMerger;

  private PartitionSpec partitionSpec;

  private SortParameters sortParameters;

  private CarbonColumn[] noDicAndComplexColumns;

  public CompactionResultSortProcessor(CarbonLoadModel carbonLoadModel, CarbonTable carbonTable,
      SegmentProperties segmentProperties, CompactionType compactionType, String tableName,
      PartitionSpec partitionSpec) {
    this.carbonLoadModel = carbonLoadModel;
    this.carbonTable = carbonTable;
    this.segmentProperties = segmentProperties;
    this.segmentId = carbonLoadModel.getSegmentId();
    this.compactionType = compactionType;
    this.tableName = tableName;
    this.partitionSpec = partitionSpec;
  }

  /**
   * This method will iterate over the query result and convert it into a format compatible
   * for data loading
   *
   * @param unsortedResultIteratorList
   * @param sortedResultIteratorList
   * @return if the compaction is success or not
   * @throws Exception
   */
  public boolean execute(List<RawResultIterator> unsortedResultIteratorList,
      List<RawResultIterator> sortedResultIteratorList) throws Exception {
    boolean isCompactionSuccess = false;
    try {
      initTempStoreLocation();
      initSortDataRows();
      dataTypes = CarbonDataProcessorUtil.initDataType(carbonTable, tableName, measureCount);
      processResult(unsortedResultIteratorList);
      // After delete command, if no records are fetched from one split,
      // below steps are not required to be initialized.
      if (isRecordFound) {
        initializeFinalThreadMergerForMergeSort();
        initDataHandler();
        readAndLoadDataFromSortTempFiles(sortedResultIteratorList);
      }
      isCompactionSuccess = true;
    } catch (Exception e) {
      LOGGER.error(e.getLocalizedMessage(), e);
      throw e;
    } finally {
      boolean isMergeIndex = Boolean.parseBoolean(CarbonProperties.getInstance().getProperty(
          CarbonCommonConstants.CARBON_MERGE_INDEX_IN_SEGMENT,
          CarbonCommonConstants.CARBON_MERGE_INDEX_IN_SEGMENT_DEFAULT));
      // mergeIndex is true, the segment file not need to be written
      // and will be written during merging index
      if (partitionSpec != null && !isMergeIndex) {
        // By default carbon.merge.index.in.segment is true and this code will be used for
        // developer debugging purpose.
        try {
          SegmentFileStore.writeSegmentFileForPartitionTable(carbonLoadModel.getTablePath(),
              carbonLoadModel.getTaskNo(), partitionSpec.getLocation().toString(),
              carbonLoadModel.getFactTimeStamp() + "", partitionSpec.getPartitions());
        } catch (IOException e) {
          throw e;
        }
      }
      // clear temp files and folders created during compaction
      deleteTempStoreLocation();
    }
    return isCompactionSuccess;
  }

  @Override
  public void close() {
    // close the sorter executor service
    if (null != sortDataRows) {
      sortDataRows.close();
    }
    // close the final merger
    if (null != finalMerger) {
      finalMerger.close();
    }
    // close data handler
    if (null != dataHandler) {
      dataHandler.closeHandler();
    }
  }

  /**
   * This method will clean up the local folders and files created during compaction process
   */
  private void deleteTempStoreLocation() {
    if (null != tempStoreLocation) {
      for (String tempLoc : tempStoreLocation) {
        try {
          CarbonUtil.deleteFoldersAndFiles(new File(tempLoc));
        } catch (IOException | InterruptedException e) {
          LOGGER.error("Problem deleting local folders during compaction: " + e.getMessage(), e);
        }
      }
    }
  }

  /**
   * This method will iterate over the query result and perform row sorting operation
   *
   * @param resultIteratorList
   */
  private void processResult(List<RawResultIterator> resultIteratorList) throws Exception {
    for (RawResultIterator resultIterator : resultIteratorList) {
      if (CompactionType.STREAMING == compactionType) {
        while (resultIterator.hasNext()) {
          // the input iterator of streaming segment is already using raw row
          addRowForSorting(prepareStreamingRowObjectForSorting(resultIterator.next()));
          isRecordFound = true;
        }
      } else {
        while (resultIterator.hasNext()) {
          addRowForSorting(prepareRowObjectForSorting(resultIterator.next()));
          isRecordFound = true;
        }
      }
      resultIterator.close();
    }
    try {
      sortDataRows.startSorting();
    } catch (CarbonSortKeyAndGroupByException e) {
      LOGGER.error(e.getMessage(), e);
      throw new Exception("Problem loading data during compaction: " + e.getMessage(), e);
    }
  }

  /**
   * This method will prepare the data from raw object that will take part in sorting
   *
   * @param row
   * @return
   */
  private Object[] prepareStreamingRowObjectForSorting(Object[] row) {
    Object[] preparedRow = new Object[dimensions.size() + measureCount];
    for (int i = 0; i < dimensions.size(); i++) {
      CarbonDimension dims = dimensions.get(i);
      if (dims.getDataType() == DataTypes.DATE) {
        // dictionary
        preparedRow[i] = row[i];
      } else {
        // no dictionary dims
        if (DataTypeUtil.isPrimitiveColumn(dims.getDataType()) && !dims.isComplex()) {
          // no dictionary measure columns are expected as original data
          preparedRow[i] = DataTypeUtil
              .getDataBasedOnDataTypeForNoDictionaryColumn((byte[]) row[i], dims.getDataType());
          // for timestamp the above method will give the original data, so it should be
          // converted again to the format to be loaded (without micros)
          if (null != preparedRow[i] && dims.getDataType() == DataTypes.TIMESTAMP) {
            preparedRow[i] = (long) preparedRow[i] / 1000L;
          }
        } else {
          preparedRow[i] = row[i];
        }
      }
    }
    // fill all the measures
    for (int i = 0; i < measureCount; i++) {
      preparedRow[dimensionColumnCount + i] = row[dimensionColumnCount + i];
    }
    return preparedRow;
  }

  /**
   * This method will prepare the data from raw object that will take part in sorting
   *
   * @param row
   * @return
   */
  private Object[] prepareRowObjectForSorting(Object[] row) {
    ByteArrayWrapper wrapper = (ByteArrayWrapper) row[0];
    Object[] preparedRow = new Object[dimensions.size() + measureCount];
    int noDictionaryIndex = 0;
    int dictionaryIndex = 0;
    int complexIndex = 0;

    for (int i = 0; i < dimensions.size(); i++) {
      CarbonDimension dims = dimensions.get(i);
      if (dims.getDataType() == DataTypes.DATE && !dims.isComplex()) {
        // dictionary
        preparedRow[i] = wrapper.getDictionaryKeyByIndex(dictionaryIndex++);
      } else if (!dims.isComplex()) {
        // no dictionary dims
        byte[] noDictionaryKeyByIndex = wrapper.getNoDictionaryKeyByIndex(noDictionaryIndex++);
        if (DataTypeUtil.isPrimitiveColumn(dims.getDataType())) {
          // no dictionary measure columns are expected as original data
          preparedRow[i] = DataTypeUtil
              .getDataBasedOnDataTypeForNoDictionaryColumn(noDictionaryKeyByIndex,
                  dims.getDataType());
          // for timestamp the above method will give the original data, so it should be
          // converted again to the format to be loaded (without micros)
          if (null != preparedRow[i] && dims.getDataType() == DataTypes.TIMESTAMP) {
            preparedRow[i] = (long) preparedRow[i] / 1000L;
          }
        } else {
          preparedRow[i] = noDictionaryKeyByIndex;
        }
      } else {
        preparedRow[i] = wrapper.getComplexKeyByIndex(complexIndex++);
      }
    }

    // fill all the measures
    // measures will always start from 1st index in the row object array
    int measureIndexInRow = 1;
    for (int i = 0; i < measureCount; i++) {
      preparedRow[dimensionColumnCount + i] =
          getConvertedMeasureValue(row[measureIndexInRow++], dataTypes[i]);
    }
    return preparedRow;
  }

  /**
   * This method will convert the spark decimal to java big decimal type
   *
   * @param value
   * @param type
   * @return
   */
  private Object getConvertedMeasureValue(Object value, DataType type) {
    if (DataTypes.isDecimal(type)) {
      if (value != null) {
        value = DataTypeUtil.getDataTypeConverter().convertFromDecimalToBigDecimal(value);
      }
      return value;
    } else {
      return value;
    }
  }

  /**
   * This method will read sort temp files, perform merge sort and add it to store for data loading
   */
  private void readAndLoadDataFromSortTempFiles(List<RawResultIterator> sortedRawResultIterator)
      throws Exception {
    try {
      intermediateFileMerger.finish();
      finalMerger.startFinalMerge();
      if (sortedRawResultIterator != null && sortedRawResultIterator.size() > 0) {
        finalMerger.addInMemoryRawResultIterator(sortedRawResultIterator, segmentProperties,
            noDicAndComplexColumns, dataTypes);
      }
      while (finalMerger.hasNext()) {
        Object[] row = finalMerger.next();
        dataHandler.addDataToStore(new CarbonRow(row));
      }
      dataHandler.finish();
    } catch (CarbonDataWriterException e) {
      LOGGER.error(e.getMessage(), e);
      throw new Exception("Problem loading data during compaction.", e);
    } catch (Exception e) {
      LOGGER.error(e.getMessage(), e);
      throw new Exception("Problem loading data during compaction.", e);
    } finally {
      if (null != dataHandler) {
        try {
          dataHandler.closeHandler();
        } catch (CarbonDataWriterException e) {
          LOGGER.error("Error in close data handler", e);
          throw new Exception("Error in close data handler", e);
        }
      }
    }
  }

  /**
   * add row to a temp array which will we written to a sort temp file after sorting
   *
   * @param row
   */
  private void addRowForSorting(Object[] row) throws Exception {
    try {
      sortDataRows.addRow(row);
    } catch (CarbonSortKeyAndGroupByException e) {
      LOGGER.error(e.getMessage(), e);
      throw new Exception("Row addition for sorting failed during compaction: "
          + e.getMessage(), e);
    }
  }

  /**
   * create an instance of sort data rows
   */
  private void initSortDataRows() {
    measureCount = carbonTable.getVisibleMeasures().size();
    dimensions = new ArrayList<>(2);
    dimensions.addAll(segmentProperties.getDimensions());
    dimensions.addAll(segmentProperties.getComplexDimensions());
    noDictionaryColMapping = new boolean[dimensions.size()];
    sortColumnMapping = new boolean[dimensions.size()];
    isVarcharDimMapping = new boolean[dimensions.size()];
    int i = 0;
    for (CarbonDimension dimension : dimensions) {
      if (dimension.isSortColumn()) {
        sortColumnMapping[i] = true;
      }
      if (dimension.getDataType() == DataTypes.DATE) {
        i++;
        continue;
      }
      noDictionaryColMapping[i] = true;
      if (dimension.getColumnSchema().getDataType() == DataTypes.VARCHAR) {
        isVarcharDimMapping[i] = true;
      }
      i++;
      noDictionaryCount++;
    }
    dimensionColumnCount = dimensions.size();
    sortParameters = createSortParameters();
    intermediateFileMerger = new SortIntermediateFileMerger(sortParameters);
    // Delete if any older file exists in sort temp folder
    CarbonDataProcessorUtil.deleteSortLocationIfExists(sortParameters.getTempFileLocation());
    // create new sort temp directory
    CarbonDataProcessorUtil.createLocations(sortParameters.getTempFileLocation());
    // TODO: Now it is only supported onheap merge, but we can have unsafe merge
    // as well by using UnsafeSortDataRows.
    this.sortDataRows = new SortDataRows(sortParameters, intermediateFileMerger);
    this.sortDataRows.initialize();
  }

  /**
   * This method will create the sort parameters VO object
   *
   * @return
   */
  private SortParameters createSortParameters() {
    int numberOfCompactingCores = CarbonProperties.getInstance().getNumberOfCompactingCores();
    return SortParameters
        .createSortParameters(carbonTable, carbonLoadModel.getDatabaseName(), tableName,
            dimensionColumnCount, segmentProperties.getComplexDimensions().size(), measureCount,
            noDictionaryCount, segmentId, carbonLoadModel.getTaskNo(), noDictionaryColMapping,
            sortColumnMapping, isVarcharDimMapping, true, numberOfCompactingCores / 2);
  }

  /**
   * create an instance of finalThread merger which will perform merge sort on all the
   * sort temp files
   */
  private void initializeFinalThreadMergerForMergeSort() {
    boolean[] noDictionarySortColumnMapping = CarbonDataProcessorUtil
        .getNoDictSortColMapping(carbonTable);
    sortParameters.setNoDictionarySortColumn(noDictionarySortColumnMapping);
    Map<String, int[]> columnIdxMap = CarbonDataProcessorUtil
        .getColumnIdxBasedOnSchemaInRow(carbonTable);
    sortParameters.setNoDictSortColumnSchemaOrderMapping(
        columnIdxMap.get("columnIdxBasedOnSchemaInRow"));
    sortParameters.setNoDictSortColIdxSchemaOrderMapping(
        columnIdxMap.get("noDictSortIdxBasedOnSchemaInRow"));
    sortParameters.setDictSortColIdxSchemaOrderMapping(
        columnIdxMap.get("dictSortIdxBasedOnSchemaInRow"));
    String[] sortTempFileLocation = CarbonDataProcessorUtil.arrayAppend(tempStoreLocation,
        CarbonCommonConstants.FILE_SEPARATOR, CarbonCommonConstants.SORT_TEMP_FILE_LOCATION);
    finalMerger =
        new SingleThreadFinalSortFilesMerger(sortTempFileLocation, tableName, sortParameters);
  }

  /**
   * initialise carbon data writer instance
   */
  private void initDataHandler() throws Exception {
    String carbonStoreLocation;
    if (partitionSpec != null) {
      carbonStoreLocation =
          partitionSpec.getLocation().toString() + CarbonCommonConstants.FILE_SEPARATOR
              + carbonLoadModel.getFactTimeStamp() + ".tmp";
    } else {
      carbonStoreLocation = CarbonDataProcessorUtil
          .createCarbonStoreLocation(carbonLoadModel.getCarbonDataLoadSchema().getCarbonTable(),
              carbonLoadModel.getSegmentId());
    }
    CarbonFactDataHandlerModel carbonFactDataHandlerModel = CarbonFactDataHandlerModel
        .getCarbonFactDataHandlerModel(carbonLoadModel, carbonTable, segmentProperties, tableName,
            tempStoreLocation, carbonStoreLocation);
    carbonFactDataHandlerModel.setSegmentId(carbonLoadModel.getSegmentId());
    carbonFactDataHandlerModel.setBucketId(carbonLoadModel.getBucketId());
    setDataFileAttributesInModel(carbonLoadModel, compactionType, carbonFactDataHandlerModel);
    this.noDicAndComplexColumns = carbonFactDataHandlerModel.getNoDictAndComplexColumns();
    dataHandler = CarbonFactHandlerFactory.createCarbonFactHandler(carbonFactDataHandlerModel);
    try {
      dataHandler.initialise();
    } catch (CarbonDataWriterException e) {
      LOGGER.error(e.getMessage(), e);
      throw new Exception("Problem initialising data handler during compaction: "
          + e.getMessage(), e);
    }
  }

  /**
   * initialise temporary store location
   */
  private void initTempStoreLocation() {
    tempStoreLocation = CarbonDataProcessorUtil
        .getLocalDataFolderLocation(carbonTable, carbonLoadModel.getTaskNo(),
           segmentId, true, false);
  }
}
