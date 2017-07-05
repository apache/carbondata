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
import java.util.List;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.datastore.exception.CarbonDataWriterException;
import org.apache.carbondata.core.datastore.row.CarbonRow;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.scan.result.iterator.RawResultIterator;
import org.apache.carbondata.core.scan.wrappers.ByteArrayWrapper;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.processing.model.CarbonLoadModel;
import org.apache.carbondata.processing.sortandgroupby.exception.CarbonSortKeyAndGroupByException;
import org.apache.carbondata.processing.sortandgroupby.sortdata.SortDataRows;
import org.apache.carbondata.processing.sortandgroupby.sortdata.SortIntermediateFileMerger;
import org.apache.carbondata.processing.sortandgroupby.sortdata.SortParameters;
import org.apache.carbondata.processing.store.CarbonFactDataHandlerModel;
import org.apache.carbondata.processing.store.CarbonFactHandler;
import org.apache.carbondata.processing.store.CarbonFactHandlerFactory;
import org.apache.carbondata.processing.store.SingleThreadFinalSortFilesMerger;
import org.apache.carbondata.processing.util.CarbonDataProcessorUtil;

import org.apache.spark.sql.types.Decimal;

/**
 * This class will process the query result and convert the data
 * into a format compatible for data load
 */
public class CompactionResultSortProcessor extends AbstractResultProcessor {

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
  private String tempStoreLocation;
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
   * whether the allocated tasks has any record
   */
  private boolean isRecordFound;
  /**
   * intermediate sort merger
   */
  private SortIntermediateFileMerger intermediateFileMerger;

  /**
   * @param carbonLoadModel
   * @param carbonTable
   * @param segmentProperties
   * @param compactionType
   * @param tableName
   */
  public CompactionResultSortProcessor(CarbonLoadModel carbonLoadModel, CarbonTable carbonTable,
      SegmentProperties segmentProperties, CompactionType compactionType, String tableName) {
    this.carbonLoadModel = carbonLoadModel;
    this.carbonTable = carbonTable;
    this.segmentProperties = segmentProperties;
    this.segmentId = carbonLoadModel.getSegmentId();
    this.compactionType = compactionType;
    this.tableName = tableName;
  }

  /**
   * This method will iterate over the query result and convert it into a format compatible
   * for data loading
   *
   * @param resultIteratorList
   */
  public boolean execute(List<RawResultIterator> resultIteratorList) {
    boolean isCompactionSuccess = false;
    try {
      initTempStoreLocation();
      initSortDataRows();
      initAggType();
      processResult(resultIteratorList);
      // After delete command, if no records are fetched from one split,
      // below steps are not required to be initialized.
      if (isRecordFound) {
        initializeFinalThreadMergerForMergeSort();
        initDataHandler();
        readAndLoadDataFromSortTempFiles();
      }
      isCompactionSuccess = true;
    } catch (Exception e) {
      LOGGER.error(e, "Compaction failed: " + e.getMessage());
    } finally {
      // clear temp files and folders created during compaction
      deleteTempStoreLocation();
    }
    return isCompactionSuccess;
  }

  /**
   * This method will clean up the local folders and files created during compaction process
   */
  private void deleteTempStoreLocation() {
    if (null != tempStoreLocation) {
      try {
        CarbonUtil.deleteFoldersAndFiles(new File[] { new File(tempStoreLocation) });
      } catch (IOException | InterruptedException e) {
        LOGGER.error("Problem deleting local folders during compaction: " + e.getMessage());
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
      while (resultIterator.hasNext()) {
        addRowForSorting(prepareRowObjectForSorting(resultIterator.next()));
        isRecordFound = true;
      }
    }
    try {
      sortDataRows.startSorting();
    } catch (CarbonSortKeyAndGroupByException e) {
      LOGGER.error(e);
      throw new Exception("Problem loading data during compaction: " + e.getMessage());
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
    long[] keyArray = segmentProperties.getDimensionKeyGenerator().getKeyArray(dictionaryKey);
    Object[] dictionaryValues = new Object[dimensionColumnCount + measureCount];
    for (int i = 0; i < keyArray.length; i++) {
      dictionaryValues[i] = Long.valueOf(keyArray[i]).intValue();
    }
    int noDictionaryIndex = 0;
    int dictionaryIndex = 0;
    for (int i = 0; i < dimensions.size(); i++) {
      CarbonDimension dims = dimensions.get(i);
      if (dims.hasEncoding(Encoding.DICTIONARY)) {
        // dictionary
        preparedRow[i] = dictionaryValues[dictionaryIndex++];
      } else {
        // no dictionary dims
        preparedRow[i] = wrapper.getNoDictionaryKeyByIndex(noDictionaryIndex++);
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
    switch (type) {
      case DECIMAL:
        if (value != null) {
          value = ((Decimal) value).toJavaBigDecimal();
        }
        return value;
      default:
        return value;
    }
  }

  /**
   * This method will read sort temp files, perform merge sort and add it to store for data loading
   */
  private void readAndLoadDataFromSortTempFiles() throws Exception {
    try {
      intermediateFileMerger.finish();
      finalMerger.startFinalMerge();
      while (finalMerger.hasNext()) {
        Object[] row = finalMerger.next();
        dataHandler.addDataToStore(new CarbonRow(row));
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
   * add row to a temp array which will we written to a sort temp file after sorting
   *
   * @param row
   */
  private void addRowForSorting(Object[] row) throws Exception {
    try {
      sortDataRows.addRow(row);
    } catch (CarbonSortKeyAndGroupByException e) {
      LOGGER.error(e);
      throw new Exception("Row addition for sorting failed during compaction: " + e.getMessage());
    }
  }

  /**
   * create an instance of sort data rows
   */
  private void initSortDataRows() throws Exception {
    measureCount = carbonTable.getMeasureByTableName(tableName).size();
    List<CarbonDimension> dimensions = carbonTable.getDimensionByTableName(tableName);
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
    intermediateFileMerger = new SortIntermediateFileMerger(parameters);
    // TODO: Now it is only supported onheap merge, but we can have unsafe merge
    // as well by using UnsafeSortDataRows.
    this.sortDataRows = new SortDataRows(parameters, intermediateFileMerger);
    try {
      this.sortDataRows.initialize();
    } catch (CarbonSortKeyAndGroupByException e) {
      LOGGER.error(e);
      throw new Exception(
          "Error initializing sort data rows object during compaction: " + e.getMessage());
    }
  }

  /**
   * This method will create the sort parameters VO object
   *
   * @return
   */
  private SortParameters createSortParameters() {
    return SortParameters
        .createSortParameters(carbonTable, carbonLoadModel.getDatabaseName(), tableName,
            dimensionColumnCount, segmentProperties.getComplexDimensions().size(), measureCount,
            noDictionaryCount, carbonLoadModel.getPartitionId(), segmentId,
            carbonLoadModel.getTaskNo(), noDictionaryColMapping, true);
  }

  /**
   * create an instance of finalThread merger which will perform merge sort on all the
   * sort temp files
   */
  private void initializeFinalThreadMergerForMergeSort() {
    String sortTempFileLocation = tempStoreLocation + CarbonCommonConstants.FILE_SEPARATOR
        + CarbonCommonConstants.SORT_TEMP_FILE_LOCATION;
    boolean[] noDictionarySortColumnMapping = null;
    if (noDictionaryColMapping.length == this.segmentProperties.getNumberOfSortColumns()) {
      noDictionarySortColumnMapping = noDictionaryColMapping;
    } else {
      noDictionarySortColumnMapping = new boolean[this.segmentProperties.getNumberOfSortColumns()];
      System.arraycopy(noDictionaryColMapping, 0,
          noDictionarySortColumnMapping, 0, noDictionarySortColumnMapping.length);
    }
    finalMerger =
        new SingleThreadFinalSortFilesMerger(sortTempFileLocation, tableName, dimensionColumnCount,
            segmentProperties.getComplexDimensions().size(), measureCount, noDictionaryCount,
            dataTypes, noDictionaryColMapping, noDictionarySortColumnMapping);
  }

  /**
   * initialise carbon data writer instance
   */
  private void initDataHandler() throws Exception {
    CarbonFactDataHandlerModel carbonFactDataHandlerModel = CarbonFactDataHandlerModel
        .getCarbonFactDataHandlerModel(carbonLoadModel, carbonTable, segmentProperties, tableName,
            tempStoreLocation);
    setDataFileAttributesInModel(carbonLoadModel, compactionType, carbonTable,
        carbonFactDataHandlerModel);
    dataHandler = CarbonFactHandlerFactory.createCarbonFactHandler(carbonFactDataHandlerModel,
        CarbonFactHandlerFactory.FactHandlerType.COLUMNAR);
    try {
      dataHandler.initialise();
    } catch (CarbonDataWriterException e) {
      LOGGER.error(e);
      throw new Exception("Problem initialising data handler during compaction: " + e.getMessage());
    }
  }

  /**
   * initialise temporary store location
   */
  private void initTempStoreLocation() {
    tempStoreLocation = CarbonDataProcessorUtil
        .getLocalDataFolderLocation(carbonLoadModel.getDatabaseName(), tableName,
            carbonLoadModel.getTaskNo(), carbonLoadModel.getPartitionId(), segmentId, true);
  }

  /**
   * initialise aggregation type for measures for their storage format
   */
  private void initAggType() {
    dataTypes = CarbonDataProcessorUtil.initDataType(carbonTable, tableName, measureCount);
  }
}
