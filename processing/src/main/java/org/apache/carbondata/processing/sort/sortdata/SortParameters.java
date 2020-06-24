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

package org.apache.carbondata.processing.sort.sortdata;

import java.io.File;
import java.io.Serializable;
import java.util.Map;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.TableSpec;
import org.apache.carbondata.core.metadata.CarbonTableIdentifier;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.processing.loading.CarbonDataLoadConfiguration;
import org.apache.carbondata.processing.loading.constants.DataLoadProcessorConstants;
import org.apache.carbondata.processing.util.CarbonDataProcessorUtil;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

public class SortParameters implements Serializable {

  private static final Logger LOGGER =
      LogServiceFactory.getLogService(SortParameters.class.getName());
  /**
   * tempFileLocation
   */
  private String[] tempFileLocation;
  /**
   * sortBufferSize
   */
  private int sortBufferSize;
  /**
   * measure count
   */
  private int measureColCount;
  /**
   * measure count
   */
  private int dimColCount;
  /**
   * measure count
   */
  private int complexDimColCount;
  /**
   * fileBufferSize
   */
  private int fileBufferSize;
  /**
   * numberOfIntermediateFileToBeMerged
   */
  private int numberOfIntermediateFileToBeMerged;
  /**
   * fileWriteBufferSize
   */
  private int fileWriteBufferSize;
  /**
   * observer
   */
  private SortObserver observer;
  private String sortTempCompressorName;
  /**
   * prefetch
   */
  private boolean prefetch;
  /**
   * bufferSize
   */
  private int bufferSize;

  private String databaseName;

  private String tableName;

  private DataType[] measureDataType;

  // no dictionary data types of the table
  private DataType[] noDictDataType;

  // no dictionary columns data types participating in sort
  // used while writing the row to sort temp file where sort no dict columns are handled seperately
  private DataType[] noDictSortDataType;

  // no dictionary columns data types not participating in sort
  // used while writing the row to sort temp file where nosort nodict columns are handled seperately
  private DataType[] noDictNoSortDataType;

  /**
   * To know how many columns are of high cardinality.
   */
  private int noDictionaryCount;
  /**
   * partitionID
   */
  private String partitionID;
  /**
   * Id of the load folder
   */
  private String segmentId;
  /**
   * task id, each spark task has a unique id
   */
  private String taskNo;

  /**
   * This will tell whether dimension is dictionary or not.
   */
  private boolean[] noDictionaryDimnesionColumn;

  private boolean[] noDictionarySortColumn;

  private boolean[] sortColumn;
  /**
   * whether dimension is varchar data type.
   * since all dimensions are string, we use an array of boolean instead of datatypes
   */
  private boolean[] isVarcharDimensionColumn;
  private int numberOfSortColumns;

  private int numberOfNoDictSortColumns;

  private int numberOfCores;

  private int rangeId = 0;

  /**
   * CarbonTable Info
   */
  private CarbonTable carbonTable;

  private boolean isUpdateDictDims;

  private boolean isUpdateNonDictDims;

  private int[] dictDimActualPosition;

  private int[] noDictActualPosition;

  /**
   * Index of the no dict Sort columns in the carbonRow for final merge step of sorting.
   */
  private int[] noDictSortColumnSchemaOrderMapping;

  private boolean isInsertWithoutReArrangeFlow;

  public SortParameters getCopy() {
    SortParameters parameters = new SortParameters();
    parameters.tempFileLocation = tempFileLocation;
    parameters.sortBufferSize = sortBufferSize;
    parameters.measureColCount = measureColCount;
    parameters.dimColCount = dimColCount;
    parameters.complexDimColCount = complexDimColCount;
    parameters.fileBufferSize = fileBufferSize;
    parameters.numberOfIntermediateFileToBeMerged = numberOfIntermediateFileToBeMerged;
    parameters.fileWriteBufferSize = fileWriteBufferSize;
    parameters.observer = observer;
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1839
    parameters.sortTempCompressorName = sortTempCompressorName;
    parameters.prefetch = prefetch;
    parameters.bufferSize = bufferSize;
    parameters.databaseName = databaseName;
    parameters.tableName = tableName;
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1015
    parameters.measureDataType = measureDataType;
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2896
    parameters.noDictDataType = noDictDataType;
    parameters.noDictSortDataType = noDictSortDataType;
    parameters.noDictNoSortDataType = noDictNoSortDataType;
    parameters.noDictionaryCount = noDictionaryCount;
    parameters.partitionID = partitionID;
    parameters.segmentId = segmentId;
    parameters.taskNo = taskNo;
    parameters.noDictionaryDimnesionColumn = noDictionaryDimnesionColumn;
    parameters.sortColumn = sortColumn;
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2420
    parameters.isVarcharDimensionColumn = isVarcharDimensionColumn;
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-782
    parameters.noDictionarySortColumn = noDictionarySortColumn;
    parameters.numberOfSortColumns = numberOfSortColumns;
    parameters.numberOfNoDictSortColumns = numberOfNoDictSortColumns;
    parameters.numberOfCores = numberOfCores;
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2091
    parameters.rangeId = rangeId;
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3042
    parameters.carbonTable = carbonTable;
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3335
    parameters.isUpdateDictDims = isUpdateDictDims;
    parameters.isUpdateNonDictDims = isUpdateNonDictDims;
    parameters.dictDimActualPosition = dictDimActualPosition;
    parameters.noDictActualPosition = noDictActualPosition;
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3552
    parameters.noDictSortColumnSchemaOrderMapping = noDictSortColumnSchemaOrderMapping;
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3728
    parameters.isInsertWithoutReArrangeFlow = isInsertWithoutReArrangeFlow;
    return parameters;
  }

  public String[] getTempFileLocation() {
    return tempFileLocation;
  }

  public void setTempFileLocation(String[] tempFileLocation) {
    this.tempFileLocation = tempFileLocation;
  }

  public int getSortBufferSize() {
    return sortBufferSize;
  }

  public void setSortBufferSize(int sortBufferSize) {
    this.sortBufferSize = sortBufferSize;
  }

  public int getMeasureColCount() {
    return measureColCount;
  }

  public void setMeasureColCount(int measureColCount) {
    this.measureColCount = measureColCount;
  }

  public int getDimColCount() {
    return dimColCount;
  }

  public void setDimColCount(int dimColCount) {
    this.dimColCount = dimColCount;
  }

  public int getComplexDimColCount() {
    return complexDimColCount;
  }

  public void setComplexDimColCount(int complexDimColCount) {
    this.complexDimColCount = complexDimColCount;
  }

  public int getNumberOfIntermediateFileToBeMerged() {
    return numberOfIntermediateFileToBeMerged;
  }

  public void setNumberOfIntermediateFileToBeMerged(int numberOfIntermediateFileToBeMerged) {
    this.numberOfIntermediateFileToBeMerged = numberOfIntermediateFileToBeMerged;
  }

  public int getFileWriteBufferSize() {
    return fileWriteBufferSize;
  }

  public void setFileWriteBufferSize(int fileWriteBufferSize) {
    this.fileWriteBufferSize = fileWriteBufferSize;
  }

  public SortObserver getObserver() {
    return observer;
  }

  public void setObserver(SortObserver observer) {
    this.observer = observer;
  }

  public String getSortTempCompressorName() {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1839
    return sortTempCompressorName;
  }

  public void setSortTempCompressorName(String sortTempCompressorName) {
    this.sortTempCompressorName = sortTempCompressorName;
  }

  public boolean isPrefetch() {
    return prefetch;
  }

  public void setPrefetch(boolean prefetch) {
    this.prefetch = prefetch;
  }

  public int getBufferSize() {
    return bufferSize;
  }

  public void setBufferSize(int bufferSize) {
    this.bufferSize = bufferSize;
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public void setDatabaseName(String databaseName) {
    this.databaseName = databaseName;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public DataType[] getMeasureDataType() {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1015
    return measureDataType;
  }

  public void setMeasureDataType(DataType[] measureDataType) {
    this.measureDataType = measureDataType;
  }

  public int getNoDictionaryCount() {
    return noDictionaryCount;
  }

  public void setNoDictionaryCount(int noDictionaryCount) {
    this.noDictionaryCount = noDictionaryCount;
  }

  public String getPartitionID() {
    return partitionID;
  }

  public void setPartitionID(String partitionID) {
    this.partitionID = partitionID;
  }

  public String getSegmentId() {
    return segmentId;
  }

  public void setSegmentId(String segmentId) {
    this.segmentId = segmentId;
  }

  public String getTaskNo() {
    return taskNo;
  }

  public void setTaskNo(String taskNo) {
    this.taskNo = taskNo;
  }

  public boolean[] getNoDictionaryDimnesionColumn() {
    return noDictionaryDimnesionColumn;
  }

  public void setNoDictionaryDimnesionColumn(boolean[] noDictionaryDimnesionColumn) {
    this.noDictionaryDimnesionColumn = noDictionaryDimnesionColumn;
  }

  public boolean[] getIsVarcharDimensionColumn() {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2420
    return isVarcharDimensionColumn;
  }

  public void setIsVarcharDimensionColumn(boolean[] isVarcharDimensionColumn) {
    this.isVarcharDimensionColumn = isVarcharDimensionColumn;
  }

  public int getNumberOfCores() {
    return numberOfCores;
  }

  public void setNumberOfCores(int numberOfCores) {
    this.numberOfCores = numberOfCores;
  }

  public int getNumberOfSortColumns() {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-782
    return numberOfSortColumns;
  }

  public void setNumberOfSortColumns(int numberOfSortColumns) {
    this.numberOfSortColumns = Math.min(numberOfSortColumns, this.dimColCount);
  }

  public boolean[] getNoDictionarySortColumn() {
    return noDictionarySortColumn;
  }

  public void setNoDictionarySortColumn(boolean[] noDictionarySortColumn) {
    this.noDictionarySortColumn = noDictionarySortColumn;
  }

  public int getNumberOfNoDictSortColumns() {
    return numberOfNoDictSortColumns;
  }

  public void setNumberOfNoDictSortColumns(int numberOfNoDictSortColumns) {
    this.numberOfNoDictSortColumns = Math.min(numberOfNoDictSortColumns, noDictionaryCount);
  }

  public void setCarbonTable(CarbonTable carbonTable) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3042
    this.carbonTable = carbonTable;
  }

  public CarbonTable getCarbonTable() {
    return carbonTable;
  }

  int[] getNoDictSortColumnSchemaOrderMapping() {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3552
    return noDictSortColumnSchemaOrderMapping;
  }

  private void setNoDictSortColumnSchemaOrderMapping(int[] noDictSortColumnSchemaOrderMapping) {
    this.noDictSortColumnSchemaOrderMapping = noDictSortColumnSchemaOrderMapping;
  }

  public boolean isInsertWithoutReArrangeFlow() {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3728
    return isInsertWithoutReArrangeFlow;
  }

  public void setInsertWithoutReArrangeFlow(boolean insertWithoutReArrangeFlow) {
    isInsertWithoutReArrangeFlow = insertWithoutReArrangeFlow;
  }

  public static SortParameters createSortParameters(CarbonDataLoadConfiguration configuration) {
    SortParameters parameters = new SortParameters();
    CarbonTableIdentifier tableIdentifier =
        configuration.getTableIdentifier().getCarbonTableIdentifier();
    CarbonProperties carbonProperties = CarbonProperties.getInstance();
    parameters.setCarbonTable(configuration.getTableSpec().getCarbonTable());
    parameters.setDatabaseName(tableIdentifier.getDatabaseName());
    parameters.setTableName(tableIdentifier.getTableName());
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1992
    parameters.setPartitionID("0");
    parameters.setSegmentId(configuration.getSegmentId());
    parameters.setTaskNo(configuration.getTaskNo());
    parameters.setMeasureColCount(configuration.getMeasureCount());
    parameters.setDimColCount(
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2388
        configuration.getDimensionCount() - (configuration.getComplexDictionaryColumnCount()
            + configuration.getComplexNonDictionaryColumnCount()));
    parameters.setNoDictionaryCount(configuration.getNoDictionaryCount());
    parameters.setComplexDimColCount(configuration.getComplexDictionaryColumnCount() + configuration
        .getComplexNonDictionaryColumnCount());
    parameters.setNoDictionaryDimnesionColumn(
        CarbonDataProcessorUtil.getNoDictionaryMapping(configuration.getDataFields()));
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2420
    parameters.setIsVarcharDimensionColumn(
        CarbonDataProcessorUtil.getIsVarcharColumnMapping(configuration.getDataFields()));
    parameters.setNumberOfSortColumns(configuration.getNumberOfSortColumns());
    parameters.setNumberOfNoDictSortColumns(configuration.getNumberOfNoDictSortColumns());
    parameters.setSortColumn(configuration.getSortColumnMapping());
    parameters.setObserver(new SortObserver());
    // get sort buffer size
    parameters.setSortBufferSize(Integer.parseInt(carbonProperties
        .getProperty(CarbonCommonConstants.SORT_SIZE,
            CarbonCommonConstants.SORT_SIZE_DEFAULT_VAL)));
    LOGGER.info("Sort size for table: " + parameters.getSortBufferSize());
    // set number of intermedaite file to merge
    parameters.setNumberOfIntermediateFileToBeMerged(Integer.parseInt(carbonProperties
        .getProperty(CarbonCommonConstants.SORT_INTERMEDIATE_FILES_LIMIT,
            CarbonCommonConstants.SORT_INTERMEDIATE_FILES_LIMIT_DEFAULT_VALUE)));

    LOGGER.info("Number of intermediate file to be merged: " + parameters
        .getNumberOfIntermediateFileToBeMerged());

//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1281
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3042
    String[] carbonDataDirectoryPath = CarbonDataProcessorUtil
        .getLocalDataFolderLocation(parameters.getCarbonTable(),
            configuration.getTaskNo(), configuration.getSegmentId(), false, false);
    String[] sortTempDirs = CarbonDataProcessorUtil.arrayAppend(carbonDataDirectoryPath,
        File.separator, CarbonCommonConstants.SORT_TEMP_FILE_LOCATION);

    parameters.setTempFileLocation(sortTempDirs);
    LOGGER.info("temp file location: " + StringUtils.join(parameters.getTempFileLocation(), ","));
    int numberOfCores = 1;
    // In case of loading from partition we should use the cores specified by it
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2093
    if (configuration.getWritingCoresCount() > 0) {
      numberOfCores = configuration.getWritingCoresCount();
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3031
    } else {
      numberOfCores = configuration.getNumberOfLoadingCores() / 2;
    }
    parameters.setNumberOfCores(numberOfCores > 0 ? numberOfCores : 1);

    parameters.setFileWriteBufferSize(Integer.parseInt(carbonProperties
        .getProperty(CarbonCommonConstants.CARBON_SORT_FILE_WRITE_BUFFER_SIZE,
            CarbonCommonConstants.CARBON_SORT_FILE_WRITE_BUFFER_SIZE_DEFAULT_VALUE)));

    parameters.setSortTempCompressorName(CarbonProperties.getInstance().getSortTempCompressor());
    if (!parameters.sortTempCompressorName.isEmpty()) {
      LOGGER.info(" Compression " + parameters.sortTempCompressorName
          + " will be used for writing the sort temp File");
    }

    parameters.setPrefetch(CarbonCommonConstants.CARBON_PREFETCH_IN_MERGE_VALUE);
    parameters.setBufferSize(Integer.parseInt(carbonProperties.getProperty(
        CarbonCommonConstants.CARBON_PREFETCH_BUFFERSIZE,
        CarbonCommonConstants.CARBON_PREFETCH_BUFFERSIZE_DEFAULT)));

//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3728
    if (configuration.getDataLoadProperty(DataLoadProcessorConstants.NO_REARRANGE_OF_ROWS) != null
        && configuration.getTableSpec().getCarbonTable().getPartitionInfo() != null) {
      // In case of partition, partition data will be present in the end for rearrange flow
      // So, prepare the indexes and mapping as per dataFields order.
      parameters.setInsertWithoutReArrangeFlow(true);
      parameters.setNoDictionarySortColumn(CarbonDataProcessorUtil
          .getNoDictSortColMappingAsDataFieldOrder(configuration.getDataFields()));
      parameters.setNoDictSortColumnSchemaOrderMapping(CarbonDataProcessorUtil
          .getColumnIdxBasedOnSchemaInRowAsDataFieldOrder(configuration.getDataFields()));
      parameters.setMeasureDataType(configuration.getMeasureDataTypeAsDataFieldOrder());
      parameters.setNoDictDataType(CarbonDataProcessorUtil
          .getNoDictDataTypesAsDataFieldOrder(configuration.getDataFields()));
      Map<String, DataType[]> noDictSortAndNoSortDataTypes = CarbonDataProcessorUtil
          .getNoDictSortAndNoSortDataTypesAsDataFieldOrder(configuration.getDataFields());
      parameters.setNoDictSortDataType(noDictSortAndNoSortDataTypes.get("noDictSortDataTypes"));
      parameters.setNoDictNoSortDataType(noDictSortAndNoSortDataTypes.get("noDictNoSortDataTypes"));
      // keep partition columns in the end for table spec by getting rearranged tale spec
      TableSpec tableSpec = new TableSpec(configuration.getTableSpec().getCarbonTable(), true);
      parameters.setNoDictActualPosition(tableSpec.getNoDictDimActualPosition());
      parameters.setDictDimActualPosition(tableSpec.getDictDimActualPosition());
      parameters.setUpdateDictDims(tableSpec.isUpdateDictDim());
      parameters.setUpdateNonDictDims(tableSpec.isUpdateNoDictDims());
    } else {
      parameters.setNoDictionarySortColumn(CarbonDataProcessorUtil
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3042
          .getNoDictSortColMapping(parameters.getCarbonTable()));
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3552
      parameters.setNoDictSortColumnSchemaOrderMapping(CarbonDataProcessorUtil
          .getColumnIdxBasedOnSchemaInRow(parameters.getCarbonTable()));
      parameters.setMeasureDataType(configuration.getMeasureDataType());
      parameters.setNoDictDataType(CarbonDataProcessorUtil
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3042
          .getNoDictDataTypes(configuration.getTableSpec().getCarbonTable()));
      Map<String, DataType[]> noDictSortAndNoSortDataTypes = CarbonDataProcessorUtil
          .getNoDictSortAndNoSortDataTypes(configuration.getTableSpec().getCarbonTable());
      parameters.setNoDictSortDataType(noDictSortAndNoSortDataTypes.get("noDictSortDataTypes"));
      parameters.setNoDictNoSortDataType(noDictSortAndNoSortDataTypes.get("noDictNoSortDataTypes"));
      TableSpec tableSpec = configuration.getTableSpec();
      parameters.setNoDictActualPosition(tableSpec.getNoDictDimActualPosition());
      parameters.setDictDimActualPosition(tableSpec.getDictDimActualPosition());
      parameters.setUpdateDictDims(tableSpec.isUpdateDictDim());
      parameters.setUpdateNonDictDims(tableSpec.isUpdateNoDictDims());
    }
    return parameters;
  }

  public int getRangeId() {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2091
    return rangeId;
  }

  public void setRangeId(int rangeId) {
    this.rangeId = rangeId;
  }

  public static SortParameters createSortParameters(CarbonTable carbonTable, String databaseName,
      String tableName, int dimColCount, int complexDimColCount, int measureColCount,
      int noDictionaryCount, String segmentId, String taskNo, boolean[] noDictionaryColMaping,
      boolean[] sortColumnMapping, boolean[] isVarcharDimensionColumn, boolean isCompactionFlow,
      int numberOfCores) {
    SortParameters parameters = new SortParameters();
    CarbonProperties carbonProperties = CarbonProperties.getInstance();
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3042
    parameters.setCarbonTable(carbonTable);
    parameters.setDatabaseName(databaseName);
    parameters.setTableName(tableName);
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3208
    parameters.setPartitionID(CarbonTablePath.DEPRECATED_PARTITION_ID);
    parameters.setSegmentId(segmentId);
    parameters.setTaskNo(taskNo);
    parameters.setMeasureColCount(measureColCount);
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3196
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3203
    parameters.setDimColCount(dimColCount);
    parameters.setNumberOfSortColumns(carbonTable.getNumberOfSortColumns());
    parameters.setNoDictionaryCount(noDictionaryCount);
    parameters.setNumberOfNoDictSortColumns(carbonTable.getNumberOfNoDictSortColumns());
    parameters.setComplexDimColCount(complexDimColCount);
    parameters.setNoDictionaryDimnesionColumn(noDictionaryColMaping);
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2896
    parameters.setSortColumn(sortColumnMapping);
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2420
    parameters.setIsVarcharDimensionColumn(isVarcharDimensionColumn);
    parameters.setObserver(new SortObserver());
    // get sort buffer size
    parameters.setSortBufferSize(Integer.parseInt(carbonProperties
        .getProperty(CarbonCommonConstants.SORT_SIZE,
            CarbonCommonConstants.SORT_SIZE_DEFAULT_VAL)));
    LOGGER.info("Sort size for table: " + parameters.getSortBufferSize());
    // set number of intermedaite file to merge
    parameters.setNumberOfIntermediateFileToBeMerged(Integer.parseInt(carbonProperties
        .getProperty(CarbonCommonConstants.SORT_INTERMEDIATE_FILES_LIMIT,
            CarbonCommonConstants.SORT_INTERMEDIATE_FILES_LIMIT_DEFAULT_VALUE)));

    LOGGER.info("Number of intermediate file to be merged: " + parameters
        .getNumberOfIntermediateFileToBeMerged());

//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1281
    String[] carbonDataDirectoryPath = CarbonDataProcessorUtil
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3042
        .getLocalDataFolderLocation(carbonTable, taskNo, segmentId,
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-940
            isCompactionFlow, false);
    String[] sortTempDirs = CarbonDataProcessorUtil.arrayAppend(carbonDataDirectoryPath,
        File.separator, CarbonCommonConstants.SORT_TEMP_FILE_LOCATION);
    parameters.setTempFileLocation(sortTempDirs);
    LOGGER.info("temp file location: " + StringUtils.join(parameters.getTempFileLocation(), ","));

    parameters.setNumberOfCores(numberOfCores > 0 ? numberOfCores : 1);

    parameters.setFileWriteBufferSize(Integer.parseInt(carbonProperties
        .getProperty(CarbonCommonConstants.CARBON_SORT_FILE_WRITE_BUFFER_SIZE,
            CarbonCommonConstants.CARBON_SORT_FILE_WRITE_BUFFER_SIZE_DEFAULT_VALUE)));

//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1839
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1839
    parameters.setSortTempCompressorName(CarbonProperties.getInstance().getSortTempCompressor());
    if (!parameters.sortTempCompressorName.isEmpty()) {
      LOGGER.info(" Compression " + parameters.sortTempCompressorName
          + " will be used for writing the sort temp File");
    }

//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3591
    parameters.setPrefetch(CarbonCommonConstants.CARBON_PREFETCH_IN_MERGE_VALUE);
    parameters.setBufferSize(Integer.parseInt(carbonProperties.getProperty(
        CarbonCommonConstants.CARBON_PREFETCH_BUFFERSIZE,
        CarbonCommonConstants.CARBON_PREFETCH_BUFFERSIZE_DEFAULT)));

    DataType[] type = CarbonDataProcessorUtil
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3042
        .getMeasureDataType(parameters.getMeasureColCount(), parameters.getCarbonTable());
    parameters.setMeasureDataType(type);
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2896
    parameters.setNoDictDataType(CarbonDataProcessorUtil
        .getNoDictDataTypes(carbonTable));
    Map<String, DataType[]> noDictSortAndNoSortDataTypes = CarbonDataProcessorUtil
        .getNoDictSortAndNoSortDataTypes(parameters.getCarbonTable());
    parameters.setNoDictSortDataType(noDictSortAndNoSortDataTypes.get("noDictSortDataTypes"));
    parameters.setNoDictNoSortDataType(noDictSortAndNoSortDataTypes.get("noDictNoSortDataTypes"));
    parameters.setNoDictionarySortColumn(CarbonDataProcessorUtil
        .getNoDictSortColMapping(parameters.getCarbonTable()));
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3552
    parameters.setNoDictSortColumnSchemaOrderMapping(CarbonDataProcessorUtil
        .getColumnIdxBasedOnSchemaInRow(parameters.getCarbonTable()));
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3728
    TableSpec tableSpec = new TableSpec(carbonTable, false);
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3335
    parameters.setNoDictActualPosition(tableSpec.getNoDictDimActualPosition());
    parameters.setDictDimActualPosition(tableSpec.getDictDimActualPosition());
    parameters.setUpdateDictDims(tableSpec.isUpdateDictDim());
    parameters.setUpdateNonDictDims(tableSpec.isUpdateNoDictDims());
    return parameters;
  }

  public DataType[] getNoDictSortDataType() {
    return noDictSortDataType;
  }

  public void setNoDictSortDataType(DataType[] noDictSortDataType) {
    this.noDictSortDataType = noDictSortDataType;
  }

  public DataType[] getNoDictNoSortDataType() {
    return noDictNoSortDataType;
  }

  public DataType[] getNoDictDataType() {
    return noDictDataType;
  }

  public void setNoDictNoSortDataType(DataType[] noDictNoSortDataType) {
    this.noDictNoSortDataType = noDictNoSortDataType;
  }

  public void setNoDictDataType(DataType[] noDictDataType) {
    this.noDictDataType = noDictDataType;
  }

  public boolean[] getSortColumn() {
    return sortColumn;
  }

  public void setSortColumn(boolean[] sortColumn) {
    this.sortColumn = sortColumn;
  }

  public boolean isUpdateDictDims() {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3335
    return isUpdateDictDims;
  }

  public void setUpdateDictDims(boolean updateDictDims) {
    isUpdateDictDims = updateDictDims;
  }

  public boolean isUpdateNonDictDims() {
    return isUpdateNonDictDims;
  }

  public void setUpdateNonDictDims(boolean updateNonDictDims) {
    isUpdateNonDictDims = updateNonDictDims;
  }

  public int[] getDictDimActualPosition() {
    return dictDimActualPosition;
  }

  public void setDictDimActualPosition(int[] dictDimActualPosition) {
    this.dictDimActualPosition = dictDimActualPosition;
  }

  public int[] getNoDictActualPosition() {
    return noDictActualPosition;
  }

  public void setNoDictActualPosition(int[] noDictActualPosition) {
    this.noDictActualPosition = noDictActualPosition;
  }
}
