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

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.metadata.CarbonTableIdentifier;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.processing.loading.CarbonDataLoadConfiguration;
import org.apache.carbondata.processing.util.CarbonDataProcessorUtil;

import org.apache.commons.lang3.StringUtils;

public class SortParameters implements Serializable {

  private static final LogService LOGGER =
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

  private int numberOfSortColumns;

  private int numberOfNoDictSortColumns;

  private int numberOfCores;

  private int batchSortSizeinMb;

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
    parameters.sortTempCompressorName = sortTempCompressorName;
    parameters.prefetch = prefetch;
    parameters.bufferSize = bufferSize;
    parameters.databaseName = databaseName;
    parameters.tableName = tableName;
    parameters.measureDataType = measureDataType;
    parameters.noDictionaryCount = noDictionaryCount;
    parameters.partitionID = partitionID;
    parameters.segmentId = segmentId;
    parameters.taskNo = taskNo;
    parameters.noDictionaryDimnesionColumn = noDictionaryDimnesionColumn;
    parameters.noDictionarySortColumn = noDictionarySortColumn;
    parameters.numberOfSortColumns = numberOfSortColumns;
    parameters.numberOfNoDictSortColumns = numberOfNoDictSortColumns;
    parameters.numberOfCores = numberOfCores;
    parameters.batchSortSizeinMb = batchSortSizeinMb;
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

  public int getFileBufferSize() {
    return fileBufferSize;
  }

  public void setFileBufferSize(int fileBufferSize) {
    this.fileBufferSize = fileBufferSize;
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

  public int getNumberOfCores() {
    return numberOfCores;
  }

  public void setNumberOfCores(int numberOfCores) {
    this.numberOfCores = numberOfCores;
  }

  public int getNumberOfSortColumns() {
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

  public int getBatchSortSizeinMb() {
    return batchSortSizeinMb;
  }

  public void setBatchSortSizeinMb(int batchSortSizeinMb) {
    this.batchSortSizeinMb = batchSortSizeinMb;
  }

  public static SortParameters createSortParameters(CarbonDataLoadConfiguration configuration) {
    SortParameters parameters = new SortParameters();
    CarbonTableIdentifier tableIdentifier =
        configuration.getTableIdentifier().getCarbonTableIdentifier();
    CarbonProperties carbonProperties = CarbonProperties.getInstance();
    parameters.setDatabaseName(tableIdentifier.getDatabaseName());
    parameters.setTableName(tableIdentifier.getTableName());
    parameters.setPartitionID("0");
    parameters.setSegmentId(configuration.getSegmentId());
    parameters.setTaskNo(configuration.getTaskNo());
    parameters.setMeasureColCount(configuration.getMeasureCount());
    parameters.setDimColCount(
        configuration.getDimensionCount() - configuration.getComplexColumnCount());
    parameters.setNoDictionaryCount(configuration.getNoDictionaryCount());
    parameters.setComplexDimColCount(configuration.getComplexColumnCount());
    parameters.setNoDictionaryDimnesionColumn(
        CarbonDataProcessorUtil.getNoDictionaryMapping(configuration.getDataFields()));
    parameters.setBatchSortSizeinMb(CarbonDataProcessorUtil.getBatchSortSizeinMb(configuration));

    parameters.setNumberOfSortColumns(configuration.getNumberOfSortColumns());
    parameters.setNumberOfNoDictSortColumns(configuration.getNumberOfNoDictSortColumns());
    setNoDictionarySortColumnMapping(parameters);
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

    // get file buffer size
    parameters.setFileBufferSize(CarbonDataProcessorUtil
        .getFileBufferSize(parameters.getNumberOfIntermediateFileToBeMerged(), carbonProperties,
            CarbonCommonConstants.CONSTANT_SIZE_TEN));

    LOGGER.info("File Buffer Size: " + parameters.getFileBufferSize());

    String[] carbonDataDirectoryPath = CarbonDataProcessorUtil.getLocalDataFolderLocation(
        tableIdentifier.getDatabaseName(), tableIdentifier.getTableName(),
        configuration.getTaskNo(), configuration.getSegmentId(), false, false);
    String[] sortTempDirs = CarbonDataProcessorUtil.arrayAppend(carbonDataDirectoryPath,
        File.separator, CarbonCommonConstants.SORT_TEMP_FILE_LOCATION);

    parameters.setTempFileLocation(sortTempDirs);
    LOGGER.info("temp file location: " + StringUtils.join(parameters.getTempFileLocation(), ","));

    int numberOfCores = carbonProperties.getNumberOfCores() / 2;
    // In case of loading from partition we should use the cores specified by it
    if (configuration.getWritingCoresCount() > 0) {
      numberOfCores = configuration.getWritingCoresCount();
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

    DataType[] measureDataType = configuration.getMeasureDataType();
    parameters.setMeasureDataType(measureDataType);
    return parameters;
  }

  /**
   * this method will set the boolean mapping for no dictionary sort columns
   *
   * @param parameters
   */
  private static void setNoDictionarySortColumnMapping(SortParameters parameters) {
    if (parameters.getNumberOfSortColumns() == parameters.getNoDictionaryDimnesionColumn().length) {
      parameters.setNoDictionarySortColumn(parameters.getNoDictionaryDimnesionColumn());
    } else {
      boolean[] noDictionarySortColumnTemp = new boolean[parameters.getNumberOfSortColumns()];
      System
          .arraycopy(parameters.getNoDictionaryDimnesionColumn(), 0, noDictionarySortColumnTemp, 0,
              parameters.getNumberOfSortColumns());
      parameters.setNoDictionarySortColumn(noDictionarySortColumnTemp);
    }
  }

  public static SortParameters createSortParameters(CarbonTable carbonTable, String databaseName,
      String tableName, int dimColCount, int complexDimColCount, int measureColCount,
      int noDictionaryCount, String segmentId, String taskNo,
      boolean[] noDictionaryColMaping, boolean isCompactionFlow) {
    SortParameters parameters = new SortParameters();
    CarbonProperties carbonProperties = CarbonProperties.getInstance();
    parameters.setDatabaseName(databaseName);
    parameters.setTableName(tableName);
    parameters.setPartitionID(CarbonTablePath.DEPRECATED_PATITION_ID);
    parameters.setSegmentId(segmentId);
    parameters.setTaskNo(taskNo);
    parameters.setMeasureColCount(measureColCount);
    parameters.setDimColCount(dimColCount - complexDimColCount);
    parameters.setNumberOfSortColumns(carbonTable.getNumberOfSortColumns());
    parameters.setNoDictionaryCount(noDictionaryCount);
    parameters.setNumberOfNoDictSortColumns(carbonTable.getNumberOfNoDictSortColumns());
    parameters.setComplexDimColCount(complexDimColCount);
    parameters.setNoDictionaryDimnesionColumn(noDictionaryColMaping);
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

    // get file buffer size
    parameters.setFileBufferSize(CarbonDataProcessorUtil
        .getFileBufferSize(parameters.getNumberOfIntermediateFileToBeMerged(), carbonProperties,
            CarbonCommonConstants.CONSTANT_SIZE_TEN));

    LOGGER.info("File Buffer Size: " + parameters.getFileBufferSize());

    String[] carbonDataDirectoryPath = CarbonDataProcessorUtil
        .getLocalDataFolderLocation(databaseName, tableName, taskNo, segmentId,
            isCompactionFlow, false);
    String[] sortTempDirs = CarbonDataProcessorUtil.arrayAppend(carbonDataDirectoryPath,
        File.separator, CarbonCommonConstants.SORT_TEMP_FILE_LOCATION);
    parameters.setTempFileLocation(sortTempDirs);
    LOGGER.info("temp file location: " + StringUtils.join(parameters.getTempFileLocation(), ","));

    int numberOfCores = carbonProperties.getNumberOfCores() / 2;
    parameters.setNumberOfCores(numberOfCores > 0 ? numberOfCores : 1);

    parameters.setFileWriteBufferSize(Integer.parseInt(carbonProperties
        .getProperty(CarbonCommonConstants.CARBON_SORT_FILE_WRITE_BUFFER_SIZE,
            CarbonCommonConstants.CARBON_SORT_FILE_WRITE_BUFFER_SIZE_DEFAULT_VALUE)));

    parameters.setSortTempCompressorName(CarbonProperties.getInstance().getSortTempCompressor());
    if (!parameters.sortTempCompressorName.isEmpty()) {
      LOGGER.info(" Compression " + parameters.sortTempCompressorName
          + " will be used for writing the sort temp File");
    }

    parameters.setPrefetch(CarbonCommonConstants. CARBON_PREFETCH_IN_MERGE_VALUE);
    parameters.setBufferSize(Integer.parseInt(carbonProperties.getProperty(
        CarbonCommonConstants.CARBON_PREFETCH_BUFFERSIZE,
        CarbonCommonConstants.CARBON_PREFETCH_BUFFERSIZE_DEFAULT)));

    DataType[] type = CarbonDataProcessorUtil
        .getMeasureDataType(parameters.getMeasureColCount(), parameters.getDatabaseName(),
            parameters.getTableName());
    parameters.setMeasureDataType(type);
    setNoDictionarySortColumnMapping(parameters);
    return parameters;
  }

}
