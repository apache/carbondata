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
package org.apache.carbondata.processing.sortandgroupby.sortdata;

import java.io.File;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.metadata.CarbonTableIdentifier;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.processing.newflow.CarbonDataLoadConfiguration;
import org.apache.carbondata.processing.schema.metadata.SortObserver;
import org.apache.carbondata.processing.util.CarbonDataProcessorUtil;

public class SortParameters {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(SortParameters.class.getName());
  /**
   * tempFileLocation
   */
  private String tempFileLocation;
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
  /**
   * sortTempFileNoOFRecordsInCompression
   */
  private int sortTempFileNoOFRecordsInCompression;
  /**
   * isSortTempFileCompressionEnabled
   */
  private boolean isSortFileCompressionEnabled;
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

  private char[] aggType;

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

  private int numberOfCores;

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
    parameters.sortTempFileNoOFRecordsInCompression = sortTempFileNoOFRecordsInCompression;
    parameters.isSortFileCompressionEnabled = isSortFileCompressionEnabled;
    parameters.prefetch = prefetch;
    parameters.bufferSize = bufferSize;
    parameters.databaseName = databaseName;
    parameters.tableName = tableName;
    parameters.aggType = aggType;
    parameters.noDictionaryCount = noDictionaryCount;
    parameters.partitionID = partitionID;
    parameters.segmentId = segmentId;
    parameters.taskNo = taskNo;
    parameters.noDictionaryDimnesionColumn = noDictionaryDimnesionColumn;
    parameters.numberOfCores = numberOfCores;
    return parameters;
  }

  public String getTempFileLocation() {
    return tempFileLocation;
  }

  public void setTempFileLocation(String tempFileLocation) {
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

  public int getSortTempFileNoOFRecordsInCompression() {
    return sortTempFileNoOFRecordsInCompression;
  }

  public void setSortTempFileNoOFRecordsInCompression(int sortTempFileNoOFRecordsInCompression) {
    this.sortTempFileNoOFRecordsInCompression = sortTempFileNoOFRecordsInCompression;
  }

  public boolean isSortFileCompressionEnabled() {
    return isSortFileCompressionEnabled;
  }

  public void setSortFileCompressionEnabled(boolean sortFileCompressionEnabled) {
    isSortFileCompressionEnabled = sortFileCompressionEnabled;
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

  public char[] getAggType() {
    return aggType;
  }

  public void setAggType(char[] aggType) {
    this.aggType = aggType;
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

  public static SortParameters createSortParameters(CarbonDataLoadConfiguration configuration) {
    SortParameters parameters = new SortParameters();
    CarbonTableIdentifier tableIdentifier =
        configuration.getTableIdentifier().getCarbonTableIdentifier();
    CarbonProperties carbonProperties = CarbonProperties.getInstance();
    parameters.setDatabaseName(tableIdentifier.getDatabaseName());
    parameters.setTableName(tableIdentifier.getTableName());
    parameters.setPartitionID(configuration.getPartitionId());
    parameters.setSegmentId(configuration.getSegmentId());
    parameters.setTaskNo(configuration.getTaskNo());
    parameters.setMeasureColCount(configuration.getMeasureCount());
    parameters.setDimColCount(
        configuration.getDimensionCount() - configuration.getComplexDimensionCount());
    parameters.setNoDictionaryCount(configuration.getNoDictionaryCount());
    parameters.setComplexDimColCount(configuration.getComplexDimensionCount());
    parameters.setNoDictionaryDimnesionColumn(
        CarbonDataProcessorUtil.getNoDictionaryMapping(configuration.getDataFields()));
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

    String carbonDataDirectoryPath = CarbonDataProcessorUtil
        .getLocalDataFolderLocation(tableIdentifier.getDatabaseName(),
            tableIdentifier.getTableName(), configuration.getTaskNo(),
            configuration.getPartitionId(), configuration.getSegmentId(), false);
    parameters.setTempFileLocation(
        carbonDataDirectoryPath + File.separator + CarbonCommonConstants.SORT_TEMP_FILE_LOCATION);
    LOGGER.info("temp file location" + parameters.getTempFileLocation());

    int numberOfCores;
    try {
      numberOfCores = Integer.parseInt(carbonProperties
          .getProperty(CarbonCommonConstants.NUM_CORES_LOADING,
              CarbonCommonConstants.NUM_CORES_DEFAULT_VAL));
      numberOfCores = numberOfCores / 2;
    } catch (NumberFormatException exc) {
      numberOfCores = Integer.parseInt(CarbonCommonConstants.NUM_CORES_DEFAULT_VAL);
    }
    parameters.setNumberOfCores(numberOfCores > 0 ? numberOfCores : 1);

    parameters.setFileWriteBufferSize(Integer.parseInt(carbonProperties
        .getProperty(CarbonCommonConstants.CARBON_SORT_FILE_WRITE_BUFFER_SIZE,
            CarbonCommonConstants.CARBON_SORT_FILE_WRITE_BUFFER_SIZE_DEFAULT_VALUE)));

    parameters.setSortFileCompressionEnabled(Boolean.parseBoolean(carbonProperties
        .getProperty(CarbonCommonConstants.IS_SORT_TEMP_FILE_COMPRESSION_ENABLED,
            CarbonCommonConstants.IS_SORT_TEMP_FILE_COMPRESSION_ENABLED_DEFAULTVALUE)));

    int sortTempFileNoOFRecordsInCompression;
    try {
      sortTempFileNoOFRecordsInCompression = Integer.parseInt(carbonProperties
          .getProperty(CarbonCommonConstants.SORT_TEMP_FILE_NO_OF_RECORDS_FOR_COMPRESSION,
              CarbonCommonConstants.SORT_TEMP_FILE_NO_OF_RECORD_FOR_COMPRESSION_DEFAULTVALUE));
      if (sortTempFileNoOFRecordsInCompression < 1) {
        LOGGER.error("Invalid value for: "
            + CarbonCommonConstants.SORT_TEMP_FILE_NO_OF_RECORDS_FOR_COMPRESSION
            + ":Only Positive Integer value(greater than zero) is allowed.Default value will "
            + "be used");

        sortTempFileNoOFRecordsInCompression = Integer.parseInt(
            CarbonCommonConstants.SORT_TEMP_FILE_NO_OF_RECORD_FOR_COMPRESSION_DEFAULTVALUE);
      }
    } catch (NumberFormatException e) {
      LOGGER.error(
          "Invalid value for: " + CarbonCommonConstants.SORT_TEMP_FILE_NO_OF_RECORDS_FOR_COMPRESSION
              + ", only Positive Integer value is allowed. Default value will be used");

      sortTempFileNoOFRecordsInCompression = Integer
          .parseInt(CarbonCommonConstants.SORT_TEMP_FILE_NO_OF_RECORD_FOR_COMPRESSION_DEFAULTVALUE);
    }
    parameters.setSortTempFileNoOFRecordsInCompression(sortTempFileNoOFRecordsInCompression);

    if (parameters.isSortFileCompressionEnabled()) {
      LOGGER.info("Compression will be used for writing the sort temp File");
    }

    parameters.setPrefetch(CarbonCommonConstants.CARBON_PREFETCH_IN_MERGE_VALUE);
    parameters.setBufferSize(Integer.parseInt(carbonProperties.getProperty(
        CarbonCommonConstants.CARBON_PREFETCH_BUFFERSIZE,
        CarbonCommonConstants.CARBON_PREFETCH_BUFFERSIZE_DEFAULT)));

    char[] aggType = CarbonDataProcessorUtil
        .getAggType(configuration.getMeasureCount(), configuration.getMeasureFields());
    parameters.setAggType(aggType);
    return parameters;
  }

  public static SortParameters createSortParameters(String databaseName, String tableName,
      int dimColCount, int complexDimColCount, int measureColCount, int noDictionaryCount,
      String partitionID, String segmentId, String taskNo,
      boolean[] noDictionaryColMaping) {
    SortParameters parameters = new SortParameters();
    CarbonProperties carbonProperties = CarbonProperties.getInstance();
    parameters.setDatabaseName(databaseName);
    parameters.setTableName(tableName);
    parameters.setPartitionID(partitionID);
    parameters.setSegmentId(segmentId);
    parameters.setTaskNo(taskNo);
    parameters.setMeasureColCount(measureColCount);
    parameters.setDimColCount(dimColCount - complexDimColCount);
    parameters.setNoDictionaryCount(noDictionaryCount);
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

    String carbonDataDirectoryPath = CarbonDataProcessorUtil
        .getLocalDataFolderLocation(databaseName, tableName, taskNo, partitionID, segmentId, false);
    parameters.setTempFileLocation(
        carbonDataDirectoryPath + File.separator + CarbonCommonConstants.SORT_TEMP_FILE_LOCATION);
    LOGGER.info("temp file location" + parameters.getTempFileLocation());

    int numberOfCores;
    try {
      numberOfCores = Integer.parseInt(carbonProperties
          .getProperty(CarbonCommonConstants.NUM_CORES_LOADING,
              CarbonCommonConstants.NUM_CORES_DEFAULT_VAL));
      numberOfCores = numberOfCores / 2;
    } catch (NumberFormatException exc) {
      numberOfCores = Integer.parseInt(CarbonCommonConstants.NUM_CORES_DEFAULT_VAL);
    }
    parameters.setNumberOfCores(numberOfCores > 0 ? numberOfCores : 1);

    parameters.setFileWriteBufferSize(Integer.parseInt(carbonProperties
        .getProperty(CarbonCommonConstants.CARBON_SORT_FILE_WRITE_BUFFER_SIZE,
            CarbonCommonConstants.CARBON_SORT_FILE_WRITE_BUFFER_SIZE_DEFAULT_VALUE)));

    parameters.setSortFileCompressionEnabled(Boolean.parseBoolean(carbonProperties
        .getProperty(CarbonCommonConstants.IS_SORT_TEMP_FILE_COMPRESSION_ENABLED,
            CarbonCommonConstants.IS_SORT_TEMP_FILE_COMPRESSION_ENABLED_DEFAULTVALUE)));

    int sortTempFileNoOFRecordsInCompression;
    try {
      sortTempFileNoOFRecordsInCompression = Integer.parseInt(carbonProperties
          .getProperty(CarbonCommonConstants.SORT_TEMP_FILE_NO_OF_RECORDS_FOR_COMPRESSION,
              CarbonCommonConstants.SORT_TEMP_FILE_NO_OF_RECORD_FOR_COMPRESSION_DEFAULTVALUE));
      if (sortTempFileNoOFRecordsInCompression < 1) {
        LOGGER.error("Invalid value for: "
            + CarbonCommonConstants.SORT_TEMP_FILE_NO_OF_RECORDS_FOR_COMPRESSION
            + ":Only Positive Integer value(greater than zero) is allowed.Default value will "
            + "be used");

        sortTempFileNoOFRecordsInCompression = Integer.parseInt(
            CarbonCommonConstants.SORT_TEMP_FILE_NO_OF_RECORD_FOR_COMPRESSION_DEFAULTVALUE);
      }
    } catch (NumberFormatException e) {
      LOGGER.error(
          "Invalid value for: " + CarbonCommonConstants.SORT_TEMP_FILE_NO_OF_RECORDS_FOR_COMPRESSION
              + ", only Positive Integer value is allowed. Default value will be used");

      sortTempFileNoOFRecordsInCompression = Integer
          .parseInt(CarbonCommonConstants.SORT_TEMP_FILE_NO_OF_RECORD_FOR_COMPRESSION_DEFAULTVALUE);
    }
    parameters.setSortTempFileNoOFRecordsInCompression(sortTempFileNoOFRecordsInCompression);

    if (parameters.isSortFileCompressionEnabled()) {
      LOGGER.info("Compression will be used for writing the sort temp File");
    }

    parameters.setPrefetch(CarbonCommonConstants. CARBON_PREFETCH_IN_MERGE_VALUE);
    parameters.setBufferSize(Integer.parseInt(carbonProperties.getProperty(
        CarbonCommonConstants.CARBON_PREFETCH_BUFFERSIZE,
        CarbonCommonConstants.CARBON_PREFETCH_BUFFERSIZE_DEFAULT)));

    char[] aggType = CarbonDataProcessorUtil
        .getAggType(parameters.getMeasureColCount(), parameters.getDatabaseName(),
            parameters.getTableName());
    parameters.setAggType(aggType);
    return parameters;
  }

}
