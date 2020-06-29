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

package org.apache.carbondata.processing.loading.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.index.Segment;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails;
import org.apache.carbondata.core.statusmanager.SegmentStatusManager;
import org.apache.carbondata.core.statusmanager.SegmentUpdateStatusManager;
import org.apache.carbondata.core.util.DataLoadMetrics;
import org.apache.carbondata.core.util.path.CarbonTablePath;

public class CarbonLoadModel implements Serializable {

  private static final long serialVersionUID = 6580168429197697465L;

  private String databaseName;

  private String tableName;

  private String factFilePath;

  private CarbonDataLoadSchema carbonDataLoadSchema;

  private String tablePath;

  /*
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2360
     This points if the carbonTable is a Non Transactional Table or not.
     The path will be pointed by the tablePath. And there will be
     no Metadata folder present for the Non Transactional Table.
   */
  private boolean carbonTransactionalTable = true;

  /* Number of thread in which sdk writer is used */
  private short sdkWriterCores;

  private String csvHeader;
  private String[] csvHeaderColumns;
  private String csvDelimiter;
  private ArrayList<String> complexDelimiters;

  private List<LoadMetadataDetails> loadMetadataDetails;
  private transient SegmentUpdateStatusManager segmentUpdateStatusManager;

  private String blocksID;

  /**
   * task id, each spark task has a unique id
   */
  private String taskNo;
  /**
   * new load start time
   */
  private long factTimeStamp;
  /**
   * load Id
   */
  private Segment segment;

  /**
   * escape Char
   */
  private String escapeChar;

  /**
   * quote Char
   */
  private String quoteChar;

  /**
   * comment Char
   */
  private String commentChar;

  private String lineSeparator;

  private String timestampFormat;

  private String dateFormat;

  private String defaultTimestampFormat;

  private String defaultDateFormat;

  /**
   * defines the string that should be treated as null while loading data
   */
  private String serializationNullFormat;

  /**
   * defines the string to specify whether the bad record logger should be enabled or not
   */
  private String badRecordsLoggerEnable;

  /**
   * defines the option to specify the bad record logger action
   */
  private String badRecordsAction;

  /**
   * Max number of columns that needs to be parsed by univocity parser
   */
  private String maxColumns;

  /**
   * defines the string to specify whether empty data is good or bad
   */
  private String isEmptyDataBadRecord;

  /**
   * defines the string to specify whether to skip empty line
   */
  private String skipEmptyLine;

  /**
   * Pre fetch data from csv reader
   */
  private boolean preFetch;

  /**
   * Batch sort should be enabled or not
   */
  private String sortScope;

  /**
   * bad record location
   */
  private String badRecordsLocation;

  /**
   * Number of partitions in global sort.
   */
  private String globalSortPartitions;

  private boolean isAggLoadRequest;
  /**
   * sort columns bounds
   */
  private String sortColumnsBoundsStr;

  /**
   * It directly writes data directly to nosort processor bypassing all other processors.
   * For this method there will be no data conversion step. It writes data which is directly
   * pushed into.
   */
  private boolean isLoadWithoutConverterStep;

  /**
   * Whether non-schema columns are present. This flag should be set only when all the schema
   * columns are already converted. Now, just need to generate and convert non-schema columns
   * present in data fields.
   */
  private boolean nonSchemaColumnsPresent;

  /**
   * for insert into flow, schema is already re-arranged. No need to re-arrange the data
   */
  private boolean isLoadWithoutConverterWithoutReArrangeStep;

  /**
   * To identify the suitable input processor step for json file loading.
   */
  private boolean isJsonFileLoad;

  /**
   * Folder path to where data should be written for this load.
   */
  private String dataWritePath;

  /**
   * sort columns bounds
   */
  private String loadMinSize;

  private List<String> mergedSegmentIds;

  /**
   * compressor used to compress column page
   */
  private String columnCompressor;

  /**
   * carbon binary decoder for loading data
   */
  private String binaryDecoder;

  /**
   * the total size of loading data
   */
  private long totalSize;

  /**
   * range partition data by this column
   */
  private CarbonColumn rangePartitionColumn;

  /**
   * control the file size of input data for each range partition
   * input size = max(bockletSize, (blockSize - blocketSize)) * scaleFactor
   */
  private int scaleFactor;

  /**
   * bucket id
   */
  private int bucketId;

  private DataLoadMetrics metrics;

  private boolean skipParsers = false;

  public void setSkipParsers() {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3687
    skipParsers = true;
  }

  public boolean isSkipParsers() {
    return skipParsers;
  }

  public boolean isAggLoadRequest() {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1520
    return isAggLoadRequest;
  }

  public void setAggLoadRequest(boolean aggLoadRequest) {
    isAggLoadRequest = aggLoadRequest;
  }

  /**
   * get escape char
   *
   * @return
   */
  public String getEscapeChar() {
    return escapeChar;
  }

  /**
   * set escape char
   *
   * @param escapeChar
   */
  public void setEscapeChar(String escapeChar) {
    this.escapeChar = escapeChar;
  }

  public String getCsvDelimiter() {
    return csvDelimiter;
  }

  public void setCsvDelimiter(String csvDelimiter) {
    this.csvDelimiter = csvDelimiter;
  }

  public void setComplexDelimiter(String delimiter) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3017
    checkAndInitializeComplexDelimiterList();
    this.complexDelimiters.add(delimiter);
  }

  public ArrayList<String> getComplexDelimiters() {
    checkAndInitializeComplexDelimiterList();
    return complexDelimiters;
  }

  public String getCsvHeader() {
    return csvHeader;
  }

  public void setCsvHeader(String csvHeader) {
    this.csvHeader = csvHeader;
  }

  public String[] getCsvHeaderColumns() {
    return csvHeaderColumns;
  }

  public void setCsvHeaderColumns(String[] csvHeaderColumns) {
    this.csvHeaderColumns = csvHeaderColumns;
  }

  /**
   * @return carbon dataload schema
   */
  public CarbonDataLoadSchema getCarbonDataLoadSchema() {
    return carbonDataLoadSchema;
  }

  /**
   * @param carbonDataLoadSchema
   */
  public void setCarbonDataLoadSchema(CarbonDataLoadSchema carbonDataLoadSchema) {
    this.carbonDataLoadSchema = carbonDataLoadSchema;
  }

  /**
   * @return the databaseName
   */
  public String getDatabaseName() {
    return databaseName;
  }

  /**
   * @param databaseName the databaseName to set
   */
  public void setDatabaseName(String databaseName) {
    this.databaseName = databaseName;
  }

  /**
   * @return the tableName
   */
  public String getTableName() {
    return tableName;
  }

  /**
   * @param tableName the tableName to set
   */
  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  /**
   * @return the factFilePath
   */
  public String getFactFilePath() {
    return factFilePath;
  }

  /**
   * @param factFilePath the factFilePath to set
   */
  public void setFactFilePath(String factFilePath) {
    this.factFilePath = factFilePath;
  }

  public String getSortColumnsBoundsStr() {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2091
    return sortColumnsBoundsStr;
  }

  public void setSortColumnsBoundsStr(String sortColumnsBoundsStr) {
    this.sortColumnsBoundsStr = sortColumnsBoundsStr;
  }

  public String getLoadMinSize() {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2309
    return loadMinSize;
  }

  public void setLoadMinSize(String loadMinSize) {
    this.loadMinSize = loadMinSize;
  }

  public int getBucketId() {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3721
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3590
    return bucketId;
  }

  public void setBucketId(int bucketId) {
    this.bucketId = bucketId;
  }

  /**
   * Get copy with taskNo.
   * Broadcast value is shared in process, so we need to copy it to make sure the value in each
   * task independently.
   *
   * @return
   */
  public CarbonLoadModel getCopyWithTaskNo(String taskNo) {
    CarbonLoadModel copy = new CarbonLoadModel();
    copy.tableName = tableName;
    copy.factFilePath = factFilePath;
    copy.databaseName = databaseName;
    copy.loadMetadataDetails = loadMetadataDetails;
    copy.csvHeader = csvHeader;
    copy.csvHeaderColumns = csvHeaderColumns;
    copy.csvDelimiter = csvDelimiter;
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3017
    copy.complexDelimiters = complexDelimiters;
    copy.carbonDataLoadSchema = carbonDataLoadSchema;
    copy.blocksID = blocksID;
    copy.taskNo = taskNo;
    copy.factTimeStamp = factTimeStamp;
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2428
    copy.segment = segment;
    copy.serializationNullFormat = serializationNullFormat;
    copy.badRecordsLoggerEnable = badRecordsLoggerEnable;
    copy.badRecordsAction = badRecordsAction;
    copy.escapeChar = escapeChar;
    copy.quoteChar = quoteChar;
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3740
    copy.lineSeparator = lineSeparator;
    copy.commentChar = commentChar;
    copy.timestampFormat = timestampFormat;
    copy.dateFormat = dateFormat;
    copy.defaultTimestampFormat = defaultTimestampFormat;
    copy.maxColumns = maxColumns;
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1573
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1573
    copy.tablePath = tablePath;
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2360
    copy.carbonTransactionalTable = carbonTransactionalTable;
    copy.preFetch = preFetch;
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-784
    copy.isEmptyDataBadRecord = isEmptyDataBadRecord;
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1734
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1734
    copy.skipEmptyLine = skipEmptyLine;
    copy.sortScope = sortScope;
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1520
    copy.isAggLoadRequest = isAggLoadRequest;
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1249
    copy.badRecordsLocation = badRecordsLocation;
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2413
    copy.isLoadWithoutConverterStep = isLoadWithoutConverterStep;
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2091
    copy.sortColumnsBoundsStr = sortColumnsBoundsStr;
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2309
    copy.loadMinSize = loadMinSize;
    copy.sdkWriterCores = sdkWriterCores;
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2851
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2852
    copy.columnCompressor = columnCompressor;
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3336
    copy.binaryDecoder = binaryDecoder;
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3262
    copy.rangePartitionColumn = rangePartitionColumn;
    copy.scaleFactor = scaleFactor;
    copy.totalSize = totalSize;
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3812
    copy.metrics = metrics;
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3637
    copy.isLoadWithoutConverterWithoutReArrangeStep = isLoadWithoutConverterWithoutReArrangeStep;
    return copy;
  }

  /**
   * get CarbonLoadModel with partition
   *
   * @param header
   * @param delimiter
   * @return
   */
  public CarbonLoadModel getCopyWithPartition(String header, String delimiter) {
    CarbonLoadModel copyObj = new CarbonLoadModel();
    copyObj.tableName = tableName;
    copyObj.factFilePath = null;
    copyObj.databaseName = databaseName;
    copyObj.loadMetadataDetails = loadMetadataDetails;
    copyObj.carbonDataLoadSchema = carbonDataLoadSchema;
    copyObj.csvHeader = header;
    copyObj.csvHeaderColumns = csvHeaderColumns;
    copyObj.csvDelimiter = delimiter;
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3017
    copyObj.complexDelimiters = complexDelimiters;
    copyObj.blocksID = blocksID;
    copyObj.taskNo = taskNo;
    copyObj.factTimeStamp = factTimeStamp;
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2428
    copyObj.segment = segment;
    copyObj.serializationNullFormat = serializationNullFormat;
    copyObj.badRecordsLoggerEnable = badRecordsLoggerEnable;
    copyObj.badRecordsAction = badRecordsAction;
    copyObj.escapeChar = escapeChar;
    copyObj.quoteChar = quoteChar;
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3740
    copyObj.lineSeparator = lineSeparator;
    copyObj.commentChar = commentChar;
    copyObj.timestampFormat = timestampFormat;
    copyObj.dateFormat = dateFormat;
    copyObj.defaultTimestampFormat = defaultTimestampFormat;
    copyObj.maxColumns = maxColumns;
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1573
    copyObj.tablePath = tablePath;
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2360
    copyObj.carbonTransactionalTable = carbonTransactionalTable;
    copyObj.preFetch = preFetch;
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-784
    copyObj.isEmptyDataBadRecord = isEmptyDataBadRecord;
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1734
    copyObj.skipEmptyLine = skipEmptyLine;
    copyObj.sortScope = sortScope;
    copyObj.badRecordsLocation = badRecordsLocation;
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1520
    copyObj.isAggLoadRequest = isAggLoadRequest;
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2091
    copyObj.sortColumnsBoundsStr = sortColumnsBoundsStr;
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2309
    copyObj.loadMinSize = loadMinSize;
    copyObj.sdkWriterCores = sdkWriterCores;
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2851
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2852
    copyObj.columnCompressor = columnCompressor;
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3336
    copyObj.binaryDecoder = binaryDecoder;
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3262
    copyObj.rangePartitionColumn = rangePartitionColumn;
    copyObj.scaleFactor = scaleFactor;
    copyObj.totalSize = totalSize;
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3812
    copyObj.metrics = metrics;
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3637
    copyObj.isLoadWithoutConverterStep = isLoadWithoutConverterStep;
    copyObj.isLoadWithoutConverterWithoutReArrangeStep = isLoadWithoutConverterWithoutReArrangeStep;
    return copyObj;
  }

  /**
   * @param tablePath The tablePath to set.
   */
  public void setTablePath(String tablePath) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1573
    this.tablePath = tablePath;
  }

  /**
   * @return Returns the factStoreLocation.
   */
  public String getTablePath() {
    return tablePath;
  }

  /**
   * getLoadMetadataDetails.
   *
   * @return
   */
  public List<LoadMetadataDetails> getLoadMetadataDetails() {
    return loadMetadataDetails;
  }

  /**
   * Get the current load metadata.
   *
   * @return
   */
  public LoadMetadataDetails getCurrentLoadMetadataDetail() {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1863
    if (loadMetadataDetails != null && loadMetadataDetails.size() > 0) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1855
      return loadMetadataDetails.get(loadMetadataDetails.size() - 1);
    } else {
      return null;
    }
  }

  /**
   * setLoadMetadataDetails.
   *
   * @param loadMetadataDetails
   */
  public void setLoadMetadataDetails(List<LoadMetadataDetails> loadMetadataDetails) {
    this.loadMetadataDetails = loadMetadataDetails;
  }

  /**
   * getSegmentUpdateStatusManager
   *
   * @return
   */
  public SegmentUpdateStatusManager getSegmentUpdateStatusManager() {
    return segmentUpdateStatusManager;
  }

  /**
   * setSegmentUpdateStatusManager
   *
   * @param segmentUpdateStatusManager
   */
  public void setSegmentUpdateStatusManager(SegmentUpdateStatusManager segmentUpdateStatusManager) {
    this.segmentUpdateStatusManager = segmentUpdateStatusManager;
  }

  /**
   * @return
   */
  public String getTaskNo() {
    return taskNo;
  }

  /**
   * @param taskNo
   */
  public void setTaskNo(String taskNo) {
    this.taskNo = taskNo;
  }

  /**
   * @return
   */
  public long getFactTimeStamp() {
    return factTimeStamp;
  }

  /**
   * @param factTimeStamp
   */
  public void setFactTimeStamp(long factTimeStamp) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-842
    this.factTimeStamp = factTimeStamp;
  }

  private void checkAndInitializeComplexDelimiterList() {
    if (null == complexDelimiters) {
      complexDelimiters = new ArrayList<>();
    }
  }

  /**
   * @return load Id
   */
  public String getSegmentId() {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2428
    if (segment != null) {
      return segment.getSegmentNo();
    } else {
      return null;
    }
  }

  /**
   * @param segmentId
   */
  public void setSegmentId(String segmentId) {
    if (segmentId != null) {
      this.segment = Segment.toSegment(segmentId);
    }
  }

  public Segment getSegment() {
    return segment;
  }

  /**
   * the method returns the value to be treated as null while data load
   *
   * @return
   */
  public String getSerializationNullFormat() {
    return serializationNullFormat;
  }

  /**
   * the method sets the value to be treated as null while data load
   *
   * @param serializationNullFormat
   */
  public void setSerializationNullFormat(String serializationNullFormat) {
    this.serializationNullFormat = serializationNullFormat;
  }

  /**
   * returns the string to enable bad record logger
   *
   * @return
   */
  public String getBadRecordsLoggerEnable() {
    return badRecordsLoggerEnable;
  }

  /**
   * method sets the string to specify whether to enable or dissable the badrecord logger.
   *
   * @param badRecordsLoggerEnable
   */
  public void setBadRecordsLoggerEnable(String badRecordsLoggerEnable) {
    this.badRecordsLoggerEnable = badRecordsLoggerEnable;
  }

  public String getQuoteChar() {
    return quoteChar;
  }

  public void setQuoteChar(String quoteChar) {
    this.quoteChar = quoteChar;
  }

  public String getLineSeparator() {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3740
    return lineSeparator;
  }

  public void setLineSeparator(String lineSeparator) {
    this.lineSeparator = lineSeparator;
  }

  public String getCommentChar() {
    return commentChar;
  }

  public void setCommentChar(String commentChar) {
    this.commentChar = commentChar;
  }

  public String getDateFormat() {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-603
    return dateFormat;
  }

  public void setDateFormat(String dateFormat) {
    this.dateFormat = dateFormat;
  }

  public String getDefaultTimestampFormat() {
    return defaultTimestampFormat;
  }

  public void setDefaultTimestampFormat(String defaultTimestampFormat) {
    this.defaultTimestampFormat = defaultTimestampFormat;
  }

  /**
   * @return
   */
  public String getMaxColumns() {
    return maxColumns;
  }

  /**
   * @param maxColumns
   */
  public void setMaxColumns(String maxColumns) {
    this.maxColumns = maxColumns;
  }

  /**
   * returns option to specify the bad record logger action
   *
   * @return
   */
  public String getBadRecordsAction() {
    return badRecordsAction;
  }

  /**
   * set option to specify the bad record logger action
   *
   * @param badRecordsAction
   */
  public void setBadRecordsAction(String badRecordsAction) {
    this.badRecordsAction = badRecordsAction;
  }

  public boolean isPreFetch() {
    return preFetch;
  }

  public void setPreFetch(boolean preFetch) {
    this.preFetch = preFetch;
  }

  public String getDefaultDateFormat() {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-603
    return defaultDateFormat;
  }

  public void setDefaultDateFormat(String defaultDateFormat) {
    this.defaultDateFormat = defaultDateFormat;
  }

  public String getIsEmptyDataBadRecord() {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-784
    return isEmptyDataBadRecord;
  }

  public void setIsEmptyDataBadRecord(String isEmptyDataBadRecord) {
    this.isEmptyDataBadRecord = isEmptyDataBadRecord;
  }

  public String getSortScope() {
    return sortScope;
  }

  public void setSortScope(String sortScope) {
    this.sortScope = sortScope;
  }

  public String getGlobalSortPartitions() {
    return globalSortPartitions;
  }

  public void setGlobalSortPartitions(String globalSortPartitions) {
    this.globalSortPartitions = globalSortPartitions;
  }

  public String getBadRecordsLocation() {
    return badRecordsLocation;
  }

  public void setBadRecordsLocation(String badRecordsLocation) {
    this.badRecordsLocation = badRecordsLocation;
  }

  public String getTimestampFormat() {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3740
    return timestampFormat;
  }

  public void setTimestampFormat(String timestampFormat) {
    this.timestampFormat = timestampFormat;
  }

  public String getSkipEmptyLine() {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1734
    return skipEmptyLine;
  }

  public void setSkipEmptyLine(String skipEmptyLine) {
    this.skipEmptyLine = skipEmptyLine;
  }

  public boolean isLoadWithoutConverterStep() {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2413
    return isLoadWithoutConverterStep;
  }

  public void setLoadWithoutConverterStep(boolean loadWithoutConverterStep) {
    isLoadWithoutConverterStep = loadWithoutConverterStep;
  }

  public boolean isJsonFileLoad() {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2608
    return isJsonFileLoad;
  }

  public void setJsonFileLoad(boolean isJsonFileLoad) {
    this.isJsonFileLoad = isJsonFileLoad;
  }

  public String getDataWritePath() {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2187
    return dataWritePath;
  }

  public void setDataWritePath(String dataWritePath) {
    this.dataWritePath = dataWritePath;
  }

  /**
   * Read segments metadata from table status file and set it to this load model object
   */
  public void readAndSetLoadMetadataDetails() {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2159
    String metadataPath = CarbonTablePath.getMetadataPath(tablePath);
    LoadMetadataDetails[] details = SegmentStatusManager.readLoadMetadata(metadataPath);
    setLoadMetadataDetails(Arrays.asList(details));
  }

  public boolean isCarbonTransactionalTable() {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2360
    return carbonTransactionalTable;
  }

  public void setCarbonTransactionalTable(boolean carbonTransactionalTable) {
    this.carbonTransactionalTable = carbonTransactionalTable;
  }

  public void setMergedSegmentIds(List<String> mergedSegmentIds) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2448
    this.mergedSegmentIds = mergedSegmentIds;
  }

  public List<String> getMergedSegmentIds() {
    if (null == mergedSegmentIds) {
      mergedSegmentIds = new ArrayList<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    }
    return mergedSegmentIds;
  }

  public short getSdkWriterCores() {
    return sdkWriterCores;
  }

  public void setSdkWriterCores(short sdkWriterCores) {
    this.sdkWriterCores = sdkWriterCores;
  }

  public String getColumnCompressor() {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2851
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2852
    return columnCompressor;
  }

  public void setColumnCompressor(String columnCompressor) {
    this.columnCompressor = columnCompressor;
  }

  public String getBinaryDecoder() {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3336
    return binaryDecoder;
  }

  public void setBinaryDecoder(String binaryDecoder) {
    this.binaryDecoder = binaryDecoder;
  }

  public CarbonColumn getRangePartitionColumn() {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3219
    return rangePartitionColumn;
  }

  public void setRangePartitionColumn(CarbonColumn rangePartitionColumn) {
    this.rangePartitionColumn = rangePartitionColumn;
  }

  public long getTotalSize() {
    return totalSize;
  }

  public void setTotalSize(long totalSize) {
    this.totalSize = totalSize;
  }

  public void setScaleFactor(int scaleFactor) {
    this.scaleFactor = scaleFactor;
  }

  public int getScaleFactor() {
    return scaleFactor;
  }

  public DataLoadMetrics getMetrics() {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3812
    return metrics;
  }

  public void setMetrics(DataLoadMetrics metrics) {
    this.metrics = metrics;
  }

  public boolean isLoadWithoutConverterWithoutReArrangeStep() {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3637
    return isLoadWithoutConverterWithoutReArrangeStep;
  }

  public void setLoadWithoutConverterWithoutReArrangeStep(
      boolean loadWithoutConverterWithoutReArrangeStep) {
    isLoadWithoutConverterWithoutReArrangeStep = loadWithoutConverterWithoutReArrangeStep;
  }

  public boolean isNonSchemaColumnsPresent() {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3548
    return nonSchemaColumnsPresent;
  }

  public void setNonSchemaColumnsPresent(boolean nonSchemaColumnsPresent) {
    this.nonSchemaColumnsPresent = nonSchemaColumnsPresent;
  }
}
