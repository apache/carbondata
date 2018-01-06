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
import java.util.HashMap;
import java.util.List;

import org.apache.carbondata.core.dictionary.service.DictionaryServiceProvider;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails;
import org.apache.carbondata.core.statusmanager.SegmentUpdateStatusManager;


public class CarbonLoadModel implements Serializable {

  private static final long serialVersionUID = 6580168429197697465L;

  private String databaseName;

  private String tableName;

  private String factFilePath;

  private String colDictFilePath;

  private CarbonDataLoadSchema carbonDataLoadSchema;

  private boolean aggLoadRequest;

  private String tablePath;

  private boolean isRetentionRequest;

  private String csvHeader;
  private String[] csvHeaderColumns;
  private String csvDelimiter;
  private String complexDelimiterLevel1;
  private String complexDelimiterLevel2;

  private List<LoadMetadataDetails> loadMetadataDetails;
  private transient SegmentUpdateStatusManager segmentUpdateStatusManager;

  private String blocksID;

  /**
   * Map from carbon dimension to pre defined dict file path
   */
  private HashMap<CarbonDimension, String> predefDictMap;

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
  private String segmentId;

  private String allDictPath;

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

  private String timestampformat;

  private String dateFormat;

  private String defaultTimestampFormat;

  private String defaultDateFormat;

  /**
   * defines the string that should be treated as null while loadind data
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
   * Use one pass to generate dictionary
   */
  private boolean useOnePass;

  /**
   * dictionary server host
   */
  private String dictionaryServerHost;

  /**
   * dictionary sever port
   */
  private int dictionaryServerPort;

  /**
   * dictionary server communication Secret Key.
   */
  private String dictionaryServerSecretKey;

  /**
   * dictionary service provider.
   */
  private DictionaryServiceProvider dictionaryServiceProvider;

  /**
   * Dictionary Secure or not.
   */
  private Boolean dictionaryEncryptServerSecure;
  /**
   * Pre fetch data from csv reader
   */
  private boolean preFetch;

  /**
   * Batch sort should be enabled or not
   */
  private String sortScope;

  /**
   * Batch sort size in mb.
   */
  private String batchSortSizeInMb;
  /**
   * bad record location
   */
  private String badRecordsLocation;

  /**
   * Number of partitions in global sort.
   */
  private String globalSortPartitions;

  private boolean isAggLoadRequest;

  public boolean isAggLoadRequest() {
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

  public String getComplexDelimiterLevel1() {
    return complexDelimiterLevel1;
  }

  public void setComplexDelimiterLevel1(String complexDelimiterLevel1) {
    this.complexDelimiterLevel1 = complexDelimiterLevel1;
  }

  public String getComplexDelimiterLevel2() {
    return complexDelimiterLevel2;
  }

  public void setComplexDelimiterLevel2(String complexDelimiterLevel2) {
    this.complexDelimiterLevel2 = complexDelimiterLevel2;
  }

  public String getAllDictPath() {
    return allDictPath;
  }

  public void setAllDictPath(String allDictPath) {
    this.allDictPath = allDictPath;
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

  public void initPredefDictMap() {
    predefDictMap = new HashMap<>();
  }

  public String getPredefDictFilePath(CarbonDimension dimension) {
    return predefDictMap.get(dimension);
  }

  public void setPredefDictMap(CarbonDimension dimension, String predefDictFilePath) {
    this.predefDictMap.put(dimension, predefDictFilePath);
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

  /**
   * @return external column dictionary file path
   */
  public String getColDictFilePath() {
    return colDictFilePath;
  }

  /**
   * set external column dictionary file path
   *
   * @param colDictFilePath
   */
  public void setColDictFilePath(String colDictFilePath) {
    this.colDictFilePath = colDictFilePath;
  }


  public DictionaryServiceProvider getDictionaryServiceProvider() {
    return dictionaryServiceProvider;
  }

  public void setDictionaryServiceProvider(DictionaryServiceProvider dictionaryServiceProvider) {
    this.dictionaryServiceProvider = dictionaryServiceProvider;
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
    copy.aggLoadRequest = aggLoadRequest;
    copy.loadMetadataDetails = loadMetadataDetails;
    copy.isRetentionRequest = isRetentionRequest;
    copy.csvHeader = csvHeader;
    copy.csvHeaderColumns = csvHeaderColumns;
    copy.csvDelimiter = csvDelimiter;
    copy.complexDelimiterLevel1 = complexDelimiterLevel1;
    copy.complexDelimiterLevel2 = complexDelimiterLevel2;
    copy.carbonDataLoadSchema = carbonDataLoadSchema;
    copy.blocksID = blocksID;
    copy.taskNo = taskNo;
    copy.factTimeStamp = factTimeStamp;
    copy.segmentId = segmentId;
    copy.serializationNullFormat = serializationNullFormat;
    copy.badRecordsLoggerEnable = badRecordsLoggerEnable;
    copy.badRecordsAction = badRecordsAction;
    copy.escapeChar = escapeChar;
    copy.quoteChar = quoteChar;
    copy.commentChar = commentChar;
    copy.timestampformat = timestampformat;
    copy.dateFormat = dateFormat;
    copy.defaultTimestampFormat = defaultTimestampFormat;
    copy.maxColumns = maxColumns;
    copy.tablePath = tablePath;
    copy.useOnePass = useOnePass;
    copy.dictionaryServerHost = dictionaryServerHost;
    copy.dictionaryServerPort = dictionaryServerPort;
    copy.dictionaryServerSecretKey = dictionaryServerSecretKey;
    copy.dictionaryServiceProvider = dictionaryServiceProvider;
    copy.dictionaryEncryptServerSecure = dictionaryEncryptServerSecure;
    copy.preFetch = preFetch;
    copy.isEmptyDataBadRecord = isEmptyDataBadRecord;
    copy.skipEmptyLine = skipEmptyLine;
    copy.sortScope = sortScope;
    copy.batchSortSizeInMb = batchSortSizeInMb;
    copy.isAggLoadRequest = isAggLoadRequest;
    copy.badRecordsLocation = badRecordsLocation;
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
    copyObj.aggLoadRequest = aggLoadRequest;
    copyObj.loadMetadataDetails = loadMetadataDetails;
    copyObj.isRetentionRequest = isRetentionRequest;
    copyObj.carbonDataLoadSchema = carbonDataLoadSchema;
    copyObj.csvHeader = header;
    copyObj.csvHeaderColumns = csvHeaderColumns;
    copyObj.csvDelimiter = delimiter;
    copyObj.complexDelimiterLevel1 = complexDelimiterLevel1;
    copyObj.complexDelimiterLevel2 = complexDelimiterLevel2;
    copyObj.blocksID = blocksID;
    copyObj.taskNo = taskNo;
    copyObj.factTimeStamp = factTimeStamp;
    copyObj.segmentId = segmentId;
    copyObj.serializationNullFormat = serializationNullFormat;
    copyObj.badRecordsLoggerEnable = badRecordsLoggerEnable;
    copyObj.badRecordsAction = badRecordsAction;
    copyObj.escapeChar = escapeChar;
    copyObj.quoteChar = quoteChar;
    copyObj.commentChar = commentChar;
    copyObj.timestampformat = timestampformat;
    copyObj.dateFormat = dateFormat;
    copyObj.defaultTimestampFormat = defaultTimestampFormat;
    copyObj.maxColumns = maxColumns;
    copyObj.tablePath = tablePath;
    copyObj.useOnePass = useOnePass;
    copyObj.dictionaryServerHost = dictionaryServerHost;
    copyObj.dictionaryServerPort = dictionaryServerPort;
    copyObj.dictionaryServerSecretKey = dictionaryServerSecretKey;
    copyObj.dictionaryServiceProvider = dictionaryServiceProvider;
    copyObj.dictionaryEncryptServerSecure = dictionaryEncryptServerSecure;
    copyObj.preFetch = preFetch;
    copyObj.isEmptyDataBadRecord = isEmptyDataBadRecord;
    copyObj.skipEmptyLine = skipEmptyLine;
    copyObj.sortScope = sortScope;
    copyObj.batchSortSizeInMb = batchSortSizeInMb;
    copyObj.badRecordsLocation = badRecordsLocation;
    copyObj.isAggLoadRequest = isAggLoadRequest;
    return copyObj;
  }

  /**
   * @param tablePath The tablePath to set.
   */
  public void setTablePath(String tablePath) {
    this.tablePath = tablePath;
  }

  /**
   * @return Returns the factStoreLocation.
   */
  public String getTablePath() {
    return tablePath;
  }

  /**
   * isRetentionRequest
   *
   * @return
   */
  public boolean isRetentionRequest() {
    return isRetentionRequest;
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
    if (loadMetadataDetails != null && loadMetadataDetails.size() > 0) {
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
    this.factTimeStamp = factTimeStamp;
  }

  public String[] getDelimiters() {
    return new String[] { complexDelimiterLevel1, complexDelimiterLevel2 };
  }

  /**
   * @return load Id
   */
  public String getSegmentId() {
    return segmentId;
  }

  /**
   * @param segmentId
   */
  public void setSegmentId(String segmentId) {
    this.segmentId = segmentId;
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

  public String getCommentChar() {
    return commentChar;
  }

  public void setCommentChar(String commentChar) {
    this.commentChar = commentChar;
  }

  public String getDateFormat() {
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

  public boolean getUseOnePass() {
    return useOnePass;
  }

  public void setUseOnePass(boolean useOnePass) {
    this.useOnePass = useOnePass;
  }

  public int getDictionaryServerPort() {
    return dictionaryServerPort;
  }

  public void setDictionaryServerPort(int dictionaryServerPort) {
    this.dictionaryServerPort = dictionaryServerPort;
  }

  public String getDictionaryServerSecretKey() {
    return dictionaryServerSecretKey;
  }

  public void setDictionaryServerSecretKey(String dictionaryServerSecretKey) {
    this.dictionaryServerSecretKey = dictionaryServerSecretKey;
  }

  public Boolean getDictionaryEncryptServerSecure() {
    return dictionaryEncryptServerSecure;
  }

  public void setDictionaryEncryptServerSecure(Boolean dictionaryEncryptServerSecure) {
    this.dictionaryEncryptServerSecure = dictionaryEncryptServerSecure;
  }

  public String getDictionaryServerHost() {
    return dictionaryServerHost;
  }

  public void setDictionaryServerHost(String dictionaryServerHost) {
    this.dictionaryServerHost = dictionaryServerHost;
  }

  public boolean isPreFetch() {
    return preFetch;
  }

  public void setPreFetch(boolean preFetch) {
    this.preFetch = preFetch;
  }

  public String getDefaultDateFormat() {
    return defaultDateFormat;
  }

  public void setDefaultDateFormat(String defaultDateFormat) {
    this.defaultDateFormat = defaultDateFormat;
  }

  public String getIsEmptyDataBadRecord() {
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

  public String getBatchSortSizeInMb() {
    return batchSortSizeInMb;
  }

  public void setBatchSortSizeInMb(String batchSortSizeInMb) {
    this.batchSortSizeInMb = batchSortSizeInMb;
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

  public String getTimestampformat() {
    return timestampformat;
  }

  public void setTimestampformat(String timestampformat) {
    this.timestampformat = timestampformat;
  }
  public String getSkipEmptyLine() {
    return skipEmptyLine;
  }

  public void setSkipEmptyLine(String skipEmptyLine) {
    this.skipEmptyLine = skipEmptyLine;
  }
}
