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
import java.util.HashMap;
import java.util.List;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datamap.Segment;
import org.apache.carbondata.core.dictionary.service.DictionaryServiceProvider;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails;
import org.apache.carbondata.core.statusmanager.SegmentDetailVO;
import org.apache.carbondata.core.statusmanager.SegmentStatusManager;
import org.apache.carbondata.core.statusmanager.SegmentUpdateStatusManager;
import org.apache.carbondata.core.util.path.CarbonTablePath;

public class CarbonLoadModel implements Serializable {

  private static final long serialVersionUID = 6580168429197697465L;

  private String databaseName;

  private String tableName;

  private String factFilePath;

  private String colDictFilePath;

  private CarbonDataLoadSchema carbonDataLoadSchema;

  private boolean aggLoadRequest;

  private String tablePath;

  private String parentTablePath;

  /*
     This points if the carbonTable is a Non Transactional Table or not.
     The path will be pointed by the tablePath. And there will be
     no Metadata folder present for the Non Transactional Table.
   */
  private boolean carbonTransactionalTable = true;

  private String csvHeader;
  private String[] csvHeaderColumns;
  private String csvDelimiter;
  private String complexDelimiterLevel1;
  private String complexDelimiterLevel2;

  private SegmentDetailVO currentDetailVO;
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
  private Segment segment;

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
   * To identify the suitable input processor step for json file loading.
   */
  private boolean isJsonFileLoad;

  /**
   * Flder path to where data should be written for this load.
   */
  private String dataWritePath;

  /**
   * sort columns bounds
   */
  private String loadMinSize;

  private List<String> mergedSegmentIds;

  public boolean isAggLoadRequest() {
    return isAggLoadRequest;
  }

  public void setAggLoadRequest(boolean aggLoadRequest) {
    isAggLoadRequest = aggLoadRequest;
  }

  public String getParentTablePath() {
    return parentTablePath;
  }

  public void setParentTablePath(String parentTablePath) {
    this.parentTablePath = parentTablePath;
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

  public String getSortColumnsBoundsStr() {
    return sortColumnsBoundsStr;
  }

  public void setSortColumnsBoundsStr(String sortColumnsBoundsStr) {
    this.sortColumnsBoundsStr = sortColumnsBoundsStr;
  }

  public String getLoadMinSize() {
    return loadMinSize;
  }

  public void setLoadMinSize(String loadMinSize) {
    this.loadMinSize = loadMinSize;
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
    copy.currentDetailVO = currentDetailVO;
    copy.csvHeader = csvHeader;
    copy.csvHeaderColumns = csvHeaderColumns;
    copy.csvDelimiter = csvDelimiter;
    copy.complexDelimiterLevel1 = complexDelimiterLevel1;
    copy.complexDelimiterLevel2 = complexDelimiterLevel2;
    copy.carbonDataLoadSchema = carbonDataLoadSchema;
    copy.blocksID = blocksID;
    copy.taskNo = taskNo;
    copy.factTimeStamp = factTimeStamp;
    copy.segment = segment;
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
    copy.carbonTransactionalTable = carbonTransactionalTable;
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
    copy.isLoadWithoutConverterStep = isLoadWithoutConverterStep;
    copy.sortColumnsBoundsStr = sortColumnsBoundsStr;
    copy.loadMinSize = loadMinSize;
    copy.parentTablePath = parentTablePath;
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
    copyObj.currentDetailVO = currentDetailVO;
    copyObj.carbonDataLoadSchema = carbonDataLoadSchema;
    copyObj.csvHeader = header;
    copyObj.csvHeaderColumns = csvHeaderColumns;
    copyObj.csvDelimiter = delimiter;
    copyObj.complexDelimiterLevel1 = complexDelimiterLevel1;
    copyObj.complexDelimiterLevel2 = complexDelimiterLevel2;
    copyObj.blocksID = blocksID;
    copyObj.taskNo = taskNo;
    copyObj.factTimeStamp = factTimeStamp;
    copyObj.segment = segment;
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
    copyObj.carbonTransactionalTable = carbonTransactionalTable;
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
    copyObj.sortColumnsBoundsStr = sortColumnsBoundsStr;
    copyObj.loadMinSize = loadMinSize;
    copyObj.parentTablePath = parentTablePath;
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
   * Get the current load metadata.
   *
   * @return
   */
  public SegmentDetailVO getCurrentDetailVO() {
    return currentDetailVO;
  }

  public void setCurrentDetailVO(SegmentDetailVO currentLoadMeta) {
    this.currentDetailVO = currentLoadMeta;
    this.factTimeStamp = currentDetailVO.getLoadStartTime();
    if (getSegmentId() == null) {
      setSegmentId(currentLoadMeta.getSegmentId());
    }
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


  public boolean isLoadWithoutConverterStep() {
    return isLoadWithoutConverterStep;
  }

  public void setLoadWithoutConverterStep(boolean loadWithoutConverterStep) {
    isLoadWithoutConverterStep = loadWithoutConverterStep;
  }

  public boolean isJsonFileLoad() {
    return isJsonFileLoad;
  }

  public void setJsonFileLoad(boolean isJsonFileLoad) {
    this.isJsonFileLoad = isJsonFileLoad;
  }

  public String getDataWritePath() {
    return dataWritePath;
  }

  public void setDataWritePath(String dataWritePath) {
    this.dataWritePath = dataWritePath;
  }

  public boolean isCarbonTransactionalTable() {
    return carbonTransactionalTable;
  }

  public void setCarbonTransactionalTable(boolean carbonTransactionalTable) {
    this.carbonTransactionalTable = carbonTransactionalTable;
  }

  public void setMergedSegmentIds(List<String> mergedSegmentIds) {
    this.mergedSegmentIds = mergedSegmentIds;
  }

  public List<String> getMergedSegmentIds() {
    if (null == mergedSegmentIds) {
      mergedSegmentIds = new ArrayList<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    }
    return mergedSegmentIds;
  }

}
