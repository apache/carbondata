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

/**
 *
 */
package org.carbondata.spark.load;

import java.io.Serializable;
import java.util.List;

import org.carbondata.core.carbon.CarbonDataLoadSchema;
import org.carbondata.core.load.LoadMetadataDetails;

public class CarbonLoadModel implements Serializable {
  /**
   *
   */
  private static final long serialVersionUID = 6580168429197697465L;

  private String databaseName;

  private String tableName;

  private String factFilePath;

  private String dimFolderPath;

  private String partitionId;

  private CarbonDataLoadSchema carbonDataLoadSchema;

  private String[] aggTables;

  private String aggTableName;

  private boolean aggLoadRequest;

  private String factStoreLocation;

  private boolean isRetentionRequest;

  private List<String> factFilesToProcess;
  private String csvHeader;
  private String csvDelimiter;
  private String complexDelimiterLevel1;
  private String complexDelimiterLevel2;

  private boolean isDirectLoad;
  private List<LoadMetadataDetails> loadMetadataDetails;

  private String blocksID;

  /**
   * task id, each spark task has a unique id
   */
  private String taskNo;
  /**
   * new load start time
   */
  private String factTimeStamp;
  /**
   * load Id
   */
  private String segmentId;

  /**
   * get blocck id
   *
   * @return
   */
  public String getBlocksID() {
    return blocksID;
  }

  /**
   * set block id for carbon load model
   *
   * @param blocksID
   */
  public void setBlocksID(String blocksID) {
    this.blocksID = blocksID;
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

  public boolean isDirectLoad() {
    return isDirectLoad;
  }

  public void setDirectLoad(boolean isDirectLoad) {
    this.isDirectLoad = isDirectLoad;
  }

  public List<String> getFactFilesToProcess() {
    return factFilesToProcess;
  }

  public void setFactFilesToProcess(List<String> factFilesToProcess) {
    this.factFilesToProcess = factFilesToProcess;
  }

  public String getCsvHeader() {
    return csvHeader;
  }

  public void setCsvHeader(String csvHeader) {
    this.csvHeader = csvHeader;
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
   * @return the dimFolderPath
   */
  public String getDimFolderPath() {
    return dimFolderPath;
  }

  //TODO SIMIAN

  /**
   * @param dimFolderPath the dimFolderPath to set
   */
  public void setDimFolderPath(String dimFolderPath) {
    this.dimFolderPath = dimFolderPath;
  }

  /**
   * get copy with parition
   *
   * @param uniqueId
   * @return
   */
  public CarbonLoadModel getCopyWithPartition(String uniqueId) {
    CarbonLoadModel copy = new CarbonLoadModel();
    copy.tableName = tableName;
    copy.dimFolderPath = dimFolderPath;
    copy.factFilePath = factFilePath + '/' + uniqueId;
    copy.databaseName = databaseName;
    copy.partitionId = uniqueId;
    copy.aggTables = aggTables;
    copy.aggTableName = aggTableName;
    copy.aggLoadRequest = aggLoadRequest;
    copy.loadMetadataDetails = loadMetadataDetails;
    copy.isRetentionRequest = isRetentionRequest;
    copy.complexDelimiterLevel1 = complexDelimiterLevel1;
    copy.complexDelimiterLevel2 = complexDelimiterLevel2;
    copy.carbonDataLoadSchema = carbonDataLoadSchema;
    copy.blocksID = blocksID;
    copy.taskNo = taskNo;
    copy.factTimeStamp = factTimeStamp;
    copy.segmentId = segmentId;
    return copy;
  }

  /**
   * get CarbonLoadModel with partition
   *
   * @param uniqueId
   * @param filesForPartition
   * @param header
   * @param delimiter
   * @return
   */
  public CarbonLoadModel getCopyWithPartition(String uniqueId, List<String> filesForPartition,
      String header, String delimiter) {
    CarbonLoadModel copyObj = new CarbonLoadModel();
    copyObj.tableName = tableName;
    copyObj.dimFolderPath = dimFolderPath;
    copyObj.factFilePath = null;
    copyObj.databaseName = databaseName;
    copyObj.partitionId = uniqueId;
    copyObj.aggTables = aggTables;
    copyObj.aggTableName = aggTableName;
    copyObj.aggLoadRequest = aggLoadRequest;
    copyObj.loadMetadataDetails = loadMetadataDetails;
    copyObj.isRetentionRequest = isRetentionRequest;
    copyObj.carbonDataLoadSchema = carbonDataLoadSchema;
    copyObj.csvHeader = header;
    copyObj.factFilesToProcess = filesForPartition;
    copyObj.isDirectLoad = true;
    copyObj.csvDelimiter = delimiter;
    copyObj.complexDelimiterLevel1 = complexDelimiterLevel1;
    copyObj.complexDelimiterLevel2 = complexDelimiterLevel2;
    copyObj.blocksID = blocksID;
    copyObj.taskNo = taskNo;
    copyObj.factTimeStamp = factTimeStamp;
    copyObj.segmentId = segmentId;
    return copyObj;
  }

  /**
   * @return the partitionId
   */
  public String getPartitionId() {
    return partitionId;
  }

  /**
   * @param partitionId the partitionId to set
   */
  public void setPartitionId(String partitionId) {
    this.partitionId = partitionId;
  }

  /**
   * @return the aggTables
   */
  public String[] getAggTables() {
    return aggTables;
  }

  /**
   * @param aggTables the aggTables to set
   */
  public void setAggTables(String[] aggTables) {
    this.aggTables = aggTables;
  }

  /**
   * @return the aggLoadRequest
   */
  public boolean isAggLoadRequest() {
    return aggLoadRequest;
  }

  /**
   * @param aggLoadRequest the aggLoadRequest to set
   */
  public void setAggLoadRequest(boolean aggLoadRequest) {
    this.aggLoadRequest = aggLoadRequest;
  }

  /**
   * @return Returns the factStoreLocation.
   */
  public String getFactStoreLocation() {
    return factStoreLocation;
  }

  /**
   * @param factStoreLocation The factStoreLocation to set.
   */
  public void setFactStoreLocation(String factStoreLocation) {
    this.factStoreLocation = factStoreLocation;
  }

  /**
   * @return Returns the aggTableName.
   */
  public String getAggTableName() {
    return aggTableName;
  }

  /**
   * @param aggTableName The aggTableName to set.
   */
  public void setAggTableName(String aggTableName) {
    this.aggTableName = aggTableName;
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
   * @param isRetentionRequest
   */
  public void setRetentionRequest(boolean isRetentionRequest) {
    this.isRetentionRequest = isRetentionRequest;
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
   * setLoadMetadataDetails.
   *
   * @param loadMetadataDetails
   */
  public void setLoadMetadataDetails(List<LoadMetadataDetails> loadMetadataDetails) {
    this.loadMetadataDetails = loadMetadataDetails;
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
  public String getFactTimeStamp() {
    return factTimeStamp;
  }

  /**
   * @param factTimeStamp
   */
  public void setFactTimeStamp(String factTimeStamp) {
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
}
