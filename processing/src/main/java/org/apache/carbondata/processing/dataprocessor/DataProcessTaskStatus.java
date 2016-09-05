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

package org.apache.carbondata.processing.dataprocessor;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.List;

public class DataProcessTaskStatus implements IDataProcessStatus, Serializable {

  private static final long serialVersionUID = 1L;

  /**
   * DataLoader Status Identifier.
   */
  private int dataloadstatusid;

  /**
   *
   */
  private Timestamp createdTime;

  /**
   * Status Identifier.
   */
  private String key;

  /**
   * Status .
   */
  private String status;

  /**
   * description for the task
   */
  private String desc;

  /**
   * task type
   */
  private int taskType;

  private String databaseName;

  private String tableName;

  private String newSchemaFilePath;

  private String oldSchemaFilePath;

  private String csvFilePath;

  /**
   * dimCSVDirLoc
   */
  private String dimCSVDirLoc;

  private String dimTables;

  private boolean isDirectLoad;
  private List<String> filesToProcess;
  private String csvHeader;
  private String csvDelimiter;
  /**
   * Set if the call to restructre from path or by upload
   */
  private boolean isFromPathApi;

  private String blocksID;

  private String escapeCharacter;

  private String quoteCharacter;

  private String commentCharacter;

  public DataProcessTaskStatus(String databaseName, String tableName) {
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.desc = "";
    this.setNewSchemaFilePath("");
    this.setOldSchemaFilePath("");
  }

  public DataProcessTaskStatus() {
  }

  public boolean isDirectLoad() {
    return isDirectLoad;
  }

  public void setDirectLoad(boolean isDirectLoad) {
    this.isDirectLoad = isDirectLoad;
  }

  public List<String> getFilesToProcess() {
    return filesToProcess;
  }

  public void setFilesToProcess(List<String> filesToProcess) {
    this.filesToProcess = filesToProcess;
  }

  public String getCsvHeader() {
    return csvHeader;
  }

  public void setCsvHeader(String csvHeader) {
    this.csvHeader = csvHeader;
  }

  public String getCsvDelimiter() {
    return csvDelimiter;
  }

  public void setCsvDelimiter(String csvDelimiter) {
    this.csvDelimiter = csvDelimiter;
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

  public String getDesc() {
    return desc;
  }

  public void setDesc(String desc) {
    this.desc = desc;
  }

  @Override public String getKey() {
    return key;
  }

  public void setKey(String key) {
    this.key = key;
  }

  @Override public String getStatus() {
    return status;
  }

  public void setStatus(String status) {
    this.status = status;
  }

  public int getDataloadstatusid() {
    return dataloadstatusid;
  }

  public void setDataloadstatusid(int dataloadstatusid) {
    this.dataloadstatusid = dataloadstatusid;
  }

  public Timestamp getCreatedTime() {
    return createdTime;
  }

  public void setCreatedTime(Timestamp createdTime) {
    this.createdTime = createdTime;
  }

  public int getTaskType() {
    return taskType;
  }

  public void setTaskType(int taskType) {
    this.taskType = taskType;
  }

  public String getOldSchemaFilePath() {
    return oldSchemaFilePath;
  }

  public void setOldSchemaFilePath(String oldSchemaFilePath) {
    this.oldSchemaFilePath = oldSchemaFilePath;
  }

  public String getNewSchemaFilePath() {
    return newSchemaFilePath;
  }

  public void setNewSchemaFilePath(String newSchemaFilePath) {
    this.newSchemaFilePath = newSchemaFilePath;
  }

  public String getCsvFilePath() {
    return csvFilePath;
  }

  public void setCsvFilePath(String csvFilePath) {
    this.csvFilePath = csvFilePath;
  }

  public String getDimTables() {
    return dimTables;
  }

  public void setDimTables(String dimTables) {
    this.dimTables = dimTables;
  }

  public boolean isFromPathApi() {
    return isFromPathApi;
  }

  public void setFromPathApi(boolean isFromPathApi) {
    this.isFromPathApi = isFromPathApi;
  }

  /**
   * to make a copy
   */
  public IDataProcessStatus makeCopy() {
    IDataProcessStatus copy = new DataProcessTaskStatus();
    copy.setTableName(this.tableName);
    copy.setDataloadstatusid(this.dataloadstatusid);
    copy.setDesc(this.desc);
    copy.setKey(this.key);
    copy.setDatabaseName(databaseName);
    copy.setStatus(status);
    return copy;
  }

  public String getDimCSVDirLoc() {
    return dimCSVDirLoc;
  }

  public void setDimCSVDirLoc(String dimCSVDirLoc) {
    this.dimCSVDirLoc = dimCSVDirLoc;
  }

  public String getBlocksID() {
    return blocksID;
  }

  public void setBlocksID(String blocksID) {
    this.blocksID = blocksID;
  }

  public String getEscapeCharacter() {
    return escapeCharacter;
  }

  public void setEscapeCharacter(String escapeCharacter) {
    this.escapeCharacter = escapeCharacter;
  }

  public String getQuoteCharacter() { return quoteCharacter; }

  public void setQuoteCharacter(String quoteCharacter) { this.quoteCharacter = quoteCharacter; }

  public String getCommentCharacter() { return commentCharacter; }

  public void setCommentCharacter(String commentCharacter) {
    this.commentCharacter = commentCharacter;
  }
}
