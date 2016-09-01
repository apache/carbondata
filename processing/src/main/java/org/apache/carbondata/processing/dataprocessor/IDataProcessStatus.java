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

import java.sql.Timestamp;
import java.util.List;

public interface IDataProcessStatus {

  /**
   * serial id
   *
   * @return
   */
  int getDataloadstatusid();

  /**
   * @param dataloadstatusid
   */
  void setDataloadstatusid(int dataloadstatusid);

  /**
   * @return the createdTime
   */
  Timestamp getCreatedTime();

  /**
   * @param createdTime the createdTime to set
   */
  void setCreatedTime(Timestamp createdTime);

  /**
   * return the description of the task
   */
  String getDesc();

  /**
   * set the description of the task
   */
  void setDesc(String desc);

  /**
   * This method is used to get the Key for saving status of data loading.
   *
   * @return String - Key (databaseName + tableName + tableName).
   */
  String getKey();

  /**
   * @param key
   */
  void setKey(String key);

  /**
   * To get the status of the data loading.
   *
   * @return String - Status
   */
  String getStatus();

  /**
   * To set the status of the data loading.
   */
  void setStatus(String status);

  /**
   * Return task type
   * 1- DATALOADING 2- RESTRUCTURE
   */
  int getTaskType();

  /**
   * 1- DATALOADING 2- RESTRUCTURE
   */
  void setTaskType(int taskType);

  /**
   * @return the databaseName
   */
  String getDatabaseName();

  /**
   * @param databaseName the databaseName to set
   */
  void setDatabaseName(String databaseName);

  /**
   * @return the tableName
   */
  String getTableName();

  /**
   * @param tableName the tableName to set
   */
  void setTableName(String tableName);

  /**
   * @return the oldSchemaFilePath
   */
  String getOldSchemaFilePath();

  /**
   * @param oldSchemaFilePath the oldSchemaFilePath to set
   */
  void setOldSchemaFilePath(String oldSchemaFilePath);

  /**
   * @return the newSchemaFilePath
   */
  String getNewSchemaFilePath();

  /**
   * @param newSchemaFilePath the newSchemaFilePath to set
   */
  void setNewSchemaFilePath(String newSchemaFilePath);

  /**
   * @return the csvFilePath
   */
  String getCsvFilePath();

  /**
   * @param csvFilePath the csvFilePath to set
   */
  void setCsvFilePath(String csvFilePath);

  String getDimCSVDirLoc();

  void setDimCSVDirLoc(String dimCSVDirLoc);

  /**
   * @return the dimTables
   */
  String getDimTables();

  /**
   * @param dimTables the dimTables to set
   */
  void setDimTables(String dimTables);

  /**
   * @return the isFromPathApi
   */
  boolean isFromPathApi();

  /**
   * @param isFromPathApi the isFromPathApi to set
   */
  void setFromPathApi(boolean isFromPathApi);

  /**
   * @return
   */
  IDataProcessStatus makeCopy();

  boolean isDirectLoad();

  void setDirectLoad(boolean isDirectLoad);

  List<String> getFilesToProcess();

  void setFilesToProcess(List<String> filesToProcess);

  String getCsvHeader();

  void setCsvHeader(String csvHeader);

  String getCsvDelimiter();

  void setCsvDelimiter(String csvDelimiter);

  String getBlocksID();

  String getEscapeCharacter();

  String getQuoteCharacter();

  String getCommentCharacter();
}
