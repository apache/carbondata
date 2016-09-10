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

package org.apache.carbondata.processing.api.dataloader;

public class DataLoadModel {
  /**
   * Schema Info
   */
  private SchemaInfo schemaInfo;

  /**
   * table table
   */
  private String tableName;

  /**
   * is CSV load
   */
  private boolean isCsvLoad;

  /**
   * Modified Dimension
   */
  private String[] modifiedDimesion;

  /**
   * loadNames separated by HASH_SPC_CHARACTER
   */
  private String loadNames;
  /**
   * modificationOrDeletionTime separated by HASH_SPC_CHARACTER
   */
  private String modificationOrDeletionTime;

  private String blocksID;
  /**
   * task id, each spark task has a unique id
   */
  private String taskNo;
  /**
   * new load start time
   */
  private String factTimeStamp;

  private String escapeCharacter;

  private String quoteCharacter;

  private String commentCharacter;

  private String maxColumns;
  /**
   * @return Returns the schemaInfo.
   */
  public SchemaInfo getSchemaInfo() {
    return schemaInfo;
  }

  /**
   * @param schemaInfo The schemaInfo to set.
   */
  public void setSchemaInfo(SchemaInfo schemaInfo) {
    this.schemaInfo = schemaInfo;
  }

  /**
   * @return Returns the tableName.
   */
  public String getTableName() {
    return tableName;
  }

  /**
   * @param tableName The tableName to set.
   */
  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  /**
   * @return Returns the isCsvLoad.
   */
  public boolean isCsvLoad() {
    return isCsvLoad;
  }

  /**
   * @param isCsvLoad The isCsvLoad to set.
   */
  public void setCsvLoad(boolean isCsvLoad) {
    this.isCsvLoad = isCsvLoad;
  }

  /**
   * @return Returns the modifiedDimesion.
   */
  public String[] getModifiedDimesion() {
    return modifiedDimesion;
  }

  /**
   * @param modifiedDimesion The modifiedDimesion to set.
   */
  public void setModifiedDimesion(String[] modifiedDimesion) {
    this.modifiedDimesion = modifiedDimesion;
  }

  /**
   * return modificationOrDeletionTime separated by HASH_SPC_CHARACTER
   */
  public String getModificationOrDeletionTime() {
    return modificationOrDeletionTime;
  }

  /**
   * set modificationOrDeletionTime separated by HASH_SPC_CHARACTER
   */
  public void setModificationOrDeletionTime(String modificationOrDeletionTime) {
    this.modificationOrDeletionTime = modificationOrDeletionTime;
  }

  /**
   * return loadNames separated by HASH_SPC_CHARACTER
   */
  public String getLoadNames() {
    return loadNames;
  }

  /**
   * set loadNames separated by HASH_SPC_CHARACTER
   */
  public void setLoadNames(String loadNames) {
    this.loadNames = loadNames;
  }

  /**
   * get block id
   *
   * @return
   */
  public String getBlocksID() {
    return blocksID;
  }

  /**
   * set block id to data load model
   *
   * @param blocksID
   */
  public void setBlocksID(String blocksID) {
    this.blocksID = blocksID;
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
}

