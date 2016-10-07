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

public class SchemaInfo {

  /**
   * databaseName
   */
  private String databaseName;

  /**
   * srcDriverName
   */
  private String srcDriverName;

  /**
   * srcConUrl
   */
  private String srcConUrl;

  /**
   * srcUserName
   */
  private String srcUserName;

  /**
   * srcPwd
   */
  private String srcPwd;

  /**
   * tableName
   */
  private String tableName;

  /**
   * isAutoAggregateRequest
   */
  private boolean isAutoAggregateRequest;

  private String complexDelimiterLevel1;

  private String complexDelimiterLevel2;
  /**
   * the value to be treated as null while data load
   */
  private String serializationNullFormat;

  /**
   * defines the string to specify whether the bad record logger should be enabled or not
   */
  private String badRecordsLoggerEnable;
  /**
   * defines the option to specify whether to redirect the bad record logger to raw csv or not
   */
  private String badRecordsLoggerRedirect;


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

  /**
   * @return Returns the srcDriverName.
   */
  public String getSrcDriverName() {
    return srcDriverName;
  }

  /**
   * @param srcDriverName The srcDriverName to set.
   */
  public void setSrcDriverName(String srcDriverName) {
    this.srcDriverName = srcDriverName;
  }

  /**
   * @return Returns the srcConUrl.
   */
  public String getSrcConUrl() {
    return srcConUrl;
  }

  /**
   * @param srcConUrl The srcConUrl to set.
   */
  public void setSrcConUrl(String srcConUrl) {
    this.srcConUrl = srcConUrl;
  }

  /**
   * @return Returns the srcUserName.
   */
  public String getSrcUserName() {
    return srcUserName;
  }

  /**
   * @param srcUserName The srcUserName to set.
   */
  public void setSrcUserName(String srcUserName) {
    this.srcUserName = srcUserName;
  }

  /**
   * @return Returns the srcPwd.
   */
  public String getSrcPwd() {
    return srcPwd;
  }

  /**
   * @param srcPwd The srcPwd to set.
   */
  public void setSrcPwd(String srcPwd) {
    this.srcPwd = srcPwd;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  /**
   * @return the isAutoAggregateRequest
   */
  public boolean isAutoAggregateRequest() {
    return isAutoAggregateRequest;
  }

  /**
   * @param isAutoAggregateRequest the isAutoAggregateRequest to set
   */
  public void setAutoAggregateRequest(boolean isAutoAggregateRequest) {
    this.isAutoAggregateRequest = isAutoAggregateRequest;
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
   * the method returns the value to be treated as null while data load
   * @return
   */
  public String getSerializationNullFormat() {
    return serializationNullFormat;
  }

  /**
   * the method sets the value to be treated as null while data load
   * @param serializationNullFormat
   */
  public void setSerializationNullFormat(String serializationNullFormat) {
    this.serializationNullFormat = serializationNullFormat;
  }

  /**
   * returns the string to enable bad record logger
   * @return
   */
  public String getBadRecordsLoggerEnable() {
    return badRecordsLoggerEnable;
  }

  /**
   * method sets the string to specify whether to enable or dissable the badrecord logger.
   * @param badRecordsLoggerEnable
   */
  public void setBadRecordsLoggerEnable(String badRecordsLoggerEnable) {
    this.badRecordsLoggerEnable = badRecordsLoggerEnable;
  }

  /**
   * returns the option to set to redirect the badrecord logger to raw csv
   * @return
   */
  public String getBadRecordsLoggerRedirect() {
    return badRecordsLoggerRedirect;
  }

  /**
   * set the option to set to redirect the badrecord logger to raw csv
   * @param badRecordsLoggerRedirect
   */
  public void setBadRecordsLoggerRedirect(String badRecordsLoggerRedirect) {
    this.badRecordsLoggerRedirect = badRecordsLoggerRedirect;
  }
}
