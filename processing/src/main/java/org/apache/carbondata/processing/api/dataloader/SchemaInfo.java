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

package org.apache.carbondata.processing.api.dataloader;

public class SchemaInfo {

  /**
   * databaseName
   */
  private String databaseName;

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
   * defines the option to specify whether to bad record logger action
   */
  private String badRecordsLoggerAction;


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
   * returns the option to set bad record logger action
   * @return
   */
  public String getBadRecordsLoggerAction() {
    return badRecordsLoggerAction;
  }

  /**
   * set the option to set set bad record logger action
   * @param badRecordsLoggerAction
   */
  public void setBadRecordsLoggerAction(String badRecordsLoggerAction) {
    this.badRecordsLoggerAction = badRecordsLoggerAction;
  }
}
