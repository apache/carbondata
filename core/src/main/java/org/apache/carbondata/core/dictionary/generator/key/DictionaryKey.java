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
package org.apache.carbondata.core.dictionary.generator.key;

import java.io.Serializable;

/**
 * Dictionary key to generate dictionary
 */
public class DictionaryKey implements Serializable {

  /**
   * tableUniqueName
   */
  private String tableUniqueName;

  /**
   * columnName
   */
  private String columnName;

  /**
   * message data
   */
  private Object data;

  /**
   * message type
   */
  private String type;

  /**
   * dictionary client thread no
   */
  private String threadNo;

  public String getTableUniqueName() {
    return tableUniqueName;
  }

  public String getColumnName() {
    return columnName;
  }

  public Object getData() {
    return data;
  }

  public void setData(Object data) {
    this.data = data;
  }

  public void setThreadNo(String threadNo) {
    this.threadNo = threadNo;
  }

  public String getThreadNo() {
    return this.threadNo;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public void setTableUniqueName(String tableUniqueName) {
    this.tableUniqueName = tableUniqueName;
  }

  public void setColumnName(String columnName) {
    this.columnName = columnName;
  }
}
