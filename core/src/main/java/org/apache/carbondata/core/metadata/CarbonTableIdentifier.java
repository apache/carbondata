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

package org.apache.carbondata.core.metadata;

import java.io.File;
import java.io.Serializable;

/**
 * Identifier class which will hold the table qualified name
 */
public class CarbonTableIdentifier implements Serializable {

  /**
   * database name
   */
  private String databaseName;

  /**
   * table name
   */
  private String tableName;

  /**
   * table id
   */
  private String tableId;

  /**
   * constructor
   */
  public CarbonTableIdentifier(String databaseName, String tableName, String tableId) {
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.tableId = tableId;
  }

  /**
   * return database name
   */
  public String getDatabaseName() {
    return databaseName;
  }

  /**
   * return table name
   */
  public String getTableName() {
    return tableName;
  }

  /**
   * @return tableId
   */
  public String getTableId() {
    return tableId;
  }

  /**
   * @return table unique name
   */
  public String getTableUniqueName() {
    return databaseName + '_' + tableName;
  }

  /**
   *Creates the key for bad record lgger.
   */
  public String getBadRecordLoggerKey() {
    return databaseName + File.separator + tableName + '_' + tableId;
  }

  @Override public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((databaseName == null) ? 0 : databaseName.hashCode());
    result = prime * result + ((tableId == null) ? 0 : tableId.hashCode());
    result = prime * result + ((tableName == null) ? 0 : tableName.hashCode());
    return result;
  }

  @Override public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    CarbonTableIdentifier other = (CarbonTableIdentifier) obj;
    if (databaseName == null) {
      if (other.databaseName != null) {
        return false;
      }
    } else if (!databaseName.equals(other.databaseName)) {
      return false;
    }
    if (tableId == null) {
      if (other.tableId != null) {
        return false;
      }
    } else if (!tableId.equals(other.tableId)) {
      return false;
    }
    if (tableName == null) {
      if (other.tableName != null) {
        return false;
      }
    } else if (!tableName.equals(other.tableName)) {
      return false;
    }
    return true;
  }

  /**
   * return unique table name
   */
  @Override public String toString() {
    return databaseName + '_' + tableName;
  }
}
