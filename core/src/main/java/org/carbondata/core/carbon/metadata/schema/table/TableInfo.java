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
package org.carbondata.core.carbon.metadata.schema.table;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.carbondata.core.constants.CarbonCommonConstants;

/**
 * Store the information about the table.
 * it stores the fact table as well as aggregate table present in the schema
 */
public class TableInfo implements Serializable {

  /**
   * serialization version
   */
  private static final long serialVersionUID = -5034287968314105193L;

  /**
   * name of the database;
   */
  private String databaseName;

  /**
   * table name to group fact table and aggregate table
   */
  private String tableUniqueName;

  /**
   * fact table information
   */
  private TableSchema factTable;

  /**
   * list of aggregate table
   */
  private List<TableSchema> aggregateTableList;

  /**
   * last updated time to update the cube if any changes
   */
  private long lastUpdatedTime;

  /**
   * metadata file path (check if it is really required )
   */
  private String metaDataFilepath;

  /**
   * store location
   */
  private String storePath;

  public TableInfo() {
    aggregateTableList = new ArrayList<TableSchema>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
  }

  /**
   * @return the factTable
   */
  public TableSchema getFactTable() {
    return factTable;
  }

  /**
   * @param factTable the factTable to set
   */
  public void setFactTable(TableSchema factTable) {
    this.factTable = factTable;
  }

  /**
   * @return the aggregateTableList
   */
  public List<TableSchema> getAggregateTableList() {
    return aggregateTableList;
  }

  /**
   * @param aggregateTableList the aggregateTableList to set
   */
  public void setAggregateTableList(List<TableSchema> aggregateTableList) {
    this.aggregateTableList = aggregateTableList;
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

  public TableSchema getTableSchemaByName(String tableName) {
    if (factTable.getTableName().equalsIgnoreCase(tableName)) {
      return factTable;
    }
    for (TableSchema aggregatTableSchema : aggregateTableList) {
      if (aggregatTableSchema.getTableName().equals(tableName)) {
        return aggregatTableSchema;
      }
    }
    return null;
  }

  public TableSchema getTableSchemaByTableId(String tableId) {
    if (factTable.getTableId().equals(tableId)) {
      return factTable;
    }
    for (TableSchema aggregatTableSchema : aggregateTableList) {
      if (aggregatTableSchema.getTableId().equals(tableId)) {
        return aggregatTableSchema;
      }
    }
    return null;
  }

  public int getNumberOfAggregateTables() {
    return aggregateTableList.size();
  }

  /**
   * @return the tableUniqueName
   */
  public String getTableUniqueName() {
    return tableUniqueName;
  }

  /**
   * @param tableUniqueName the tableUniqueName to set
   */
  public void setTableUniqueName(String tableUniqueName) {
    this.tableUniqueName = tableUniqueName;
  }

  /**
   * @return the lastUpdatedTime
   */
  public long getLastUpdatedTime() {
    return lastUpdatedTime;
  }

  /**
   * @param lastUpdatedTime the lastUpdatedTime to set
   */
  public void setLastUpdatedTime(long lastUpdatedTime) {
    this.lastUpdatedTime = lastUpdatedTime;
  }

  /**
   * @return
   */
  public String getMetaDataFilepath() {
    return metaDataFilepath;
  }

  /**
   * @param metaDataFilepath
   */
  public void setMetaDataFilepath(String metaDataFilepath) {
    this.metaDataFilepath = metaDataFilepath;
  }

  public String getStorePath() {
    return storePath;
  }

  public void setStorePath(String storePath) {
    this.storePath = storePath;
  }

  /**
   * to generate the hash code
   */
  @Override public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((databaseName == null) ? 0 : databaseName.hashCode());
    result = prime * result + ((tableUniqueName == null) ? 0 : tableUniqueName.hashCode());
    return result;
  }

  /**
   * Overridden equals method
   */
  @Override public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (obj instanceof TableInfo) {
      return false;
    }
    TableInfo other = (TableInfo) obj;
    if (databaseName == null) {
      if (other.databaseName != null) {
        return false;
      }
    } else if (!tableUniqueName.equals(other.tableUniqueName)) {
      return false;
    }

    if (tableUniqueName == null) {
      if (other.tableUniqueName != null) {
        return false;
      }
    } else if (!tableUniqueName.equals(other.tableUniqueName)) {
      return false;
    }
    return true;
  }
}
